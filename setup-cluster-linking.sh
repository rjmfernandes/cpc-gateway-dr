#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# Configuration
# ============================================================================

TOPIC="${TOPIC:-test-topic}"
CONSUMER_GROUP="${CONSUMER_GROUP:-dr-demo-consumer}"

KAFKA1_CONTAINER="${KAFKA1_CONTAINER:-kafka-1}"
KAFKA2_CONTAINER="${KAFKA2_CONTAINER:-kafka-2}"

KAFKA1_BOOTSTRAP="${KAFKA1_BOOTSTRAP:-kafka-1:44444}"
KAFKA2_BOOTSTRAP="${KAFKA2_BOOTSTRAP:-kafka-2:22222}"

LINK_NAME="${LINK_NAME:-source-to-destination}"

FILTER_FILE="${FILTER_FILE:-cluster-linking/consumer-offset-group-filters.json}"

POLL_SECONDS="${POLL_SECONDS:-1}"
MAX_WAIT_SECONDS="${MAX_WAIT_SECONDS:-120}"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT


# ============================================================================
# Colors
# ============================================================================

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'


# ============================================================================
# Helpers
# ============================================================================

separator() {
    printf '%s\n' \
        "============================================================"
}

step() {
    printf '\n'
    printf "${CYAN}${BOLD}[%s] %s${NC}\n" "$1" "$2"
    printf '\n'
}

success() {
    printf "${GREEN}OK${NC}  %s\n" "$1"
}

failure() {
    printf "${RED}ERROR${NC}  %s\n" "$1"
}

warning() {
    printf "${YELLOW}WARN${NC}  %s\n" "$1"
}


wait_for_kafka() {

    local container="$1"
    local bootstrap="$2"
    local elapsed=0

    while [ "$elapsed" -lt "$MAX_WAIT_SECONDS" ]; do

        if docker exec "$container" \
            kafka-topics \
            --bootstrap-server "$bootstrap" \
            --list \
            >/dev/null 2>&1
        then
            return 0
        fi

        sleep "$POLL_SECONDS"
        elapsed=$((elapsed + POLL_SECONDS))
    done

    return 1
}


get_link_description() {

    local container="$1"
    local bootstrap="$2"

    docker exec "$container" \
        kafka-cluster-links \
        --bootstrap-server "$bootstrap" \
        --describe \
        --link "$LINK_NAME" \
        2>&1 || true
}


link_exists() {

    local container="$1"
    local bootstrap="$2"
    local output

    output="$(
        get_link_description \
            "$container" \
            "$bootstrap"
    )"

    printf '%s\n' "$output" |
        grep -Fq "Link name: '$LINK_NAME'"
}


get_mirror_description() {

    local container="$1"
    local bootstrap="$2"

    docker exec "$container" \
        kafka-mirrors \
        --bootstrap-server "$bootstrap" \
        --describe \
        --topics "$TOPIC" \
        2>&1 || true
}


# ============================================================================
# Header
# ============================================================================

clear

printf '\n'
separator
printf '\n'

printf "${BOLD}       BIDIRECTIONAL CLUSTER LINKING SETUP${NC}\n"

printf '\n'
separator
printf '\n'


# ============================================================================
# Validate filter file
# ============================================================================

if [ ! -f "$FILTER_FILE" ]; then

    failure "Required file '$FILTER_FILE' was not found."

    printf '\n'
    printf 'Expected path:\n'
    printf '\n'
    printf '  %s\n' "$FILTER_FILE"
    printf '\n'

    exit 1
fi


printf 'Waiting for Kafka clusters...\n'
printf '\n'

if ! wait_for_kafka \
    "$KAFKA1_CONTAINER" \
    "$KAFKA1_BOOTSTRAP"
then

    failure "Kafka 1 is unavailable."
    exit 1
fi


if ! wait_for_kafka \
    "$KAFKA2_CONTAINER" \
    "$KAFKA2_BOOTSTRAP"
then

    failure "Kafka 2 is unavailable."
    exit 1
fi


success "Both Kafka clusters are online."

printf '\n'

printf 'Initial topology:\n'
printf '\n'

printf '  Kafka 1 [SOURCE / WRITABLE]\n'
printf '       |\n'
printf '       | %s\n' "$TOPIC"
printf '       v\n'
printf '  Kafka 2 [MIRROR / DR]\n'

printf '\n'

printf 'Topic          : %s\n' "$TOPIC"
printf 'Consumer group : %s\n' "$CONSUMER_GROUP"
printf 'Cluster Link   : %s\n' "$LINK_NAME"

printf '\n'
separator


# ============================================================================
# STEP 1
# Source topic
# ============================================================================

step "1/7" "PREPARING SOURCE TOPIC"

docker exec "$KAFKA1_CONTAINER" \
    kafka-topics \
    --bootstrap-server "$KAFKA1_BOOTSTRAP" \
    --create \
    --if-not-exists \
    --topic "$TOPIC" \
    --partitions 1 \
    --replication-factor 1


success "Topic '$TOPIC' exists on Kafka 1."


# ============================================================================
# STEP 2
# Cluster IDs
# ============================================================================

step "2/7" "DISCOVERING CLUSTER IDS"


KAFKA1_ID="$(
    docker exec "$KAFKA1_CONTAINER" \
        kafka-cluster cluster-id \
        --bootstrap-server "$KAFKA1_BOOTSTRAP" \
        2>/dev/null |
        awk 'NF {x=$NF} END {print x}'
)"


KAFKA2_ID="$(
    docker exec "$KAFKA2_CONTAINER" \
        kafka-cluster cluster-id \
        --bootstrap-server "$KAFKA2_BOOTSTRAP" \
        2>/dev/null |
        awk 'NF {x=$NF} END {print x}'
)"


if [ -z "$KAFKA1_ID" ] || [ -z "$KAFKA2_ID" ]; then

    failure "Could not discover Kafka cluster IDs."

    exit 1
fi


printf 'Kafka 1 : %s\n' "$KAFKA1_ID"
printf 'Kafka 2 : %s\n' "$KAFKA2_ID"

success "Cluster IDs discovered."


# ============================================================================
# STEP 3
# Prepare link configuration
#
# IMPORTANT:
#
# consumer.offset.group.filters is NOT placed in these property files.
#
# This version of kafka-cluster-links requires the filters to be supplied
# separately with:
#
#     --consumer-group-filters-json-file
#
# ============================================================================

step "3/7" "PREPARING LINK CONFIGURATION"


cat > "$TMP_DIR/kafka1-link.properties" <<EOF
bootstrap.servers=$KAFKA2_BOOTSTRAP
link.mode=BIDIRECTIONAL
consumer.offset.sync.enable=true
consumer.offset.sync.ms=1000
consumer.offset.sync.clamp.offsets=true
acl.sync.enable=false
EOF


cat > "$TMP_DIR/kafka2-link.properties" <<EOF
bootstrap.servers=$KAFKA1_BOOTSTRAP
link.mode=BIDIRECTIONAL
consumer.offset.sync.enable=true
consumer.offset.sync.ms=1000
consumer.offset.sync.clamp.offsets=true
acl.sync.enable=false
EOF


printf 'Copying Kafka 1 link configuration...\n'

docker cp \
    "$TMP_DIR/kafka1-link.properties" \
    "$KAFKA1_CONTAINER:/tmp/link-to-kafka2.properties"


printf 'Copying Kafka 2 link configuration...\n'

docker cp \
    "$TMP_DIR/kafka2-link.properties" \
    "$KAFKA2_CONTAINER:/tmp/link-to-kafka1.properties"


printf 'Copying consumer offset filters to Kafka 1...\n'

docker cp \
    "$FILTER_FILE" \
    "$KAFKA1_CONTAINER:/tmp/consumer-offset-group-filters.json"


printf 'Copying consumer offset filters to Kafka 2...\n'

docker cp \
    "$FILTER_FILE" \
    "$KAFKA2_CONTAINER:/tmp/consumer-offset-group-filters.json"


docker exec --user root "$KAFKA1_CONTAINER" \
    chmod 644 \
    /tmp/link-to-kafka2.properties \
    /tmp/consumer-offset-group-filters.json


docker exec --user root "$KAFKA2_CONTAINER" \
    chmod 644 \
    /tmp/link-to-kafka1.properties \
    /tmp/consumer-offset-group-filters.json


success "Configuration prepared."

printf '\n'

printf 'Consumer group filter:\n'
printf '\n'
printf '  %s\n' "$FILTER_FILE"
printf '\n'

printf 'The JSON filter is passed separately to kafka-cluster-links.\n'


# ============================================================================
# STEP 4
# Link on Kafka 2
#
# Kafka 2 -> remote Kafka 1
# ============================================================================

step "4/7" "CREATING LINK ON KAFKA 2"

printf 'Local  : Kafka 2\n'
printf 'Remote : Kafka 1\n'
printf 'Link   : %s\n' "$LINK_NAME"
printf '\n'


if link_exists \
    "$KAFKA2_CONTAINER" \
    "$KAFKA2_BOOTSTRAP"
then

    success "Link '$LINK_NAME' already exists on Kafka 2."

else

    docker exec "$KAFKA2_CONTAINER" \
        kafka-cluster-links \
        --bootstrap-server "$KAFKA2_BOOTSTRAP" \
        --create \
        --link "$LINK_NAME" \
        --cluster-id "$KAFKA1_ID" \
        --config-file /tmp/link-to-kafka1.properties \
        --consumer-group-filters-json-file \
            /tmp/consumer-offset-group-filters.json

fi


if ! link_exists \
    "$KAFKA2_CONTAINER" \
    "$KAFKA2_BOOTSTRAP"
then

    printf '\n'

    failure "Cluster Link creation failed on Kafka 2."

    printf '\n'
    printf 'Kafka output:\n'
    printf '\n'

    get_link_description \
        "$KAFKA2_CONTAINER" \
        "$KAFKA2_BOOTSTRAP"

    exit 1
fi


success "Cluster Link exists on Kafka 2."


# ============================================================================
# STEP 5
# Link on Kafka 1
#
# Kafka 1 -> remote Kafka 2
# ============================================================================

step "5/7" "CREATING LINK ON KAFKA 1"

printf 'Local  : Kafka 1\n'
printf 'Remote : Kafka 2\n'
printf 'Link   : %s\n' "$LINK_NAME"
printf '\n'


if link_exists \
    "$KAFKA1_CONTAINER" \
    "$KAFKA1_BOOTSTRAP"
then

    success "Link '$LINK_NAME' already exists on Kafka 1."

else

    docker exec "$KAFKA1_CONTAINER" \
        kafka-cluster-links \
        --bootstrap-server "$KAFKA1_BOOTSTRAP" \
        --create \
        --link "$LINK_NAME" \
        --cluster-id "$KAFKA2_ID" \
        --config-file /tmp/link-to-kafka2.properties \
        --consumer-group-filters-json-file \
            /tmp/consumer-offset-group-filters.json

fi


if ! link_exists \
    "$KAFKA1_CONTAINER" \
    "$KAFKA1_BOOTSTRAP"
then

    printf '\n'

    failure "Cluster Link creation failed on Kafka 1."

    printf '\n'
    printf 'Kafka output:\n'
    printf '\n'

    get_link_description \
        "$KAFKA1_CONTAINER" \
        "$KAFKA1_BOOTSTRAP"

    exit 1
fi


success "Cluster Link exists on Kafka 1."


# ============================================================================
# STEP 6
# Verify link configuration
# ============================================================================

step "6/7" "VERIFYING BIDIRECTIONAL CLUSTER LINK"


KAFKA1_LINK="$(
    get_link_description \
        "$KAFKA1_CONTAINER" \
        "$KAFKA1_BOOTSTRAP"
)"


KAFKA2_LINK="$(
    get_link_description \
        "$KAFKA2_CONTAINER" \
        "$KAFKA2_BOOTSTRAP"
)"


printf 'Kafka 1:\n'
printf '\n'

printf '%s\n' "$KAFKA1_LINK" |
    grep -E \
        "Link name:|link mode:|link state:|ConsumerOffsetSync" \
        || true


printf '\n'
printf 'Kafka 2:\n'
printf '\n'

printf '%s\n' "$KAFKA2_LINK" |
    grep -E \
        "Link name:|link mode:|link state:|ConsumerOffsetSync" \
        || true


if ! printf '%s\n' "$KAFKA1_LINK" |
    grep -Fq "link mode: 'BIDIRECTIONAL'"
then

    failure "Kafka 1 Cluster Link is not BIDIRECTIONAL."

    exit 1
fi


if ! printf '%s\n' "$KAFKA2_LINK" |
    grep -Fq "link mode: 'BIDIRECTIONAL'"
then

    failure "Kafka 2 Cluster Link is not BIDIRECTIONAL."

    exit 1
fi


success "Bidirectional Cluster Link verified."


# ============================================================================
# STEP 7
# Create initial mirror
#
# Kafka 1 source -> Kafka 2 mirror
# ============================================================================

step "7/7" "CREATING INITIAL DR MIRROR"

printf 'Replication direction:\n'
printf '\n'

printf "  Kafka 1 ${GREEN}[SOURCE / WRITABLE]${NC}"
printf '  --------------------->  '
printf "Kafka 2 ${YELLOW}[MIRROR / DR]${NC}\n"

printf '\n'


MIRROR_OUTPUT="$(
    get_mirror_description \
        "$KAFKA2_CONTAINER" \
        "$KAFKA2_BOOTSTRAP"
)"


if printf '%s\n' "$MIRROR_OUTPUT" |
    grep -Eq 'State:[[:space:]]*(ACTIVE|PAUSED|STOPPED|PENDING)'
then

    success "Mirror topic '$TOPIC' already exists on Kafka 2."

else

    printf 'Creating mirror topic on Kafka 2...\n'
    printf '\n'

    CREATE_RC=0

    CREATE_OUTPUT="$(
        docker exec "$KAFKA2_CONTAINER" \
            kafka-mirrors \
            --bootstrap-server "$KAFKA2_BOOTSTRAP" \
            --create \
            --mirror-topic "$TOPIC" \
            --link "$LINK_NAME" \
            --replication-factor 1 \
            2>&1
    )" || CREATE_RC=$?


    printf '%s\n' "$CREATE_OUTPUT"


    if [ "$CREATE_RC" -ne 0 ]; then

        printf '\n'

        failure "Mirror topic creation failed."

        printf '\n'
        printf 'Kafka 2 Cluster Link:\n'
        printf '\n'

        get_link_description \
            "$KAFKA2_CONTAINER" \
            "$KAFKA2_BOOTSTRAP"

        exit "$CREATE_RC"
    fi

fi


printf '\n'
printf 'Waiting for mirror topic to become ACTIVE...\n'
printf '\n'


elapsed=0
READY=false
LAST_OUTPUT=""


while [ "$elapsed" -lt "$MAX_WAIT_SECONDS" ]; do

    LAST_OUTPUT="$(
        get_mirror_description \
            "$KAFKA2_CONTAINER" \
            "$KAFKA2_BOOTSTRAP"
    )"


    STATE="$(
        printf '%s\n' "$LAST_OUTPUT" |
            grep -Eo \
                'PENDING_SETUP_FOR_RESTORE|PENDING_RESTORE_MIRROR|PENDING_SYNCHRONIZE|ACTIVE|STOPPED|FAILED|PAUSED' |
            head -1 || true
    )"


    LAG="$(
        printf '%s\n' "$LAST_OUTPUT" |
            sed -n \
                's/.*Lag:[[:space:]]*\([0-9][0-9]*\).*/\1/p' |
            sort -nr |
            head -1 || true
    )"


    STATE="${STATE:-unknown}"
    LAG="${LAG:-unknown}"


    printf '\r  Kafka 2 mirror: %-20s Lag: %-8s' \
        "$STATE" \
        "$LAG"


    if [ "$STATE" = "ACTIVE" ]; then

        READY=true
        break
    fi


    if [ "$STATE" = "FAILED" ]; then

        printf '\n'
        printf '\n'

        failure "Mirror entered FAILED state."

        printf '\n'
        printf '%s\n' "$LAST_OUTPUT"

        exit 1
    fi


    sleep "$POLL_SECONDS"

    elapsed=$((elapsed + POLL_SECONDS))

done


printf '\n'
printf '\n'


if [ "$READY" != true ]; then

    failure "Mirror topic did not become ACTIVE."

    printf '\n'
    printf 'Last kafka-mirrors output:\n'
    printf '\n'
    printf '%s\n' "$LAST_OUTPUT"

    exit 1
fi


success "Kafka 2 mirror is ACTIVE."


# ============================================================================
# Complete
# ============================================================================

printf '\n'
separator
printf '\n'

printf "${GREEN}${BOLD}             CLUSTER LINKING READY${NC}\n"

printf '\n'
separator
printf '\n'

printf 'Initial topology:\n'
printf '\n'

printf "  Kafka 1 ${GREEN}[PRIMARY / WRITABLE]${NC}\n"
printf '       |\n'
printf '       | Cluster Linking\n'
printf '       v\n'
printf "  Kafka 2 ${GREEN}[DR MIRROR]${NC}\n"

printf '\n'

printf 'Link           : %s\n' "$LINK_NAME"
printf 'Topic          : %s\n' "$TOPIC"
printf 'Consumer group : %s\n' "$CONSUMER_GROUP"
printf 'Offset sync    : enabled\n'
printf 'Filter         : LOCAL_MIRROR\n'

printf '\n'
separator
printf '\n'