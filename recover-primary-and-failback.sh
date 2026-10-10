#!/usr/bin/env bash

set -euo pipefail


# =============================================================================
# CPC Gateway PRIMARY RECOVERY / FAILBACK
#
# EXPECTED STARTING STATE
#
#   Producer          RUNNING
#   Consumer          MAY BE RUNNING
#   Gateway           -> Kafka 2
#   Kafka 2           SOURCE / WRITABLE
#   Kafka 1           DOWN / RECOVERING
#
# IMPORTANT:
#
#   Producer and consumer may remain running.
#   Consumer offsets are replicated asynchronously; failback may replay records
#   already consumed on Kafka 2. Offset changes do not block recovery.
#   Topic synchronization and mirror state checks remain required.
# =============================================================================


TOPIC="${TOPIC:-test-topic}"
GROUP="${GROUP:-dr-demo-consumer}"

KAFKA1_CONTAINER="${KAFKA1_CONTAINER:-kafka-1}"
KAFKA2_CONTAINER="${KAFKA2_CONTAINER:-kafka-2}"

KAFKA1_BOOTSTRAP="${KAFKA1_BOOTSTRAP:-kafka-1:44444}"
KAFKA2_BOOTSTRAP="${KAFKA2_BOOTSTRAP:-kafka-2:22222}"

LINK_NAME="${LINK_NAME:-source-to-destination}"


# -----------------------------------------------------------------------------
# Gateway / Compose
# -----------------------------------------------------------------------------

COMPOSE_DIR="${COMPOSE_DIR:-/Users/rjmfernandes/workspace/cpc-gateway-dr}"

COMPOSE_FILE="${COMPOSE_FILE:-$COMPOSE_DIR/gateway-compose.local.yaml}"

GATEWAY_KAFKA1_CONFIG="${GATEWAY_KAFKA1_CONFIG:-$COMPOSE_DIR/gateway-compose.before.yaml}"

COMPOSE_PROJECT="${COMPOSE_PROJECT:-cpc-gateway-dr}"

GATEWAY_CONTAINER="${GATEWAY_CONTAINER:-gateway}"
GATEWAY_SERVICE="${GATEWAY_SERVICE:-gateway}"


# -----------------------------------------------------------------------------
# Timing
# -----------------------------------------------------------------------------

POLL_SECONDS="${POLL_SECONDS:-1}"

MAX_WAIT_SECONDS="${MAX_WAIT_SECONDS:-180}"


# -----------------------------------------------------------------------------
# Colours
# -----------------------------------------------------------------------------

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'


# =============================================================================
# Helpers
# =============================================================================

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


warning() {
    printf "${YELLOW}WARN${NC}  %s\n" "$1"
}


failure() {
    printf "${RED}ERROR${NC}  %s\n" "$1"
}


# -----------------------------------------------------------------------------
# Wait for Kafka
# -----------------------------------------------------------------------------

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


# -----------------------------------------------------------------------------
# Mirror description
# -----------------------------------------------------------------------------

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


# -----------------------------------------------------------------------------
# Get committed consumer offset.
#
# Demo assumption:
#
#     test-topic
#     partition 0
#
# kafka-consumer-groups output:
#
# GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET ...
#
# Therefore:
#
#     $1 group
#     $2 topic
#     $3 partition
#     $4 current offset
# -----------------------------------------------------------------------------

get_group_offset() {

    local container="$1"
    local bootstrap="$2"

    local output
    local offset

    output="$(
        docker exec "$container" \
            kafka-consumer-groups \
            --bootstrap-server "$bootstrap" \
            --describe \
            --group "$GROUP" \
            2>/dev/null || true
    )"

    offset="$(
        printf '%s\n' "$output" |
            awk \
                -v group="$GROUP" \
                -v topic="$TOPIC" \
                '
                $1 == group &&
                $2 == topic &&
                $3 == 0 {
                    print $4
                    exit
                }
                '
    )"

    case "$offset" in

        ''|'-')
            printf 'unknown'
            ;;

        *)
            printf '%s' "$offset"
            ;;

    esac
}


# -----------------------------------------------------------------------------
# Wait for mirror ACTIVE / Lag 0
# -----------------------------------------------------------------------------

wait_for_mirror_zero_lag() {

    local container="$1"
    local bootstrap="$2"
    local label="$3"

    local elapsed=0

    local output=""
    local state=""
    local lag=""

    while [ "$elapsed" -lt "$MAX_WAIT_SECONDS" ]; do

        output="$(
            get_mirror_description \
                "$container" \
                "$bootstrap"
        )"

        state="$(
            printf '%s\n' "$output" |
                grep -Eo \
                'PENDING_SETUP_FOR_RESTORE|PENDING_RESTORE_MIRROR|PENDING_SYNCHRONIZE|ACTIVE|STOPPED|FAILED|PAUSED' |
                head -1 || true
        )"

        lag="$(
            printf '%s\n' "$output" |
                sed -n \
                's/.*Lag:[[:space:]]*\([0-9][0-9]*\).*/\1/p' |
                sort -nr |
                head -1 || true
        )"

        state="${state:-unknown}"
        lag="${lag:-unknown}"

        printf '\r  %s mirror: %-20s Lag: %-8s' \
            "$label" \
            "$state" \
            "$lag"

        if [ "$state" = "ACTIVE" ] &&
           [ "$lag" = "0" ]
        then

            printf '\n'

            return 0
        fi

        if [ "$state" = "FAILED" ]; then

            printf '\n\n'

            failure "$label mirror entered FAILED state."

            printf '\n%s\n' "$output"

            return 1
        fi

        sleep "$POLL_SECONDS"

        elapsed=$((elapsed + POLL_SECONDS))
    done

    printf '\n\n'

    failure "$label mirror did not reach ACTIVE / lag 0."

    printf '\nLast mirror description:\n\n'

    printf '%s\n' "$output"

    return 1
}


# =============================================================================
# Header
# =============================================================================

clear

printf '\n'

separator

printf '\n'

printf "${BOLD}          CPC GATEWAY PRIMARY RECOVERY / FAILBACK${NC}\n"

printf '\n'

separator

printf '\n'


printf 'Expected current state:\n\n'

printf '  Producer [RUNNING]\n'
printf '  Consumer [MAY BE RUNNING]\n'
printf '           |\n'
printf '           | localhost:19092\n'
printf '           v\n'
printf '      CPC Gateway\n'
printf '           |\n'
printf '           | kafka2-domain\n'
printf '           v\n'
printf '      Kafka 2 [ACTIVE / WRITABLE]\n'

printf '\n'

printf '      Kafka 1 [DOWN / RECOVERING]\n'

printf '\n'


printf 'Recovery objective:\n\n'

printf '  Producer             KEEP RUNNING\n'
printf '  Consumer             MAY KEEP RUNNING\n'
printf '  Bootstrap address    NEVER CHANGES\n'
printf '  Topic data           FULLY SYNCHRONIZED\n'
printf '  Consumer offset      ASYNCHRONOUSLY REPLICATED\n'
printf '  Duplicates           ACCEPTED\n'
printf '  Reversal             WAIT FOR MIRROR READY\n'

printf '\n'


printf 'Recovery sequence:\n\n'

printf '  1. Recover Kafka 1\n'
printf '  2. Restore Kafka 1 from Kafka 2\n'
printf '  3. Synchronize topic data\n'
printf '  4. Observe consumer recovery position\n'
printf '  5. Reverse Cluster Linking\n'
printf '  6. Switch CPC Gateway to Kafka 1\n'

printf '\n'

separator


# =============================================================================
# 1/6
# Recover Kafka 1
# =============================================================================

step "1/6" "RECOVERING KAFKA 1"


if docker ps \
    --format '{{.Names}}' |
    grep -Fxq "$KAFKA1_CONTAINER"
then

    success "Kafka 1 is already running."

else

    printf 'Starting Kafka 1...\n\n'

    docker start "$KAFKA1_CONTAINER"

fi


printf '\nWaiting for Kafka 1...\n\n'


if ! wait_for_kafka \
    "$KAFKA1_CONTAINER" \
    "$KAFKA1_BOOTSTRAP"
then

    failure "Kafka 1 did not become available."

    exit 1
fi


success "Kafka 1 is online."

printf '\n'

printf 'Producer traffic remains on Kafka 2.\n'
printf 'Consumer may remain running.\n'


# =============================================================================
# 2/6
# Restore Kafka 1 from Kafka 2
# =============================================================================

step "2/6" "RESTORING KAFKA 1 FROM KAFKA 2"


printf 'Recovery direction:\n\n'

printf '  Kafka 2 [SOURCE / WRITABLE]'
printf '  --------------------->  '
printf 'Kafka 1 [MIRROR]\n\n'


printf 'Running truncate-and-restore for topic %s...\n\n' \
    "$TOPIC"


set +e


RESTORE_OUTPUT="$(
    docker exec "$KAFKA1_CONTAINER" \
        kafka-mirrors \
        --bootstrap-server "$KAFKA1_BOOTSTRAP" \
        --truncate-and-restore \
        --topics "$TOPIC" \
        --link "$LINK_NAME" \
        2>&1
)"


RESTORE_RC=$?


set -e


printf '%s\n' "$RESTORE_OUTPUT"


if [ "$RESTORE_RC" -ne 0 ]; then

    failure "truncate-and-restore failed."

    printf '\n'

    printf 'Kafka 1 was NOT restored.\n'
    printf 'Failback will NOT continue.\n'

    printf '\n'

    printf 'Command output:\n\n'

    printf '%s\n' "$RESTORE_OUTPUT"

    exit "$RESTORE_RC"
fi


success "Restore operation scheduled."


# =============================================================================
# 3/6
# Synchronize topic data
# =============================================================================

step "3/6" "SYNCHRONIZING TOPIC DATA"


printf 'Producer continues running on Kafka 2.\n'
printf 'Consumer may remain running.\n\n'

printf '  Kafka 2 [SOURCE]'
printf '  --------------------->  '
printf 'Kafka 1 [MIRROR]\n\n'


printf 'Waiting for Kafka 1 ACTIVE / lag 0...\n\n'


if ! wait_for_mirror_zero_lag \
    "$KAFKA1_CONTAINER" \
    "$KAFKA1_BOOTSTRAP" \
    "Kafka 1"
then

    exit 1
fi


success "Kafka 1 topic data is synchronized."


# =============================================================================
# 4/6
# Observe consumer offsets without requiring a static source offset.
# =============================================================================

step "4/6" "CHECKING CONSUMER RECOVERY POSITION"

printf 'Consumer group: %s\n\n' "$GROUP"
printf 'The consumer may remain running and committing on Kafka 2.\n'
printf 'Offsets below are snapshots, not a final handoff position.\n\n'

KAFKA2_CAPTURED_OFFSET="$(
    get_group_offset "$KAFKA2_CONTAINER" "$KAFKA2_BOOTSTRAP"
)"
KAFKA1_OBSERVED_OFFSET="$(
    get_group_offset "$KAFKA1_CONTAINER" "$KAFKA1_BOOTSTRAP"
)"

printf '  Kafka 2 committed offset : %s\n' "$KAFKA2_CAPTURED_OFFSET"
printf '  Kafka 1 mirrored offset  : %s\n\n' "$KAFKA1_OBSERVED_OFFSET"

warning "Failback permits consumer replay; exact offset equality is not required."

if [ "$KAFKA1_OBSERVED_OFFSET" = "unknown" ]; then
    warning "Kafka 1 has no visible committed offset; the consumer reset policy will apply if none is available at reconnect."
fi

# =============================================================================
# 5/6
# Reverse Cluster Linking
# =============================================================================

step "5/6" "REVERSING CLUSTER LINKING"

printf 'Performing FINAL topic synchronization check...\n\n'

if ! wait_for_mirror_zero_lag \
    "$KAFKA1_CONTAINER" \
    "$KAFKA1_BOOTSTRAP" \
    "Kafka 1"
then
    exit 1
fi

success "Topic is synchronized immediately before reversal."

printf '\n'

printf 'Before:\n\n'

printf '  Kafka 2 [SOURCE / WRITABLE]'
printf '  --------------------->  '
printf 'Kafka 1 [MIRROR]\n\n'


printf 'Running reverse-and-start for Cluster Link %s...\n\n' \
    "$LINK_NAME"


# IMPORTANT:
#
# In the CLI installed in this environment:
#
#     --topics and --link cannot be supplied together for
#     reverse-and-start.
#
# Therefore this operation is performed on the link.
#
# --force suppresses the confirmation prompt.


set +e


REVERSE_OUTPUT="$(
    docker exec "$KAFKA1_CONTAINER" \
        kafka-mirrors \
        --bootstrap-server "$KAFKA1_BOOTSTRAP" \
        --reverse-and-start \
        --link "$LINK_NAME" \
        --force \
        2>&1
)"


REVERSE_RC=$?


set -e


printf '%s\n' "$REVERSE_OUTPUT"


if [ "$REVERSE_RC" -ne 0 ]; then

    failure "reverse-and-start failed."

    printf '\n'

    printf 'Gateway remains on Kafka 2.\n'

    exit "$REVERSE_RC"
fi


success "Reverse-and-start accepted."


printf '\n'

printf 'Waiting for Kafka 2 mirror ACTIVE / lag 0...\n\n'


if ! wait_for_mirror_zero_lag \
    "$KAFKA2_CONTAINER" \
    "$KAFKA2_BOOTSTRAP" \
    "Kafka 2"
then

    failure "Kafka 2 did not become an ACTIVE synchronized mirror."

    printf '\n'

    printf 'Gateway has NOT been switched.\n'

    exit 1
fi


success "Kafka 2 is now the synchronized mirror."


printf '\n'

printf 'After reversal:\n\n'

printf '  Kafka 1 [SOURCE / WRITABLE]'
printf '  --------------------->  '
printf 'Kafka 2 [MIRROR / DR]\n'


# =============================================================================
# 6/6
# Switch CPC Gateway
# =============================================================================

step "6/6" "SWITCHING CPC GATEWAY TO KAFKA 1"


printf 'Current gateway route:\n\n'

printf '  switchover-route -> kafka2-domain\n\n'


printf 'Target gateway route:\n\n'

printf '  switchover-route -> kafka1-domain\n\n'


# -----------------------------------------------------------------------------
# Verify known-good Kafka 1 Gateway config
# -----------------------------------------------------------------------------

if [ ! -f "$GATEWAY_KAFKA1_CONFIG" ]; then

    failure "Required Gateway configuration does not exist."

    printf '\n'

    printf 'Expected:\n\n'

    printf '  %s\n' "$GATEWAY_KAFKA1_CONFIG"

    exit 1
fi


if ! grep -A10 \
        'name: switchover-route' \
        "$GATEWAY_KAFKA1_CONFIG" |
        grep -q 'name: kafka1-domain'
then

    failure "gateway-compose.before.yaml does not route to kafka1-domain."

    exit 1
fi


if ! grep -A10 \
        'name: switchover-route' \
        "$GATEWAY_KAFKA1_CONFIG" |
        grep -q \
        'bootstrapServerId: internal-kafka1-listener'
then

    failure "gateway-compose.before.yaml does not use the Kafka 1 listener."

    exit 1
fi


success "Kafka 1 Gateway configuration verified."


# -----------------------------------------------------------------------------
# Install Kafka 1 Gateway configuration
# -----------------------------------------------------------------------------

printf '\n'

printf 'Installing Kafka 1 Gateway configuration...\n\n'


cp \
    "$GATEWAY_KAFKA1_CONFIG" \
    "$COMPOSE_FILE"


success "gateway-compose.local.yaml now targets Kafka 1."


# -----------------------------------------------------------------------------
# Recreate ONLY Gateway
# -----------------------------------------------------------------------------

printf '\n'

printf 'Recreating ONLY the Gateway container...\n'

printf 'Producer remains running.\n'
printf 'Consumer may remain running.\n'

printf 'bootstrap.servers remains localhost:19092.\n\n'


set +e


docker compose \
    -f "$COMPOSE_FILE" \
    -p "$COMPOSE_PROJECT" \
    up \
    -d \
    --no-deps \
    --force-recreate \
    "$GATEWAY_SERVICE"


GATEWAY_RECREATE_RC=$?


set -e


if [ "$GATEWAY_RECREATE_RC" -ne 0 ]; then

    failure "Could not recreate Gateway."

    printf '\n'

    printf 'Kafka 1 is SOURCE.\n'
    printf 'Kafka 2 is MIRROR.\n'
    printf 'Gateway cutover was NOT confirmed.\n'

    exit "$GATEWAY_RECREATE_RC"
fi


# -----------------------------------------------------------------------------
# Wait for Gateway container
# -----------------------------------------------------------------------------

printf '\n'

printf 'Waiting for Gateway container...\n\n'


elapsed=0

GATEWAY_READY=false


while [ "$elapsed" -lt "$MAX_WAIT_SECONDS" ]; do

    if docker ps \
        --filter "name=^/${GATEWAY_CONTAINER}$" \
        --format '{{.Names}}' |
        grep -Fxq "$GATEWAY_CONTAINER"
    then

        GATEWAY_READY=true

        break
    fi

    sleep "$POLL_SECONDS"

    elapsed=$((elapsed + POLL_SECONDS))
done


if [ "$GATEWAY_READY" != true ]; then

    failure "Gateway container did not return."

    exit 1
fi


success "Gateway container is running."


# -----------------------------------------------------------------------------
# Verify ACTUAL running Gateway configuration
# -----------------------------------------------------------------------------

printf '\n'

printf 'Verifying actual running Gateway configuration...\n\n'


RUNNING_GATEWAY_CONFIG="$(
    docker inspect "$GATEWAY_CONTAINER" \
        --format \
        '{{range .Config.Env}}{{println .}}{{end}}'
)"


ROUTE_CONFIG="$(
    printf '%s\n' "$RUNNING_GATEWAY_CONFIG" |
        grep -A10 \
        'name: switchover-route' || true
)"


printf '%s\n' "$ROUTE_CONFIG"

printf '\n'


if ! printf '%s\n' "$ROUTE_CONFIG" |
    grep -q 'name: kafka1-domain'
then

    failure "Running Gateway is NOT pointing to kafka1-domain."

    exit 1
fi


if ! printf '%s\n' "$ROUTE_CONFIG" |
    grep -q \
    'bootstrapServerId: internal-kafka1-listener'
then

    failure "Running Gateway is NOT using internal-kafka1-listener."

    exit 1
fi


success "Gateway is routing clients to Kafka 1."


# =============================================================================
# Final DR mirror verification
# =============================================================================

printf '\n'

printf 'Verifying Kafka 2 DR mirror...\n\n'


if ! wait_for_mirror_zero_lag \
    "$KAFKA2_CONTAINER" \
    "$KAFKA2_BOOTSTRAP" \
    "Kafka 2"
then

    warning "Gateway cutover succeeded, but Kafka 2 mirror is not currently at lag 0."

else

    success "Kafka 2 DR mirror is ACTIVE with lag 0."

fi


# =============================================================================
# Complete
# =============================================================================

printf '\n'

separator

printf '\n'

printf "${GREEN}${BOLD}                FAILBACK COMPLETE${NC}\n"

printf '\n'

separator

printf '\n'


printf 'Final topology:\n\n'

printf '  Producer [RUNNING]\n'
printf '  Consumer [MAY BE RUNNING]\n'
printf '           |\n'
printf '           | localhost:19092\n'
printf '           v\n'
printf '      CPC Gateway\n'
printf '           |\n'
printf '           | kafka1-domain\n'
printf '           v\n'
printf '      Kafka 1 [PRIMARY / WRITABLE]\n'
printf '           |\n'
printf '           | Cluster Linking\n'
printf '           v\n'
printf '      Kafka 2 [DR MIRROR / ACTIVE]\n'

printf '\n'


printf 'Producer                   : still running\n'
printf 'Consumer                   : may remain running\n'
printf 'bootstrap.servers          : localhost:19092\n'
printf 'Primary                    : Kafka 1\n'
printf 'DR                         : Kafka 2\n'
printf 'Kafka 2 offset snapshot    : %s\n' \
    "$KAFKA2_CAPTURED_OFFSET"

printf 'Kafka 1 current offset     : %s\n' \
    "$(get_group_offset "$KAFKA1_CONTAINER" "$KAFKA1_BOOTSTRAP")"


printf '\n'

printf 'Running clients can reconnect through the unchanged bootstrap address.\n'
printf 'Consumer replay is possible after failback.\n'

printf '\n'

separator

printf '\n'
