#!/usr/bin/env bash

set -euo pipefail


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TOPIC="${TOPIC:-test-topic}"

SOURCE_CONTAINER="${SOURCE_CONTAINER:-kafka-1}"

DESTINATION_CONTAINER="${DESTINATION_CONTAINER:-kafka-2}"

DESTINATION_BOOTSTRAP="${DESTINATION_BOOTSTRAP:-kafka-2:22222}"

CLIENT_BOOTSTRAP="${CLIENT_BOOTSTRAP:-localhost:19092}"


# ---------------------------------------------------------------------------
# Terminal colors
# ---------------------------------------------------------------------------

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m'


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

separator() {

    printf '%s\n' \
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

}


step() {

    local number="$1"
    local description="$2"

    printf '\n'
    printf "${CYAN}${BOLD}[%s] %s${NC}\n" \
        "$number" \
        "$description"
    printf '\n'

}


success() {

    printf "${GREEN}✓${NC} %s\n" "$1"

}


failure() {

    printf "${RED}✗${NC} %s\n" "$1"

}


info() {

    printf "${DIM}%s${NC}\n" "$1"

}


container_running() {

    local container="$1"

    local state

    state="$(
        docker inspect \
            -f '{{.State.Running}}' \
            "$container" \
            2>/dev/null \
            || true
    )"

    [ "$state" = "true" ]

}


show_cluster_status() {

    printf '\n'

    if container_running "$SOURCE_CONTAINER"; then

        printf "  Kafka 1    ${GREEN}● ONLINE${NC}\n"

    else

        printf "  Kafka 1    ${RED}● FAILED${NC}\n"

    fi


    if container_running "$DESTINATION_CONTAINER"; then

        printf "  Kafka 2    ${GREEN}● ONLINE${NC}\n"

    else

        printf "  Kafka 2    ${RED}● DOWN${NC}\n"

    fi

    printf '\n'

}


# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------

if ! docker inspect "$SOURCE_CONTAINER" >/dev/null 2>&1; then

    failure "Container '$SOURCE_CONTAINER' does not exist."

    printf '\n'
    printf 'Run:\n\n'
    printf '  ./start-kafka.sh\n'
    printf '  ./setup-cluster-linking.sh\n\n'

    exit 1

fi


if ! docker inspect "$DESTINATION_CONTAINER" >/dev/null 2>&1; then

    failure "Container '$DESTINATION_CONTAINER' does not exist."

    printf '\n'
    printf 'Run:\n\n'
    printf '  ./start-kafka.sh\n'
    printf '  ./setup-cluster-linking.sh\n\n'

    exit 1

fi


if ! container_running "$SOURCE_CONTAINER"; then

    failure "Kafka 1 is already stopped."

    printf '\n'
    printf 'The primary cluster must be running before starting the DR demo.\n\n'

    exit 1

fi


if ! container_running "$DESTINATION_CONTAINER"; then

    failure "Kafka 2 is not running."

    printf '\n'
    printf 'The DR cluster must be available before starting the switchover.\n\n'

    exit 1

fi


# ---------------------------------------------------------------------------
# Demo introduction
# ---------------------------------------------------------------------------

clear

printf '\n'
separator
printf '\n'
printf "${BOLD}             CPC GATEWAY DISASTER RECOVERY${NC}\n"
printf '\n'
separator

printf '\n'
printf 'Client bootstrap address:\n'
printf '\n'
printf "             ${GREEN}${BOLD}%s${NC}\n" \
    "$CLIENT_BOOTSTRAP"
printf '\n'
printf "       ${GREEN}UNCHANGED DURING SWITCHOVER${NC}\n"
printf '\n'

separator

printf '\n'
printf 'Current traffic path:\n'
printf '\n'

printf "  Client ${DIM}(%s)${NC}\n" \
    "$CLIENT_BOOTSTRAP"

printf '       |\n'
printf '       v\n'
printf '  CPC Gateway\n'
printf '       |\n'
printf "       | ${BLUE}kafka1-domain${NC}\n"
printf '       v\n'
printf "  Kafka 1 ${GREEN}[PRIMARY]${NC}\n"
printf '\n'
printf "       ${GREEN}Cluster Linking${NC}\n"
printf '              |\n'
printf '              v\n'
printf "           Kafka 2 ${DIM}[DR]${NC}\n"

show_cluster_status


# ---------------------------------------------------------------------------
# Step 1 - Simulate unexpected Kafka 1 failure
# ---------------------------------------------------------------------------

step "1/3" "SIMULATING UNPLANNED PRIMARY CLUSTER FAILURE"

printf "${YELLOW}Kafka 1 is being stopped immediately.${NC}\n"
printf '\n'

info "No replication wait is performed: this simulates an unexpected failure."

printf '\n'

docker compose \
    -f kafka-compose.yaml \
    stop kafka-1

printf '\n'

success "Kafka 1 has been stopped."

show_cluster_status

printf "${RED}${BOLD}PRIMARY CLUSTER IS NOW UNAVAILABLE${NC}\n"


# ---------------------------------------------------------------------------
# Step 2 - Fail over mirror topic
# ---------------------------------------------------------------------------

step "2/3" "FAILING OVER MIRROR TOPIC"

printf 'Destination cluster:\n\n'
printf "  ${GREEN}Kafka 2${NC}\n"

printf '\n'
printf 'Mirror topic:\n\n'
printf "  ${BOLD}%s${NC}\n" "$TOPIC"

printf '\n'

info "Making the existing mirror topic writable on Kafka 2..."

printf '\n'

docker exec "$DESTINATION_CONTAINER" \
    kafka-mirrors \
    --bootstrap-server "$DESTINATION_BOOTSTRAP" \
    --failover \
    --topics "$TOPIC"

printf '\n'

success "Mirror topic '$TOPIC' failed over successfully."
success "Kafka 2 can now serve the topic."


# ---------------------------------------------------------------------------
# Step 3 - Switch CPC Gateway
# ---------------------------------------------------------------------------

step "3/3" "SWITCHING CPC GATEWAY ROUTE"

printf 'Gateway route:\n'
printf '\n'

printf "  ${RED}kafka1-domain${NC}\n"
printf '         |\n'
printf '         v\n'
printf "  ${GREEN}${BOLD}kafka2-domain${NC}\n"

printf '\n'
printf 'Applying DR Gateway configuration...\n'

cp \
    gateway-compose.after.yaml \
    gateway-compose.local.yaml

printf '\n'

success "Gateway configuration updated."

printf '\n'
printf 'Restarting CPC Gateway...\n'
printf '\n'

sh ./start-gateway.sh

printf '\n'

success "CPC Gateway restarted."
success "Gateway now routes clients to Kafka 2."


# ---------------------------------------------------------------------------
# Final state
# ---------------------------------------------------------------------------

printf '\n'
separator
printf '\n'
printf "${GREEN}${BOLD}                  DR SWITCHOVER COMPLETE${NC}\n"
printf '\n'
separator

printf '\n'
printf 'New traffic path:\n'
printf '\n'

printf "  Client ${DIM}(%s)${NC}\n" \
    "$CLIENT_BOOTSTRAP"

printf '       |\n'
printf '       v\n'
printf '  CPC Gateway\n'
printf '       |\n'
printf "       | ${GREEN}${BOLD}kafka2-domain${NC}\n"
printf '       v\n'
printf "  Kafka 2 ${GREEN}${BOLD}[ACTIVE]${NC}\n"

printf '\n'

separator

printf '\n'
printf "Client bootstrap before:  ${BOLD}%s${NC}\n" \
    "$CLIENT_BOOTSTRAP"

printf "Client bootstrap after:   ${BOLD}%s${NC}\n" \
    "$CLIENT_BOOTSTRAP"

printf '\n'
printf "Client configuration:     ${GREEN}${BOLD}UNCHANGED${NC}\n"
printf '\n'

separator

printf '\n'
printf 'Restart the same producer and consumer commands.\n'
printf '\n'
printf 'The clients continue using:\n'
printf '\n'

printf "  ${GREEN}${BOLD}bootstrap.servers=%s${NC}\n" \
    "$CLIENT_BOOTSTRAP"

printf '\n'