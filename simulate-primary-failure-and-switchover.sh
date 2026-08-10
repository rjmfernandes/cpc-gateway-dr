#!/usr/bin/env bash
set -euo pipefail

TOPIC="${TOPIC:-test-topic}"
DESTINATION_CONTAINER="${DESTINATION_CONTAINER:-kafka-2}"
DESTINATION_BOOTSTRAP="${DESTINATION_BOOTSTRAP:-kafka-2:22222}"
SYNC_WAIT_SECONDS="${SYNC_WAIT_SECONDS:-10}"

if ! docker inspect "$DESTINATION_CONTAINER" >/dev/null 2>&1; then
  echo "Container '$DESTINATION_CONTAINER' does not exist. Run ./start-kafka.sh and ./setup-cluster-linking.sh first."
  exit 1
fi

echo "Waiting ${SYNC_WAIT_SECONDS}s for mirror data and consumer offsets to settle..."
sleep "$SYNC_WAIT_SECONDS"

echo "Simulating primary cluster failure by stopping kafka-1..."
docker compose -f kafka-compose.yaml stop kafka-1

echo "Failing over mirror topic '$TOPIC' on kafka-2..."
docker exec "$DESTINATION_CONTAINER" kafka-mirrors \
  --bootstrap-server "$DESTINATION_BOOTSTRAP" \
  --failover \
  --topics "$TOPIC"

echo "Pointing Gateway switchover-route to kafka-2..."
cp gateway-compose.after.yaml gateway-compose.local.yaml

echo "Restarting Gateway..."
sh ./start-gateway.sh

echo "DR switchover complete. Restart the same clients against localhost:19092."
