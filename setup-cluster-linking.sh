#!/usr/bin/env bash
set -euo pipefail

TOPIC="${TOPIC:-test-topic}"
LINK="${LINK:-source-to-destination}"
SOURCE_CONTAINER="${SOURCE_CONTAINER:-kafka-1}"
DESTINATION_CONTAINER="${DESTINATION_CONTAINER:-kafka-2}"
SOURCE_BOOTSTRAP="${SOURCE_BOOTSTRAP:-kafka-1:44444}"
DESTINATION_BOOTSTRAP="${DESTINATION_BOOTSTRAP:-kafka-2:22222}"
LINK_CONFIG="${LINK_CONFIG:-cluster-linking/source-to-destination.properties}"
GROUP_FILTERS_CONFIG="${GROUP_FILTERS_CONFIG:-cluster-linking/consumer-offset-group-filters.json}"
REMOTE_LINK_CONFIG="/tmp/source-to-destination.properties"
REMOTE_GROUP_FILTERS_CONFIG="/tmp/consumer-offset-group-filters.json"

require_container() {
  local container="$1"
  if ! docker inspect "$container" >/dev/null 2>&1; then
    echo "Container '$container' does not exist. Run ./start-kafka.sh first."
    exit 1
  fi
}

require_running_container() {
  local container="$1"
  local state
  state="$(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null || true)"
  if [ "$state" != "true" ]; then
    echo "Container '$container' is not running. Run ./start-kafka.sh first."
    exit 1
  fi
}

require_container "$SOURCE_CONTAINER"
require_container "$DESTINATION_CONTAINER"
require_running_container "$SOURCE_CONTAINER"
require_running_container "$DESTINATION_CONTAINER"

echo "Creating source topic '$TOPIC' on kafka-1 if needed..."
docker exec "$SOURCE_CONTAINER" kafka-topics \
  --bootstrap-server "$SOURCE_BOOTSTRAP" \
  --create \
  --if-not-exists \
  --topic "$TOPIC" \
  --partitions 1 \
  --replication-factor 1

echo "Copying Cluster Link config into kafka-2..."
docker cp "$LINK_CONFIG" "$DESTINATION_CONTAINER:$REMOTE_LINK_CONFIG"
docker cp "$GROUP_FILTERS_CONFIG" "$DESTINATION_CONTAINER:$REMOTE_GROUP_FILTERS_CONFIG"

SOURCE_CLUSTER_ID="$(docker exec "$SOURCE_CONTAINER" kafka-cluster cluster-id --bootstrap-server "$SOURCE_BOOTSTRAP" | tr -d '\r' | awk 'NF {value=$NF} END {print value}')"
if [ -z "$SOURCE_CLUSTER_ID" ]; then
  echo "Could not determine source cluster ID from '$SOURCE_CONTAINER'."
  exit 1
fi

if docker exec "$DESTINATION_CONTAINER" kafka-cluster-links --bootstrap-server "$DESTINATION_BOOTSTRAP" --list | grep -q "Link name: '$LINK'"; then
  echo "Cluster Link '$LINK' already exists on kafka-2."
else
  echo "Creating Cluster Link '$LINK' on kafka-2 from source cluster '$SOURCE_CLUSTER_ID'..."
  docker exec "$DESTINATION_CONTAINER" kafka-cluster-links \
    --bootstrap-server "$DESTINATION_BOOTSTRAP" \
    --create \
    --link "$LINK" \
    --cluster-id "$SOURCE_CLUSTER_ID" \
    --config-file "$REMOTE_LINK_CONFIG" \
    --consumer-group-filters-json-file "$REMOTE_GROUP_FILTERS_CONFIG"
fi

if docker exec "$DESTINATION_CONTAINER" kafka-mirrors --bootstrap-server "$DESTINATION_BOOTSTRAP" --list --link "$LINK" | grep -qx "$TOPIC"; then
  echo "Mirror topic '$TOPIC' already exists on kafka-2."
else
  echo "Creating mirror topic '$TOPIC' on kafka-2..."
  docker exec "$DESTINATION_CONTAINER" kafka-mirrors \
    --bootstrap-server "$DESTINATION_BOOTSTRAP" \
    --create \
    --mirror-topic "$TOPIC" \
    --link "$LINK"
fi

echo "Cluster Linking is ready: kafka-1 '$TOPIC' -> kafka-2 mirror topic '$TOPIC'."
