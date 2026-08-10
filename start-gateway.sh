#!/usr/bin/env bash

export GATEWAY_IMAGE="confluentinc/cpc-gateway:latest"
GATEWAY_COMPOSE_FILE="${GATEWAY_COMPOSE_FILE:-gateway-compose.local.yaml}"
GATEWAY_TEMPLATE_FILE="${GATEWAY_TEMPLATE_FILE:-gateway-compose.before.yaml}"

if [ ! -f "$GATEWAY_COMPOSE_FILE" ]; then
  cp "$GATEWAY_TEMPLATE_FILE" "$GATEWAY_COMPOSE_FILE"
  echo "Initialized $GATEWAY_COMPOSE_FILE from $GATEWAY_TEMPLATE_FILE"
fi

echo "Starting Gateway container..."
docker compose -f "$GATEWAY_COMPOSE_FILE" down -v || true
docker compose -f "$GATEWAY_COMPOSE_FILE" up -d

echo "Gateway container started."
