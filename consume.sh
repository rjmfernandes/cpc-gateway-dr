#!/usr/bin/env bash

set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-localhost:19092}"
TOPIC="${TOPIC:-test-topic}"
GROUP="${GROUP:-dr-demo-consumer}"

echo
echo "Consuming through CPC Gateway"
echo "bootstrap.servers = $BOOTSTRAP"
echo "consumer.group    = $GROUP"
echo
echo "────────────────────────────────────────"

kafka-console-consumer \
    --bootstrap-server "$BOOTSTRAP" \
    --topic "$TOPIC" \
    --group "$GROUP" \
    --command-property auto.offset.reset=earliest \
    --command-property enable.auto.commit=true \
    --command-property auto.commit.interval.ms=100