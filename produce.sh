#!/usr/bin/env bash

set -u

BROKER="${BROKER:-localhost:19092}"
TOPIC="${TOPIC:-test-topic}"

counter=1

while true
do

    printf -v id "%06d" "$counter"

    timestamp=$(date '+%H:%M:%S')
    message="MSG-${id} | ${timestamp}"

    echo "Producing: $message"

    # Temporary file is needed because kafka-console-producer can return
    # exit code 0 even when an asynchronous producer callback reports that
    # the record was NOT successfully produced.
    error_file="$(mktemp)"

    echo "$message" |
        kafka-console-producer \
            --bootstrap-server "$BROKER" \
            --topic "$TOPIC" \
            --producer-property acks=all \
            >/dev/null \
            2>"$error_file"

    producer_rc=$?

    # Preserve Kafka's diagnostics on the terminal.
    if [ -s "$error_file" ]; then
        cat "$error_file" >&2
    fi

    # kafka-console-producer may return 0 despite an asynchronous send error.
    #
    # Therefore the message is considered failed if:
    #
    #   1. kafka-console-producer returned non-zero
    #      OR
    #   2. stderr contains an ERROR from the producer
    #
    # In either case the message ID is NOT advanced.

    if [ "$producer_rc" -ne 0 ] ||
       grep -qE '(^|[[:space:]])ERROR([[:space:]]|$)' "$error_file"
    then

        echo "FAILED: MSG-${id} was not confirmed."
        echo "Retrying SAME message ID..."

        rm -f "$error_file"

        sleep 1

        continue
    fi

    rm -f "$error_file"

    echo "OK: MSG-${id}"

    # ONLY advance after a send without a reported producer error.
    counter=$((counter + 1))

    sleep 1

done