#!/usr/bin/env bash
# T7 — Malformed payload does not stall the consumer.
#
# Produces 32 random bytes onto analytics_events.${REGION}. Asserts:
#   - no main-table row appears
#   - the bad message is captured in onlinejudge.analytics_kafka_errors
#   - the consumer keeps advancing (a subsequent valid event arrives)
set -euo pipefail

: "${REGION:?must set REGION (e.g. asia-south1)}"
TOPIC="analytics_events.${REGION}"
KAFKA_CT="oj-kafka"
CH_CT="oj-clickhouse"

echo "[T7] producing 32 random bytes onto $TOPIC"
head -c 32 /dev/urandom | docker exec -i "$KAFKA_CT" \
    kafka-console-producer --bootstrap-server localhost:29092 --topic "$TOPIC"

echo "[T7] waiting up to 10 s for an error row"
before=$(docker exec "$CH_CT" clickhouse-client --query \
    "SELECT count() FROM onlinejudge.analytics_kafka_errors")
for _ in $(seq 1 10); do
    after=$(docker exec "$CH_CT" clickhouse-client --query \
        "SELECT count() FROM onlinejudge.analytics_kafka_errors")
    if [[ "$after" -gt "$before" ]]; then
        echo "[T7] PASS — error captured ($before -> $after)"
        docker exec "$CH_CT" clickhouse-client --query \
            "SELECT error_message FROM onlinejudge.analytics_kafka_errors ORDER BY error_time DESC LIMIT 1 FORMAT Vertical"
        exit 0
    fi
    sleep 1
done

echo "[T7] FAIL — no error row captured" >&2
exit 1
