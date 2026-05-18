#!/usr/bin/env bash
# T1 + T2 — Schema apply (idempotency) smoke test.
#
# Spins up a throwaway clickhouse/clickhouse-server container, applies the
# DDL twice, asserts:
#   - first apply exits 0
#   - SHOW TABLES FROM onlinejudge lists the four expected objects
#   - second apply also exits 0 (idempotent — every object is IF NOT EXISTS)
#
# Does NOT need Kafka — the Kafka Engine table is created but won't be queried.
# Test T3 (compose stand-up) runs against the full region.yml stack.
set -euo pipefail

CT_NAME="oj-clickhouse-test-$$"
PROTO_DIR="$(cd "$(dirname "$0")/../../../common/src/main/proto" && pwd)"
SCHEMA_FILE="$(cd "$(dirname "$0")/../.." && pwd)/src/main/resources/schema/submission_analytics.sql"
SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

cleanup() { docker rm -f "$CT_NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo "[T1] starting throwaway ClickHouse: $CT_NAME"
docker run -d --rm --name "$CT_NAME" \
    -v "${PROTO_DIR}:/var/lib/clickhouse/format_schemas/proto:ro" \
    clickhouse/clickhouse-server:24.8 >/dev/null

# Wait for HTTP /ping to come up.
for _ in $(seq 1 30); do
    if docker exec "$CT_NAME" wget -q -O- http://localhost:8123/ping >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

apply() {
    SCHEMA_FILE="$SCHEMA_FILE" \
    CH_HOST=localhost \
    CH_PORT=9000 \
    KAFKA_BROKER_LIST="kafka:29092" \
    KAFKA_ANALYTICS_TOPIC="analytics_events" \
    KAFKA_GROUP_NAME="analytics-clickhouse" \
    KAFKA_CONSUMER_OFFSET="latest" \
    bash "$SCRIPT_DIR/apply-ddl.sh" \
        --net=container:"$CT_NAME"
}

# clickhouse-client needs to run *inside* the container so it can reach the
# server on localhost. Cheaper than installing the client on the host.
docker cp "$SCHEMA_FILE" "$CT_NAME":/tmp/submission_analytics.sql
RENDERED=$(mktemp)
sed \
    -e "s|__KAFKA_BROKER_LIST__|kafka:29092|g" \
    -e "s|__KAFKA_TOPIC__|analytics_events|g" \
    -e "s|__KAFKA_GROUP_NAME__|analytics-clickhouse|g" \
    -e "s|__KAFKA_CONSUMER_OFFSET__|latest|g" \
    "$SCHEMA_FILE" > "$RENDERED"
docker cp "$RENDERED" "$CT_NAME":/tmp/rendered.sql

echo "[T1] applying DDL (first run)"
docker exec "$CT_NAME" bash -c "clickhouse-client --multiquery < /tmp/rendered.sql"

echo "[T1] verifying objects exist"
tables=$(docker exec "$CT_NAME" clickhouse-client --query "SHOW TABLES FROM onlinejudge FORMAT TSV")
echo "$tables"
for t in analytics_errors_mv analytics_kafka analytics_kafka_errors analytics_mv submission_analytics; do
    if ! echo "$tables" | grep -qx "$t"; then
        echo "[T1] FAIL — missing $t" >&2
        exit 1
    fi
done

echo "[T2] applying DDL second time (idempotency)"
docker exec "$CT_NAME" bash -c "clickhouse-client --multiquery < /tmp/rendered.sql"

echo "[T1+T2] PASS"
