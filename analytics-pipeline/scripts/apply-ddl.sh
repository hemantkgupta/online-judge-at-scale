#!/usr/bin/env bash
# Apply the Kafka-Engine analytics DDL to a ClickHouse instance.
#
#   ./apply-ddl.sh
#
# Variables (with sensible defaults for the per-region GCP compose stack):
#   CH_HOST                  default: oj-clickhouse
#   CH_PORT                  default: 9000          (native client port)
#   CH_USER                  default: default
#   CH_PASSWORD              default: ""
#   KAFKA_BROKER_LIST        default: oj-kafka:29092
#   KAFKA_ANALYTICS_TOPIC    default: analytics_events
#   KAFKA_GROUP_NAME         default: analytics-clickhouse
#   KAFKA_CONSUMER_OFFSET    default: latest        ("earliest" for backfill,
#                                                    see design doc §3.4)
#
# Idempotent — every object is created with IF NOT EXISTS.
set -euo pipefail

: "${CH_HOST:=oj-clickhouse}"
: "${CH_PORT:=9000}"
: "${CH_USER:=default}"
: "${CH_PASSWORD:=}"
: "${KAFKA_BROKER_LIST:=oj-kafka:29092}"
: "${KAFKA_ANALYTICS_TOPIC:=analytics_events}"
: "${KAFKA_GROUP_NAME:=analytics-clickhouse}"
: "${KAFKA_CONSUMER_OFFSET:=latest}"

if [[ -z "${SCHEMA_FILE:-}" ]]; then
    # Default to the in-repo location (script lives in analytics-pipeline/scripts/).
    SCHEMA_FILE="$(cd "$(dirname "$0")" && pwd)/../src/main/resources/schema/submission_analytics.sql"
fi
if [[ ! -f "$SCHEMA_FILE" ]]; then
    echo "[apply-ddl] schema not found: $SCHEMA_FILE" >&2
    exit 1
fi

echo "[apply-ddl] host=$CH_HOST port=$CH_PORT topic=$KAFKA_ANALYTICS_TOPIC group=$KAFKA_GROUP_NAME offset=$KAFKA_CONSUMER_OFFSET"

rendered="$(mktemp)"
trap 'rm -f "$rendered"' EXIT
sed \
    -e "s|__KAFKA_BROKER_LIST__|${KAFKA_BROKER_LIST}|g" \
    -e "s|__KAFKA_TOPIC__|${KAFKA_ANALYTICS_TOPIC}|g" \
    -e "s|__KAFKA_GROUP_NAME__|${KAFKA_GROUP_NAME}|g" \
    -e "s|__KAFKA_CONSUMER_OFFSET__|${KAFKA_CONSUMER_OFFSET}|g" \
    "$SCHEMA_FILE" > "$rendered"

clickhouse-client \
    --host="$CH_HOST" \
    --port="$CH_PORT" \
    --user="$CH_USER" \
    --password="$CH_PASSWORD" \
    --multiquery < "$rendered"

echo "[apply-ddl] done"
