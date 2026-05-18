#!/usr/bin/env bash
# T4 — Protobuf parse happy path.
#
# Produces a single AnalyticsEvent into Kafka, waits ≤ 5 s, asserts the row
# arrived in onlinejudge.submission_analytics.
#
# Requires the full compose stack (region.yml) already up. Assumes the
# canonical proto file is mounted into the ClickHouse container at
# /var/lib/clickhouse/format_schemas/proto/events.proto.
set -euo pipefail

: "${REGION:?must set REGION (e.g. asia-south1)}"
TOPIC="analytics_events.${REGION}"
KAFKA_CT="oj-kafka"
CH_CT="oj-clickhouse"
SUB_ID="smoke-$(date +%s)"

# Hand-rolled protobuf encoding for AnalyticsEvent — tags 1–11.
# Avoids pulling in protoc / a JVM producer just for one event. Field
# encoding: <field<<3 | wire_type> <varint or length-delimited payload>.
python3 - "$SUB_ID" <<'PY' > /tmp/event.bin
import struct, sys

def varint(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)

def str_field(tag, val):
    b = val.encode()
    return bytes([(tag << 3) | 2]) + varint(len(b)) + b

def int_field(tag, val):
    return bytes([(tag << 3) | 0]) + varint(val)

sub_id = sys.argv[1]
sys.stdout.buffer.write(
    str_field(1,  sub_id)
  + str_field(2,  "user-smoke")
  + str_field(3,  "prob-smoke")
  + str_field(4,  "contest-smoke")
  + str_field(5,  "python")
  + str_field(6,  "ACCEPTED")
  + int_field(7,  42)
  + int_field(8,  16)
  + int_field(9,  1_700_000_000_000)
  + str_field(10, "asia-south1")
  + str_field(11, "pretest")
)
PY

echo "[T4] produced AnalyticsEvent submission_id=$SUB_ID size=$(wc -c < /tmp/event.bin) bytes"

docker cp /tmp/event.bin "$KAFKA_CT":/tmp/event.bin
docker exec -i "$KAFKA_CT" bash -c \
    "kafka-console-producer --bootstrap-server localhost:29092 --topic '$TOPIC' < /tmp/event.bin"

echo "[T4] waiting up to 10 s for the row to land"
for _ in $(seq 1 10); do
    n=$(docker exec "$CH_CT" clickhouse-client --query \
        "SELECT count() FROM onlinejudge.submission_analytics WHERE submission_id = '$SUB_ID'")
    if [[ "$n" -ge 1 ]]; then
        echo "[T4] PASS — row arrived ($n)"
        docker exec "$CH_CT" clickhouse-client --query \
            "SELECT * FROM onlinejudge.submission_analytics WHERE submission_id = '$SUB_ID' FORMAT Vertical"
        exit 0
    fi
    sleep 1
done

echo "[T4] FAIL — row never arrived" >&2
docker exec "$CH_CT" clickhouse-client --query \
    "SELECT * FROM onlinejudge.analytics_kafka_errors ORDER BY error_time DESC LIMIT 5 FORMAT Vertical" || true
exit 1
