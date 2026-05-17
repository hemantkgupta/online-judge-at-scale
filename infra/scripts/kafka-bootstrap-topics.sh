#!/usr/bin/env bash
# =============================================================================
# Kafka topic bootstrap (roadmap §2.7 stepping-stone).
#
# Now that KAFKA_AUTO_CREATE_TOPICS_ENABLE=false on the broker, every topic
# must be created explicitly with the right partition / RF / retention.
# This script is idempotent — `kafka-topics --create --if-not-exists` is a
# no-op when the topic already exists.
#
# Run on oj-control-plane (where the broker container lives):
#   sudo bash infra/scripts/kafka-bootstrap-topics.sh
#
# Under the eventual 3-broker move, the RF on every line below changes
# 1 → 3 and min.insync.replicas → 2. The partitions counts stay the same.
# =============================================================================

set -euo pipefail

BROKER="${BROKER:-localhost:29092}"   # in-container path
EXEC="${EXEC:-sudo docker exec -i oj-kafka kafka-topics --bootstrap-server $BROKER}"

# Replication factor + min.insync.replicas track the broker count today.
# 3-broker rollout will bump these to RF=3 / ISR=2 — keep the rest of the
# script structure stable so the only diff is two numbers.
RF=1
ISR=1

create_topic() {
  local name="$1" partitions="$2" retention_ms="${3:-604800000}"  # 7d default
  $EXEC --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor "$RF" \
    --config "min.insync.replicas=$ISR" \
    --config "retention.ms=$retention_ms"
  echo "[bootstrap] $name partitions=$partitions rf=$RF isr=$ISR retention_ms=$retention_ms"
}

# Hot path topics — 12 partitions to match the worker's 4-consumer pretest
# parallelism and the eventual 2nd compute VM (consumer-group can grow to 8
# instances).
create_topic submissions.pretest    12
create_topic submissions.system     12
create_topic evaluated_results      12
create_topic analytics              12
# Roadmap §2.5 dead-letter topic. 6 partitions is plenty — DLQ traffic is
# rare and a contestant's submission ID hashes evenly.
create_topic submissions.dlq         6 2592000000   # 30d retention for forensics

echo "[bootstrap] done — listing all topics:"
$EXEC --list
