#!/usr/bin/env bash
# =============================================================================
# Kafka topic bootstrap — REGIONAL.
#
# Each region runs its own Kafka broker(s) and owns a NAMESPACED set of topics
# (submissions.<region>.pretest etc.). This script creates that one region's
# topics on the local broker. Run ONCE per region after `docker compose up`
# on that region's control-plane VM — and again on each new region as they
# come online.
#
# Backwards-incompatible note: the previous globally-named topics
# (submissions.pretest, evaluated_results, ...) are GONE in this layout.
# api-gateway, execution-worker, contest-service, leaderboard-service have
# all been moved to the region-namespaced names in parallel.
#
# Re-running is safe — every create uses --if-not-exists.
#
# Examples:
#   sudo bash infra/scripts/kafka-bootstrap-topics.sh --region asia-south1
#   sudo bash infra/scripts/kafka-bootstrap-topics.sh --region us-central1 --rf 3 --partitions 24
#
# Under the eventual 3-broker move per region: pass --rf 3 (and bump ISR via
# the env var if you care). Partition count stays 12 unless capacity demands.
# =============================================================================

set -euo pipefail

REGION=""
BROKER="${BROKER:-localhost:29092}"   # in-container path
RF="${RF:-1}"                          # bumps to 3 on the 3-broker move
ISR="${ISR:-1}"
PARTITIONS="${PARTITIONS:-12}"
RETENTION_MS="${RETENTION_MS:-604800000}"     # 7d default for hot-path topics
DLQ_RETENTION_MS="${DLQ_RETENTION_MS:-2592000000}"   # 30d for forensics
CONTAINER="${CONTAINER:-oj-kafka}"

usage() {
  cat <<EOF
Usage: $0 --region <name> [--broker host:port] [--rf N] [--partitions N]

Required:
  --region <name>      Region slug (e.g. asia-south1, us-central1).
                        Interpolated into every topic name.

Optional:
  --broker host:port   Kafka bootstrap server. Default: ${BROKER}
  --rf N               Replication factor. Default: ${RF}
  --partitions N       Partition count for hot-path topics. Default: ${PARTITIONS}
  --container NAME     Docker container running kafka-topics. Default: ${CONTAINER}
  -h | --help          Show this help.

Environment overrides also accepted: BROKER, RF, ISR, PARTITIONS, RETENTION_MS,
DLQ_RETENTION_MS, CONTAINER.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --region) REGION="$2"; shift 2 ;;
    --broker) BROKER="$2"; shift 2 ;;
    --rf) RF="$2"; shift 2 ;;
    --partitions) PARTITIONS="$2"; shift 2 ;;
    --container) CONTAINER="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "ERROR: unknown arg: $1" >&2; usage >&2; exit 1 ;;
  esac
done

if [[ -z "$REGION" ]]; then
  echo "ERROR: --region is required (e.g. --region asia-south1)" >&2
  usage >&2
  exit 1
fi

EXEC="sudo docker exec -i ${CONTAINER} kafka-topics --bootstrap-server ${BROKER}"

create_topic() {
  local name="$1" partitions="$2" retention_ms="${3:-$RETENTION_MS}"
  $EXEC --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor "$RF" \
    --config "min.insync.replicas=$ISR" \
    --config "retention.ms=$retention_ms"
  echo "[bootstrap] $name partitions=$partitions rf=$RF isr=$ISR retention_ms=$retention_ms"
}

echo "[bootstrap] region=$REGION broker=$BROKER rf=$RF partitions=$PARTITIONS"

# Hot-path topics. Partition count matches the worker's pretest parallelism
# (4 consumers × 3 instances = 12 desired) and gives headroom for the eventual
# second compute VM.
create_topic "submissions.${REGION}.pretest"     "$PARTITIONS"
create_topic "submissions.${REGION}.system"      "$PARTITIONS"
create_topic "evaluated_results.${REGION}"       "$PARTITIONS"
create_topic "analytics_events.${REGION}"        "$PARTITIONS"
create_topic "contest_events.${REGION}"          "$PARTITIONS"
# Dead-letter — rare traffic, long retention for forensics.
create_topic "submissions.${REGION}.dlq"          6 "$DLQ_RETENTION_MS"

echo "[bootstrap] done. Topics in cluster:"
$EXEC --list
