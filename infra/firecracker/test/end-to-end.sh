#!/usr/bin/env bash
# =============================================================================
# end-to-end.sh — Workstream H smoke test for the full submission pipeline.
#
# Walks the production data path end-to-end:
#
#   1. Health-check api-gateway + problem-service + sandbox-manager.
#   2. POST a known-good Python submission ("print(2+2)") to api-gateway,
#      capture the returned submission_id.
#   3. Tail the `evaluated_results` Kafka topic for up to 30s; match on the
#      submission_id (best-effort string match in the protobuf payload —
#      full proto decode here is overkill).
#   4. Assert the verdict is "OK"/"ACCEPTED" and report ok / fail.
#
# Idempotent: every run uses a freshly-generated submission_id; no state on
# disk; safe to invoke in a tight loop from a healthcheck cron.
#
# Where to run it from:
#   - On a developer Mac when running the repo-root docker-compose stack —
#     all three services are reachable on localhost.
#   - On the control-plane VM in GCP — api-gateway is local; problem-service
#     is local (once Workstream A lands it); sandbox-manager is on the
#     compute VM with no public IP, so its healthcheck is best-effort
#     (`--skip-sm-health` works around it). If you need to reach the SM
#     directly, run this script from inside the VPC or via SSH on
#     oj-compute.
#
# Dependencies on PATH:
#   - curl, jq
#   - one of:  kcat  OR  the oj-kafka container's bundled kafka-console-consumer
#
# Usage:
#   ./end-to-end.sh                                 # all defaults
#   ./end-to-end.sh --gateway-url http://localhost:8088
#   ./end-to-end.sh --problem-id <uuid> --language python
#   ./end-to-end.sh --skip-sm-health                # skip sandbox-manager probe
# =============================================================================
set -euo pipefail

# ---------- defaults --------------------------------------------------------

GATEWAY_URL="${GATEWAY_URL:-http://localhost:8088}"
PROBLEM_SERVICE_URL="${PROBLEM_SERVICE_URL:-http://localhost:8089}"
SANDBOX_MANAGER_URL="${SANDBOX_MANAGER_URL:-http://localhost:9100}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-oj-kafka}"
KAFKA_INTERNAL_BOOTSTRAP="${KAFKA_INTERNAL_BOOTSTRAP:-localhost:29092}"
EVALUATED_TOPIC="${EVALUATED_TOPIC:-evaluated_results}"

PROBLEM_ID="00000000-0000-0000-0000-0000000cafee"
LANGUAGE="python"
SKIP_SM_HEALTH=0
WAIT_SECONDS=30

# Known-good submission. The agent should produce stdout "4\n", which the
# default test case for problem 0000…cafee expects.
SOURCE_CODE="print(2+2)"

# ---------- args ------------------------------------------------------------

while [[ $# -gt 0 ]]; do
  case "$1" in
    --gateway-url)         GATEWAY_URL="$2"; shift 2 ;;
    --problem-service-url) PROBLEM_SERVICE_URL="$2"; shift 2 ;;
    --sandbox-manager-url) SANDBOX_MANAGER_URL="$2"; shift 2 ;;
    --problem-id)          PROBLEM_ID="$2"; shift 2 ;;
    --language)            LANGUAGE="$2"; shift 2 ;;
    --skip-sm-health)      SKIP_SM_HEALTH=1; shift ;;
    --wait-seconds)        WAIT_SECONDS="$2"; shift 2 ;;
    --help|-h)             sed -n '2,40p' "$0"; exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

# ---------- helpers ---------------------------------------------------------

log()  { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }
fail() { printf '\nRESULT: FAIL — %s\n' "$*" >&2; exit 1; }
ok()   { printf '\nRESULT: OK — %s\n' "$*"; exit 0; }

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "missing required command: $1"
}

# Two consumer strategies. Prefer kcat (lighter, no docker dep); fall
# back to invoking the kafka-console-consumer that ships inside the
# oj-kafka container.
have_kcat()                  { command -v kcat >/dev/null 2>&1; }
have_docker_kafka_consumer() { docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "$KAFKA_CONTAINER"; }

# ---------- preflight -------------------------------------------------------

require_cmd curl
require_cmd jq

if ! have_kcat && ! have_docker_kafka_consumer; then
  fail "need either kcat on PATH OR a running '$KAFKA_CONTAINER' container with kafka-console-consumer."
fi

# ---------- 1) health checks -----------------------------------------------

health_check() {
  local name="$1" url="$2"
  if curl -fsS -m 3 "$url/actuator/health" >/dev/null 2>&1; then
    log "OK    $name @ $url"
  else
    fail "$name @ $url is not healthy (actuator/health failed)"
  fi
}

log "preflight: hitting actuator/health on each service"
health_check api-gateway      "$GATEWAY_URL"
# problem-service may not be deployed yet (Workstream A). Treat 404 / connect
# failure as informational, not fatal — the smoke test can still drive the
# rest of the pipeline.
if curl -fsS -m 3 "$PROBLEM_SERVICE_URL/actuator/health" >/dev/null 2>&1; then
  log "OK    problem-service @ $PROBLEM_SERVICE_URL"
else
  log "SKIP  problem-service @ $PROBLEM_SERVICE_URL (not reachable — Workstream A may not be deployed yet)"
fi
if [[ "$SKIP_SM_HEALTH" -eq 0 ]]; then
  if curl -fsS -m 3 "$SANDBOX_MANAGER_URL/actuator/health" >/dev/null 2>&1; then
    log "OK    sandbox-manager @ $SANDBOX_MANAGER_URL"
  else
    log "SKIP  sandbox-manager @ $SANDBOX_MANAGER_URL (not directly reachable — run from inside VPC or pass --skip-sm-health)"
  fi
fi

# ---------- 2) POST submission ---------------------------------------------

log "submitting source to $GATEWAY_URL/api/v1/submissions"

submission_body=$(jq -n \
  --arg problem_id "$PROBLEM_ID" \
  --arg language "$LANGUAGE" \
  --arg source "$SOURCE_CODE" \
  '{problemId: $problem_id, language: $language, source: $source}')

response=$(curl -fsS -m 10 -X POST \
  -H 'Content-Type: application/json' \
  -d "$submission_body" \
  "$GATEWAY_URL/api/v1/submissions" \
  || fail "submission POST failed (is api-gateway up? auth required?)")

submission_id=$(printf '%s' "$response" | jq -r '.submissionId // .submission_id // empty')
if [[ -z "$submission_id" ]]; then
  fail "could not extract submissionId from response: $response"
fi
log "submission_id = $submission_id"

# ---------- 3) wait for verdict --------------------------------------------

log "tailing $EVALUATED_TOPIC for up to ${WAIT_SECONDS}s, looking for submission_id"

verdict_payload_file=$(mktemp)
trap 'rm -f "$verdict_payload_file"' EXIT

# best-effort string match — the proto wire encoding stores submission_id
# as a length-delimited UTF-8 string at field 1, so the literal UUID
# bytes appear unchanged in the binary payload. grep -a treats binary as text.
match_submission() {
  grep -a -m1 "$submission_id" > "$verdict_payload_file"
}

if have_kcat; then
  log "using kcat against $KAFKA_BOOTSTRAP"
  # -e exits at end-of-topic; -o end then would miss messages, so use -o
  # beginning bounded by -c (count of messages we'll scan) and timeout via timeout(1).
  timeout "${WAIT_SECONDS}" \
    kcat -b "$KAFKA_BOOTSTRAP" -t "$EVALUATED_TOPIC" -C -o end -u \
    2>/dev/null \
    | match_submission || true
else
  log "using docker exec $KAFKA_CONTAINER kafka-console-consumer against $KAFKA_INTERNAL_BOOTSTRAP"
  timeout "${WAIT_SECONDS}" \
    docker exec "$KAFKA_CONTAINER" \
      kafka-console-consumer \
        --bootstrap-server "$KAFKA_INTERNAL_BOOTSTRAP" \
        --topic "$EVALUATED_TOPIC" \
        --from-beginning \
        --timeout-ms $((WAIT_SECONDS * 1000)) \
    2>/dev/null \
    | match_submission || true
fi

if [[ ! -s "$verdict_payload_file" ]]; then
  fail "no verdict for $submission_id appeared on $EVALUATED_TOPIC within ${WAIT_SECONDS}s"
fi

# ---------- 4) assert verdict ----------------------------------------------

# The VerdictEvent proto encodes `result` (field 5) as a length-delim string.
# We don't need a full parser — grep for any of the success-shaped tokens.
# If both ACCEPTED and OK appear (unlikely), we still pass.
if grep -aqE 'ACCEPTED|"OK"|^OK|\bOK\b' "$verdict_payload_file"; then
  ok "submission $submission_id verdict looks like ACCEPTED/OK"
fi

# Otherwise dump a short hex preview so a human can see what shape we got.
preview=$(head -c 200 "$verdict_payload_file" | od -c | head -5 | tr '\n' ' ')
fail "verdict for $submission_id was not ACCEPTED/OK. payload preview: $preview"
