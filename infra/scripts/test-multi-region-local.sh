#!/usr/bin/env bash
# =============================================================================
# infra/scripts/test-multi-region-local.sh
#
# Phase 5 — local failure-mode validation harness. Spins up a two-region oj
# stack on the host (see infra/compose/test-multi-region.yml), exercises the
# three failure paths that Phases 1–4 added, and tears down. Idempotent; safe
# to re-run.
#
# Scenarios:
#   S1 — 307 region-mismatch redirect (RegionMismatchFilter)
#   S2 — leaderboard global=true graceful peer degradation
#   S3 — per-region Kafka topic isolation (bootstrap covers both regions)
#
# Usage:
#   bash infra/scripts/test-multi-region-local.sh
#   bash infra/scripts/test-multi-region-local.sh --keep-up    # skip teardown
#
# Runtime: ~15 min on a cold build, ~5 min on subsequent runs (cached layers).
# =============================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="${ROOT}/infra/compose/test-multi-region.yml"
DC=(docker compose -f "${COMPOSE_FILE}")

KEEP_UP=false
case "${1:-}" in
  --keep-up) KEEP_UP=true ;;
  "") ;;
  *) echo "ERROR: unknown arg: $1" >&2; echo "Usage: $0 [--keep-up]" >&2; exit 1 ;;
esac

FAILED_SCENARIOS=()
WORKDIR="$(mktemp -d)"
trap 'rm -rf "${WORKDIR}"' EXIT

teardown() {
  if "${KEEP_UP}"; then
    echo "[teardown] --keep-up set; leaving stack running. Tear down manually:"
    echo "           docker compose -f ${COMPOSE_FILE} down -v"
    return
  fi
  echo "[teardown] docker compose down -v"
  "${DC[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}

wait_for() {
  # wait_for "<label>" <seconds> <cmd...>
  local label="$1" max="$2"; shift 2
  local i=0
  while ! "$@" >/dev/null 2>&1; do
    i=$((i+1))
    if [ "$i" -ge "$max" ]; then
      echo "[wait] timed out waiting for ${label} after ${max}s" >&2
      return 1
    fi
    sleep 1
  done
  echo "[wait] ${label} ready (${i}s)"
}

# ─── Step 1: build ─────────────────────────────────────────────────────────
echo "[step 1] building images (this is the slow first-run step)"
"${DC[@]}" build api-gateway-a leaderboard-a

# ─── Step 2: infra ─────────────────────────────────────────────────────────
echo "[step 2] starting infra (zookeeper, kafka, crdb-a, crdb-b, redis-a, redis-b)"
"${DC[@]}" up -d zookeeper kafka crdb-a crdb-b redis-a redis-b

# ─── Step 3: CRDB cluster init + multi-region declaration ─────────────────
echo "[step 3] waiting for CRDB nodes to be reachable"
wait_for "crdb-a http" 60 docker exec oj-test-crdb-a curl -fsS http://localhost:8080/health
wait_for "crdb-b http" 60 docker exec oj-test-crdb-b curl -fsS http://localhost:8080/health

echo "[step 3] cockroach init (idempotent — tolerates 'already initialized')"
docker exec oj-test-crdb-a cockroach init --insecure --host=crdb-a:26257 \
  || echo "[step 3]   (init tolerated — cluster already initialised)"

echo "[step 3] waiting for both nodes to report is_available=true"
wait_for "crdb cluster healthy" 90 bash -c '
  out=$(docker exec oj-test-crdb-a cockroach node status --insecure --host=crdb-a:26257 --format=tsv 2>/dev/null) || exit 1
  # Header + 2 data rows expected; both is_available columns must be "true".
  echo "$out" | tail -n +2 | awk -F"\t" "\$8==\"true\"{c++} END{exit (c==2)?0:1}"
'

# ─── Step 4a: Flyway V1..V8 BEFORE multi-region init ─────────────────────
# Order matters: applying V2..V8 (which include ALTER TABLE statements) on a
# database that already has PRIMARY REGION declared is pathologically slow on
# Docker Desktop CRDB — non-transactional schema changes block for minutes per
# step waiting for region-lease replication. So we run V1..V8 against a
# single-region database first (fast: ~5 s), THEN declare regions, THEN run
# V9 which is the only migration that needs multi-region semantics.
# This also matches the production runbook order: operators run V8, declare
# regions, then run V9.
# Create the database explicitly. The old harness ordering had
# crdb-multiregion-init.sh run first (which creates it as a side-effect of
# its `CREATE DATABASE IF NOT EXISTS` step). Now that init runs AFTER Flyway,
# we have to create the bare DB here ourselves.
echo "[step 4-prep] creating database onlinejudge"
docker exec oj-test-crdb-a cockroach sql --insecure --host=crdb-a:26257 \
  -e "CREATE DATABASE IF NOT EXISTS onlinejudge;"

echo "[step 4a] applying Flyway V1..V8 against crdb-a (single-region phase)"
docker run --rm --network=oj-test-net \
  -v "${ROOT}/api-gateway/src/main/resources/db/migration:/flyway/sql:ro" \
  flyway/flyway:9.22 \
  -url="jdbc:postgresql://crdb-a:26257/onlinejudge?sslmode=disable" \
  -user=root -password= \
  -baselineOnMigrate=true -baselineVersion=0 \
  -target=8 \
  migrate

echo "[step 4b] declaring multi-region (asia-south1 primary, us-central1 secondary)"
DOCKER=docker CRDB_CONTAINER=oj-test-crdb-a \
  bash "${ROOT}/infra/scripts/crdb-multiregion-init.sh" asia-south1 us-central1

echo "[step 4c] applying Flyway V9 against crdb-a (multi-region LOCALITY DDL)"
docker run --rm --network=oj-test-net \
  -v "${ROOT}/api-gateway/src/main/resources/db/migration:/flyway/sql:ro" \
  flyway/flyway:9.22 \
  -url="jdbc:postgresql://crdb-a:26257/onlinejudge?sslmode=disable" \
  -user=root -password= \
  migrate

# ─── Step 5: Kafka topic bootstrap, per region ────────────────────────────
echo "[step 5] bootstrapping per-region Kafka topics"
DOCKER=docker bash "${ROOT}/infra/scripts/kafka-bootstrap-topics.sh" \
  --region asia-south1 --container oj-test-kafka --broker localhost:29092
DOCKER=docker bash "${ROOT}/infra/scripts/kafka-bootstrap-topics.sh" \
  --region us-central1 --container oj-test-kafka --broker localhost:29092

# kafka-bootstrap-topics.sh honours DOCKER=docker (added in this phase to
# match crdb-multiregion-init.sh's existing pattern), so the calls above
# skip sudo entirely on dev hosts.

# ─── Step 6: app containers ───────────────────────────────────────────────
echo "[step 6] starting api-gateway-a/b and leaderboard-a/b"
"${DC[@]}" up -d api-gateway-a api-gateway-b leaderboard-a leaderboard-b

wait_for "api-gateway-a UP"  180 bash -c "curl -fsS http://localhost:18088/actuator/health | grep -q '\"status\":\"UP\"'"
wait_for "api-gateway-b UP"  180 bash -c "curl -fsS http://localhost:18089/actuator/health | grep -q '\"status\":\"UP\"'"
wait_for "leaderboard-a UP"  120 bash -c "curl -fsS http://localhost:18182/actuator/health | grep -q '\"status\":\"UP\"'"
wait_for "leaderboard-b UP"  120 bash -c "curl -fsS http://localhost:18083/actuator/health | grep -q '\"status\":\"UP\"'"

# ─── Step 7: scenarios ────────────────────────────────────────────────────

# Unique username per run so re-runs against the same volumes don't collide.
USERNAME="alice-$(date +%s)"
PASSWORD="supersecret123"

# S1 — 307 region-mismatch redirect.
#
# Pick: GET /api/v1/submissions/health on gateway-b with gateway-a's JWT.
# Why this endpoint:
#   * It lives under /api/, so RegionMismatchFilter inspects it.
#   * It is permitAll in SecurityConfig, so a malformed token can't 401 ahead
#     of the filter (the filter only redirects when JwtAuthenticationFilter
#     successfully extracted a region claim).
#   * It is NOT in RegionMismatchFilter.PUBLIC_PATHS (which only lists
#     /api/v1/auth/{signup,login,refresh}), so the filter will not bypass.
echo "[scenario 1] 307 region-mismatch redirect"
SIGNUP_BODY="{\"username\":\"${USERNAME}\",\"password\":\"${PASSWORD}\"}"
curl -fsS -X POST -H 'Content-Type: application/json' \
  -d "${SIGNUP_BODY}" \
  http://localhost:18088/api/v1/auth/signup >/dev/null
LOGIN_RESP=$(curl -fsS -X POST -H 'Content-Type: application/json' \
  -d "${SIGNUP_BODY}" \
  http://localhost:18088/api/v1/auth/login)
ACCESS_TOKEN=$(printf '%s' "${LOGIN_RESP}" \
  | sed -n 's/.*"accessToken":"\([^"]*\)".*/\1/p')
if [ -z "${ACCESS_TOKEN}" ]; then
  echo "[scenario 1] FAIL — could not extract accessToken from login response: ${LOGIN_RESP}"
  FAILED_SCENARIOS+=("S1")
else
  S1_OUT="${WORKDIR}/s1-headers.txt"
  curl -sS -o /dev/null -D "${S1_OUT}" \
    -H "Authorization: Bearer ${ACCESS_TOKEN}" \
    http://localhost:18089/api/v1/submissions/health
  STATUS=$(awk 'NR==1 {print $2}' "${S1_OUT}")
  LOCATION=$(awk -F': ' 'tolower($1)=="location"{sub(/\r$/,"",$2); print $2; exit}' "${S1_OUT}")
  EXPECTED_LOC="http://localhost:18088/api/v1/submissions/health"
  if [ "${STATUS}" = "307" ] && [ "${LOCATION}" = "${EXPECTED_LOC}" ]; then
    echo "[scenario 1] PASS — status=307 Location=${LOCATION}"
  else
    echo "[scenario 1] FAIL — status=${STATUS} Location=${LOCATION} (expected 307 and ${EXPECTED_LOC})"
    sed 's/^/[scenario 1]   | /' "${S1_OUT}"
    FAILED_SCENARIOS+=("S1")
  fi
fi

# S2 — peer-leaderboard graceful degradation.
#
# Hit leaderboard-a's global=true endpoint while leaderboard-b is down.
# Expected: HTTP 200 + X-Peer-Region-Unreachable: true.
echo "[scenario 2] leaderboard global=true graceful degradation"
"${DC[@]}" stop leaderboard-b >/dev/null
# Give leaderboard-a's HttpClient a moment to see the next attempt fail fast.
sleep 2
S2_OUT="${WORKDIR}/s2-headers.txt"
curl -sS -o /dev/null -D "${S2_OUT}" \
  "http://localhost:18182/api/v1/leaderboard/test-contest-phase5?global=true&page=0&size=10"
S2_STATUS=$(awk 'NR==1 {print $2}' "${S2_OUT}")
S2_UNREACH=$(awk -F': ' 'tolower($1)=="x-peer-region-unreachable"{sub(/\r$/,"",$2); print tolower($2); exit}' "${S2_OUT}")
if [ "${S2_STATUS}" = "200" ] && [ "${S2_UNREACH}" = "true" ]; then
  echo "[scenario 2] PASS — status=200 X-Peer-Region-Unreachable=true"
else
  echo "[scenario 2] FAIL — status=${S2_STATUS} X-Peer-Region-Unreachable=${S2_UNREACH:-<absent>}"
  sed 's/^/[scenario 2]   | /' "${S2_OUT}"
  FAILED_SCENARIOS+=("S2")
fi
# Hygiene — bring B back so a --keep-up run leaves a complete stack.
"${DC[@]}" up -d leaderboard-b >/dev/null

# S3 — worker region isolation (via topic presence).
#
# The execution-worker subscribes to submissions.${APP_REGION}.pretest. Rather
# than run a worker, validate the BOOTSTRAP produced both regions' topic
# families — that's the contract the worker depends on.
echo "[scenario 3] per-region Kafka topic isolation"
TOPICS=$(docker exec oj-test-kafka kafka-topics \
  --bootstrap-server localhost:29092 --list 2>/dev/null || true)
MISSING=()
for t in \
  "submissions.asia-south1.pretest" \
  "submissions.us-central1.pretest" \
  "submissions.asia-south1.dlq" \
  "submissions.us-central1.dlq" \
  "evaluated_results.asia-south1" \
  "evaluated_results.us-central1"; do
  if ! printf '%s\n' "${TOPICS}" | grep -qx "${t}"; then
    MISSING+=("${t}")
  fi
done
if [ ${#MISSING[@]} -eq 0 ]; then
  echo "[scenario 3] PASS — both regions' topic families present"
else
  echo "[scenario 3] FAIL — missing topics: ${MISSING[*]}"
  echo "[scenario 3]   topics seen:"
  printf '%s\n' "${TOPICS}" | sed 's/^/[scenario 3]   | /'
  FAILED_SCENARIOS+=("S3")
fi

# ─── Step 8: teardown ──────────────────────────────────────────────────────
teardown

if [ ${#FAILED_SCENARIOS[@]} -gt 0 ]; then
  echo "[summary] FAIL — failing scenarios: ${FAILED_SCENARIOS[*]}"
  exit 1
fi
echo "[summary] PASS — all three scenarios green"
