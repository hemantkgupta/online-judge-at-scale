#!/usr/bin/env bash
# CI smoke test: bring up the docker-compose stack, submit a synthetic
# SubmissionEvent through the Firecracker test harness, and verify a
# verdict comes back within 30 s. Works locally and inside GitHub Actions.
# Requires Docker + docker compose v2 on the host (GitHub-hosted ubuntu
# runners satisfy both). No gcloud, no GCP credentials — all localhost.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${REPO_ROOT}"

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"
API_HEALTH_URL="${API_HEALTH_URL:-http://localhost:8080/actuator/health}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"
VERDICT_TIMEOUT_S="${VERDICT_TIMEOUT_S:-30}"
WAIT_FOR_HEALTH_S="${WAIT_FOR_HEALTH_S:-180}"

log() { printf '[ci-smoke] %s\n' "$*"; }

cleanup() {
  log "tearing down compose stack"
  docker compose -f "${COMPOSE_FILE}" down -v --remove-orphans || true
}
trap cleanup EXIT

if [[ ! -f "${COMPOSE_FILE}" ]]; then
  log "ERROR: ${COMPOSE_FILE} not found at repo root"
  log ""
  log "This smoke harness needs a self-contained docker-compose.yml at the"
  log "repo root that brings up: kafka, cockroachdb, redis, api-gateway,"
  log "problem-service, sandbox-manager, execution-worker — all on local"
  log "ports (no GCP). The control-plane / compute compose files under"
  log "infra/gcp/compose/ are GCP-specific (Artifact Registry image refs,"
  log "Secret Manager mounts) and won't run on a CI runner. Authoring the"
  log "local compose file is a follow-up task; until then this smoke is a"
  log "skeleton and ci.yml does not invoke it."
  exit 1
fi

log "bringing up stack via ${COMPOSE_FILE}"
docker compose -f "${COMPOSE_FILE}" up -d

log "waiting up to ${WAIT_FOR_HEALTH_S}s for api-gateway health at ${API_HEALTH_URL}"
deadline=$(( $(date +%s) + WAIT_FOR_HEALTH_S ))
while ! curl -fsS --max-time 3 "${API_HEALTH_URL}" >/dev/null 2>&1; do
  if (( $(date +%s) >= deadline )); then
    log "ERROR: api-gateway did not become healthy in ${WAIT_FOR_HEALTH_S}s"
    docker compose -f "${COMPOSE_FILE}" ps
    docker compose -f "${COMPOSE_FILE}" logs --tail=200 api-gateway || true
    exit 1
  fi
  sleep 2
done
log "api-gateway healthy"

SUBMIT_SCRIPT="${REPO_ROOT}/infra/firecracker/test/submit-sample.py"
if [[ ! -f "${SUBMIT_SCRIPT}" ]]; then
  log "ERROR: submit-sample.py not found at ${SUBMIT_SCRIPT}"
  exit 1
fi

log "submitting synthetic submission and waiting for verdict (timeout ${VERDICT_TIMEOUT_S}s)"
# submit-sample.py is expected to publish a SubmissionEvent and consume
# the evaluated_results topic until a verdict arrives (or it times out).
# Exit code 0 means a verdict of ACCEPTED was observed; non-zero fails the job.
timeout "${VERDICT_TIMEOUT_S}" python3 "${SUBMIT_SCRIPT}" \
  --code 'print(2+2)' \
  --bootstrap "${KAFKA_BOOTSTRAP}" \
  --expect-verdict ACCEPTED

log "smoke test passed"
