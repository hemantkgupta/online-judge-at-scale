#!/usr/bin/env bash
#
# infra/gcp/scripts/scoring-deploy.sh — productionised JAR-submission step for
# the scoring-pipeline Flink job. Companion to the JM/TM blocks added to
# `infra/gcp/compose/region.yml`.
#
# This script is idempotent:
#   - If a RUNNING job already exists on the JM, it logs and exits 0.
#   - Otherwise it builds the shadowJar, uploads it via /jars/upload, parses
#     the returned jar-id, and submits the job with parallelism = the TM's
#     `taskmanager.numberOfTaskSlots` (default 2).
#
# It does NOT touch the leaderboard-service writer — that cutover is a
# separate PR, run only after operator observes parity on
# `oj.scoring.score_updates_total`. See docs/services/scoring-pipeline.md
# "Follow-up: leaderboard-service writer cutover".
#
# Environment variables read by the Flink JOB at runtime (set on the JM/TM
# container in region.yml; listed here so operators editing this script
# understand the full surface):
#   SCORING_KAFKA_BOOTSTRAP_SERVERS  Kafka bootstrap (default oj-kafka:29092
#                                    in compose).
#   SCORING_INPUT_TOPIC              Source topic. Per-region wiring uses
#                                    `evaluated_results.${REGION}`.
#   REDIS_HOST                       Redis sink host (oj-redis in compose).
#   CHECKPOINT_DIR                   Checkpoint URI. `file:///flink-checkpoints`
#                                    (named volume) for v1; production target
#                                    is gs://<bucket>/flink-checkpoints once
#                                    the flink-gs-fs-hadoop plugin is staged
#                                    on the JM/TM images.
#
# Environment variables read by THIS SCRIPT:
#   FLINK_REST_HOST                  default: localhost
#   FLINK_REST_PORT                  default: 18081
#   SCORING_PARALLELISM              default: 1
#   REPO_ROOT                        default: derived from script location
#   BUILD                            default: 1; set BUILD=0 to skip
#                                    `./gradlew :scoring-pipeline:shadowJar`
#                                    when re-using an existing JAR.
#
# Exit codes:
#   0   job is RUNNING (either just submitted or already was)
#   1   any prerequisite or submission failure
#
# Usage (on the region VM, after `docker compose -f region.yml up -d
# oj-flink-jobmanager oj-flink-taskmanager`):
#   ./infra/gcp/scripts/scoring-deploy.sh

set -euo pipefail

# ---------------- config ----------------

# Walk two levels up from infra/gcp/scripts/ to the repo root.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="${REPO_ROOT:-$(cd "${SCRIPT_DIR}/../../.." && pwd)}"

FLINK_REST_HOST="${FLINK_REST_HOST:-localhost}"
FLINK_REST_PORT="${FLINK_REST_PORT:-18081}"
FLINK_BASE="http://${FLINK_REST_HOST}:${FLINK_REST_PORT}"

SCORING_PARALLELISM="${SCORING_PARALLELISM:-1}"
BUILD="${BUILD:-1}"

ENTRY_CLASS="com.onlinejudge.scoring.ScoringJobApplication"

# ---------------- ergonomics ----------------

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m'

pass() { printf "${GREEN}\xE2\x9C\x93${NC} %s\n" "$1"; }
fail() { printf "${RED}\xE2\x9C\x97${NC} %s\n" "$1"; exit 1; }
info() { printf "${YELLOW}\xE2\x80\xA6${NC} %s\n" "$1"; }

flink() {
    curl -fsS "${FLINK_BASE}$1"
}

# ---------------- pre-flight ----------------

check_prereqs() {
    info "Checking prerequisites"
    command -v curl >/dev/null 2>&1 || fail "curl is required"
    flink /overview >/dev/null \
        || fail "Flink JobManager REST API not reachable at ${FLINK_BASE}. Start the cluster: docker compose -f infra/gcp/compose/region.yml up -d oj-flink-jobmanager oj-flink-taskmanager"
    pass "JobManager reachable at ${FLINK_BASE}"
}

# ---------------- idempotency check ----------------

job_already_running() {
    local overview running_count
    overview="$(flink /jobs/overview)"
    # `grep -c ... || true` keeps a no-match exit from killing the script
    # under `set -euo pipefail`.
    running_count="$(printf "%s" "$overview" | grep -c '"state":"RUNNING"' || true)"
    [ "$running_count" -ge 1 ]
}

# ---------------- build + submit ----------------

build_shadow_jar() {
    if [ "$BUILD" = "0" ]; then
        info "BUILD=0 set — skipping ./gradlew :scoring-pipeline:shadowJar"
        return 0
    fi
    info "Building scoring-pipeline shadowJar"
    (cd "$REPO_ROOT" && ./gradlew :scoring-pipeline:shadowJar -q) \
        || fail "shadowJar build failed"
    pass "shadowJar built"
}

locate_shadow_jar() {
    # Gradle pins archiveBaseName=scoring-pipeline + archiveClassifier=all,
    # but the project version lives in the middle of the filename — glob it.
    local jar
    jar="$(ls -t "$REPO_ROOT"/scoring-pipeline/build/libs/scoring-pipeline-*-all.jar 2>/dev/null | head -1)"
    [ -n "$jar" ] && [ -f "$jar" ] \
        || fail "shadowJar output not found under $REPO_ROOT/scoring-pipeline/build/libs/ — re-run without BUILD=0"
    printf "%s" "$jar"
}

upload_and_submit() {
    local jar="$1"
    info "Uploading $(basename "$jar") to ${FLINK_BASE}"
    local upload_resp
    upload_resp="$(curl -fsS -X POST -H "Expect:" \
        -F "jarfile=@${jar}" \
        "${FLINK_BASE}/jars/upload")"

    # Flink's `/jars/<id>/run` id is the basename of the uploaded JAR — i.e.
    # `<UUID>_<originalFilename>.jar` — visible inside the `filename` field
    # of the upload response as an absolute path under `/flink-web-upload/`.
    local jar_id
    jar_id="$(printf "%s" "$upload_resp" \
        | sed -nE 's|.*"filename":"[^"]*/flink-web-upload/([^"]+)".*|\1|p')"
    [ -n "$jar_id" ] || fail "could not parse jar id from upload response: $upload_resp"
    pass "Uploaded as ${jar_id}"

    info "Submitting job (entry-class=${ENTRY_CLASS}, parallelism=${SCORING_PARALLELISM})"
    curl -fsS -X POST -H 'Content-Type: application/json' -d '{}' \
        "${FLINK_BASE}/jars/${jar_id}/run?entry-class=${ENTRY_CLASS}&parallelism=${SCORING_PARALLELISM}" \
        >/dev/null \
        || fail "Failed to submit Flink job"

    # Poll for RUNNING — generous 60s budget covers TM slot allocation +
    # Kafka source initialisation.
    local waited=0
    until job_already_running; do
        sleep 2; waited=$((waited+2))
        [ "$waited" -ge 60 ] && fail "Flink job did not reach RUNNING within 60s — check ${FLINK_BASE}/jobs/overview"
    done
    pass "Flink job RUNNING (took ${waited}s)"
}

# ---------------- main ----------------

main() {
    check_prereqs

    if job_already_running; then
        pass "A RUNNING Flink job already exists — skipping submission (idempotent re-run)"
        printf "${GREEN}OK${NC} scoring-pipeline already deployed\n"
        return 0
    fi

    info "No RUNNING job found — preparing fresh submission"
    build_shadow_jar
    local jar
    jar="$(locate_shadow_jar)"
    upload_and_submit "$jar"

    printf "${GREEN}OK${NC} scoring-pipeline deployed (Web UI: %s)\n" "$FLINK_BASE"
}

main "$@"
