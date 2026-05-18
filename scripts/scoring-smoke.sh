#!/usr/bin/env bash
#
# scripts/scoring-smoke.sh — end-to-end smoke test for the scoring-pipeline
# Flink job. Verifies the real wire path:
#
#   1. Produce protobuf VerdictEvents to `evaluated_results` on the regional
#      Kafka broker (kafka:9092).
#   2. MirrorMaker 2 mirrors them onto `regional.evaluated_results` on
#      kafka-global:9093.
#   3. The Flink scoring-pipeline job consumes from kafka-global, runs ICPC
#      scoring (Scorer.java), and atomically writes via Lua ZADD to the
#      score-range sharded ZSET `leaderboard:{contestId}:shard:{0..2}`.
#   4. Assert the exact zsetScore for each user and the single-shard
#      invariant (a user appears in exactly one of the 3 shards).
#
# Pre-requisites on the host:
#   redis-cli, nc, curl, docker — and the `compose` plugin.
#
# Usage:
#   ./scripts/scoring-smoke.sh
#
# The Flink job is uploaded + started ONLY if no RUNNING job is already
# present. The script never cancels a running job — re-running is idempotent.
# Cleanup at the end deletes only the contest's shard keys (DEL on
# leaderboard:<contest>:shard:0..2 and the sentinel contest's keys).
#
# Exit code 0 on success, non-zero on first failure.

set -euo pipefail

# ---------------- config ----------------

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

REDIS_HOST="${REDIS_HOST:-localhost}"
REDIS_PORT="${REDIS_PORT:-6379}"

KAFKA_REGIONAL_HOST="${KAFKA_REGIONAL_HOST:-localhost}"
KAFKA_REGIONAL_PORT="${KAFKA_REGIONAL_PORT:-9092}"

KAFKA_GLOBAL_HOST="${KAFKA_GLOBAL_HOST:-localhost}"
KAFKA_GLOBAL_PORT="${KAFKA_GLOBAL_PORT:-9093}"

FLINK_REST_HOST="${FLINK_REST_HOST:-localhost}"
FLINK_REST_PORT="${FLINK_REST_PORT:-18081}"

NUM_SHARDS="${NUM_SHARDS:-3}"  # ScoreRangeShardRouter.defaultIcpcRouter()

# Reproducible base timestamp: midnight UTC, May 18 2026.
SMOKE_BASE_TS_MS="${SMOKE_BASE_TS_MS:-1747526400000}"
SMOKE_CONTEST="${SMOKE_CONTEST:-smoke-contest-$$}"

# How long to wait (seconds) for the Flink job to project the verdicts into
# Redis after the producer flush completes. MM2 mirror lag + Flink scheduling
# dominates this — 60s is generous.
ZSCORE_WAIT_SECONDS="${ZSCORE_WAIT_SECONDS:-60}"

# Expected ICPC math (verified against Scorer.java + ScoreEncoder.java).
# Note: the score-range shard boundaries from ScoreRangeShardRouter.defaultIcpcRouter()
# are {0, 30M, 60M}, so shard 0 = [0, 30M), shard 1 = [30M, 60M), shard 2 = [60M, ∞).
# With a SCORE_MULTIPLIER of 10M, a single AC contributes >= 1_000_000_000 to the
# zsetScore — far above 60M. Both expected users therefore land in shard 2, not
# shard 0 as the original task description implied.
#   userA: 200 points, 20 min penalty → 200 * 10_000_000 - 20 = 1_999_999_980  (shard 2)
#   userB: 100 points,  0 min penalty → 100 * 10_000_000      = 1_000_000_000  (shard 2)
EXPECTED_SCORE_USER_A="1999999980"
EXPECTED_SCORE_USER_B="1000000000"
EXPECTED_SHARD_USER_A="2"
EXPECTED_SHARD_USER_B="2"

# ---------------- ergonomics ----------------

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m'

pass() { printf "${GREEN}\xE2\x9C\x93${NC} %s\n" "$1"; }
fail() { printf "${RED}\xE2\x9C\x97${NC} %s\n" "$1"; exit 1; }
info() { printf "${YELLOW}\xE2\x80\xA6${NC} %s\n" "$1"; }

redis_cli() {
    redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" "$@"
}

flink() {
    curl -fsS "http://${FLINK_REST_HOST}:${FLINK_REST_PORT}$1"
}

# ---------------- pre-flight ----------------

check_prereqs() {
    info "Checking prerequisites"
    command -v redis-cli >/dev/null 2>&1 || fail "redis-cli is required"
    command -v nc        >/dev/null 2>&1 || fail "nc is required"
    command -v curl      >/dev/null 2>&1 || fail "curl is required"
    command -v docker    >/dev/null 2>&1 || fail "docker is required"
    pass "Prerequisites present"
}

check_ports() {
    info "Verifying kafka:${KAFKA_REGIONAL_PORT}, kafka-global:${KAFKA_GLOBAL_PORT}, redis:${REDIS_PORT}, flink:${FLINK_REST_PORT} are open"
    nc -z "$KAFKA_REGIONAL_HOST" "$KAFKA_REGIONAL_PORT" \
        || fail "Regional Kafka not reachable on ${KAFKA_REGIONAL_HOST}:${KAFKA_REGIONAL_PORT} — try: docker compose up -d kafka zookeeper kafka-global mirrormaker2 redis jobmanager taskmanager"
    nc -z "$KAFKA_GLOBAL_HOST"   "$KAFKA_GLOBAL_PORT"   \
        || fail "Global Kafka not reachable on ${KAFKA_GLOBAL_HOST}:${KAFKA_GLOBAL_PORT}"
    [ "$(redis_cli ping)" = "PONG" ] \
        || fail "Redis PING failed against ${REDIS_HOST}:${REDIS_PORT}"
    flink /overview >/dev/null \
        || fail "Flink REST API not reachable at http://${FLINK_REST_HOST}:${FLINK_REST_PORT}"
    pass "All four endpoints reachable"
}

# ---------------- Flink job lifecycle ----------------

ensure_flink_job_running() {
    info "Inspecting Flink job state"
    local overview
    overview="$(flink /jobs/overview)"
    local running_count
    # `grep -c` plus `|| true` keeps a no-match exit from killing the script
    # under `set -euo pipefail`.
    running_count="$(printf "%s" "$overview" | grep -c '"state":"RUNNING"' || true)"

    if [ "$running_count" -ge 1 ]; then
        pass "Reusing existing RUNNING Flink job (${running_count} found)"
        return 0
    fi

    info "No RUNNING job; building shadowJar"
    (cd "$REPO_ROOT" && ./gradlew :scoring-pipeline:shadowJar -q) \
        || fail "shadowJar build failed"
    # Gradle's default archiveFileName is `<baseName>-<version>-<classifier>.jar`;
    # build.gradle pins baseName=scoring-pipeline and classifier=all, but the
    # project version (e.g. 1.0-SNAPSHOT) shows up in the middle. Glob it.
    local jar
    jar="$(ls -t "$REPO_ROOT"/scoring-pipeline/build/libs/scoring-pipeline-*-all.jar 2>/dev/null | head -1)"
    [ -n "$jar" ] && [ -f "$jar" ] \
        || fail "shadowJar output not found under $REPO_ROOT/scoring-pipeline/build/libs/"

    info "Uploading $(basename "$jar") to Flink"
    local upload_resp jar_id
    upload_resp="$(curl -fsS -X POST -H "Expect:" \
        -F "jarfile=@${jar}" \
        "http://${FLINK_REST_HOST}:${FLINK_REST_PORT}/jars/upload")"
    # The `id` Flink uses for /jars/<id>/run is the full basename of the upload
    # — i.e. `<UUID>_<originalFilename>.jar`. The upload-response `filename`
    # field gives us the absolute path; we want only its basename.
    jar_id="$(printf "%s" "$upload_resp" \
        | sed -nE 's|.*"filename":"[^"]*/flink-web-upload/([^"]+)".*|\1|p')"
    [ -n "$jar_id" ] || fail "could not parse jar id from upload response: $upload_resp"

    info "Submitting job (jar id ${jar_id})"
    curl -fsS -X POST -H 'Content-Type: application/json' -d '{}' \
        "http://${FLINK_REST_HOST}:${FLINK_REST_PORT}/jars/${jar_id}/run?parallelism=1" \
        >/dev/null \
        || fail "Failed to submit Flink job"

    # Poll for RUNNING
    local waited=0
    until [ "$(flink /jobs/overview | grep -c '"state":"RUNNING"' || true)" -ge 1 ]; do
        sleep 2; waited=$((waited+2))
        [ "$waited" -ge 60 ] && fail "Flink job did not reach RUNNING within 60s"
    done
    pass "Flink job RUNNING (took ${waited}s)"
}

# ---------------- produce + assert ----------------

produce_verdicts() {
    # Default: produce to regional Kafka and rely on MM2 to mirror to
    # `regional.evaluated_results`. Set SMOKE_DIRECT_TO_GLOBAL=1 to skip MM2
    # and produce directly to kafka-global on the mirrored topic name —
    # useful when MM2 is broken in this dev environment.
    if [ "${SMOKE_DIRECT_TO_GLOBAL:-0}" = "1" ]; then
        info "Producing VerdictEvents DIRECTLY to regional.evaluated_results on kafka-global (MM2 bypass) — contest=${SMOKE_CONTEST}"
        local bootstrap="${KAFKA_GLOBAL_HOST}:${KAFKA_GLOBAL_PORT}"
        local topic="regional.evaluated_results"
    else
        info "Producing VerdictEvents to evaluated_results on regional Kafka — contest=${SMOKE_CONTEST}, base=${SMOKE_BASE_TS_MS}"
        local bootstrap="${KAFKA_REGIONAL_HOST}:${KAFKA_REGIONAL_PORT}"
        local topic="evaluated_results"
    fi
    (
        cd "$REPO_ROOT" && \
        KAFKA_BOOTSTRAP="$bootstrap" \
        SMOKE_TOPIC="$topic" \
        SMOKE_CONTEST="$SMOKE_CONTEST" \
        SMOKE_BASE_TS_MS="$SMOKE_BASE_TS_MS" \
        SMOKE_DIRECT_TO_GLOBAL="${SMOKE_DIRECT_TO_GLOBAL:-0}" \
        ./gradlew :scoring-pipeline:runSmokeProducer -q --console=plain
    ) || fail "runSmokeProducer failed"
    pass "Verdicts produced"
}

# Look up a userId's score across all shards. Echoes "<shard_index> <score>"
# for the shard containing the user, or "" if absent. Fails if the user
# appears in more than one shard (single-shard invariant violation).
locate_user_in_shards() {
    local user="$1"
    local found_shard="" found_score=""
    for shard in $(seq 0 $((NUM_SHARDS - 1))); do
        local key="leaderboard:${SMOKE_CONTEST}:shard:${shard}"
        local score
        score="$(redis_cli ZSCORE "$key" "$user" || true)"
        if [ -n "$score" ]; then
            if [ -n "$found_shard" ]; then
                fail "Single-shard invariant violated for ${user}: present in shard ${found_shard} (score=${found_score}) AND shard ${shard} (score=${score})"
            fi
            found_shard="$shard"
            found_score="$score"
        fi
    done
    if [ -n "$found_shard" ]; then
        printf "%s %s\n" "$found_shard" "$found_score"
    fi
}

await_score() {
    local user="$1" expected_score="$2" expected_shard="$3"
    info "Polling ZSCORE for ${user} (expecting score=${expected_score} on shard ${expected_shard})"
    local waited=0
    local result
    while [ "$waited" -lt "$ZSCORE_WAIT_SECONDS" ]; do
        result="$(locate_user_in_shards "$user")"
        if [ -n "$result" ]; then
            local actual_score actual_shard
            actual_shard="$(printf "%s" "$result" | awk '{print $1}')"
            actual_score="$(printf "%s" "$result" | awk '{print $2}')"
            # ZSCORE returns a stringified double; compare integer portion.
            actual_score="${actual_score%%.*}"
            if [ "$actual_score" = "$expected_score" ] && [ "$actual_shard" = "$expected_shard" ]; then
                pass "${user}: zsetScore=${actual_score} on shard ${actual_shard} (after ${waited}s)"
                return 0
            fi
        fi
        sleep 2; waited=$((waited+2))
    done
    fail "${user}: did not converge to score=${expected_score} on shard ${expected_shard} within ${ZSCORE_WAIT_SECONDS}s (last=${result:-<nil>})"
}

# ---------------- cleanup ----------------

cleanup_contest_keys() {
    info "Cleaning up Redis keys for contest=${SMOKE_CONTEST}"
    for shard in $(seq 0 $((NUM_SHARDS - 1))); do
        redis_cli DEL "leaderboard:${SMOKE_CONTEST}:shard:${shard}" >/dev/null
        redis_cli DEL "leaderboard:${SMOKE_CONTEST}::sentinel:shard:${shard}" >/dev/null
    done
    pass "Shard keys deleted (Flink job left RUNNING for next run)"
}

# ---------------- main ----------------

main() {
    check_prereqs
    check_ports
    ensure_flink_job_running
    produce_verdicts

    # SmokeProducer scopes its user/problem ids by the contest id so reruns
    # don't collide with Flink's per-user state from prior runs.
    await_score "userA-${SMOKE_CONTEST}" "$EXPECTED_SCORE_USER_A" "$EXPECTED_SHARD_USER_A"
    await_score "userB-${SMOKE_CONTEST}" "$EXPECTED_SCORE_USER_B" "$EXPECTED_SHARD_USER_B"

    cleanup_contest_keys
    printf "${GREEN}OK${NC} scoring-pipeline smoke green (contest=%s)\n" "$SMOKE_CONTEST"
}

main "$@"
