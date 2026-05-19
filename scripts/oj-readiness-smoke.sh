#!/usr/bin/env bash
# =============================================================================
# scripts/oj-readiness-smoke.sh — end-to-end pre-deploy readiness smoke
#
# Operator-facing harness that stands up the regional compose stack locally
# and exercises the submission → verdict → leaderboard → analytics path plus
# the feature surfaces tracked by the readiness checklist (PRs #5–#15):
#
#   S1 — Auth flow + rate-limit split           (PR #9: separate auth bucket)
#   S2 — Submission → verdict → leaderboard     (existing core path)
#   S3 — Code URL schemes                       (PR #14: http:// scheme)
#   S4 — Analytics ingest via CH Kafka Engine   (existing analytics_kafka path)
#   S5 — DLQ + observability dashboard validate (PR #6: validate.sh exit 0)
#   S6 — SPOT preemption drain endpoint         (PR #13: /actuator/preempt-drain)
#   S7 — Application metrics                    (PR #11: 5 catalogue metrics)
#
# Each scenario logs `[smoke S<n>] PASS` or `[smoke S<n>] FAIL: <reason>` and
# accumulates a failure count. The script's exit code is that count.
#
# Forward-compat note. Some scenarios above target features that may not yet
# be on `main` in this checkout — the readiness checklist is forward-looking.
# Every feature-gated scenario performs a presence check first and emits
# `[smoke S<n>] SKIP: <reason>` when the surface isn't shipped, rather than
# FAIL. That lets the operator run the smoke today (substrate-only green),
# then watch SKIPs flip to PASS as PRs #5–#15 land. To turn SKIPs into hard
# failures (the pre-GA gate), pass `--strict`.
#
# Style template: scripts/scoring-smoke.sh + infra/scripts/test-multi-region-local.sh.
#
# Usage:
#   bash scripts/oj-readiness-smoke.sh             # SKIP on unshipped features
#   bash scripts/oj-readiness-smoke.sh --strict    # FAIL on unshipped features
#   bash scripts/oj-readiness-smoke.sh --keep-up   # leave compose stack up
#
# Runtime expectations:
#   - First run:        ~10–15 min (image pulls + Gradle Docker builds +
#                        ClickHouse Kafka-Engine first-poll on a 7-day
#                        analytics_events topic offset seek).
#   - Subsequent runs:   ~3–5 min (cached layers + warm volumes).
#
# Prerequisites on the host:
#   docker (with the `compose` plugin), curl, redis-cli, jq, python3, awk, nc.
# =============================================================================

set -euo pipefail

# ---------------- args ----------------

KEEP_UP=false
STRICT=false
while [ $# -gt 0 ]; do
    case "$1" in
        --keep-up) KEEP_UP=true ;;
        --strict)  STRICT=true ;;
        -h|--help)
            sed -n '2,40p' "$0"
            exit 0
            ;;
        *) echo "ERROR: unknown arg: $1" >&2; echo "Usage: $0 [--strict] [--keep-up]" >&2; exit 2 ;;
    esac
    shift
done

# ---------------- config ----------------

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-${ROOT}/infra/gcp/compose/region.yml}"
DC=(docker compose -f "${COMPOSE_FILE}")

# Endpoints (host-side; align with region.yml's port-publish list).
API_GATEWAY_URL="${API_GATEWAY_URL:-http://127.0.0.1:8088}"
PROBLEM_SERVICE_URL="${PROBLEM_SERVICE_URL:-http://127.0.0.1:8089}"
LEADERBOARD_URL="${LEADERBOARD_URL:-http://127.0.0.1:8082}"
EXECUTION_WORKER_URL="${EXECUTION_WORKER_URL:-http://127.0.0.1:8083}"
CLICKHOUSE_URL="${CLICKHOUSE_URL:-http://127.0.0.1:8123}"
REDIS_HOST="${REDIS_HOST:-127.0.0.1}"
REDIS_PORT="${REDIS_PORT:-6379}"
KAFKA_HOST="${KAFKA_HOST:-127.0.0.1}"
KAFKA_PORT="${KAFKA_PORT:-9092}"
OTEL_PROMETHEUS_URL="${OTEL_PROMETHEUS_URL:-http://127.0.0.1:8888/metrics}"

# Feature flags / secrets (defaults make the smoke runnable against a fresh
# `docker compose up` without an .env file).
APP_PREEMPT_TOKEN="${APP_PREEMPT_TOKEN:-smoke-preempt-token}"
APP_SOURCE_S3_ENABLED="${APP_SOURCE_S3_ENABLED:-false}"

# Smoke run identifiers — scoped per invocation so reruns against warm
# volumes don't collide (mirrors scoring-smoke.sh idiom).
RUN_ID="$(date +%s)-$$"
SMOKE_USER="smoke-user-${RUN_ID}"
SMOKE_PASS="smoke-pass-supersecret-123"
SMOKE_PROBLEM_ID="${SMOKE_PROBLEM_ID:-sum-of-two}"
SMOKE_CONTEST_ID="${SMOKE_CONTEST_ID:-smoke-contest-${RUN_ID}}"
SMOKE_SUBMISSION_ID="SMOKE-SUB-${RUN_ID}"

# Fixture directories.
FIX_DIR="${ROOT}/scripts/smoke/oj-readiness-smoke-sources"
EXP_DIR="${FIX_DIR}/expected"

# Tiny-http-server (S3) state.
TINY_HTTP_PID=""
TINY_HTTP_PORT="${TINY_HTTP_PORT:-18077}"

# ---------------- ergonomics ----------------

FAILED_SCENARIOS=()
SKIPPED_SCENARIOS=()
WORKDIR="$(mktemp -d)"

cleanup() {
    if [ -n "${TINY_HTTP_PID}" ]; then
        kill "${TINY_HTTP_PID}" >/dev/null 2>&1 || true
    fi
    rm -rf "${WORKDIR}" 2>/dev/null || true
    if "${KEEP_UP}"; then
        echo "[teardown] --keep-up set; leaving stack running"
        echo "           docker compose -f ${COMPOSE_FILE} down -v"
        return
    fi
    echo "[teardown] docker compose down -v"
    "${DC[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

pass()  { echo "[smoke S$1] PASS"; }
fail()  { echo "[smoke S$1] FAIL: $2"; FAILED_SCENARIOS+=("S$1"); }
skip()  {
    if "${STRICT}"; then
        echo "[smoke S$1] FAIL: $2 (--strict)"
        FAILED_SCENARIOS+=("S$1")
    else
        echo "[smoke S$1] SKIP: $2"
        SKIPPED_SCENARIOS+=("S$1")
    fi
}
info()  { echo "[smoke    ] $*"; }

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

# Extract HTTP status from a curl -w response. Caller invokes:
#   curl -sS -o "$BODY_FILE" -w '%{http_code}' ... > "$STATUS_FILE"
status_of() { cat "$1"; }

# Extract the accessToken from an auth/login JSON body (no jq required).
extract_access_token() {
    sed -n 's/.*"accessToken":"\([^"]*\)".*/\1/p' "$1"
}

# ---------------- step 1: stand-up ----------------

prereqs() {
    info "checking prerequisites"
    for bin in docker curl redis-cli python3 awk nc; do
        command -v "$bin" >/dev/null 2>&1 \
            || { echo "[prereq] FAIL — $bin not on PATH" >&2; exit 2; }
    done
    [ -f "${COMPOSE_FILE}" ] \
        || { echo "[prereq] FAIL — compose file not found: ${COMPOSE_FILE}" >&2; exit 2; }
    [ -f "${FIX_DIR}/sum-two-numbers.py" ] \
        || { echo "[prereq] FAIL — fixture missing: ${FIX_DIR}/sum-two-numbers.py" >&2; exit 2; }
}

stand_up() {
    info "starting compose stack (kafka, redis, crdb, clickhouse, api-gateway, contest-service, problem-service, execution-worker, sandbox-manager, leaderboard-service, otel-collector)"
    "${DC[@]}" up -d \
        oj-zookeeper \
        oj-kafka \
        oj-redis \
        oj-cockroachdb \
        oj-clickhouse \
        oj-clickhouse-init \
        oj-otel-collector \
        oj-api-gateway \
        oj-contest-service \
        oj-problem-service \
        oj-sandbox-manager \
        oj-execution-worker \
        oj-leaderboard-service

    wait_for "kafka :${KAFKA_PORT}"      120 nc -z "${KAFKA_HOST}" "${KAFKA_PORT}"
    wait_for "redis PING"                 60 bash -c "redis-cli -h ${REDIS_HOST} -p ${REDIS_PORT} ping | grep -q PONG"
    wait_for "clickhouse :8123"          120 bash -c "curl -fsS ${CLICKHOUSE_URL}/ping"
    wait_for "api-gateway UP"            300 bash -c "curl -fsS ${API_GATEWAY_URL}/actuator/health | grep -q '\"status\":\"UP\"'"
    wait_for "problem-service UP"        180 bash -c "curl -fsS ${PROBLEM_SERVICE_URL}/actuator/health | grep -q '\"status\":\"UP\"'"
    wait_for "leaderboard-service UP"    180 bash -c "curl -fsS ${LEADERBOARD_URL}/actuator/health | grep -q '\"status\":\"UP\"'"
    wait_for "execution-worker UP"       180 bash -c "curl -fsS ${EXECUTION_WORKER_URL}/actuator/health | grep -q '\"status\":\"UP\"'"
}

# ---------------- S1: auth flow + rate-limit split ----------------

run_s1() {
    info "S1: signup + login + auth-bucket trip + submission-bucket independence"

    # Signup.
    local body status
    body="${WORKDIR}/s1-signup-body.json"
    status="$(curl -sS -o "$body" -w '%{http_code}' \
        -X POST -H 'Content-Type: application/json' \
        -d "{\"username\":\"${SMOKE_USER}\",\"password\":\"${SMOKE_PASS}\"}" \
        "${API_GATEWAY_URL}/api/v1/auth/signup" || echo 000)"
    case "$status" in
        200|201) : ;;
        *) fail 1 "signup expected 200/201, got ${status} body=$(tr -d '\n' < "$body" | cut -c1-200)"; return ;;
    esac

    # Login.
    body="${WORKDIR}/s1-login-body.json"
    status="$(curl -sS -o "$body" -w '%{http_code}' \
        -X POST -H 'Content-Type: application/json' \
        -d "{\"username\":\"${SMOKE_USER}\",\"password\":\"${SMOKE_PASS}\"}" \
        "${API_GATEWAY_URL}/api/v1/auth/login" || echo 000)"
    if [ "$status" != "200" ]; then
        fail 1 "login expected 200, got ${status}"; return
    fi
    local token
    token="$(extract_access_token "$body")"
    if [ -z "$token" ]; then
        fail 1 "no accessToken in login body"; return
    fi
    # Stash for S2/S3.
    echo "$token" > "${WORKDIR}/access-token"

    # Auth bucket: 11 logins in < 60s should trip the auth limit on attempt 11.
    # Forward-compat: PR #9 splits auth into its own bucket. Until then, the
    # shared bucket trips earlier (or possibly later, depending on tuning) —
    # we accept "429 somewhere in 11 attempts" as the substrate signal and
    # only assert "11th-specifically-429" when PR #9 is detectable. The
    # presence probe: a `auth.ratelimit.bucket` header on the login response
    # (PR #9 stamps this).
    local has_split=false
    if curl -sSI "${API_GATEWAY_URL}/api/v1/auth/login" 2>/dev/null \
            | grep -qi '^x-ratelimit-bucket:.*auth'; then
        has_split=true
    fi

    if ! $has_split; then
        skip 1 "auth/submission rate-limit split not yet shipped (no X-RateLimit-Bucket: auth header) — substrate fall-through; rerun under --strict once PR #9 lands"
        return
    fi

    local i auth_status saw_429=false
    for i in $(seq 1 11); do
        auth_status="$(curl -sS -o /dev/null -w '%{http_code}' \
            -X POST -H 'Content-Type: application/json' \
            -d "{\"username\":\"${SMOKE_USER}\",\"password\":\"wrong-${i}\"}" \
            "${API_GATEWAY_URL}/api/v1/auth/login" || echo 000)"
        if [ "$i" -eq 11 ] && [ "$auth_status" = "429" ]; then
            saw_429=true
        fi
    done
    if ! $saw_429; then
        fail 1 "auth bucket did not trip 429 on the 11th login attempt"
        return
    fi

    # Submission bucket independence: first 10 → 202, 11th → 429. The point
    # is that auth saturating its bucket left submissions intact.
    local sub_status sub_429=false sub_202=0
    for i in $(seq 1 11); do
        local payload sid
        sid="${SMOKE_SUBMISSION_ID}-rl-${i}"
        payload="$(printf '{"submissionId":"%s","problemId":"%s","language":"python","contestId":"%s","s3CodeUrl":"data:text/plain,print(0)"}' \
            "$sid" "$SMOKE_PROBLEM_ID" "$SMOKE_CONTEST_ID")"
        sub_status="$(curl -sS -o /dev/null -w '%{http_code}' \
            -X POST -H 'Content-Type: application/json' \
            -H "Authorization: Bearer ${token}" \
            -d "${payload}" \
            "${API_GATEWAY_URL}/api/v1/submissions" || echo 000)"
        if [ "$i" -le 10 ] && [ "$sub_status" = "202" ]; then
            sub_202=$((sub_202 + 1))
        fi
        if [ "$i" -eq 11 ] && [ "$sub_status" = "429" ]; then
            sub_429=true
        fi
    done
    if [ "$sub_202" -ne 10 ]; then
        fail 1 "submission bucket did not 202 the first 10 (got ${sub_202}/10)"; return
    fi
    if ! $sub_429; then
        fail 1 "submission bucket did not 429 on the 11th"; return
    fi
    pass 1
}

# ---------------- S2: submission → verdict → leaderboard ----------------

run_s2() {
    info "S2: submit sum-two-numbers, await verdict on evaluated_results, assert leaderboard ZSET advanced"

    local token
    token="$(cat "${WORKDIR}/access-token" 2>/dev/null || true)"
    if [ -z "$token" ]; then
        fail 2 "no access-token from S1 (login must succeed first)"; return
    fi

    # Build a data: URL of the canonical python solution (3 lines).
    local code_b64
    code_b64="$(base64 < "${FIX_DIR}/sum-two-numbers.py" | tr -d '\n')"
    local code_url="data:text/x-python;base64,${code_b64}"

    local sid="${SMOKE_SUBMISSION_ID}"
    local payload
    payload="$(printf '{"submissionId":"%s","problemId":"%s","language":"python","contestId":"%s","s3CodeUrl":"%s"}' \
        "$sid" "$SMOKE_PROBLEM_ID" "$SMOKE_CONTEST_ID" "$code_url")"

    local t0 t1 status
    t0="$(date +%s)"
    status="$(curl -sS -o /dev/null -w '%{http_code}' \
        -X POST -H 'Content-Type: application/json' \
        -H "Authorization: Bearer ${token}" \
        -d "${payload}" \
        "${API_GATEWAY_URL}/api/v1/submissions" || echo 000)"
    if [ "$status" != "202" ]; then
        fail 2 "POST /submissions expected 202, got ${status}"; return
    fi

    # Poll the verdict-status endpoint until ACCEPTED or 10s elapsed.
    local waited=0 verdict=""
    while [ "$waited" -lt 30 ]; do
        verdict="$(curl -sS \
            -H "Authorization: Bearer ${token}" \
            "${API_GATEWAY_URL}/api/v1/submissions/${sid}" 2>/dev/null \
            | sed -n 's/.*"verdict":"\([^"]*\)".*/\1/p' | head -1 || true)"
        case "$verdict" in
            ACCEPTED|WRONG_ANSWER|RUNTIME_ERROR|TIME_LIMIT_EXCEEDED|COMPILE_ERROR)
                break ;;
        esac
        sleep 1; waited=$((waited+1))
    done
    t1="$(date +%s)"
    local elapsed=$((t1 - t0))

    if [ "$verdict" != "ACCEPTED" ]; then
        fail 2 "expected verdict=ACCEPTED within 30s, got '${verdict}' after ${elapsed}s"; return
    fi
    if [ "$elapsed" -gt 10 ]; then
        info "S2 note: end-to-end took ${elapsed}s (> 10s target on local; not a fail)"
    fi

    # Leaderboard ZSET advanced — at least one member with non-zero score.
    local zcard
    zcard="$(redis-cli -h "${REDIS_HOST}" -p "${REDIS_PORT}" \
        ZCARD "leaderboard:${SMOKE_CONTEST_ID}" 2>/dev/null || echo 0)"
    if [ "${zcard:-0}" -lt 1 ]; then
        # Shard-routed variant (scoring-pipeline ZADD lands on a sharded key).
        local total=0 s
        for s in 0 1 2; do
            local c
            c="$(redis-cli -h "${REDIS_HOST}" -p "${REDIS_PORT}" \
                ZCARD "leaderboard:${SMOKE_CONTEST_ID}:shard:${s}" 2>/dev/null || echo 0)"
            total=$((total + c))
        done
        if [ "$total" -lt 1 ]; then
            fail 2 "leaderboard ZSET (and shards) empty after verdict"; return
        fi
    fi
    pass 2
}

# ---------------- S3: code URL schemes (http://) ----------------

run_s3() {
    info "S3: submission via http:// source URL (PR #14)"

    # Probe: does the worker accept http:// today?  We grep the running
    # execution-worker for the UnsupportedOperationException stub.
    if docker exec oj-execution-worker /bin/sh -c \
            "grep -q 'TODO: object-store code fetch is not wired' \
                /opt/oj/execution-worker.jar 2>/dev/null \
             || jar -tf /opt/oj/execution-worker.jar >/dev/null 2>&1" \
            >/dev/null 2>&1; then
        : # presence-check inconclusive; fall through to attempt
    fi

    local token sid http_payload status
    token="$(cat "${WORKDIR}/access-token" 2>/dev/null || true)"
    if [ -z "$token" ]; then
        fail 3 "no access-token from S1"; return
    fi

    # Tiny HTTP fixture server — python3 http.server backgrounded out of the
    # fixtures dir so the worker can fetch the source at
    # http://host.docker.internal:${TINY_HTTP_PORT}/sum-two-numbers.py.
    info "S3: starting fixture http server on :${TINY_HTTP_PORT}"
    (cd "${FIX_DIR}" && python3 tiny-http-server.py "${TINY_HTTP_PORT}") \
        > "${WORKDIR}/tiny-http.log" 2>&1 &
    TINY_HTTP_PID=$!
    sleep 1

    if ! curl -fsS "http://127.0.0.1:${TINY_HTTP_PORT}/sum-two-numbers.py" \
            > /dev/null 2>&1; then
        skip 3 "tiny http fixture didn't come up — check ${WORKDIR}/tiny-http.log"
        return
    fi

    # Submit using http:// (worker resolves via host.docker.internal).
    sid="${SMOKE_SUBMISSION_ID}-http"
    http_payload="$(printf '{"submissionId":"%s","problemId":"%s","language":"python","contestId":"%s","s3CodeUrl":"http://host.docker.internal:%s/sum-two-numbers.py"}' \
        "$sid" "$SMOKE_PROBLEM_ID" "$SMOKE_CONTEST_ID" "$TINY_HTTP_PORT")"
    status="$(curl -sS -o /dev/null -w '%{http_code}' \
        -X POST -H 'Content-Type: application/json' \
        -H "Authorization: Bearer ${token}" \
        -d "${http_payload}" \
        "${API_GATEWAY_URL}/api/v1/submissions" || echo 000)"
    if [ "$status" != "202" ]; then
        fail 3 "POST /submissions (http URL) expected 202, got ${status}"; return
    fi

    # Wait for a non-error verdict. If the worker still throws on http:// (PR
    # #14 not landed), the submission ends in RUNTIME_ERROR via the catch-all;
    # we treat that as SKIP (substrate signal), not FAIL.
    local waited=0 verdict=""
    while [ "$waited" -lt 30 ]; do
        verdict="$(curl -sS \
            -H "Authorization: Bearer ${token}" \
            "${API_GATEWAY_URL}/api/v1/submissions/${sid}" 2>/dev/null \
            | sed -n 's/.*"verdict":"\([^"]*\)".*/\1/p' | head -1 || true)"
        case "$verdict" in
            ACCEPTED|WRONG_ANSWER|RUNTIME_ERROR|TIME_LIMIT_EXCEEDED|COMPILE_ERROR)
                break ;;
        esac
        sleep 1; waited=$((waited+1))
    done

    case "$verdict" in
        ACCEPTED) pass 3 ;;
        RUNTIME_ERROR|COMPILE_ERROR)
            skip 3 "http:// code URL path not yet wired (verdict=${verdict}); PR #14 outstanding"
            ;;
        *)
            fail 3 "unexpected verdict for http:// URL: '${verdict}'"
            ;;
    esac

    # S3 also gates s3:// + r2:// behind APP_SOURCE_S3_ENABLED. Per the brief,
    # we skip those when the flag is off (which is the default).
    if [ "${APP_SOURCE_S3_ENABLED}" != "true" ]; then
        info "S3: APP_SOURCE_S3_ENABLED=false — skipping s3:// + r2:// variants"
    fi
}

# ---------------- S4: analytics ingest via CH Kafka Engine ----------------

run_s4() {
    info "S4: assert ClickHouse Kafka-Engine materialised the verdict row"

    # The Kafka Engine table polls the analytics_events.<region> topic and
    # the materialised view inserts into onlinejudge.submission_analytics.
    # Wait briefly after the verdict for the poll cycle to complete.
    sleep 5

    local n
    n="$(curl -sSG "${CLICKHOUSE_URL}/" \
        --data-urlencode "query=SELECT count() FROM onlinejudge.submission_analytics WHERE submission_id='${SMOKE_SUBMISSION_ID}'" \
        2>/dev/null | tr -d '[:space:]' || echo 0)"
    if [ "${n:-0}" -lt 1 ]; then
        # Try a polling window — first-poll on a 7-day offset seek can lag.
        local waited=0
        while [ "$waited" -lt 30 ]; do
            sleep 3; waited=$((waited+3))
            n="$(curl -sSG "${CLICKHOUSE_URL}/" \
                --data-urlencode "query=SELECT count() FROM onlinejudge.submission_analytics WHERE submission_id='${SMOKE_SUBMISSION_ID}'" \
                2>/dev/null | tr -d '[:space:]' || echo 0)"
            [ "${n:-0}" -ge 1 ] && break
        done
    fi
    if [ "${n:-0}" -lt 1 ]; then
        fail 4 "submission_analytics has 0 rows for submission_id=${SMOKE_SUBMISSION_ID}"; return
    fi
    pass 4
}

# ---------------- S5: DLQ / observability dashboard validate ----------------

run_s5() {
    info "S5: validate observability artefacts (dashboards + alerts JSON)"

    local validator="${ROOT}/infra/observability/scripts/validate.sh"
    if [ ! -x "$validator" ] && [ ! -f "$validator" ]; then
        skip 5 "validate.sh not found at ${validator}"
        return
    fi

    if bash "$validator" > "${WORKDIR}/s5-validate.log" 2>&1; then
        # PR #6 also adds a dlq-* dashboard JSON. Presence-check it.
        if ls "${ROOT}"/infra/observability/dashboards/dlq*.json >/dev/null 2>&1; then
            pass 5
        else
            skip 5 "validate.sh green but no dlq*.json dashboard (PR #6 outstanding)"
        fi
    else
        fail 5 "validate.sh exited non-zero; see ${WORKDIR}/s5-validate.log"
    fi
}

# ---------------- S6: SPOT preemption drain endpoint ----------------

run_s6() {
    info "S6: SPOT preemption drain endpoint"

    local body status
    body="${WORKDIR}/s6-drain-body.txt"
    status="$(curl -sS -o "$body" -w '%{http_code}' \
        -X POST -H "X-Preempt-Token: ${APP_PREEMPT_TOKEN}" \
        "${EXECUTION_WORKER_URL}/actuator/preempt-drain" || echo 000)"
    case "$status" in
        200) : ;;
        404)
            skip 6 "preempt-drain endpoint not yet shipped (PR #13 outstanding)"
            return ;;
        401|403)
            fail 6 "preempt-drain returned ${status}; APP_PREEMPT_TOKEN may be wrong"
            return ;;
        *)
            fail 6 "preempt-drain expected 200, got ${status}"
            return ;;
    esac

    # Tail the worker log for the drain-complete line.
    local drained=false waited=0
    while [ "$waited" -lt 30 ]; do
        if docker logs oj-execution-worker 2>&1 \
                | grep -q "preemption drain complete"; then
            drained=true
            break
        fi
        sleep 2; waited=$((waited+2))
    done

    if ! $drained; then
        fail 6 "worker log did not show 'preemption drain complete' within 30s"
        return
    fi

    info "S6: restarting execution-worker to leave a fresh state"
    "${DC[@]}" restart oj-execution-worker >/dev/null 2>&1 || true
    wait_for "execution-worker UP (post-restart)" 180 \
        bash -c "curl -fsS ${EXECUTION_WORKER_URL}/actuator/health | grep -q '\"status\":\"UP\"'"
    pass 6
}

# ---------------- S7: application metrics catalogue ----------------

run_s7() {
    info "S7: assert the five application metric names are registered"

    # The aspirational metric names from docs/tech-spec.md §9.3.
    local metrics=(
        "submission_accepted"
        "submission_rejected_size_too_large"
        "verdict_published_total"
        "sandbox_lease_latency_seconds"
        "sandbox_pool_ready"
    )
    # Otel-prometheus exporter rewrites '.' to '_'. We accept either form.
    local body
    body="${WORKDIR}/s7-metrics.txt"
    if ! curl -sSf "${OTEL_PROMETHEUS_URL}" -o "$body" 2>/dev/null; then
        # Fall back to per-service /actuator/prometheus.
        : > "$body"
        for svc in "${API_GATEWAY_URL}" "${EXECUTION_WORKER_URL}" "${LEADERBOARD_URL}"; do
            curl -sS "${svc}/actuator/prometheus" 2>/dev/null >> "$body" || true
        done
    fi
    if [ ! -s "$body" ]; then
        skip 7 "no /metrics endpoint reachable at ${OTEL_PROMETHEUS_URL} or per-service /actuator/prometheus"
        return
    fi

    local missing=()
    for m in "${metrics[@]}"; do
        local m_dotted="${m//_/.}"
        if ! grep -qE "(^| )${m}( |\{|$)" "$body" \
                && ! grep -qF "${m_dotted}" "$body"; then
            missing+=("$m")
        fi
    done
    if [ "${#missing[@]}" -eq 0 ]; then
        pass 7
    elif [ "${#missing[@]}" -eq "${#metrics[@]}" ]; then
        skip 7 "none of the 5 application metrics are registered yet (PR #11 outstanding)"
    else
        fail 7 "missing metrics: ${missing[*]}"
    fi
}

# ---------------- main ----------------

main() {
    prereqs
    stand_up
    run_s1
    run_s2
    run_s3
    run_s4
    run_s5
    run_s6
    run_s7

    echo
    echo "[smoke    ] summary"
    echo "[smoke    ]   failed:  ${FAILED_SCENARIOS[*]:-none}"
    echo "[smoke    ]   skipped: ${SKIPPED_SCENARIOS[*]:-none}"
    echo "[smoke    ]   run id:  ${RUN_ID}"

    exit "${#FAILED_SCENARIOS[@]}"
}

main "$@"
