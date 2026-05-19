#!/usr/bin/env bash
#
# spot-preempt-drain.sh — GCP SPOT preemption drain hook for oj-compute.
#
# Tech-spec §11.2 / roadmap §3.8. A SPOT VM gets 30 s notice from GCP
# before the host reclaims it. This script is invoked from
# /etc/systemd/system/oj-preempt-drain.service (ExecStop=) and POSTs to
# the execution-worker's localhost-only drain endpoint.
#
# On success the worker has:
#   1) stopped consuming submissions.<region>.{pretest,system},
#   2) published a WORKER_PREEMPTED VerdictEvent for every in-flight
#      idempotency row owned by this worker instance,
#   3) flushed Kafka producer batches,
#   4) marked the in-flight idempotency rows poisoned.
#
# Failure modes:
#   - endpoint unreachable      → exit non-zero; systemd logs the error;
#                                 the VM is going down anyway.
#   - 401 (bad token)            → exit non-zero; operator must check
#                                 APP_PREEMPT_TOKEN is in /opt/oj/.env.
#   - HTTP 200, JSON body        → log the drain summary and exit 0.
#
# Requirements: curl (preinstalled on GCE Debian images) + jq (optional,
# only used to pretty-print the response).

set -uo pipefail

# Defaults match application.yml. The .env file on the VM may override
# any of these so we don't have to bake constants in two places.
ENDPOINT_HOST="${APP_PREEMPT_HOST:-127.0.0.1}"
ENDPOINT_PORT="${APP_PREEMPT_PORT:-8083}"
ENDPOINT_PATH="${APP_PREEMPT_PATH:-/actuator/preempt-drain}"
TOKEN="${APP_PREEMPT_TOKEN:-}"
TIMEOUT_S="${APP_PREEMPT_CURL_TIMEOUT:-28}"   # < 30 s preempt budget

if [ -z "$TOKEN" ]; then
    echo "[preempt-drain] APP_PREEMPT_TOKEN not set — skipping (endpoint disabled)" >&2
    # Exit 0: a worker built without the token is not a misconfiguration
    # we want the systemd unit to surface as a failure.
    exit 0
fi

URL="http://${ENDPOINT_HOST}:${ENDPOINT_PORT}${ENDPOINT_PATH}"
TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT

# --max-time is the wall-clock cap for the entire request. We deliberately
# don't retry: a SPOT preempt is one-shot, and a hung drain endpoint isn't
# going to magically unstick on the second curl.
HTTP_CODE="$(curl -sS -o "$TMP" -w '%{http_code}' \
    --max-time "$TIMEOUT_S" \
    -X POST \
    -H "X-Preempt-Token: ${TOKEN}" \
    -H "Content-Length: 0" \
    "$URL" || echo "000")"

case "$HTTP_CODE" in
    200)
        if command -v jq >/dev/null 2>&1; then
            jq -c '.' < "$TMP"
        else
            cat "$TMP"
        fi
        echo
        echo "[preempt-drain] OK"
        exit 0
        ;;
    401)
        echo "[preempt-drain] FAIL: 401 unauthorized — token mismatch" >&2
        exit 2
        ;;
    000)
        echo "[preempt-drain] FAIL: endpoint unreachable at $URL" >&2
        exit 3
        ;;
    *)
        echo "[preempt-drain] FAIL: HTTP $HTTP_CODE from $URL" >&2
        cat "$TMP" >&2 || true
        exit 4
        ;;
esac
