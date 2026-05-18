#!/usr/bin/env bash
# Validate the OTel observability artefacts before applying them to GCP.
#
#   ./validate.sh
#
# Checks:
#   * region.yml YAML parses and the x-otel-defaults anchor resolves on every
#     JVM service block (each service ends up with the same OTEL_* defaults
#     plus its own OTEL_SERVICE_NAME).
#   * otel-collector-config.yaml is valid OTLP-collector config (pipeline
#     graph builds). Uses `otelcol-contrib --dry-run` if the binary is on
#     $PATH; otherwise falls back to a YAML-only parse.
#   * Every dashboard / alert JSON parses and has the expected top-level
#     `displayName` field. (Cloud Monitoring's full JSON schema isn't
#     checkable offline — this is the cheapest smoke test.)
#
# Exits non-zero on the first failure.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
COMPOSE="$ROOT/infra/gcp/compose/region.yml"
OTEL_CFG="$ROOT/infra/gcp/compose/otel-collector-config.yaml"
DASH_DIR="$ROOT/infra/observability/dashboards"
ALERT_DIR="$ROOT/infra/observability/alerts"

echo "[validate] region.yml YAML + anchor resolution"
python3 - "$COMPOSE" <<'PY'
import sys, yaml
with open(sys.argv[1]) as f:
    doc = yaml.safe_load(f)
required = {
    "OTEL_JAVAAGENT_ENABLED",
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "OTEL_EXPORTER_OTLP_PROTOCOL",
    "OTEL_METRICS_EXPORTER",
    "OTEL_TRACES_EXPORTER",
    "OTEL_LOGS_EXPORTER",
    "OTEL_TRACES_SAMPLER",
    "OTEL_TRACES_SAMPLER_ARG",
    "OTEL_RESOURCE_ATTRIBUTES",
    "OTEL_SERVICE_NAME",
}
jvm_services = [
    "oj-api-gateway", "oj-problem-service", "oj-contest-service",
    "oj-leaderboard-service", "oj-sandbox-manager", "oj-execution-worker",
]
fail = False
for svc in jvm_services:
    env = doc["services"][svc].get("environment") or {}
    missing = required - env.keys()
    if missing:
        print(f"  FAIL  {svc} missing: {sorted(missing)}")
        fail = True
    elif env["OTEL_SERVICE_NAME"] != svc:
        print(f"  FAIL  {svc} OTEL_SERVICE_NAME={env['OTEL_SERVICE_NAME']!r} should equal service name")
        fail = True
    else:
        print(f"  ok    {svc}")
if fail:
    sys.exit(1)
PY

echo "[validate] otel-collector-config.yaml"
if command -v otelcol-contrib >/dev/null 2>&1; then
    otelcol-contrib validate --config="$OTEL_CFG"
    echo "  ok    otelcol-contrib validate"
else
    python3 -c "import yaml; yaml.safe_load(open('$OTEL_CFG'))"
    echo "  ok    YAML parse only (install otelcol-contrib for pipeline-graph check)"
fi

echo "[validate] dashboards/*.json"
for f in "$DASH_DIR"/*.json; do
    python3 -c "
import json, sys
d = json.load(open('$f'))
assert 'displayName' in d, 'missing displayName'
assert 'mosaicLayout' in d, 'missing mosaicLayout'
print('  ok   ', '$(basename "$f")')
"
done

echo "[validate] alerts/*.json"
for f in "$ALERT_DIR"/*.json; do
    python3 -c "
import json, sys
d = json.load(open('$f'))
assert 'displayName' in d, 'missing displayName'
assert 'conditions' in d and d['conditions'], 'missing/empty conditions'
assert 'notificationChannels' in d, 'missing notificationChannels (use [] until channels are provisioned)'
print('  ok   ', '$(basename "$f")')
"
done

echo "[validate] done"
