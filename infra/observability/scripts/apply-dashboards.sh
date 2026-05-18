#!/usr/bin/env bash
# Create / update the OJ Cloud Monitoring dashboards.
#
#   gcloud auth application-default login   # one-time
#   PROJECT_ID=online-judge-hk ./apply-dashboards.sh
#
# Idempotent — if a dashboard with the same displayName already exists,
# it is updated in place (via gcloud monitoring dashboards update).
set -euo pipefail

: "${PROJECT_ID:?must set PROJECT_ID (e.g. online-judge-hk)}"
DASH_DIR="$(cd "$(dirname "$0")/../dashboards" && pwd)"

list_existing() {
    gcloud monitoring dashboards list \
        --project="$PROJECT_ID" \
        --format='value(name,displayName)' 2>/dev/null
}

for f in "$DASH_DIR"/*.json; do
    name=$(python3 -c "import json; print(json.load(open('$f'))['displayName'])")
    existing=$(list_existing | awk -F'\t' -v n="$name" '$2==n {print $1; exit}')
    if [[ -n "$existing" ]]; then
        echo "[apply-dashboards] updating: $name"
        gcloud monitoring dashboards update "$existing" \
            --project="$PROJECT_ID" --config-from-file="$f"
    else
        echo "[apply-dashboards] creating: $name"
        gcloud monitoring dashboards create \
            --project="$PROJECT_ID" --config-from-file="$f"
    fi
done

echo "[apply-dashboards] done"
