#!/usr/bin/env bash
# Create / update the OJ Cloud Monitoring alert policies.
#
#   PROJECT_ID=online-judge-hk ./apply-alerts.sh
#
# Idempotent: matches on displayName and updates in place when found.
# Notification channels are intentionally empty in the JSON checked in —
# set NOTIFY_CHANNEL_IDS=projects/.../notificationChannels/...,... to
# inject channel IDs at apply time.
set -euo pipefail

: "${PROJECT_ID:?must set PROJECT_ID}"
: "${NOTIFY_CHANNEL_IDS:=}"
ALERT_DIR="$(cd "$(dirname "$0")/../alerts" && pwd)"

list_existing() {
    gcloud alpha monitoring policies list \
        --project="$PROJECT_ID" \
        --format='value(name,displayName)' 2>/dev/null
}

inject_channels() {
    local src="$1"
    if [[ -z "$NOTIFY_CHANNEL_IDS" ]]; then
        cat "$src"
        return
    fi
    python3 - "$src" "$NOTIFY_CHANNEL_IDS" <<'PY'
import json, sys
doc = json.load(open(sys.argv[1]))
doc["notificationChannels"] = [c.strip() for c in sys.argv[2].split(",") if c.strip()]
print(json.dumps(doc))
PY
}

for f in "$ALERT_DIR"/*.json; do
    name=$(python3 -c "import json; print(json.load(open('$f'))['displayName'])")
    existing=$(list_existing | awk -F'\t' -v n="$name" '$2==n {print $1; exit}')
    rendered=$(mktemp)
    inject_channels "$f" > "$rendered"
    if [[ -n "$existing" ]]; then
        echo "[apply-alerts] updating: $name"
        gcloud alpha monitoring policies update "$existing" \
            --project="$PROJECT_ID" --policy-from-file="$rendered"
    else
        echo "[apply-alerts] creating: $name"
        gcloud alpha monitoring policies create \
            --project="$PROJECT_ID" --policy-from-file="$rendered"
    fi
    rm -f "$rendered"
done

echo "[apply-alerts] done"
