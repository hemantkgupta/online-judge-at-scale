#!/usr/bin/env bash
# infra/scripts/crdb-multiregion-init.sh
#
# Run ONCE per cluster, on either region VM, after both CRDB nodes have joined
# and `cockroach init` has completed. Declares the cluster's PRIMARY REGION
# and adds the secondary region. After this script succeeds, api-gateway can
# boot and run Flyway V9, which assumes a multi-region cluster.
#
# Usage:
#   sudo bash infra/scripts/crdb-multiregion-init.sh <primary_region> <secondary_region>
#
# Example:
#   sudo bash infra/scripts/crdb-multiregion-init.sh asia-south1 us-central1
#
# Idempotent — safe to re-run:
#   * CREATE DATABASE IF NOT EXISTS is a no-op on existing database.
#   * SET PRIMARY REGION on the same region is a no-op.
#   * ADD REGION on an already-added region errors; we tolerate that explicitly.
#
# Pre-flight assumptions:
#   * `cockroach init` has already been run (the cluster is initialised).
#   * The CRDB container is named `oj-cockroachdb` (override with CRDB_CONTAINER).
#   * On macOS / dev machines Docker Desktop doesn't need sudo; on the GCP VMs
#     the operator calls this via `sudo bash ...` and that sudo is what reaches
#     docker. The script picks the right docker invocation: `sudo docker` if
#     sudo is on PATH (production), plain `docker` if the operator overrode
#     via DOCKER=docker env var (dev / local testing).

set -euo pipefail

PRIMARY="${1:?usage: crdb-multiregion-init.sh <primary> <secondary>}"
SECONDARY="${2:?usage: crdb-multiregion-init.sh <primary> <secondary>}"

CRDB_CONTAINER="${CRDB_CONTAINER:-oj-cockroachdb}"
DB_NAME="${DB_NAME:-onlinejudge}"

# Docker invocation prefix. On the GCP VMs the operator runs this via
# `sudo bash crdb-multiregion-init.sh ...` so sudo is already in scope; on
# macOS / dev machines Docker Desktop doesn't need sudo. Default tries sudo
# only if available; override with DOCKER=docker for the dev case.
if [ -z "${DOCKER:-}" ]; then
    if command -v sudo >/dev/null 2>&1 && sudo -n true 2>/dev/null; then
        DOCKER="sudo docker"
    else
        DOCKER="docker"
    fi
fi

run_sql() {
    local sql="$1"
    $DOCKER exec -i "${CRDB_CONTAINER}" cockroach sql --insecure --execute "${sql}"
}

# Same as run_sql but tolerates ALTER ADD REGION on an already-added region.
# CRDB's error string contains "already in the database"; we grep for that
# and exit 0 if so, otherwise re-raise.
run_sql_tolerant() {
    local sql="$1"
    local out
    if out=$($DOCKER exec -i "${CRDB_CONTAINER}" cockroach sql --insecure --execute "${sql}" 2>&1); then
        printf '%s\n' "$out"
        return 0
    fi
    if printf '%s' "$out" | grep -q "already.*database\|already.*region"; then
        printf '[crdb-init] (tolerated: region already configured)\n'
        return 0
    fi
    printf '%s\n' "$out" >&2
    return 1
}

echo "[crdb-init] ensuring database ${DB_NAME} exists"
run_sql "CREATE DATABASE IF NOT EXISTS ${DB_NAME};"

echo "[crdb-init] declaring PRIMARY REGION=${PRIMARY} on database ${DB_NAME}"
run_sql_tolerant "ALTER DATABASE ${DB_NAME} SET PRIMARY REGION '${PRIMARY}';"

echo "[crdb-init] adding secondary region ${SECONDARY}"
run_sql_tolerant "ALTER DATABASE ${DB_NAME} ADD REGION '${SECONDARY}';"

# SURVIVE REGION FAILURE requires 3+ regions; with 2 it stays at the default
# SURVIVE ZONE FAILURE. Not configurable here; document in the multi-region
# runbook for the operator who scales to a 3rd region.

echo "[crdb-init] cluster regions on database ${DB_NAME}:"
run_sql "SHOW REGIONS FROM DATABASE ${DB_NAME};"

echo "[crdb-init] done"
