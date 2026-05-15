#!/usr/bin/env bash
# =============================================================================
# Build the api-gateway + execution-worker Docker images on your Mac (ARM64)
# for the GCP VMs (linux/amd64), and push to Artifact Registry.
#
# Run from the repo root or from inside infra/gcp/images/. Either works:
#   ./infra/gcp/images/push-images.sh
#   cd infra/gcp/images && ./push-images.sh
#
# Pre-requisites (one-time):
#   * `gcloud auth login` succeeded.
#   * `tofu apply` from infra/gcp/terraform/ completed (so the Artifact
#     Registry repo exists). This script reads `tofu output` for the URL.
#   * `docker` (Desktop or OrbStack) is running on the Mac, with `buildx`
#     available (it ships with Docker Desktop / OrbStack by default).
#
# What it does:
#   1. Reads `artifact_registry_url` from `tofu output`.
#   2. Configures Docker to authenticate to that AR host via gcloud.
#   3. For each service: `docker buildx build --platform=linux/amd64 ...
#      --push`.
#
# Cost note: ~2 GB of images pushed to AR storage ≈ $0.20/mo idle. Pulls
# from VMs in the same region are free; the only paid bandwidth is your
# Mac → GCP push.
# =============================================================================

set -euo pipefail

# Resolve repo root regardless of where this script is invoked from.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "${SCRIPT_DIR}/../../.." && pwd )"
TERRAFORM_DIR="${REPO_ROOT}/infra/gcp/terraform"

# Services to build. Add more here (e.g. leaderboard-service) if you need them
# in the cloud-side compose later.
SERVICES=("api-gateway" "execution-worker")

# ---------- discover the AR URL ---------------------------------------------

if [[ -n "${AR_URL:-}" ]]; then
    echo "[push-images] using AR_URL from environment: ${AR_URL}"
else
    if [[ ! -d "${TERRAFORM_DIR}/.terraform" ]]; then
        echo "[push-images] ERROR: ${TERRAFORM_DIR} has no .terraform/."
        echo "             Run `cd ${TERRAFORM_DIR} && tofu init && tofu apply` first."
        exit 1
    fi
    AR_URL="$( cd "${TERRAFORM_DIR}" && tofu output -raw artifact_registry_url )"
    echo "[push-images] discovered AR URL: ${AR_URL}"
fi

# AR_URL looks like: asia-south1-docker.pkg.dev/online-judge-hk/oj-images
AR_HOST="${AR_URL%%/*}"

# ---------- authenticate docker to AR ---------------------------------------

echo "[push-images] configuring docker auth for ${AR_HOST}"
gcloud auth configure-docker "${AR_HOST}" --quiet

# Sanity: do we have buildx?
if ! docker buildx version >/dev/null 2>&1; then
    echo "[push-images] ERROR: docker buildx is not available."
    echo "             Install Docker Desktop / OrbStack which both ship buildx."
    exit 1
fi

# Ensure there's a builder that supports linux/amd64 output.
if ! docker buildx inspect oj-cross >/dev/null 2>&1; then
    docker buildx create --name oj-cross --use >/dev/null
    docker buildx inspect --bootstrap oj-cross >/dev/null
else
    docker buildx use oj-cross
fi

# ---------- build + push each service ---------------------------------------

cd "${REPO_ROOT}"

for service in "${SERVICES[@]}"; do
    if [[ ! -f "${service}/Dockerfile" ]]; then
        echo "[push-images] SKIP ${service}: no Dockerfile at ${service}/Dockerfile"
        continue
    fi

    IMAGE_TAG="${AR_URL}/${service}:latest"
    IMAGE_SHA="${AR_URL}/${service}:$(git rev-parse --short HEAD 2>/dev/null || date +%Y%m%d-%H%M%S)"

    echo
    echo "[push-images] ====================================================="
    echo "[push-images] building ${service} → ${IMAGE_TAG}"
    echo "[push-images] ====================================================="

    docker buildx build \
        --platform=linux/amd64 \
        --file "${service}/Dockerfile" \
        --tag  "${IMAGE_TAG}" \
        --tag  "${IMAGE_SHA}" \
        --push \
        --progress=plain \
        .

    echo "[push-images] ✓ ${service} pushed as ${IMAGE_TAG}"
    echo "[push-images]   also tagged ${IMAGE_SHA}"
done

echo
echo "[push-images] done. Verify with:"
echo "  gcloud artifacts docker images list ${AR_URL}"
