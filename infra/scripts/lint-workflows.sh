#!/usr/bin/env bash
# Lint every workflow under .github/workflows using the repo's actionlint config.
# Maintainers run this locally before pushing; CI runs the same check on PRs.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CONFIG="${REPO_ROOT}/.github/actionlint.yaml"

if ! command -v actionlint >/dev/null 2>&1; then
  echo "actionlint not installed. Install via one of:"
  echo "  brew install actionlint                                       # macOS"
  echo "  go install github.com/rhysd/actionlint/cmd/actionlint@latest  # any OS"
  exit 1
fi

exec actionlint -config-file "${CONFIG}" "${REPO_ROOT}"/.github/workflows/*.yml
