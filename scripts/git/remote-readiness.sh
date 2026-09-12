#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

git diff --quiet || { echo "working tree has unstaged changes" >&2; exit 1; }
git diff --cached --quiet || { echo "index has staged changes" >&2; exit 1; }
if grep -q '@OWNER' CODEOWNERS; then
  echo "CODEOWNERS still contains @OWNER; run scripts/git/configure-codeowners.sh @your-owner before first remote push" >&2
  exit 1
fi
make ci
if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  make integration
else
  echo "Docker unavailable: integration gate must run on a Docker-capable workstation or CI before push" >&2
  exit 1
fi

echo "remote readiness: PASS"
