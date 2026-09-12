#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"
[[ -f .env ]] || { echo "missing .env; run make dev-init" >&2; exit 1; }
docker compose --env-file .env -f deploy/compose/compose.yaml down --remove-orphans
