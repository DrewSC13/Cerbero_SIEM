#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"
[[ -f .env ]] || { echo "missing .env; run make dev-init" >&2; exit 1; }
docker compose --env-file .env -f deploy/compose/compose.yaml down --volumes --remove-orphans
find var/raw -mindepth 1 -maxdepth 1 ! -name README.md ! -name .gitignore -exec rm -rf -- {} +
echo "development state reset"
