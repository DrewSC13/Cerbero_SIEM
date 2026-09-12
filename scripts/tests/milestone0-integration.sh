#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

command -v docker >/dev/null 2>&1 || { echo "Docker is required for Milestone 0 integration tests" >&2; exit 2; }
docker compose version >/dev/null

cleanup() {
  docker compose --env-file .env -f deploy/compose/compose.yaml down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

./scripts/dev/init.sh
docker compose --env-file .env -f deploy/compose/compose.yaml config --quiet
./scripts/dev/up.sh
./scripts/dev/bootstrap-nats.sh
./scripts/dev/health.sh

echo "Milestone 0 infrastructure integration: PASS"
