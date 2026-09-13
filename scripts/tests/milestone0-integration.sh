#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

command -v docker >/dev/null 2>&1 || { echo "Docker is required for integration tests" >&2; exit 2; }
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
./scripts/tests/raw-preservation-postgres.sh

set -a
# shellcheck disable=SC1091
source .env
set +a
export CERBERO_NATS_URL="nats://127.0.0.1:${NATS_PORT}"

(
  cd services/cerbero-ingest
  go test -tags=integration ./internal/eventbus -run '^TestJetStreamAcceptorIntegration$' -count=1
  go test -tags=integration ./internal/ingestapp -run '^TestDevelopmentRuntime(HTTPToJetStream|NATSOutageRejectsAcceptance)$' -count=1
)

echo "Milestone 2 NATS-outage integration: PASS"
echo "Milestone 2 JSON/HTTP durable-ingest runtime integration: PASS"
