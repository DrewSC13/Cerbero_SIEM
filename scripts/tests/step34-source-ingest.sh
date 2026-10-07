#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

command -v docker >/dev/null 2>&1 || {
  echo "Docker is required for Step34 source-ingest gate" >&2
  exit 2
}
docker compose version >/dev/null

[[ -f .env ]] || ./scripts/dev/init.sh

export COMPOSE_PROJECT_NAME="${STEP34_SOURCE_PROJECT_NAME:-cerbero-step34-source-ingest}"

cleanup() {
  docker compose --env-file .env -f deploy/compose/compose.yaml \
    down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker compose --env-file .env -f deploy/compose/compose.yaml \
  down --volumes --remove-orphans >/dev/null 2>&1 || true
docker compose --env-file .env -f deploy/compose/compose.yaml up -d nats
./scripts/dev/bootstrap-nats.sh

set -a
# shellcheck disable=SC1091
source .env
set +a
export CERBERO_NATS_URL="nats://127.0.0.1:${NATS_PORT}"

(
  cd services/cerbero-ingest

  go test ./cmd/cerbero-syslog ./cmd/cerbero-journald -count=1

  go test -tags=integration ./internal/ingestapp \
    -run '^TestDevelopmentRuntimeHTTPToJetStream$' -count=1 -v

  go test -tags=integration ./internal/syslogingest \
    -run '^TestStep34SyslogTCPToJetStream$' -count=1 -v

  go test -tags=integration ./internal/journald \
    -run '^TestStep34JournaldExportToJetStream$' -count=1 -v
)

echo "STEP34_JSON_INGEST_PASS"
echo "STEP34_SYSLOG_TCP_INGEST_PASS"
echo "STEP34_JOURNALD_INGEST_PASS"
echo "STEP34_SOURCE_INGEST_PASS"
