#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"
[[ -f .env ]] || { echo "missing .env; run make dev-init" >&2; exit 1; }
set -a
# shellcheck disable=SC1091
source .env
set +a
compose=(docker compose --env-file .env -f deploy/compose/compose.yaml)

"${compose[@]}" exec -T postgres pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"
"${compose[@]}" exec -T clickhouse clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --query 'SELECT 1'
"${compose[@]}" --profile bootstrap run --rm --entrypoint /bin/sh nats-bootstrap -c \
  'nats --server nats://nats:4222 --user "$NATS_ADMIN_USER" --password "$NATS_ADMIN_PASSWORD" server ping --count 1 >/dev/null && nats --server nats://nats:4222 --user "$NATS_ADMIN_USER" --password "$NATS_ADMIN_PASSWORD" stream ls'

echo "development infrastructure health checks: PASS"
