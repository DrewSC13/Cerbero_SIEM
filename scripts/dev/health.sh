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

readonly HEALTH_MAX_ATTEMPTS=60
readonly HEALTH_RETRY_SECONDS=2

wait_for() {
  local name="$1"
  shift
  local attempt

  for ((attempt = 1; attempt <= HEALTH_MAX_ATTEMPTS; attempt++)); do
    if "$@" >/dev/null 2>&1; then
      return 0
    fi

    if ((attempt == HEALTH_MAX_ATTEMPTS)); then
      echo "$name did not become ready after $HEALTH_MAX_ATTEMPTS attempts" >&2
      return 1
    fi

    sleep "$HEALTH_RETRY_SECONDS"
  done
}

postgres_ready() {
  "${compose[@]}" exec -T postgres /bin/sh -eu -c '
    [ "$(cat /proc/1/comm)" = "postgres" ] &&
    exec pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"
  '
}

clickhouse_ready() {
  "${compose[@]}" exec -T clickhouse clickhouse-client \
    --user "$CLICKHOUSE_USER" \
    --password "$CLICKHOUSE_PASSWORD" \
    --query 'SELECT 1'
}

nats_ready() {
  "${compose[@]}" --profile bootstrap run --rm --entrypoint /bin/sh nats-bootstrap -c \
    'nats --server nats://nats:4222 --user "$NATS_ADMIN_USER" --password "$NATS_ADMIN_PASSWORD" stream ls'
}

echo "waiting for PostgreSQL readiness"
wait_for "PostgreSQL" postgres_ready
echo "PostgreSQL readiness: PASS"

echo "waiting for ClickHouse readiness"
wait_for "ClickHouse" clickhouse_ready
echo "ClickHouse readiness: PASS"

echo "waiting for NATS JetStream readiness"
wait_for "NATS JetStream" nats_ready
echo "NATS JetStream readiness: PASS"

echo "development infrastructure health checks: PASS"
