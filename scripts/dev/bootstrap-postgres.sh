#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

[[ -f .env ]] || { echo "missing .env; run make dev-init" >&2; exit 1; }

set -a
# shellcheck disable=SC1091
source .env
set +a

: "${POSTGRES_DB:?POSTGRES_DB is required}"
: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${POSTGRES_RAW_PRESERVER_USER:?POSTGRES_RAW_PRESERVER_USER is required}"
: "${POSTGRES_RAW_PRESERVER_PASSWORD:?POSTGRES_RAW_PRESERVER_PASSWORD is required}"

if [[ "$POSTGRES_RAW_PRESERVER_USER" == "$POSTGRES_USER" ]]; then
  echo "raw-preserver PostgreSQL login must differ from development admin" >&2
  exit 1
fi

docker compose --env-file .env -f deploy/compose/compose.yaml exec -T postgres \
  psql \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    --set ON_ERROR_STOP=1 \
    --set app_user="$POSTGRES_RAW_PRESERVER_USER" \
    --set app_password="$POSTGRES_RAW_PRESERVER_PASSWORD" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN', :'app_user')
WHERE NOT EXISTS (
    SELECT 1 FROM pg_roles WHERE rolname = :'app_user'
)
\gexec

SELECT format(
    'ALTER ROLE %I WITH LOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
    :'app_user',
    :'app_password'
)
\gexec

SELECT format(
    'GRANT cerbero_raw_preserver TO %I',
    :'app_user'
)
\gexec
SQL

echo "development raw-preserver PostgreSQL login: PASS"
