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
: "${POSTGRES_NORMALIZER_USER:?POSTGRES_NORMALIZER_USER is required}"
: "${POSTGRES_NORMALIZER_PASSWORD:?POSTGRES_NORMALIZER_PASSWORD is required}"
: "${POSTGRES_WORKER_USER:?POSTGRES_WORKER_USER is required}"
: "${POSTGRES_WORKER_PASSWORD:?POSTGRES_WORKER_PASSWORD is required}"

if [[ "$POSTGRES_RAW_PRESERVER_USER" == "$POSTGRES_USER" ]]; then
  echo "raw-preserver PostgreSQL login must differ from development admin" >&2
  exit 1
fi

if [[ "$POSTGRES_NORMALIZER_USER" == "$POSTGRES_USER" ]]; then
  echo "normalizer PostgreSQL login must differ from development admin" >&2
  exit 1
fi

if [[ "$POSTGRES_NORMALIZER_USER" == "$POSTGRES_RAW_PRESERVER_USER" ]]; then
  echo "normalizer PostgreSQL login must differ from raw-preserver login" >&2
  exit 1
fi

if [[ "$POSTGRES_WORKER_USER" == "$POSTGRES_USER" ]]; then
  echo "worker PostgreSQL login must differ from development admin" >&2
  exit 1
fi

if [[ "$POSTGRES_WORKER_USER" == "$POSTGRES_RAW_PRESERVER_USER" ]]; then
  echo "worker PostgreSQL login must differ from raw-preserver login" >&2
  exit 1
fi

if [[ "$POSTGRES_WORKER_USER" == "$POSTGRES_NORMALIZER_USER" ]]; then
  echo "worker PostgreSQL login must differ from normalizer login" >&2
  exit 1
fi

docker compose --env-file .env -f deploy/compose/compose.yaml exec -T postgres \
  psql \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    --set ON_ERROR_STOP=1 \
    --set app_user="$POSTGRES_RAW_PRESERVER_USER" \
    --set app_password="$POSTGRES_RAW_PRESERVER_PASSWORD" \
    --set normalizer_user="$POSTGRES_NORMALIZER_USER" \
    --set normalizer_password="$POSTGRES_NORMALIZER_PASSWORD" \
    --set worker_user="$POSTGRES_WORKER_USER" \
    --set worker_password="$POSTGRES_WORKER_PASSWORD" <<'SQL'
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

SELECT format('CREATE ROLE %I LOGIN', :'normalizer_user')
WHERE NOT EXISTS (
    SELECT 1 FROM pg_roles WHERE rolname = :'normalizer_user'
)
\gexec

SELECT format(
    'ALTER ROLE %I WITH LOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
    :'normalizer_user',
    :'normalizer_password'
)
\gexec

SELECT format(
    'GRANT cerbero_normalizer TO %I',
    :'normalizer_user'
)
\gexec

SELECT format('CREATE ROLE %I LOGIN', :'worker_user')
WHERE NOT EXISTS (
    SELECT 1 FROM pg_roles WHERE rolname = :'worker_user'
)
\gexec

SELECT format(
    'ALTER ROLE %I WITH LOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
    :'worker_user',
    :'worker_password'
)
\gexec

SELECT format(
    'GRANT cerbero_worker TO %I',
    :'worker_user'
)
\gexec
SQL

echo "development raw-preserver + normalizer + worker PostgreSQL logins: PASS"
