#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

[[ -f .env ]] || { echo "missing .env; run make dev-init" >&2; exit 1; }

set -a
# shellcheck disable=SC1091
source .env
set +a

: "${CLICKHOUSE_DB:?CLICKHOUSE_DB is required}"
: "${CLICKHOUSE_USER:?CLICKHOUSE_USER is required}"
: "${CLICKHOUSE_PASSWORD:?CLICKHOUSE_PASSWORD is required}"
: "${CLICKHOUSE_NORMALIZER_USER:?CLICKHOUSE_NORMALIZER_USER is required}"
: "${CLICKHOUSE_NORMALIZER_PASSWORD:?CLICKHOUSE_NORMALIZER_PASSWORD is required}"
: "${CLICKHOUSE_DETECTION_USER:?CLICKHOUSE_DETECTION_USER is required}"
: "${CLICKHOUSE_DETECTION_PASSWORD:?CLICKHOUSE_DETECTION_PASSWORD is required}"
: "${CLICKHOUSE_API_USER:?CLICKHOUSE_API_USER is required}"
: "${CLICKHOUSE_API_PASSWORD:?CLICKHOUSE_API_PASSWORD is required}"

for variable in CLICKHOUSE_NORMALIZER_USER CLICKHOUSE_DETECTION_USER CLICKHOUSE_API_USER; do
  value="${!variable}"
  if [[ ! "$value" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
    echo "$variable must be a simple ClickHouse identifier" >&2
    exit 1
  fi
done

if [[ "$CLICKHOUSE_NORMALIZER_USER" == "$CLICKHOUSE_USER" ]]; then
  echo "normalizer ClickHouse login must differ from development admin" >&2
  exit 1
fi
if [[ "$CLICKHOUSE_DETECTION_USER" == "$CLICKHOUSE_USER" || "$CLICKHOUSE_DETECTION_USER" == "$CLICKHOUSE_NORMALIZER_USER" ]]; then
  echo "detection ClickHouse login must differ from development admin and normalizer" >&2
  exit 1
fi
if [[ "$CLICKHOUSE_API_USER" == "$CLICKHOUSE_USER" || "$CLICKHOUSE_API_USER" == "$CLICKHOUSE_NORMALIZER_USER" || "$CLICKHOUSE_API_USER" == "$CLICKHOUSE_DETECTION_USER" ]]; then
  echo "api ClickHouse login must be distinct from admin, normalizer, and detection" >&2
  exit 1
fi

quote_sql_string() {
  printf "%s" "$1" | sed "s/'/''/g"
}

normalizer_user="$(quote_sql_string "$CLICKHOUSE_NORMALIZER_USER")"
normalizer_password="$(quote_sql_string "$CLICKHOUSE_NORMALIZER_PASSWORD")"
detection_user="$(quote_sql_string "$CLICKHOUSE_DETECTION_USER")"
detection_password="$(quote_sql_string "$CLICKHOUSE_DETECTION_PASSWORD")"
api_user="$(quote_sql_string "$CLICKHOUSE_API_USER")"
api_password="$(quote_sql_string "$CLICKHOUSE_API_PASSWORD")"

docker compose --env-file .env -f deploy/compose/compose.yaml exec -T clickhouse \
  clickhouse-client \
    --user "$CLICKHOUSE_USER" \
    --password "$CLICKHOUSE_PASSWORD" \
    --multiquery <<SQL
CREATE USER IF NOT EXISTS
  \`$normalizer_user\`
  IDENTIFIED WITH plaintext_password BY '$normalizer_password';
ALTER USER
  \`$normalizer_user\`
  IDENTIFIED WITH plaintext_password BY '$normalizer_password';
GRANT SELECT, INSERT ON ${CLICKHOUSE_DB}.normalized_events TO \`$normalizer_user\`;

CREATE USER IF NOT EXISTS
  \`$detection_user\`
  IDENTIFIED WITH plaintext_password BY '$detection_password';
ALTER USER
  \`$detection_user\`
  IDENTIFIED WITH plaintext_password BY '$detection_password';
GRANT SELECT ON ${CLICKHOUSE_DB}.normalized_events TO \`$detection_user\`;

CREATE USER IF NOT EXISTS
  \`$api_user\`
  IDENTIFIED WITH plaintext_password BY '$api_password';
ALTER USER
  \`$api_user\`
  IDENTIFIED WITH plaintext_password BY '$api_password';
GRANT SELECT ON ${CLICKHOUSE_DB}.normalized_events TO \`$api_user\`;
SQL

echo "development normalizer + detection + api ClickHouse logins: PASS"
