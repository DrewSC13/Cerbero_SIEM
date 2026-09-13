#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

if [[ ! -f .env ]]; then
  echo ".env is required; run 'make dev-init' first" >&2
  exit 2
fi

set -a
# shellcheck disable=SC1091
source .env
set +a

compose=(docker compose --env-file .env -f deploy/compose/compose.yaml)
psql_cmd=(
  "${compose[@]}" exec -T postgres
  psql -X -v ON_ERROR_STOP=1
  -U "$POSTGRES_USER"
  -d "$POSTGRES_DB"
)

"${psql_cmd[@]}" < migrations/postgres/000002_raw_preservation.sql >/dev/null

"${psql_cmd[@]}" >/dev/null <<'SQL'
DO $checks$
BEGIN
    IF to_regclass('system.raw_objects') IS NULL THEN
        RAISE EXCEPTION 'system.raw_objects is missing';
    END IF;
    IF to_regclass('system.processed_messages') IS NULL THEN
        RAISE EXCEPTION 'system.processed_messages is missing';
    END IF;
    IF to_regclass('system.outbox') IS NULL THEN
        RAISE EXCEPTION 'system.outbox is missing';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM pg_roles
        WHERE rolname = 'cerbero_raw_preserver'
          AND rolcanlogin = false
    ) THEN
        RAISE EXCEPTION 'cerbero_raw_preserver NOLOGIN role is missing';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM system.schema_migrations
        WHERE version = 2
          AND name = 'raw_preservation'
    ) THEN
        RAISE EXCEPTION 'raw preservation migration is not registered';
    END IF;
END
$checks$;

BEGIN;
SET LOCAL ROLE cerbero_raw_preserver;

INSERT INTO system.raw_objects (
    event_id,
    storage_uri,
    segment_id,
    byte_offset,
    byte_length,
    raw_hash
) VALUES (
    '018f47d0-7b5c-7cc0-98c0-3f2b9859d3e1',
    'raw://tenant-a/2026/09/13/12/segment-test',
    'segment-test',
    0,
    3,
    'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
);

INSERT INTO system.outbox (
    message_id,
    subject,
    request_id,
    payload
) VALUES (
    '018f47d0-7b5c-7cc1-98c0-3f2b9859d3e2',
    'cerbero.v1.raw.persisted',
    '018f47d0-7b5c-7cc2-98c0-3f2b9859d3e3',
    decode('0a01ff', 'hex')
);

INSERT INTO system.processed_messages (
    consumer_name,
    message_id,
    event_id,
    publication_message_id,
    result
) VALUES (
    'raw-preserver-integration',
    '018f47d0-7b5c-7cc3-98c0-3f2b9859d3e4',
    '018f47d0-7b5c-7cc0-98c0-3f2b9859d3e1',
    '018f47d0-7b5c-7cc1-98c0-3f2b9859d3e2',
    'RAW_PRESERVED'
);

UPDATE system.outbox
SET published_at = now()
WHERE message_id = '018f47d0-7b5c-7cc1-98c0-3f2b9859d3e2';

DO $checks$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM system.processed_messages AS processed
        JOIN system.raw_objects AS raw
          ON raw.event_id = processed.event_id
        JOIN system.outbox AS outbox
          ON outbox.message_id = processed.publication_message_id
        WHERE processed.consumer_name = 'raw-preserver-integration'
          AND processed.message_id = '018f47d0-7b5c-7cc3-98c0-3f2b9859d3e4'
          AND raw.byte_length = 3
          AND outbox.published_at IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'raw preservation relational state is incomplete';
    END IF;
END
$checks$;

ROLLBACK;
SQL

if "${psql_cmd[@]}" >/dev/null 2>&1 <<'SQL'
SET ROLE cerbero_raw_preserver;
UPDATE system.raw_objects
SET storage_uri = 'raw://forbidden'
WHERE event_id = '018f47d0-7b5c-7cc0-98c0-3f2b9859d3e1';
SQL
then
  echo "cerbero_raw_preserver unexpectedly has UPDATE on system.raw_objects" >&2
  exit 1
fi

if "${psql_cmd[@]}" >/dev/null 2>&1 <<'SQL'
SET ROLE cerbero_raw_preserver;
DELETE FROM system.outbox;
SQL
then
  echo "cerbero_raw_preserver unexpectedly has DELETE on system.outbox" >&2
  exit 1
fi

echo "Milestone 3 PostgreSQL raw-preservation schema: PASS"
