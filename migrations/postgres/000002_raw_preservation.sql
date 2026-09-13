BEGIN;

DO $roles$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'cerbero_raw_preserver') THEN
        CREATE ROLE cerbero_raw_preserver NOLOGIN;
    END IF;
END
$roles$;

CREATE TABLE IF NOT EXISTS system.raw_objects (
    event_id uuid PRIMARY KEY,
    storage_uri text NOT NULL CHECK (storage_uri <> ''),
    segment_id text NOT NULL CHECK (segment_id <> ''),
    byte_offset numeric(20, 0) NOT NULL CHECK (
        byte_offset >= 0
        AND byte_offset <= 18446744073709551615
    ),
    byte_length numeric(20, 0) NOT NULL CHECK (
        byte_length >= 0
        AND byte_length <= 18446744073709551615
    ),
    raw_hash text NOT NULL CHECK (raw_hash ~ '^[0-9a-f]{64}$'),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS system.outbox (
    message_id uuid PRIMARY KEY,
    subject text NOT NULL CHECK (subject <> ''),
    request_id uuid,
    payload bytea NOT NULL CHECK (octet_length(payload) > 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);

CREATE INDEX IF NOT EXISTS system_outbox_unpublished_created_idx
    ON system.outbox (created_at, message_id)
    WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS system.processed_messages (
    consumer_name text NOT NULL CHECK (consumer_name <> ''),
    message_id uuid NOT NULL,
    event_id uuid NOT NULL REFERENCES system.raw_objects (event_id) ON DELETE RESTRICT,
    publication_message_id uuid NOT NULL
        REFERENCES system.outbox (message_id) ON DELETE RESTRICT,
    processed_at timestamptz NOT NULL DEFAULT now(),
    result text NOT NULL CHECK (result <> ''),
    PRIMARY KEY (consumer_name, message_id),
    UNIQUE (publication_message_id)
);

CREATE INDEX IF NOT EXISTS system_processed_messages_event_idx
    ON system.processed_messages (event_id);

REVOKE ALL ON TABLE
    system.raw_objects,
    system.processed_messages,
    system.outbox
FROM PUBLIC;

GRANT USAGE ON SCHEMA system TO cerbero_raw_preserver;
GRANT SELECT, INSERT ON
    system.raw_objects,
    system.processed_messages
TO cerbero_raw_preserver;
GRANT SELECT, INSERT ON system.outbox TO cerbero_raw_preserver;
GRANT UPDATE (published_at) ON system.outbox TO cerbero_raw_preserver;

INSERT INTO system.schema_migrations (version, name)
VALUES (2, 'raw_preservation')
ON CONFLICT (version) DO NOTHING;

COMMIT;
