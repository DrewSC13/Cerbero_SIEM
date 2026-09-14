BEGIN;

DO $roles$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'cerbero_worker') THEN
        CREATE ROLE cerbero_worker NOLOGIN;
    END IF;
END
$roles$;

CREATE TABLE IF NOT EXISTS system.normalization_dlq_replay_state (
    lifecycle_key text PRIMARY KEY,
    replay_root_dlq_record_id text NOT NULL,
    consumed_attempts bigint NOT NULL DEFAULT 0,
    max_attempts bigint NOT NULL,
    first_reserved_at timestamptz,
    last_reserved_at timestamptz,
    last_source_dlq_record_id text NOT NULL,
    last_source_stream_sequence numeric(20, 0) NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT normalization_dlq_replay_state_lifecycle_key_check
        CHECK (char_length(lifecycle_key) > 0 AND octet_length(lifecycle_key) <= 256),
    CONSTRAINT normalization_dlq_replay_state_root_check
        CHECK (
            char_length(replay_root_dlq_record_id) > 0
            AND octet_length(replay_root_dlq_record_id) <= 128
        ),
    CONSTRAINT normalization_dlq_replay_state_source_record_check
        CHECK (
            char_length(last_source_dlq_record_id) > 0
            AND octet_length(last_source_dlq_record_id) <= 128
        ),
    CONSTRAINT normalization_dlq_replay_state_consumed_check
        CHECK (consumed_attempts >= 0),
    CONSTRAINT normalization_dlq_replay_state_max_check
        CHECK (max_attempts >= 1 AND max_attempts <= 4294967295),
    CONSTRAINT normalization_dlq_replay_state_budget_check
        CHECK (consumed_attempts <= max_attempts),
    CONSTRAINT normalization_dlq_replay_state_stream_sequence_check
        CHECK (
            last_source_stream_sequence >= 1
            AND last_source_stream_sequence <= 18446744073709551615
        ),
    CONSTRAINT normalization_dlq_replay_state_timestamps_check
        CHECK (
            (
                consumed_attempts = 0
                AND first_reserved_at IS NULL
                AND last_reserved_at IS NULL
            )
            OR (
                consumed_attempts > 0
                AND first_reserved_at IS NOT NULL
                AND last_reserved_at IS NOT NULL
                AND last_reserved_at >= first_reserved_at
            )
        )
);

REVOKE ALL ON TABLE system.normalization_dlq_replay_state FROM PUBLIC;

GRANT USAGE ON SCHEMA system TO cerbero_worker;
GRANT SELECT, INSERT, UPDATE ON TABLE system.normalization_dlq_replay_state
TO cerbero_worker;

INSERT INTO system.schema_migrations (version, name)
VALUES (4, 'normalization_dlq_replay_state')
ON CONFLICT (version) DO NOTHING;

COMMIT;
