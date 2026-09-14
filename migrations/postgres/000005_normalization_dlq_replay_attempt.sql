BEGIN;

CREATE TABLE system.normalization_dlq_replay_attempt (
    lifecycle_key text NOT NULL
        REFERENCES system.normalization_dlq_replay_state(lifecycle_key)
        ON DELETE RESTRICT,
    source_dlq_record_id text NOT NULL,
    attempt bigint NOT NULL,
    source_stream_sequence numeric(20, 0) NOT NULL,
    reserved_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (lifecycle_key, source_dlq_record_id),
    UNIQUE (lifecycle_key, attempt),
    CONSTRAINT normalization_dlq_replay_attempt_lifecycle_key_ck
        CHECK (length(btrim(lifecycle_key)) BETWEEN 1 AND 512),
    CONSTRAINT normalization_dlq_replay_attempt_source_dlq_record_id_ck
        CHECK (length(btrim(source_dlq_record_id)) BETWEEN 1 AND 256),
    CONSTRAINT normalization_dlq_replay_attempt_attempt_ck
        CHECK (attempt BETWEEN 1 AND 4294967295),
    CONSTRAINT normalization_dlq_replay_attempt_source_stream_sequence_ck
        CHECK (
            source_stream_sequence BETWEEN 1 AND 18446744073709551615
        )
);

REVOKE ALL ON TABLE system.normalization_dlq_replay_attempt FROM PUBLIC;
GRANT SELECT, INSERT
    ON TABLE system.normalization_dlq_replay_attempt
    TO cerbero_worker;

INSERT INTO system.schema_migrations(version, name)
VALUES (5, 'normalization_dlq_replay_attempt');

COMMIT;
