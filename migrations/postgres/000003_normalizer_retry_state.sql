BEGIN;

DO $roles$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $$cerbero_normalizer$$) THEN
        CREATE ROLE cerbero_normalizer NOLOGIN;
    END IF;
END
$roles$;

CREATE TABLE IF NOT EXISTS system.normalizer_retry_state (
    consumer_name text NOT NULL,
    message_key text NOT NULL,
    message_id uuid,
    first_failure_at timestamptz NOT NULL,
    last_failure_at timestamptz NOT NULL,
    failure_count bigint NOT NULL,
    retry_budget bigint NOT NULL,
    last_delivery_attempt bigint NOT NULL,
    last_error_code text NOT NULL,
    last_error_retryable boolean NOT NULL,
    last_error_message text NOT NULL,
    next_retry_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT normalizer_retry_state_pkey
        PRIMARY KEY (consumer_name, message_key),
    CONSTRAINT normalizer_retry_state_consumer_name_check
        CHECK (char_length(consumer_name) > 0),
    CONSTRAINT normalizer_retry_state_message_key_check
        CHECK (char_length(message_key) > 0 AND octet_length(message_key) <= 256),
    CONSTRAINT normalizer_retry_state_failure_count_check
        CHECK (failure_count >= 1),
    CONSTRAINT normalizer_retry_state_retry_budget_check
        CHECK (retry_budget >= 1),
    CONSTRAINT normalizer_retry_state_delivery_attempt_check
        CHECK (last_delivery_attempt >= 1),
    CONSTRAINT normalizer_retry_state_error_code_check
        CHECK (char_length(last_error_code) > 0),
    CONSTRAINT normalizer_retry_state_budget_check
        CHECK (failure_count <= retry_budget),
    CONSTRAINT normalizer_retry_state_failure_time_check
        CHECK (last_failure_at >= first_failure_at),
    CONSTRAINT normalizer_retry_state_next_retry_time_check
        CHECK (next_retry_at IS NULL OR next_retry_at >= last_failure_at),
    CONSTRAINT normalizer_retry_state_lifecycle_check
        CHECK (
            (
                last_error_retryable
                AND failure_count < retry_budget
                AND next_retry_at IS NOT NULL
            )
            OR (
                (NOT last_error_retryable OR failure_count >= retry_budget)
                AND next_retry_at IS NULL
            )
        )
);

REVOKE ALL ON TABLE system.normalizer_retry_state FROM PUBLIC;

GRANT USAGE ON SCHEMA system TO cerbero_normalizer;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE system.normalizer_retry_state
TO cerbero_normalizer;

INSERT INTO system.schema_migrations (version, name)
VALUES (3, $$normalizer_retry_state$$)
ON CONFLICT (version) DO NOTHING;

COMMIT;
