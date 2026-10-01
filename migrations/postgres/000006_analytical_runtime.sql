BEGIN;

DO $roles$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'cerbero_detection') THEN
        CREATE ROLE cerbero_detection NOLOGIN;
    END IF;
END
$roles$;

CREATE TABLE detection.signals (
    signal_id uuid PRIMARY KEY,
    logical_key char(64) NOT NULL UNIQUE CHECK (logical_key ~ '^[0-9a-f]{64}$'),
    tenant_id uuid NOT NULL,
    created_at timestamptz NOT NULL,
    payload bytea NOT NULL CHECK (octet_length(payload) > 0)
);
CREATE INDEX detection_signals_tenant_id_idx ON detection.signals (tenant_id, signal_id);

CREATE TABLE detection.signal_inputs (
    signal_id uuid NOT NULL REFERENCES detection.signals(signal_id) ON DELETE RESTRICT,
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    input_type smallint NOT NULL CHECK (input_type BETWEEN 1 AND 3),
    input_id text NOT NULL CHECK (input_id <> ''),
    relation text NOT NULL CHECK (relation <> ''),
    PRIMARY KEY (signal_id, ordinal)
);

CREATE TABLE investigation.findings (
    finding_id uuid PRIMARY KEY,
    logical_key char(64) NOT NULL UNIQUE CHECK (logical_key ~ '^[0-9a-f]{64}$'),
    tenant_id uuid NOT NULL,
    created_at timestamptz NOT NULL,
    payload bytea NOT NULL CHECK (octet_length(payload) > 0)
);
CREATE INDEX investigation_findings_tenant_id_idx ON investigation.findings (tenant_id, finding_id);

CREATE TABLE investigation.finding_inputs (
    finding_id uuid NOT NULL REFERENCES investigation.findings(finding_id) ON DELETE RESTRICT,
    input_type text NOT NULL CHECK (input_type <> ''),
    input_id text NOT NULL CHECK (input_id <> ''),
    relation text NOT NULL CHECK (relation <> ''),
    PRIMARY KEY (finding_id, input_type, input_id, relation)
);

CREATE TABLE investigation.finding_correlation_provenance (
    finding_id uuid PRIMARY KEY REFERENCES investigation.findings(finding_id) ON DELETE RESTRICT,
    correlation_rule_id text NOT NULL CHECK (correlation_rule_id <> ''),
    correlation_rule_version text NOT NULL CHECK (correlation_rule_version <> ''),
    configuration_hash text NOT NULL CHECK (configuration_hash <> ''),
    input_ids jsonb NOT NULL CHECK (jsonb_typeof(input_ids) = 'array')
);

REVOKE ALL ON TABLE
    detection.signals,
    detection.signal_inputs,
    investigation.findings,
    investigation.finding_inputs,
    investigation.finding_correlation_provenance
FROM PUBLIC;

GRANT USAGE ON SCHEMA detection, investigation, system TO cerbero_detection;
GRANT SELECT, INSERT ON TABLE
    detection.signals,
    detection.signal_inputs,
    investigation.findings,
    investigation.finding_inputs,
    investigation.finding_correlation_provenance
TO cerbero_detection;
GRANT SELECT, INSERT ON system.outbox TO cerbero_detection;

GRANT USAGE ON SCHEMA detection, investigation TO cerbero_api;
GRANT SELECT ON TABLE
    detection.signals,
    detection.signal_inputs,
    investigation.findings,
    investigation.finding_inputs,
    investigation.finding_correlation_provenance
TO cerbero_api;

INSERT INTO system.schema_migrations(version, name)
VALUES (6, 'analytical_runtime');

COMMIT;
