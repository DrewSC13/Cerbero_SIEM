BEGIN;

-- Step 31 keeps Entity/Risk, Incident/Case, and Audit as distinct durable
-- control-plane objects. Exact risk scoring and automatic promotion policies
-- remain intentionally outside this migration.

ALTER TABLE investigation.findings
    ADD CONSTRAINT investigation_findings_tenant_finding_unique
    UNIQUE (tenant_id, finding_id);

CREATE TABLE risk.entities (
    entity_id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL,
    entity_type text NOT NULL CHECK (
        entity_type IN (
            'host', 'user', 'account', 'ip', 'process', 'file',
            'container', 'service', 'application'
        )
    ),
    canonical_key text NOT NULL CHECK (canonical_key <> ''),
    first_seen timestamptz NOT NULL,
    last_seen timestamptz NOT NULL,
    criticality text,
    attributes jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(attributes) = 'object'),
    risk_score numeric(18, 4) NOT NULL DEFAULT 0,
    CONSTRAINT risk_entities_seen_order_check CHECK (last_seen >= first_seen),
    CONSTRAINT risk_entities_tenant_logical_unique
        UNIQUE (tenant_id, entity_type, canonical_key),
    CONSTRAINT risk_entities_tenant_entity_unique
        UNIQUE (tenant_id, entity_id)
);
CREATE INDEX risk_entities_tenant_last_seen_idx
    ON risk.entities (tenant_id, last_seen DESC, entity_id);

CREATE TABLE risk.entity_aliases (
    alias_id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL,
    entity_id uuid NOT NULL,
    alias_type text NOT NULL CHECK (alias_type <> ''),
    alias_value text NOT NULL CHECK (alias_value <> ''),
    source text NOT NULL CHECK (source <> ''),
    confidence text,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT risk_entity_aliases_entity_fk
        FOREIGN KEY (tenant_id, entity_id)
        REFERENCES risk.entities (tenant_id, entity_id)
        ON DELETE RESTRICT,
    CONSTRAINT risk_entity_aliases_tenant_value_unique
        UNIQUE (tenant_id, alias_type, alias_value, entity_id)
);
CREATE INDEX risk_entity_aliases_tenant_entity_idx
    ON risk.entity_aliases (tenant_id, entity_id, created_at DESC);

CREATE TABLE investigation.finding_entities (
    tenant_id uuid NOT NULL,
    finding_id uuid NOT NULL,
    entity_id uuid NOT NULL,
    role text NOT NULL CHECK (
        role IN ('SUBJECT', 'SOURCE', 'DESTINATION', 'ACTOR', 'TARGET', 'AFFECTED', 'RELATED')
    ),
    confidence text,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (finding_id, entity_id, role),
    CONSTRAINT investigation_finding_entities_finding_fk
        FOREIGN KEY (tenant_id, finding_id)
        REFERENCES investigation.findings (tenant_id, finding_id)
        ON DELETE RESTRICT,
    CONSTRAINT investigation_finding_entities_entity_fk
        FOREIGN KEY (tenant_id, entity_id)
        REFERENCES risk.entities (tenant_id, entity_id)
        ON DELETE RESTRICT
);
CREATE INDEX investigation_finding_entities_tenant_entity_idx
    ON investigation.finding_entities (tenant_id, entity_id, finding_id);

CREATE TABLE risk.contributions (
    contribution_id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL,
    entity_id uuid NOT NULL,
    finding_id uuid NOT NULL,
    amount numeric(18, 4) NOT NULL,
    severity text NOT NULL CHECK (severity <> ''),
    confidence text,
    reason text NOT NULL CHECK (reason <> ''),
    rule_id text,
    rule_version text,
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz,
    CONSTRAINT risk_contributions_entity_fk
        FOREIGN KEY (tenant_id, entity_id)
        REFERENCES risk.entities (tenant_id, entity_id)
        ON DELETE RESTRICT,
    CONSTRAINT risk_contributions_finding_fk
        FOREIGN KEY (tenant_id, finding_id)
        REFERENCES investigation.findings (tenant_id, finding_id)
        ON DELETE RESTRICT,
    CONSTRAINT risk_contributions_expiry_check
        CHECK (expires_at IS NULL OR expires_at >= created_at)
);
CREATE INDEX risk_contributions_tenant_entity_created_idx
    ON risk.contributions (tenant_id, entity_id, created_at DESC, contribution_id);
CREATE INDEX risk_contributions_tenant_finding_idx
    ON risk.contributions (tenant_id, finding_id, contribution_id);

CREATE TABLE investigation.incidents (
    incident_id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL,
    status text NOT NULL CHECK (
        status IN ('OPEN', 'TRIAGED', 'INVESTIGATING', 'CONTAINED', 'RESOLVED', 'CLOSED', 'INVALIDATED')
    ),
    severity text NOT NULL CHECK (severity <> ''),
    confidence text,
    title text NOT NULL CHECK (title <> ''),
    description text NOT NULL DEFAULT '',
    first_seen timestamptz NOT NULL,
    last_seen timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    correlation_reason text,
    correlation_rule_id text,
    correlation_rule_version text,
    window_start timestamptz,
    window_end timestamptz,
    owner_user_id uuid,
    disposition text,
    version bigint NOT NULL DEFAULT 1 CHECK (version >= 1),
    CONSTRAINT investigation_incidents_seen_order_check CHECK (last_seen >= first_seen),
    CONSTRAINT investigation_incidents_window_check
        CHECK (
            (window_start IS NULL AND window_end IS NULL)
            OR (window_start IS NOT NULL AND window_end IS NOT NULL AND window_end >= window_start)
        ),
    CONSTRAINT investigation_incidents_tenant_incident_unique
        UNIQUE (tenant_id, incident_id)
);
CREATE INDEX investigation_incidents_tenant_status_updated_idx
    ON investigation.incidents (tenant_id, status, updated_at DESC, incident_id);

CREATE TABLE investigation.incident_findings (
    tenant_id uuid NOT NULL,
    incident_id uuid NOT NULL,
    finding_id uuid NOT NULL,
    relation text NOT NULL CHECK (relation <> ''),
    added_at timestamptz NOT NULL DEFAULT now(),
    added_by uuid,
    PRIMARY KEY (incident_id, finding_id),
    CONSTRAINT investigation_incident_findings_incident_fk
        FOREIGN KEY (tenant_id, incident_id)
        REFERENCES investigation.incidents (tenant_id, incident_id)
        ON DELETE RESTRICT,
    CONSTRAINT investigation_incident_findings_finding_fk
        FOREIGN KEY (tenant_id, finding_id)
        REFERENCES investigation.findings (tenant_id, finding_id)
        ON DELETE RESTRICT
);
CREATE INDEX investigation_incident_findings_tenant_finding_idx
    ON investigation.incident_findings (tenant_id, finding_id, incident_id);

CREATE TABLE investigation.cases (
    case_id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL,
    case_number text NOT NULL CHECK (case_number <> ''),
    status text NOT NULL CHECK (
        status IN ('OPEN', 'TRIAGE', 'INVESTIGATING', 'ON_HOLD', 'RESPONSE', 'RESOLVED', 'CLOSED')
    ),
    priority text NOT NULL CHECK (
        priority IN ('P1_CRITICAL', 'P2_HIGH', 'P3_MEDIUM', 'P4_LOW')
    ),
    title text NOT NULL CHECK (title <> ''),
    description text NOT NULL DEFAULT '',
    owner_user_id uuid,
    created_by uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    closed_at timestamptz,
    disposition text NOT NULL DEFAULT 'UNDETERMINED' CHECK (
        disposition IN (
            'UNDETERMINED', 'CONFIRMED_INCIDENT', 'BENIGN_ACTIVITY',
            'FALSE_POSITIVE', 'DUPLICATE', 'TEST', 'OTHER'
        )
    ),
    closure_reason text,
    version bigint NOT NULL DEFAULT 1 CHECK (version >= 1),
    CONSTRAINT investigation_cases_tenant_number_unique UNIQUE (tenant_id, case_number),
    CONSTRAINT investigation_cases_tenant_case_unique UNIQUE (tenant_id, case_id),
    CONSTRAINT investigation_cases_closed_invariant CHECK (
        status <> 'CLOSED'
        OR (
            disposition <> 'UNDETERMINED'
            AND closure_reason IS NOT NULL
            AND closure_reason <> ''
            AND closed_at IS NOT NULL
        )
    )
);
CREATE INDEX investigation_cases_tenant_status_updated_idx
    ON investigation.cases (tenant_id, status, updated_at DESC, case_id);

CREATE TABLE investigation.case_incidents (
    tenant_id uuid NOT NULL,
    case_id uuid NOT NULL,
    incident_id uuid NOT NULL,
    relation text NOT NULL DEFAULT 'RELATED' CHECK (relation <> ''),
    added_at timestamptz NOT NULL DEFAULT now(),
    added_by uuid NOT NULL,
    PRIMARY KEY (case_id, incident_id),
    CONSTRAINT investigation_case_incidents_case_fk
        FOREIGN KEY (tenant_id, case_id)
        REFERENCES investigation.cases (tenant_id, case_id)
        ON DELETE RESTRICT,
    CONSTRAINT investigation_case_incidents_incident_fk
        FOREIGN KEY (tenant_id, incident_id)
        REFERENCES investigation.incidents (tenant_id, incident_id)
        ON DELETE RESTRICT
);

CREATE TABLE investigation.case_findings (
    tenant_id uuid NOT NULL,
    case_id uuid NOT NULL,
    finding_id uuid NOT NULL,
    relation text NOT NULL DEFAULT 'RELATED' CHECK (relation <> ''),
    added_at timestamptz NOT NULL DEFAULT now(),
    added_by uuid NOT NULL,
    PRIMARY KEY (case_id, finding_id),
    CONSTRAINT investigation_case_findings_case_fk
        FOREIGN KEY (tenant_id, case_id)
        REFERENCES investigation.cases (tenant_id, case_id)
        ON DELETE RESTRICT,
    CONSTRAINT investigation_case_findings_finding_fk
        FOREIGN KEY (tenant_id, finding_id)
        REFERENCES investigation.findings (tenant_id, finding_id)
        ON DELETE RESTRICT
);

CREATE TABLE investigation.case_entities (
    tenant_id uuid NOT NULL,
    case_id uuid NOT NULL,
    entity_id uuid NOT NULL,
    relation text NOT NULL DEFAULT 'RELATED' CHECK (relation <> ''),
    added_at timestamptz NOT NULL DEFAULT now(),
    added_by uuid NOT NULL,
    PRIMARY KEY (case_id, entity_id),
    CONSTRAINT investigation_case_entities_case_fk
        FOREIGN KEY (tenant_id, case_id)
        REFERENCES investigation.cases (tenant_id, case_id)
        ON DELETE RESTRICT,
    CONSTRAINT investigation_case_entities_entity_fk
        FOREIGN KEY (tenant_id, entity_id)
        REFERENCES risk.entities (tenant_id, entity_id)
        ON DELETE RESTRICT
);

CREATE TABLE audit.events (
    audit_event_id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL,
    event_type text NOT NULL CHECK (event_type <> ''),
    actor_type text NOT NULL CHECK (actor_type <> ''),
    actor_id text NOT NULL CHECK (actor_id <> ''),
    action text NOT NULL CHECK (action <> ''),
    object_type text NOT NULL CHECK (object_type <> ''),
    object_id text NOT NULL CHECK (object_id <> ''),
    request_id uuid,
    occurred_at timestamptz NOT NULL DEFAULT now(),
    reason text,
    before_state jsonb,
    after_state jsonb,
    result text NOT NULL CHECK (result <> ''),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(metadata) = 'object')
);
CREATE INDEX audit_events_tenant_occurred_idx
    ON audit.events (tenant_id, occurred_at DESC, audit_event_id DESC);
CREATE INDEX audit_events_tenant_object_idx
    ON audit.events (tenant_id, object_type, object_id, occurred_at DESC, audit_event_id DESC);

REVOKE ALL ON TABLE
    risk.entities,
    risk.entity_aliases,
    risk.contributions,
    investigation.finding_entities,
    investigation.incidents,
    investigation.incident_findings,
    investigation.cases,
    investigation.case_incidents,
    investigation.case_findings,
    investigation.case_entities,
    audit.events
FROM PUBLIC;

GRANT USAGE ON SCHEMA risk, investigation, audit TO cerbero_api;
GRANT SELECT ON TABLE
    risk.entities,
    risk.entity_aliases,
    risk.contributions,
    investigation.finding_entities,
    investigation.incidents,
    investigation.incident_findings,
    investigation.cases,
    investigation.case_incidents,
    investigation.case_findings,
    investigation.case_entities,
    audit.events
TO cerbero_api;
GRANT UPDATE ON TABLE investigation.incidents TO cerbero_api;
GRANT INSERT, UPDATE ON TABLE investigation.cases TO cerbero_api;
GRANT INSERT ON TABLE
    investigation.case_incidents,
    investigation.case_findings,
    investigation.case_entities,
    audit.events
TO cerbero_api;
REVOKE UPDATE, DELETE ON TABLE audit.events FROM cerbero_api;

INSERT INTO system.schema_migrations(version, name)
VALUES (7, 'operational_investigation');

COMMIT;
