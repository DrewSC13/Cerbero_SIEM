BEGIN;

CREATE SCHEMA IF NOT EXISTS auth;
CREATE SCHEMA IF NOT EXISTS inventory;
CREATE SCHEMA IF NOT EXISTS detection;
CREATE SCHEMA IF NOT EXISTS investigation;
CREATE SCHEMA IF NOT EXISTS risk;
CREATE SCHEMA IF NOT EXISTS audit;
CREATE SCHEMA IF NOT EXISTS system;

CREATE TABLE IF NOT EXISTS system.schema_migrations (
    version bigint PRIMARY KEY,
    name text NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO system.schema_migrations (version, name)
VALUES (1, 'bootstrap')
ON CONFLICT (version) DO NOTHING;

DO $roles$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'cerbero_api') THEN
        CREATE ROLE cerbero_api NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'cerbero_ingest') THEN
        CREATE ROLE cerbero_ingest NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'cerbero_worker') THEN
        CREATE ROLE cerbero_worker NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'cerbero_scheduler') THEN
        CREATE ROLE cerbero_scheduler NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'cerbero_coordinator') THEN
        CREATE ROLE cerbero_coordinator NOLOGIN;
    END IF;
END
$roles$;

REVOKE CREATE ON SCHEMA public FROM PUBLIC;

COMMIT;
