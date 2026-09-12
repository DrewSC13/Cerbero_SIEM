CREATE DATABASE IF NOT EXISTS cerbero;

CREATE TABLE IF NOT EXISTS cerbero.schema_migrations
(
    version UInt64,
    name String,
    applied_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = MergeTree
ORDER BY version;

INSERT INTO cerbero.schema_migrations (version, name)
SELECT 1, 'bootstrap'
WHERE NOT EXISTS (
    SELECT 1 FROM cerbero.schema_migrations WHERE version = 1
);
