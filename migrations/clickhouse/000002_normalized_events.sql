CREATE TABLE IF NOT EXISTS cerbero.normalized_events
(
    logical_key FixedString(64),
    tenant_id String,
    normalized_event_id UUID,
    raw_event_id UUID,
    event_time_present UInt8,
    event_time_seconds Int64,
    event_time_nanos Int32,
    ingest_time_seconds Int64,
    ingest_time_nanos Int32,
    normalized_at_seconds Int64,
    normalized_at_nanos Int32,
    ocsf_version LowCardinality(String),
    class_uid UInt32,
    category_uid UInt32,
    activity_id Nullable(UInt32),
    severity Nullable(UInt32),
    parser_id LowCardinality(String),
    parser_version LowCardinality(String),
    mapping_id LowCardinality(String),
    mapping_version LowCardinality(String),
    normalization_status UInt8,
    normalized_hash_algorithm LowCardinality(String),
    normalized_hash FixedString(64),
    pipeline_version LowCardinality(String),
    ocsf_event_json String,
    transformation_id UUID,
    configuration_hash FixedString(64),
    execution_mode UInt8,
    publication_message_id UUID,
    causation_message_id UUID,
    trace_id String,
    correlation_id String,
    producer_component_version LowCardinality(String),
    producer_instance_id String,
    created_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = MergeTree
ORDER BY (tenant_id, event_time_seconds, normalized_event_id)
SETTINGS non_replicated_deduplication_window = 1000;

INSERT INTO cerbero.schema_migrations (version, name)
SELECT 2, 'normalized_events'
WHERE NOT EXISTS (
    SELECT 1 FROM cerbero.schema_migrations WHERE version = 2
);
