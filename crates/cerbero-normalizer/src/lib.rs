#![forbid(unsafe_code)]
#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::module_name_repetitions
)]

mod canonical;
mod clickhouse;
mod core;
mod dead_letter;
mod eventbus;
mod mapping;
mod metrics;
mod parser;
mod parser_formats;
mod raw_store;
mod retry_state;
mod retry_state_postgres;
mod runtime;
mod runtime_config;
mod source_time;
mod system_id;

pub use canonical::{canonical_json_bytes, canonical_json_hash};
pub use core::{
    Clock, DerivationIdentity, FixedClock, FixedIdGenerator, IdGenerator, NormalizationPlan,
    NormalizerCore, NormalizerCoreConfig, NormalizerError, SystemClock,
};
pub use mapping::{
    LINUX_SSH_AUTH_MAPPING_ID, LINUX_SSH_AUTH_MAPPING_VERSION, MappingOutput, MappingRegistry,
    OCSF_VERSION, OcsfMapping, SshAuthenticationMapping,
};
pub use parser::{
    LINUX_SSHD_PARSER_ID, LINUX_SSHD_PARSER_V2_VERSION, LINUX_SSHD_PARSER_VERSION, LinuxSshdParser,
    LinuxSshdParserV2, ParseError, ParsedEvent, ParsedValue, Parser, ParserCandidate, ParserInput,
    ParserLimits, ParserRegistry, ParserSelection, ParserSelectionTrace, ParserTier, ParsingStatus,
    TimestampCandidate,
};
pub use parser_formats::{
    GENERIC_JSON_PARSER_ID, GENERIC_JSON_PARSER_VERSION, GenericJsonParser,
    JOURNALD_CANONICAL_PARSER_ID, JOURNALD_CANONICAL_PARSER_VERSION, JournaldCanonicalParser,
    SYSLOG_RFC3164_PARSER_ID, SYSLOG_RFC3164_PARSER_VERSION, SYSLOG_RFC5424_PARSER_ID,
    SYSLOG_RFC5424_PARSER_VERSION, SyslogRfc3164Parser, SyslogRfc5424Parser,
};

pub use clickhouse::{ClickHouseStore, StoredNormalization};
pub use dead_letter::{NORMALIZATION_DLQ_SCHEMA, NormalizationDeadLetter, unix_time_millis};
pub use eventbus::{
    ANALYTICS_STREAM_NAME, DLQ_SCHEMA_HEADER, DLQ_STREAM_NAME, EXECUTION_MODE_HEADER, EventBus,
    IncomingRawPersisted, NORMALIZATION_DLQ_SUBJECT, NORMALIZED_CREATED_SUBJECT,
    NORMALIZER_CONSUMER_NAME, NORMALIZER_REPLAY_CONSUMER_NAME, NORMALIZER_TEST_CONSUMER_NAME,
    RAW_PERSISTED_SUBJECT, RAW_STREAM_NAME, REQUEST_ID_HEADER, decode_raw_persisted,
    normalizer_consumer_name, retry_delay, retry_delay_with_jitter,
};
pub use metrics::{
    DLQ_NORMALIZATION_TOTAL, EVENTS_NORMALIZED_TOTAL, EVENTS_PARSED_TOTAL, MAPPING_BY_ID,
    MemoryNormalizerMetrics, MetricsSnapshot, NORMALIZATION_FAILED_TOTAL, NORMALIZATION_LATENCY,
    NORMALIZATION_PARTIAL_TOTAL, NORMALIZATION_SUCCESS_TOTAL, NoopNormalizerMetrics,
    NormalizationMetricStatus, NormalizerMetrics, PARSE_FAILED_TOTAL, PARSE_PARTIAL_TOTAL,
    PARSE_SUCCESS_TOTAL, PARSE_UNSUPPORTED_TOTAL, PARSER_BY_ID, PARSER_LATENCY, ParseMetricStatus,
};
pub use raw_store::FilesystemRawReader;
pub use retry_state::{RetryFailure, RetryState, RetryStateStore};
pub use retry_state_postgres::PostgresRetryStateStore;
pub use runtime::{run, run_with_metrics};
pub use runtime_config::{ReplayInput, RuntimeConfig};
pub use source_time::{
    EventTimeContext, SourceTimeError, SourceTimePolicy, SourceTimePolicyRegistry,
};
pub use system_id::{SystemIdGenerator, new_uuid_v7};
