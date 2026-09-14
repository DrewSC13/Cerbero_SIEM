#![forbid(unsafe_code)]
#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::module_name_repetitions
)]

mod canonical;
mod clickhouse;
mod core;
mod eventbus;
mod mapping;
mod parser;
mod raw_store;
mod runtime;
mod runtime_config;
mod system_id;

pub use canonical::{canonical_json_bytes, canonical_json_hash};
pub use core::{
    Clock, FixedClock, FixedIdGenerator, IdGenerator, NormalizationPlan, NormalizerCore,
    NormalizerCoreConfig, NormalizerError, SystemClock,
};
pub use mapping::{
    LINUX_SSH_AUTH_MAPPING_ID, LINUX_SSH_AUTH_MAPPING_VERSION, MappingOutput, MappingRegistry,
    OCSF_VERSION, OcsfMapping, SshAuthenticationMapping,
};
pub use parser::{
    LINUX_SSHD_PARSER_ID, LINUX_SSHD_PARSER_VERSION, LinuxSshdParser, ParsedEvent, Parser,
    ParserInput, ParserRegistry, ParsingStatus, TimestampCandidate,
};

pub use clickhouse::{ClickHouseStore, StoredNormalization};
pub use eventbus::{
    ANALYTICS_STREAM_NAME, EventBus, IncomingRawPersisted, NORMALIZED_CREATED_SUBJECT,
    NORMALIZER_CONSUMER_NAME, RAW_PERSISTED_SUBJECT, RAW_STREAM_NAME, REQUEST_ID_HEADER,
    decode_raw_persisted, retry_delay,
};
pub use raw_store::FilesystemRawReader;
pub use runtime::run;
pub use runtime_config::RuntimeConfig;
pub use system_id::{SystemIdGenerator, new_uuid_v7};
