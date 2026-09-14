#![forbid(unsafe_code)]
#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::module_name_repetitions
)]

mod canonical;
mod core;
mod mapping;
mod parser;

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
