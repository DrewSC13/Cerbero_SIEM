use std::collections::BTreeMap;
use std::fmt;

use cerbero_common::contracts::v1::{NormalizationStatus, RawEventPersisted};
use serde_json::{Value, json};

use crate::parser::{LINUX_SSHD_PARSER_ID, ParsedEvent};

pub const OCSF_VERSION: &str = "1.9.0";
pub const LINUX_SSH_AUTH_MAPPING_ID: &str = "linux.ssh.authentication";
pub const LINUX_SSH_AUTH_MAPPING_VERSION: &str = "1";

#[derive(Clone, Debug, PartialEq)]
pub struct MappingOutput {
    pub mapping_id: String,
    pub mapping_version: String,
    pub class_uid: u32,
    pub category_uid: u32,
    pub activity_id: Option<u32>,
    pub severity: Option<u32>,
    pub status: NormalizationStatus,
    pub ocsf_event: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappingError {
    pub code: &'static str,
    pub message: String,
    pub retryable: bool,
}

impl fmt::Display for MappingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for MappingError {}

pub trait OcsfMapping: Send + Sync {
    fn id(&self) -> &'static str;
    fn version(&self) -> &'static str;
    fn parser_id(&self) -> &'static str;
    fn map(
        &self,
        parsed: &ParsedEvent,
        persisted: &RawEventPersisted,
        normalized_at_unix_millis: i64,
    ) -> Result<MappingOutput, MappingError>;
}

#[derive(Default)]
pub struct MappingRegistry {
    mappings: BTreeMap<&'static str, Box<dyn OcsfMapping>>,
}

impl MappingRegistry {
    #[must_use]
    pub fn with_defaults() -> Self {
        let mut registry = Self::default();
        registry.register(Box::new(SshAuthenticationMapping));
        registry
    }

    pub fn register(&mut self, mapping: Box<dyn OcsfMapping>) {
        self.mappings.insert(mapping.parser_id(), mapping);
    }

    pub fn for_parser(&self, parser_id: &str) -> Result<&dyn OcsfMapping, MappingError> {
        self.mappings
            .get(parser_id)
            .map(Box::as_ref)
            .ok_or_else(|| MappingError {
                code: "CER-NORM-MAPPING-UNSUPPORTED",
                message: format!("no OCSF mapping is registered for parser {parser_id}"),
                retryable: false,
            })
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SshAuthenticationMapping;

impl OcsfMapping for SshAuthenticationMapping {
    fn id(&self) -> &'static str {
        LINUX_SSH_AUTH_MAPPING_ID
    }

    fn version(&self) -> &'static str {
        LINUX_SSH_AUTH_MAPPING_VERSION
    }

    fn parser_id(&self) -> &'static str {
        LINUX_SSHD_PARSER_ID
    }

    fn map(
        &self,
        parsed: &ParsedEvent,
        persisted: &RawEventPersisted,
        normalized_at_unix_millis: i64,
    ) -> Result<MappingOutput, MappingError> {
        if parsed.parser_id != LINUX_SSHD_PARSER_ID
            || parsed.source_event_type != "linux.ssh.authentication"
        {
            return Err(MappingError {
                code: "CER-NORM-MAPPING-PARSER-MISMATCH",
                message: format!(
                    "mapping does not accept parser {} source_event_type {}",
                    parsed.parser_id, parsed.source_event_type
                ),
                retryable: false,
            });
        }

        let username = required_string(parsed, "username")?;
        let source_ip = required_string(parsed, "source_ip")?;
        let source_port = required_u16(parsed, "source_port")?;
        let invalid_user = required_bool(parsed, "invalid_user")?;
        let message = required_string(parsed, "message")?;

        let (event_time_millis, status, time_source) =
            if let Some(event_time) = &persisted.event_time {
                (
                    timestamp_to_unix_millis(event_time)?,
                    NormalizationStatus::Success,
                    "event_time",
                )
            } else {
                let ingest_time = persisted.ingest_time.as_ref().ok_or_else(|| MappingError {
                    code: "CER-NORM-MISSING-INGEST-TIME",
                    message: "RawEventPersisted.ingest_time is required".to_string(),
                    retryable: false,
                })?;
                (
                    timestamp_to_unix_millis(ingest_time)?,
                    NormalizationStatus::Partial,
                    "ingest_time_fallback",
                )
            };
        let ingest_time = persisted.ingest_time.as_ref().ok_or_else(|| MappingError {
            code: "CER-NORM-MISSING-INGEST-TIME",
            message: "RawEventPersisted.ingest_time is required".to_string(),
            retryable: false,
        })?;
        let ingest_time_millis = timestamp_to_unix_millis(ingest_time)?;

        let ocsf_event = json!({
            "activity_id": 1,
            "activity_name": "Logon",
            "auth_protocol": "SSH",
            "auth_protocol_id": 99,
            "category_name": "Identity & Access Management",
            "category_uid": 3,
            "class_name": "Authentication",
            "class_uid": 3002,
            "is_remote": true,
            "message": message,
            "metadata": {
                "logged_time": ingest_time_millis,
                "processed_time": normalized_at_unix_millis,
                "product": {
                    "name": "Cerbero Normalizer",
                    "vendor_name": "Cerbero"
                },
                "version": OCSF_VERSION,
                "cerbero_time_source": time_source
            },
            "service": {"name": "sshd"},
            "severity": "Low",
            "severity_id": 2,
            "src_endpoint": {
                "ip": source_ip,
                "port": source_port
            },
            "status": "Failure",
            "status_detail": if invalid_user {"Invalid user"} else {"Authentication failed"},
            "status_id": 2,
            "time": event_time_millis,
            "type_name": "Authentication: Logon",
            "type_uid": 300_201,
            "user": {"name": username}
        });

        Ok(MappingOutput {
            mapping_id: LINUX_SSH_AUTH_MAPPING_ID.to_string(),
            mapping_version: LINUX_SSH_AUTH_MAPPING_VERSION.to_string(),
            class_uid: 3002,
            category_uid: 3,
            activity_id: Some(1),
            severity: Some(2),
            status,
            ocsf_event,
        })
    }
}

fn required_string<'a>(parsed: &'a ParsedEvent, field: &str) -> Result<&'a str, MappingError> {
    parsed.string_field(field).ok_or_else(|| MappingError {
        code: "CER-NORM-MISSING-REQUIRED-FIELD",
        message: format!("required parsed field {field} must be a string"),
        retryable: false,
    })
}

fn required_bool(parsed: &ParsedEvent, field: &str) -> Result<bool, MappingError> {
    parsed.bool_field(field).ok_or_else(|| MappingError {
        code: "CER-NORM-MISSING-REQUIRED-FIELD",
        message: format!("required parsed field {field} must be a bool"),
        retryable: false,
    })
}

fn required_u16(parsed: &ParsedEvent, field: &str) -> Result<u16, MappingError> {
    let value = parsed.u64_field(field).ok_or_else(|| MappingError {
        code: "CER-NORM-MISSING-REQUIRED-FIELD",
        message: format!("required parsed field {field} must be an unsigned integer"),
        retryable: false,
    })?;
    u16::try_from(value).map_err(|_| MappingError {
        code: "CER-NORM-MISSING-REQUIRED-FIELD",
        message: format!("required parsed field {field} is outside uint16 range"),
        retryable: false,
    })
}

fn timestamp_to_unix_millis(timestamp: &prost_types::Timestamp) -> Result<i64, MappingError> {
    let millis_from_seconds = timestamp
        .seconds
        .checked_mul(1000)
        .ok_or_else(|| MappingError {
            code: "CER-NORM-TIMESTAMP-RANGE",
            message: "timestamp seconds overflow milliseconds".to_string(),
            retryable: false,
        })?;
    let millis_from_nanos = i64::from(timestamp.nanos) / 1_000_000;
    millis_from_seconds
        .checked_add(millis_from_nanos)
        .ok_or_else(|| MappingError {
            code: "CER-NORM-TIMESTAMP-RANGE",
            message: "timestamp overflows milliseconds".to_string(),
            retryable: false,
        })
}

#[cfg(test)]
mod tests {
    use cerbero_common::contracts::v1::RawEventPersisted;
    use prost_types::Timestamp;

    use super::*;
    use crate::parser::{LinuxSshdParser, ParsedEvent, Parser, ParserInput};

    fn parsed() -> ParsedEvent {
        let persisted = RawEventPersisted::default();
        LinuxSshdParser
            .parse(&ParserInput {
                raw: b"Failed password for invalid user admin from 10.0.0.8 port 50341 ssh2",
                persisted: &persisted,
                configured_parser_id: None,
            })
            .unwrap()
    }

    #[test]
    fn maps_governed_sshd_vertical_to_authentication() {
        let persisted = RawEventPersisted {
            event_time: Some(Timestamp {
                seconds: 1_789_000_000,
                nanos: 0,
            }),
            ingest_time: Some(Timestamp {
                seconds: 1_789_000_001,
                nanos: 0,
            }),
            ..Default::default()
        };
        let output = SshAuthenticationMapping
            .map(&parsed(), &persisted, 1_789_000_002_000)
            .unwrap();
        assert_eq!(output.class_uid, 3002);
        assert_eq!(output.category_uid, 3);
        assert_eq!(output.activity_id, Some(1));
        assert_eq!(output.status, NormalizationStatus::Success);
        assert_eq!(output.ocsf_event["auth_protocol_id"], 99);
        assert_eq!(output.ocsf_event["metadata"]["version"], OCSF_VERSION);
    }

    #[test]
    fn missing_event_time_is_explicitly_partial() {
        let persisted = RawEventPersisted {
            ingest_time: Some(Timestamp {
                seconds: 1_789_000_001,
                nanos: 0,
            }),
            ..Default::default()
        };
        let output = SshAuthenticationMapping
            .map(&parsed(), &persisted, 1_789_000_002_000)
            .unwrap();
        assert_eq!(output.status, NormalizationStatus::Partial);
        assert_eq!(
            output.ocsf_event["metadata"]["cerbero_time_source"],
            "ingest_time_fallback"
        );
    }
}
