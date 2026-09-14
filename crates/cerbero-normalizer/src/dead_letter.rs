use std::time::{SystemTime, UNIX_EPOCH};

use async_nats::jetstream;
use cerbero_common::contracts::sha256_lower_hex;
use cerbero_common::contracts::v1::{CerberoEnvelope, RawEventPersisted};
use prost::Message as _;
use serde::{Deserialize, Serialize};

use crate::{NormalizerError, new_uuid_v7};

pub const NORMALIZATION_DLQ_SCHEMA: &str = "cerbero.normalization_dlq.v1";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NormalizationDeadLetter {
    pub schema_version: String,
    pub dlq_record_id: String,
    pub original_message_id: Option<String>,
    pub original_subject: String,
    pub original_payload_sha256: String,
    pub consumer: String,
    pub attempt_count: u64,
    pub stream_sequence: Option<u64>,
    pub consumer_sequence: Option<u64>,
    pub error_code: String,
    pub error_category: String,
    pub failure_stage: String,
    pub error_message: String,
    pub retryable: bool,
    pub first_failure_unix_ms: i64,
    pub last_failure_unix_ms: i64,
    pub tenant_id: Option<String>,
    pub raw_event_id: Option<String>,
    pub parser_id: Option<String>,
    pub parser_version: Option<String>,
    pub request_id: Option<String>,
    pub trace_id: Option<String>,
    pub correlation_id: Option<String>,
}

impl NormalizationDeadLetter {
    #[must_use]
    pub fn from_message(
        message: &jetstream::Message,
        consumer: &str,
        error: &NormalizerError,
        parser_identity: Option<(&str, &str)>,
        first_failure_unix_ms: i64,
    ) -> Self {
        let info = message.info().ok();
        let envelope = CerberoEnvelope::decode(message.payload.as_ref()).ok();
        let persisted = envelope
            .as_ref()
            .and_then(|envelope| envelope.payload.as_ref())
            .and_then(|payload| RawEventPersisted::decode(payload.value.as_slice()).ok());
        let original_message_id = envelope
            .as_ref()
            .and_then(|value| non_empty(value.message_id.as_str()));
        let request_id = message
            .headers
            .as_ref()
            .and_then(|headers| headers.get("Cerbero-Request-Id"))
            .and_then(|value| non_empty(value.as_str()));
        let last_failure_unix_ms = unix_time_millis();

        Self {
            schema_version: NORMALIZATION_DLQ_SCHEMA.to_string(),
            dlq_record_id: new_uuid_v7(),
            original_message_id,
            original_subject: message.subject.to_string(),
            original_payload_sha256: sha256_lower_hex(message.payload.as_ref()),
            consumer: consumer.to_string(),
            attempt_count: info.as_ref().map_or(1, |value| {
                u64::try_from(value.delivered.max(1)).unwrap_or(u64::MAX)
            }),
            stream_sequence: info.as_ref().map(|value| value.stream_sequence),
            consumer_sequence: info.as_ref().map(|value| value.consumer_sequence),
            error_code: error.code.to_string(),
            error_category: error_category(error.code).to_string(),
            failure_stage: failure_stage(error.code).to_string(),
            error_message: error.message.clone(),
            retryable: error.retryable,
            first_failure_unix_ms,
            last_failure_unix_ms,
            tenant_id: envelope
                .as_ref()
                .and_then(|value| non_empty(value.tenant_id.as_str())),
            raw_event_id: persisted
                .as_ref()
                .and_then(|value| non_empty(value.event_id.as_str())),
            parser_id: parser_identity.map(|(parser_id, _)| parser_id.to_string()),
            parser_version: parser_identity.map(|(_, version)| version.to_string()),
            request_id,
            trace_id: envelope
                .as_ref()
                .and_then(|value| non_empty(value.trace_id.as_str())),
            correlation_id: envelope
                .as_ref()
                .and_then(|value| non_empty(value.correlation_id.as_str())),
        }
    }

    #[must_use]
    pub fn nats_message_id(&self) -> String {
        if let Some(message_id) = &self.original_message_id {
            format!("normalization-dlq:v1:{message_id}")
        } else if let Some(sequence) = self.stream_sequence {
            format!("normalization-dlq:v1:stream:{sequence}")
        } else {
            format!(
                "normalization-dlq:v1:payload:{}",
                self.original_payload_sha256
            )
        }
    }
}

#[must_use]
pub fn unix_time_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
        })
}

fn non_empty(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| value.to_string())
}

fn error_category(code: &str) -> &'static str {
    if code.starts_with("CER-PARSE-") {
        "PARSING"
    } else if code.starts_with("CER-NORM-RAW-HASH")
        || code.starts_with("CER-NORM-RAW-LENGTH")
        || code.starts_with("CER-NORM-RAW-INTEGRITY")
    {
        "INTEGRITY"
    } else if code.starts_with("CER-NORM-CLICKHOUSE") || code.starts_with("CER-NORM-RAW-STORE") {
        "STORAGE"
    } else if code.starts_with("CER-NORM-NATS") || code.starts_with("CER-NORM-PUBLISH") {
        "TRANSPORT"
    } else if code.starts_with("CER-NORM-MAPPING")
        || code.starts_with("CER-NORM-CONTRACT")
        || code.starts_with("CER-NORM-TRANSFORMATION")
        || code.starts_with("CER-NORM-SOURCE-TIME")
    {
        "NORMALIZATION"
    } else {
        "INTERNAL"
    }
}

fn failure_stage(code: &str) -> &'static str {
    if code.starts_with("CER-PARSE-") {
        "parsing"
    } else if code.starts_with("CER-NORM-MAPPING") {
        "mapping"
    } else if code.starts_with("CER-NORM-CLICKHOUSE") {
        "normalized_storage"
    } else if code.starts_with("CER-NORM-RAW") {
        "raw_read_or_integrity"
    } else if code.starts_with("CER-NORM-ENVELOPE")
        || code.starts_with("CER-NORM-PAYLOAD")
        || code.starts_with("CER-NORM-TENANT")
        || code.starts_with("CER-NORM-UNSUPPORTED-CONTRACT")
    {
        "handoff_validation"
    } else if code.starts_with("CER-NORM-PUBLISH") || code.starts_with("CER-NORM-NATS") {
        "event_bus"
    } else {
        "normalization"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn category_mapping_keeps_stable_low_cardinality_classes() {
        assert_eq!(error_category("CER-PARSE-MALFORMED"), "PARSING");
        assert_eq!(
            error_category("CER-NORM-MAPPING-UNSUPPORTED"),
            "NORMALIZATION"
        );
        assert_eq!(error_category("CER-NORM-RAW-HASH"), "INTEGRITY");
        assert_eq!(error_category("CER-NORM-CLICKHOUSE-UNAVAILABLE"), "STORAGE");
        assert_eq!(error_category("CER-NORM-NATS-UNAVAILABLE"), "TRANSPORT");
    }
}
