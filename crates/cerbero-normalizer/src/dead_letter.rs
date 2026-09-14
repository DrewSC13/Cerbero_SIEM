use std::time::{SystemTime, UNIX_EPOCH};

use async_nats::jetstream;
use cerbero_common::contracts::sha256_lower_hex;
use cerbero_common::contracts::v1::{CerberoEnvelope, RawEventPersisted};
use prost::Message as _;
use serde::{Deserialize, Serialize};

use crate::eventbus::{
    RAW_PERSISTED_SUBJECT, RAW_REPLAY_SUBJECT, REPLAY_ATTEMPT_HEADER,
    REPLAY_ROOT_DLQ_RECORD_ID_HEADER, REPLAY_SOURCE_STREAM_SEQUENCE_HEADER,
};
use crate::new_uuid_v7;

pub const NORMALIZATION_DLQ_SCHEMA: &str = "cerbero.normalization_dlq.v1";

#[derive(Clone, Debug)]
pub(crate) struct DeadLetterFailure<'a> {
    pub error_code: &'a str,
    pub error_message: &'a str,
    pub retryable: bool,
    pub attempt_count: u64,
    pub first_failure_at: SystemTime,
    pub last_failure_at: SystemTime,
}

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
    pub replay_root_dlq_record_id: Option<String>,
    pub replay_attempt: Option<u32>,
}

impl NormalizationDeadLetter {
    #[must_use]
    pub(crate) fn from_message(
        message: &jetstream::Message,
        consumer: &str,
        failure: &DeadLetterFailure<'_>,
        parser_identity: Option<(&str, &str)>,
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
        let replay_root_dlq_record_id = message
            .headers
            .as_ref()
            .and_then(|headers| headers.get(REPLAY_ROOT_DLQ_RECORD_ID_HEADER))
            .and_then(|value| non_empty(value.as_str()));
        let replay_attempt = message
            .headers
            .as_ref()
            .and_then(|headers| headers.get(REPLAY_ATTEMPT_HEADER))
            .and_then(|value| value.as_str().parse::<u32>().ok())
            .filter(|value| *value > 0);
        let replay_source_stream_sequence = message
            .headers
            .as_ref()
            .and_then(|headers| headers.get(REPLAY_SOURCE_STREAM_SEQUENCE_HEADER))
            .and_then(|value| value.as_str().parse::<u64>().ok())
            .filter(|value| *value > 0);
        let (original_subject, stream_sequence) = dlq_source_identity(
            message.subject.as_str(),
            info.as_ref().map(|value| value.stream_sequence),
            replay_root_dlq_record_id.as_deref(),
            replay_attempt,
            replay_source_stream_sequence,
        );
        Self {
            schema_version: NORMALIZATION_DLQ_SCHEMA.to_string(),
            dlq_record_id: new_uuid_v7(),
            original_message_id,
            original_subject,
            original_payload_sha256: sha256_lower_hex(message.payload.as_ref()),
            consumer: consumer.to_string(),
            attempt_count: failure.attempt_count,
            stream_sequence,
            consumer_sequence: info.as_ref().map(|value| value.consumer_sequence),
            error_code: failure.error_code.to_string(),
            error_category: error_category(failure.error_code).to_string(),
            failure_stage: failure_stage(failure.error_code).to_string(),
            error_message: failure.error_message.to_string(),
            retryable: failure.retryable,
            first_failure_unix_ms: system_time_millis(failure.first_failure_at),
            last_failure_unix_ms: system_time_millis(failure.last_failure_at),
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
            replay_root_dlq_record_id,
            replay_attempt,
        }
    }

    #[must_use]
    pub fn nats_message_id(&self) -> String {
        if let (Some(replay_root), Some(replay_attempt)) =
            (&self.replay_root_dlq_record_id, self.replay_attempt)
        {
            return format!(
                "normalization-dlq:v1:replay:{replay_root}:{replay_attempt}:{}",
                self.original_payload_sha256
            );
        }
        if let Some(message_id) = &self.original_message_id {
            format!(
                "normalization-dlq:v1:{message_id}:{}",
                self.original_payload_sha256
            )
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
    system_time_millis(SystemTime::now())
}

fn system_time_millis(time: SystemTime) -> i64 {
    time.duration_since(UNIX_EPOCH).map_or(0, |duration| {
        i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
    })
}

fn non_empty(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| value.to_string())
}

fn dlq_source_identity(
    message_subject: &str,
    message_stream_sequence: Option<u64>,
    replay_root_dlq_record_id: Option<&str>,
    replay_attempt: Option<u32>,
    replay_source_stream_sequence: Option<u64>,
) -> (String, Option<u64>) {
    if message_subject == RAW_REPLAY_SUBJECT
        && replay_root_dlq_record_id.is_some()
        && replay_attempt.is_some()
        && replay_source_stream_sequence.is_some()
    {
        (
            RAW_PERSISTED_SUBJECT.to_string(),
            replay_source_stream_sequence,
        )
    } else {
        (message_subject.to_string(), message_stream_sequence)
    }
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
    fn dlq_dedup_identity_does_not_collapse_distinct_payloads_with_reused_message_id() {
        fn record(
            message_id: Option<&str>,
            payload_hash: &str,
            sequence: Option<u64>,
        ) -> NormalizationDeadLetter {
            NormalizationDeadLetter {
                schema_version: NORMALIZATION_DLQ_SCHEMA.to_string(),
                dlq_record_id: "01995000-0000-7000-8000-000000000001".to_string(),
                original_message_id: message_id.map(str::to_string),
                original_subject: "cerbero.v1.raw.persisted".to_string(),
                original_payload_sha256: payload_hash.to_string(),
                consumer: "normalizer".to_string(),
                attempt_count: 1,
                stream_sequence: sequence,
                consumer_sequence: Some(1),
                error_code: "CER-NORM-ENVELOPE-CONTRACT".to_string(),
                error_category: "NORMALIZATION".to_string(),
                failure_stage: "handoff_validation".to_string(),
                error_message: "invalid envelope".to_string(),
                retryable: false,
                first_failure_unix_ms: 1,
                last_failure_unix_ms: 1,
                tenant_id: None,
                raw_event_id: None,
                parser_id: None,
                parser_version: None,
                request_id: None,
                trace_id: None,
                correlation_id: None,
                replay_root_dlq_record_id: None,
                replay_attempt: None,
            }
        }

        let hash_a = "a".repeat(64);
        let hash_b = "b".repeat(64);
        let first = record(Some("reused-invalid-id"), &hash_a, Some(7));
        let duplicate = record(Some("reused-invalid-id"), &hash_a, Some(8));
        let distinct_payload = record(Some("reused-invalid-id"), &hash_b, Some(9));

        assert_eq!(first.nats_message_id(), duplicate.nats_message_id());
        assert_ne!(first.nats_message_id(), distinct_payload.nats_message_id());

        let no_message_id_a = record(None, &hash_a, Some(11));
        let no_message_id_b = record(None, &hash_a, Some(12));
        assert_ne!(
            no_message_id_a.nats_message_id(),
            no_message_id_b.nats_message_id()
        );

        let mut replay_attempt_one = record(Some("reused-invalid-id"), &hash_a, Some(21));
        replay_attempt_one.replay_root_dlq_record_id = Some("root-dlq".to_string());
        replay_attempt_one.replay_attempt = Some(1);
        let mut replay_attempt_one_redelivery =
            record(Some("reused-invalid-id"), &hash_a, Some(22));
        replay_attempt_one_redelivery.replay_root_dlq_record_id = Some("root-dlq".to_string());
        replay_attempt_one_redelivery.replay_attempt = Some(1);
        let mut replay_attempt_two = record(Some("reused-invalid-id"), &hash_a, Some(23));
        replay_attempt_two.replay_root_dlq_record_id = Some("root-dlq".to_string());
        replay_attempt_two.replay_attempt = Some(2);

        assert_eq!(
            replay_attempt_one.nats_message_id(),
            replay_attempt_one_redelivery.nats_message_id()
        );
        assert_ne!(
            replay_attempt_one.nats_message_id(),
            replay_attempt_two.nats_message_id()
        );
    }

    #[test]
    fn selective_replay_dlq_preserves_original_raw_locator() {
        let (subject, sequence) = dlq_source_identity(
            RAW_REPLAY_SUBJECT,
            Some(900),
            Some("root-dlq"),
            Some(2),
            Some(42),
        );
        assert_eq!(subject, RAW_PERSISTED_SUBJECT);
        assert_eq!(sequence, Some(42));

        let (fallback_subject, fallback_sequence) = dlq_source_identity(
            RAW_REPLAY_SUBJECT,
            Some(900),
            Some("root-dlq"),
            Some(2),
            None,
        );
        assert_eq!(fallback_subject, RAW_REPLAY_SUBJECT);
        assert_eq!(fallback_sequence, Some(900));
    }

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
