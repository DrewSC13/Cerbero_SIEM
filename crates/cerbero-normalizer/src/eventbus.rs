use std::time::Duration;

use async_nats::HeaderMap;
use async_nats::jetstream;
use async_nats::jetstream::consumer::{AckPolicy, DeliverPolicy, PullConsumer, pull};
use prost::Message as _;
use prost_types::Any;

use cerbero_common::contracts::v1::{CerberoEnvelope, ExecutionMode, Producer, RawEventPersisted};
use cerbero_common::contracts::{
    sha256_lower_hex, validate_envelope, validate_raw_event_persisted,
};

use crate::runtime_config::ReplayInput;
use crate::{NormalizationDeadLetter, NormalizerError, StoredNormalization};

pub const RAW_STREAM_NAME: &str = "CERBERO_RAW";
pub const ANALYTICS_STREAM_NAME: &str = "CERBERO_ANALYTICS";
pub const DLQ_STREAM_NAME: &str = "CERBERO_DLQ";
pub const RAW_PERSISTED_SUBJECT: &str = "cerbero.v1.raw.persisted";
pub const NORMALIZED_CREATED_SUBJECT: &str = "cerbero.v1.normalized.created";
pub const NORMALIZATION_DLQ_SUBJECT: &str = "cerbero.v1.dlq.normalization";
pub const DLQ_SCHEMA_HEADER: &str = "Cerbero-DLQ-Schema";
pub const NORMALIZER_CONSUMER_NAME: &str = "normalizer";
pub const NORMALIZER_REPLAY_CONSUMER_NAME: &str = "normalizer-replay";
pub const NORMALIZER_SELECTIVE_REPLAY_CONSUMER_NAME: &str = "normalizer-replay-selective";
pub const NORMALIZER_TEST_CONSUMER_NAME: &str = "normalizer-test";
pub const REQUEST_ID_HEADER: &str = "Cerbero-Request-Id";
pub const EXECUTION_MODE_HEADER: &str = "Cerbero-Execution-Mode";
pub const RAW_REPLAY_SUBJECT: &str = "cerbero.v1.raw.replay";
pub const REPLAY_ROOT_DLQ_RECORD_ID_HEADER: &str = "Cerbero-Replay-Root-DLQ-Record-Id";
pub const REPLAY_ATTEMPT_HEADER: &str = "Cerbero-Replay-Attempt";
pub const REPLAY_SOURCE_STREAM_SEQUENCE_HEADER: &str = "Cerbero-Replay-Source-Stream-Sequence";
const NATS_MESSAGE_ID_HEADER: &str = "Nats-Msg-Id";

#[derive(Clone)]
pub struct EventBus {
    jetstream: jetstream::Context,
}

pub struct IncomingRawPersisted {
    pub envelope: CerberoEnvelope,
    pub persisted: RawEventPersisted,
    pub request_id: Option<String>,
}

impl EventBus {
    pub async fn connect(
        url: &str,
        user: String,
        password: String,
        instance_name: String,
    ) -> Result<Self, NormalizerError> {
        let client = async_nats::ConnectOptions::with_user_and_password(user, password)
            .name(instance_name)
            .connect(url)
            .await
            .map_err(transport_error)?;
        Ok(Self {
            jetstream: jetstream::new(client),
        })
    }

    pub async fn consumer(
        &self,
        execution_mode: ExecutionMode,
        replay_input: ReplayInput,
    ) -> Result<PullConsumer, NormalizerError> {
        let (consumer_name, filter_subject) =
            normalizer_consumer_route(execution_mode, replay_input)?;
        let deliver_policy = normalizer_deliver_policy(execution_mode, replay_input);
        let stream = self
            .jetstream
            .get_stream(RAW_STREAM_NAME)
            .await
            .map_err(transport_error)?;
        stream
            .get_or_create_consumer(
                consumer_name,
                pull::Config {
                    durable_name: Some(consumer_name.to_string()),
                    deliver_policy,
                    ack_policy: AckPolicy::Explicit,
                    filter_subject: filter_subject.to_string(),
                    max_ack_pending: 1,
                    ..Default::default()
                },
            )
            .await
            .map_err(transport_error)
    }

    pub async fn delete_execution_consumer(
        &self,
        execution_mode: ExecutionMode,
        replay_input: ReplayInput,
    ) -> Result<(), NormalizerError> {
        if execution_mode == ExecutionMode::Live {
            return Ok(());
        }
        let (consumer_name, _) = normalizer_consumer_route(execution_mode, replay_input)?;
        let stream = self
            .jetstream
            .get_stream(RAW_STREAM_NAME)
            .await
            .map_err(transport_error)?;
        stream
            .delete_consumer(consumer_name)
            .await
            .map_err(transport_error)?;
        Ok(())
    }

    pub async fn publish_normalized(
        &self,
        row: &StoredNormalization,
        request_id: Option<&str>,
    ) -> Result<(), NormalizerError> {
        let normalized = row.normalized_event()?;
        let payload = prost_types::Any {
            type_url: "type.googleapis.com/cerbero.contracts.v1.NormalizedEvent".to_string(),
            value: normalized.encode_to_vec(),
        };
        let emitted_at = normalized.normalized_at.ok_or_else(|| NormalizerError {
            code: "CER-NORM-PUBLISH-CONTRACT",
            message: "stored NormalizedEvent.normalized_at is required".to_string(),
            retryable: false,
        })?;
        let envelope = CerberoEnvelope {
            contract_version: "1".to_string(),
            message_id: row.publication_message_id.clone(),
            message_type: "NormalizedEventCreated".to_string(),
            tenant_id: row.tenant_id.clone(),
            producer: Some(Producer {
                component: "cerbero-normalizer".to_string(),
                component_version: row.producer_component_version.clone(),
                instance_id: row.producer_instance_id.clone(),
            }),
            emitted_at: Some(emitted_at),
            trace_id: row.trace_id.clone(),
            causation_id: row.causation_message_id.clone(),
            correlation_id: row.correlation_id.clone(),
            payload_schema: "cerbero.normalized_event.v1".to_string(),
            payload: Some(payload),
        };
        validate_envelope(&envelope).map_err(|error| NormalizerError {
            code: "CER-NORM-PUBLISH-CONTRACT",
            message: error.to_string(),
            retryable: false,
        })?;
        let bytes = envelope.encode_to_vec();
        let mut headers = HeaderMap::new();
        headers.insert(NATS_MESSAGE_ID_HEADER, row.publication_message_id.clone());
        let execution_mode = row.execution_mode().ok_or_else(|| NormalizerError {
            code: "CER-NORM-PUBLISH-CONTRACT",
            message: "stored normalization execution_mode is invalid".to_string(),
            retryable: false,
        })?;
        headers.insert(
            EXECUTION_MODE_HEADER,
            execution_mode_header_value(execution_mode)?.to_string(),
        );
        if let Some(request_id) = request_id.filter(|value| !value.is_empty()) {
            headers.insert(REQUEST_ID_HEADER, request_id.to_string());
        }
        let ack = self
            .jetstream
            .publish_with_headers(NORMALIZED_CREATED_SUBJECT, headers, bytes.into())
            .await
            .map_err(transport_error)?
            .await
            .map_err(transport_error)?;
        if ack.stream != ANALYTICS_STREAM_NAME || ack.sequence == 0 {
            return Err(NormalizerError {
                code: "CER-NORM-PUBLISH-ACK",
                message: format!(
                    "unexpected JetStream PubAck stream={} sequence={}",
                    ack.stream, ack.sequence
                ),
                retryable: true,
            });
        }
        Ok(())
    }

    pub async fn publish_normalization_dlq(
        &self,
        record: &NormalizationDeadLetter,
    ) -> Result<(), NormalizerError> {
        let bytes = serde_json::to_vec(record).map_err(|error| NormalizerError {
            code: "CER-NORM-DLQ-ENCODE",
            message: error.to_string(),
            retryable: false,
        })?;
        let mut headers = HeaderMap::new();
        headers.insert(NATS_MESSAGE_ID_HEADER, record.nats_message_id());
        headers.insert(DLQ_SCHEMA_HEADER, record.schema_version.clone());
        if let Some(request_id) = record.request_id.as_ref() {
            headers.insert(REQUEST_ID_HEADER, request_id.clone());
        }
        let ack = self
            .jetstream
            .publish_with_headers(NORMALIZATION_DLQ_SUBJECT, headers, bytes.into())
            .await
            .map_err(transport_error)?
            .await
            .map_err(transport_error)?;
        if ack.stream != DLQ_STREAM_NAME || ack.sequence == 0 {
            return Err(NormalizerError {
                code: "CER-NORM-DLQ-PUBLISH-ACK",
                message: format!(
                    "unexpected normalization DLQ PubAck stream={} sequence={}",
                    ack.stream, ack.sequence
                ),
                retryable: true,
            });
        }
        Ok(())
    }
}

pub fn decode_raw_persisted(
    message: &async_nats::jetstream::Message,
) -> Result<IncomingRawPersisted, NormalizerError> {
    let envelope =
        CerberoEnvelope::decode(message.payload.as_ref()).map_err(|error| NormalizerError {
            code: "CER-NORM-ENVELOPE-DECODE",
            message: error.to_string(),
            retryable: false,
        })?;
    validate_envelope(&envelope).map_err(|error| NormalizerError {
        code: "CER-NORM-ENVELOPE-CONTRACT",
        message: error.to_string(),
        retryable: false,
    })?;
    if envelope.message_type != "RawEventPersisted"
        || envelope.payload_schema != "cerbero.raw_event_persisted.v1"
    {
        return Err(NormalizerError {
            code: "CER-NORM-UNSUPPORTED-CONTRACT",
            message: format!(
                "unsupported raw handoff type={} schema={}",
                envelope.message_type, envelope.payload_schema
            ),
            retryable: false,
        });
    }
    let payload = envelope.payload.as_ref().ok_or_else(|| NormalizerError {
        code: "CER-NORM-ENVELOPE-CONTRACT",
        message: "raw.persisted envelope payload is required".to_string(),
        retryable: false,
    })?;
    validate_type_url(payload)?;
    let persisted =
        RawEventPersisted::decode(payload.value.as_slice()).map_err(|error| NormalizerError {
            code: "CER-NORM-PAYLOAD-DECODE",
            message: error.to_string(),
            retryable: false,
        })?;
    validate_raw_event_persisted(&persisted).map_err(|error| NormalizerError {
        code: "CER-NORM-PAYLOAD-CONTRACT",
        message: error.to_string(),
        retryable: false,
    })?;
    if persisted.tenant_id != envelope.tenant_id {
        return Err(NormalizerError {
            code: "CER-NORM-TENANT-MISMATCH",
            message: "envelope tenant_id differs from RawEventPersisted tenant_id".to_string(),
            retryable: false,
        });
    }
    let request_id = message
        .headers
        .as_ref()
        .and_then(|headers| headers.get(REQUEST_ID_HEADER))
        .map(|value| value.as_str().to_string());
    Ok(IncomingRawPersisted {
        envelope,
        persisted,
        request_id,
    })
}

fn validate_type_url(payload: &Any) -> Result<(), NormalizerError> {
    if payload
        .type_url
        .ends_with("cerbero.contracts.v1.RawEventPersisted")
    {
        Ok(())
    } else {
        Err(NormalizerError {
            code: "CER-NORM-PAYLOAD-TYPE",
            message: format!("unexpected Any type_url {}", payload.type_url),
            retryable: false,
        })
    }
}

#[must_use]
pub fn retry_delay(deliveries: i64, min: Duration, max: Duration) -> Duration {
    let exponent = u32::try_from(deliveries.saturating_sub(1).clamp(0, 20)).unwrap_or(20);
    let factor = 1_u32.checked_shl(exponent).unwrap_or(u32::MAX);
    min.saturating_mul(factor).min(max)
}

#[must_use]
pub fn retry_delay_with_jitter(
    deliveries: i64,
    min: Duration,
    max: Duration,
    seed: &str,
) -> Duration {
    let base = retry_delay(deliveries, min, max);
    let digest = sha256_lower_hex(format!("{seed}:{deliveries}").as_bytes());
    let sample = u64::from_str_radix(&digest[..16], 16).unwrap_or(0);
    let percentage = 80_u128 + u128::from(sample % 41);
    let nanos = base.as_nanos().saturating_mul(percentage) / 100;
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
        .max(min)
        .min(max)
}

fn execution_mode_header_value(
    execution_mode: ExecutionMode,
) -> Result<&'static str, NormalizerError> {
    match execution_mode {
        ExecutionMode::Live => Ok("LIVE"),
        ExecutionMode::Replay => Ok("REPLAY"),
        ExecutionMode::Test => Ok("TEST"),
        ExecutionMode::Unspecified => Err(NormalizerError {
            code: "CER-NORM-EXECUTION-MODE",
            message: "execution mode must be LIVE, REPLAY, or TEST".to_string(),
            retryable: false,
        }),
    }
}

pub fn normalizer_consumer_name(
    execution_mode: ExecutionMode,
    replay_input: ReplayInput,
) -> Result<&'static str, NormalizerError> {
    normalizer_consumer_route(execution_mode, replay_input).map(|(name, _)| name)
}

fn normalizer_deliver_policy(
    execution_mode: ExecutionMode,
    replay_input: ReplayInput,
) -> DeliverPolicy {
    if execution_mode == ExecutionMode::Replay && replay_input == ReplayInput::Selective {
        DeliverPolicy::New
    } else {
        DeliverPolicy::All
    }
}

fn normalizer_consumer_route(
    execution_mode: ExecutionMode,
    replay_input: ReplayInput,
) -> Result<(&'static str, &'static str), NormalizerError> {
    match (execution_mode, replay_input) {
        (ExecutionMode::Live, _) => Ok((NORMALIZER_CONSUMER_NAME, RAW_PERSISTED_SUBJECT)),
        (ExecutionMode::Replay, ReplayInput::Historical) => {
            Ok((NORMALIZER_REPLAY_CONSUMER_NAME, RAW_PERSISTED_SUBJECT))
        }
        (ExecutionMode::Replay, ReplayInput::Selective) => Ok((
            NORMALIZER_SELECTIVE_REPLAY_CONSUMER_NAME,
            RAW_REPLAY_SUBJECT,
        )),
        (ExecutionMode::Test, _) => Ok((NORMALIZER_TEST_CONSUMER_NAME, RAW_PERSISTED_SUBJECT)),
        (ExecutionMode::Unspecified, _) => Err(NormalizerError {
            code: "CER-NORM-EXECUTION-MODE",
            message: "execution mode must be LIVE, REPLAY, or TEST".to_string(),
            retryable: false,
        }),
    }
}

fn transport_error(error: impl std::fmt::Display) -> NormalizerError {
    NormalizerError {
        code: "CER-NORM-NATS-UNAVAILABLE",
        message: error.to_string(),
        retryable: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_routes_keep_historical_and_selective_consumers_disjoint() {
        assert_eq!(
            normalizer_consumer_route(ExecutionMode::Replay, ReplayInput::Historical).unwrap(),
            (NORMALIZER_REPLAY_CONSUMER_NAME, RAW_PERSISTED_SUBJECT)
        );
        assert_eq!(
            normalizer_consumer_route(ExecutionMode::Replay, ReplayInput::Selective).unwrap(),
            (
                NORMALIZER_SELECTIVE_REPLAY_CONSUMER_NAME,
                RAW_REPLAY_SUBJECT
            )
        );
        assert_eq!(
            normalizer_consumer_route(ExecutionMode::Live, ReplayInput::Selective).unwrap(),
            (NORMALIZER_CONSUMER_NAME, RAW_PERSISTED_SUBJECT)
        );
    }

    #[test]
    fn selective_replay_starts_at_new_messages_only() {
        assert_eq!(
            normalizer_deliver_policy(ExecutionMode::Replay, ReplayInput::Selective),
            DeliverPolicy::New
        );
        assert_eq!(
            normalizer_deliver_policy(ExecutionMode::Replay, ReplayInput::Historical),
            DeliverPolicy::All
        );
        assert_eq!(
            normalizer_deliver_policy(ExecutionMode::Live, ReplayInput::Historical),
            DeliverPolicy::All
        );
    }

    #[test]
    fn jitter_is_deterministic_bounded_and_not_below_minimum() {
        let min = Duration::from_secs(1);
        let max = Duration::from_secs(30);
        let first = retry_delay_with_jitter(3, min, max, "message-a");
        let second = retry_delay_with_jitter(3, min, max, "message-a");
        assert_eq!(first, second);
        assert!(first >= min);
        assert!(first <= max);
    }
}
