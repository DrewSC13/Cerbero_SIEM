use std::time::Duration;

use async_nats::HeaderMap;
use async_nats::jetstream;
use async_nats::jetstream::consumer::{AckPolicy, PullConsumer, pull};
use prost::Message as _;
use prost_types::Any;

use cerbero_common::contracts::v1::{CerberoEnvelope, Producer, RawEventPersisted};
use cerbero_common::contracts::{validate_envelope, validate_raw_event_persisted};

use crate::{NormalizerError, StoredNormalization};

pub const RAW_STREAM_NAME: &str = "CERBERO_RAW";
pub const ANALYTICS_STREAM_NAME: &str = "CERBERO_ANALYTICS";
pub const RAW_PERSISTED_SUBJECT: &str = "cerbero.v1.raw.persisted";
pub const NORMALIZED_CREATED_SUBJECT: &str = "cerbero.v1.normalized.created";
pub const NORMALIZER_CONSUMER_NAME: &str = "normalizer";
pub const REQUEST_ID_HEADER: &str = "Cerbero-Request-Id";
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

    pub async fn consumer(&self) -> Result<PullConsumer, NormalizerError> {
        let stream = self
            .jetstream
            .get_stream(RAW_STREAM_NAME)
            .await
            .map_err(transport_error)?;
        stream
            .get_or_create_consumer(
                NORMALIZER_CONSUMER_NAME,
                pull::Config {
                    durable_name: Some(NORMALIZER_CONSUMER_NAME.to_string()),
                    ack_policy: AckPolicy::Explicit,
                    filter_subject: RAW_PERSISTED_SUBJECT.to_string(),
                    max_ack_pending: 1,
                    ..Default::default()
                },
            )
            .await
            .map_err(transport_error)
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

fn transport_error(error: impl std::fmt::Display) -> NormalizerError {
    NormalizerError {
        code: "CER-NORM-NATS-UNAVAILABLE",
        message: error.to_string(),
        retryable: true,
    }
}
