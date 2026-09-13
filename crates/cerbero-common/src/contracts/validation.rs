use std::fmt::{self, Write as _};

use prost_types::Timestamp;
use sha2::{Digest, Sha256};

use super::v1::{CerberoEnvelope, NormalizedEvent, RawEvent, Transformation};

const PROTO_TIMESTAMP_MIN_SECONDS: i64 = -62_135_596_800;
const PROTO_TIMESTAMP_MAX_SECONDS: i64 = 253_402_300_799;
const PROTO_TIMESTAMP_MAX_NANOS: i32 = 999_999_999;

/// A stable contract validation failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContractViolation {
    /// Contract field that violated an invariant.
    pub field: &'static str,
    /// Human-readable reason suitable for diagnostics, not programmatic branching.
    pub reason: String,
}

impl ContractViolation {
    fn new(field: &'static str, reason: impl Into<String>) -> Self {
        Self {
            field,
            reason: reason.into(),
        }
    }
}

impl fmt::Display for ContractViolation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.field, self.reason)
    }
}

impl std::error::Error for ContractViolation {}

/// Validates a canonical RFC 9562 `UUIDv7` string.
///
/// # Errors
///
/// Returns a [`ContractViolation`] when the value is not a canonical hyphenated `UUIDv7` using the
/// RFC variant.
pub fn validate_uuid_v7(field: &'static str, value: &str) -> Result<(), ContractViolation> {
    let bytes = value.as_bytes();
    if bytes.len() != 36
        || bytes[8] != b'-'
        || bytes[13] != b'-'
        || bytes[18] != b'-'
        || bytes[23] != b'-'
    {
        return Err(ContractViolation::new(
            field,
            "must be a canonical UUID string",
        ));
    }

    for (index, byte) in bytes.iter().copied().enumerate() {
        if matches!(index, 8 | 13 | 18 | 23) {
            continue;
        }
        if !byte.is_ascii_hexdigit() {
            return Err(ContractViolation::new(
                field,
                "contains non-hexadecimal characters",
            ));
        }
    }

    if bytes[14] != b'7' {
        return Err(ContractViolation::new(field, "must use UUID version 7"));
    }

    if !matches!(bytes[19].to_ascii_lowercase(), b'8' | b'9' | b'a' | b'b') {
        return Err(ContractViolation::new(
            field,
            "must use the RFC UUID variant",
        ));
    }

    Ok(())
}

/// Validates a Protobuf timestamp range.
///
/// # Errors
///
/// Returns a [`ContractViolation`] when seconds or nanoseconds fall outside the Protobuf Timestamp
/// contract.
pub fn validate_timestamp(
    field: &'static str,
    timestamp: &Timestamp,
) -> Result<(), ContractViolation> {
    if !(PROTO_TIMESTAMP_MIN_SECONDS..=PROTO_TIMESTAMP_MAX_SECONDS).contains(&timestamp.seconds) {
        return Err(ContractViolation::new(
            field,
            "seconds are outside the Protobuf Timestamp range",
        ));
    }
    if !(0..=PROTO_TIMESTAMP_MAX_NANOS).contains(&timestamp.nanos) {
        return Err(ContractViolation::new(
            field,
            "nanoseconds must be between 0 and 999999999",
        ));
    }
    Ok(())
}

/// Returns the lowercase hexadecimal SHA-256 digest of the exact supplied bytes.
#[must_use]
pub fn sha256_lower_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

fn validate_required_timestamp(
    field: &'static str,
    timestamp: Option<&Timestamp>,
) -> Result<(), ContractViolation> {
    let timestamp = timestamp.ok_or_else(|| ContractViolation::new(field, "is required"))?;
    validate_timestamp(field, timestamp)
}

fn validate_sha256(
    algorithm_field: &'static str,
    hash_field: &'static str,
    algorithm: &str,
    hash: &str,
    bytes: &[u8],
) -> Result<(), ContractViolation> {
    if algorithm != "sha256" {
        return Err(ContractViolation::new(algorithm_field, "must equal sha256"));
    }
    if hash.len() != 64
        || !hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(ContractViolation::new(
            hash_field,
            "must be a 64-character lowercase hexadecimal SHA-256 digest",
        ));
    }
    if sha256_lower_hex(bytes) != hash {
        return Err(ContractViolation::new(
            hash_field,
            "does not match the exact raw payload bytes",
        ));
    }
    Ok(())
}

/// Validates the M1 invariants that apply to an event-bus envelope.
///
/// # Errors
///
/// Returns a [`ContractViolation`] when the version, message identity, producer identity,
/// timestamp, payload schema, or payload is missing or invalid.
pub fn validate_envelope(envelope: &CerberoEnvelope) -> Result<(), ContractViolation> {
    if envelope.contract_version != "1" {
        return Err(ContractViolation::new(
            "contract_version",
            "must equal 1 for v1 envelopes",
        ));
    }
    validate_uuid_v7("message_id", &envelope.message_id)?;
    if envelope.message_type.is_empty() {
        return Err(ContractViolation::new("message_type", "is required"));
    }

    let producer = envelope
        .producer
        .as_ref()
        .ok_or_else(|| ContractViolation::new("producer", "is required"))?;
    if producer.component.is_empty() {
        return Err(ContractViolation::new("producer.component", "is required"));
    }
    if producer.component_version.is_empty() {
        return Err(ContractViolation::new(
            "producer.component_version",
            "is required",
        ));
    }

    validate_required_timestamp("emitted_at", envelope.emitted_at.as_ref())?;
    if envelope.payload_schema.is_empty() {
        return Err(ContractViolation::new("payload_schema", "is required"));
    }
    if envelope.payload.is_none() {
        return Err(ContractViolation::new("payload", "is required"));
    }
    Ok(())
}

/// Validates the immutable raw-evidence invariants available in M1.
///
/// `event_time` is source-declared metadata and may be absent; when present its Protobuf range is
/// validated. `ingest_time`, raw byte length, and SHA-256 are authoritative acquisition metadata.
///
/// # Errors
///
/// Returns a [`ContractViolation`] when identity, timestamp, byte count, or raw hash invariants
/// fail.
pub fn validate_raw_event(event: &RawEvent) -> Result<(), ContractViolation> {
    validate_uuid_v7("event_id", &event.event_id)?;
    if let Some(event_time) = event.event_time.as_ref() {
        validate_timestamp("event_time", event_time)?;
    }
    validate_required_timestamp("ingest_time", event.ingest_time.as_ref())?;

    let payload_len = u64::try_from(event.raw_payload.len())
        .map_err(|_| ContractViolation::new("raw_size", "payload length does not fit uint64"))?;
    if event.raw_size != payload_len {
        return Err(ContractViolation::new(
            "raw_size",
            "does not match raw_payload byte length",
        ));
    }

    validate_sha256(
        "raw_hash_algorithm",
        "raw_hash",
        &event.raw_hash_algorithm,
        &event.raw_hash,
        &event.raw_payload,
    )
}

/// Validates the identity and timestamp invariants of a normalized derivation.
///
/// Hash scope and the exact OCSF version remain governed by the parsing/OCSF milestone and are not
/// invented here.
///
/// # Errors
///
/// Returns a [`ContractViolation`] when IDs or the normalization timestamp are invalid.
pub fn validate_normalized_event(event: &NormalizedEvent) -> Result<(), ContractViolation> {
    validate_uuid_v7("normalized_event_id", &event.normalized_event_id)?;
    validate_uuid_v7("raw_event_id", &event.raw_event_id)?;
    validate_required_timestamp("normalized_at", event.normalized_at.as_ref())
}

/// Validates the M1 provenance invariants for a transformation.
///
/// # Errors
///
/// Returns a [`ContractViolation`] when transformation identity, timestamps, status, execution
/// mode, or failed-transformation error provenance are invalid.
pub fn validate_transformation(transformation: &Transformation) -> Result<(), ContractViolation> {
    validate_uuid_v7("transformation_id", &transformation.transformation_id)?;
    validate_required_timestamp("started_at", transformation.started_at.as_ref())?;
    validate_required_timestamp("completed_at", transformation.completed_at.as_ref())?;

    if transformation.status == 0 {
        return Err(ContractViolation::new("status", "must not be unspecified"));
    }
    if transformation.execution_mode == 0 {
        return Err(ContractViolation::new(
            "execution_mode",
            "must distinguish LIVE, REPLAY, or TEST",
        ));
    }
    if transformation.status == 2 && transformation.error.is_none() {
        return Err(ContractViolation::new(
            "error",
            "is required when transformation status is FAILED",
        ));
    }
    Ok(())
}
