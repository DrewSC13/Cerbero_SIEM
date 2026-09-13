use cerbero_common::contracts::{
    sha256_lower_hex, v1, validate_envelope, validate_raw_event, validate_timestamp,
    validate_transformation, validate_uuid_v7,
};
use prost::Message;
use prost_types::{Any, Timestamp};

const EVENT_ID: &str = "018f47d0-7b5c-7cc0-98c0-3f2b9859d3e1";
const MESSAGE_ID: &str = "018f47d0-7b5c-7cc1-98c0-3f2b9859d3e2";
const TRANSFORMATION_ID: &str = "018f47d0-7b5c-7cc2-98c0-3f2b9859d3e3";

fn timestamp() -> Timestamp {
    Timestamp {
        seconds: 1,
        nanos: 0,
    }
}

fn raw_event() -> v1::RawEvent {
    let payload = b"abc".to_vec();
    v1::RawEvent {
        event_id: EVENT_ID.to_owned(),
        tenant_id: "tenant-a".to_owned(),
        source_id: "source-a".to_owned(),
        sensor_id: "sensor-a".to_owned(),
        event_time: None,
        ingest_time: Some(timestamp()),
        content_type: "text/plain".to_owned(),
        encoding: "utf-8".to_owned(),
        raw_payload: payload.clone(),
        raw_size: u64::try_from(payload.len()).expect("fixture size fits uint64"),
        raw_hash_algorithm: "sha256".to_owned(),
        raw_hash: sha256_lower_hex(&payload),
        transport: "test".to_owned(),
        remote_identity: "fixture".to_owned(),
        sequence_number: Some(42),
        integrity_status: 1,
        pipeline_version: "v1".to_owned(),
    }
}

fn decode_hex(input: &str) -> Vec<u8> {
    let input = input.trim();
    assert_eq!(input.len() % 2, 0);
    (0..input.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&input[index..index + 2], 16).expect("valid fixture hex"))
        .collect()
}

#[test]
fn shared_wire_fixture_round_trips() {
    let fixture = decode_hex(include_str!(
        "../../../tests/fixtures/contracts/v1/raw_event_minimal.hex"
    ));
    let decoded = v1::RawEvent::decode(fixture.as_slice()).expect("fixture must decode");
    validate_raw_event(&decoded).expect("fixture must satisfy contract invariants");

    let mut encoded = Vec::new();
    decoded.encode(&mut encoded).expect("message must encode");
    assert_eq!(encoded, fixture);
}

#[test]
fn raw_hash_is_over_exact_bytes_and_mismatch_is_rejected() {
    let mut event = raw_event();
    validate_raw_event(&event).expect("valid fixture");

    event.raw_payload.push(b'\n');
    event.raw_size += 1;
    let violation =
        validate_raw_event(&event).expect_err("changed bytes must fail hash validation");
    assert_eq!(violation.field, "raw_hash");
}

#[test]
fn raw_size_mismatch_is_invalid_payload_metadata() {
    let fixture = decode_hex(include_str!(
        "../../../tests/fixtures/contracts/v1/raw_event_invalid_size.hex"
    ));
    let event = v1::RawEvent::decode(fixture.as_slice()).expect("invalid fixture must decode");
    let violation = validate_raw_event(&event).expect_err("mismatched byte count must fail");
    assert_eq!(violation.field, "raw_size");
}

#[test]
fn duplicate_delivery_keeps_the_same_message_id() {
    let raw = raw_event();
    let mut raw_bytes = Vec::new();
    raw.encode(&mut raw_bytes).expect("raw event encodes");
    let envelope = v1::CerberoEnvelope {
        contract_version: "1".to_owned(),
        message_id: MESSAGE_ID.to_owned(),
        message_type: "RawEventReceived".to_owned(),
        tenant_id: "tenant-a".to_owned(),
        producer: Some(v1::Producer {
            component: "cerbero-ingest".to_owned(),
            component_version: "0.0.0".to_owned(),
            instance_id: "instance-a".to_owned(),
        }),
        emitted_at: Some(timestamp()),
        trace_id: "trace-a".to_owned(),
        causation_id: String::new(),
        correlation_id: String::new(),
        payload_schema: "cerbero.raw_event.v1".to_owned(),
        payload: Some(Any {
            type_url: "type.googleapis.com/cerbero.contracts.v1.RawEvent".to_owned(),
            value: raw_bytes,
        }),
    };
    validate_envelope(&envelope).expect("envelope is valid");

    let first = envelope.encode_to_vec();
    let redelivery = v1::CerberoEnvelope::decode(first.as_slice()).expect("redelivery decodes");
    assert_eq!(redelivery.message_id, MESSAGE_ID);
}

#[test]
fn execution_modes_survive_serialization() {
    for execution_mode in [1, 2, 3] {
        let transformation = v1::Transformation {
            transformation_id: TRANSFORMATION_ID.to_owned(),
            input_object_id: EVENT_ID.to_owned(),
            input_object_type: "RawEvent".to_owned(),
            output_object_id: String::new(),
            output_object_type: String::new(),
            component: "cerbero-normalizer".to_owned(),
            component_version: "0.0.0".to_owned(),
            configuration_hash: "fixture".to_owned(),
            started_at: Some(timestamp()),
            completed_at: Some(timestamp()),
            status: 1,
            error: None,
            execution_mode,
        };
        validate_transformation(&transformation).expect("execution provenance is valid");

        let encoded = transformation.encode_to_vec();
        let decoded =
            v1::Transformation::decode(encoded.as_slice()).expect("transformation decodes");
        assert_eq!(decoded.execution_mode, execution_mode);
    }
}

#[test]
fn invalid_uuid_timestamp_and_missing_payload_are_rejected() {
    let uuid_violation = validate_uuid_v7("message_id", "67e55044-10b1-426f-9247-bb680e5fe0c8")
        .expect_err("UUIDv4 must not satisfy a UUIDv7 field");
    assert_eq!(uuid_violation.field, "message_id");

    let timestamp_violation = validate_timestamp(
        "ingest_time",
        &Timestamp {
            seconds: 1,
            nanos: -1,
        },
    )
    .expect_err("negative nanos must be rejected");
    assert_eq!(timestamp_violation.field, "ingest_time");

    let envelope = v1::CerberoEnvelope {
        contract_version: "1".to_owned(),
        message_id: MESSAGE_ID.to_owned(),
        message_type: "RawEventReceived".to_owned(),
        tenant_id: "tenant-a".to_owned(),
        producer: Some(v1::Producer {
            component: "cerbero-ingest".to_owned(),
            component_version: "0.0.0".to_owned(),
            instance_id: "instance-a".to_owned(),
        }),
        emitted_at: Some(timestamp()),
        trace_id: String::new(),
        causation_id: String::new(),
        correlation_id: String::new(),
        payload_schema: "cerbero.raw_event.v1".to_owned(),
        payload: None,
    };
    let payload_violation = validate_envelope(&envelope).expect_err("missing payload must fail");
    assert_eq!(payload_violation.field, "payload");
}

#[test]
fn parser_failure_preserves_raw_event_and_error_provenance() {
    let raw = raw_event();
    let before = raw.encode_to_vec();
    let transformation = v1::Transformation {
        transformation_id: TRANSFORMATION_ID.to_owned(),
        input_object_id: raw.event_id.clone(),
        input_object_type: "RawEvent".to_owned(),
        output_object_id: String::new(),
        output_object_type: String::new(),
        component: "cerbero-normalizer".to_owned(),
        component_version: "0.0.0".to_owned(),
        configuration_hash: "fixture".to_owned(),
        started_at: Some(timestamp()),
        completed_at: Some(timestamp()),
        status: 2,
        error: Some(v1::CerberoError {
            code: "CER-PARSE-FAILED".to_owned(),
            category: 6,
            message: "fixture parser failure".to_owned(),
            retryable: false,
            component: "cerbero-normalizer".to_owned(),
            request_id: String::new(),
            metadata: std::collections::HashMap::default(),
        }),
        execution_mode: 1,
    };
    validate_transformation(&transformation)
        .expect("failed transformation carries error provenance");

    assert_eq!(
        raw.encode_to_vec(),
        before,
        "parser failure must not mutate RawEvent"
    );
}
