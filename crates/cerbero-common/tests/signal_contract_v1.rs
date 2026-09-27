use cerbero_common::contracts::{
    v1::{Signal, SignalInput, SignalProvenance},
    validate_signal,
};
use prost_types::Timestamp;

const SIGNAL_ID: &str = "018f47a2-4b00-7a00-8000-000000000101";
const TENANT_ID: &str = "018f47a2-4b00-7a00-8000-000000000102";
const EVENT_ID: &str = "018f47a2-4b00-7a00-8000-000000000103";

fn timestamp(seconds: i64) -> Timestamp {
    Timestamp { seconds, nanos: 0 }
}

fn event_signal() -> Signal {
    Signal {
        signal_id: SIGNAL_ID.to_string(),
        tenant_id: TENANT_ID.to_string(),
        rule_id: "CER-DET-000001".to_string(),
        rule_version: "7".to_string(),
        rule_type: 1,
        severity: "HIGH".to_string(),
        confidence: Some("LOW".to_string()),
        status: 1,
        first_observed_at: Some(timestamp(10)),
        last_observed_at: Some(timestamp(10)),
        created_at: Some(timestamp(12)),
        execution_mode: 2,
        source_count: 1,
        event_count: 1,
        summary: "failed SSH login matched".to_string(),
        provenance: Some(SignalProvenance {
            evaluated_at: Some(timestamp(11)),
            execution_backend: 1,
        }),
        inputs: vec![SignalInput {
            signal_id: SIGNAL_ID.to_string(),
            input_type: 1,
            input_id: EVENT_ID.to_string(),
            relation: "MATCHED".to_string(),
            ordinal: 0,
        }],
    }
}

#[test]
fn canonical_event_signal_validates() {
    validate_signal(&event_signal()).expect("canonical EVENT Signal must validate");
}

#[test]
fn observed_time_is_never_half_present_or_reversed() {
    let mut signal = event_signal();
    signal.last_observed_at = None;
    let error = validate_signal(&signal).expect_err("half-present observed time must fail");
    assert_eq!(error.field, "first_observed_at");

    let mut signal = event_signal();
    signal.first_observed_at = Some(timestamp(20));
    let error = validate_signal(&signal).expect_err("reversed observed time must fail");
    assert_eq!(error.field, "first_observed_at");

    let mut signal = event_signal();
    signal.first_observed_at = None;
    signal.last_observed_at = None;
    validate_signal(&signal).expect("missing source event_time must remain representable");
}

#[test]
fn event_signal_requires_exact_event_input_semantics() {
    let mut signal = event_signal();
    signal.event_count = 2;
    let error = validate_signal(&signal).expect_err("EVENT event_count drift must fail");
    assert_eq!(error.field, "event_count");

    let mut signal = event_signal();
    signal.inputs[0].input_type = 3;
    let error =
        validate_signal(&signal).expect_err("EVENT must preserve its NormalizedEvent input");
    assert_eq!(error.field, "event_count");
}

#[test]
fn duplicate_or_misordered_inputs_fail_closed() {
    let mut signal = event_signal();
    let mut duplicate = signal.inputs[0].clone();
    duplicate.ordinal = 1;
    signal.inputs.push(duplicate);
    let error = validate_signal(&signal).expect_err("duplicate inputs must fail");
    assert_eq!(error.field, "inputs");

    let mut signal = event_signal();
    signal.inputs[0].ordinal = 1;
    let error = validate_signal(&signal).expect_err("ordinal drift must fail");
    assert_eq!(error.field, "inputs.ordinal");
}

#[test]
fn unspecified_execution_provenance_fails_closed() {
    let mut signal = event_signal();
    signal.execution_mode = 0;
    let error = validate_signal(&signal).expect_err("unspecified execution mode must fail");
    assert_eq!(error.field, "execution_mode");

    let mut signal = event_signal();
    signal.provenance.as_mut().unwrap().execution_backend = 0;
    let error = validate_signal(&signal).expect_err("unspecified execution backend must fail");
    assert_eq!(error.field, "provenance.execution_backend");
}
