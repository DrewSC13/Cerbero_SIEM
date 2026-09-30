use cerbero_common::contracts::{
    v1::{Finding, FindingCorrelationProvenance, FindingInput},
    validate_finding,
};
use prost_types::Timestamp;

const FINDING_ID: &str = "018f47a2-4b00-7a00-8000-000000000601";
const TENANT_ID: &str = "018f47a2-4b00-7a00-8000-000000000602";
const SIGNAL_A: &str = "018f47a2-4b00-7a00-8000-000000000603";
const SIGNAL_B: &str = "018f47a2-4b00-7a00-8000-000000000604";
const SIGNAL_C: &str = "018f47a2-4b00-7a00-8000-000000000605";

fn timestamp(seconds: i64) -> Timestamp {
    Timestamp { seconds, nanos: 0 }
}

fn correlation_finding() -> Finding {
    let input_ids = [SIGNAL_A, SIGNAL_B, SIGNAL_C];
    Finding {
        finding_id: FINDING_ID.to_string(),
        tenant_id: TENANT_ID.to_string(),
        finding_type: 3,
        status: 1,
        severity: "HIGH".to_string(),
        confidence: Some("MEDIUM".to_string()),
        title: "successful login after failures followed by privilege escalation".to_string(),
        description: "three-stage bounded sequence correlation matched".to_string(),
        first_seen: Some(timestamp(100)),
        last_seen: Some(timestamp(220)),
        created_at: Some(timestamp(230)),
        updated_at: Some(timestamp(230)),
        primary_rule_id: None,
        primary_rule_version: None,
        correlation_rule_id: Some("CER-COR-0003".to_string()),
        correlation_rule_version: Some("2".to_string()),
        disposition: 1,
        metadata: None,
        execution_mode: 1,
        inputs: input_ids
            .iter()
            .map(|input_id| FindingInput {
                finding_id: FINDING_ID.to_string(),
                input_type: "SIGNAL".to_string(),
                input_id: (*input_id).to_string(),
                relation: "CORRELATION_INPUT".to_string(),
            })
            .collect(),
        correlation_provenance: Some(FindingCorrelationProvenance {
            correlation_rule_id: "CER-COR-0003".to_string(),
            correlation_rule_version: "2".to_string(),
            window_start: Some(timestamp(100)),
            window_end: Some(timestamp(220)),
            input_ids: input_ids.iter().map(|value| (*value).to_string()).collect(),
            output_finding_id: FINDING_ID.to_string(),
            execution_backend: 1,
            execution_mode: 1,
            configuration_hash: "sequence-config-sha256".to_string(),
        }),
    }
}

#[test]
fn canonical_correlation_finding_validates() {
    validate_finding(&correlation_finding()).expect("canonical CORRELATION Finding must validate");
}

#[test]
fn seen_time_is_never_half_present_or_reversed() {
    let mut finding = correlation_finding();
    finding.last_seen = None;
    let error = validate_finding(&finding).expect_err("half-present seen time must fail");
    assert_eq!(error.field, "first_seen");

    let mut finding = correlation_finding();
    finding.first_seen = Some(timestamp(300));
    let error = validate_finding(&finding).expect_err("reversed seen time must fail");
    assert_eq!(error.field, "first_seen");

    let mut finding = correlation_finding();
    finding.first_seen = None;
    finding.last_seen = None;
    validate_finding(&finding).expect("absence of source-observation bounds remains representable");
}

#[test]
fn finding_inputs_are_explicit_and_duplicate_safe() {
    let mut finding = correlation_finding();
    finding.inputs[0].finding_id = SIGNAL_A.to_string();
    let error = validate_finding(&finding).expect_err("foreign containing identity must fail");
    assert_eq!(error.field, "inputs.finding_id");

    let mut finding = correlation_finding();
    finding.inputs.push(finding.inputs[0].clone());
    let error = validate_finding(&finding).expect_err("exact duplicate relation must fail");
    assert_eq!(error.field, "inputs");
}

#[test]
fn correlation_provenance_must_bind_output_rules_inputs_and_execution_domain() {
    let mut finding = correlation_finding();
    finding
        .correlation_provenance
        .as_mut()
        .unwrap()
        .output_finding_id = SIGNAL_A.to_string();
    let error = validate_finding(&finding).expect_err("wrong output identity must fail");
    assert_eq!(error.field, "correlation_provenance.output_finding_id");

    let mut finding = correlation_finding();
    finding.correlation_provenance.as_mut().unwrap().input_ids[0] =
        "018f47a2-4b00-7a00-8000-000000000699".to_string();
    let error = validate_finding(&finding).expect_err("unbacked correlation input must fail");
    assert_eq!(error.field, "correlation_provenance.input_ids");

    let mut finding = correlation_finding();
    finding
        .correlation_provenance
        .as_mut()
        .unwrap()
        .execution_mode = 2;
    let error = validate_finding(&finding).expect_err("execution-domain mismatch must fail");
    assert_eq!(error.field, "correlation_provenance.execution_mode");
}

#[test]
fn non_correlation_finding_cannot_smuggle_correlation_provenance() {
    let mut finding = correlation_finding();
    finding.finding_type = 4;
    finding.correlation_rule_id = None;
    finding.correlation_rule_version = None;
    let error = validate_finding(&finding)
        .expect_err("ANALYTICAL Finding cannot carry correlation provenance");
    assert_eq!(error.field, "correlation_provenance");
}

#[test]
fn unspecified_lifecycle_values_fail_closed() {
    let mut finding = correlation_finding();
    finding.status = 0;
    let error = validate_finding(&finding).expect_err("unspecified status must fail");
    assert_eq!(error.field, "status");

    let mut finding = correlation_finding();
    finding.disposition = 0;
    let error = validate_finding(&finding).expect_err("unspecified disposition must fail");
    assert_eq!(error.field, "disposition");

    let mut finding = correlation_finding();
    finding.execution_mode = 0;
    let error = validate_finding(&finding).expect_err("unspecified execution mode must fail");
    assert_eq!(error.field, "execution_mode");
}
