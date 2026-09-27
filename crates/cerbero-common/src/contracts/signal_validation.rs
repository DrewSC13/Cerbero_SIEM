use std::collections::HashSet;

use prost_types::Timestamp;

use super::{
    ContractViolation,
    v1::{Signal, SignalInput},
    validate_timestamp, validate_uuid_v7,
};

const RULE_ID_PREFIX: &str = "CER-DET-";
const RULE_ID_DIGITS: usize = 6;
const SIGNAL_RULE_TYPE_EVENT: i32 = 1;
const SIGNAL_RULE_TYPE_THRESHOLD: i32 = 2;
const SIGNAL_RULE_TYPE_CORRELATION: i32 = 3;
const SIGNAL_INPUT_NORMALIZED_EVENT: i32 = 1;

fn violation(field: &'static str, reason: impl Into<String>) -> ContractViolation {
    ContractViolation {
        field,
        reason: reason.into(),
    }
}

fn require_text(field: &'static str, value: &str) -> Result<(), ContractViolation> {
    if value.trim().is_empty() {
        return Err(violation(field, "is required"));
    }
    Ok(())
}

fn validate_detection_rule_id(value: &str) -> Result<(), ContractViolation> {
    let Some(suffix) = value.strip_prefix(RULE_ID_PREFIX) else {
        return Err(violation(
            "rule_id",
            "must match CER-DET-XXXXXX with exactly six ASCII digits",
        ));
    };
    if suffix.len() != RULE_ID_DIGITS || !suffix.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(violation(
            "rule_id",
            "must match CER-DET-XXXXXX with exactly six ASCII digits",
        ));
    }
    Ok(())
}

fn timestamp_le(left: &Timestamp, right: &Timestamp) -> bool {
    (left.seconds, left.nanos) <= (right.seconds, right.nanos)
}

fn validate_signal_input(
    signal_id: &str,
    input: &SignalInput,
    expected_ordinal: usize,
) -> Result<(), ContractViolation> {
    if input.signal_id != signal_id {
        return Err(violation(
            "inputs.signal_id",
            "must equal the containing Signal.signal_id",
        ));
    }
    if !matches!(input.input_type, 1..=3) {
        return Err(violation(
            "inputs.input_type",
            "must be NORMALIZED_EVENT, SIGNAL, or ENTITY",
        ));
    }
    validate_uuid_v7("inputs.input_id", &input.input_id)?;
    require_text("inputs.relation", &input.relation)?;
    let expected_ordinal = u32::try_from(expected_ordinal).map_err(|_| {
        violation(
            "inputs.ordinal",
            "input list exceeds uint32 ordinal capacity",
        )
    })?;
    if input.ordinal != expected_ordinal {
        return Err(violation(
            "inputs.ordinal",
            format!("must equal zero-based input position {expected_ordinal}"),
        ));
    }
    Ok(())
}

fn validate_signal_identity(signal: &Signal) -> Result<(), ContractViolation> {
    validate_uuid_v7("signal_id", &signal.signal_id)?;
    validate_uuid_v7("tenant_id", &signal.tenant_id)?;
    validate_detection_rule_id(&signal.rule_id)?;
    require_text("rule_version", &signal.rule_version)?;
    if !matches!(
        signal.rule_type,
        SIGNAL_RULE_TYPE_EVENT | SIGNAL_RULE_TYPE_THRESHOLD | SIGNAL_RULE_TYPE_CORRELATION
    ) {
        return Err(violation(
            "rule_type",
            "must be EVENT, THRESHOLD, or CORRELATION",
        ));
    }
    require_text("severity", &signal.severity)?;
    if let Some(confidence) = signal.confidence.as_deref() {
        require_text("confidence", confidence)?;
    }
    if !matches!(signal.status, 1..=4) {
        return Err(violation(
            "status",
            "must be ACTIVE, SUPPRESSED, PROMOTED, or INVALIDATED",
        ));
    }
    if !matches!(signal.execution_mode, 1..=3) {
        return Err(violation(
            "execution_mode",
            "must distinguish LIVE, REPLAY, or TEST",
        ));
    }
    Ok(())
}

fn validate_signal_times(signal: &Signal) -> Result<(), ContractViolation> {
    match (
        signal.first_observed_at.as_ref(),
        signal.last_observed_at.as_ref(),
    ) {
        (None, None) => {}
        (Some(first), Some(last)) => {
            validate_timestamp("first_observed_at", first)?;
            validate_timestamp("last_observed_at", last)?;
            if !timestamp_le(first, last) {
                return Err(violation(
                    "first_observed_at",
                    "must be less than or equal to last_observed_at",
                ));
            }
        }
        _ => {
            return Err(violation(
                "first_observed_at",
                "first_observed_at and last_observed_at must both be present or both be absent",
            ));
        }
    }

    let created_at = signal
        .created_at
        .as_ref()
        .ok_or_else(|| violation("created_at", "is required"))?;
    validate_timestamp("created_at", created_at)
}

fn validate_signal_provenance(signal: &Signal) -> Result<(), ContractViolation> {
    let provenance = signal
        .provenance
        .as_ref()
        .ok_or_else(|| violation("provenance", "is required"))?;
    let evaluated_at = provenance
        .evaluated_at
        .as_ref()
        .ok_or_else(|| violation("provenance.evaluated_at", "is required"))?;
    validate_timestamp("provenance.evaluated_at", evaluated_at)?;
    if !matches!(provenance.execution_backend, 1..=2) {
        return Err(violation(
            "provenance.execution_backend",
            "must be CLICKHOUSE or STREAM",
        ));
    }
    Ok(())
}

fn validate_signal_counts(signal: &Signal) -> Result<(), ContractViolation> {
    if signal.source_count == 0 {
        return Err(violation("source_count", "must be greater than zero"));
    }
    if signal.event_count == 0 {
        return Err(violation("event_count", "must be greater than zero"));
    }
    require_text("summary", &signal.summary)
}

fn validate_signal_inputs(signal: &Signal) -> Result<u64, ContractViolation> {
    if signal.inputs.is_empty() {
        return Err(violation(
            "inputs",
            "must preserve at least one contributing object",
        ));
    }
    let mut unique_inputs = HashSet::with_capacity(signal.inputs.len());
    let mut normalized_event_inputs = 0_u64;
    for (index, input) in signal.inputs.iter().enumerate() {
        validate_signal_input(&signal.signal_id, input, index)?;
        if !unique_inputs.insert((input.input_type, input.input_id.as_str())) {
            return Err(violation(
                "inputs",
                "must not contain duplicate input_type/input_id pairs",
            ));
        }
        if input.input_type == SIGNAL_INPUT_NORMALIZED_EVENT {
            normalized_event_inputs += 1;
        }
    }
    Ok(normalized_event_inputs)
}

fn validate_signal_rule_counts(
    signal: &Signal,
    normalized_event_inputs: u64,
) -> Result<(), ContractViolation> {
    match signal.rule_type {
        SIGNAL_RULE_TYPE_EVENT => {
            if normalized_event_inputs != 1 || signal.event_count != 1 {
                return Err(violation(
                    "event_count",
                    "EVENT Signals require exactly one NORMALIZED_EVENT input and event_count = 1",
                ));
            }
        }
        SIGNAL_RULE_TYPE_THRESHOLD => {
            if normalized_event_inputs == 0 || normalized_event_inputs != signal.event_count {
                return Err(violation(
                    "event_count",
                    "THRESHOLD Signals require one input per contributing NormalizedEvent",
                ));
            }
        }
        SIGNAL_RULE_TYPE_CORRELATION => return Ok(()),
        _ => unreachable!("rule_type was validated above"),
    }

    if signal.source_count > signal.event_count {
        return Err(violation(
            "source_count",
            "cannot exceed event_count for EVENT or THRESHOLD Signals",
        ));
    }
    Ok(())
}

/// Validates the canonical Signal v1 wire invariants.
///
/// # Errors
///
/// Returns a [`ContractViolation`] when identity, enum, temporal, count, or input-provenance
/// invariants do not satisfy ADR-0016.
pub fn validate_signal(signal: &Signal) -> Result<(), ContractViolation> {
    validate_signal_identity(signal)?;
    validate_signal_times(signal)?;
    validate_signal_counts(signal)?;
    validate_signal_provenance(signal)?;
    let normalized_event_inputs = validate_signal_inputs(signal)?;
    validate_signal_rule_counts(signal, normalized_event_inputs)
}
