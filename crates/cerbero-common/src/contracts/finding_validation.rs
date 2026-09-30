use std::collections::HashSet;

use prost_types::Timestamp;

use super::{
    ContractViolation,
    v1::{Finding, FindingCorrelationProvenance, FindingInput},
    validate_timestamp, validate_uuid_v7,
};

const FINDING_TYPE_DETECTION: i32 = 1;
const FINDING_TYPE_THRESHOLD: i32 = 2;
const FINDING_TYPE_CORRELATION: i32 = 3;
const FINDING_TYPE_ANALYTICAL: i32 = 4;

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

fn timestamp_le(left: &Timestamp, right: &Timestamp) -> bool {
    (left.seconds, left.nanos) <= (right.seconds, right.nanos)
}

fn validate_optional_pair(
    first_field: &'static str,
    first: Option<&str>,
    second_field: &'static str,
    second: Option<&str>,
) -> Result<(), ContractViolation> {
    match (first, second) {
        (None, None) => Ok(()),
        (Some(first), Some(second)) => {
            require_text(first_field, first)?;
            require_text(second_field, second)
        }
        _ => Err(violation(
            first_field,
            format!("{first_field} and {second_field} must both be present or both be absent"),
        )),
    }
}

fn validate_seen_pair(finding: &Finding) -> Result<(), ContractViolation> {
    match (finding.first_seen.as_ref(), finding.last_seen.as_ref()) {
        (None, None) => Ok(()),
        (Some(first), Some(last)) => {
            validate_timestamp("first_seen", first)?;
            validate_timestamp("last_seen", last)?;
            if !timestamp_le(first, last) {
                return Err(violation(
                    "first_seen",
                    "must be less than or equal to last_seen",
                ));
            }
            Ok(())
        }
        _ => Err(violation(
            "first_seen",
            "first_seen and last_seen must both be present or both be absent",
        )),
    }
}

fn validate_required_times(finding: &Finding) -> Result<(), ContractViolation> {
    let created_at = finding
        .created_at
        .as_ref()
        .ok_or_else(|| violation("created_at", "is required"))?;
    validate_timestamp("created_at", created_at)?;
    let updated_at = finding
        .updated_at
        .as_ref()
        .ok_or_else(|| violation("updated_at", "is required"))?;
    validate_timestamp("updated_at", updated_at)
}

fn validate_finding_input(finding_id: &str, input: &FindingInput) -> Result<(), ContractViolation> {
    if input.finding_id != finding_id {
        return Err(violation(
            "inputs.finding_id",
            "must equal the containing Finding.finding_id",
        ));
    }
    require_text("inputs.input_type", &input.input_type)?;
    validate_uuid_v7("inputs.input_id", &input.input_id)?;
    require_text("inputs.relation", &input.relation)
}

fn validate_inputs(finding: &Finding) -> Result<HashSet<String>, ContractViolation> {
    if finding.inputs.is_empty() {
        return Err(violation(
            "inputs",
            "must preserve at least one contributing object",
        ));
    }

    let mut exact_relations = HashSet::with_capacity(finding.inputs.len());
    let mut input_ids = HashSet::with_capacity(finding.inputs.len());
    for input in &finding.inputs {
        validate_finding_input(&finding.finding_id, input)?;
        if !exact_relations.insert((
            input.input_type.as_str(),
            input.input_id.as_str(),
            input.relation.as_str(),
        )) {
            return Err(violation(
                "inputs",
                "must not contain duplicate input_type/input_id/relation entries",
            ));
        }
        input_ids.insert(input.input_id.clone());
    }
    Ok(input_ids)
}

fn validate_correlation_provenance(
    finding: &Finding,
    provenance: &FindingCorrelationProvenance,
    finding_input_ids: &HashSet<String>,
) -> Result<(), ContractViolation> {
    let correlation_rule_id = finding.correlation_rule_id.as_deref().ok_or_else(|| {
        violation(
            "correlation_rule_id",
            "is required for CORRELATION Findings",
        )
    })?;
    let correlation_rule_version =
        finding.correlation_rule_version.as_deref().ok_or_else(|| {
            violation(
                "correlation_rule_version",
                "is required for CORRELATION Findings",
            )
        })?;

    if provenance.correlation_rule_id != correlation_rule_id {
        return Err(violation(
            "correlation_provenance.correlation_rule_id",
            "must equal Finding.correlation_rule_id",
        ));
    }
    if provenance.correlation_rule_version != correlation_rule_version {
        return Err(violation(
            "correlation_provenance.correlation_rule_version",
            "must equal Finding.correlation_rule_version",
        ));
    }

    let window_start = provenance
        .window_start
        .as_ref()
        .ok_or_else(|| violation("correlation_provenance.window_start", "is required"))?;
    let window_end = provenance
        .window_end
        .as_ref()
        .ok_or_else(|| violation("correlation_provenance.window_end", "is required"))?;
    validate_timestamp("correlation_provenance.window_start", window_start)?;
    validate_timestamp("correlation_provenance.window_end", window_end)?;
    if !timestamp_le(window_start, window_end) {
        return Err(violation(
            "correlation_provenance.window_start",
            "must be less than or equal to window_end",
        ));
    }

    if provenance.input_ids.is_empty() {
        return Err(violation(
            "correlation_provenance.input_ids",
            "must preserve at least one contributing object identity",
        ));
    }
    let mut provenance_ids = HashSet::with_capacity(provenance.input_ids.len());
    for input_id in &provenance.input_ids {
        validate_uuid_v7("correlation_provenance.input_ids", input_id)?;
        if !provenance_ids.insert(input_id.as_str()) {
            return Err(violation(
                "correlation_provenance.input_ids",
                "must not contain duplicate object identities",
            ));
        }
        if !finding_input_ids.contains(input_id) {
            return Err(violation(
                "correlation_provenance.input_ids",
                "every correlation input_id must be backed by Finding.inputs",
            ));
        }
    }

    if provenance.output_finding_id != finding.finding_id {
        return Err(violation(
            "correlation_provenance.output_finding_id",
            "must equal Finding.finding_id",
        ));
    }
    if !matches!(provenance.execution_backend, 1..=2) {
        return Err(violation(
            "correlation_provenance.execution_backend",
            "must be CLICKHOUSE or STREAM",
        ));
    }
    if provenance.execution_mode != finding.execution_mode {
        return Err(violation(
            "correlation_provenance.execution_mode",
            "must equal Finding.execution_mode",
        ));
    }
    require_text(
        "correlation_provenance.configuration_hash",
        &provenance.configuration_hash,
    )
}

fn validate_type_specific_provenance(
    finding: &Finding,
    finding_input_ids: &HashSet<String>,
) -> Result<(), ContractViolation> {
    match finding.finding_type {
        FINDING_TYPE_DETECTION | FINDING_TYPE_THRESHOLD => {
            if finding.primary_rule_id.is_none() || finding.primary_rule_version.is_none() {
                return Err(violation(
                    "primary_rule_id",
                    "DETECTION and THRESHOLD Findings require primary rule identity/version",
                ));
            }
            if finding.correlation_provenance.is_some() {
                return Err(violation(
                    "correlation_provenance",
                    "is only valid for CORRELATION Findings",
                ));
            }
            Ok(())
        }
        FINDING_TYPE_CORRELATION => {
            let provenance = finding.correlation_provenance.as_ref().ok_or_else(|| {
                violation(
                    "correlation_provenance",
                    "is required for CORRELATION Findings",
                )
            })?;
            validate_correlation_provenance(finding, provenance, finding_input_ids)
        }
        FINDING_TYPE_ANALYTICAL => {
            if finding.correlation_provenance.is_some() {
                return Err(violation(
                    "correlation_provenance",
                    "is only valid for CORRELATION Findings",
                ));
            }
            Ok(())
        }
        _ => unreachable!("finding_type was validated above"),
    }
}

/// Validates the canonical Finding v1 wire invariants.
///
/// # Errors
///
/// Returns a [`ContractViolation`] when identity, lifecycle, temporal, input-traceability,
/// execution-domain, or correlation-provenance invariants fail ADR-0017.
pub fn validate_finding(finding: &Finding) -> Result<(), ContractViolation> {
    validate_uuid_v7("finding_id", &finding.finding_id)?;
    validate_uuid_v7("tenant_id", &finding.tenant_id)?;
    if !matches!(finding.finding_type, 1..=4) {
        return Err(violation(
            "finding_type",
            "must be DETECTION, THRESHOLD, CORRELATION, or ANALYTICAL",
        ));
    }
    if !matches!(finding.status, 1..=5) {
        return Err(violation(
            "status",
            "must be OPEN, ACKNOWLEDGED, SUPPRESSED, RESOLVED, or INVALIDATED",
        ));
    }
    require_text("severity", &finding.severity)?;
    if let Some(confidence) = finding.confidence.as_deref() {
        require_text("confidence", confidence)?;
    }
    require_text("title", &finding.title)?;
    require_text("description", &finding.description)?;
    validate_seen_pair(finding)?;
    validate_required_times(finding)?;
    validate_optional_pair(
        "primary_rule_id",
        finding.primary_rule_id.as_deref(),
        "primary_rule_version",
        finding.primary_rule_version.as_deref(),
    )?;
    validate_optional_pair(
        "correlation_rule_id",
        finding.correlation_rule_id.as_deref(),
        "correlation_rule_version",
        finding.correlation_rule_version.as_deref(),
    )?;
    if !matches!(finding.disposition, 1..=6) {
        return Err(violation(
            "disposition",
            "must be UNDETERMINED, TRUE_POSITIVE, BENIGN_TRUE_POSITIVE, FALSE_POSITIVE, DUPLICATE, or TEST_ACTIVITY",
        ));
    }
    if !matches!(finding.execution_mode, 1..=3) {
        return Err(violation(
            "execution_mode",
            "must distinguish LIVE, REPLAY, or TEST",
        ));
    }

    let finding_input_ids = validate_inputs(finding)?;
    validate_type_specific_provenance(finding, &finding_input_ids)
}
