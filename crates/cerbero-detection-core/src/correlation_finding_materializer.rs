use cerbero_common::contracts::{
    ContractViolation,
    v1::{
        DetectionExecutionBackend, Finding, FindingCorrelationProvenance, FindingDisposition,
        FindingInput, FindingStatus, FindingType,
    },
    validate_finding,
};
use prost_types::Timestamp;

use crate::{ExecutionBackend, SequenceCorrelationMatch};

const SIGNAL_INPUT_TYPE: &str = "SIGNAL";

/// Producer-owned values required to turn one deterministic SEQUENCE correlation match
/// into a canonical `Finding`.
///
/// Finding identity and analyst-facing presentation are deliberately supplied by the
/// producer. This materializer does not allocate IDs, infer severity/confidence, persist,
/// publish, deduplicate durably, suppress, or promote the Finding to an Incident.
#[derive(Debug, Clone)]
pub struct CorrelationFindingMaterializationContext {
    pub finding_id: String,
    pub severity: String,
    pub confidence: Option<String>,
    pub title: String,
    pub description: String,
    pub input_relation: String,
    pub created_at: Timestamp,
}

const fn contract_backend(backend: ExecutionBackend) -> DetectionExecutionBackend {
    match backend {
        ExecutionBackend::ClickHouse => DetectionExecutionBackend::Clickhouse,
        ExecutionBackend::Stream => DetectionExecutionBackend::Stream,
    }
}

/// Materializes deterministic SEQUENCE correlation evidence as canonical Finding v1.
///
/// The correlation window becomes the Finding observation window. Signal identities are
/// preserved in their deterministic SEQUENCE order in both `Finding.inputs` and the typed
/// correlation provenance. Because `FindingInput` v1 has no ordinal, ordered correlation
/// semantics remain authoritative in `correlation_provenance.input_ids`.
///
/// `created_at` and `updated_at` are equal at initial materialization. The caller-provided
/// creation timestamp is also the auditable promotion time for this boundary; no event,
/// evaluation, or processing timestamp is substituted.
///
/// # Errors
///
/// Returns a [`ContractViolation`] when producer-owned context or correlation evidence
/// cannot satisfy canonical Finding v1 invariants.
pub fn materialize_sequence_correlation_finding(
    correlation_match: &SequenceCorrelationMatch,
    context: CorrelationFindingMaterializationContext,
) -> Result<Finding, ContractViolation> {
    let CorrelationFindingMaterializationContext {
        finding_id,
        severity,
        confidence,
        title,
        description,
        input_relation,
        created_at,
    } = context;

    let inputs = correlation_match
        .input_signal_ids
        .iter()
        .map(|signal_id| FindingInput {
            finding_id: finding_id.clone(),
            input_type: SIGNAL_INPUT_TYPE.to_owned(),
            input_id: signal_id.clone(),
            relation: input_relation.clone(),
        })
        .collect();

    let finding = Finding {
        finding_id: finding_id.clone(),
        tenant_id: correlation_match.tenant_id.clone(),
        finding_type: FindingType::Correlation as i32,
        status: FindingStatus::Open as i32,
        severity,
        confidence,
        title,
        description,
        first_seen: Some(correlation_match.window_start),
        last_seen: Some(correlation_match.window_end),
        created_at: Some(created_at),
        updated_at: Some(created_at),
        primary_rule_id: None,
        primary_rule_version: None,
        correlation_rule_id: Some(correlation_match.correlation_rule_id.clone()),
        correlation_rule_version: Some(correlation_match.correlation_rule_version.clone()),
        disposition: FindingDisposition::Undetermined as i32,
        metadata: None,
        execution_mode: correlation_match.execution_mode as i32,
        inputs,
        correlation_provenance: Some(FindingCorrelationProvenance {
            correlation_rule_id: correlation_match.correlation_rule_id.clone(),
            correlation_rule_version: correlation_match.correlation_rule_version.clone(),
            window_start: Some(correlation_match.window_start),
            window_end: Some(correlation_match.window_end),
            input_ids: correlation_match.input_signal_ids.clone(),
            output_finding_id: finding_id,
            execution_backend: contract_backend(correlation_match.execution_backend) as i32,
            execution_mode: correlation_match.execution_mode as i32,
            configuration_hash: correlation_match.configuration_hash.clone(),
        }),
    };

    validate_finding(&finding)?;
    Ok(finding)
}

#[cfg(test)]
mod tests {
    use cerbero_common::contracts::{
        v1::{
            DetectionExecutionBackend, ExecutionMode, FindingDisposition, FindingStatus,
            FindingType,
        },
        validate_finding,
    };
    use prost_types::Timestamp;

    use crate::{ExecutionBackend, SequenceCorrelationMatch};

    use super::{
        CorrelationFindingMaterializationContext, materialize_sequence_correlation_finding,
    };

    const FINDING_ID: &str = "018f47a2-4b00-7a00-8000-000000000701";
    const TENANT_ID: &str = "018f47a2-4b00-7a00-8000-000000000702";
    const SIGNAL_A: &str = "018f47a2-4b00-7a00-8000-000000000703";
    const SIGNAL_B: &str = "018f47a2-4b00-7a00-8000-000000000704";
    const SIGNAL_C: &str = "018f47a2-4b00-7a00-8000-000000000705";

    fn timestamp(seconds: i64) -> Timestamp {
        Timestamp { seconds, nanos: 0 }
    }

    fn correlation_match() -> SequenceCorrelationMatch {
        SequenceCorrelationMatch {
            correlation_rule_id: "CER-COR-0003".to_owned(),
            correlation_rule_version: "2".to_owned(),
            tenant_id: TENANT_ID.to_owned(),
            window_start: timestamp(1_800_000_000),
            window_end: timestamp(1_800_000_120),
            input_signal_ids: vec![
                SIGNAL_A.to_owned(),
                SIGNAL_B.to_owned(),
                SIGNAL_C.to_owned(),
            ],
            execution_backend: ExecutionBackend::ClickHouse,
            execution_mode: ExecutionMode::Replay,
            configuration_hash: "sequence-config-v1".to_owned(),
        }
    }

    fn context() -> CorrelationFindingMaterializationContext {
        CorrelationFindingMaterializationContext {
            finding_id: FINDING_ID.to_owned(),
            severity: "HIGH".to_owned(),
            confidence: Some("MEDIUM".to_owned()),
            title: "Account compromise sequence".to_owned(),
            description: "Failed login followed by success and privilege escalation".to_owned(),
            input_relation: "SEQUENCE_INPUT".to_owned(),
            created_at: timestamp(1_800_000_130),
        }
    }

    #[test]
    fn sequence_match_materializes_canonical_correlation_finding() {
        let finding = materialize_sequence_correlation_finding(&correlation_match(), context())
            .expect("valid SEQUENCE evidence should materialize");

        assert_eq!(finding.finding_id, FINDING_ID);
        assert_eq!(finding.tenant_id, TENANT_ID);
        assert_eq!(finding.finding_type, FindingType::Correlation as i32);
        assert_eq!(finding.status, FindingStatus::Open as i32);
        assert_eq!(finding.disposition, FindingDisposition::Undetermined as i32);
        assert_eq!(finding.execution_mode, ExecutionMode::Replay as i32);
        assert_eq!(finding.first_seen, Some(timestamp(1_800_000_000)));
        assert_eq!(finding.last_seen, Some(timestamp(1_800_000_120)));
        assert_eq!(finding.created_at, Some(timestamp(1_800_000_130)));
        assert_eq!(finding.updated_at, finding.created_at);
        assert!(finding.primary_rule_id.is_none());
        assert!(finding.primary_rule_version.is_none());
        assert_eq!(finding.correlation_rule_id.as_deref(), Some("CER-COR-0003"));
        assert_eq!(finding.correlation_rule_version.as_deref(), Some("2"));

        let input_ids: Vec<_> = finding
            .inputs
            .iter()
            .map(|input| input.input_id.as_str())
            .collect();
        assert_eq!(input_ids, [SIGNAL_A, SIGNAL_B, SIGNAL_C]);
        assert!(
            finding
                .inputs
                .iter()
                .all(|input| input.input_type == "SIGNAL"
                    && input.relation == "SEQUENCE_INPUT"
                    && input.finding_id == FINDING_ID)
        );

        let provenance = finding
            .correlation_provenance
            .as_ref()
            .expect("CORRELATION Finding requires typed provenance");
        assert_eq!(
            provenance.input_ids,
            vec![
                SIGNAL_A.to_owned(),
                SIGNAL_B.to_owned(),
                SIGNAL_C.to_owned()
            ]
        );
        assert_eq!(provenance.output_finding_id, FINDING_ID);
        assert_eq!(
            provenance.execution_backend,
            DetectionExecutionBackend::Clickhouse as i32
        );
        assert_eq!(provenance.execution_mode, ExecutionMode::Replay as i32);
        assert_eq!(provenance.configuration_hash, "sequence-config-v1");
        assert_eq!(validate_finding(&finding), Ok(()));
    }

    #[test]
    fn invalid_producer_identity_fails_canonical_validation() {
        let mut invalid = context();
        invalid.finding_id = "not-a-uuid".to_owned();

        let error = materialize_sequence_correlation_finding(&correlation_match(), invalid)
            .expect_err("invalid Finding identity must fail closed");

        assert_eq!(error.field, "finding_id");
    }

    #[test]
    fn duplicate_correlation_inputs_fail_closed() {
        let mut invalid = correlation_match();
        invalid.input_signal_ids = vec![SIGNAL_A.to_owned(), SIGNAL_A.to_owned()];

        let error = materialize_sequence_correlation_finding(&invalid, context())
            .expect_err("duplicate contributing identities must fail closed");

        assert_eq!(error.field, "inputs");
    }

    #[test]
    fn unspecified_execution_mode_fails_closed() {
        let mut invalid = correlation_match();
        invalid.execution_mode = ExecutionMode::Unspecified;

        let error = materialize_sequence_correlation_finding(&invalid, context())
            .expect_err("unspecified execution mode must fail closed");

        assert_eq!(error.field, "execution_mode");
    }
}
