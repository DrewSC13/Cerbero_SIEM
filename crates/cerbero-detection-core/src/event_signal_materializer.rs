use std::fmt;

use cerbero_common::contracts::{
    ContractViolation,
    v1::{
        DetectionExecutionBackend, NormalizedEvent, Signal, SignalInput, SignalInputType,
        SignalProvenance, SignalRuleType, SignalStatus,
    },
    validate_signal,
};
use prost_types::Timestamp;

use crate::{EventSignalProvenance, ExecutionBackend};

/// Producer-owned values required to turn an EVENT match into a canonical `Signal`.
///
/// The source event timestamp is supplied explicitly because the canonical `NormalizedEvent`
/// lifecycle contract does not expose source `event_time` at its top level. A producer must pass
/// authoritative lineage time when available and must use `None` when it is unavailable; this
/// materializer never substitutes normalization, evaluation, or creation time.
#[derive(Debug, Clone)]
pub struct EventSignalMaterializationContext {
    pub signal_id: String,
    pub severity: String,
    pub confidence: Option<String>,
    pub summary: String,
    pub input_relation: String,
    pub created_at: Timestamp,
    pub source_event_time: Option<Timestamp>,
}

fn violation(field: &'static str, reason: impl Into<String>) -> ContractViolation {
    ContractViolation {
        field,
        reason: reason.into(),
    }
}

const fn contract_backend(backend: ExecutionBackend) -> DetectionExecutionBackend {
    match backend {
        ExecutionBackend::ClickHouse => DetectionExecutionBackend::Clickhouse,
        ExecutionBackend::Stream => DetectionExecutionBackend::Stream,
    }
}

/// Materializes one matched EVENT provenance record as the canonical Signal v1 wire object.
///
/// This function does not allocate identity, infer severity/confidence, persist the Signal, publish
/// `SignalCreated`, implement deduplication storage, or apply suppression. Those remain separate
/// producer/runtime responsibilities. The returned object always passes the canonical Signal v1
/// validator.
///
/// # Errors
///
/// Returns a [`ContractViolation`] when the supplied event does not match the provenance input or
/// when any canonical Signal v1 invariant is violated.
pub fn materialize_event_signal<RuleVersion>(
    provenance: &EventSignalProvenance<RuleVersion>,
    event: &NormalizedEvent,
    context: EventSignalMaterializationContext,
) -> Result<Signal, ContractViolation>
where
    RuleVersion: fmt::Display,
{
    if provenance.normalized_event_id != event.normalized_event_id {
        return Err(violation(
            "normalized_event_id",
            "must match EventSignalProvenance.normalized_event_id",
        ));
    }

    let EventSignalMaterializationContext {
        signal_id,
        severity,
        confidence,
        summary,
        input_relation,
        created_at,
        source_event_time,
    } = context;

    let signal = Signal {
        signal_id: signal_id.clone(),
        tenant_id: event.tenant_id.clone(),
        rule_id: provenance.rule_id.as_str().to_string(),
        rule_version: provenance.rule_version.to_string(),
        rule_type: SignalRuleType::Event as i32,
        severity,
        confidence,
        status: SignalStatus::Active as i32,
        first_observed_at: source_event_time,
        last_observed_at: source_event_time,
        created_at: Some(created_at),
        execution_mode: provenance.execution_mode as i32,
        source_count: 1,
        event_count: 1,
        summary,
        provenance: Some(SignalProvenance {
            evaluated_at: Some(provenance.evaluated_at),
            execution_backend: contract_backend(provenance.execution_backend) as i32,
        }),
        inputs: vec![SignalInput {
            signal_id,
            input_type: SignalInputType::NormalizedEvent as i32,
            input_id: event.normalized_event_id.clone(),
            relation: input_relation,
            ordinal: 0,
        }],
    };

    validate_signal(&signal)?;
    Ok(signal)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr as _;

    use cerbero_common::contracts::{
        v1::{
            DetectionExecutionBackend, ExecutionMode, NormalizedEvent, SignalInputType,
            SignalRuleType, SignalStatus,
        },
        validate_signal,
    };
    use prost_types::Timestamp;

    use crate::{EventSignalProvenance, ExecutionBackend, RuleId};

    use super::{EventSignalMaterializationContext, materialize_event_signal};

    const SIGNAL_ID: &str = "018f47a2-4b00-7a00-8000-000000000201";
    const TENANT_ID: &str = "018f47a2-4b00-7a00-8000-000000000202";
    const EVENT_ID: &str = "018f47a2-4b00-7a00-8000-000000000203";

    fn timestamp(seconds: i64) -> Timestamp {
        Timestamp { seconds, nanos: 0 }
    }

    fn event() -> NormalizedEvent {
        NormalizedEvent {
            normalized_event_id: EVENT_ID.to_string(),
            tenant_id: TENANT_ID.to_string(),
            ..Default::default()
        }
    }

    fn provenance(backend: ExecutionBackend) -> EventSignalProvenance<u64> {
        EventSignalProvenance {
            rule_id: RuleId::from_str("CER-DET-000001").expect("valid test rule id"),
            rule_version: 7,
            normalized_event_id: EVENT_ID.to_string(),
            evaluated_at: timestamp(1_800_000_010),
            execution_backend: backend,
            execution_mode: ExecutionMode::Replay,
        }
    }

    fn context(source_event_time: Option<Timestamp>) -> EventSignalMaterializationContext {
        EventSignalMaterializationContext {
            signal_id: SIGNAL_ID.to_string(),
            severity: "HIGH".to_string(),
            confidence: Some("LOW".to_string()),
            summary: "failed SSH login matched".to_string(),
            input_relation: "MATCHED".to_string(),
            created_at: timestamp(1_800_000_020),
            source_event_time,
        }
    }

    #[test]
    fn event_match_materializes_canonical_signal() {
        let observed_at = timestamp(1_800_000_000);
        let signal = materialize_event_signal(
            &provenance(ExecutionBackend::ClickHouse),
            &event(),
            context(Some(observed_at)),
        )
        .expect("valid EVENT provenance should materialize");

        assert_eq!(signal.signal_id, SIGNAL_ID);
        assert_eq!(signal.tenant_id, TENANT_ID);
        assert_eq!(signal.rule_id, "CER-DET-000001");
        assert_eq!(signal.rule_version, "7");
        assert_eq!(signal.rule_type, SignalRuleType::Event as i32);
        assert_eq!(signal.severity, "HIGH");
        assert_eq!(signal.confidence.as_deref(), Some("LOW"));
        assert_eq!(signal.status, SignalStatus::Active as i32);
        assert_eq!(signal.first_observed_at, Some(observed_at));
        assert_eq!(signal.last_observed_at, Some(observed_at));
        assert_eq!(signal.execution_mode, ExecutionMode::Replay as i32);
        assert_eq!(signal.source_count, 1);
        assert_eq!(signal.event_count, 1);

        let actual_provenance = signal
            .provenance
            .as_ref()
            .expect("canonical Signal requires provenance");
        assert_eq!(
            actual_provenance.execution_backend,
            DetectionExecutionBackend::Clickhouse as i32
        );

        assert_eq!(signal.inputs.len(), 1);
        assert_eq!(
            signal.inputs[0].input_type,
            SignalInputType::NormalizedEvent as i32
        );
        assert_eq!(signal.inputs[0].input_id, EVENT_ID);
        assert_eq!(signal.inputs[0].ordinal, 0);
        assert_eq!(validate_signal(&signal), Ok(()));
    }

    #[test]
    fn missing_source_event_time_is_preserved_as_missing() {
        let signal = materialize_event_signal(
            &provenance(ExecutionBackend::Stream),
            &event(),
            context(None),
        )
        .expect("missing source event_time is representable");

        assert!(signal.first_observed_at.is_none());
        assert!(signal.last_observed_at.is_none());
        assert_eq!(
            signal
                .provenance
                .as_ref()
                .expect("canonical Signal requires provenance")
                .execution_backend,
            DetectionExecutionBackend::Stream as i32
        );
    }

    #[test]
    fn event_identity_mismatch_fails_closed() {
        let mut mismatched = provenance(ExecutionBackend::Stream);
        mismatched.normalized_event_id = "018f47a2-4b00-7a00-8000-000000000204".to_string();

        let error = materialize_event_signal(&mismatched, &event(), context(None))
            .expect_err("mismatched EVENT provenance must be rejected");

        assert_eq!(error.field, "normalized_event_id");
    }

    #[test]
    fn canonical_validator_rejects_invalid_producer_context() {
        let mut invalid = context(None);
        invalid.signal_id = "not-a-uuid".to_string();

        let error =
            materialize_event_signal(&provenance(ExecutionBackend::Stream), &event(), invalid)
                .expect_err("invalid Signal identity must fail canonical validation");

        assert_eq!(error.field, "signal_id");
    }

    #[test]
    fn canonical_validator_rejects_invalid_tenant_propagation() {
        let mut invalid_event = event();
        invalid_event.tenant_id = "not-a-uuid".to_string();

        let error = materialize_event_signal(
            &provenance(ExecutionBackend::Stream),
            &invalid_event,
            context(None),
        )
        .expect_err("invalid tenant identity must fail canonical validation");

        assert_eq!(error.field, "tenant_id");
    }
}
