use std::fmt;

use cerbero_common::contracts::{
    ContractViolation,
    v1::{ExecutionMode, NormalizedEvent},
    validate_timestamp, validate_uuid_v7,
};
use prost_types::Timestamp;

use crate::{
    EventEvaluationResult, EventExecutionPlanView, EventLeafEvaluationPolicy, ExecutionBackend,
    ExecutionPlan, NormalizedEventFieldView, RuleId, evaluate_event_execution_plan,
};

/// Minimal provenance preserved when one EVENT rule matches a canonical `NormalizedEvent`.
///
/// This is deliberately not the canonical `Signal` schema. Analytical Model v1 freezes additional
/// Signal fields whose concrete wire representation is not yet governed. This boundary preserves
/// only the EVENT-match provenance already fixed by Detection & Correlation v1 plus execution mode.
#[derive(Debug, Clone)]
pub struct EventSignalProvenance<RuleVersion> {
    pub rule_id: RuleId,
    pub rule_version: RuleVersion,
    pub normalized_event_id: String,
    pub evaluated_at: Timestamp,
    pub execution_backend: ExecutionBackend,
    pub execution_mode: ExecutionMode,
}

/// Outcome of EVENT evaluation at the Signal-creation boundary.
#[derive(Debug, Clone)]
pub enum EventSignalEvaluationResult<RuleVersion> {
    NoMatch,
    Match(EventSignalProvenance<RuleVersion>),
}

/// Error raised before Signal provenance can be materialized.
#[derive(Debug)]
pub enum EventSignalEvaluationError<PolicyError> {
    Policy(PolicyError),
    InvalidProvenance(ContractViolation),
    UnspecifiedExecutionMode,
}

impl<PolicyError> fmt::Display for EventSignalEvaluationError<PolicyError>
where
    PolicyError: fmt::Display,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Policy(error) => write!(formatter, "EVENT evaluation failed: {error}"),
            Self::InvalidProvenance(error) => {
                write!(formatter, "invalid Signal provenance: {error}")
            }
            Self::UnspecifiedExecutionMode => formatter
                .write_str("Signal provenance requires LIVE, REPLAY, or TEST execution mode"),
        }
    }
}

impl<PolicyError> std::error::Error for EventSignalEvaluationError<PolicyError> where
    PolicyError: std::error::Error + 'static
{
}

/// Read-only rule provenance required to materialize EVENT Signal provenance.
pub trait EventSignalExecutionPlanView<RuleVersion> {
    fn signal_rule_id(&self) -> &RuleId;
    fn signal_rule_version(&self) -> &RuleVersion;
    fn signal_execution_backend(&self) -> ExecutionBackend;
}

impl<PlanId, RuleVersion, RequiredFields, Predicates, Grouping, Window, ResourceLimits, PlanHash>
    EventSignalExecutionPlanView<RuleVersion>
    for ExecutionPlan<
        PlanId,
        RuleVersion,
        RequiredFields,
        Predicates,
        Grouping,
        Window,
        ResourceLimits,
        PlanHash,
    >
{
    fn signal_rule_id(&self) -> &RuleId {
        &self.rule_id
    }

    fn signal_rule_version(&self) -> &RuleVersion {
        &self.rule_version
    }

    fn signal_execution_backend(&self) -> ExecutionBackend {
        self.backend
    }
}

/// Evaluates one canonical `NormalizedEvent` and preserves Signal provenance only on `Match`.
///
/// The function fails closed when the referenced normalized-event identity, execution timestamp, or
/// execution mode cannot satisfy the already-governed contract. It does not allocate `signal_id`,
/// choose Signal severity/confidence/status, persist data, publish NATS messages, or define the
/// unresolved canonical Signal wire schema.
///
/// # Errors
///
/// Returns [`EventSignalEvaluationError::InvalidProvenance`] when `normalized_event_id` or
/// `evaluated_at` violates the governed contract, [`EventSignalEvaluationError::UnspecifiedExecutionMode`]
/// when execution mode is not LIVE/REPLAY/TEST, or [`EventSignalEvaluationError::Policy`] when the
/// leaf-evaluation policy fails.
pub fn evaluate_normalized_event_for_signal<Field, RuleValue, RuleVersion, Plan, Policy>(
    plan: &Plan,
    event: &NormalizedEvent,
    execution_mode: ExecutionMode,
    evaluated_at: Timestamp,
    policy: &Policy,
) -> Result<EventSignalEvaluationResult<RuleVersion>, EventSignalEvaluationError<Policy::Error>>
where
    Field: AsRef<str>,
    RuleVersion: Clone,
    Plan: EventExecutionPlanView<Field, RuleValue> + EventSignalExecutionPlanView<RuleVersion>,
    Policy: EventLeafEvaluationPolicy<Field, RuleValue, prost_types::Value>,
{
    validate_uuid_v7("normalized_event_id", &event.normalized_event_id)
        .map_err(EventSignalEvaluationError::InvalidProvenance)?;
    validate_timestamp("evaluated_at", &evaluated_at)
        .map_err(EventSignalEvaluationError::InvalidProvenance)?;
    if execution_mode == ExecutionMode::Unspecified {
        return Err(EventSignalEvaluationError::UnspecifiedExecutionMode);
    }

    let view = NormalizedEventFieldView::new(event);
    let evaluation = evaluate_event_execution_plan(plan, &view, policy)
        .map_err(EventSignalEvaluationError::Policy)?;

    if evaluation == EventEvaluationResult::NoMatch {
        return Ok(EventSignalEvaluationResult::NoMatch);
    }

    Ok(EventSignalEvaluationResult::Match(EventSignalProvenance {
        rule_id: plan.signal_rule_id().clone(),
        rule_version: plan.signal_rule_version().clone(),
        normalized_event_id: event.normalized_event_id.clone(),
        evaluated_at,
        execution_backend: plan.signal_execution_backend(),
        execution_mode,
    }))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::convert::Infallible;
    use std::str::FromStr as _;

    use cerbero_common::contracts::v1::{ExecutionMode, NormalizedEvent};
    use prost_types::{Struct, Timestamp, Value, value::Kind};

    use crate::{
        ComparisonOperator, EventFieldState, EventLeafEvaluationPolicy, EventPredicate,
        ExecutionBackend, ExecutionPlan, RuleId,
    };

    use super::{
        EventSignalEvaluationError, EventSignalEvaluationResult,
        evaluate_normalized_event_for_signal,
    };

    type TestPlan = ExecutionPlan<
        &'static str,
        u64,
        Vec<&'static str>,
        EventPredicate<&'static str, &'static str>,
        (),
        (),
        (),
        &'static str,
    >;

    struct TestPolicy;

    impl EventLeafEvaluationPolicy<&'static str, &'static str, Value> for TestPolicy {
        type Error = Infallible;

        fn evaluate_exists(
            &self,
            _field: &&'static str,
            state: EventFieldState<'_, Value>,
        ) -> Result<bool, Self::Error> {
            Ok(matches!(state, EventFieldState::Present(_)))
        }

        fn evaluate_comparison(
            &self,
            _field: &&'static str,
            state: EventFieldState<'_, Value>,
            operator: ComparisonOperator,
            rule_value: &&'static str,
        ) -> Result<bool, Self::Error> {
            let EventFieldState::Present(actual) = state else {
                return Ok(false);
            };
            let Some(Kind::StringValue(actual)) = actual.kind.as_ref() else {
                return Ok(false);
            };

            Ok(operator == ComparisonOperator::Eq && actual.as_str() == *rule_value)
        }
    }

    fn plan(expected_status: &'static str, backend: ExecutionBackend) -> TestPlan {
        ExecutionPlan {
            plan_id: "event-signal-plan",
            rule_id: RuleId::from_str("CER-DET-000001").expect("valid test rule id"),
            rule_version: 7,
            backend,
            required_fields: vec!["status"],
            predicates: EventPredicate::Comparison {
                field: "status",
                operator: ComparisonOperator::Eq,
                value: expected_status,
            },
            grouping: (),
            window: (),
            resource_limits: (),
            plan_hash: "event-signal-plan-hash",
        }
    }

    fn event(status: &str, normalized_event_id: &str) -> NormalizedEvent {
        NormalizedEvent {
            normalized_event_id: normalized_event_id.to_string(),
            ocsf_event: Some(Struct {
                fields: BTreeMap::from([(
                    "status".to_string(),
                    Value {
                        kind: Some(Kind::StringValue(status.to_string())),
                    },
                )]),
            }),
            ..Default::default()
        }
    }

    fn evaluated_at() -> Timestamp {
        Timestamp {
            seconds: 1_800_000_000,
            nanos: 123_000_000,
        }
    }

    #[test]
    fn matching_event_preserves_frozen_signal_provenance() {
        let result = evaluate_normalized_event_for_signal(
            &plan("Failure", ExecutionBackend::ClickHouse),
            &event("Failure", "018f47a2-4b00-7a00-8000-000000000001"),
            ExecutionMode::Replay,
            evaluated_at(),
            &TestPolicy,
        )
        .expect("valid EVENT match should preserve Signal provenance");

        let EventSignalEvaluationResult::Match(provenance) = result else {
            panic!("positive EVENT fixture must match");
        };

        assert_eq!(provenance.rule_id.as_str(), "CER-DET-000001");
        assert_eq!(provenance.rule_version, 7);
        assert_eq!(
            provenance.normalized_event_id,
            "018f47a2-4b00-7a00-8000-000000000001"
        );
        assert_eq!(provenance.evaluated_at, evaluated_at());
        assert_eq!(provenance.execution_backend, ExecutionBackend::ClickHouse);
        assert_eq!(provenance.execution_mode, ExecutionMode::Replay);
    }

    #[test]
    fn non_matching_event_does_not_materialize_signal_provenance() {
        let result = evaluate_normalized_event_for_signal(
            &plan("Success", ExecutionBackend::Stream),
            &event("Failure", "018f47a2-4b00-7a00-8000-000000000001"),
            ExecutionMode::Live,
            evaluated_at(),
            &TestPolicy,
        )
        .expect("valid non-match should evaluate cleanly");

        assert!(matches!(result, EventSignalEvaluationResult::NoMatch));
    }

    #[test]
    fn invalid_normalized_event_identity_fails_closed() {
        let error = evaluate_normalized_event_for_signal(
            &plan("Failure", ExecutionBackend::Stream),
            &event("Failure", "not-a-uuid"),
            ExecutionMode::Live,
            evaluated_at(),
            &TestPolicy,
        )
        .expect_err("invalid normalized_event_id must reject Signal provenance");

        assert!(matches!(
            error,
            EventSignalEvaluationError::InvalidProvenance(ref violation)
                if violation.field == "normalized_event_id"
        ));
    }

    #[test]
    fn unspecified_execution_mode_fails_closed() {
        let error = evaluate_normalized_event_for_signal(
            &plan("Failure", ExecutionBackend::Stream),
            &event("Failure", "018f47a2-4b00-7a00-8000-000000000001"),
            ExecutionMode::Unspecified,
            evaluated_at(),
            &TestPolicy,
        )
        .expect_err("unspecified execution mode must reject Signal provenance");

        assert!(matches!(
            error,
            EventSignalEvaluationError::UnspecifiedExecutionMode
        ));
    }

    #[test]
    fn invalid_evaluation_timestamp_fails_closed() {
        let error = evaluate_normalized_event_for_signal(
            &plan("Failure", ExecutionBackend::Stream),
            &event("Failure", "018f47a2-4b00-7a00-8000-000000000001"),
            ExecutionMode::Test,
            Timestamp {
                seconds: 253_402_300_800,
                nanos: 0,
            },
            &TestPolicy,
        )
        .expect_err("out-of-range evaluation timestamp must reject Signal provenance");

        assert!(matches!(
            error,
            EventSignalEvaluationError::InvalidProvenance(ref violation)
                if violation.field == "evaluated_at"
        ));
    }
}
