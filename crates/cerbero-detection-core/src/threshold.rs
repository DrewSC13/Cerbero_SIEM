use std::{
    collections::{BTreeMap, HashSet},
    fmt,
};

use cerbero_common::contracts::{
    ContractViolation,
    v1::{
        DetectionExecutionBackend, ExecutionMode, NormalizedEvent, Signal, SignalInput,
        SignalInputType, SignalProvenance, SignalRuleType, SignalStatus,
    },
    validate_signal, validate_timestamp, validate_uuid_v7,
};
use prost_types::{Timestamp, Value};

use crate::{
    ComparisonTypePolicy, CompiledEventExecutionPlan, EventAstOptimizer, EventCompilationStages,
    EventExecutionPlanCompiler, EventExecutionPlanView, EventFieldProfileResolver,
    EventLeafEvaluationPolicy, EventSignalExecutionPlanView, ExecutionBackend,
    ExecutionBackendCapabilities, FieldTypeResolver, NormalizedEventFieldView,
    RuleCompilationErrorKind, RuleId, ValueTypeResolver, compile_event_execution_plan,
    evaluate_event_execution_plan, require_field_type,
};

/// Aggregations admitted by the first THRESHOLD MVP vertical.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ThresholdAggregation {
    Count,
}

/// Clock basis declared by a temporal THRESHOLD rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ThresholdTimeBasis {
    EventTime,
    IngestTime,
}

/// Late-event policy declared by a temporal THRESHOLD rule.
///
/// Step 26 executes only deterministic ClickHouse/batch evaluation with `Accept`.
/// The other governed policy shapes remain representable without pretending that
/// streaming watermark/checkpoint semantics have been implemented.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ThresholdLateEventPolicy {
    Accept,
    AcceptWithLimit { max_lateness_millis: u64 },
    IgnoreForStream,
}

/// Temporal contract carried by a THRESHOLD rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThresholdWindow<Field> {
    pub duration_millis: u64,
    pub time_basis: ThresholdTimeBasis,
    pub time_field: Field,
    pub late_event_policy: ThresholdLateEventPolicy,
}

/// Backend-neutral THRESHOLD rule contract for the MVP vertical.
///
/// `filter` intentionally reuses the governed EVENT expression semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThresholdRule<Field, RuleValue> {
    pub filter: crate::DetectionExpression<Field, RuleValue>,
    pub group_by: Vec<Field>,
    pub aggregation: ThresholdAggregation,
    pub threshold: u64,
    pub window: ThresholdWindow<Field>,
}

/// Compiled THRESHOLD plan built around an already-governed EVENT filter plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledThresholdExecutionPlan<Plan, Profile, Field> {
    filter: CompiledEventExecutionPlan<Plan, Profile>,
    group_by: Vec<Field>,
    aggregation: ThresholdAggregation,
    threshold: u64,
    window: ThresholdWindow<Field>,
    supported_backends: ExecutionBackendCapabilities,
}

impl<Plan, Profile, Field> CompiledThresholdExecutionPlan<Plan, Profile, Field> {
    #[must_use]
    pub const fn filter(&self) -> &CompiledEventExecutionPlan<Plan, Profile> {
        &self.filter
    }

    #[must_use]
    pub fn group_by(&self) -> &[Field] {
        &self.group_by
    }

    #[must_use]
    pub const fn aggregation(&self) -> ThresholdAggregation {
        self.aggregation
    }

    #[must_use]
    pub const fn threshold(&self) -> u64 {
        self.threshold
    }

    #[must_use]
    pub const fn window(&self) -> &ThresholdWindow<Field> {
        &self.window
    }

    #[must_use]
    pub const fn supported_backends(&self) -> ExecutionBackendCapabilities {
        self.supported_backends
    }
}

/// Compiles a THRESHOLD rule by reusing the complete EVENT filter compiler.
///
/// Step 26 deliberately supports only deterministic ClickHouse/batch execution.
/// STREAM is rejected rather than approximated until watermark, checkpoint, and
/// persistent-state decisions are governed.
///
/// # Errors
///
/// Returns a frozen compilation error for invalid grouping/window configuration,
/// unsupported backend/late-event semantics, schema/type failures, or EVENT filter
/// compilation failures.
pub fn compile_threshold_execution_plan<
    Field,
    RuleValue,
    FieldResolver,
    ValueResolver,
    ComparisonPolicy,
    Optimizer,
    ProfileResolver,
    PlanCompiler,
>(
    rule: &ThresholdRule<Field, RuleValue>,
    backend: ExecutionBackend,
    field_resolver: &FieldResolver,
    value_resolver: &ValueResolver,
    comparison_policy: &ComparisonPolicy,
    stages: &EventCompilationStages<'_, Optimizer, ProfileResolver, PlanCompiler>,
) -> Result<
    CompiledThresholdExecutionPlan<PlanCompiler::Plan, ProfileResolver::Profile, Field>,
    RuleCompilationErrorKind,
>
where
    Field: AsRef<str> + Clone,
    RuleValue: Clone,
    FieldResolver: FieldTypeResolver,
    ValueResolver: ValueTypeResolver<RuleValue>,
    ComparisonPolicy: ComparisonTypePolicy<FieldResolver::FieldType, ValueResolver::ValueType>,
    Optimizer: EventAstOptimizer<Field, RuleValue>,
    ProfileResolver: EventFieldProfileResolver<Field, RuleValue>,
    PlanCompiler: EventExecutionPlanCompiler<Field, RuleValue, ProfileResolver::Profile>,
{
    if backend != ExecutionBackend::ClickHouse {
        return Err(RuleCompilationErrorKind::UnsupportedBackend);
    }
    if rule.group_by.is_empty() || rule.threshold == 0 {
        return Err(RuleCompilationErrorKind::InvalidMapping);
    }
    if rule.window.duration_millis == 0 || i64::try_from(rule.window.duration_millis).is_err() {
        return Err(RuleCompilationErrorKind::InvalidWindow);
    }
    if rule.window.late_event_policy != ThresholdLateEventPolicy::Accept {
        return Err(RuleCompilationErrorKind::UnsupportedBackend);
    }

    for (index, field) in rule.group_by.iter().enumerate() {
        if rule.group_by[..index]
            .iter()
            .any(|previous| previous.as_ref() == field.as_ref())
        {
            return Err(RuleCompilationErrorKind::InvalidMapping);
        }
        require_field_type(field_resolver, field.as_ref())?;
    }
    require_field_type(field_resolver, rule.window.time_field.as_ref())?;

    let filter = compile_event_execution_plan(
        &rule.filter,
        backend,
        field_resolver,
        value_resolver,
        comparison_policy,
        stages,
    )?;

    Ok(CompiledThresholdExecutionPlan {
        filter,
        group_by: rule.group_by.clone(),
        aggregation: rule.aggregation,
        threshold: rule.threshold,
        window: rule.window.clone(),
        supported_backends: ExecutionBackendCapabilities::new(true, false),
    })
}

/// Policy boundary for THRESHOLD grouping and governed time extraction.
///
/// EVENT leaf evaluation remains owned by [`EventLeafEvaluationPolicy`]. This
/// extension adds only grouping-key and clock extraction from canonical
/// `NormalizedEvent` objects.
pub trait ThresholdEvaluationPolicy<Field, RuleValue>:
    EventLeafEvaluationPolicy<Field, RuleValue, Value>
{
    type GroupKey: Ord + Clone;

    /// Returns the logical grouping key, or `None` when the event cannot
    /// participate in this THRESHOLD evaluation.
    ///
    /// # Errors
    ///
    /// Propagates policy-owned extraction errors.
    fn threshold_group_key(
        &self,
        group_by: &[Field],
        event: &NormalizedEvent,
    ) -> Result<Option<Self::GroupKey>, Self::Error>;

    /// Returns the governed clock value in Unix milliseconds, or `None` when
    /// the selected clock is unavailable/unusable for this event.
    ///
    /// # Errors
    ///
    /// Propagates policy-owned extraction errors.
    fn threshold_event_time_millis(
        &self,
        time_basis: ThresholdTimeBasis,
        time_field: &Field,
        event: &NormalizedEvent,
    ) -> Result<Option<i64>, Self::Error>;
}

/// Minimal provenance emitted by a deterministic THRESHOLD match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThresholdSignalProvenance<RuleVersion> {
    pub rule_id: RuleId,
    pub rule_version: RuleVersion,
    pub tenant_id: String,
    pub normalized_event_ids: Vec<String>,
    pub first_observed_at: Timestamp,
    pub last_observed_at: Timestamp,
    pub evaluated_at: Timestamp,
    pub execution_backend: ExecutionBackend,
    pub execution_mode: ExecutionMode,
}

/// THRESHOLD evaluation failure before canonical Signal materialization.
#[derive(Debug)]
pub enum ThresholdEvaluationError<PolicyError> {
    Policy(PolicyError),
    InvalidProvenance(ContractViolation),
    UnspecifiedExecutionMode,
}

impl<PolicyError> fmt::Display for ThresholdEvaluationError<PolicyError>
where
    PolicyError: fmt::Display,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Policy(error) => write!(formatter, "THRESHOLD evaluation failed: {error}"),
            Self::InvalidProvenance(error) => {
                write!(formatter, "invalid THRESHOLD provenance: {error}")
            }
            Self::UnspecifiedExecutionMode => formatter
                .write_str("THRESHOLD provenance requires LIVE, REPLAY, or TEST execution mode"),
        }
    }
}

impl<PolicyError> std::error::Error for ThresholdEvaluationError<PolicyError> where
    PolicyError: std::error::Error + 'static
{
}

#[derive(Debug)]
struct Candidate {
    normalized_event_id: String,
    event_time_millis: i64,
}

fn timestamp_from_millis(value: i64) -> Timestamp {
    Timestamp {
        seconds: value.div_euclid(1_000),
        nanos: i32::try_from(value.rem_euclid(1_000) * 1_000_000)
            .expect("millisecond remainder always fits i32 nanos"),
    }
}

/// Evaluates one bounded dataset deterministically for THRESHOLD matches.
///
/// Input order is irrelevant: matching events are deduplicated by
/// `normalized_event_id`, partitioned by tenant plus policy-owned grouping key,
/// and ordered by governed event time plus event ID. The function emits at most
/// one earliest qualifying window per group for this bounded evaluation.
/// Runtime scheduling/repeated emission, persistent streaming state, watermarks,
/// and checkpoints remain outside Step 26.
///
/// # Errors
///
/// Returns a policy error, an invalid provenance error, or an execution-mode
/// error.
pub fn evaluate_threshold_normalized_events<Field, RuleValue, RuleVersion, Plan, Profile, Policy>(
    plan: &CompiledThresholdExecutionPlan<Plan, Profile, Field>,
    events: &[NormalizedEvent],
    execution_mode: ExecutionMode,
    evaluated_at: Timestamp,
    policy: &Policy,
) -> Result<Vec<ThresholdSignalProvenance<RuleVersion>>, ThresholdEvaluationError<Policy::Error>>
where
    Field: AsRef<str>,
    RuleVersion: Clone,
    Plan: EventExecutionPlanView<Field, RuleValue> + EventSignalExecutionPlanView<RuleVersion>,
    Policy: ThresholdEvaluationPolicy<Field, RuleValue>,
{
    validate_timestamp("evaluated_at", &evaluated_at)
        .map_err(ThresholdEvaluationError::InvalidProvenance)?;
    if execution_mode == ExecutionMode::Unspecified {
        return Err(ThresholdEvaluationError::UnspecifiedExecutionMode);
    }

    let threshold = usize::try_from(plan.threshold).map_err(|_| {
        ThresholdEvaluationError::InvalidProvenance(ContractViolation {
            field: "threshold",
            reason: "exceeds platform collection capacity".to_string(),
        })
    })?;
    let window_millis = i64::try_from(plan.window.duration_millis).map_err(|_| {
        ThresholdEvaluationError::InvalidProvenance(ContractViolation {
            field: "window.duration_millis",
            reason: "exceeds supported deterministic batch range".to_string(),
        })
    })?;

    let mut seen_event_ids = HashSet::new();
    let mut groups: BTreeMap<(String, Policy::GroupKey), Vec<Candidate>> = BTreeMap::new();

    for event in events {
        validate_uuid_v7("normalized_event_id", &event.normalized_event_id)
            .map_err(ThresholdEvaluationError::InvalidProvenance)?;
        validate_uuid_v7("tenant_id", &event.tenant_id)
            .map_err(ThresholdEvaluationError::InvalidProvenance)?;

        if !seen_event_ids.insert(event.normalized_event_id.clone()) {
            continue;
        }

        let view = NormalizedEventFieldView::new(event);
        if crate::EventEvaluationResult::NoMatch
            == evaluate_event_execution_plan(&plan.filter.plan, &view, policy)
                .map_err(ThresholdEvaluationError::Policy)?
        {
            continue;
        }

        let Some(group_key) = policy
            .threshold_group_key(&plan.group_by, event)
            .map_err(ThresholdEvaluationError::Policy)?
        else {
            continue;
        };
        let Some(event_time_millis) = policy
            .threshold_event_time_millis(plan.window.time_basis, &plan.window.time_field, event)
            .map_err(ThresholdEvaluationError::Policy)?
        else {
            continue;
        };

        groups
            .entry((event.tenant_id.clone(), group_key))
            .or_default()
            .push(Candidate {
                normalized_event_id: event.normalized_event_id.clone(),
                event_time_millis,
            });
    }

    let mut matches = Vec::new();

    for ((tenant_id, _group_key), mut candidates) in groups {
        candidates.sort_by(|left, right| {
            left.event_time_millis
                .cmp(&right.event_time_millis)
                .then_with(|| left.normalized_event_id.cmp(&right.normalized_event_id))
        });

        let mut left = 0_usize;
        for right in 0..candidates.len() {
            while left <= right
                && i128::from(candidates[right].event_time_millis)
                    - i128::from(candidates[left].event_time_millis)
                    > i128::from(window_millis)
            {
                left += 1;
            }

            if right + 1 - left < threshold {
                continue;
            }

            let contributors = &candidates[left..=right];
            let normalized_event_ids = contributors
                .iter()
                .map(|candidate| candidate.normalized_event_id.clone())
                .collect();

            let first_observed_at = timestamp_from_millis(contributors[0].event_time_millis);
            let last_observed_at =
                timestamp_from_millis(contributors[contributors.len() - 1].event_time_millis);

            validate_timestamp("first_observed_at", &first_observed_at)
                .map_err(ThresholdEvaluationError::InvalidProvenance)?;
            validate_timestamp("last_observed_at", &last_observed_at)
                .map_err(ThresholdEvaluationError::InvalidProvenance)?;

            matches.push(ThresholdSignalProvenance {
                rule_id: plan.filter.plan.signal_rule_id().clone(),
                rule_version: plan.filter.plan.signal_rule_version().clone(),
                tenant_id,
                normalized_event_ids,
                first_observed_at,
                last_observed_at,
                evaluated_at,
                execution_backend: plan.filter.plan.signal_execution_backend(),
                execution_mode,
            });
            break;
        }
    }

    Ok(matches)
}

/// Producer-owned values required to materialize one THRESHOLD match.
#[derive(Debug, Clone)]
pub struct ThresholdSignalMaterializationContext {
    pub signal_id: String,
    pub severity: String,
    pub confidence: Option<String>,
    pub summary: String,
    pub input_relation: String,
    pub created_at: Timestamp,
    pub source_count: u64,
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

/// Materializes a deterministic THRESHOLD match as canonical Signal v1.
///
/// All contributing normalized-event IDs are preserved as ordered Signal inputs.
/// Identity allocation, persistence, publication, suppression, and distributed
/// deduplication remain producer/runtime responsibilities.
///
/// # Errors
///
/// Returns a contract violation when supplied provenance/context cannot satisfy
/// canonical Signal v1.
pub fn materialize_threshold_signal<RuleVersion>(
    provenance: &ThresholdSignalProvenance<RuleVersion>,
    context: ThresholdSignalMaterializationContext,
) -> Result<Signal, ContractViolation>
where
    RuleVersion: fmt::Display,
{
    if provenance.normalized_event_ids.is_empty() {
        return Err(violation(
            "normalized_event_ids",
            "THRESHOLD provenance requires contributing events",
        ));
    }

    let event_count = u64::try_from(provenance.normalized_event_ids.len()).map_err(|_| {
        violation(
            "event_count",
            "contributing event list exceeds uint64 capacity",
        )
    })?;

    let ThresholdSignalMaterializationContext {
        signal_id,
        severity,
        confidence,
        summary,
        input_relation,
        created_at,
        source_count,
    } = context;

    let inputs = provenance
        .normalized_event_ids
        .iter()
        .enumerate()
        .map(|(index, input_id)| {
            let ordinal = u32::try_from(index).map_err(|_| {
                violation(
                    "inputs.ordinal",
                    "input list exceeds uint32 ordinal capacity",
                )
            })?;
            Ok(SignalInput {
                signal_id: signal_id.clone(),
                input_type: SignalInputType::NormalizedEvent as i32,
                input_id: input_id.clone(),
                relation: input_relation.clone(),
                ordinal,
            })
        })
        .collect::<Result<Vec<_>, ContractViolation>>()?;

    let signal = Signal {
        signal_id,
        tenant_id: provenance.tenant_id.clone(),
        rule_id: provenance.rule_id.as_str().to_string(),
        rule_version: provenance.rule_version.to_string(),
        rule_type: SignalRuleType::Threshold as i32,
        severity,
        confidence,
        status: SignalStatus::Active as i32,
        first_observed_at: Some(provenance.first_observed_at),
        last_observed_at: Some(provenance.last_observed_at),
        created_at: Some(created_at),
        execution_mode: provenance.execution_mode as i32,
        source_count,
        event_count,
        summary,
        provenance: Some(SignalProvenance {
            evaluated_at: Some(provenance.evaluated_at),
            execution_backend: contract_backend(provenance.execution_backend) as i32,
        }),
        inputs,
    };

    validate_signal(&signal)?;
    Ok(signal)
}

#[cfg(test)]
mod tests {
    use std::{
        cell::Cell,
        collections::{BTreeMap, HashMap},
        convert::Infallible,
        str::FromStr as _,
    };

    use cerbero_common::contracts::{
        v1::{ExecutionMode, NormalizedEvent, SignalRuleType},
        validate_signal,
    };
    use prost_types::{Struct, Timestamp, Value, value::Kind};

    use crate::{
        ComparisonExpression, ComparisonOperator, DetectionExpression, EventCompilationStages,
        EventExecutionPlanCompiler, EventFieldProfile, EventFieldProfileResolver, EventFieldState,
        EventFieldView, EventLeafEvaluationPolicy, EventPredicate, ExecutionBackend,
        ExecutionBackendCapabilities, ExecutionPlan, FieldTypeResolver, IdentityEventAstOptimizer,
        NormalizedEventFieldView, RuleCompilationErrorKind, RuleId, ValueTypeResolver,
    };

    use super::{
        ThresholdAggregation, ThresholdEvaluationPolicy, ThresholdLateEventPolicy, ThresholdRule,
        ThresholdSignalMaterializationContext, ThresholdTimeBasis, ThresholdWindow,
        compile_threshold_execution_plan, evaluate_threshold_normalized_events,
        materialize_threshold_signal,
    };

    const TENANT_ID: &str = "018f47a2-4b00-7a00-8000-000000000401";
    const SIGNAL_ID: &str = "018f47a2-4b00-7a00-8000-000000000402";

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum TestType {
        Text,
        Number,
    }

    struct TestFieldResolver {
        fields: HashMap<&'static str, TestType>,
    }

    impl FieldTypeResolver for TestFieldResolver {
        type FieldType = TestType;

        fn resolve_field_type(&self, field: &str) -> Option<Self::FieldType> {
            self.fields.get(field).copied()
        }
    }

    struct TestValueResolver;

    impl ValueTypeResolver<&'static str> for TestValueResolver {
        type ValueType = TestType;

        fn resolve_value_type(&self, _value: &&'static str) -> Self::ValueType {
            TestType::Text
        }
    }

    struct TestComparisonPolicy;

    impl crate::ComparisonTypePolicy<TestType, TestType> for TestComparisonPolicy {
        fn validate_comparison(
            &self,
            field_type: &TestType,
            operator: ComparisonOperator,
            value_type: &TestType,
        ) -> Result<(), RuleCompilationErrorKind> {
            if *field_type != *value_type || operator != ComparisonOperator::Eq {
                return Err(RuleCompilationErrorKind::TypeError);
            }
            Ok(())
        }
    }

    type TestProfile =
        EventFieldProfile<Vec<&'static str>, Vec<&'static str>, Vec<(&'static str, TestType)>>;
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

    struct TestProfileResolver;

    impl EventFieldProfileResolver<&'static str, &'static str> for TestProfileResolver {
        type Profile = TestProfile;

        fn resolve_event_field_profile(
            &self,
            _expression: &DetectionExpression<&'static str, &'static str>,
        ) -> Result<Self::Profile, RuleCompilationErrorKind> {
            Ok(EventFieldProfile::new(
                vec!["status", "src_endpoint.ip", "user.name", "time"],
                Vec::new(),
                vec![
                    ("status", TestType::Text),
                    ("src_endpoint.ip", TestType::Text),
                    ("user.name", TestType::Text),
                    ("time", TestType::Number),
                ],
            ))
        }
    }

    struct TestPlanCompiler {
        calls: Cell<u32>,
    }

    impl EventExecutionPlanCompiler<&'static str, &'static str, TestProfile> for TestPlanCompiler {
        type Plan = TestPlan;

        fn supported_backends(
            &self,
            _predicate: &EventPredicate<&'static str, &'static str>,
            _field_profile: &TestProfile,
        ) -> Result<ExecutionBackendCapabilities, RuleCompilationErrorKind> {
            Ok(ExecutionBackendCapabilities::new(true, true))
        }

        fn compile_event_plan(
            &self,
            backend: ExecutionBackend,
            predicate: &EventPredicate<&'static str, &'static str>,
            field_profile: &TestProfile,
        ) -> Result<Self::Plan, RuleCompilationErrorKind> {
            self.calls.set(self.calls.get() + 1);
            Ok(ExecutionPlan {
                plan_id: "threshold-filter-plan",
                rule_id: RuleId::from_str("CER-DET-000001").expect("valid test rule id"),
                rule_version: 1,
                backend,
                required_fields: field_profile.required_fields().clone(),
                predicates: predicate.clone(),
                grouping: (),
                window: (),
                resource_limits: (),
                plan_hash: "threshold-filter-plan-hash",
            })
        }
    }

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

    /// `google.protobuf.Value.number_value` uses `f64` for JSON numbers.
    ///
    /// The conversion is intentionally isolated to test-fixture glue. The range check uses
    /// exact powers-of-two bounds so the subsequent integer cast cannot overflow or truncate
    /// a fractional value.
    #[allow(clippy::cast_possible_truncation)]
    fn ocsf_number_to_i64(value: f64) -> Option<i64> {
        const I64_MIN_F64: f64 = -9_223_372_036_854_775_808.0;
        const I64_MAX_EXCLUSIVE_F64: f64 = 9_223_372_036_854_775_808.0;

        if !value.is_finite()
            || value.fract() != 0.0
            || value < I64_MIN_F64
            || value >= I64_MAX_EXCLUSIVE_F64
        {
            return None;
        }

        Some(value as i64)
    }

    /// Test-fixture bridge into protobuf/JSON numeric representation.
    ///
    /// Millisecond values used by this suite are below 2^53 and therefore exactly
    /// representable as `f64`.
    #[allow(clippy::cast_precision_loss)]
    fn i64_to_ocsf_number(value: i64) -> f64 {
        debug_assert!(value.unsigned_abs() <= (1_u64 << 53));
        value as f64
    }

    impl ThresholdEvaluationPolicy<&'static str, &'static str> for TestPolicy {
        type GroupKey = Vec<String>;

        fn threshold_group_key(
            &self,
            group_by: &[&'static str],
            event: &NormalizedEvent,
        ) -> Result<Option<Self::GroupKey>, Self::Error> {
            let view = NormalizedEventFieldView::new(event);
            let mut key = Vec::with_capacity(group_by.len());
            for field in group_by {
                let EventFieldState::Present(value) = view.field_state(field) else {
                    return Ok(None);
                };
                let Some(Kind::StringValue(value)) = value.kind.as_ref() else {
                    return Ok(None);
                };
                key.push(value.clone());
            }
            Ok(Some(key))
        }

        fn threshold_event_time_millis(
            &self,
            _time_basis: ThresholdTimeBasis,
            time_field: &&'static str,
            event: &NormalizedEvent,
        ) -> Result<Option<i64>, Self::Error> {
            let view = NormalizedEventFieldView::new(event);
            let EventFieldState::Present(value) = view.field_state(time_field) else {
                return Ok(None);
            };
            let Some(Kind::NumberValue(value)) = value.kind.as_ref() else {
                return Ok(None);
            };
            Ok(ocsf_number_to_i64(*value))
        }
    }

    fn field_resolver() -> TestFieldResolver {
        TestFieldResolver {
            fields: HashMap::from([
                ("status", TestType::Text),
                ("src_endpoint.ip", TestType::Text),
                ("user.name", TestType::Text),
                ("time", TestType::Number),
            ]),
        }
    }

    fn rule() -> ThresholdRule<&'static str, &'static str> {
        ThresholdRule {
            filter: DetectionExpression::Comparison(ComparisonExpression {
                field: "status",
                operator: ComparisonOperator::Eq,
                value: "Failure",
            }),
            group_by: vec!["src_endpoint.ip", "user.name"],
            aggregation: ThresholdAggregation::Count,
            threshold: 10,
            window: ThresholdWindow {
                duration_millis: 300_000,
                time_basis: ThresholdTimeBasis::EventTime,
                time_field: "time",
                late_event_policy: ThresholdLateEventPolicy::Accept,
            },
        }
    }

    fn compiled_plan() -> super::CompiledThresholdExecutionPlan<TestPlan, TestProfile, &'static str>
    {
        let optimizer = IdentityEventAstOptimizer;
        let profile_resolver = TestProfileResolver;
        let plan_compiler = TestPlanCompiler {
            calls: Cell::new(0),
        };
        let stages = EventCompilationStages::new(&optimizer, &profile_resolver, &plan_compiler);

        compile_threshold_execution_plan(
            &rule(),
            ExecutionBackend::ClickHouse,
            &field_resolver(),
            &TestValueResolver,
            &TestComparisonPolicy,
            &stages,
        )
        .expect("valid deterministic THRESHOLD plan")
    }

    fn nested_string(field: &str, child: &str, value: &str) -> (String, Value) {
        (
            field.to_string(),
            Value {
                kind: Some(Kind::StructValue(Struct {
                    fields: BTreeMap::from([(
                        child.to_string(),
                        Value {
                            kind: Some(Kind::StringValue(value.to_string())),
                        },
                    )]),
                })),
            },
        )
    }

    fn event(index: u64, user: &str, ip: &str, time_millis: i64) -> NormalizedEvent {
        let id = format!("018f47a2-4b00-7a00-8000-{index:012x}");
        NormalizedEvent {
            normalized_event_id: id,
            tenant_id: TENANT_ID.to_string(),
            ocsf_event: Some(Struct {
                fields: BTreeMap::from([
                    (
                        "status".to_string(),
                        Value {
                            kind: Some(Kind::StringValue("Failure".to_string())),
                        },
                    ),
                    nested_string("src_endpoint", "ip", ip),
                    nested_string("user", "name", user),
                    (
                        "time".to_string(),
                        Value {
                            kind: Some(Kind::NumberValue(i64_to_ocsf_number(time_millis))),
                        },
                    ),
                ]),
            }),
            ..Default::default()
        }
    }

    fn events(count: usize) -> Vec<NormalizedEvent> {
        (0..count)
            .map(|index| {
                event(
                    u64::try_from(index + 1).expect("small test index"),
                    "admin",
                    "10.0.0.8",
                    1_789_000_000_000 + i64::try_from(index).expect("small test index") * 20_000,
                )
            })
            .collect()
    }

    fn evaluated_at() -> Timestamp {
        Timestamp {
            seconds: 1_800_000_000,
            nanos: 0,
        }
    }

    #[test]
    fn compiler_rejects_stream_without_approximating_semantics() {
        let optimizer = IdentityEventAstOptimizer;
        let profile_resolver = TestProfileResolver;
        let plan_compiler = TestPlanCompiler {
            calls: Cell::new(0),
        };
        let stages = EventCompilationStages::new(&optimizer, &profile_resolver, &plan_compiler);

        let result = compile_threshold_execution_plan(
            &rule(),
            ExecutionBackend::Stream,
            &field_resolver(),
            &TestValueResolver,
            &TestComparisonPolicy,
            &stages,
        );

        assert_eq!(
            result.expect_err("Step 26 must not fake streaming support"),
            RuleCompilationErrorKind::UnsupportedBackend
        );
        assert_eq!(plan_compiler.calls.get(), 0);
    }

    #[test]
    fn ten_matches_nine_does_not_and_duplicate_does_not_extra_count() {
        let plan = compiled_plan();

        let ten = evaluate_threshold_normalized_events(
            &plan,
            &events(10),
            ExecutionMode::Live,
            evaluated_at(),
            &TestPolicy,
        )
        .expect("valid ten-event dataset");
        assert_eq!(ten.len(), 1);
        assert_eq!(ten[0].normalized_event_ids.len(), 10);

        let nine_events = events(9);
        let nine = evaluate_threshold_normalized_events(
            &plan,
            &nine_events,
            ExecutionMode::Live,
            evaluated_at(),
            &TestPolicy,
        )
        .expect("valid nine-event dataset");
        assert!(nine.is_empty());

        let mut duplicate = nine_events;
        duplicate.push(duplicate[0].clone());
        let deduped = evaluate_threshold_normalized_events(
            &plan,
            &duplicate,
            ExecutionMode::Live,
            evaluated_at(),
            &TestPolicy,
        )
        .expect("duplicate delivery remains evaluable");
        assert!(deduped.is_empty());
    }

    #[test]
    fn out_of_order_and_replay_remain_deterministic_and_isolated() {
        let plan = compiled_plan();
        let ordered_events = events(10);
        let mut reversed_events = ordered_events.clone();
        reversed_events.reverse();

        let live = evaluate_threshold_normalized_events(
            &plan,
            &ordered_events,
            ExecutionMode::Live,
            evaluated_at(),
            &TestPolicy,
        )
        .expect("live dataset");
        let replay = evaluate_threshold_normalized_events(
            &plan,
            &reversed_events,
            ExecutionMode::Replay,
            evaluated_at(),
            &TestPolicy,
        )
        .expect("out-of-order replay dataset");

        assert_eq!(live[0].normalized_event_ids, replay[0].normalized_event_ids);
        assert_eq!(live[0].first_observed_at, replay[0].first_observed_at);
        assert_eq!(live[0].last_observed_at, replay[0].last_observed_at);
        assert_eq!(live[0].execution_mode, ExecutionMode::Live);
        assert_eq!(replay[0].execution_mode, ExecutionMode::Replay);
    }

    #[test]
    fn grouping_prevents_cross_user_false_threshold() {
        let plan = compiled_plan();
        let mut mixed = Vec::new();
        for index in 0_i64..5 {
            mixed.push(event(
                u64::try_from(index + 1).expect("small test index"),
                "admin",
                "10.0.0.8",
                1_789_000_000_000 + index * 20_000,
            ));
        }
        for index in 0_i64..5 {
            mixed.push(event(
                u64::try_from(index + 100).expect("small test index"),
                "root",
                "10.0.0.8",
                1_789_000_000_000 + index * 20_000,
            ));
        }

        let matches = evaluate_threshold_normalized_events(
            &plan,
            &mixed,
            ExecutionMode::Live,
            evaluated_at(),
            &TestPolicy,
        )
        .expect("mixed grouping dataset");

        assert!(matches.is_empty());
    }

    #[test]
    fn threshold_match_materializes_canonical_signal_with_all_inputs() {
        let plan = compiled_plan();
        let matches = evaluate_threshold_normalized_events(
            &plan,
            &events(10),
            ExecutionMode::Replay,
            evaluated_at(),
            &TestPolicy,
        )
        .expect("valid threshold dataset");
        let provenance = matches.first().expect("ten events must match");

        let signal = materialize_threshold_signal(
            provenance,
            ThresholdSignalMaterializationContext {
                signal_id: SIGNAL_ID.to_string(),
                severity: "HIGH".to_string(),
                confidence: Some("HIGH".to_string()),
                summary: "10 failed SSH logins within 5m".to_string(),
                input_relation: "THRESHOLD_MATCH".to_string(),
                created_at: Timestamp {
                    seconds: 1_800_000_010,
                    nanos: 0,
                },
                source_count: 1,
            },
        )
        .expect("threshold match must satisfy canonical Signal v1");

        assert_eq!(signal.rule_type, SignalRuleType::Threshold as i32);
        assert_eq!(signal.event_count, 10);
        assert_eq!(signal.inputs.len(), 10);
        assert_eq!(signal.execution_mode, ExecutionMode::Replay as i32);
        assert!(signal.first_observed_at.is_some());
        assert!(signal.last_observed_at.is_some());
        assert_eq!(validate_signal(&signal), Ok(()));
    }
}
