use std::{collections::BTreeMap, fmt};

use cerbero_common::contracts::{
    ContractViolation,
    v1::{ExecutionMode, Signal},
    validate_signal, validate_timestamp,
};
use prost_types::Timestamp;

use crate::{ExecutionBackend, ExecutionBackendCapabilities, RuleCompilationErrorKind};

/// Backend-neutral SEQUENCE correlation rule contract for the first correlation vertical.
///
/// Stage selection, join-key extraction, and ordering semantics remain behind an
/// evaluation policy because canonical Signal v1 does not embed arbitrary OCSF
/// fields directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequenceCorrelationRule<Stage, JoinKey, Ordering> {
    pub correlation_rule_id: String,
    pub correlation_rule_version: String,
    pub stages: Vec<Stage>,
    pub join_keys: Vec<JoinKey>,
    pub max_window_millis: u64,
    pub ordering: Ordering,
    pub configuration_hash: String,
}

/// Compiled deterministic SEQUENCE correlation plan.
///
/// Step 27 supports bounded ClickHouse/batch evaluation only. Streaming state,
/// checkpointing, watermarks, and correlation-state retention remain governed
/// open decisions and are therefore not approximated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledSequenceCorrelationPlan<Stage, JoinKey, Ordering> {
    correlation_rule_id: String,
    correlation_rule_version: String,
    stages: Vec<Stage>,
    join_keys: Vec<JoinKey>,
    max_window_millis: u64,
    ordering: Ordering,
    backend: ExecutionBackend,
    supported_backends: ExecutionBackendCapabilities,
    configuration_hash: String,
}

impl<Stage, JoinKey, Ordering> CompiledSequenceCorrelationPlan<Stage, JoinKey, Ordering> {
    #[must_use]
    pub fn correlation_rule_id(&self) -> &str {
        &self.correlation_rule_id
    }

    #[must_use]
    pub fn correlation_rule_version(&self) -> &str {
        &self.correlation_rule_version
    }

    #[must_use]
    pub fn stages(&self) -> &[Stage] {
        &self.stages
    }

    #[must_use]
    pub fn join_keys(&self) -> &[JoinKey] {
        &self.join_keys
    }

    #[must_use]
    pub const fn max_window_millis(&self) -> u64 {
        self.max_window_millis
    }

    #[must_use]
    pub const fn ordering(&self) -> &Ordering {
        &self.ordering
    }

    #[must_use]
    pub const fn backend(&self) -> ExecutionBackend {
        self.backend
    }

    #[must_use]
    pub const fn supported_backends(&self) -> ExecutionBackendCapabilities {
        self.supported_backends
    }

    #[must_use]
    pub fn configuration_hash(&self) -> &str {
        &self.configuration_hash
    }
}

fn has_text(value: &str) -> bool {
    !value.trim().is_empty()
}

/// Compiles a bounded SEQUENCE correlation rule without introducing streaming approximations.
///
/// # Errors
///
/// Returns a frozen compilation error when identity/version/configuration metadata,
/// stages, join keys, window configuration, or backend selection cannot preserve the
/// governed SEQUENCE semantics.
pub fn compile_sequence_correlation_plan<Stage, JoinKey, Ordering>(
    rule: &SequenceCorrelationRule<Stage, JoinKey, Ordering>,
    backend: ExecutionBackend,
) -> Result<CompiledSequenceCorrelationPlan<Stage, JoinKey, Ordering>, RuleCompilationErrorKind>
where
    Stage: Clone,
    JoinKey: Clone + PartialEq,
    Ordering: Clone,
{
    if backend != ExecutionBackend::ClickHouse {
        return Err(RuleCompilationErrorKind::UnsupportedBackend);
    }
    if !has_text(&rule.correlation_rule_id)
        || !has_text(&rule.correlation_rule_version)
        || !has_text(&rule.configuration_hash)
        || rule.stages.is_empty()
        || rule.join_keys.is_empty()
    {
        return Err(RuleCompilationErrorKind::InvalidMapping);
    }
    if rule.max_window_millis == 0 || i64::try_from(rule.max_window_millis).is_err() {
        return Err(RuleCompilationErrorKind::InvalidWindow);
    }

    for (index, join_key) in rule.join_keys.iter().enumerate() {
        if rule.join_keys[..index]
            .iter()
            .any(|previous| previous == join_key)
        {
            return Err(RuleCompilationErrorKind::InvalidMapping);
        }
    }

    Ok(CompiledSequenceCorrelationPlan {
        correlation_rule_id: rule.correlation_rule_id.clone(),
        correlation_rule_version: rule.correlation_rule_version.clone(),
        stages: rule.stages.clone(),
        join_keys: rule.join_keys.clone(),
        max_window_millis: rule.max_window_millis,
        ordering: rule.ordering.clone(),
        backend,
        supported_backends: ExecutionBackendCapabilities::new(true, false),
        configuration_hash: rule.configuration_hash.clone(),
    })
}

/// Policy boundary for stage matching, grouping, temporal extraction, and ordering.
///
/// The policy intentionally owns how canonical Signals are mapped back to stage
/// predicates and join keys. Step 27 therefore does not pretend that fields such
/// as `user.name` are embedded directly in Signal v1.
pub trait SequenceCorrelationEvaluationPolicy<Stage, JoinKey, Ordering> {
    type GroupKey: Ord + Clone;
    type OrderKey: Ord + Clone;
    type Error;

    /// Returns whether one canonical Signal satisfies the requested sequence stage.
    ///
    /// # Errors
    ///
    /// Propagates policy-owned stage-matching errors.
    fn stage_matches(&self, stage: &Stage, signal: &Signal) -> Result<bool, Self::Error>;

    /// Returns the logical join/group key, or `None` when the Signal cannot
    /// participate in this sequence.
    ///
    /// # Errors
    ///
    /// Propagates policy-owned join-key extraction errors.
    fn sequence_group_key(
        &self,
        join_keys: &[JoinKey],
        signal: &Signal,
    ) -> Result<Option<Self::GroupKey>, Self::Error>;

    /// Returns the governed temporal value in Unix milliseconds, or `None` when
    /// the Signal cannot participate in this temporal sequence.
    ///
    /// # Errors
    ///
    /// Propagates policy-owned timestamp extraction errors.
    fn sequence_time_millis(&self, signal: &Signal) -> Result<Option<i64>, Self::Error>;

    /// Returns the ordering key used to establish stage order, or `None` when
    /// ordering cannot be established for this Signal.
    ///
    /// # Errors
    ///
    /// Propagates policy-owned ordering extraction errors.
    fn sequence_order_key(
        &self,
        ordering: &Ordering,
        signal: &Signal,
    ) -> Result<Option<Self::OrderKey>, Self::Error>;
}

/// Deterministic pre-Finding evidence emitted by one SEQUENCE correlation match.
///
/// `output_finding_id` is deliberately absent at this boundary: Detection &
/// Correlation v1 requires it in completed correlation provenance, but Step 27
/// does not invent a canonical Finding/FindingInput wire shape. A later governed
/// Finding materializer will attach the allocated Finding identity while
/// preserving these inputs unchanged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequenceCorrelationMatch {
    pub correlation_rule_id: String,
    pub correlation_rule_version: String,
    pub tenant_id: String,
    pub window_start: Timestamp,
    pub window_end: Timestamp,
    pub input_signal_ids: Vec<String>,
    pub execution_backend: ExecutionBackend,
    pub execution_mode: ExecutionMode,
    pub configuration_hash: String,
}

/// SEQUENCE evaluation failure before Finding materialization.
#[derive(Debug)]
pub enum SequenceCorrelationEvaluationError<PolicyError> {
    Policy(PolicyError),
    InvalidInput(ContractViolation),
    UnspecifiedExecutionMode,
}

impl<PolicyError> fmt::Display for SequenceCorrelationEvaluationError<PolicyError>
where
    PolicyError: fmt::Display,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Policy(error) => {
                write!(formatter, "SEQUENCE correlation evaluation failed: {error}")
            }
            Self::InvalidInput(error) => {
                write!(formatter, "invalid SEQUENCE correlation input: {error}")
            }
            Self::UnspecifiedExecutionMode => formatter
                .write_str("SEQUENCE correlation requires LIVE, REPLAY, or TEST execution mode"),
        }
    }
}

impl<PolicyError> std::error::Error for SequenceCorrelationEvaluationError<PolicyError> where
    PolicyError: std::error::Error + 'static
{
}

#[derive(Debug)]
struct Candidate<OrderKey> {
    signal_id: String,
    stage_indices: Vec<usize>,
    time_millis: i64,
    order_key: OrderKey,
}

type CandidateGroups<GroupKey, OrderKey> = BTreeMap<(String, GroupKey), Vec<Candidate<OrderKey>>>;
type CandidateCollectionResult<GroupKey, OrderKey, PolicyError> =
    Result<CandidateGroups<GroupKey, OrderKey>, SequenceCorrelationEvaluationError<PolicyError>>;
type SelectedSequence = (Vec<usize>, i64, i64);

fn violation(field: &'static str, reason: impl Into<String>) -> ContractViolation {
    ContractViolation {
        field,
        reason: reason.into(),
    }
}

fn timestamp_from_millis(value: i64) -> Timestamp {
    Timestamp {
        seconds: value.div_euclid(1_000),
        nanos: i32::try_from(value.rem_euclid(1_000) * 1_000_000)
            .expect("millisecond remainder always fits i32 nanos"),
    }
}

fn validate_execution_mode(
    signal: &Signal,
    execution_mode: ExecutionMode,
) -> Result<(), ContractViolation> {
    if signal.execution_mode != execution_mode as i32 {
        return Err(violation(
            "signals.execution_mode",
            "must equal the bounded correlation evaluation mode",
        ));
    }
    Ok(())
}

fn validate_unique_signals(
    signals: &[Signal],
    execution_mode: ExecutionMode,
) -> Result<BTreeMap<String, &Signal>, ContractViolation> {
    let mut unique_signals = BTreeMap::new();

    for signal in signals {
        validate_signal(signal)?;
        validate_execution_mode(signal, execution_mode)?;

        if let Some(existing) = unique_signals.get(&signal.signal_id) {
            if *existing != signal {
                return Err(violation(
                    "signals.signal_id",
                    "duplicate identity carries conflicting canonical Signal content",
                ));
            }
            continue;
        }

        unique_signals.insert(signal.signal_id.clone(), signal);
    }

    Ok(unique_signals)
}

fn collect_sequence_candidates<Stage, JoinKey, Ordering, Policy>(
    plan: &CompiledSequenceCorrelationPlan<Stage, JoinKey, Ordering>,
    signals: BTreeMap<String, &Signal>,
    policy: &Policy,
) -> CandidateCollectionResult<Policy::GroupKey, Policy::OrderKey, Policy::Error>
where
    Policy: SequenceCorrelationEvaluationPolicy<Stage, JoinKey, Ordering>,
{
    let mut groups: CandidateGroups<Policy::GroupKey, Policy::OrderKey> = BTreeMap::new();

    for signal in signals.into_values() {
        let mut stage_indices = Vec::new();
        for (stage_index, stage) in plan.stages.iter().enumerate() {
            if policy
                .stage_matches(stage, signal)
                .map_err(SequenceCorrelationEvaluationError::Policy)?
            {
                stage_indices.push(stage_index);
            }
        }
        if stage_indices.is_empty() {
            continue;
        }

        let Some(group_key) = policy
            .sequence_group_key(&plan.join_keys, signal)
            .map_err(SequenceCorrelationEvaluationError::Policy)?
        else {
            continue;
        };
        let Some(time_millis) = policy
            .sequence_time_millis(signal)
            .map_err(SequenceCorrelationEvaluationError::Policy)?
        else {
            continue;
        };
        let Some(order_key) = policy
            .sequence_order_key(&plan.ordering, signal)
            .map_err(SequenceCorrelationEvaluationError::Policy)?
        else {
            continue;
        };

        groups
            .entry((signal.tenant_id.clone(), group_key))
            .or_default()
            .push(Candidate {
                signal_id: signal.signal_id.clone(),
                stage_indices,
                time_millis,
                order_key,
            });
    }

    Ok(groups)
}

fn select_sequence<OrderKey>(
    candidates: &[Candidate<OrderKey>],
    stage_count: usize,
    max_window_millis: i64,
) -> Option<SelectedSequence>
where
    OrderKey: Ord + Clone,
{
    for (start_index, start_candidate) in candidates.iter().enumerate() {
        if !start_candidate.stage_indices.contains(&0) {
            continue;
        }

        let mut selected_indices = vec![start_index];
        let mut last_index = start_index;
        let mut last_order_key = start_candidate.order_key.clone();
        let mut min_time = start_candidate.time_millis;
        let mut max_time = start_candidate.time_millis;
        let mut complete = true;

        for stage_index in 1..stage_count {
            let mut next_selection = None;

            for (candidate_index, candidate) in candidates.iter().enumerate().skip(last_index + 1) {
                if !candidate.stage_indices.contains(&stage_index)
                    || candidate.order_key <= last_order_key
                {
                    continue;
                }

                let candidate_min_time = min_time.min(candidate.time_millis);
                let candidate_max_time = max_time.max(candidate.time_millis);
                if i128::from(candidate_max_time) - i128::from(candidate_min_time)
                    > i128::from(max_window_millis)
                {
                    continue;
                }

                next_selection = Some((candidate_index, candidate_min_time, candidate_max_time));
                break;
            }

            let Some((candidate_index, candidate_min_time, candidate_max_time)) = next_selection
            else {
                complete = false;
                break;
            };

            selected_indices.push(candidate_index);
            last_index = candidate_index;
            last_order_key = candidates[candidate_index].order_key.clone();
            min_time = candidate_min_time;
            max_time = candidate_max_time;
        }

        if complete {
            return Some((selected_indices, min_time, max_time));
        }
    }

    None
}

/// Evaluates one bounded canonical-Signal dataset deterministically for SEQUENCE matches.
///
/// Input order is irrelevant. Canonical Signals are validated, duplicate
/// `signal_id` deliveries are collapsed only when the complete Signal is identical,
/// candidates are partitioned by tenant plus a policy-owned join key, and sequence
/// stages are selected using a policy-owned strict ordering key. At most one earliest
/// qualifying sequence is emitted per tenant/join-key group.
///
/// # Errors
///
/// Returns a policy error, canonical Signal validation error, duplicate-identity
/// conflict, execution-mode mismatch, invalid derived timestamp, or unspecified
/// execution-mode error.
pub fn evaluate_sequence_signals<Stage, JoinKey, Ordering, Policy>(
    plan: &CompiledSequenceCorrelationPlan<Stage, JoinKey, Ordering>,
    signals: &[Signal],
    execution_mode: ExecutionMode,
    policy: &Policy,
) -> Result<Vec<SequenceCorrelationMatch>, SequenceCorrelationEvaluationError<Policy::Error>>
where
    Policy: SequenceCorrelationEvaluationPolicy<Stage, JoinKey, Ordering>,
{
    if execution_mode == ExecutionMode::Unspecified {
        return Err(SequenceCorrelationEvaluationError::UnspecifiedExecutionMode);
    }

    let max_window_millis = i64::try_from(plan.max_window_millis).map_err(|_| {
        SequenceCorrelationEvaluationError::InvalidInput(violation(
            "max_window_millis",
            "exceeds supported deterministic batch range",
        ))
    })?;
    let unique_signals = validate_unique_signals(signals, execution_mode)
        .map_err(SequenceCorrelationEvaluationError::InvalidInput)?;
    let groups = collect_sequence_candidates(plan, unique_signals, policy)?;
    let mut matches = Vec::new();

    for ((tenant_id, _group_key), mut candidates) in groups {
        candidates.sort_by(|left, right| {
            left.order_key
                .cmp(&right.order_key)
                .then_with(|| left.signal_id.cmp(&right.signal_id))
        });

        let Some((selected_indices, min_time, max_time)) =
            select_sequence(&candidates, plan.stages.len(), max_window_millis)
        else {
            continue;
        };

        let window_start = timestamp_from_millis(min_time);
        let window_end = timestamp_from_millis(max_time);
        validate_timestamp("window_start", &window_start)
            .map_err(SequenceCorrelationEvaluationError::InvalidInput)?;
        validate_timestamp("window_end", &window_end)
            .map_err(SequenceCorrelationEvaluationError::InvalidInput)?;

        let input_signal_ids = selected_indices
            .iter()
            .map(|index| candidates[*index].signal_id.clone())
            .collect();

        matches.push(SequenceCorrelationMatch {
            correlation_rule_id: plan.correlation_rule_id.clone(),
            correlation_rule_version: plan.correlation_rule_version.clone(),
            tenant_id,
            window_start,
            window_end,
            input_signal_ids,
            execution_backend: plan.backend,
            execution_mode,
            configuration_hash: plan.configuration_hash.clone(),
        });
    }

    Ok(matches)
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use cerbero_common::contracts::{
        v1::{
            DetectionExecutionBackend, ExecutionMode, Signal, SignalInput, SignalInputType,
            SignalProvenance, SignalRuleType, SignalStatus,
        },
        validate_signal,
    };

    use super::{
        SequenceCorrelationEvaluationError, SequenceCorrelationEvaluationPolicy,
        SequenceCorrelationRule, compile_sequence_correlation_plan, evaluate_sequence_signals,
        timestamp_from_millis,
    };
    use crate::{ExecutionBackend, RuleCompilationErrorKind};

    const TENANT_A: &str = "018f47a2-4b00-7a00-8000-000000000501";
    const TENANT_B: &str = "018f47a2-4b00-7a00-8000-000000000502";

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Ordering {
        Observed,
    }

    struct TestPolicy;

    impl SequenceCorrelationEvaluationPolicy<&'static str, &'static str, Ordering> for TestPolicy {
        type GroupKey = String;
        type OrderKey = i64;
        type Error = Infallible;

        fn stage_matches(
            &self,
            stage: &&'static str,
            signal: &Signal,
        ) -> Result<bool, Self::Error> {
            Ok(signal.rule_id == *stage)
        }

        fn sequence_group_key(
            &self,
            join_keys: &[&'static str],
            signal: &Signal,
        ) -> Result<Option<Self::GroupKey>, Self::Error> {
            if join_keys != ["user.name"] {
                return Ok(None);
            }
            Ok(signal.summary.strip_prefix("user=").map(ToOwned::to_owned))
        }

        fn sequence_time_millis(&self, signal: &Signal) -> Result<Option<i64>, Self::Error> {
            Ok(signal.first_observed_at.as_ref().map(|timestamp| {
                timestamp.seconds * 1_000 + i64::from(timestamp.nanos / 1_000_000)
            }))
        }

        fn sequence_order_key(
            &self,
            _ordering: &Ordering,
            signal: &Signal,
        ) -> Result<Option<Self::OrderKey>, Self::Error> {
            self.sequence_time_millis(signal)
        }
    }

    fn rule() -> SequenceCorrelationRule<&'static str, &'static str, Ordering> {
        SequenceCorrelationRule {
            correlation_rule_id: "CER-COR-0003".to_string(),
            correlation_rule_version: "2".to_string(),
            stages: vec!["CER-DET-000101", "CER-DET-000102", "CER-DET-000103"],
            join_keys: vec!["user.name"],
            max_window_millis: 600_000,
            ordering: Ordering::Observed,
            configuration_hash: "test-sequence-config-v1".to_string(),
        }
    }

    fn plan() -> super::CompiledSequenceCorrelationPlan<&'static str, &'static str, Ordering> {
        compile_sequence_correlation_plan(&rule(), ExecutionBackend::ClickHouse)
            .expect("valid bounded SEQUENCE plan")
    }

    fn signal(
        index: u64,
        tenant_id: &str,
        rule_id: &str,
        user: &str,
        time_millis: i64,
        execution_mode: ExecutionMode,
    ) -> Signal {
        let signal_id = format!("018f47a2-4b00-7a00-8000-{index:012x}");
        let input_id = format!("018f47a2-4b00-7a00-8001-{index:012x}");
        let observed = timestamp_from_millis(time_millis);
        let created = timestamp_from_millis(time_millis + 1_000);

        Signal {
            signal_id: signal_id.clone(),
            tenant_id: tenant_id.to_string(),
            rule_id: rule_id.to_string(),
            rule_version: "1".to_string(),
            rule_type: SignalRuleType::Event as i32,
            severity: "MEDIUM".to_string(),
            confidence: None,
            status: SignalStatus::Active as i32,
            first_observed_at: Some(observed),
            last_observed_at: Some(observed),
            created_at: Some(created),
            execution_mode: execution_mode as i32,
            source_count: 1,
            event_count: 1,
            summary: format!("user={user}"),
            provenance: Some(SignalProvenance {
                evaluated_at: Some(created),
                execution_backend: DetectionExecutionBackend::Clickhouse as i32,
            }),
            inputs: vec![SignalInput {
                signal_id,
                input_type: SignalInputType::NormalizedEvent as i32,
                input_id,
                relation: "EVENT_MATCH".to_string(),
                ordinal: 0,
            }],
        }
    }

    fn sequence(tenant_id: &str, user: &str, mode: ExecutionMode) -> Vec<Signal> {
        vec![
            signal(
                1,
                tenant_id,
                "CER-DET-000101",
                user,
                1_789_000_000_000,
                mode,
            ),
            signal(
                2,
                tenant_id,
                "CER-DET-000102",
                user,
                1_789_000_060_000,
                mode,
            ),
            signal(
                3,
                tenant_id,
                "CER-DET-000103",
                user,
                1_789_000_120_000,
                mode,
            ),
        ]
    }

    #[test]
    fn compiler_rejects_stream_without_approximating_stateful_semantics() {
        let result = compile_sequence_correlation_plan(&rule(), ExecutionBackend::Stream);

        assert_eq!(
            result.expect_err("Step 27 must not fake streaming correlation"),
            RuleCompilationErrorKind::UnsupportedBackend
        );
    }

    #[test]
    fn complete_sequence_matches_and_incomplete_sequence_does_not() {
        let signals = sequence(TENANT_A, "jdoe", ExecutionMode::Live);

        let matches =
            evaluate_sequence_signals(&plan(), &signals, ExecutionMode::Live, &TestPolicy)
                .expect("valid sequence dataset");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].input_signal_ids.len(), 3);
        assert_eq!(matches[0].correlation_rule_id, "CER-COR-0003");
        assert_eq!(matches[0].correlation_rule_version, "2");
        assert_eq!(matches[0].tenant_id, TENANT_A);
        assert_eq!(matches[0].execution_backend, ExecutionBackend::ClickHouse);
        assert_eq!(matches[0].execution_mode, ExecutionMode::Live);

        let incomplete = &signals[..2];
        let no_match =
            evaluate_sequence_signals(&plan(), incomplete, ExecutionMode::Live, &TestPolicy)
                .expect("valid incomplete dataset");
        assert!(no_match.is_empty());
    }

    #[test]
    fn order_window_user_and_tenant_boundaries_prevent_false_matches() {
        let mut wrong_order = sequence(TENANT_A, "jdoe", ExecutionMode::Live);
        wrong_order[1] = signal(
            22,
            TENANT_A,
            "CER-DET-000102",
            "jdoe",
            1_788_999_900_000,
            ExecutionMode::Live,
        );
        assert!(
            evaluate_sequence_signals(&plan(), &wrong_order, ExecutionMode::Live, &TestPolicy)
                .expect("wrong-order dataset remains evaluable")
                .is_empty()
        );

        let mut outside_window = sequence(TENANT_A, "jdoe", ExecutionMode::Live);
        outside_window[2] = signal(
            23,
            TENANT_A,
            "CER-DET-000103",
            "jdoe",
            1_789_000_601_000,
            ExecutionMode::Live,
        );
        assert!(
            evaluate_sequence_signals(&plan(), &outside_window, ExecutionMode::Live, &TestPolicy,)
                .expect("outside-window dataset remains evaluable")
                .is_empty()
        );

        let mixed_user = vec![
            signal(
                31,
                TENANT_A,
                "CER-DET-000101",
                "jdoe",
                1_789_000_000_000,
                ExecutionMode::Live,
            ),
            signal(
                32,
                TENANT_A,
                "CER-DET-000102",
                "root",
                1_789_000_060_000,
                ExecutionMode::Live,
            ),
            signal(
                33,
                TENANT_A,
                "CER-DET-000103",
                "jdoe",
                1_789_000_120_000,
                ExecutionMode::Live,
            ),
        ];
        assert!(
            evaluate_sequence_signals(&plan(), &mixed_user, ExecutionMode::Live, &TestPolicy)
                .expect("mixed-user dataset remains evaluable")
                .is_empty()
        );

        let mixed_tenant = vec![
            signal(
                41,
                TENANT_A,
                "CER-DET-000101",
                "jdoe",
                1_789_000_000_000,
                ExecutionMode::Live,
            ),
            signal(
                42,
                TENANT_B,
                "CER-DET-000102",
                "jdoe",
                1_789_000_060_000,
                ExecutionMode::Live,
            ),
            signal(
                43,
                TENANT_A,
                "CER-DET-000103",
                "jdoe",
                1_789_000_120_000,
                ExecutionMode::Live,
            ),
        ];
        assert!(
            evaluate_sequence_signals(&plan(), &mixed_tenant, ExecutionMode::Live, &TestPolicy)
                .expect("mixed-tenant dataset remains evaluable")
                .is_empty()
        );
    }

    #[test]
    fn duplicates_and_input_reordering_remain_deterministic() {
        let ordered = sequence(TENANT_A, "jdoe", ExecutionMode::Live);
        let mut reversed = ordered.clone();
        reversed.reverse();
        reversed.push(ordered[0].clone());

        let expected =
            evaluate_sequence_signals(&plan(), &ordered, ExecutionMode::Live, &TestPolicy)
                .expect("ordered sequence");
        let actual =
            evaluate_sequence_signals(&plan(), &reversed, ExecutionMode::Live, &TestPolicy)
                .expect("reordered duplicate sequence");

        assert_eq!(expected, actual);
    }

    #[test]
    fn conflicting_duplicate_identity_fails_closed() {
        let mut signals = sequence(TENANT_A, "jdoe", ExecutionMode::Live);
        let mut conflicting = signals[0].clone();
        conflicting.summary = "user=root".to_string();
        signals.push(conflicting);

        let error = evaluate_sequence_signals(&plan(), &signals, ExecutionMode::Live, &TestPolicy)
            .expect_err("conflicting duplicate identity must fail closed");

        let SequenceCorrelationEvaluationError::InvalidInput(error) = error else {
            panic!("expected canonical input failure");
        };
        assert_eq!(error.field, "signals.signal_id");
    }

    #[test]
    fn live_and_replay_inputs_are_isolated() {
        let live = sequence(TENANT_A, "jdoe", ExecutionMode::Live);
        let mismatch =
            evaluate_sequence_signals(&plan(), &live, ExecutionMode::Replay, &TestPolicy)
                .expect_err("LIVE Signals cannot enter REPLAY correlation");

        let SequenceCorrelationEvaluationError::InvalidInput(error) = mismatch else {
            panic!("expected execution-mode input failure");
        };
        assert_eq!(error.field, "signals.execution_mode");

        let replay = sequence(TENANT_A, "jdoe", ExecutionMode::Replay);
        let matches =
            evaluate_sequence_signals(&plan(), &replay, ExecutionMode::Replay, &TestPolicy)
                .expect("valid replay sequence");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].execution_mode, ExecutionMode::Replay);
    }

    #[test]
    fn invalid_canonical_signal_fails_closed() {
        let mut signals = sequence(TENANT_A, "jdoe", ExecutionMode::Live);
        signals[0].signal_id = "not-a-uuid".to_string();

        let error = evaluate_sequence_signals(&plan(), &signals, ExecutionMode::Live, &TestPolicy)
            .expect_err("invalid canonical Signal must fail closed");

        let SequenceCorrelationEvaluationError::InvalidInput(error) = error else {
            panic!("expected canonical Signal validation failure");
        };
        assert_eq!(error.field, "signal_id");
    }

    #[test]
    fn test_fixture_signals_satisfy_canonical_signal_contract() {
        for signal in sequence(TENANT_A, "jdoe", ExecutionMode::Test) {
            assert_eq!(validate_signal(&signal), Ok(()));
        }
    }
}
