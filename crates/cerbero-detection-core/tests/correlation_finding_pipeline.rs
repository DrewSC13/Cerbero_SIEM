use std::convert::Infallible;

use cerbero_common::contracts::{
    v1::{
        DetectionExecutionBackend, ExecutionMode, FindingDisposition, FindingStatus, FindingType,
        Signal, SignalInput, SignalInputType, SignalProvenance, SignalRuleType, SignalStatus,
    },
    validate_finding,
};
use cerbero_detection_core::{
    CorrelationFindingMaterializationContext, ExecutionBackend,
    SequenceCorrelationEvaluationPolicy, SequenceCorrelationRule,
    compile_sequence_correlation_plan, evaluate_sequence_signals,
    materialize_sequence_correlation_finding,
};
use prost_types::Timestamp;

const TENANT_ID: &str = "018f47a2-4b00-7a00-8000-000000000801";
const FINDING_ID: &str = "018f47a2-4b00-7a00-8000-000000000802";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ordering {
    Observed,
}

struct TestPolicy;

impl SequenceCorrelationEvaluationPolicy<&'static str, &'static str, Ordering> for TestPolicy {
    type GroupKey = String;
    type OrderKey = i64;
    type Error = Infallible;

    fn stage_matches(&self, stage: &&'static str, signal: &Signal) -> Result<bool, Self::Error> {
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
        Ok(signal
            .first_observed_at
            .as_ref()
            .map(|timestamp| timestamp.seconds * 1_000 + i64::from(timestamp.nanos / 1_000_000)))
    }

    fn sequence_order_key(
        &self,
        _ordering: &Ordering,
        signal: &Signal,
    ) -> Result<Option<Self::OrderKey>, Self::Error> {
        self.sequence_time_millis(signal)
    }
}

fn timestamp_from_millis(value: i64) -> Timestamp {
    Timestamp {
        seconds: value.div_euclid(1_000),
        nanos: i32::try_from(value.rem_euclid(1_000) * 1_000_000)
            .expect("millisecond remainder always fits timestamp nanos"),
    }
}

fn rule() -> SequenceCorrelationRule<&'static str, &'static str, Ordering> {
    SequenceCorrelationRule {
        correlation_rule_id: "CER-COR-0003".to_owned(),
        correlation_rule_version: "2".to_owned(),
        stages: vec!["CER-DET-000101", "CER-DET-000102", "CER-DET-000103"],
        join_keys: vec!["user.name"],
        max_window_millis: 600_000,
        ordering: Ordering::Observed,
        configuration_hash: "sequence-config-v1".to_owned(),
    }
}

fn signal(index: u64, rule_id: &str, time_millis: i64) -> Signal {
    let signal_id = format!("018f47a2-4b00-7a00-8000-{index:012x}");
    let normalized_event_id = format!("018f47a2-4b00-7a00-8001-{index:012x}");
    let observed = timestamp_from_millis(time_millis);
    let created = timestamp_from_millis(time_millis + 1_000);

    Signal {
        signal_id: signal_id.clone(),
        tenant_id: TENANT_ID.to_owned(),
        rule_id: rule_id.to_owned(),
        rule_version: "1".to_owned(),
        rule_type: SignalRuleType::Event as i32,
        severity: "MEDIUM".to_owned(),
        confidence: None,
        status: SignalStatus::Active as i32,
        first_observed_at: Some(observed),
        last_observed_at: Some(observed),
        created_at: Some(created),
        execution_mode: ExecutionMode::Replay as i32,
        source_count: 1,
        event_count: 1,
        summary: "user=jdoe".to_owned(),
        provenance: Some(SignalProvenance {
            evaluated_at: Some(created),
            execution_backend: DetectionExecutionBackend::Clickhouse as i32,
        }),
        inputs: vec![SignalInput {
            signal_id,
            input_type: SignalInputType::NormalizedEvent as i32,
            input_id: normalized_event_id,
            relation: "EVENT_MATCH".to_owned(),
            ordinal: 0,
        }],
    }
}

#[test]
fn canonical_signals_flow_through_sequence_match_into_canonical_finding() {
    let plan = compile_sequence_correlation_plan(&rule(), ExecutionBackend::ClickHouse)
        .expect("bounded ClickHouse SEQUENCE plan should compile");
    let signals = vec![
        signal(0x811, "CER-DET-000101", 1_789_000_000_000),
        signal(0x812, "CER-DET-000102", 1_789_000_060_000),
        signal(0x813, "CER-DET-000103", 1_789_000_120_000),
    ];

    let matches = evaluate_sequence_signals(&plan, &signals, ExecutionMode::Replay, &TestPolicy)
        .expect("canonical Signals should evaluate deterministically");
    assert_eq!(matches.len(), 1);

    let correlation_match = &matches[0];
    let expected_signal_ids: Vec<_> = signals
        .iter()
        .map(|signal| signal.signal_id.clone())
        .collect();
    assert_eq!(correlation_match.input_signal_ids, expected_signal_ids);

    let finding = materialize_sequence_correlation_finding(
        correlation_match,
        CorrelationFindingMaterializationContext {
            finding_id: FINDING_ID.to_owned(),
            severity: "HIGH".to_owned(),
            confidence: Some("MEDIUM".to_owned()),
            title: "Account compromise sequence".to_owned(),
            description: "Failed login followed by success and privilege escalation".to_owned(),
            input_relation: "SEQUENCE_INPUT".to_owned(),
            created_at: timestamp_from_millis(1_789_000_130_000),
        },
    )
    .expect("SEQUENCE match should materialize canonical Finding");

    assert_eq!(finding.finding_id, FINDING_ID);
    assert_eq!(finding.tenant_id, TENANT_ID);
    assert_eq!(finding.finding_type, FindingType::Correlation as i32);
    assert_eq!(finding.status, FindingStatus::Open as i32);
    assert_eq!(finding.disposition, FindingDisposition::Undetermined as i32);
    assert_eq!(finding.execution_mode, ExecutionMode::Replay as i32);
    assert_eq!(
        finding.first_seen,
        Some(timestamp_from_millis(1_789_000_000_000))
    );
    assert_eq!(
        finding.last_seen,
        Some(timestamp_from_millis(1_789_000_120_000))
    );

    let finding_input_ids: Vec<_> = finding
        .inputs
        .iter()
        .map(|input| input.input_id.clone())
        .collect();
    assert_eq!(finding_input_ids, expected_signal_ids);

    let provenance = finding
        .correlation_provenance
        .as_ref()
        .expect("typed correlation provenance is mandatory");
    assert_eq!(provenance.input_ids, expected_signal_ids);
    assert_eq!(provenance.output_finding_id, FINDING_ID);
    assert_eq!(provenance.correlation_rule_id, "CER-COR-0003");
    assert_eq!(provenance.correlation_rule_version, "2");
    assert_eq!(
        provenance.execution_backend,
        DetectionExecutionBackend::Clickhouse as i32
    );
    assert_eq!(provenance.execution_mode, ExecutionMode::Replay as i32);
    assert_eq!(provenance.configuration_hash, "sequence-config-v1");
    assert_eq!(validate_finding(&finding), Ok(()));
}
