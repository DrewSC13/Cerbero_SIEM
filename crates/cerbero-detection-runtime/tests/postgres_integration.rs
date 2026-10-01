use cerbero_common::contracts::v1::{
    DetectionExecutionBackend, ExecutionMode, Finding, FindingCorrelationProvenance,
    FindingDisposition, FindingInput, FindingStatus, FindingType, Signal, SignalInput,
    SignalInputType, SignalProvenance, SignalRuleType, SignalStatus,
};
use cerbero_detection_runtime::PostgresAnalyticalRepository;
use prost_types::Timestamp;

const TENANT: &str = "018f47a2-4b00-7a00-8000-00000000d001";
const SIGNAL: &str = "018f47a2-4b00-7a00-8000-00000000d101";
const EVENT: &str = "018f47a2-4b00-7a00-8000-00000000d102";
const FINDING: &str = "018f47a2-4b00-7a00-8000-00000000d201";
fn time(seconds: i64) -> Timestamp {
    Timestamp { seconds, nanos: 0 }
}
fn connection_string() -> String {
    format!(
        "host={} port={} dbname={} user={} password={}",
        std::env::var("POSTGRES_HOST").unwrap(),
        std::env::var("POSTGRES_PORT").unwrap(),
        std::env::var("POSTGRES_DB").unwrap(),
        std::env::var("POSTGRES_DETECTION_USER").unwrap(),
        std::env::var("POSTGRES_DETECTION_PASSWORD").unwrap()
    )
}
fn signal(id: &str) -> Signal {
    Signal {
        signal_id: id.to_string(),
        tenant_id: TENANT.to_string(),
        rule_id: "CER-DET-000101".to_string(),
        rule_version: "1".to_string(),
        rule_type: SignalRuleType::Event as i32,
        severity: "LOW".to_string(),
        confidence: Some("HIGH".to_string()),
        status: SignalStatus::Active as i32,
        first_observed_at: Some(time(1_789_000_000)),
        last_observed_at: Some(time(1_789_000_000)),
        created_at: Some(time(1_789_000_001)),
        execution_mode: ExecutionMode::Test as i32,
        source_count: 1,
        event_count: 1,
        summary: "user=jdoe".to_string(),
        provenance: Some(SignalProvenance {
            evaluated_at: Some(time(1_789_000_001)),
            execution_backend: DetectionExecutionBackend::Clickhouse as i32,
        }),
        inputs: vec![SignalInput {
            signal_id: id.to_string(),
            input_type: SignalInputType::NormalizedEvent as i32,
            input_id: EVENT.to_string(),
            relation: "EVENT_MATCH".to_string(),
            ordinal: 0,
        }],
    }
}
fn finding(id: &str, signal_id: &str) -> Finding {
    Finding {
        finding_id: id.to_string(),
        tenant_id: TENANT.to_string(),
        finding_type: FindingType::Correlation as i32,
        status: FindingStatus::Open as i32,
        severity: "HIGH".to_string(),
        confidence: Some("HIGH".to_string()),
        title: "Account compromise sequence".to_string(),
        description: "Step 30 durable integration fixture".to_string(),
        first_seen: Some(time(1_789_000_000)),
        last_seen: Some(time(1_789_000_100)),
        created_at: Some(time(1_789_000_101)),
        updated_at: Some(time(1_789_000_101)),
        primary_rule_id: None,
        primary_rule_version: None,
        correlation_rule_id: Some("CER-COR-000003".to_string()),
        correlation_rule_version: Some("1".to_string()),
        disposition: FindingDisposition::Undetermined as i32,
        metadata: None,
        execution_mode: ExecutionMode::Test as i32,
        inputs: vec![FindingInput {
            finding_id: id.to_string(),
            input_type: "SIGNAL".to_string(),
            input_id: signal_id.to_string(),
            relation: "SEQUENCE_INPUT".to_string(),
        }],
        correlation_provenance: Some(FindingCorrelationProvenance {
            correlation_rule_id: "CER-COR-000003".to_string(),
            correlation_rule_version: "1".to_string(),
            window_start: Some(time(1_789_000_000)),
            window_end: Some(time(1_789_000_100)),
            input_ids: vec![signal_id.to_string()],
            output_finding_id: id.to_string(),
            execution_backend: DetectionExecutionBackend::Clickhouse as i32,
            execution_mode: ExecutionMode::Test as i32,
            configuration_hash: "step30-config-v1".to_string(),
        }),
    }
}

#[tokio::test]
#[ignore = "requires development PostgreSQL"]
async fn durable_signal_and_finding_are_idempotent() {
    let repo = PostgresAnalyticalRepository::connect(
        &connection_string(),
        "0.1.0-dev".to_string(),
        "step30-integration".to_string(),
    )
    .await
    .unwrap();
    let first_signal = repo.persist_signal(&signal(SIGNAL)).await.unwrap();
    let retry_signal = repo
        .persist_signal(&signal("018f47a2-4b00-7a00-8000-00000000d199"))
        .await
        .unwrap();
    assert_eq!(first_signal.signal_id, retry_signal.signal_id);
    let first_finding = repo
        .persist_finding(&finding(FINDING, &first_signal.signal_id))
        .await
        .unwrap();
    let retry_finding = repo
        .persist_finding(&finding(
            "018f47a2-4b00-7a00-8000-00000000d299",
            &first_signal.signal_id,
        ))
        .await
        .unwrap();
    assert_eq!(first_finding.finding_id, retry_finding.finding_id);
}
