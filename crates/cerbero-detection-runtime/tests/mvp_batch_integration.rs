#![allow(clippy::too_many_lines)]

use std::collections::{BTreeSet, HashMap};

use cerbero_common::{
    contracts::sha256_lower_hex,
    contracts::v1::{ExecutionMode, SignalRuleType},
};
use cerbero_detection_runtime::mvp_batch::{MvpBatchRequest, MvpBatchRuntime, MvpClickHouseConfig};
use prost_types::Timestamp;
use reqwest::Client;
use serde_json::{Value, json};
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires development ClickHouse and PostgreSQL"]
async fn bounded_rulepack_is_durable_idempotent_and_mode_isolated() {
    let tenant_id = Uuid::now_v7().to_string();
    let negative_user = format!("negative-{}", Uuid::now_v7());
    let base_millis = 1_789_000_000_000_i64;

    let mut rows = Vec::new();
    let mut positive_failed_ids = BTreeSet::new();

    for index in 0..10_i64 {
        let row = normalized_row(
            &tenant_id,
            "alice",
            "10.0.0.8",
            "Logon",
            "Failure",
            base_millis + index * 20_000,
            ExecutionMode::Replay,
        );
        positive_failed_ids.insert(row["normalized_event_id"].as_str().unwrap().to_string());
        rows.push(row);
    }

    rows.push(rows[0].clone());

    for index in 0..9_i64 {
        rows.push(normalized_row(
            &tenant_id,
            &negative_user,
            "10.0.0.9",
            "Logon",
            "Failure",
            base_millis + index * 20_000,
            ExecutionMode::Replay,
        ));
    }

    rows.push(normalized_row(
        &tenant_id,
        "alice",
        "10.0.0.8",
        "Logon",
        "Success",
        base_millis + 220_000,
        ExecutionMode::Replay,
    ));
    rows.push(normalized_row(
        &tenant_id,
        "alice",
        "10.0.0.8",
        "Privilege Escalation",
        "Success",
        base_millis + 260_000,
        ExecutionMode::Replay,
    ));

    rows.push(normalized_row(
        &tenant_id,
        "alice",
        "10.0.0.8",
        "Logon",
        "Failure",
        base_millis + 10_000,
        ExecutionMode::Live,
    ));

    insert_rows(&rows).await;

    let signal_outbox_before = outbox_count("cerbero.v1.signal.created").await;
    let finding_outbox_before = outbox_count("cerbero.v1.finding.created").await;

    let runtime = MvpBatchRuntime::connect(
        MvpClickHouseConfig {
            endpoint: format!(
                "http://{}:{}",
                env("CLICKHOUSE_HOST"),
                env("CLICKHOUSE_HTTP_PORT")
            ),
            database: env("CLICKHOUSE_DB"),
            user: env("CLICKHOUSE_DETECTION_USER"),
            password: env("CLICKHOUSE_DETECTION_PASSWORD"),
        },
        &postgres_connection_string(),
        "0.1.0-dev".to_string(),
        "step30-mvp-batch-integration".to_string(),
    )
    .await
    .unwrap();

    let request = MvpBatchRequest {
        tenant_id,
        execution_mode: ExecutionMode::Replay,
        start_millis: base_millis - 1_000,
        end_millis: base_millis + 600_000,
        max_events: 100,
        evaluated_at: timestamp_from_millis(base_millis + 500_000),
        created_at: timestamp_from_millis(base_millis + 510_000),
    };

    let first = runtime.execute(&request).await.unwrap();

    assert_eq!(first.events_loaded, 21);
    assert_eq!(first.signals.len(), 22);
    assert_eq!(first.findings.len(), 1);

    let counts = first
        .signals
        .iter()
        .fold(HashMap::<&str, usize>::new(), |mut counts, signal| {
            *counts.entry(signal.rule_id.as_str()).or_default() += 1;
            counts
        });
    assert_eq!(counts.get("CER-DET-000101"), Some(&19));
    assert_eq!(counts.get("CER-DET-000102"), Some(&1));
    assert_eq!(counts.get("CER-DET-000103"), Some(&1));
    assert_eq!(counts.get("CER-DET-000001"), Some(&1));

    let threshold = first
        .signals
        .iter()
        .find(|signal| signal.rule_id == "CER-DET-000001")
        .unwrap();
    assert_eq!(threshold.rule_type, SignalRuleType::Threshold as i32);
    assert_eq!(threshold.event_count, 10);
    assert_eq!(threshold.inputs.len(), 10);
    let threshold_inputs = threshold
        .inputs
        .iter()
        .map(|input| input.input_id.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(threshold_inputs, positive_failed_ids);

    let finding = &first.findings[0];
    let provenance = finding.correlation_provenance.as_ref().unwrap();
    assert_eq!(provenance.correlation_rule_id, "CER-COR-0003");
    assert_eq!(provenance.correlation_rule_version, "2");
    assert_eq!(provenance.input_ids.len(), 3);

    let rules_by_signal = first
        .signals
        .iter()
        .map(|signal| (signal.signal_id.as_str(), signal.rule_id.as_str()))
        .collect::<HashMap<_, _>>();
    let sequence_rule_ids = provenance
        .input_ids
        .iter()
        .map(|signal_id| *rules_by_signal.get(signal_id.as_str()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        sequence_rule_ids,
        vec!["CER-DET-000101", "CER-DET-000102", "CER-DET-000103"]
    );

    assert_eq!(
        outbox_count("cerbero.v1.signal.created").await - signal_outbox_before,
        22
    );
    assert_eq!(
        outbox_count("cerbero.v1.finding.created").await - finding_outbox_before,
        1
    );

    let retry = runtime.execute(&request).await.unwrap();
    assert_eq!(retry.events_loaded, first.events_loaded);

    let first_signal_ids = first
        .signals
        .iter()
        .map(|signal| signal.signal_id.clone())
        .collect::<BTreeSet<_>>();
    let retry_signal_ids = retry
        .signals
        .iter()
        .map(|signal| signal.signal_id.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(retry_signal_ids, first_signal_ids);

    let first_finding_ids = first
        .findings
        .iter()
        .map(|finding| finding.finding_id.clone())
        .collect::<BTreeSet<_>>();
    let retry_finding_ids = retry
        .findings
        .iter()
        .map(|finding| finding.finding_id.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(retry_finding_ids, first_finding_ids);

    assert_eq!(
        outbox_count("cerbero.v1.signal.created").await - signal_outbox_before,
        22
    );
    assert_eq!(
        outbox_count("cerbero.v1.finding.created").await - finding_outbox_before,
        1
    );
}

fn normalized_row(
    tenant_id: &str,
    user: &str,
    source_ip: &str,
    activity_name: &str,
    status: &str,
    event_millis: i64,
    execution_mode: ExecutionMode,
) -> Value {
    let normalized_event_id = Uuid::now_v7().to_string();
    let raw_event_id = Uuid::now_v7().to_string();
    let ocsf_event = json!({
        "activity_name": activity_name,
        "status": status,
        "user": {"name": user},
        "src_endpoint": {"ip": source_ip},
        "time": event_millis,
    });
    let canonical = serde_json::to_string(&ocsf_event).unwrap();

    json!({
        "logical_key": sha256_lower_hex(format!("logical:{normalized_event_id}").as_bytes()),
        "tenant_id": tenant_id,
        "normalized_event_id": normalized_event_id,
        "raw_event_id": raw_event_id,
        "event_time_present": 1,
        "event_time_seconds": event_millis.div_euclid(1000),
        "event_time_nanos": event_millis.rem_euclid(1000) * 1_000_000,
        "ingest_time_seconds": event_millis.div_euclid(1000),
        "ingest_time_nanos": event_millis.rem_euclid(1000) * 1_000_000,
        "normalized_at_seconds": event_millis.div_euclid(1000) + 1,
        "normalized_at_nanos": 0,
        "ocsf_version": "1.9.0",
        "class_uid": if activity_name == "Privilege Escalation" { 6001 } else { 3002 },
        "category_uid": if activity_name == "Privilege Escalation" { 6 } else { 3 },
        "activity_id": 1,
        "severity": 2,
        "parser_id": "integration/canonical",
        "parser_version": "1",
        "mapping_id": "integration.canonical",
        "mapping_version": "1",
        "normalization_status": 1,
        "normalized_hash_algorithm": "sha256",
        "normalized_hash": sha256_lower_hex(canonical.as_bytes()),
        "pipeline_version": "step30-mvp-batch-integration",
        "ocsf_event_json": canonical,
        "transformation_id": Uuid::now_v7().to_string(),
        "configuration_hash": sha256_lower_hex(b"step30-mvp-batch-integration"),
        "execution_mode": execution_mode as i32,
        "publication_message_id": Uuid::now_v7().to_string(),
        "causation_message_id": Uuid::now_v7().to_string(),
        "trace_id": Uuid::now_v7().to_string(),
        "correlation_id": Uuid::now_v7().to_string(),
        "producer_component_version": "integration",
        "producer_instance_id": "step30-mvp-batch",
    })
}

async fn insert_rows(rows: &[Value]) {
    let body = rows
        .iter()
        .map(Value::to_string)
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    let query = format!(
        "INSERT INTO {}.normalized_events FORMAT JSONEachRow",
        env("CLICKHOUSE_DB")
    );
    let response = Client::new()
        .post(format!(
            "http://{}:{}",
            env("CLICKHOUSE_HOST"),
            env("CLICKHOUSE_HTTP_PORT")
        ))
        .basic_auth(env("CLICKHOUSE_USER"), Some(env("CLICKHOUSE_PASSWORD")))
        .query(&[("query", query)])
        .body(body)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let response_body = response.text().await.unwrap();
    assert!(
        status.is_success(),
        "ClickHouse seed failed HTTP {}: {}",
        status.as_u16(),
        response_body
    );
}

async fn outbox_count(subject: &str) -> i64 {
    let (client, connection) = tokio_postgres::connect(&postgres_connection_string(), NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    client
        .query_one(
            "SELECT count(*)::bigint FROM system.outbox WHERE subject = $1",
            &[&subject],
        )
        .await
        .unwrap()
        .get(0)
}

fn postgres_connection_string() -> String {
    format!(
        "host={} port={} dbname={} user={} password={}",
        env("POSTGRES_HOST"),
        env("POSTGRES_PORT"),
        env("POSTGRES_DB"),
        env("POSTGRES_DETECTION_USER"),
        env("POSTGRES_DETECTION_PASSWORD"),
    )
}

fn timestamp_from_millis(value: i64) -> Timestamp {
    Timestamp {
        seconds: value.div_euclid(1_000),
        nanos: i32::try_from(value.rem_euclid(1_000) * 1_000_000).unwrap(),
    }
}

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} is required"))
}
