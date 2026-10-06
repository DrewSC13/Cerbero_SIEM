use cerbero_common::contracts::v1::ExecutionMode;
use cerbero_detection_runtime::mvp_batch::{MvpBatchRequest, MvpBatchRuntime, MvpClickHouseConfig};
use prost_types::Timestamp;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the Step33 composed PostgreSQL/ClickHouse source pipeline"]
async fn step33_source_to_tui_threshold_is_durable() {
    let tenant_id = env("STEP33_E2E_TENANT_ID");
    let start_millis = env("STEP33_E2E_START_MILLIS").parse::<i64>().unwrap();
    let end_millis = env("STEP33_E2E_END_MILLIS").parse::<i64>().unwrap();

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
        &format!(
            "host={} port={} dbname={} user={} password={}",
            env("POSTGRES_HOST"),
            env("POSTGRES_PORT"),
            env("POSTGRES_DB"),
            env("POSTGRES_DETECTION_USER"),
            env("POSTGRES_DETECTION_PASSWORD"),
        ),
        "step33-e2e".to_string(),
        "step33-e2e-detection".to_string(),
    )
    .await
    .unwrap();

    let evaluated_millis = end_millis + 60_000;
    let result = runtime
        .execute(&MvpBatchRequest {
            tenant_id: tenant_id.clone(),
            execution_mode: ExecutionMode::Live,
            start_millis,
            end_millis,
            max_events: 100,
            evaluated_at: timestamp(evaluated_millis),
            created_at: timestamp(evaluated_millis + 1_000),
        })
        .await
        .unwrap();

    assert_eq!(result.events_loaded, 10);
    assert!(result.findings.is_empty());

    let threshold = result
        .signals
        .iter()
        .filter(|signal| signal.rule_id == "CER-DET-000001")
        .collect::<Vec<_>>();
    assert_eq!(threshold.len(), 1);
    let threshold = threshold[0];
    assert_eq!(threshold.rule_version, "1");
    assert_eq!(threshold.event_count, 10);
    assert_eq!(threshold.inputs.len(), 10);
    assert_eq!(threshold.execution_mode, ExecutionMode::Live as i32);

    println!(
        "STEP33_DETECTION_PASS tenant={} signal={} matched_events={}",
        tenant_id, threshold.signal_id, threshold.event_count
    );
}

fn timestamp(millis: i64) -> Timestamp {
    Timestamp {
        seconds: millis.div_euclid(1_000),
        nanos: i32::try_from(millis.rem_euclid(1_000) * 1_000_000).unwrap(),
    }
}

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} is required"))
}
