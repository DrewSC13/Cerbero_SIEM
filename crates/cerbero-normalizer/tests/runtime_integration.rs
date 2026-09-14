#![allow(clippy::too_many_lines)]

use std::fs;
use std::time::Duration;

use async_nats::HeaderMap;
use cerbero_common::contracts::sha256_lower_hex;
use cerbero_common::contracts::v1::{
    CerberoEnvelope, ExecutionMode, IntegrityStatus, Producer, RawEventPersisted,
};
use cerbero_normalizer::{
    ANALYTICS_STREAM_NAME, DLQ_STREAM_NAME, EXECUTION_MODE_HEADER, GENERIC_JSON_PARSER_ID,
    NORMALIZATION_DLQ_SUBJECT, NORMALIZED_CREATED_SUBJECT, NORMALIZER_CONSUMER_NAME,
    NORMALIZER_REPLAY_CONSUMER_NAME, NORMALIZER_TEST_CONSUMER_NAME, NormalizationDeadLetter,
    RAW_PERSISTED_SUBJECT, RAW_STREAM_NAME, RuntimeConfig, SourceTimePolicyRegistry,
};
use futures_util::StreamExt;
use prost::Message as _;
use prost_types::{Any, Timestamp};
use serde::Deserialize;
use tempfile::TempDir;
use tokio::sync::watch;
use uuid::Uuid;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires development NATS JetStream and ClickHouse"]
async fn development_sshd_ocsf_runtime_is_duplicate_safe() {
    let root = TempDir::new().expect("temp Raw Store");
    let raw = b"Failed password for invalid user admin from 10.0.0.8 port 50341 ssh2";
    let event_id = Uuid::now_v7().to_string();
    let raw_path = root
        .path()
        .join(format!("tenant-dev/2026/09/13/22/{event_id}/raw.bin"));
    fs::create_dir_all(raw_path.parent().unwrap()).unwrap();
    fs::write(&raw_path, raw).unwrap();
    assert_eq!(fs::read(&raw_path).unwrap(), raw);

    let nats_url = env("CERBERO_NATS_URL");
    isolate_test_streams(&nats_url).await;
    let config = RuntimeConfig {
        nats_url: nats_url.clone(),
        nats_user: env("NATS_NORMALIZER_USER"),
        nats_password: env("NATS_NORMALIZER_PASSWORD"),
        component_version: "0.1.0-integration".to_string(),
        instance_id: format!("normalizer-integration-{}", Uuid::now_v7()),
        pipeline_version: "normalizer-v1-integration".to_string(),
        execution_mode: ExecutionMode::Live,
        linux_sshd_parser_version: "1".to_string(),
        source_time_policies: SourceTimePolicyRegistry::default(),
        raw_store_path: root.path().to_path_buf(),
        clickhouse_url: format!(
            "http://{}:{}",
            env("CLICKHOUSE_HOST"),
            env("CLICKHOUSE_HTTP_PORT")
        ),
        clickhouse_database: env("CLICKHOUSE_DB"),
        clickhouse_user: env("CLICKHOUSE_NORMALIZER_USER"),
        clickhouse_password: env("CLICKHOUSE_NORMALIZER_PASSWORD"),
        retry_min_delay: Duration::from_millis(100),
        retry_max_delay: Duration::from_millis(250),
    }
    .validate()
    .unwrap();

    let detection = async_nats::ConnectOptions::with_user_and_password(
        env("NATS_DETECTION_USER"),
        env("NATS_DETECTION_PASSWORD"),
    )
    .name("normalizer-integration-observer")
    .connect(&nats_url)
    .await
    .unwrap();
    let mut normalized = detection
        .subscribe(NORMALIZED_CREATED_SUBJECT)
        .await
        .unwrap();
    detection.flush().await.unwrap();

    let admin_observer = async_nats::ConnectOptions::with_user_and_password(
        env("NATS_ADMIN_USER"),
        env("NATS_ADMIN_PASSWORD"),
    )
    .name("normalizer-integration-dlq-observer")
    .connect(&nats_url)
    .await
    .unwrap();
    let mut dead_letters = admin_observer
        .subscribe(NORMALIZATION_DLQ_SUBJECT)
        .await
        .unwrap();
    admin_observer.flush().await.unwrap();

    let raw_preserver = async_nats::ConnectOptions::with_user_and_password(
        env("NATS_RAW_PRESERVER_USER"),
        env("NATS_RAW_PRESERVER_PASSWORD"),
    )
    .name("normalizer-integration-producer")
    .connect(&nats_url)
    .await
    .unwrap();
    let raw_js = async_nats::jetstream::new(raw_preserver);

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let runtime = tokio::spawn(cerbero_normalizer::run(config.clone(), shutdown_rx));
    tokio::time::sleep(Duration::from_millis(250)).await;

    let envelope = raw_persisted_envelope(&event_id, raw);
    let wire = envelope.encode_to_vec();
    publish_raw_persisted(&raw_js, &wire, Uuid::now_v7().to_string()).await;

    let first = tokio::time::timeout(Duration::from_secs(5), normalized.next())
        .await
        .expect("normalized.created timeout")
        .expect("normalized.created subscription ended");
    let first_envelope = CerberoEnvelope::decode(first.payload.as_ref()).unwrap();
    assert_eq!(first_envelope.message_type, "NormalizedEventCreated");
    assert_eq!(first_envelope.causation_id, envelope.message_id);

    let second_raw_sequence =
        publish_raw_persisted(&raw_js, &wire, Uuid::now_v7().to_string()).await;
    wait_for_normalizer_ack(&nats_url, second_raw_sequence).await;

    let count = clickhouse_count(&config, &event_id).await;
    assert_eq!(
        count, 1,
        "duplicate logical input created multiple normalized rows"
    );

    let normalized_count = normalized_subject_count(&nats_url).await;
    assert_eq!(
        normalized_count, 1,
        "duplicate logical input created multiple stored normalized.created messages"
    );

    let malformed_event_id = Uuid::now_v7().to_string();
    let malformed_raw = br#"{"event":"login","source":"integration"}"#;
    let malformed_path = root.path().join(format!(
        "tenant-dev/2026/09/13/22/{malformed_event_id}/raw.bin"
    ));
    fs::create_dir_all(malformed_path.parent().unwrap()).unwrap();
    fs::write(&malformed_path, malformed_raw).unwrap();

    let malformed_envelope = raw_persisted_envelope_with(
        &malformed_event_id,
        malformed_raw,
        "application/json",
        "integration-json",
    );
    let malformed_wire = malformed_envelope.encode_to_vec();
    let malformed_sequence =
        publish_raw_persisted(&raw_js, &malformed_wire, Uuid::now_v7().to_string()).await;

    let dead_letter_message = tokio::time::timeout(Duration::from_secs(5), dead_letters.next())
        .await
        .expect("normalization DLQ timeout")
        .expect("normalization DLQ subscription ended");
    let dead_letter: NormalizationDeadLetter =
        serde_json::from_slice(dead_letter_message.payload.as_ref()).unwrap();
    assert_eq!(dead_letter.schema_version, "cerbero.normalization_dlq.v1");
    assert_eq!(
        dead_letter.original_message_id.as_deref(),
        Some(malformed_envelope.message_id.as_str())
    );
    assert_eq!(
        dead_letter.raw_event_id.as_deref(),
        Some(malformed_event_id.as_str())
    );
    assert_eq!(
        dead_letter.parser_id.as_deref(),
        Some(GENERIC_JSON_PARSER_ID)
    );
    assert_eq!(dead_letter.parser_version.as_deref(), Some("1"));
    assert_eq!(dead_letter.error_code, "CER-NORM-MAPPING-UNSUPPORTED");
    assert_eq!(dead_letter.failure_stage, "mapping");
    assert!(!dead_letter.retryable);
    wait_for_normalizer_ack(&nats_url, malformed_sequence).await;
    assert_eq!(clickhouse_count(&config, &malformed_event_id).await, 0);

    let duplicate_malformed_sequence =
        publish_raw_persisted(&raw_js, &malformed_wire, Uuid::now_v7().to_string()).await;
    wait_for_normalizer_ack(&nats_url, duplicate_malformed_sequence).await;
    assert_eq!(
        dlq_subject_count(&nats_url).await,
        1,
        "duplicate logical dead letter created multiple stored DLQ records"
    );

    shutdown_tx.send(true).unwrap();
    let runtime_result = tokio::time::timeout(Duration::from_secs(5), runtime)
        .await
        .expect("runtime shutdown timeout")
        .expect("runtime task join failed");
    runtime_result.expect("normalizer runtime failed");
}

#[derive(Clone, Debug, Deserialize)]
struct HistoryRow {
    logical_key: String,
    normalized_event_id: String,
    parser_version: String,
    execution_mode: i32,
    normalized_hash: String,
    configuration_hash: String,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires development NATS JetStream and ClickHouse"]
async fn development_execution_modes_preserve_historical_renormalization() {
    let root = TempDir::new().expect("temp Raw Store");
    let raw = b"Failed password for invalid user admin from 10.0.0.8 port 50341 ssh2";
    let event_id = Uuid::now_v7().to_string();
    let raw_path = root
        .path()
        .join(format!("tenant-dev/2026/09/13/22/{event_id}/raw.bin"));
    fs::create_dir_all(raw_path.parent().unwrap()).unwrap();
    fs::write(&raw_path, raw).unwrap();

    let nats_url = env("CERBERO_NATS_URL");
    isolate_test_streams(&nats_url).await;

    let raw_preserver = async_nats::ConnectOptions::with_user_and_password(
        env("NATS_RAW_PRESERVER_USER"),
        env("NATS_RAW_PRESERVER_PASSWORD"),
    )
    .name("normalizer-history-producer")
    .connect(&nats_url)
    .await
    .unwrap();
    let raw_js = async_nats::jetstream::new(raw_preserver);

    let detection = async_nats::ConnectOptions::with_user_and_password(
        env("NATS_DETECTION_USER"),
        env("NATS_DETECTION_PASSWORD"),
    )
    .name("normalizer-history-observer")
    .connect(&nats_url)
    .await
    .unwrap();
    let mut normalized = detection
        .subscribe(NORMALIZED_CREATED_SUBJECT)
        .await
        .unwrap();
    detection.flush().await.unwrap();

    let envelope = raw_persisted_envelope(&event_id, raw);
    let wire = envelope.encode_to_vec();

    let live_config = mode_config(
        root.path(),
        &nats_url,
        ExecutionMode::Live,
        "1",
        "history-live",
    );
    let (live_shutdown_tx, live_shutdown_rx) = watch::channel(false);
    let live_runtime = tokio::spawn(cerbero_normalizer::run(
        live_config.clone(),
        live_shutdown_rx,
    ));
    tokio::time::sleep(Duration::from_millis(250)).await;
    let live_sequence = publish_raw_persisted(&raw_js, &wire, Uuid::now_v7().to_string()).await;
    let live_message = next_normalized(&mut normalized).await;
    assert_execution_header(&live_message, "LIVE");
    wait_for_consumer_ack(&nats_url, NORMALIZER_CONSUMER_NAME, live_sequence).await;
    stop_runtime(live_shutdown_tx, live_runtime).await;

    let live_rows = clickhouse_history(&live_config, &event_id).await;
    assert_eq!(live_rows.len(), 1);
    let n1 = live_rows[0].clone();
    assert_eq!(n1.parser_version, "1");
    assert_eq!(n1.execution_mode, ExecutionMode::Live as i32);

    let replay_config = mode_config(
        root.path(),
        &nats_url,
        ExecutionMode::Replay,
        "2",
        "history-replay-v2",
    );
    let (replay_shutdown_tx, replay_shutdown_rx) = watch::channel(false);
    let replay_runtime = tokio::spawn(cerbero_normalizer::run(
        replay_config.clone(),
        replay_shutdown_rx,
    ));
    let replay_message = next_normalized(&mut normalized).await;
    assert_execution_header(&replay_message, "REPLAY");
    stop_runtime(replay_shutdown_tx, replay_runtime).await;

    let replay_rows = clickhouse_history(&replay_config, &event_id).await;
    assert_eq!(replay_rows.len(), 2);
    let n1_after_replay = replay_rows
        .iter()
        .find(|row| row.normalized_event_id == n1.normalized_event_id)
        .expect("N1 must remain after parser-v2 replay");
    assert_eq!(n1_after_replay.logical_key, n1.logical_key);
    assert_eq!(n1_after_replay.normalized_hash, n1.normalized_hash);
    assert_eq!(n1_after_replay.configuration_hash, n1.configuration_hash);
    let n2 = replay_rows
        .iter()
        .find(|row| row.execution_mode == ExecutionMode::Replay as i32)
        .expect("REPLAY N2 row");
    assert_eq!(n2.parser_version, "2");
    assert_ne!(n2.logical_key, n1.logical_key);
    assert_ne!(n2.normalized_event_id, n1.normalized_event_id);
    assert_ne!(n2.normalized_hash, n1.normalized_hash);

    let test_config = mode_config(
        root.path(),
        &nats_url,
        ExecutionMode::Test,
        "1",
        "history-test-v1",
    );
    let (test_shutdown_tx, test_shutdown_rx) = watch::channel(false);
    let test_runtime = tokio::spawn(cerbero_normalizer::run(
        test_config.clone(),
        test_shutdown_rx,
    ));
    let test_message = next_normalized(&mut normalized).await;
    assert_execution_header(&test_message, "TEST");
    stop_runtime(test_shutdown_tx, test_runtime).await;

    let final_rows = clickhouse_history(&test_config, &event_id).await;
    assert_eq!(final_rows.len(), 3);
    assert!(final_rows.iter().any(|row| {
        row.normalized_event_id == n1.normalized_event_id
            && row.normalized_hash == n1.normalized_hash
            && row.execution_mode == ExecutionMode::Live as i32
    }));
    assert!(final_rows.iter().any(|row| {
        row.normalized_event_id == n2.normalized_event_id
            && row.execution_mode == ExecutionMode::Replay as i32
            && row.parser_version == "2"
    }));
    let test_row = final_rows
        .iter()
        .find(|row| row.execution_mode == ExecutionMode::Test as i32)
        .expect("TEST historical row");
    assert_eq!(test_row.parser_version, "1");
    assert_ne!(test_row.logical_key, n1.logical_key);

    assert!(consumer_exists(&nats_url, NORMALIZER_CONSUMER_NAME).await);
    assert!(!consumer_exists(&nats_url, NORMALIZER_REPLAY_CONSUMER_NAME).await);
    assert!(!consumer_exists(&nats_url, NORMALIZER_TEST_CONSUMER_NAME).await);
}

fn mode_config(
    raw_store_path: &std::path::Path,
    nats_url: &str,
    execution_mode: ExecutionMode,
    parser_version: &str,
    suffix: &str,
) -> RuntimeConfig {
    RuntimeConfig {
        nats_url: nats_url.to_string(),
        nats_user: env("NATS_NORMALIZER_USER"),
        nats_password: env("NATS_NORMALIZER_PASSWORD"),
        component_version: "0.1.0-integration".to_string(),
        instance_id: format!("normalizer-{suffix}-{}", Uuid::now_v7()),
        pipeline_version: "normalizer-v1-integration".to_string(),
        execution_mode,
        linux_sshd_parser_version: parser_version.to_string(),
        source_time_policies: SourceTimePolicyRegistry::default(),
        raw_store_path: raw_store_path.to_path_buf(),
        clickhouse_url: format!(
            "http://{}:{}",
            env("CLICKHOUSE_HOST"),
            env("CLICKHOUSE_HTTP_PORT")
        ),
        clickhouse_database: env("CLICKHOUSE_DB"),
        clickhouse_user: env("CLICKHOUSE_NORMALIZER_USER"),
        clickhouse_password: env("CLICKHOUSE_NORMALIZER_PASSWORD"),
        retry_min_delay: Duration::from_millis(100),
        retry_max_delay: Duration::from_millis(250),
    }
    .validate()
    .unwrap()
}

async fn next_normalized(subscription: &mut async_nats::Subscriber) -> async_nats::Message {
    tokio::time::timeout(Duration::from_secs(5), subscription.next())
        .await
        .expect("normalized.created timeout")
        .expect("normalized.created subscription ended")
}

fn assert_execution_header(message: &async_nats::Message, expected: &str) {
    let actual = message
        .headers
        .as_ref()
        .and_then(|headers| headers.get(EXECUTION_MODE_HEADER))
        .map(async_nats::HeaderValue::as_str);
    assert_eq!(actual, Some(expected));
}

async fn stop_runtime(
    shutdown_tx: watch::Sender<bool>,
    runtime: tokio::task::JoinHandle<Result<(), cerbero_normalizer::NormalizerError>>,
) {
    shutdown_tx.send(true).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), runtime)
        .await
        .expect("runtime shutdown timeout")
        .expect("runtime task join failed");
    result.expect("normalizer runtime failed");
}

async fn consumer_exists(nats_url: &str, consumer_name: &str) -> bool {
    let jetstream = admin_jetstream(nats_url, "normalizer-consumer-existence").await;
    let stream = jetstream.get_stream(RAW_STREAM_NAME).await.unwrap();
    let result: Result<async_nats::jetstream::consumer::PullConsumer, _> =
        stream.get_consumer(consumer_name).await;
    result.is_ok()
}

async fn clickhouse_history(config: &RuntimeConfig, raw_event_id: &str) -> Vec<HistoryRow> {
    let query = format!(
        "SELECT logical_key, normalized_event_id, parser_version, execution_mode, normalized_hash, configuration_hash \
         FROM {}.normalized_events WHERE raw_event_id = '{}' ORDER BY created_at, normalized_event_id FORMAT JSONEachRow",
        config.clickhouse_database, raw_event_id
    );
    let response = reqwest::Client::new()
        .get(&config.clickhouse_url)
        .basic_auth(&config.clickhouse_user, Some(&config.clickhouse_password))
        .query(&[("query", query)])
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    response
        .text()
        .await
        .unwrap()
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

async fn admin_jetstream(nats_url: &str, name: &str) -> async_nats::jetstream::Context {
    let admin = async_nats::ConnectOptions::with_user_and_password(
        env("NATS_ADMIN_USER"),
        env("NATS_ADMIN_PASSWORD"),
    )
    .name(name.to_string())
    .connect(nats_url)
    .await
    .unwrap();
    async_nats::jetstream::new(admin)
}

async fn isolate_test_streams(nats_url: &str) {
    let jetstream = admin_jetstream(nats_url, "normalizer-integration-isolation").await;

    let raw_stream = jetstream.get_stream(RAW_STREAM_NAME).await.unwrap();
    raw_stream
        .purge()
        .filter(RAW_PERSISTED_SUBJECT)
        .await
        .unwrap();

    let _ = raw_stream
        .delete_consumer(NORMALIZER_REPLAY_CONSUMER_NAME)
        .await;
    let _ = raw_stream
        .delete_consumer(NORMALIZER_TEST_CONSUMER_NAME)
        .await;

    let analytics_stream = jetstream.get_stream(ANALYTICS_STREAM_NAME).await.unwrap();
    analytics_stream
        .purge()
        .filter(NORMALIZED_CREATED_SUBJECT)
        .await
        .unwrap();

    let dlq_stream = jetstream.get_stream(DLQ_STREAM_NAME).await.unwrap();
    dlq_stream
        .purge()
        .filter(NORMALIZATION_DLQ_SUBJECT)
        .await
        .unwrap();
}

async fn wait_for_normalizer_ack(nats_url: &str, raw_sequence: u64) {
    wait_for_consumer_ack(nats_url, NORMALIZER_CONSUMER_NAME, raw_sequence).await;
}

async fn wait_for_consumer_ack(nats_url: &str, consumer_name: &str, raw_sequence: u64) {
    let jetstream = admin_jetstream(nats_url, "normalizer-integration-ack-observer").await;
    let raw_stream = jetstream.get_stream(RAW_STREAM_NAME).await.unwrap();
    let consumer: async_nats::jetstream::consumer::PullConsumer =
        raw_stream.get_consumer(consumer_name).await.unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let info = consumer.get_info().await.unwrap();
            if info.ack_floor.stream_sequence >= raw_sequence {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("normalizer did not ACK raw.persisted");
}

async fn normalized_subject_count(nats_url: &str) -> usize {
    let jetstream = admin_jetstream(nats_url, "normalizer-integration-stream-observer").await;
    let analytics_stream = jetstream.get_stream(ANALYTICS_STREAM_NAME).await.unwrap();
    let mut subjects = analytics_stream
        .info_with_subjects(NORMALIZED_CREATED_SUBJECT)
        .await
        .unwrap();
    let mut count = 0_usize;

    while let Some(entry) = subjects.next().await {
        let (subject, messages) = entry.unwrap();
        if subject == NORMALIZED_CREATED_SUBJECT {
            count += messages;
        }
    }

    count
}

async fn dlq_subject_count(nats_url: &str) -> usize {
    let jetstream = admin_jetstream(nats_url, "normalizer-integration-dlq-stream-observer").await;
    let dlq_stream = jetstream.get_stream(DLQ_STREAM_NAME).await.unwrap();
    let mut subjects = dlq_stream
        .info_with_subjects(NORMALIZATION_DLQ_SUBJECT)
        .await
        .unwrap();
    let mut count = 0_usize;

    while let Some(entry) = subjects.next().await {
        let (subject, messages) = entry.unwrap();
        if subject == NORMALIZATION_DLQ_SUBJECT {
            count += messages;
        }
    }

    count
}

async fn publish_raw_persisted(
    js: &async_nats::jetstream::Context,
    wire: &[u8],
    transport_message_id: String,
) -> u64 {
    let mut headers = HeaderMap::new();
    headers.insert("Nats-Msg-Id", transport_message_id);
    headers.insert("Cerbero-Request-Id", Uuid::now_v7().to_string());
    let ack = js
        .publish_with_headers(RAW_PERSISTED_SUBJECT, headers, wire.to_vec().into())
        .await
        .unwrap()
        .await
        .unwrap();
    assert_eq!(ack.stream, RAW_STREAM_NAME);
    assert!(ack.sequence > 0);
    ack.sequence
}

fn raw_persisted_envelope(event_id: &str, raw: &[u8]) -> CerberoEnvelope {
    raw_persisted_envelope_with(event_id, raw, "text/plain", "integration-sshd")
}

fn raw_persisted_envelope_with(
    event_id: &str,
    raw: &[u8],
    content_type: &str,
    source_id: &str,
) -> CerberoEnvelope {
    let ingest = Timestamp {
        seconds: 1_789_315_200,
        nanos: 0,
    };
    let persisted = RawEventPersisted {
        event_id: event_id.to_string(),
        tenant_id: "tenant-dev".to_string(),
        source_id: source_id.to_string(),
        sensor_id: "integration-sensor".to_string(),
        event_time: Some(ingest),
        ingest_time: Some(ingest),
        content_type: content_type.to_string(),
        encoding: "utf-8".to_string(),
        raw_size: u64::try_from(raw.len()).expect("integration raw length fits u64"),
        raw_hash_algorithm: "sha256".to_string(),
        raw_hash: sha256_lower_hex(raw),
        transport: "integration".to_string(),
        remote_identity: "integration".to_string(),
        sequence_number: None,
        integrity_status: IntegrityStatus::IntegrityValid as i32,
        pipeline_version: "ingest-v1-integration".to_string(),
        storage_uri: format!("raw:///tenant-dev/2026/09/13/22/{event_id}/raw.bin"),
        segment_id: event_id.to_string(),
        offset: 0,
        length: u64::try_from(raw.len()).expect("integration raw length fits u64"),
        persisted_at: Some(ingest),
    };
    CerberoEnvelope {
        contract_version: "1".to_string(),
        message_id: Uuid::now_v7().to_string(),
        message_type: "RawEventPersisted".to_string(),
        tenant_id: "tenant-dev".to_string(),
        producer: Some(Producer {
            component: "integration-raw-preserver".to_string(),
            component_version: "test".to_string(),
            instance_id: "integration".to_string(),
        }),
        emitted_at: Some(ingest),
        trace_id: Uuid::now_v7().to_string(),
        causation_id: Uuid::now_v7().to_string(),
        correlation_id: String::new(),
        payload_schema: "cerbero.raw_event_persisted.v1".to_string(),
        payload: Some(Any {
            type_url: "type.googleapis.com/cerbero.contracts.v1.RawEventPersisted".to_string(),
            value: persisted.encode_to_vec(),
        }),
    }
}

async fn clickhouse_count(config: &RuntimeConfig, raw_event_id: &str) -> u64 {
    let query = format!(
        "SELECT count() FROM {}.normalized_events WHERE raw_event_id = '{}' FORMAT TabSeparatedRaw",
        config.clickhouse_database, raw_event_id
    );
    let response = reqwest::Client::new()
        .get(&config.clickhouse_url)
        .basic_auth(&config.clickhouse_user, Some(&config.clickhouse_password))
        .query(&[("query", query)])
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    response
        .text()
        .await
        .unwrap()
        .trim()
        .parse::<u64>()
        .unwrap()
}

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} is required for integration test"))
}
