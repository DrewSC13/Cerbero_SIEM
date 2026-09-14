#![allow(clippy::too_many_lines)]

use std::fs;
use std::time::Duration;

use async_nats::HeaderMap;
use cerbero_common::contracts::sha256_lower_hex;
use cerbero_common::contracts::v1::{
    CerberoEnvelope, ExecutionMode, IntegrityStatus, Producer, RawEventPersisted,
};
use cerbero_normalizer::{
    ANALYTICS_STREAM_NAME, NORMALIZED_CREATED_SUBJECT, NORMALIZER_CONSUMER_NAME,
    RAW_PERSISTED_SUBJECT, RAW_STREAM_NAME, RuntimeConfig,
};
use futures_util::StreamExt;
use prost::Message as _;
use prost_types::{Any, Timestamp};
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

    shutdown_tx.send(true).unwrap();
    let runtime_result = tokio::time::timeout(Duration::from_secs(5), runtime)
        .await
        .expect("runtime shutdown timeout")
        .expect("runtime task join failed");
    runtime_result.expect("normalizer runtime failed");
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

    let analytics_stream = jetstream.get_stream(ANALYTICS_STREAM_NAME).await.unwrap();
    analytics_stream
        .purge()
        .filter(NORMALIZED_CREATED_SUBJECT)
        .await
        .unwrap();
}

async fn wait_for_normalizer_ack(nats_url: &str, raw_sequence: u64) {
    let jetstream = admin_jetstream(nats_url, "normalizer-integration-ack-observer").await;
    let raw_stream = jetstream.get_stream(RAW_STREAM_NAME).await.unwrap();
    let consumer: async_nats::jetstream::consumer::PullConsumer = raw_stream
        .get_consumer(NORMALIZER_CONSUMER_NAME)
        .await
        .unwrap();

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
    .expect("normalizer did not ACK duplicate raw.persisted");
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
    let ingest = Timestamp {
        seconds: 1_789_315_200,
        nanos: 0,
    };
    let persisted = RawEventPersisted {
        event_id: event_id.to_string(),
        tenant_id: "tenant-dev".to_string(),
        source_id: "integration-sshd".to_string(),
        sensor_id: "integration-sensor".to_string(),
        event_time: Some(ingest),
        ingest_time: Some(ingest),
        content_type: "text/plain".to_string(),
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
