use std::collections::BTreeMap;
use std::sync::Arc;

use async_nats::jetstream::AckKind;
use cerbero_common::contracts::sha256_lower_hex;
use futures_util::StreamExt;
use tokio::sync::watch;

use crate::clickhouse::{ClickHouseStore, StoredNormalization};
use crate::dead_letter::{NormalizationDeadLetter, unix_time_millis};
use crate::eventbus::{EventBus, decode_raw_persisted, retry_delay_with_jitter};
use crate::metrics::{MemoryNormalizerMetrics, NormalizerMetrics};
use crate::raw_store::FilesystemRawReader;
use crate::runtime_config::RuntimeConfig;
use crate::system_id::{SystemIdGenerator, new_uuid_v7};
use crate::{NormalizerCore, NormalizerCoreConfig, NormalizerError, SystemClock};

pub async fn run(
    config: RuntimeConfig,
    shutdown: watch::Receiver<bool>,
) -> Result<(), NormalizerError> {
    let metrics: Arc<dyn NormalizerMetrics> = Arc::new(MemoryNormalizerMetrics::default());
    run_with_metrics(config, shutdown, metrics).await
}

pub async fn run_with_metrics(
    config: RuntimeConfig,
    mut shutdown: watch::Receiver<bool>,
    metrics: Arc<dyn NormalizerMetrics>,
) -> Result<(), NormalizerError> {
    let raw_reader = FilesystemRawReader::new(&config.raw_store_path)?;
    let store = ClickHouseStore::new(
        config.clickhouse_url.clone(),
        config.clickhouse_database.clone(),
        config.clickhouse_user.clone(),
        config.clickhouse_password.clone(),
    );
    let bus = EventBus::connect(
        &config.nats_url,
        config.nats_user.clone(),
        config.nats_password.clone(),
        config.instance_id.clone(),
    )
    .await?;
    let consumer = bus.consumer().await?;
    let mut messages = consumer.messages().await.map_err(|error| NormalizerError {
        code: "CER-NORM-NATS-CONSUME",
        message: error.to_string(),
        retryable: true,
    })?;
    let mut core = NormalizerCore::new(
        NormalizerCoreConfig {
            pipeline_version: config.pipeline_version.clone(),
            execution_mode: config.execution_mode,
        },
        Box::new(SystemClock),
        Box::new(SystemIdGenerator),
    )?;
    core.set_source_time_policies(config.source_time_policies.clone());
    core.set_metrics(Arc::clone(&metrics));

    let mut processor = RuntimeProcessor {
        config,
        raw_reader,
        store,
        bus,
        core,
        metrics,
        first_failures: BTreeMap::new(),
    };

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                match changed {
                    Ok(()) if *shutdown.borrow() => return Ok(()),
                    Ok(()) => {},
                    Err(_) => return Ok(()),
                }
            }
            next = messages.next() => {
                let Some(next) = next else {
                    return Err(NormalizerError {
                        code: "CER-NORM-NATS-CLOSED",
                        message: "normalizer JetStream consumer stream ended".to_string(),
                        retryable: true,
                    });
                };
                let message = next.map_err(|error| NormalizerError {
                    code: "CER-NORM-NATS-CONSUME",
                    message: error.to_string(),
                    retryable: true,
                })?;
                processor.handle_message(&message).await?;
            }
        }
    }
}

struct RuntimeProcessor {
    config: RuntimeConfig,
    raw_reader: FilesystemRawReader,
    store: ClickHouseStore,
    bus: EventBus,
    core: NormalizerCore,
    metrics: Arc<dyn NormalizerMetrics>,
    first_failures: BTreeMap<String, i64>,
}

impl RuntimeProcessor {
    async fn handle_message(
        &mut self,
        message: &async_nats::jetstream::Message,
    ) -> Result<(), NormalizerError> {
        let deliveries = message.info().map_or(1, |info| info.delivered);
        let failure_key = delivery_identity(message);
        let outcome = process_message(
            &self.config,
            &self.raw_reader,
            &self.store,
            &self.bus,
            &mut self.core,
            message,
        )
        .await;

        match outcome {
            Ok(()) => {
                self.first_failures.remove(&failure_key);
                message.double_ack().await.map_err(|error| NormalizerError {
                    code: "CER-NORM-NATS-ACK",
                    message: error.to_string(),
                    retryable: true,
                })
            }
            Err(error) if error.retryable => {
                self.first_failures
                    .entry(failure_key.clone())
                    .or_insert_with(unix_time_millis);
                let delay = retry_delay_with_jitter(
                    deliveries,
                    self.config.retry_min_delay,
                    self.config.retry_max_delay,
                    &failure_key,
                );
                eprintln!("normalizer transient failure (retry in {delay:?}): {error}");
                message
                    .ack_with(AckKind::Nak(Some(delay)))
                    .await
                    .map_err(|ack_error| NormalizerError {
                        code: "CER-NORM-NATS-NAK",
                        message: ack_error.to_string(),
                        retryable: true,
                    })
            }
            Err(error) => {
                self.handle_permanent_failure(message, deliveries, &failure_key, &error)
                    .await
            }
        }
    }

    async fn handle_permanent_failure(
        &mut self,
        message: &async_nats::jetstream::Message,
        deliveries: i64,
        failure_key: &str,
        error: &NormalizerError,
    ) -> Result<(), NormalizerError> {
        let first_failure_unix_ms = *self
            .first_failures
            .entry(failure_key.to_string())
            .or_insert_with(unix_time_millis);
        let parser_identity = best_effort_parser_identity(&self.raw_reader, &self.core, message);
        let dead_letter = NormalizationDeadLetter::from_message(
            message,
            crate::NORMALIZER_CONSUMER_NAME,
            error,
            parser_identity
                .as_ref()
                .map(|(parser_id, version)| (parser_id.as_str(), version.as_str())),
            first_failure_unix_ms,
        );

        match self.bus.publish_normalization_dlq(&dead_letter).await {
            Ok(()) => {
                self.metrics.record_dlq();
                eprintln!("normalizer permanent failure dead-lettered before Term: {error}");
                message
                    .ack_with(AckKind::Term)
                    .await
                    .map_err(|ack_error| NormalizerError {
                        code: "CER-NORM-NATS-TERM",
                        message: ack_error.to_string(),
                        retryable: true,
                    })?;
                self.first_failures.remove(failure_key);
                Ok(())
            }
            Err(dlq_error) => {
                let delay = retry_delay_with_jitter(
                    deliveries,
                    self.config.retry_min_delay,
                    self.config.retry_max_delay,
                    failure_key,
                );
                eprintln!(
                    "normalizer DLQ publication failed; preserving original message for retry \
                     (retry in {delay:?}): {dlq_error}"
                );
                message
                    .ack_with(AckKind::Nak(Some(delay)))
                    .await
                    .map_err(|ack_error| NormalizerError {
                        code: "CER-NORM-NATS-NAK",
                        message: ack_error.to_string(),
                        retryable: true,
                    })
            }
        }
    }
}

async fn process_message(
    config: &RuntimeConfig,
    raw_reader: &FilesystemRawReader,
    store: &ClickHouseStore,
    bus: &EventBus,
    core: &mut NormalizerCore,
    message: &async_nats::jetstream::Message,
) -> Result<(), NormalizerError> {
    let incoming = decode_raw_persisted(message)?;
    let logical_key = core.logical_key(&incoming.persisted);
    if let Some(existing) = store.find_by_logical_key(&logical_key).await? {
        validate_existing(&existing, &incoming.persisted.event_id, &logical_key)?;
        return bus
            .publish_normalized(&existing, incoming.request_id.as_deref())
            .await;
    }

    let raw = raw_reader.read(&incoming.persisted)?;
    let plan = core.normalize(&incoming.envelope, &incoming.persisted, &raw)?;
    let ingest_time = incoming
        .persisted
        .ingest_time
        .as_ref()
        .ok_or_else(|| NormalizerError {
            code: "CER-NORM-MISSING-INGEST-TIME",
            message: "RawEventPersisted.ingest_time is required".to_string(),
            retryable: false,
        })?;
    let candidate = StoredNormalization::from_plan(
        &plan,
        plan.resolved_event_time.as_ref(),
        ingest_time,
        &incoming.envelope,
        new_uuid_v7(),
        config.component_version.clone(),
        config.instance_id.clone(),
    )?;
    let stored = store.insert_and_read_back(&candidate).await?;
    validate_existing(&stored, &incoming.persisted.event_id, &logical_key)?;
    bus.publish_normalized(&stored, incoming.request_id.as_deref())
        .await
}

fn best_effort_parser_identity(
    raw_reader: &FilesystemRawReader,
    core: &NormalizerCore,
    message: &async_nats::jetstream::Message,
) -> Option<(String, String)> {
    let incoming = decode_raw_persisted(message).ok()?;
    let raw = raw_reader.read(&incoming.persisted).ok()?;
    core.parser_identity(&incoming.persisted, &raw).ok()
}

fn delivery_identity(message: &async_nats::jetstream::Message) -> String {
    message.info().map_or_else(
        |_| format!("payload:{}", sha256_lower_hex(message.payload.as_ref())),
        |info| format!("stream:{}", info.stream_sequence),
    )
}

fn validate_existing(
    row: &StoredNormalization,
    raw_event_id: &str,
    logical_key: &str,
) -> Result<(), NormalizerError> {
    if row.logical_key != logical_key || row.raw_event_id != raw_event_id {
        return Err(NormalizerError {
            code: "CER-NORM-IDEMPOTENCY-CONFLICT",
            message: "stored logical normalization conflicts with incoming RawEvent".to_string(),
            retryable: false,
        });
    }
    if row.normalized_hash_algorithm != "sha256"
        || row.ocsf_version != crate::OCSF_VERSION
        || row.parser_id != crate::LINUX_SSHD_PARSER_ID
        || row.parser_version != crate::LINUX_SSHD_PARSER_VERSION
        || row.mapping_id != crate::LINUX_SSH_AUTH_MAPPING_ID
        || row.mapping_version != crate::LINUX_SSH_AUTH_MAPPING_VERSION
    {
        return Err(NormalizerError {
            code: "CER-NORM-IDEMPOTENCY-CONFLICT",
            message: "stored logical normalization metadata differs from governed vertical"
                .to_string(),
            retryable: false,
        });
    }
    Ok(())
}
