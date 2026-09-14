use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_nats::jetstream::AckKind;
use cerbero_common::contracts::sha256_lower_hex;
use futures_util::StreamExt;
use tokio::sync::watch;
use tokio_postgres::NoTls;
use uuid::Uuid;

use crate::clickhouse::{ClickHouseStore, StoredNormalization};
use crate::dead_letter::{DeadLetterFailure, NormalizationDeadLetter};
use crate::eventbus::{
    EventBus, IncomingRawPersisted, decode_raw_persisted, normalizer_consumer_name,
    retry_delay_with_jitter,
};
use crate::metrics::{MemoryNormalizerMetrics, NormalizerMetrics};
use crate::raw_store::FilesystemRawReader;
use crate::retry_state::{RetryFailure, RetryState, RetryStateStore};
use crate::retry_state_postgres::PostgresRetryStateStore;
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

    let mut postgres_config = tokio_postgres::Config::new();
    postgres_config
        .host(&config.postgres_host)
        .port(config.postgres_port)
        .dbname(&config.postgres_database)
        .user(&config.postgres_user)
        .password(&config.postgres_password);
    let (postgres_client, postgres_connection) =
        postgres_config
            .connect(NoTls)
            .await
            .map_err(|error| NormalizerError {
                code: "CER-NORM-RETRY-STATE-POSTGRES-CONNECT",
                message: error.to_string(),
                retryable: true,
            })?;
    tokio::pin!(postgres_connection);
    let retry_store = PostgresRetryStateStore::new(Arc::new(postgres_client));

    let bus = EventBus::connect(
        &config.nats_url,
        config.nats_user.clone(),
        config.nats_password.clone(),
        config.instance_id.clone(),
    )
    .await?;
    let consumer = bus.consumer(config.execution_mode).await?;
    let mut messages = consumer.messages().await.map_err(|error| NormalizerError {
        code: "CER-NORM-NATS-CONSUME",
        message: error.to_string(),
        retryable: true,
    })?;
    let core = build_core(&config, Arc::clone(&metrics))?;

    let mut processor = RuntimeProcessor {
        config,
        raw_reader,
        store,
        retry_store,
        bus,
        core,
        metrics,
    };

    let processing = async move {
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    match changed {
                        Ok(()) if *shutdown.borrow() => return processor.clean_shutdown().await,
                        Ok(()) => {},
                        Err(_) => return processor.clean_shutdown().await,
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
    };
    tokio::pin!(processing);

    tokio::select! {
        postgres_result = &mut postgres_connection => {
            Err(match postgres_result {
                Ok(()) => NormalizerError {
                    code: "CER-NORM-RETRY-STATE-POSTGRES-CLOSED",
                    message: "normalizer retry-state PostgreSQL connection closed".to_string(),
                    retryable: true,
                },
                Err(error) => NormalizerError {
                    code: "CER-NORM-RETRY-STATE-POSTGRES-CONNECTION",
                    message: error.to_string(),
                    retryable: true,
                },
            })
        }
        processing_result = &mut processing => processing_result,
    }
}

fn build_core(
    config: &RuntimeConfig,
    metrics: Arc<dyn NormalizerMetrics>,
) -> Result<NormalizerCore, NormalizerError> {
    let mut core = NormalizerCore::new_with_linux_sshd_version(
        NormalizerCoreConfig {
            pipeline_version: config.pipeline_version.clone(),
            execution_mode: config.execution_mode,
        },
        &config.linux_sshd_parser_version,
        Box::new(SystemClock),
        Box::new(SystemIdGenerator),
    )?;
    core.set_source_time_policies(config.source_time_policies.clone());
    core.set_metrics(metrics);
    Ok(core)
}

struct RuntimeProcessor {
    config: RuntimeConfig,
    raw_reader: FilesystemRawReader,
    store: ClickHouseStore,
    retry_store: PostgresRetryStateStore,
    bus: EventBus,
    core: NormalizerCore,
    metrics: Arc<dyn NormalizerMetrics>,
}

impl RuntimeProcessor {
    async fn clean_shutdown(&mut self) -> Result<(), NormalizerError> {
        self.bus
            .delete_execution_consumer(self.config.execution_mode)
            .await
    }

    async fn handle_message(
        &mut self,
        message: &async_nats::jetstream::Message,
    ) -> Result<(), NormalizerError> {
        let deliveries = message.info().map_or(1, |info| info.delivered.max(1));
        let message_key = delivery_identity(message);
        let consumer_name = normalizer_consumer_name(self.config.execution_mode)?;

        if let Some(state) = self.retry_store.load(consumer_name, &message_key).await?
            && state.isolation_required()
        {
            return self
                .handle_isolation(message, deliveries, &message_key, &state)
                .await;
        }

        let incoming = match decode_raw_persisted(message) {
            Ok(incoming) => incoming,
            Err(error) => {
                return self
                    .handle_processing_failure(
                        message,
                        deliveries,
                        &message_key,
                        consumer_name,
                        None,
                        &error,
                    )
                    .await;
            }
        };
        let message_id = Some(parse_message_id(&incoming.envelope.message_id)?);

        let outcome = process_message(
            &self.config,
            &self.raw_reader,
            &self.store,
            &self.bus,
            &mut self.core,
            &incoming,
        )
        .await;

        match outcome {
            Ok(()) => {
                self.retry_store.clear(consumer_name, &message_key).await?;
                message.double_ack().await.map_err(|error| NormalizerError {
                    code: "CER-NORM-NATS-ACK",
                    message: error.to_string(),
                    retryable: true,
                })
            }
            Err(error) => {
                self.handle_processing_failure(
                    message,
                    deliveries,
                    &message_key,
                    consumer_name,
                    message_id,
                    &error,
                )
                .await
            }
        }
    }

    async fn handle_processing_failure(
        &mut self,
        message: &async_nats::jetstream::Message,
        deliveries: i64,
        message_key: &str,
        consumer_name: &str,
        message_id: Option<Uuid>,
        error: &NormalizerError,
    ) -> Result<(), NormalizerError> {
        let delay = retry_delay_with_jitter(
            deliveries,
            self.config.retry_min_delay,
            self.config.retry_max_delay,
            message_key,
        );
        let occurred_at = SystemTime::now();
        let next_retry_at = occurred_at
            .checked_add(delay)
            .ok_or_else(|| NormalizerError {
                code: "CER-NORM-RETRY-STATE-TIME",
                message: "normalizer retry time overflow".to_string(),
                retryable: false,
            })?;

        let state = self
            .retry_store
            .record_failure(RetryFailure {
                consumer_name: consumer_name.to_string(),
                message_key: message_key.to_string(),
                message_id,
                occurred_at,
                retry_budget: self.config.retry_budget,
                delivery_attempt: deliveries,
                error_code: error.code.to_string(),
                error_message: error.message.clone(),
                error_retryable: error.retryable,
                next_retry_at,
            })
            .await?;

        if !state.isolation_required() {
            eprintln!(
                "normalizer transient failure {}/{} (retry in {delay:?}): {error}",
                state.failure_count, state.retry_budget
            );
            return nak_with_delay(message, delay).await;
        }

        self.handle_isolation(message, deliveries, message_key, &state)
            .await
    }

    async fn handle_isolation(
        &mut self,
        message: &async_nats::jetstream::Message,
        deliveries: i64,
        failure_key: &str,
        state: &RetryState,
    ) -> Result<(), NormalizerError> {
        let parser_identity = best_effort_parser_identity(&self.raw_reader, &self.core, message);
        let failure = DeadLetterFailure {
            error_code: &state.last_error_code,
            error_message: &state.last_error_message,
            retryable: state.last_error_retryable,
            attempt_count: u64::try_from(deliveries.max(1)).unwrap_or(u64::MAX),
            first_failure_at: state.first_failure_at,
            last_failure_at: state.last_failure_at,
        };
        let dead_letter = NormalizationDeadLetter::from_message(
            message,
            &state.consumer_name,
            &failure,
            parser_identity
                .as_ref()
                .map(|(parser_id, version)| (parser_id.as_str(), version.as_str())),
        );

        match self.bus.publish_normalization_dlq(&dead_letter).await {
            Ok(()) => {
                self.metrics.record_dlq();
                eprintln!(
                    "normalizer failure dead-lettered before Term: {}: {}",
                    state.last_error_code, state.last_error_message
                );
                message
                    .ack_with(AckKind::Term)
                    .await
                    .map_err(|ack_error| NormalizerError {
                        code: "CER-NORM-NATS-TERM",
                        message: ack_error.to_string(),
                        retryable: true,
                    })?;
                self.retry_store
                    .clear(&state.consumer_name, &state.message_key)
                    .await
            }
            Err(dlq_error) => {
                let delay = retry_delay_with_jitter(
                    deliveries,
                    self.config.retry_min_delay,
                    self.config.retry_max_delay,
                    failure_key,
                );
                eprintln!(
                    "normalizer DLQ publication failed; preserving original message and durable \
                     retry state (retry in {delay:?}): {dlq_error}"
                );
                nak_with_delay(message, delay).await
            }
        }
    }
}

async fn nak_with_delay(
    message: &async_nats::jetstream::Message,
    delay: Duration,
) -> Result<(), NormalizerError> {
    message
        .ack_with(AckKind::Nak(Some(delay)))
        .await
        .map_err(|ack_error| NormalizerError {
            code: "CER-NORM-NATS-NAK",
            message: ack_error.to_string(),
            retryable: true,
        })
}

fn parse_message_id(value: &str) -> Result<Uuid, NormalizerError> {
    Uuid::parse_str(value).map_err(|error| NormalizerError {
        code: "CER-NORM-RETRY-STATE-IDENTITY",
        message: format!("validated message_id could not be parsed as UUID: {error}"),
        retryable: false,
    })
}

async fn process_message(
    config: &RuntimeConfig,
    raw_reader: &FilesystemRawReader,
    store: &ClickHouseStore,
    bus: &EventBus,
    core: &mut NormalizerCore,
    incoming: &IncomingRawPersisted,
) -> Result<(), NormalizerError> {
    let raw = raw_reader.read(&incoming.persisted)?;
    let identity = match core.derivation_identity(&incoming.persisted, &raw) {
        Ok(identity) => identity,
        Err(preflight_error) => {
            return match core.normalize(&incoming.envelope, &incoming.persisted, &raw) {
                Err(error) => Err(error),
                Ok(_) => Err(NormalizerError {
                    code: "CER-NORM-DERIVATION-IDENTITY",
                    message: format!(
                        "derivation preflight failed but normalization succeeded: {preflight_error}"
                    ),
                    retryable: false,
                }),
            };
        }
    };

    if let Some(existing) = store.find_by_logical_key(&identity.logical_key).await? {
        validate_existing_identity(&existing, &incoming.persisted.event_id, &identity)?;
        return bus
            .publish_normalized(&existing, incoming.request_id.as_deref())
            .await;
    }

    let plan = core.normalize(&incoming.envelope, &incoming.persisted, &raw)?;
    if plan.logical_key != identity.logical_key {
        return Err(NormalizerError {
            code: "CER-NORM-DERIVATION-IDENTITY",
            message: "normalization plan identity differs from deterministic preflight".to_string(),
            retryable: false,
        });
    }
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
    validate_existing_plan(&stored, &plan)?;
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

fn validate_existing_identity(
    row: &StoredNormalization,
    raw_event_id: &str,
    identity: &crate::DerivationIdentity,
) -> Result<(), NormalizerError> {
    let matches = row.logical_key == identity.logical_key
        && row.raw_event_id == raw_event_id
        && row.ocsf_version == identity.ocsf_version
        && row.pipeline_version == identity.pipeline_version
        && row.parser_id == identity.parser_id
        && row.parser_version == identity.parser_version
        && row.mapping_id == identity.mapping_id
        && row.mapping_version == identity.mapping_version
        && row.configuration_hash == identity.configuration_hash
        && row.execution_mode == identity.execution_mode as i32;
    if matches {
        Ok(())
    } else {
        Err(NormalizerError {
            code: "CER-NORM-IDEMPOTENCY-CONFLICT",
            message: "stored logical normalization metadata conflicts with derivation identity"
                .to_string(),
            retryable: false,
        })
    }
}

fn validate_existing_plan(
    row: &StoredNormalization,
    plan: &crate::NormalizationPlan,
) -> Result<(), NormalizerError> {
    let execution_mode =
        cerbero_common::contracts::v1::ExecutionMode::try_from(plan.transformation.execution_mode)
            .map_err(|_| NormalizerError {
                code: "CER-NORM-IDEMPOTENCY-CONFLICT",
                message: "normalization plan execution_mode is invalid".to_string(),
                retryable: false,
            })?;
    let identity = crate::DerivationIdentity {
        logical_key: plan.logical_key.clone(),
        parser_id: plan.normalized_event.parser_id.clone(),
        parser_version: plan.normalized_event.parser_version.clone(),
        mapping_id: plan.mapping_id.clone(),
        mapping_version: plan.mapping_version.clone(),
        ocsf_version: plan.normalized_event.ocsf_version.clone(),
        pipeline_version: plan.normalized_event.pipeline_version.clone(),
        configuration_hash: plan.transformation.configuration_hash.clone(),
        execution_mode,
    };
    validate_existing_identity(row, &plan.normalized_event.raw_event_id, &identity)?;
    if row.normalized_hash_algorithm == plan.normalized_event.normalized_hash_algorithm
        && row.normalized_hash == plan.normalized_event.normalized_hash
    {
        Ok(())
    } else {
        Err(NormalizerError {
            code: "CER-NORM-IDEMPOTENCY-CONFLICT",
            message: "stored normalization payload metadata conflicts with normalization plan"
                .to_string(),
            retryable: false,
        })
    }
}
