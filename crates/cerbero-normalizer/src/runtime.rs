use async_nats::jetstream::AckKind;
use futures_util::StreamExt;
use tokio::sync::watch;

use crate::clickhouse::{ClickHouseStore, StoredNormalization};
use crate::eventbus::{EventBus, decode_raw_persisted, retry_delay};
use crate::raw_store::FilesystemRawReader;
use crate::runtime_config::RuntimeConfig;
use crate::system_id::{SystemIdGenerator, new_uuid_v7};
use crate::{NormalizerCore, NormalizerCoreConfig, NormalizerError, SystemClock};

pub async fn run(
    config: RuntimeConfig,
    mut shutdown: watch::Receiver<bool>,
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
                let deliveries = message.info().map_or(1, |info| info.delivered);
                match process_message(
                    &config,
                    &raw_reader,
                    &store,
                    &bus,
                    &mut core,
                    &message,
                )
                .await
                {
                    Ok(()) => message.double_ack().await.map_err(|error| NormalizerError {
                        code: "CER-NORM-NATS-ACK",
                        message: error.to_string(),
                        retryable: true,
                    })?,
                    Err(error) if error.retryable => {
                        let delay = retry_delay(
                            deliveries,
                            config.retry_min_delay,
                            config.retry_max_delay,
                        );
                        eprintln!("normalizer transient failure (retry in {delay:?}): {error}");
                        message
                            .ack_with(AckKind::Nak(Some(delay)))
                            .await
                            .map_err(|ack_error| NormalizerError {
                                code: "CER-NORM-NATS-NAK",
                                message: ack_error.to_string(),
                                retryable: true,
                            })?;
                    }
                    Err(error) => {
                        eprintln!("normalizer permanent failure (isolated): {error}");
                        message.ack_with(AckKind::Term).await.map_err(|ack_error| NormalizerError {
                            code: "CER-NORM-NATS-TERM",
                            message: ack_error.to_string(),
                            retryable: true,
                        })?;
                    }
                }
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
        incoming.persisted.event_time.as_ref(),
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
