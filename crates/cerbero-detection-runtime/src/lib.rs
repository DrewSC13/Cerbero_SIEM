#![forbid(unsafe_code)]

use std::{fmt::Write as _, sync::Arc};

use cerbero_common::contracts::{
    sha256_lower_hex,
    v1::{CerberoEnvelope, Finding, Producer, Signal},
    validate_envelope, validate_finding, validate_signal,
};
use prost::Message as _;
use prost_types::Any;
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls, Transaction};
use uuid::Uuid;

#[derive(Debug)]
pub struct RuntimeError(String);

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for RuntimeError {}

pub struct PostgresAnalyticalRepository {
    client: Arc<Mutex<Client>>,
    component_version: String,
    instance_id: String,
}

impl PostgresAnalyticalRepository {
    /// Connects the governed analytical repository to PostgreSQL.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] when the PostgreSQL connection cannot be established.
    pub async fn connect(
        connection_string: &str,
        component_version: String,
        instance_id: String,
    ) -> Result<Self, RuntimeError> {
        let (client, connection) = tokio_postgres::connect(connection_string, NoTls)
            .await
            .map_err(runtime_error)?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            component_version,
            instance_id,
        })
    }

    /// Persists one canonical Signal idempotently by logical evaluation identity.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] when validation, persistence, outbox staging,
    /// readback, or canonical decoding fails.
    pub async fn persist_signal(&self, signal: &Signal) -> Result<Signal, RuntimeError> {
        validate_signal(signal).map_err(runtime_error)?;
        let logical_key = signal_logical_key(signal);
        let payload = signal.encode_to_vec();
        let mut client = self.client.lock().await;
        let tx = client.transaction().await.map_err(runtime_error)?;
        let (created_seconds, created_nanos) =
            timestamp_parts(signal.created_at.as_ref(), "signal.created_at")?;
        let inserted = tx.execute(
            "INSERT INTO detection.signals(signal_id, logical_key, tenant_id, created_at, payload)\
             VALUES (\
               $1::text::uuid, $2, $3::text::uuid,\
               to_timestamp($4::bigint::double precision + $5::integer::double precision / 1000000000.0), $6\
             )\
             ON CONFLICT (logical_key) DO NOTHING",
            &[&signal.signal_id, &logical_key, &signal.tenant_id,
              &created_seconds, &created_nanos, &payload],
        ).await.map_err(runtime_error)?;
        if inserted == 1 {
            for input in &signal.inputs {
                tx.execute(
                    "INSERT INTO detection.signal_inputs\
                     (signal_id, ordinal, input_type, input_id, relation)\
                     VALUES ($1::text::uuid, $2, $3, $4, $5)",
                    &[
                        &signal.signal_id,
                        &i32::try_from(input.ordinal).map_err(runtime_error)?,
                        &i16::try_from(input.input_type).map_err(runtime_error)?,
                        &input.input_id,
                        &input.relation,
                    ],
                )
                .await
                .map_err(runtime_error)?;
            }
            persist_outbox(
                &tx,
                "cerbero.v1.signal.created",
                envelope(
                    &EnvelopeSpec {
                        message_type: "SignalCreated",
                        payload_schema: "cerbero.signal.v1",
                        type_url: "type.googleapis.com/cerbero.contracts.v1.Signal",
                    },
                    &signal.tenant_id,
                    signal.created_at,
                    payload.clone(),
                    &self.component_version,
                    &self.instance_id,
                )?,
            )
            .await?;
        }
        let row = tx
            .query_one(
                "SELECT payload FROM detection.signals WHERE logical_key = $1",
                &[&logical_key],
            )
            .await
            .map_err(runtime_error)?;
        let stored: Vec<u8> = row.get(0);
        tx.commit().await.map_err(runtime_error)?;
        Signal::decode(stored.as_slice()).map_err(runtime_error)
    }

    /// Persists one canonical correlation Finding idempotently by logical identity.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] when validation, persistence, provenance/input
    /// persistence, outbox staging, readback, or canonical decoding fails.
    pub async fn persist_finding(&self, finding: &Finding) -> Result<Finding, RuntimeError> {
        validate_finding(finding).map_err(runtime_error)?;
        let logical_key = finding_logical_key(finding)?;
        let payload = finding.encode_to_vec();
        let mut client = self.client.lock().await;
        let tx = client.transaction().await.map_err(runtime_error)?;
        let (created_seconds, created_nanos) =
            timestamp_parts(finding.created_at.as_ref(), "finding.created_at")?;
        let inserted = tx.execute(
            "INSERT INTO investigation.findings(finding_id, logical_key, tenant_id, created_at, payload)\
             VALUES (\
               $1::text::uuid, $2, $3::text::uuid,\
               to_timestamp($4::bigint::double precision + $5::integer::double precision / 1000000000.0), $6\
             )\
             ON CONFLICT (logical_key) DO NOTHING",
            &[&finding.finding_id, &logical_key, &finding.tenant_id,
              &created_seconds, &created_nanos, &payload],
        ).await.map_err(runtime_error)?;
        if inserted == 1 {
            for input in &finding.inputs {
                tx.execute(
                    "INSERT INTO investigation.finding_inputs\
                     (finding_id, input_type, input_id, relation) VALUES ($1::text::uuid, $2, $3, $4)",
                    &[
                        &finding.finding_id,
                        &input.input_type,
                        &input.input_id,
                        &input.relation,
                    ],
                )
                .await
                .map_err(runtime_error)?;
            }
            if let Some(provenance) = finding.correlation_provenance.as_ref() {
                tx.execute(
                    "INSERT INTO investigation.finding_correlation_provenance\
                     (finding_id, correlation_rule_id, correlation_rule_version, configuration_hash, input_ids)\
                     VALUES ($1::text::uuid, $2, $3, $4, $5::text::jsonb)",
                    &[&finding.finding_id, &provenance.correlation_rule_id,
                      &provenance.correlation_rule_version, &provenance.configuration_hash,
                      &json_array(&provenance.input_ids)],
                ).await.map_err(runtime_error)?;
            }
            persist_outbox(
                &tx,
                "cerbero.v1.finding.created",
                envelope(
                    &EnvelopeSpec {
                        message_type: "FindingCreated",
                        payload_schema: "cerbero.finding.v1",
                        type_url: "type.googleapis.com/cerbero.contracts.v1.Finding",
                    },
                    &finding.tenant_id,
                    finding.created_at,
                    payload.clone(),
                    &self.component_version,
                    &self.instance_id,
                )?,
            )
            .await?;
        }
        let row = tx
            .query_one(
                "SELECT payload FROM investigation.findings WHERE logical_key = $1",
                &[&logical_key],
            )
            .await
            .map_err(runtime_error)?;
        let stored: Vec<u8> = row.get(0);
        tx.commit().await.map_err(runtime_error)?;
        Finding::decode(stored.as_slice()).map_err(runtime_error)
    }
}

async fn persist_outbox(
    tx: &Transaction<'_>,
    subject: &str,
    envelope: CerberoEnvelope,
) -> Result<(), RuntimeError> {
    tx.execute(
        "INSERT INTO system.outbox(message_id, subject, payload) VALUES ($1::text::uuid, $2, $3)",
        &[&envelope.message_id, &subject, &envelope.encode_to_vec()],
    )
    .await
    .map_err(runtime_error)?;
    Ok(())
}

struct EnvelopeSpec<'a> {
    message_type: &'a str,
    payload_schema: &'a str,
    type_url: &'a str,
}

fn envelope(
    spec: &EnvelopeSpec<'_>,
    tenant_id: &str,
    emitted_at: Option<prost_types::Timestamp>,
    value: Vec<u8>,
    component_version: &str,
    instance_id: &str,
) -> Result<CerberoEnvelope, RuntimeError> {
    let envelope = CerberoEnvelope {
        contract_version: "1".to_string(),
        message_id: Uuid::now_v7().to_string(),
        message_type: spec.message_type.to_string(),
        tenant_id: tenant_id.to_string(),
        producer: Some(Producer {
            component: "cerbero-detection-runtime".to_string(),
            component_version: component_version.to_string(),
            instance_id: instance_id.to_string(),
        }),
        emitted_at,
        trace_id: String::new(),
        causation_id: String::new(),
        correlation_id: String::new(),
        payload_schema: spec.payload_schema.to_string(),
        payload: Some(Any {
            type_url: spec.type_url.to_string(),
            value,
        }),
    };
    validate_envelope(&envelope).map_err(runtime_error)?;
    Ok(envelope)
}

#[must_use]
pub fn signal_logical_key(signal: &Signal) -> String {
    let mut value = format!(
        "{}|{}|{}|{}|{}",
        signal.tenant_id,
        signal.rule_id,
        signal.rule_version,
        signal.rule_type,
        signal.execution_mode
    );
    for input in &signal.inputs {
        write!(
            &mut value,
            "|{}:{}:{}:{}",
            input.ordinal, input.input_type, input.input_id, input.relation
        )
        .expect("writing to String cannot fail");
    }
    sha256_lower_hex(value.as_bytes())
}

/// Derives the stable logical deduplication key for a canonical correlation Finding.
///
/// # Errors
///
/// Returns [`RuntimeError`] when required correlation provenance is absent.
pub fn finding_logical_key(finding: &Finding) -> Result<String, RuntimeError> {
    let provenance = finding
        .correlation_provenance
        .as_ref()
        .ok_or_else(|| RuntimeError("correlation provenance is required".to_string()))?;
    let mut value = format!(
        "{}|{}|{}|{}|{}",
        finding.tenant_id,
        provenance.correlation_rule_id,
        provenance.correlation_rule_version,
        finding.execution_mode,
        provenance.configuration_hash
    );
    for input_id in &provenance.input_ids {
        value.push('|');
        value.push_str(input_id);
    }
    Ok(sha256_lower_hex(value.as_bytes()))
}

fn timestamp_parts(
    value: Option<&prost_types::Timestamp>,
    field: &str,
) -> Result<(i64, i32), RuntimeError> {
    let value = value.ok_or_else(|| RuntimeError(format!("{field} is required")))?;
    Ok((value.seconds, value.nanos))
}

fn json_array(values: &[String]) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(|value| format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\"")))
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn runtime_error(value: impl std::fmt::Display) -> RuntimeError {
    RuntimeError(value.to_string())
}

pub mod mvp_batch;
