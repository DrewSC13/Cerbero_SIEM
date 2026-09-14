use cerbero_common::contracts::v1::{
    CerberoEnvelope, ExecutionMode, NormalizationStatus, NormalizedEvent, Transformation,
    TransformationStatus,
};
use prost_types::Timestamp;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::{NormalizationPlan, NormalizerError};

#[derive(Clone)]
pub struct ClickHouseStore {
    client: Client,
    endpoint: String,
    database: String,
    user: String,
    password: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredNormalization {
    pub logical_key: String,
    pub tenant_id: String,
    pub normalized_event_id: String,
    pub raw_event_id: String,
    pub event_time_present: u8,
    pub event_time_seconds: i64,
    pub event_time_nanos: i32,
    pub ingest_time_seconds: i64,
    pub ingest_time_nanos: i32,
    pub normalized_at_seconds: i64,
    pub normalized_at_nanos: i32,
    pub ocsf_version: String,
    pub class_uid: u32,
    pub category_uid: u32,
    pub activity_id: Option<u32>,
    pub severity: Option<u32>,
    pub parser_id: String,
    pub parser_version: String,
    pub mapping_id: String,
    pub mapping_version: String,
    pub normalization_status: i32,
    pub normalized_hash_algorithm: String,
    pub normalized_hash: String,
    pub pipeline_version: String,
    pub ocsf_event_json: String,
    pub transformation_id: String,
    pub configuration_hash: String,
    pub execution_mode: i32,
    pub publication_message_id: String,
    pub causation_message_id: String,
    pub trace_id: String,
    pub correlation_id: String,
    pub producer_component_version: String,
    pub producer_instance_id: String,
}

impl ClickHouseStore {
    #[must_use]
    pub fn new(endpoint: String, database: String, user: String, password: String) -> Self {
        Self {
            client: Client::new(),
            endpoint,
            database,
            user,
            password,
        }
    }

    pub async fn find_by_logical_key(
        &self,
        logical_key: &str,
    ) -> Result<Option<StoredNormalization>, NormalizerError> {
        validate_hash_key(logical_key)?;
        let query = format!(
            "SELECT * FROM {}.normalized_events WHERE logical_key = '{}' LIMIT 1 FORMAT JSONEachRow",
            self.database, logical_key
        );
        let response = self
            .client
            .get(&self.endpoint)
            .basic_auth(&self.user, Some(&self.password))
            .query(&[("query", query)])
            .send()
            .await
            .map_err(|error| clickhouse_transport(&error))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|error| clickhouse_transport(&error))?;
        if !status.is_success() {
            return Err(clickhouse_server(status.as_u16(), &body));
        }
        let trimmed = body.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }
        serde_json::from_str(trimmed)
            .map(Some)
            .map_err(|error| NormalizerError {
                code: "CER-NORM-CLICKHOUSE-DECODE",
                message: format!("decode ClickHouse normalization row: {error}"),
                retryable: false,
            })
    }

    pub async fn insert_and_read_back(
        &self,
        row: &StoredNormalization,
    ) -> Result<StoredNormalization, NormalizerError> {
        validate_hash_key(&row.logical_key)?;
        let mut body = serde_json::to_vec(row).map_err(|error| NormalizerError {
            code: "CER-NORM-CLICKHOUSE-ENCODE",
            message: format!("encode ClickHouse normalization row: {error}"),
            retryable: false,
        })?;
        body.push(b'\n');
        let query = format!(
            "INSERT INTO {}.normalized_events FORMAT JSONEachRow",
            self.database
        );
        let response = self
            .client
            .post(&self.endpoint)
            .basic_auth(&self.user, Some(&self.password))
            .query(&[
                ("query", query.as_str()),
                ("async_insert", "0"),
                ("insert_deduplication_token", row.logical_key.as_str()),
            ])
            .body(body)
            .send()
            .await
            .map_err(|error| clickhouse_transport(&error))?;
        let status = response.status();
        let response_body = response
            .text()
            .await
            .map_err(|error| clickhouse_transport(&error))?;
        if !status.is_success() {
            return Err(clickhouse_server(status.as_u16(), &response_body));
        }
        self.find_by_logical_key(&row.logical_key)
            .await?
            .ok_or_else(|| NormalizerError {
                code: "CER-NORM-CLICKHOUSE-READBACK",
                message: "ClickHouse acknowledged insert but logical row was not readable"
                    .to_string(),
                retryable: true,
            })
    }
}

impl StoredNormalization {
    pub fn from_plan(
        plan: &NormalizationPlan,
        persisted_event_time: Option<&Timestamp>,
        persisted_ingest_time: &Timestamp,
        incoming: &CerberoEnvelope,
        publication_message_id: String,
        producer_component_version: String,
        producer_instance_id: String,
    ) -> Result<Self, NormalizerError> {
        let normalized_at = plan
            .normalized_event
            .normalized_at
            .as_ref()
            .ok_or_else(|| invalid("NormalizedEvent.normalized_at is required"))?;
        let (event_time_present, event_time_seconds, event_time_nanos) =
            persisted_event_time.map_or((0, 0, 0), |time| (1, time.seconds, time.nanos));
        let transformation = &plan.transformation;
        Ok(Self {
            logical_key: plan.logical_key.clone(),
            tenant_id: plan.normalized_event.tenant_id.clone(),
            normalized_event_id: plan.normalized_event.normalized_event_id.clone(),
            raw_event_id: plan.normalized_event.raw_event_id.clone(),
            event_time_present,
            event_time_seconds,
            event_time_nanos,
            ingest_time_seconds: persisted_ingest_time.seconds,
            ingest_time_nanos: persisted_ingest_time.nanos,
            normalized_at_seconds: normalized_at.seconds,
            normalized_at_nanos: normalized_at.nanos,
            ocsf_version: plan.normalized_event.ocsf_version.clone(),
            class_uid: plan.normalized_event.class_uid,
            category_uid: plan.normalized_event.category_uid,
            activity_id: plan.normalized_event.activity_id,
            severity: plan.normalized_event.severity,
            parser_id: plan.normalized_event.parser_id.clone(),
            parser_version: plan.normalized_event.parser_version.clone(),
            mapping_id: plan.mapping_id.clone(),
            mapping_version: plan.mapping_version.clone(),
            normalization_status: plan.normalized_event.normalization_status,
            normalized_hash_algorithm: plan.normalized_event.normalized_hash_algorithm.clone(),
            normalized_hash: plan.normalized_event.normalized_hash.clone(),
            pipeline_version: plan.normalized_event.pipeline_version.clone(),
            ocsf_event_json: String::from_utf8(plan.canonical_ocsf_json.clone()).map_err(
                |error| NormalizerError {
                    code: "CER-NORM-OCSF-UTF8",
                    message: error.to_string(),
                    retryable: false,
                },
            )?,
            transformation_id: transformation.transformation_id.clone(),
            configuration_hash: transformation.configuration_hash.clone(),
            execution_mode: transformation.execution_mode,
            publication_message_id,
            causation_message_id: incoming.message_id.clone(),
            trace_id: incoming.trace_id.clone(),
            correlation_id: incoming.correlation_id.clone(),
            producer_component_version,
            producer_instance_id,
        })
    }

    pub fn normalized_event(&self) -> Result<NormalizedEvent, NormalizerError> {
        let value: serde_json::Value =
            serde_json::from_str(&self.ocsf_event_json).map_err(|error| NormalizerError {
                code: "CER-NORM-OCSF-DECODE",
                message: format!("decode stored canonical OCSF JSON: {error}"),
                retryable: false,
            })?;
        Ok(NormalizedEvent {
            normalized_event_id: self.normalized_event_id.clone(),
            raw_event_id: self.raw_event_id.clone(),
            tenant_id: self.tenant_id.clone(),
            normalized_at: Some(Timestamp {
                seconds: self.normalized_at_seconds,
                nanos: self.normalized_at_nanos,
            }),
            ocsf_version: self.ocsf_version.clone(),
            class_uid: self.class_uid,
            category_uid: self.category_uid,
            severity: self.severity,
            activity_id: self.activity_id,
            ocsf_event: Some(json_to_prost_struct(&value)?),
            parser_id: self.parser_id.clone(),
            parser_version: self.parser_version.clone(),
            normalization_status: self.normalization_status,
            normalized_hash_algorithm: self.normalized_hash_algorithm.clone(),
            normalized_hash: self.normalized_hash.clone(),
            pipeline_version: self.pipeline_version.clone(),
        })
    }

    #[must_use]
    pub fn transformation(&self) -> Transformation {
        Transformation {
            transformation_id: self.transformation_id.clone(),
            input_object_id: self.raw_event_id.clone(),
            input_object_type: "RawEvent".to_string(),
            output_object_id: self.normalized_event_id.clone(),
            output_object_type: "NormalizedEvent".to_string(),
            component: format!("cerbero-normalizer:{}", self.mapping_id),
            component_version: self.mapping_version.clone(),
            configuration_hash: self.configuration_hash.clone(),
            started_at: Some(Timestamp {
                seconds: self.normalized_at_seconds,
                nanos: self.normalized_at_nanos,
            }),
            completed_at: Some(Timestamp {
                seconds: self.normalized_at_seconds,
                nanos: self.normalized_at_nanos,
            }),
            status: TransformationStatus::Success as i32,
            error: None,
            execution_mode: self.execution_mode,
        }
    }

    #[must_use]
    pub fn execution_mode(&self) -> Option<ExecutionMode> {
        ExecutionMode::try_from(self.execution_mode).ok()
    }

    #[must_use]
    pub fn normalization_status(&self) -> Option<NormalizationStatus> {
        NormalizationStatus::try_from(self.normalization_status).ok()
    }
}

fn validate_hash_key(value: &str) -> Result<(), NormalizerError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        Ok(())
    } else {
        Err(invalid(
            "logical normalization key must be lowercase SHA-256 hex",
        ))
    }
}

fn clickhouse_transport(error: &reqwest::Error) -> NormalizerError {
    NormalizerError {
        code: "CER-NORM-CLICKHOUSE-UNAVAILABLE",
        message: error.to_string(),
        retryable: true,
    }
}

fn clickhouse_server(status: u16, body: &str) -> NormalizerError {
    NormalizerError {
        code: "CER-NORM-CLICKHOUSE-ERROR",
        message: format!("ClickHouse HTTP {status}: {body}"),
        retryable: status >= 500 || status == 429,
    }
}

fn invalid(message: &str) -> NormalizerError {
    NormalizerError {
        code: "CER-NORM-STORED-ROW",
        message: message.to_string(),
        retryable: false,
    }
}

fn json_to_prost_struct(value: &serde_json::Value) -> Result<prost_types::Struct, NormalizerError> {
    let serde_json::Value::Object(object) = value else {
        return Err(invalid("stored OCSF event root must be an object"));
    };
    Ok(prost_types::Struct {
        fields: object
            .iter()
            .map(|(key, value)| Ok((key.clone(), json_to_prost_value(value)?)))
            .collect::<Result<_, NormalizerError>>()?,
    })
}

fn json_to_prost_value(value: &serde_json::Value) -> Result<prost_types::Value, NormalizerError> {
    use prost_types::value::Kind;
    let kind = match value {
        serde_json::Value::Null => Kind::NullValue(0),
        serde_json::Value::Bool(value) => Kind::BoolValue(*value),
        serde_json::Value::Number(value) => Kind::NumberValue(value.as_f64().ok_or_else(|| {
            invalid("stored OCSF number cannot be represented by protobuf Struct")
        })?),
        serde_json::Value::String(value) => Kind::StringValue(value.clone()),
        serde_json::Value::Array(values) => Kind::ListValue(prost_types::ListValue {
            values: values
                .iter()
                .map(json_to_prost_value)
                .collect::<Result<_, _>>()?,
        }),
        serde_json::Value::Object(object) => Kind::StructValue(prost_types::Struct {
            fields: object
                .iter()
                .map(|(key, value)| Ok((key.clone(), json_to_prost_value(value)?)))
                .collect::<Result<_, NormalizerError>>()?,
        }),
    };
    Ok(prost_types::Value { kind: Some(kind) })
}
