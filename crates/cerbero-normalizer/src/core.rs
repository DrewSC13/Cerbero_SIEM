use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use cerbero_common::contracts::v1::{
    CerberoEnvelope, ExecutionMode, NormalizationStatus, NormalizedEvent, RawEventPersisted,
    Transformation, TransformationStatus,
};
use cerbero_common::contracts::{
    sha256_lower_hex, validate_normalized_event, validate_transformation,
};
use prost_types::{Struct, Timestamp, value::Kind};
use serde_json::Value;

use crate::canonical::{canonical_json_bytes, canonical_json_hash};
use crate::mapping::{MappingRegistry, OCSF_VERSION};
use crate::metrics::{
    NoopNormalizerMetrics, NormalizationMetricStatus, NormalizerMetrics, ParseMetricStatus,
};
use crate::parser::{ParserInput, ParserRegistry, ParsingStatus};
use crate::source_time::{SourceTimeError, SourceTimePolicyRegistry};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NormalizerError {
    pub code: &'static str,
    pub message: String,
    pub retryable: bool,
}

impl fmt::Display for NormalizerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for NormalizerError {}

pub trait Clock: Send + Sync {
    fn now(&self) -> Result<Timestamp, NormalizerError>;
}

pub trait IdGenerator: Send {
    fn new_uuid_v7(&mut self) -> Result<String, NormalizerError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Result<Timestamp, NormalizerError> {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| NormalizerError {
                code: "CER-NORM-CLOCK",
                message: error.to_string(),
                retryable: true,
            })?;
        Ok(Timestamp {
            seconds: i64::try_from(duration.as_secs()).map_err(|_| NormalizerError {
                code: "CER-NORM-CLOCK",
                message: "system time seconds exceed i64".to_string(),
                retryable: false,
            })?,
            nanos: i32::try_from(duration.subsec_nanos()).expect("subsecond nanos fit i32"),
        })
    }
}

#[derive(Clone, Debug)]
pub struct FixedClock(pub Timestamp);

impl Clock for FixedClock {
    fn now(&self) -> Result<Timestamp, NormalizerError> {
        Ok(self.0)
    }
}

#[derive(Clone, Debug, Default)]
pub struct FixedIdGenerator {
    values: VecDeque<String>,
}

impl FixedIdGenerator {
    #[must_use]
    pub fn new(values: impl IntoIterator<Item = String>) -> Self {
        Self {
            values: values.into_iter().collect(),
        }
    }
}

impl IdGenerator for FixedIdGenerator {
    fn new_uuid_v7(&mut self) -> Result<String, NormalizerError> {
        self.values.pop_front().ok_or_else(|| NormalizerError {
            code: "CER-NORM-ID-EXHAUSTED",
            message: "test ID generator is exhausted".to_string(),
            retryable: false,
        })
    }
}

#[derive(Clone, Debug)]
pub struct NormalizerCoreConfig {
    pub pipeline_version: String,
    pub execution_mode: ExecutionMode,
}

impl NormalizerCoreConfig {
    #[must_use]
    pub fn configuration_hash(&self) -> String {
        self.configuration_hash_for(
            crate::LINUX_SSHD_PARSER_ID,
            crate::LINUX_SSHD_PARSER_VERSION,
            crate::LINUX_SSH_AUTH_MAPPING_ID,
            crate::LINUX_SSH_AUTH_MAPPING_VERSION,
        )
    }

    #[must_use]
    pub fn configuration_hash_for(
        &self,
        parser_id: &str,
        parser_version: &str,
        mapping_id: &str,
        mapping_version: &str,
    ) -> String {
        sha256_lower_hex(
            format!(
                "parser={parser_id}@{parser_version}\nmapping={mapping_id}@{mapping_version}\nocsf={}\npipeline={}\nmode={}\n",
                OCSF_VERSION,
                self.pipeline_version,
                self.execution_mode.as_str_name()
            )
            .as_bytes(),
        )
    }
}

#[derive(Clone, Debug)]
pub struct NormalizationPlan {
    pub logical_key: String,
    pub resolved_event_time: Option<Timestamp>,
    pub mapping_id: String,
    pub mapping_version: String,
    pub canonical_ocsf_json: Vec<u8>,
    pub normalized_event: NormalizedEvent,
    pub transformation: Transformation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DerivationIdentity {
    pub logical_key: String,
    pub parser_id: String,
    pub parser_version: String,
    pub mapping_id: String,
    pub mapping_version: String,
    pub ocsf_version: String,
    pub pipeline_version: String,
    pub configuration_hash: String,
    pub execution_mode: ExecutionMode,
}

pub struct NormalizerCore {
    config: NormalizerCoreConfig,
    parsers: ParserRegistry,
    mappings: MappingRegistry,
    source_time_policies: SourceTimePolicyRegistry,
    metrics: Arc<dyn NormalizerMetrics>,
    clock: Box<dyn Clock>,
    ids: Box<dyn IdGenerator>,
}

impl NormalizerCore {
    pub fn new(
        config: NormalizerCoreConfig,
        clock: Box<dyn Clock>,
        ids: Box<dyn IdGenerator>,
    ) -> Result<Self, NormalizerError> {
        Self::new_with_linux_sshd_version(config, crate::LINUX_SSHD_PARSER_VERSION, clock, ids)
    }

    pub fn new_with_linux_sshd_version(
        config: NormalizerCoreConfig,
        linux_sshd_parser_version: &str,
        clock: Box<dyn Clock>,
        ids: Box<dyn IdGenerator>,
    ) -> Result<Self, NormalizerError> {
        if config.pipeline_version.is_empty() {
            return Err(invalid("pipeline_version is required"));
        }
        if config.execution_mode == ExecutionMode::Unspecified {
            return Err(invalid("execution_mode must be LIVE, REPLAY, or TEST"));
        }
        let parsers = ParserRegistry::with_linux_sshd_version(linux_sshd_parser_version)
            .map_err(from_parse)?;
        Ok(Self {
            config,
            parsers,
            mappings: MappingRegistry::with_defaults(),
            source_time_policies: SourceTimePolicyRegistry::default(),
            metrics: Arc::new(NoopNormalizerMetrics),
            clock,
            ids,
        })
    }

    pub fn set_source_time_policies(&mut self, policies: SourceTimePolicyRegistry) {
        self.source_time_policies = policies;
    }

    pub fn set_metrics(&mut self, metrics: Arc<dyn NormalizerMetrics>) {
        self.metrics = metrics;
    }

    pub fn parser_identity(
        &self,
        persisted: &RawEventPersisted,
        raw: &[u8],
    ) -> Result<(String, String), NormalizerError> {
        let input = ParserInput {
            raw,
            persisted,
            configured_parser_id: None,
        };
        let selection = self.parsers.select(&input).map_err(from_parse)?;
        Ok((
            selection.parser().id().to_string(),
            selection.parser().version().to_string(),
        ))
    }

    fn configuration_hash_for(
        &self,
        persisted: &RawEventPersisted,
        parser_id: &str,
        parser_version: &str,
        mapping_id: &str,
        mapping_version: &str,
    ) -> String {
        let base = self.config.configuration_hash_for(
            parser_id,
            parser_version,
            mapping_id,
            mapping_version,
        );
        if persisted.event_time.is_some() {
            return base;
        }
        self.source_time_policies
            .policy_canonical(&persisted.source_id)
            .map_or(base.clone(), |policy| {
                sha256_lower_hex(
                    format!("base_configuration={base}\nsource_time_policy={policy}\n").as_bytes(),
                )
            })
    }

    #[must_use]
    pub fn logical_key(&self, persisted: &RawEventPersisted) -> String {
        self.logical_key_for(
            persisted,
            crate::LINUX_SSHD_PARSER_ID,
            crate::LINUX_SSHD_PARSER_VERSION,
            crate::LINUX_SSH_AUTH_MAPPING_ID,
            crate::LINUX_SSH_AUTH_MAPPING_VERSION,
        )
    }

    #[must_use]
    pub fn logical_key_for(
        &self,
        persisted: &RawEventPersisted,
        parser_id: &str,
        parser_version: &str,
        mapping_id: &str,
        mapping_version: &str,
    ) -> String {
        let configuration_hash = self.configuration_hash_for(
            persisted,
            parser_id,
            parser_version,
            mapping_id,
            mapping_version,
        );
        sha256_lower_hex(
            format!(
                "raw_event_id={}\nparser={parser_id}@{parser_version}\nmapping={mapping_id}@{mapping_version}\nocsf={}\npipeline={}\nconfiguration={configuration_hash}\nexecution_mode={}\n",
                persisted.event_id,
                OCSF_VERSION,
                self.config.pipeline_version,
                self.config.execution_mode.as_str_name()
            )
            .as_bytes(),
        )
    }

    pub fn derivation_identity(
        &self,
        persisted: &RawEventPersisted,
        raw: &[u8],
    ) -> Result<DerivationIdentity, NormalizerError> {
        validate_raw_bytes(persisted, raw)?;
        let input = ParserInput {
            raw,
            persisted,
            configured_parser_id: None,
        };
        let selection = self.parsers.select(&input).map_err(from_parse)?;
        let parser = selection.parser();
        let mapping = self
            .mappings
            .for_parser(parser.id())
            .map_err(from_mapping)?;
        let configuration_hash = self.configuration_hash_for(
            persisted,
            parser.id(),
            parser.version(),
            mapping.id(),
            mapping.version(),
        );
        let logical_key = self.logical_key_for(
            persisted,
            parser.id(),
            parser.version(),
            mapping.id(),
            mapping.version(),
        );
        Ok(DerivationIdentity {
            logical_key,
            parser_id: parser.id().to_string(),
            parser_version: parser.version().to_string(),
            mapping_id: mapping.id().to_string(),
            mapping_version: mapping.version().to_string(),
            ocsf_version: OCSF_VERSION.to_string(),
            pipeline_version: self.config.pipeline_version.clone(),
            configuration_hash,
            execution_mode: self.config.execution_mode,
        })
    }

    pub fn normalize(
        &mut self,
        incoming: &CerberoEnvelope,
        persisted: &RawEventPersisted,
        raw: &[u8],
    ) -> Result<NormalizationPlan, NormalizerError> {
        validate_handoff(incoming, persisted)?;
        validate_raw_bytes(persisted, raw)?;

        let input = ParserInput {
            raw,
            persisted,
            configured_parser_id: None,
        };
        let parse_started = Instant::now();
        let parsed = match self.parsers.parse(&input) {
            Ok(parsed) => {
                self.metrics.record_parse(
                    parse_metric_status(&parsed.status),
                    Some(&parsed.parser_id),
                    parse_started.elapsed(),
                );
                parsed
            }
            Err(error) => {
                let status = if error.code == "CER-PARSE-NO-MATCH" {
                    ParseMetricStatus::Unsupported
                } else {
                    ParseMetricStatus::Failed
                };
                self.metrics
                    .record_parse(status, None, parse_started.elapsed());
                return Err(from_parse(error));
            }
        };
        let normalization_started = Instant::now();
        let result = self.build_normalization_plan(persisted, &parsed);

        match &result {
            Ok(plan) => {
                let status =
                    NormalizationStatus::try_from(plan.normalized_event.normalization_status)
                        .unwrap_or(NormalizationStatus::Unspecified);
                self.metrics.record_normalization(
                    normalization_metric_status(status),
                    Some(&plan.mapping_id),
                    normalization_started.elapsed(),
                );
            }
            Err(_) => self.metrics.record_normalization(
                NormalizationMetricStatus::Failed,
                None,
                normalization_started.elapsed(),
            ),
        }
        result
    }

    fn build_normalization_plan(
        &mut self,
        persisted: &RawEventPersisted,
        parsed: &crate::parser::ParsedEvent,
    ) -> Result<NormalizationPlan, NormalizerError> {
        let mapping = self
            .mappings
            .for_parser(&parsed.parser_id)
            .map_err(from_mapping)?;
        let event_time = self
            .source_time_policies
            .resolve(persisted, parsed)
            .map_err(from_source_time)?;
        let normalized_at = self.clock.now()?;
        let normalized_at_millis = timestamp_to_unix_millis(&normalized_at)?;
        let mapped = mapping
            .map(parsed, persisted, &event_time, normalized_at_millis)
            .map_err(from_mapping)?;
        let canonical_ocsf_json = canonical_json_bytes(&mapped.ocsf_event);
        let normalized_hash = canonical_json_hash(&mapped.ocsf_event);
        let configuration_hash = self.configuration_hash_for(
            persisted,
            &parsed.parser_id,
            &parsed.parser_version,
            &mapped.mapping_id,
            &mapped.mapping_version,
        );
        let logical_key = self.logical_key_for(
            persisted,
            &parsed.parser_id,
            &parsed.parser_version,
            &mapped.mapping_id,
            &mapped.mapping_version,
        );

        let normalized_event_id = self.ids.new_uuid_v7()?;
        let transformation_id = self.ids.new_uuid_v7()?;
        let normalized_event = NormalizedEvent {
            normalized_event_id: normalized_event_id.clone(),
            raw_event_id: persisted.event_id.clone(),
            tenant_id: persisted.tenant_id.clone(),
            normalized_at: Some(normalized_at),
            ocsf_version: OCSF_VERSION.to_string(),
            class_uid: mapped.class_uid,
            category_uid: mapped.category_uid,
            severity: mapped.severity,
            activity_id: mapped.activity_id,
            ocsf_event: Some(json_to_prost_struct(&mapped.ocsf_event)?),
            parser_id: parsed.parser_id.clone(),
            parser_version: parsed.parser_version.clone(),
            normalization_status: mapped.status as i32,
            normalized_hash_algorithm: "sha256".to_string(),
            normalized_hash,
            pipeline_version: self.config.pipeline_version.clone(),
        };
        validate_normalized_event(&normalized_event).map_err(|error| NormalizerError {
            code: "CER-NORM-CONTRACT",
            message: error.to_string(),
            retryable: false,
        })?;

        let transformation = Transformation {
            transformation_id,
            input_object_id: persisted.event_id.clone(),
            input_object_type: "RawEvent".to_string(),
            output_object_id: normalized_event_id,
            output_object_type: "NormalizedEvent".to_string(),
            component: format!("cerbero-normalizer:{}", mapped.mapping_id),
            component_version: mapped.mapping_version.clone(),
            configuration_hash,
            started_at: Some(normalized_at),
            completed_at: Some(normalized_at),
            status: TransformationStatus::Success as i32,
            error: None,
            execution_mode: self.config.execution_mode as i32,
        };
        validate_transformation(&transformation).map_err(|error| NormalizerError {
            code: "CER-NORM-TRANSFORMATION-CONTRACT",
            message: error.to_string(),
            retryable: false,
        })?;

        Ok(NormalizationPlan {
            logical_key,
            resolved_event_time: event_time.event_time,
            mapping_id: mapped.mapping_id,
            mapping_version: mapped.mapping_version,
            canonical_ocsf_json,
            normalized_event,
            transformation,
        })
    }
}

fn validate_raw_bytes(persisted: &RawEventPersisted, raw: &[u8]) -> Result<(), NormalizerError> {
    if u64::try_from(raw.len()).map_err(|_| invalid("raw byte length exceeds uint64"))?
        != persisted.raw_size
    {
        return Err(NormalizerError {
            code: "CER-NORM-RAW-LENGTH",
            message: "Raw Store byte length does not match RawEventPersisted.raw_size".to_string(),
            retryable: false,
        });
    }
    let raw_hash = sha256_lower_hex(raw);
    if raw_hash != persisted.raw_hash {
        return Err(NormalizerError {
            code: "CER-NORM-RAW-HASH",
            message: "Raw Store SHA-256 does not match RawEventPersisted.raw_hash".to_string(),
            retryable: false,
        });
    }
    Ok(())
}

fn validate_handoff(
    incoming: &CerberoEnvelope,
    persisted: &RawEventPersisted,
) -> Result<(), NormalizerError> {
    if incoming.message_type != "RawEventPersisted" {
        return Err(invalid("incoming message_type must be RawEventPersisted"));
    }
    if incoming.payload_schema != "cerbero.raw_event_persisted.v1" {
        return Err(invalid(
            "incoming payload_schema must be cerbero.raw_event_persisted.v1",
        ));
    }
    if incoming.tenant_id != persisted.tenant_id {
        return Err(invalid(
            "envelope tenant_id differs from RawEventPersisted tenant_id",
        ));
    }
    if persisted.raw_hash_algorithm != "sha256" {
        return Err(invalid(
            "RawEventPersisted.raw_hash_algorithm must be sha256",
        ));
    }
    Ok(())
}

fn invalid(message: &str) -> NormalizerError {
    NormalizerError {
        code: "CER-NORM-INVALID",
        message: message.to_string(),
        retryable: false,
    }
}

fn from_parse(error: crate::parser::ParseError) -> NormalizerError {
    NormalizerError {
        code: error.code,
        message: error.message,
        retryable: error.retryable,
    }
}

fn from_mapping(error: crate::mapping::MappingError) -> NormalizerError {
    NormalizerError {
        code: error.code,
        message: error.message,
        retryable: error.retryable,
    }
}

fn from_source_time(error: SourceTimeError) -> NormalizerError {
    NormalizerError {
        code: "CER-NORM-SOURCE-TIME-POLICY",
        message: error.message,
        retryable: false,
    }
}

fn parse_metric_status(status: &ParsingStatus) -> ParseMetricStatus {
    match status {
        ParsingStatus::Success => ParseMetricStatus::Success,
        ParsingStatus::Partial => ParseMetricStatus::Partial,
        ParsingStatus::Failed => ParseMetricStatus::Failed,
        ParsingStatus::Unsupported => ParseMetricStatus::Unsupported,
    }
}

fn normalization_metric_status(status: NormalizationStatus) -> NormalizationMetricStatus {
    match status {
        NormalizationStatus::Success => NormalizationMetricStatus::Success,
        NormalizationStatus::Partial => NormalizationMetricStatus::Partial,
        NormalizationStatus::Failed | NormalizationStatus::Unspecified => {
            NormalizationMetricStatus::Failed
        }
    }
}

fn timestamp_to_unix_millis(timestamp: &Timestamp) -> Result<i64, NormalizerError> {
    timestamp
        .seconds
        .checked_mul(1000)
        .and_then(|value| value.checked_add(i64::from(timestamp.nanos) / 1_000_000))
        .ok_or_else(|| invalid("timestamp overflows milliseconds"))
}

fn json_to_prost_struct(value: &Value) -> Result<Struct, NormalizerError> {
    let Value::Object(object) = value else {
        return Err(invalid("OCSF event root must be a JSON object"));
    };
    Ok(Struct {
        fields: object
            .iter()
            .map(|(key, value)| Ok((key.clone(), json_to_prost_value(value)?)))
            .collect::<Result<_, NormalizerError>>()?,
    })
}

fn json_to_prost_value(value: &Value) -> Result<prost_types::Value, NormalizerError> {
    let kind = match value {
        Value::Null => Kind::NullValue(0),
        Value::Bool(value) => Kind::BoolValue(*value),
        Value::Number(value) => Kind::NumberValue(value.as_f64().ok_or_else(|| {
            invalid("OCSF JSON number cannot be represented as protobuf Struct number")
        })?),
        Value::String(value) => Kind::StringValue(value.clone()),
        Value::Array(values) => Kind::ListValue(prost_types::ListValue {
            values: values
                .iter()
                .map(json_to_prost_value)
                .collect::<Result<_, _>>()?,
        }),
        Value::Object(object) => Kind::StructValue(Struct {
            fields: object
                .iter()
                .map(|(key, value)| Ok((key.clone(), json_to_prost_value(value)?)))
                .collect::<Result<_, NormalizerError>>()?,
        }),
    };
    Ok(prost_types::Value { kind: Some(kind) })
}

#[cfg(test)]
mod tests {
    use cerbero_common::contracts::sha256_lower_hex;
    use cerbero_common::contracts::v1::{
        CerberoEnvelope, ExecutionMode, NormalizationStatus, RawEventPersisted,
    };
    use prost_types::Timestamp;

    use super::*;

    fn persisted(raw: &[u8]) -> RawEventPersisted {
        RawEventPersisted {
            event_id: "01995000-0000-7000-8000-000000000002".to_string(),
            tenant_id: "tenant-a".to_string(),
            source_id: "source-a".to_string(),
            ingest_time: Some(Timestamp {
                seconds: 1_789_000_001,
                nanos: 0,
            }),
            event_time: Some(Timestamp {
                seconds: 1_789_000_000,
                nanos: 0,
            }),
            raw_size: u64::try_from(raw.len()).expect("test raw length fits u64"),
            raw_hash_algorithm: "sha256".to_string(),
            raw_hash: sha256_lower_hex(raw),
            pipeline_version: "ingest-v1".to_string(),
            storage_uri: "raw:///tenant-a/2026/09/13/12/x/raw.bin".to_string(),
            segment_id: "01995000-0000-7000-8000-000000000002".to_string(),
            length: u64::try_from(raw.len()).expect("test raw length fits u64"),
            persisted_at: Some(Timestamp {
                seconds: 1_789_000_001,
                nanos: 1,
            }),
            ..Default::default()
        }
    }

    fn envelope() -> CerberoEnvelope {
        CerberoEnvelope {
            contract_version: "1".to_string(),
            message_id: "01995000-0000-7000-8000-000000000003".to_string(),
            message_type: "RawEventPersisted".to_string(),
            tenant_id: "tenant-a".to_string(),
            payload_schema: "cerbero.raw_event_persisted.v1".to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn first_vertical_builds_normalized_event_and_provenance() {
        let raw = b"Failed password for invalid user admin from 10.0.0.8 port 50341 ssh2";
        let mut core = NormalizerCore::new(
            NormalizerCoreConfig {
                pipeline_version: "normalizer-v1".to_string(),
                execution_mode: ExecutionMode::Live,
            },
            Box::new(FixedClock(Timestamp {
                seconds: 1_789_000_002,
                nanos: 0,
            })),
            Box::new(FixedIdGenerator::new([
                "01995000-0000-7000-8000-000000000010".to_string(),
                "01995000-0000-7000-8000-000000000011".to_string(),
            ])),
        )
        .unwrap();
        let plan = core.normalize(&envelope(), &persisted(raw), raw).unwrap();
        assert_eq!(plan.normalized_event.class_uid, 3002);
        assert_eq!(plan.normalized_event.category_uid, 3);
        assert_eq!(plan.normalized_event.activity_id, Some(1));
        assert_eq!(
            plan.normalized_event.normalization_status,
            NormalizationStatus::Success as i32
        );
        assert_eq!(plan.normalized_event.normalized_hash_algorithm, "sha256");
        assert_eq!(plan.transformation.input_object_id, persisted(raw).event_id);
        assert_eq!(
            plan.transformation.output_object_id,
            plan.normalized_event.normalized_event_id
        );
        assert_eq!(
            plan.transformation.execution_mode,
            ExecutionMode::Live as i32
        );
        assert_eq!(
            plan.normalized_event.normalized_hash,
            sha256_lower_hex(&plan.canonical_ocsf_json)
        );
    }

    #[test]
    fn logical_identity_changes_for_replay_and_pipeline_change() {
        let raw = b"Failed password for admin from 10.0.0.8 port 22 ssh2";
        let persisted = persisted(raw);
        let live = NormalizerCore::new(
            NormalizerCoreConfig {
                pipeline_version: "v1".to_string(),
                execution_mode: ExecutionMode::Live,
            },
            Box::new(SystemClock),
            Box::new(FixedIdGenerator::default()),
        )
        .unwrap();
        let replay = NormalizerCore::new(
            NormalizerCoreConfig {
                pipeline_version: "v1".to_string(),
                execution_mode: ExecutionMode::Replay,
            },
            Box::new(SystemClock),
            Box::new(FixedIdGenerator::default()),
        )
        .unwrap();
        let next_pipeline = NormalizerCore::new(
            NormalizerCoreConfig {
                pipeline_version: "v2".to_string(),
                execution_mode: ExecutionMode::Live,
            },
            Box::new(SystemClock),
            Box::new(FixedIdGenerator::default()),
        )
        .unwrap();
        assert_ne!(live.logical_key(&persisted), replay.logical_key(&persisted));
        assert_ne!(
            live.logical_key(&persisted),
            next_pipeline.logical_key(&persisted)
        );
    }

    #[test]
    fn parser_v2_creates_a_distinct_historical_derivation_without_rewriting_v1() {
        let raw = b"Failed password for invalid user admin from 10.0.0.8 port 50341 ssh2";
        let persisted = persisted(raw);
        let config = NormalizerCoreConfig {
            pipeline_version: "normalizer-v1".to_string(),
            execution_mode: ExecutionMode::Live,
        };
        let clock = Timestamp {
            seconds: 1_789_000_002,
            nanos: 0,
        };

        let mut v1 = NormalizerCore::new_with_linux_sshd_version(
            config.clone(),
            crate::LINUX_SSHD_PARSER_VERSION,
            Box::new(FixedClock(clock)),
            Box::new(FixedIdGenerator::new([
                "01995000-0000-7000-8000-000000000020".to_string(),
                "01995000-0000-7000-8000-000000000021".to_string(),
            ])),
        )
        .unwrap();
        let mut v2 = NormalizerCore::new_with_linux_sshd_version(
            config,
            crate::LINUX_SSHD_PARSER_V2_VERSION,
            Box::new(FixedClock(clock)),
            Box::new(FixedIdGenerator::new([
                "01995000-0000-7000-8000-000000000022".to_string(),
                "01995000-0000-7000-8000-000000000023".to_string(),
            ])),
        )
        .unwrap();

        let n1 = v1.normalize(&envelope(), &persisted, raw).unwrap();
        let n2 = v2.normalize(&envelope(), &persisted, raw).unwrap();

        assert_eq!(
            n1.normalized_event.raw_event_id,
            n2.normalized_event.raw_event_id
        );
        assert_eq!(n1.normalized_event.parser_version, "1");
        assert_eq!(n2.normalized_event.parser_version, "2");
        assert_eq!(n1.logical_key, v1.logical_key(&persisted));
        assert_ne!(n1.logical_key, n2.logical_key);
        assert_ne!(
            n1.normalized_event.normalized_hash,
            n2.normalized_event.normalized_hash
        );
        assert_ne!(
            n1.normalized_event.normalized_event_id,
            n2.normalized_event.normalized_event_id
        );
    }

    #[test]
    fn parse_only_parser_does_not_create_false_normalization() {
        let raw = br#"{"event":"login"}"#;
        let mut persisted = persisted(raw);
        persisted.content_type = "application/json".to_string();
        let mut core = NormalizerCore::new(
            NormalizerCoreConfig {
                pipeline_version: "normalizer-v1".to_string(),
                execution_mode: ExecutionMode::Live,
            },
            Box::new(SystemClock),
            Box::new(FixedIdGenerator::default()),
        )
        .unwrap();
        let error = core.normalize(&envelope(), &persisted, raw).unwrap_err();
        assert_eq!(error.code, "CER-NORM-MAPPING-UNSUPPORTED");
        assert!(!error.retryable);
    }
}
