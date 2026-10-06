#![allow(clippy::module_name_repetitions)]
#![allow(clippy::too_many_lines)]

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt,
    str::FromStr as _,
};

use cerbero_common::contracts::{
    sha256_lower_hex,
    v1::{ExecutionMode, Finding, NormalizedEvent, Signal, SignalInputType},
    validate_timestamp, validate_uuid_v7,
};
use cerbero_detection_core::{
    AutomatedTestCoverage, BooleanExpression, ComparisonExpression, ComparisonOperator,
    ComparisonTypePolicy, CompiledEventExecutionPlan, CompiledThresholdExecutionPlan,
    CorrelationFindingMaterializationContext, DetectionExpression, EventCompilationStages,
    EventExecutionPlanCompiler, EventFieldProfile, EventFieldProfileResolver, EventFieldState,
    EventFieldView, EventLeafEvaluationPolicy, EventPredicate, EventSignalEvaluationResult,
    EventSignalMaterializationContext, ExecutionBackend, ExecutionBackendCapabilities,
    ExecutionPlan, FieldTypeResolver, IdentityEventAstOptimizer, RuleCompilationErrorKind, RuleId,
    SequenceCorrelationEvaluationPolicy, SequenceCorrelationRule, ThresholdEvaluationPolicy,
    ThresholdSignalMaterializationContext, ThresholdTimeBasis, ValueTypeResolver,
    compile_event_execution_plan, compile_sequence_correlation_plan,
    compile_threshold_execution_plan, evaluate_normalized_event_for_signal,
    evaluate_sequence_signals, evaluate_threshold_normalized_events, materialize_event_signal,
    materialize_sequence_correlation_finding, materialize_threshold_signal,
    parse_sigma_threshold_rule, validate_rule_status,
};
use prost_types::{Struct, Timestamp, Value, value::Kind};
use reqwest::Client;
use serde::Deserialize;
use uuid::Uuid;

use crate::{PostgresAnalyticalRepository, RuntimeError};

const FAILED_LOGIN_RULE_ID: &str = "CER-DET-000101";
const SUCCESSFUL_LOGIN_RULE_ID: &str = "CER-DET-000102";
const PRIVILEGE_ESCALATION_RULE_ID: &str = "CER-DET-000103";
const THRESHOLD_FAILED_LOGIN_RULE_ID: &str = "CER-DET-000001";
const THRESHOLD_SIGMA_SOURCE: &str =
    include_str!("../../../rules/sigma/linux_sshd_failed_login_burst.yml");
const ACCOUNT_COMPROMISE_RULE_ID: &str = "CER-COR-0003";
const RULE_VERSION: &str = "1";
const CORRELATION_RULE_VERSION: &str = "2";
const MAX_BATCH_EVENTS: u64 = 10_000;

type MvpExpression = DetectionExpression<String, String>;
type MvpExecutionPlan =
    ExecutionPlan<String, String, Vec<String>, EventPredicate<String, String>, (), (), (), String>;
type MvpProfile = EventFieldProfile<Vec<String>, Vec<String>, BTreeMap<String, MvpFieldType>>;
type MvpCompiledEventPlan = CompiledEventExecutionPlan<MvpExecutionPlan, MvpProfile>;
type MvpCompiledThresholdPlan =
    CompiledThresholdExecutionPlan<MvpExecutionPlan, MvpProfile, String>;

/// Configuration for the bounded `ClickHouse` source used by the MVP batch runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MvpClickHouseConfig {
    pub endpoint: String,
    pub database: String,
    pub user: String,
    pub password: String,
}

/// One deterministic bounded analytical execution request.
#[derive(Debug, Clone, PartialEq)]
pub struct MvpBatchRequest {
    pub tenant_id: String,
    pub execution_mode: ExecutionMode,
    pub start_millis: i64,
    pub end_millis: i64,
    pub max_events: u64,
    pub evaluated_at: Timestamp,
    pub created_at: Timestamp,
}

/// Canonical durable analytical outputs returned by one bounded execution.
#[derive(Debug, Clone, PartialEq)]
pub struct MvpBatchResult {
    pub events_loaded: usize,
    pub signals: Vec<Signal>,
    pub findings: Vec<Finding>,
}

/// Bounded MVP analytical runtime.
///
/// This runtime deliberately executes deterministic ClickHouse/batch semantics.
/// Streaming state, watermarks, checkpoints, JOIN, ABSENCE, and generic
/// aggregation remain outside Step 30.
pub struct MvpBatchRuntime {
    source: ClickHouseNormalizedEventSource,
    repository: PostgresAnalyticalRepository,
}

impl MvpBatchRuntime {
    /// Connects the bounded `ClickHouse` source and durable PostgreSQL repository.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] when configuration is invalid or PostgreSQL
    /// cannot be reached.
    pub async fn connect(
        clickhouse: MvpClickHouseConfig,
        postgres_connection_string: &str,
        component_version: String,
        instance_id: String,
    ) -> Result<Self, RuntimeError> {
        let source = ClickHouseNormalizedEventSource::new(clickhouse)?;
        let repository = PostgresAnalyticalRepository::connect(
            postgres_connection_string,
            component_version,
            instance_id,
        )
        .await?;

        Ok(Self { source, repository })
    }

    /// Executes the frozen Step 30 MVP rule pack over one bounded dataset.
    ///
    /// EVENT rules materialize canonical Signals, the failed-login THRESHOLD
    /// materializes one Signal per qualifying group, and the account-compromise
    /// SEQUENCE materializes canonical Findings. Every result is made durable
    /// through [`PostgresAnalyticalRepository`] before it is returned.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] when request validation, `ClickHouse` access,
    /// rule compilation/evaluation/materialization, or durable persistence fails.
    pub async fn execute(&self, request: &MvpBatchRequest) -> Result<MvpBatchResult, RuntimeError> {
        validate_request(request)?;

        let events = self.source.load(request).await?;
        let policy = MvpPolicy;

        let failed_plan = compile_event_rule(
            FAILED_LOGIN_RULE_ID,
            "mvp-failed-login-v1",
            &failed_login_expression(),
        )?;
        let success_plan = compile_event_rule(
            SUCCESSFUL_LOGIN_RULE_ID,
            "mvp-successful-login-v1",
            &successful_login_expression(),
        )?;
        let privilege_plan = compile_event_rule(
            PRIVILEGE_ESCALATION_RULE_ID,
            "mvp-privilege-escalation-v1",
            &privilege_escalation_expression(),
        )?;

        let mut durable_signals = Vec::new();
        let mut sequence_signals = Vec::new();

        for batch_event in &events {
            for (plan, presentation) in [
                (
                    &failed_plan,
                    EventPresentation {
                        severity: "MEDIUM",
                        summary: "Failed login matched",
                    },
                ),
                (
                    &success_plan,
                    EventPresentation {
                        severity: "LOW",
                        summary: "Successful login matched",
                    },
                ),
                (
                    &privilege_plan,
                    EventPresentation {
                        severity: "HIGH",
                        summary: "Privilege escalation matched",
                    },
                ),
            ] {
                let EventSignalEvaluationResult::Match(provenance) =
                    evaluate_normalized_event_for_signal(
                        &plan.plan,
                        &batch_event.event,
                        request.execution_mode,
                        request.evaluated_at,
                        &policy,
                    )
                    .map_err(|error| runtime(format!("EVENT evaluation failed: {error}")))?
                else {
                    continue;
                };

                let signal = materialize_event_signal(
                    &provenance,
                    &batch_event.event,
                    EventSignalMaterializationContext {
                        signal_id: Uuid::now_v7().to_string(),
                        severity: presentation.severity.to_string(),
                        confidence: None,
                        summary: presentation.summary.to_string(),
                        input_relation: "EVENT_MATCH".to_string(),
                        created_at: request.created_at,
                        source_event_time: batch_event.source_event_time,
                    },
                )
                .map_err(|error| runtime(format!("EVENT materialization failed: {error}")))?;

                let durable = self.repository.persist_signal(&signal).await?;
                sequence_signals.push(durable.clone());
                durable_signals.push(durable);
            }
        }

        let threshold_plan = compile_threshold_rule()?;
        let normalized_events = events
            .iter()
            .map(|batch_event| batch_event.event.clone())
            .collect::<Vec<_>>();
        let threshold_matches =
            evaluate_threshold_normalized_events::<String, String, String, _, _, _>(
                &threshold_plan,
                &normalized_events,
                request.execution_mode,
                request.evaluated_at,
                &policy,
            )
            .map_err(|error| runtime(format!("THRESHOLD evaluation failed: {error}")))?;

        for provenance in threshold_matches {
            let signal = materialize_threshold_signal(
                &provenance,
                ThresholdSignalMaterializationContext {
                    signal_id: Uuid::now_v7().to_string(),
                    severity: "HIGH".to_string(),
                    confidence: None,
                    summary: "SSH authentication burst: 10 failed logins within 5 minutes"
                        .to_string(),
                    input_relation: "THRESHOLD_INPUT".to_string(),
                    created_at: request.created_at,
                    source_count: 1,
                },
            )
            .map_err(|error| runtime(format!("THRESHOLD materialization failed: {error}")))?;

            durable_signals.push(self.repository.persist_signal(&signal).await?);
        }

        let sequence_plan = compile_sequence_rule()?;
        let sequence_policy = MvpSequencePolicy::new(&events)?;
        let sequence_matches = evaluate_sequence_signals(
            &sequence_plan,
            &sequence_signals,
            request.execution_mode,
            &sequence_policy,
        )
        .map_err(|error| runtime(format!("SEQUENCE evaluation failed: {error}")))?;

        let mut durable_findings = Vec::new();
        for correlation_match in sequence_matches {
            let finding = materialize_sequence_correlation_finding(
                &correlation_match,
                CorrelationFindingMaterializationContext {
                    finding_id: Uuid::now_v7().to_string(),
                    severity: "HIGH".to_string(),
                    confidence: Some("MEDIUM".to_string()),
                    title: "Account compromise sequence".to_string(),
                    description:
                        "Failed login followed by successful login and privilege escalation"
                            .to_string(),
                    input_relation: "SEQUENCE_INPUT".to_string(),
                    created_at: request.created_at,
                },
            )
            .map_err(|error| runtime(format!("SEQUENCE materialization failed: {error}")))?;

            durable_findings.push(self.repository.persist_finding(&finding).await?);
        }

        durable_signals.sort_by(|left, right| left.signal_id.cmp(&right.signal_id));
        durable_findings.sort_by(|left, right| left.finding_id.cmp(&right.finding_id));

        Ok(MvpBatchResult {
            events_loaded: events.len(),
            signals: durable_signals,
            findings: durable_findings,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
struct BatchEvent {
    event: NormalizedEvent,
    source_event_time: Option<Timestamp>,
}

#[derive(Debug, Deserialize)]
struct StoredNormalizedRow {
    tenant_id: String,
    normalized_event_id: String,
    raw_event_id: String,
    event_time_present: u8,
    event_time_seconds: i64,
    event_time_nanos: i32,
    normalized_at_seconds: i64,
    normalized_at_nanos: i32,
    ocsf_version: String,
    class_uid: u32,
    category_uid: u32,
    activity_id: Option<u32>,
    severity: Option<u32>,
    parser_id: String,
    parser_version: String,
    normalization_status: i32,
    normalized_hash_algorithm: String,
    normalized_hash: String,
    pipeline_version: String,
    ocsf_event_json: String,
}

struct ClickHouseNormalizedEventSource {
    client: Client,
    config: MvpClickHouseConfig,
}

impl ClickHouseNormalizedEventSource {
    fn new(config: MvpClickHouseConfig) -> Result<Self, RuntimeError> {
        if !valid_identifier(&config.database) {
            return Err(runtime("ClickHouse database must be a simple identifier"));
        }
        if config.endpoint.trim().is_empty()
            || config.user.trim().is_empty()
            || config.password.is_empty()
        {
            return Err(runtime(
                "ClickHouse endpoint, user, and password are required",
            ));
        }

        Ok(Self {
            client: Client::new(),
            config,
        })
    }

    async fn load(&self, request: &MvpBatchRequest) -> Result<Vec<BatchEvent>, RuntimeError> {
        let query = format!(
            "SELECT tenant_id, toString(normalized_event_id) AS normalized_event_id, \
             toString(raw_event_id) AS raw_event_id, event_time_present, event_time_seconds, \
             event_time_nanos, normalized_at_seconds, normalized_at_nanos, ocsf_version, \
             class_uid, category_uid, activity_id, severity, parser_id, parser_version, \
             toInt32(normalization_status) AS normalization_status, normalized_hash_algorithm, \
             normalized_hash, pipeline_version, ocsf_event_json \
             FROM {}.normalized_events \
             WHERE tenant_id = {{tenant:String}} \
               AND execution_mode = {{mode:UInt8}} \
               AND event_time_present = 1 \
               AND (event_time_seconds * 1000 + intDiv(event_time_nanos, 1000000)) >= {{start:Int64}} \
               AND (event_time_seconds * 1000 + intDiv(event_time_nanos, 1000000)) <= {{end:Int64}} \
             ORDER BY event_time_seconds, event_time_nanos, normalized_event_id \
             LIMIT {{limit:UInt64}} FORMAT JSONEachRow",
            self.config.database
        );

        let mode = u8::try_from(request.execution_mode as i32)
            .map_err(|_| runtime("execution mode does not fit ClickHouse UInt8"))?;

        let response = self
            .client
            .get(&self.config.endpoint)
            .basic_auth(&self.config.user, Some(&self.config.password))
            .query(&[
                ("query", query),
                ("param_tenant", request.tenant_id.clone()),
                ("param_mode", mode.to_string()),
                ("param_start", request.start_millis.to_string()),
                ("param_end", request.end_millis.to_string()),
                ("param_limit", request.max_events.to_string()),
            ])
            .send()
            .await
            .map_err(|error| runtime(format!("ClickHouse transport failed: {error}")))?;

        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|error| runtime(format!("ClickHouse response read failed: {error}")))?;

        if !status.is_success() {
            return Err(runtime(format!(
                "ClickHouse query failed with HTTP {}: {}",
                status.as_u16(),
                body.trim()
            )));
        }

        let mut unique = BTreeMap::<String, BatchEvent>::new();
        for line in body.lines().filter(|line| !line.trim().is_empty()) {
            let row: StoredNormalizedRow = serde_json::from_str(line)
                .map_err(|error| runtime(format!("ClickHouse row decode failed: {error}")))?;
            let batch_event = row.into_batch_event()?;
            let event_id = batch_event.event.normalized_event_id.clone();

            if let Some(existing) = unique.get(&event_id) {
                if existing != &batch_event {
                    return Err(runtime(format!(
                        "conflicting duplicate NormalizedEvent identity {event_id}"
                    )));
                }
                continue;
            }

            unique.insert(event_id, batch_event);
        }

        let mut events = unique.into_values().collect::<Vec<_>>();
        events.sort_by(|left, right| {
            timestamp_millis(left.source_event_time.as_ref())
                .cmp(&timestamp_millis(right.source_event_time.as_ref()))
                .then_with(|| {
                    left.event
                        .normalized_event_id
                        .cmp(&right.event.normalized_event_id)
                })
        });

        Ok(events)
    }
}

impl StoredNormalizedRow {
    fn into_batch_event(self) -> Result<BatchEvent, RuntimeError> {
        let ocsf: serde_json::Value = serde_json::from_str(&self.ocsf_event_json)
            .map_err(|error| runtime(format!("stored OCSF JSON decode failed: {error}")))?;

        let source_event_time = match self.event_time_present {
            0 => None,
            1 => Some(Timestamp {
                seconds: self.event_time_seconds,
                nanos: self.event_time_nanos,
            }),
            other => {
                return Err(runtime(format!(
                    "event_time_present must be 0 or 1, observed {other}"
                )));
            }
        };

        if let Some(timestamp) = source_event_time.as_ref() {
            validate_timestamp("event_time", timestamp)
                .map_err(|error| runtime(format!("invalid stored event_time: {error}")))?;
        }

        Ok(BatchEvent {
            event: NormalizedEvent {
                normalized_event_id: self.normalized_event_id,
                raw_event_id: self.raw_event_id,
                tenant_id: self.tenant_id,
                normalized_at: Some(Timestamp {
                    seconds: self.normalized_at_seconds,
                    nanos: self.normalized_at_nanos,
                }),
                ocsf_version: self.ocsf_version,
                class_uid: self.class_uid,
                category_uid: self.category_uid,
                severity: self.severity,
                activity_id: self.activity_id,
                ocsf_event: Some(json_to_prost_struct(&ocsf)?),
                parser_id: self.parser_id,
                parser_version: self.parser_version,
                normalization_status: self.normalization_status,
                normalized_hash_algorithm: self.normalized_hash_algorithm,
                normalized_hash: self.normalized_hash,
                pipeline_version: self.pipeline_version,
            },
            source_event_time,
        })
    }
}

fn validate_request(request: &MvpBatchRequest) -> Result<(), RuntimeError> {
    validate_uuid_v7("tenant_id", &request.tenant_id)
        .map_err(|error| runtime(format!("invalid tenant_id: {error}")))?;
    validate_timestamp("evaluated_at", &request.evaluated_at)
        .map_err(|error| runtime(format!("invalid evaluated_at: {error}")))?;
    validate_timestamp("created_at", &request.created_at)
        .map_err(|error| runtime(format!("invalid created_at: {error}")))?;

    if request.execution_mode == ExecutionMode::Unspecified {
        return Err(runtime("execution_mode must be LIVE, REPLAY, or TEST"));
    }
    if request.start_millis > request.end_millis {
        return Err(runtime("batch start_millis must be <= end_millis"));
    }
    if request.max_events == 0 || request.max_events > MAX_BATCH_EVENTS {
        return Err(runtime(format!(
            "max_events must be between 1 and {MAX_BATCH_EVENTS}"
        )));
    }

    Ok(())
}

fn valid_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn json_to_prost_struct(value: &serde_json::Value) -> Result<Struct, RuntimeError> {
    let serde_json::Value::Object(object) = value else {
        return Err(runtime("stored OCSF event root must be an object"));
    };

    Ok(Struct {
        fields: object
            .iter()
            .map(|(key, value)| Ok((key.clone(), json_to_prost_value(value)?)))
            .collect::<Result<_, RuntimeError>>()?,
    })
}

fn json_to_prost_value(value: &serde_json::Value) -> Result<Value, RuntimeError> {
    let kind = match value {
        serde_json::Value::Null => Kind::NullValue(0),
        serde_json::Value::Bool(value) => Kind::BoolValue(*value),
        serde_json::Value::Number(value) => Kind::NumberValue(
            value
                .as_f64()
                .ok_or_else(|| runtime("stored OCSF number is outside protobuf Struct range"))?,
        ),
        serde_json::Value::String(value) => Kind::StringValue(value.clone()),
        serde_json::Value::Array(values) => Kind::ListValue(prost_types::ListValue {
            values: values
                .iter()
                .map(json_to_prost_value)
                .collect::<Result<_, RuntimeError>>()?,
        }),
        serde_json::Value::Object(object) => Kind::StructValue(Struct {
            fields: object
                .iter()
                .map(|(key, value)| Ok((key.clone(), json_to_prost_value(value)?)))
                .collect::<Result<_, RuntimeError>>()?,
        }),
    };
    Ok(Value { kind: Some(kind) })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MvpFieldType {
    Text,
    Number,
}

struct MvpFieldResolver;

impl FieldTypeResolver for MvpFieldResolver {
    type FieldType = MvpFieldType;

    fn resolve_field_type(&self, field: &str) -> Option<Self::FieldType> {
        match field {
            "activity_name" | "status" | "user.name" | "src_endpoint.ip" => {
                Some(MvpFieldType::Text)
            }
            "time" => Some(MvpFieldType::Number),
            _ => None,
        }
    }
}

struct MvpValueResolver;

impl ValueTypeResolver<String> for MvpValueResolver {
    type ValueType = MvpFieldType;

    fn resolve_value_type(&self, _value: &String) -> Self::ValueType {
        MvpFieldType::Text
    }
}

struct MvpComparisonPolicy;

impl ComparisonTypePolicy<MvpFieldType, MvpFieldType> for MvpComparisonPolicy {
    fn validate_comparison(
        &self,
        field_type: &MvpFieldType,
        operator: ComparisonOperator,
        value_type: &MvpFieldType,
    ) -> Result<(), RuleCompilationErrorKind> {
        if field_type != value_type {
            return Err(RuleCompilationErrorKind::TypeError);
        }
        if operator != ComparisonOperator::Eq {
            return Err(RuleCompilationErrorKind::InvalidOperator);
        }
        Ok(())
    }
}

struct MvpFieldProfileResolver;

impl EventFieldProfileResolver<String, String> for MvpFieldProfileResolver {
    type Profile = MvpProfile;

    fn resolve_event_field_profile(
        &self,
        expression: &MvpExpression,
    ) -> Result<Self::Profile, RuleCompilationErrorKind> {
        let mut fields = BTreeSet::new();
        collect_expression_fields(expression, &mut fields);
        let required_fields = fields.into_iter().collect::<Vec<_>>();
        let mut field_types = BTreeMap::new();

        for field in &required_fields {
            let field_type = MvpFieldResolver
                .resolve_field_type(field)
                .ok_or(RuleCompilationErrorKind::UnknownField)?;
            field_types.insert(field.clone(), field_type);
        }

        Ok(EventFieldProfile::new(
            required_fields,
            Vec::<String>::new(),
            field_types,
        ))
    }
}

fn collect_expression_fields(expression: &MvpExpression, fields: &mut BTreeSet<String>) {
    match expression {
        DetectionExpression::Boolean(
            BooleanExpression::And(children) | BooleanExpression::Or(children),
        ) => {
            for child in children {
                collect_expression_fields(child, fields);
            }
        }
        DetectionExpression::Boolean(BooleanExpression::Not(child)) => {
            collect_expression_fields(child, fields);
        }
        DetectionExpression::Comparison(comparison) => {
            fields.insert(comparison.field.clone());
        }
        DetectionExpression::Exists(exists) => {
            fields.insert(exists.field.clone());
        }
    }
}

struct MvpPlanCompiler {
    plan_id: String,
    rule_id: String,
    rule_version: String,
    plan_hash: String,
}

impl EventExecutionPlanCompiler<String, String, MvpProfile> for MvpPlanCompiler {
    type Plan = MvpExecutionPlan;

    fn supported_backends(
        &self,
        _predicate: &EventPredicate<String, String>,
        _field_profile: &MvpProfile,
    ) -> Result<ExecutionBackendCapabilities, RuleCompilationErrorKind> {
        Ok(ExecutionBackendCapabilities::new(true, false))
    }

    fn compile_event_plan(
        &self,
        backend: ExecutionBackend,
        predicate: &EventPredicate<String, String>,
        field_profile: &MvpProfile,
    ) -> Result<Self::Plan, RuleCompilationErrorKind> {
        Ok(ExecutionPlan {
            plan_id: self.plan_id.clone(),
            rule_id: RuleId::from_str(&self.rule_id)
                .map_err(|_| RuleCompilationErrorKind::InvalidMapping)?,
            rule_version: self.rule_version.clone(),
            backend,
            required_fields: field_profile.required_fields().clone(),
            predicates: predicate.clone(),
            grouping: (),
            window: (),
            resource_limits: (),
            plan_hash: self.plan_hash.clone(),
        })
    }
}

fn compile_event_rule(
    rule_id: &str,
    plan_name: &str,
    expression: &MvpExpression,
) -> Result<MvpCompiledEventPlan, RuntimeError> {
    let optimizer = IdentityEventAstOptimizer;
    let profile = MvpFieldProfileResolver;
    let compiler = MvpPlanCompiler {
        plan_id: plan_name.to_string(),
        rule_id: rule_id.to_string(),
        rule_version: RULE_VERSION.to_string(),
        plan_hash: sha256_lower_hex(plan_name.as_bytes()),
    };
    let stages = EventCompilationStages::new(&optimizer, &profile, &compiler);

    compile_event_execution_plan(
        expression,
        ExecutionBackend::ClickHouse,
        &MvpFieldResolver,
        &MvpValueResolver,
        &MvpComparisonPolicy,
        &stages,
    )
    .map_err(|error| {
        runtime(format!(
            "EVENT rule {rule_id} compilation failed: {}",
            error.as_str()
        ))
    })
}

fn compile_threshold_rule() -> Result<MvpCompiledThresholdPlan, RuntimeError> {
    let imported = parse_sigma_threshold_rule(THRESHOLD_SIGMA_SOURCE)
        .map_err(|error| runtime(format!("Sigma THRESHOLD import failed: {error}")))?;
    if imported.rule_id.as_str() != THRESHOLD_FAILED_LOGIN_RULE_ID {
        return Err(runtime(format!(
            "Sigma THRESHOLD identity drift: expected {THRESHOLD_FAILED_LOGIN_RULE_ID}, observed {}",
            imported.rule_id
        )));
    }
    validate_rule_status(imported.status, AutomatedTestCoverage::complete())
        .map_err(|error| runtime(format!("Sigma STABLE rule contract failed: {error}")))?;

    let optimizer = IdentityEventAstOptimizer;
    let profile = MvpFieldProfileResolver;
    let compiler = MvpPlanCompiler {
        plan_id: "sigma-linux-sshd-failed-login-burst-v1".to_string(),
        rule_id: imported.rule_id.to_string(),
        rule_version: imported.rule_version,
        plan_hash: sha256_lower_hex(
            format!("sigma-threshold-clickhouse-v1:{}", imported.content_hash).as_bytes(),
        ),
    };
    let stages = EventCompilationStages::new(&optimizer, &profile, &compiler);

    compile_threshold_execution_plan(
        &imported.threshold_rule,
        ExecutionBackend::ClickHouse,
        &MvpFieldResolver,
        &MvpValueResolver,
        &MvpComparisonPolicy,
        &stages,
    )
    .map_err(|error| {
        runtime(format!(
            "THRESHOLD rule compilation failed: {}",
            error.as_str()
        ))
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MvpSequenceOrdering {
    Observed,
}

fn compile_sequence_rule() -> Result<
    cerbero_detection_core::CompiledSequenceCorrelationPlan<String, String, MvpSequenceOrdering>,
    RuntimeError,
> {
    let rule = SequenceCorrelationRule {
        correlation_rule_id: ACCOUNT_COMPROMISE_RULE_ID.to_string(),
        correlation_rule_version: CORRELATION_RULE_VERSION.to_string(),
        stages: vec![
            FAILED_LOGIN_RULE_ID.to_string(),
            SUCCESSFUL_LOGIN_RULE_ID.to_string(),
            PRIVILEGE_ESCALATION_RULE_ID.to_string(),
        ],
        join_keys: vec!["user.name".to_string()],
        max_window_millis: 600_000,
        ordering: MvpSequenceOrdering::Observed,
        configuration_hash: sha256_lower_hex(b"mvp-account-compromise-sequence-v1"),
    };

    compile_sequence_correlation_plan(&rule, ExecutionBackend::ClickHouse).map_err(|error| {
        runtime(format!(
            "SEQUENCE rule compilation failed: {}",
            error.as_str()
        ))
    })
}

fn failed_login_expression() -> MvpExpression {
    and(vec![eq("activity_name", "Logon"), eq("status", "Failure")])
}

fn successful_login_expression() -> MvpExpression {
    and(vec![eq("activity_name", "Logon"), eq("status", "Success")])
}

fn privilege_escalation_expression() -> MvpExpression {
    eq("activity_name", "Privilege Escalation")
}

fn and(children: Vec<MvpExpression>) -> MvpExpression {
    DetectionExpression::Boolean(BooleanExpression::And(children))
}

fn eq(field: &str, value: &str) -> MvpExpression {
    DetectionExpression::Comparison(ComparisonExpression {
        field: field.to_string(),
        operator: ComparisonOperator::Eq,
        value: value.to_string(),
    })
}

#[derive(Debug)]
struct MvpPolicyError(String);

impl fmt::Display for MvpPolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for MvpPolicyError {}

struct MvpPolicy;

impl EventLeafEvaluationPolicy<String, String, Value> for MvpPolicy {
    type Error = MvpPolicyError;

    fn evaluate_exists(
        &self,
        _field: &String,
        state: EventFieldState<'_, Value>,
    ) -> Result<bool, Self::Error> {
        Ok(matches!(state, EventFieldState::Present(_)))
    }

    fn evaluate_comparison(
        &self,
        field: &String,
        state: EventFieldState<'_, Value>,
        operator: ComparisonOperator,
        rule_value: &String,
    ) -> Result<bool, Self::Error> {
        let EventFieldState::Present(actual) = state else {
            return Ok(false);
        };
        let Some(Kind::StringValue(actual)) = actual.kind.as_ref() else {
            return Err(MvpPolicyError(format!(
                "field {field} is not a string for MVP comparison"
            )));
        };

        match operator {
            ComparisonOperator::Eq => Ok(actual == rule_value),
            _ => Err(MvpPolicyError(format!(
                "operator {} is unsupported by the MVP leaf policy",
                operator.as_str()
            ))),
        }
    }
}

impl ThresholdEvaluationPolicy<String, String> for MvpPolicy {
    type GroupKey = Vec<String>;

    fn threshold_group_key(
        &self,
        group_by: &[String],
        event: &NormalizedEvent,
    ) -> Result<Option<Self::GroupKey>, Self::Error> {
        let mut key = Vec::with_capacity(group_by.len());
        for field in group_by {
            let Some(value) = event_string_field(event, field)? else {
                return Ok(None);
            };
            key.push(value);
        }
        Ok(Some(key))
    }

    fn threshold_event_time_millis(
        &self,
        time_basis: ThresholdTimeBasis,
        time_field: &String,
        event: &NormalizedEvent,
    ) -> Result<Option<i64>, Self::Error> {
        if time_basis != ThresholdTimeBasis::EventTime {
            return Err(MvpPolicyError(
                "MVP batch threshold supports EVENT_TIME only".to_string(),
            ));
        }
        event_integer_field(event, time_field)
    }
}

struct MvpSequencePolicy {
    user_by_event_id: HashMap<String, String>,
}

impl MvpSequencePolicy {
    fn new(events: &[BatchEvent]) -> Result<Self, RuntimeError> {
        let mut user_by_event_id = HashMap::new();
        for batch_event in events {
            if let Some(user) = event_string_field(&batch_event.event, "user.name")
                .map_err(|error| runtime(format!("user extraction failed: {error}")))?
            {
                user_by_event_id.insert(batch_event.event.normalized_event_id.clone(), user);
            }
        }
        Ok(Self { user_by_event_id })
    }
}

impl SequenceCorrelationEvaluationPolicy<String, String, MvpSequenceOrdering>
    for MvpSequencePolicy
{
    type GroupKey = String;
    type OrderKey = (i64, String);
    type Error = MvpPolicyError;

    fn stage_matches(&self, stage: &String, signal: &Signal) -> Result<bool, Self::Error> {
        Ok(signal.rule_id == *stage)
    }

    fn sequence_group_key(
        &self,
        join_keys: &[String],
        signal: &Signal,
    ) -> Result<Option<Self::GroupKey>, Self::Error> {
        if join_keys.len() != 1 || join_keys[0] != "user.name" {
            return Err(MvpPolicyError(
                "MVP sequence requires join key user.name".to_string(),
            ));
        }

        let Some(input) = signal
            .inputs
            .iter()
            .find(|input| input.input_type == SignalInputType::NormalizedEvent as i32)
        else {
            return Ok(None);
        };

        Ok(self.user_by_event_id.get(&input.input_id).cloned())
    }

    fn sequence_time_millis(&self, signal: &Signal) -> Result<Option<i64>, Self::Error> {
        Ok(signal
            .first_observed_at
            .as_ref()
            .and_then(|timestamp| timestamp_to_millis_checked(timestamp).ok()))
    }

    fn sequence_order_key(
        &self,
        ordering: &MvpSequenceOrdering,
        signal: &Signal,
    ) -> Result<Option<Self::OrderKey>, Self::Error> {
        if *ordering != MvpSequenceOrdering::Observed {
            return Err(MvpPolicyError(
                "unsupported MVP sequence ordering".to_string(),
            ));
        }

        Ok(self
            .sequence_time_millis(signal)?
            .map(|millis| (millis, signal.signal_id.clone())))
    }
}

fn event_string_field(
    event: &NormalizedEvent,
    field: &str,
) -> Result<Option<String>, MvpPolicyError> {
    let field = field.to_string();
    let view = cerbero_detection_core::NormalizedEventFieldView::new(event);
    match view.field_state(&field) {
        EventFieldState::Present(value) => match value.kind.as_ref() {
            Some(Kind::StringValue(value)) => Ok(Some(value.clone())),
            _ => Err(MvpPolicyError(format!("field {field} is not a string"))),
        },
        EventFieldState::Absent | EventFieldState::Null => Ok(None),
        EventFieldState::Malformed => Err(MvpPolicyError(format!("field {field} is malformed"))),
    }
}

fn event_integer_field(
    event: &NormalizedEvent,
    field: &str,
) -> Result<Option<i64>, MvpPolicyError> {
    let field = field.to_string();
    let view = cerbero_detection_core::NormalizedEventFieldView::new(event);
    match view.field_state(&field) {
        EventFieldState::Present(value) => {
            let Some(Kind::NumberValue(number)) = value.kind.as_ref() else {
                return Err(MvpPolicyError(format!("field {field} is not numeric")));
            };
            if !number.is_finite()
                || number.fract().abs() > f64::EPSILON
                || number.abs() > 9_007_199_254_740_991.0
            {
                return Err(MvpPolicyError(format!(
                    "field {field} is not an integral i64-compatible number"
                )));
            }
            #[allow(clippy::cast_possible_truncation)]
            let integer = *number as i64;
            Ok(Some(integer))
        }
        EventFieldState::Absent | EventFieldState::Null => Ok(None),
        EventFieldState::Malformed => Err(MvpPolicyError(format!("field {field} is malformed"))),
    }
}

fn timestamp_to_millis_checked(timestamp: &Timestamp) -> Result<i64, MvpPolicyError> {
    timestamp
        .seconds
        .checked_mul(1_000)
        .and_then(|millis| millis.checked_add(i64::from(timestamp.nanos / 1_000_000)))
        .ok_or_else(|| MvpPolicyError("timestamp milliseconds overflow i64".to_string()))
}

fn timestamp_millis(timestamp: Option<&Timestamp>) -> i64 {
    timestamp
        .and_then(|value| timestamp_to_millis_checked(value).ok())
        .unwrap_or(i64::MIN)
}

#[derive(Clone, Copy)]
struct EventPresentation {
    severity: &'static str,
    summary: &'static str,
}

fn runtime(message: impl Into<String>) -> RuntimeError {
    RuntimeError(message.into())
}

#[cfg(test)]
mod stable_sigma_tests {
    use std::{
        collections::{BTreeMap, HashMap},
        fs,
        path::PathBuf,
    };

    use cerbero_common::contracts::v1::{ExecutionMode, NormalizedEvent};
    use prost_types::{Struct, Timestamp, Value, value::Kind};
    use serde::Deserialize;
    use uuid::Uuid;

    use super::{
        MvpPolicy, THRESHOLD_FAILED_LOGIN_RULE_ID, compile_threshold_rule,
        evaluate_threshold_normalized_events,
    };

    #[derive(Debug, Deserialize)]
    struct FixtureEvent {
        event_id: String,
        user: Option<String>,
        source_ip: Option<String>,
        activity_name: String,
        status: String,
        event_time_ms: serde_json::Value,
    }

    fn fixture_path(relative: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/detections/CER-DET-000001")
            .join(relative)
    }

    fn load_fixture(relative: &str) -> Vec<FixtureEvent> {
        fs::read_to_string(fixture_path(relative))
            .expect("fixture must be readable")
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| serde_json::from_str(line).expect("fixture JSON must be valid"))
            .collect()
    }

    fn prost_string(value: String) -> Value {
        Value {
            kind: Some(Kind::StringValue(value)),
        }
    }

    fn prost_number(value: f64) -> Value {
        Value {
            kind: Some(Kind::NumberValue(value)),
        }
    }

    fn prost_struct(fields: BTreeMap<String, Value>) -> Value {
        Value {
            kind: Some(Kind::StructValue(Struct { fields })),
        }
    }

    fn materialize_fixture(fixture: Vec<FixtureEvent>, tenant_id: &str) -> Vec<NormalizedEvent> {
        let mut logical_ids = HashMap::<String, String>::new();

        fixture
            .into_iter()
            .map(|row| {
                let normalized_event_id = logical_ids
                    .entry(row.event_id)
                    .or_insert_with(|| Uuid::now_v7().to_string())
                    .clone();

                let mut fields = BTreeMap::new();
                fields.insert("activity_name".to_string(), prost_string(row.activity_name));
                fields.insert("status".to_string(), prost_string(row.status));

                if let Some(user) = row.user {
                    let mut nested = BTreeMap::new();
                    nested.insert("name".to_string(), prost_string(user));
                    fields.insert("user".to_string(), prost_struct(nested));
                }
                if let Some(source_ip) = row.source_ip {
                    let mut nested = BTreeMap::new();
                    nested.insert("ip".to_string(), prost_string(source_ip));
                    fields.insert("src_endpoint".to_string(), prost_struct(nested));
                }

                let time = match row.event_time_ms {
                    serde_json::Value::Number(number) => {
                        prost_number(number.as_f64().expect("fixture time must fit f64"))
                    }
                    serde_json::Value::String(value) => prost_string(value),
                    other => panic!("unsupported fixture time value: {other:?}"),
                };
                fields.insert("time".to_string(), time);

                NormalizedEvent {
                    normalized_event_id,
                    raw_event_id: Uuid::now_v7().to_string(),
                    tenant_id: tenant_id.to_string(),
                    normalized_at: Some(Timestamp {
                        seconds: 1_789_000_001,
                        nanos: 0,
                    }),
                    ocsf_version: "1.9.0".to_string(),
                    class_uid: 3002,
                    category_uid: 3,
                    severity: Some(2),
                    activity_id: Some(1),
                    ocsf_event: Some(Struct { fields }),
                    parser_id: "stable-sigma-test".to_string(),
                    parser_version: "1".to_string(),
                    normalization_status: 1,
                    normalized_hash_algorithm: "sha256".to_string(),
                    normalized_hash: "0".repeat(64),
                    pipeline_version: "step33-stable-sigma".to_string(),
                }
            })
            .collect()
    }

    fn evaluate(
        relative: &str,
    ) -> Result<Vec<cerbero_detection_core::ThresholdSignalProvenance<String>>, String> {
        let tenant_id = Uuid::now_v7().to_string();
        let events = materialize_fixture(load_fixture(relative), &tenant_id);
        let plan = compile_threshold_rule().map_err(|error| error.to_string())?;

        evaluate_threshold_normalized_events(
            &plan,
            &events,
            ExecutionMode::Replay,
            Timestamp {
                seconds: 1_789_000_600,
                nanos: 0,
            },
            &MvpPolicy,
        )
        .map_err(|error| error.to_string())
    }

    #[test]
    fn stable_sigma_positive_and_negative_datasets_execute_real_threshold_plan() {
        let positive =
            evaluate("positive/ten_failed.jsonl").expect("positive fixture must execute");
        assert_eq!(positive.len(), 1);
        assert_eq!(positive[0].rule_id.as_str(), THRESHOLD_FAILED_LOGIN_RULE_ID);
        assert_eq!(positive[0].rule_version, "1");
        assert_eq!(positive[0].normalized_event_ids.len(), 10);

        let negative =
            evaluate("negative/nine_failed.jsonl").expect("negative fixture must execute");
        assert!(negative.is_empty());
    }

    #[test]
    fn stable_sigma_duplicate_out_of_order_and_late_inputs_are_deterministic() {
        for fixture in [
            "edge_cases/duplicates.jsonl",
            "edge_cases/out_of_order.jsonl",
            "edge_cases/late_events.jsonl",
        ] {
            let matches = evaluate(fixture).expect("deterministic edge fixture must execute");
            assert_eq!(matches.len(), 1, "unexpected result for {fixture}");
            assert_eq!(
                matches[0].normalized_event_ids.len(),
                10,
                "unexpected contributor count for {fixture}"
            );
        }
    }

    #[test]
    fn stable_sigma_missing_group_field_does_not_false_positive() {
        let matches = evaluate("edge_cases/missing_fields.jsonl")
            .expect("missing field fixture must execute");
        assert!(matches.is_empty());
    }

    #[test]
    fn stable_sigma_unicode_group_key_is_deterministic() {
        let matches = evaluate("edge_cases/strange_encoding.jsonl")
            .expect("Unicode fixture must execute deterministically");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].normalized_event_ids.len(), 10);
    }

    #[test]
    fn stable_sigma_invalid_timestamp_fails_closed() {
        let error = evaluate("edge_cases/invalid_timestamp.jsonl")
            .expect_err("invalid timestamp must fail closed");
        assert!(error.contains("field time is not numeric"), "{error}");
    }
}
