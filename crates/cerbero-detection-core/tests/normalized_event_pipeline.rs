use std::collections::BTreeMap;
use std::str::FromStr as _;

use cerbero_common::contracts::v1::NormalizedEvent;
use cerbero_detection_core::{
    BooleanExpression, ComparisonExpression, ComparisonOperator, ComparisonTypePolicy,
    CompiledEventExecutionPlan, DetectionExpression, EventCompilationStages, EventEvaluationResult,
    EventExecutionPlanCompiler, EventFieldProfile, EventFieldProfileResolver, EventFieldState,
    EventFieldView, EventLeafEvaluationPolicy, EventPredicate, ExecutionBackend,
    ExecutionBackendCapabilities, ExecutionPlan, ExistsExpression, FieldTypeResolver,
    IdentityEventAstOptimizer, NormalizedEventFieldView, RuleCompilationErrorKind, RuleId,
    ValueTypeResolver, compile_event_execution_plan, evaluate_event_execution_plan,
};
use prost_types::{Struct, Value, value::Kind};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TestType {
    Text,
}

struct TestFieldResolver;

impl FieldTypeResolver for TestFieldResolver {
    type FieldType = TestType;

    fn resolve_field_type(&self, field: &str) -> Option<Self::FieldType> {
        match field {
            "status" | "user.name" | "src_endpoint.ip" => Some(TestType::Text),
            _ => None,
        }
    }
}

struct TestRuleValueResolver;

impl ValueTypeResolver<&'static str> for TestRuleValueResolver {
    type ValueType = TestType;

    fn resolve_value_type(&self, _value: &&'static str) -> Self::ValueType {
        TestType::Text
    }
}

struct TestComparisonTypePolicy;

impl ComparisonTypePolicy<TestType, TestType> for TestComparisonTypePolicy {
    fn validate_comparison(
        &self,
        field_type: &TestType,
        operator: ComparisonOperator,
        value_type: &TestType,
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

type TestExpression = DetectionExpression<&'static str, &'static str>;
type TestProfile =
    EventFieldProfile<Vec<&'static str>, Vec<&'static str>, Vec<(&'static str, TestType)>>;
type TestPlan = ExecutionPlan<
    &'static str,
    u64,
    Vec<&'static str>,
    EventPredicate<&'static str, &'static str>,
    (),
    (),
    (),
    &'static str,
>;

struct TestProfileResolver;

impl EventFieldProfileResolver<&'static str, &'static str> for TestProfileResolver {
    type Profile = TestProfile;

    fn resolve_event_field_profile(
        &self,
        _expression: &TestExpression,
    ) -> Result<Self::Profile, RuleCompilationErrorKind> {
        Ok(EventFieldProfile::new(
            vec!["status", "user.name"],
            vec!["src_endpoint.ip"],
            vec![
                ("status", TestType::Text),
                ("user.name", TestType::Text),
                ("src_endpoint.ip", TestType::Text),
            ],
        ))
    }
}

struct TestPlanCompiler;

impl EventExecutionPlanCompiler<&'static str, &'static str, TestProfile> for TestPlanCompiler {
    type Plan = TestPlan;

    fn supported_backends(
        &self,
        _predicate: &EventPredicate<&'static str, &'static str>,
        _field_profile: &TestProfile,
    ) -> Result<ExecutionBackendCapabilities, RuleCompilationErrorKind> {
        Ok(ExecutionBackendCapabilities::new(true, true))
    }

    fn compile_event_plan(
        &self,
        backend: ExecutionBackend,
        predicate: &EventPredicate<&'static str, &'static str>,
        field_profile: &TestProfile,
    ) -> Result<Self::Plan, RuleCompilationErrorKind> {
        Ok(ExecutionPlan {
            plan_id: "normalized-event-plan",
            rule_id: RuleId::from_str("CER-DET-000001").expect("valid test rule id"),
            rule_version: 1,
            backend,
            required_fields: field_profile.required_fields().clone(),
            predicates: predicate.clone(),
            grouping: (),
            window: (),
            resource_limits: (),
            plan_hash: "normalized-event-plan-hash",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TestEvaluationError {
    UnsupportedOperator,
    NonStringValue,
}

struct TestLeafPolicy;

impl EventLeafEvaluationPolicy<&'static str, &'static str, Value> for TestLeafPolicy {
    type Error = TestEvaluationError;

    fn evaluate_exists(
        &self,
        _field: &&'static str,
        state: EventFieldState<'_, Value>,
    ) -> Result<bool, Self::Error> {
        Ok(matches!(state, EventFieldState::Present(_)))
    }

    fn evaluate_comparison(
        &self,
        _field: &&'static str,
        state: EventFieldState<'_, Value>,
        operator: ComparisonOperator,
        rule_value: &&'static str,
    ) -> Result<bool, Self::Error> {
        let EventFieldState::Present(actual) = state else {
            return Ok(false);
        };
        let Some(Kind::StringValue(actual)) = actual.kind.as_ref() else {
            return Err(TestEvaluationError::NonStringValue);
        };

        match operator {
            ComparisonOperator::Eq => Ok(actual.as_str() == *rule_value),
            _ => Err(TestEvaluationError::UnsupportedOperator),
        }
    }
}

fn event_expression() -> TestExpression {
    TestExpression::Boolean(BooleanExpression::And(vec![
        TestExpression::Comparison(ComparisonExpression {
            field: "status",
            operator: ComparisonOperator::Eq,
            value: "Failure",
        }),
        TestExpression::Comparison(ComparisonExpression {
            field: "user.name",
            operator: ComparisonOperator::Eq,
            value: "admin",
        }),
        TestExpression::Exists(ExistsExpression {
            field: "src_endpoint.ip",
        }),
    ]))
}

fn compiled_plan() -> CompiledEventExecutionPlan<TestPlan, TestProfile> {
    compile_event_execution_plan(
        &event_expression(),
        ExecutionBackend::Stream,
        &TestFieldResolver,
        &TestRuleValueResolver,
        &TestComparisonTypePolicy,
        &EventCompilationStages::new(
            &IdentityEventAstOptimizer,
            &TestProfileResolver,
            &TestPlanCompiler,
        ),
    )
    .expect("real NormalizedEvent EVENT pipeline should compile")
}

fn string_value(value: &str) -> Value {
    Value {
        kind: Some(Kind::StringValue(value.to_string())),
    }
}

fn null_value() -> Value {
    Value {
        kind: Some(Kind::NullValue(0)),
    }
}

fn struct_value(entries: &[(&str, Value)]) -> Value {
    Value {
        kind: Some(Kind::StructValue(Struct {
            fields: entries
                .iter()
                .map(|(key, value)| ((*key).to_string(), value.clone()))
                .collect(),
        })),
    }
}

fn normalized_event(
    status: &str,
    user_name: &str,
    source_endpoint: Option<Value>,
) -> NormalizedEvent {
    let mut fields = BTreeMap::from([
        ("status".to_string(), string_value(status)),
        (
            "user".to_string(),
            struct_value(&[("name", string_value(user_name))]),
        ),
    ]);
    if let Some(source_endpoint) = source_endpoint {
        fields.insert("src_endpoint".to_string(), source_endpoint);
    }

    NormalizedEvent {
        normalized_event_id: "018f47a2-4b00-7a00-8000-000000000001".to_string(),
        raw_event_id: "018f47a2-4b00-7a00-8000-000000000002".to_string(),
        tenant_id: "tenant-test".to_string(),
        ocsf_version: "1.9.0".to_string(),
        class_uid: 3002,
        category_uid: 3,
        severity: Some(2),
        activity_id: Some(1),
        ocsf_event: Some(Struct { fields }),
        parser_id: "linux/sshd".to_string(),
        parser_version: "1".to_string(),
        pipeline_version: "test".to_string(),
        ..Default::default()
    }
}

fn source_endpoint(ip: &str) -> Value {
    struct_value(&[("ip", string_value(ip))])
}

#[test]
fn normalized_event_field_view_preserves_present_absent_null_and_malformed() {
    let positive = normalized_event("Failure", "admin", Some(source_endpoint("10.0.0.8")));
    let positive_view = NormalizedEventFieldView::new(&positive);
    let status = "status";
    let missing = "actor.name";

    let EventFieldState::Present(status_value) = positive_view.field_state(&status) else {
        panic!("status must be present");
    };
    assert!(matches!(
        status_value.kind.as_ref(),
        Some(Kind::StringValue(value)) if value == "Failure"
    ));
    assert!(matches!(
        positive_view.field_state(&missing),
        EventFieldState::Absent
    ));

    let null = normalized_event("Failure", "admin", Some(null_value()));
    let null_view = NormalizedEventFieldView::new(&null);
    let source_ip = "src_endpoint.ip";
    assert!(matches!(
        null_view.field_state(&source_ip),
        EventFieldState::Null
    ));

    let malformed = normalized_event("Failure", "admin", Some(string_value("10.0.0.8")));
    let malformed_view = NormalizedEventFieldView::new(&malformed);
    assert!(matches!(
        malformed_view.field_state(&source_ip),
        EventFieldState::Malformed
    ));
}

#[test]
fn real_normalized_event_pipeline_matches_positive_fixture() {
    let event = normalized_event("Failure", "admin", Some(source_endpoint("10.0.0.8")));
    let view = NormalizedEventFieldView::new(&event);

    assert_eq!(
        evaluate_event_execution_plan(&compiled_plan().plan, &view, &TestLeafPolicy),
        Ok(EventEvaluationResult::Match)
    );
}

#[test]
fn real_normalized_event_pipeline_rejects_negative_fixture() {
    let event = normalized_event("Failure", "root", Some(source_endpoint("10.0.0.8")));
    let view = NormalizedEventFieldView::new(&event);

    assert_eq!(
        evaluate_event_execution_plan(&compiled_plan().plan, &view, &TestLeafPolicy),
        Ok(EventEvaluationResult::NoMatch)
    );
}

#[test]
fn real_normalized_event_pipeline_handles_non_present_source_ip_explicitly() {
    let cases = [
        normalized_event("Failure", "admin", None),
        normalized_event("Failure", "admin", Some(null_value())),
        normalized_event("Failure", "admin", Some(string_value("10.0.0.8"))),
    ];

    for event in &cases {
        let view = NormalizedEventFieldView::new(event);
        assert_eq!(
            evaluate_event_execution_plan(&compiled_plan().plan, &view, &TestLeafPolicy),
            Ok(EventEvaluationResult::NoMatch)
        );
    }
}
