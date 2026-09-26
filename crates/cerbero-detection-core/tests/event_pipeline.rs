use std::collections::HashMap;
use std::str::FromStr as _;

use cerbero_detection_core::{
    BooleanExpression, ComparisonExpression, ComparisonOperator, ComparisonTypePolicy,
    CompiledEventExecutionPlan, DetectionExpression, EventCompilationStages, EventEvaluationResult,
    EventExecutionPlanCompiler, EventFieldProfile, EventFieldProfileResolver, EventFieldState,
    EventFieldView, EventLeafEvaluationPolicy, EventPredicate, ExecutionBackend,
    ExecutionBackendCapabilities, ExecutionPlan, FieldTypeResolver, IdentityEventAstOptimizer,
    RuleCompilationErrorKind, RuleId, ValueTypeResolver, compile_event_execution_plan,
    evaluate_event_execution_plan,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TestType {
    Text,
}

struct TestFieldResolver;

impl FieldTypeResolver for TestFieldResolver {
    type FieldType = TestType;

    fn resolve_field_type(&self, field: &str) -> Option<Self::FieldType> {
        match field {
            "process.name" | "process.command_line" => Some(TestType::Text),
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

        if !matches!(
            operator,
            ComparisonOperator::Eq | ComparisonOperator::Contains
        ) {
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
            vec!["process.name"],
            vec!["process.command_line"],
            vec![
                ("process.name", TestType::Text),
                ("process.command_line", TestType::Text),
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
            plan_id: "public-api-event-plan",
            rule_id: RuleId::from_str("CER-DET-000001").expect("valid test rule id"),
            rule_version: 1,
            backend,
            required_fields: field_profile.required_fields().clone(),
            predicates: predicate.clone(),
            grouping: (),
            window: (),
            resource_limits: (),
            plan_hash: "public-api-event-plan-hash",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TestEventValue {
    Text(&'static str),
    Null,
    Malformed,
}

struct TestEvent {
    fields: HashMap<&'static str, TestEventValue>,
}

impl EventFieldView<&'static str> for TestEvent {
    type Value = TestEventValue;

    fn field_state(&self, field: &&'static str) -> EventFieldState<'_, Self::Value> {
        match self.fields.get(field) {
            Some(TestEventValue::Null) => EventFieldState::Null,
            Some(TestEventValue::Malformed) => EventFieldState::Malformed,
            Some(value) => EventFieldState::Present(value),
            None => EventFieldState::Absent,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TestEvaluationError {
    UnsupportedOperator,
}

struct TestLeafPolicy;

impl EventLeafEvaluationPolicy<&'static str, &'static str, TestEventValue> for TestLeafPolicy {
    type Error = TestEvaluationError;

    fn evaluate_exists(
        &self,
        _field: &&'static str,
        state: EventFieldState<'_, TestEventValue>,
    ) -> Result<bool, Self::Error> {
        Ok(matches!(state, EventFieldState::Present(_)))
    }

    fn evaluate_comparison(
        &self,
        _field: &&'static str,
        state: EventFieldState<'_, TestEventValue>,
        operator: ComparisonOperator,
        rule_value: &&'static str,
    ) -> Result<bool, Self::Error> {
        let EventFieldState::Present(TestEventValue::Text(actual)) = state else {
            return Ok(false);
        };

        match operator {
            ComparisonOperator::Eq => Ok(actual == rule_value),
            ComparisonOperator::Contains => Ok(actual.contains(*rule_value)),
            _ => Err(TestEvaluationError::UnsupportedOperator),
        }
    }
}

fn event_expression() -> TestExpression {
    TestExpression::Boolean(BooleanExpression::And(vec![
        TestExpression::Comparison(ComparisonExpression {
            field: "process.name",
            operator: ComparisonOperator::Eq,
            value: "powershell.exe",
        }),
        TestExpression::Comparison(ComparisonExpression {
            field: "process.command_line",
            operator: ComparisonOperator::Contains,
            value: "-EncodedCommand",
        }),
    ]))
}

fn compiled_plan() -> CompiledEventExecutionPlan<TestPlan, TestProfile> {
    compile_event_execution_plan(
        &event_expression(),
        ExecutionBackend::ClickHouse,
        &TestFieldResolver,
        &TestRuleValueResolver,
        &TestComparisonTypePolicy,
        &EventCompilationStages::new(
            &IdentityEventAstOptimizer,
            &TestProfileResolver,
            &TestPlanCompiler,
        ),
    )
    .expect("public EVENT pipeline should compile")
}

#[test]
fn public_event_pipeline_matches_positive_fixture() {
    let event = TestEvent {
        fields: HashMap::from([
            ("process.name", TestEventValue::Text("powershell.exe")),
            (
                "process.command_line",
                TestEventValue::Text("powershell.exe -EncodedCommand AAAA"),
            ),
        ]),
    };

    assert_eq!(
        evaluate_event_execution_plan(&compiled_plan().plan, &event, &TestLeafPolicy),
        Ok(EventEvaluationResult::Match)
    );
}

#[test]
fn public_event_pipeline_rejects_negative_fixture() {
    let event = TestEvent {
        fields: HashMap::from([
            ("process.name", TestEventValue::Text("powershell.exe")),
            (
                "process.command_line",
                TestEventValue::Text("powershell.exe -NoProfile"),
            ),
        ]),
    };

    assert_eq!(
        evaluate_event_execution_plan(&compiled_plan().plan, &event, &TestLeafPolicy),
        Ok(EventEvaluationResult::NoMatch)
    );
}

#[test]
fn public_event_pipeline_handles_non_present_edge_states_explicitly() {
    let cases = [
        TestEvent {
            fields: HashMap::from([("process.name", TestEventValue::Text("powershell.exe"))]),
        },
        TestEvent {
            fields: HashMap::from([
                ("process.name", TestEventValue::Text("powershell.exe")),
                ("process.command_line", TestEventValue::Null),
            ]),
        },
        TestEvent {
            fields: HashMap::from([
                ("process.name", TestEventValue::Text("powershell.exe")),
                ("process.command_line", TestEventValue::Malformed),
            ]),
        },
    ];

    for event in cases {
        assert_eq!(
            evaluate_event_execution_plan(&compiled_plan().plan, &event, &TestLeafPolicy),
            Ok(EventEvaluationResult::NoMatch)
        );
    }
}
