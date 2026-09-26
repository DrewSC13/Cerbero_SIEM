use crate::{
    BooleanExpression, ComparisonTypePolicy, DetectionExpression, EventAstOptimizer,
    EventPredicate, ExecutionBackend, ExecutionBackendCapabilities, FieldTypeResolver,
    RuleCompilationErrorKind, ValueTypeResolver, type_check_expression, validate_expression,
};

/// Schema-owned field profile required before an EVENT execution plan is materialized.
///
/// The concrete collection and field-type representations remain generic so this
/// crate does not freeze the OCSF/Cerbero field taxonomy or optional-field policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventFieldProfile<RequiredFields, OptionalFields, FieldTypes> {
    required_fields: RequiredFields,
    optional_fields: OptionalFields,
    field_types: FieldTypes,
}

impl<RequiredFields, OptionalFields, FieldTypes>
    EventFieldProfile<RequiredFields, OptionalFields, FieldTypes>
{
    #[must_use]
    pub const fn new(
        required_fields: RequiredFields,
        optional_fields: OptionalFields,
        field_types: FieldTypes,
    ) -> Self {
        Self {
            required_fields,
            optional_fields,
            field_types,
        }
    }

    #[must_use]
    pub const fn required_fields(&self) -> &RequiredFields {
        &self.required_fields
    }

    #[must_use]
    pub const fn optional_fields(&self) -> &OptionalFields {
        &self.optional_fields
    }

    #[must_use]
    pub const fn field_types(&self) -> &FieldTypes {
        &self.field_types
    }
}

/// Contract implemented by field profiles supplied to EVENT plan compilation.
pub trait EventFieldProfileContract {
    type RequiredFields;
    type OptionalFields;
    type FieldTypes;

    fn required_fields(&self) -> &Self::RequiredFields;
    fn optional_fields(&self) -> &Self::OptionalFields;
    fn field_types(&self) -> &Self::FieldTypes;
}

impl<RequiredFields, OptionalFields, FieldTypes> EventFieldProfileContract
    for EventFieldProfile<RequiredFields, OptionalFields, FieldTypes>
{
    type RequiredFields = RequiredFields;
    type OptionalFields = OptionalFields;
    type FieldTypes = FieldTypes;

    fn required_fields(&self) -> &Self::RequiredFields {
        &self.required_fields
    }

    fn optional_fields(&self) -> &Self::OptionalFields {
        &self.optional_fields
    }

    fn field_types(&self) -> &Self::FieldTypes {
        &self.field_types
    }
}

/// Schema/mapping-owned resolution of required fields, optional fields, and types.
pub trait EventFieldProfileResolver<Field, Value> {
    type Profile: EventFieldProfileContract;

    /// Resolves the field profile needed before an EVENT execution plan exists.
    ///
    /// # Errors
    ///
    /// Returns a frozen [`RuleCompilationErrorKind`] when the mapping/schema
    /// cannot provide a valid field profile for the expression.
    fn resolve_event_field_profile(
        &self,
        expression: &DetectionExpression<Field, Value>,
    ) -> Result<Self::Profile, RuleCompilationErrorKind>;
}

/// Backend/schema-owned materialization boundary for a compiled EVENT predicate.
pub trait EventExecutionPlanCompiler<Field, Value, Profile> {
    type Plan;

    /// Declares which frozen v1 backends can preserve this EVENT predicate.
    ///
    /// # Errors
    ///
    /// Returns a frozen [`RuleCompilationErrorKind`] when capability resolution
    /// itself cannot be completed deterministically.
    fn supported_backends(
        &self,
        predicate: &EventPredicate<Field, Value>,
        field_profile: &Profile,
    ) -> Result<ExecutionBackendCapabilities, RuleCompilationErrorKind>;

    /// Materializes an EVENT execution plan after validation, type checking,
    /// field profiling, and semantics-preserving predicate lowering.
    ///
    /// # Errors
    ///
    /// Returns a frozen [`RuleCompilationErrorKind`] when plan construction or
    /// backend capability validation fails.
    fn compile_event_plan(
        &self,
        backend: ExecutionBackend,
        predicate: &EventPredicate<Field, Value>,
        field_profile: &Profile,
    ) -> Result<Self::Plan, RuleCompilationErrorKind>;
}

/// Result of compiling an EVENT AST into an execution plan and its field profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledEventExecutionPlan<Plan, Profile> {
    pub plan: Plan,
    pub field_profile: Profile,
    pub supported_backends: ExecutionBackendCapabilities,
}

/// Ordered post-type-check stages for EVENT compilation.
///
/// Grouping these stage dependencies keeps the public compile API compact while
/// preserving explicit optimizer, field-profile, and planner boundaries.
pub struct EventCompilationStages<'a, Optimizer, ProfileResolver, PlanCompiler> {
    optimizer: &'a Optimizer,
    profile_resolver: &'a ProfileResolver,
    plan_compiler: &'a PlanCompiler,
}

impl<'a, Optimizer, ProfileResolver, PlanCompiler>
    EventCompilationStages<'a, Optimizer, ProfileResolver, PlanCompiler>
{
    #[must_use]
    pub const fn new(
        optimizer: &'a Optimizer,
        profile_resolver: &'a ProfileResolver,
        plan_compiler: &'a PlanCompiler,
    ) -> Self {
        Self {
            optimizer,
            profile_resolver,
            plan_compiler,
        }
    }
}

fn lower_event_predicate<Field: Clone, Value: Clone>(
    expression: &DetectionExpression<Field, Value>,
) -> EventPredicate<Field, Value> {
    match expression {
        DetectionExpression::Boolean(boolean) => match boolean {
            BooleanExpression::And(children) => {
                EventPredicate::And(children.iter().map(lower_event_predicate).collect())
            }
            BooleanExpression::Or(children) => {
                EventPredicate::Or(children.iter().map(lower_event_predicate).collect())
            }
            BooleanExpression::Not(child) => {
                EventPredicate::Not(Box::new(lower_event_predicate(child)))
            }
        },
        DetectionExpression::Comparison(comparison) => EventPredicate::Comparison {
            field: comparison.field.clone(),
            operator: comparison.operator,
            value: comparison.value.clone(),
        },
        DetectionExpression::Exists(exists) => EventPredicate::Exists {
            field: exists.field.clone(),
        },
    }
}

/// Compiles an already parsed EVENT AST through the validation/type-check boundary.
///
/// This function does not parse Sigma or Cerbero Native syntax. It accepts the
/// internal AST, rejects invalid structure, performs schema-owned type checking,
/// passes the validated AST through the explicit optimizer boundary, revalidates
/// the optimized AST, resolves required/optional fields and field types, and only
/// then delegates backend capability resolution and execution-plan materialization.
/// Structural AST failures map to the frozen `SYNTAX_ERROR` category because v1 defines no
/// separate structural-validation compilation category.
///
/// # Errors
///
/// Returns [`RuleCompilationErrorKind::SyntaxError`] for an invalid AST structure,
/// propagates field/type/operator, field-profile, and capability-resolution
/// failures. Returns [`RuleCompilationErrorKind::UnsupportedBackend`] before plan
/// materialization when the selected backend cannot preserve the predicate.
pub fn compile_event_execution_plan<
    Field,
    Value,
    FieldResolver,
    ValueResolver,
    Policy,
    Optimizer,
    ProfileResolver,
    PlanCompiler,
>(
    expression: &DetectionExpression<Field, Value>,
    backend: ExecutionBackend,
    field_resolver: &FieldResolver,
    value_resolver: &ValueResolver,
    policy: &Policy,
    stages: &EventCompilationStages<'_, Optimizer, ProfileResolver, PlanCompiler>,
) -> Result<
    CompiledEventExecutionPlan<PlanCompiler::Plan, ProfileResolver::Profile>,
    RuleCompilationErrorKind,
>
where
    Field: AsRef<str> + Clone,
    Value: Clone,
    FieldResolver: FieldTypeResolver,
    ValueResolver: ValueTypeResolver<Value>,
    Policy: ComparisonTypePolicy<FieldResolver::FieldType, ValueResolver::ValueType>,
    Optimizer: EventAstOptimizer<Field, Value>,
    ProfileResolver: EventFieldProfileResolver<Field, Value>,
    PlanCompiler: EventExecutionPlanCompiler<Field, Value, ProfileResolver::Profile>,
{
    validate_expression(expression).map_err(|_| RuleCompilationErrorKind::SyntaxError)?;
    type_check_expression(expression, field_resolver, value_resolver, policy)?;

    let optimized_expression = stages.optimizer.optimize_event_ast(expression.clone())?;
    validate_expression(&optimized_expression)
        .map_err(|_| RuleCompilationErrorKind::SyntaxError)?;
    type_check_expression(
        &optimized_expression,
        field_resolver,
        value_resolver,
        policy,
    )?;

    let field_profile = stages
        .profile_resolver
        .resolve_event_field_profile(&optimized_expression)?;
    let predicate = lower_event_predicate(&optimized_expression);
    let supported_backends = stages
        .plan_compiler
        .supported_backends(&predicate, &field_profile)?;
    if !supported_backends.supports(backend) {
        return Err(RuleCompilationErrorKind::UnsupportedBackend);
    }
    let plan = stages
        .plan_compiler
        .compile_event_plan(backend, &predicate, &field_profile)?;

    Ok(CompiledEventExecutionPlan {
        plan,
        field_profile,
        supported_backends,
    })
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::HashMap;
    use std::str::FromStr as _;

    use crate::{
        BooleanExpression, ComparisonExpression, ComparisonOperator, DetectionExpression,
        EventAstOptimizer, EventPredicate, ExecutionBackend, ExecutionBackendCapabilities,
        ExecutionPlan, ExistsExpression, FieldTypeResolver, RuleCompilationErrorKind, RuleId,
    };

    use super::{
        ComparisonTypePolicy, EventCompilationStages, EventExecutionPlanCompiler,
        EventFieldProfile, EventFieldProfileResolver, ValueTypeResolver,
        compile_event_execution_plan,
    };

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum TestType {
        Number,
        Text,
    }

    struct TestFieldResolver {
        fields: HashMap<&'static str, TestType>,
    }

    impl FieldTypeResolver for TestFieldResolver {
        type FieldType = TestType;

        fn resolve_field_type(&self, field: &str) -> Option<Self::FieldType> {
            self.fields.get(field).copied()
        }
    }

    struct TestValueResolver;

    impl ValueTypeResolver<&'static str> for TestValueResolver {
        type ValueType = TestType;

        fn resolve_value_type(&self, value: &&'static str) -> Self::ValueType {
            if value.parse::<u64>().is_ok() {
                TestType::Number
            } else {
                TestType::Text
            }
        }
    }

    struct TestComparisonPolicy;

    impl ComparisonTypePolicy<TestType, TestType> for TestComparisonPolicy {
        fn validate_comparison(
            &self,
            field_type: &TestType,
            operator: ComparisonOperator,
            value_type: &TestType,
        ) -> Result<(), RuleCompilationErrorKind> {
            if operator == ComparisonOperator::Contains && *field_type != TestType::Text {
                return Err(RuleCompilationErrorKind::InvalidOperator);
            }
            if field_type != value_type {
                return Err(RuleCompilationErrorKind::TypeError);
            }
            Ok(())
        }
    }

    type Expression = DetectionExpression<&'static str, &'static str>;
    type TestProfile =
        EventFieldProfile<Vec<&'static str>, Vec<&'static str>, Vec<(&'static str, TestType)>>;
    type TestPlan = ExecutionPlan<
        &'static str,
        &'static str,
        Vec<&'static str>,
        EventPredicate<&'static str, &'static str>,
        (),
        (),
        (),
        &'static str,
    >;

    struct TestOptimizer {
        calls: Cell<u32>,
        failure: Option<RuleCompilationErrorKind>,
    }

    impl EventAstOptimizer<&'static str, &'static str> for TestOptimizer {
        fn optimize_event_ast(
            &self,
            expression: Expression,
        ) -> Result<Expression, RuleCompilationErrorKind> {
            self.calls.set(self.calls.get() + 1);
            if let Some(failure) = self.failure {
                Err(failure)
            } else {
                Ok(expression)
            }
        }
    }

    fn optimizer() -> TestOptimizer {
        TestOptimizer {
            calls: Cell::new(0),
            failure: None,
        }
    }

    struct TestProfileResolver {
        calls: Cell<u32>,
    }

    impl EventFieldProfileResolver<&'static str, &'static str> for TestProfileResolver {
        type Profile = TestProfile;

        fn resolve_event_field_profile(
            &self,
            _expression: &Expression,
        ) -> Result<Self::Profile, RuleCompilationErrorKind> {
            self.calls.set(self.calls.get() + 1);
            Ok(EventFieldProfile::new(
                vec!["process.name"],
                vec!["user.name"],
                vec![
                    ("process.name", TestType::Text),
                    ("user.name", TestType::Text),
                ],
            ))
        }
    }

    struct TestPlanCompiler {
        calls: Cell<u32>,
    }

    impl EventExecutionPlanCompiler<&'static str, &'static str, TestProfile> for TestPlanCompiler {
        type Plan = TestPlan;

        fn supported_backends(
            &self,
            _predicate: &EventPredicate<&'static str, &'static str>,
            _field_profile: &TestProfile,
        ) -> Result<ExecutionBackendCapabilities, RuleCompilationErrorKind> {
            Ok(ExecutionBackendCapabilities::new(true, false))
        }

        fn compile_event_plan(
            &self,
            backend: ExecutionBackend,
            predicate: &EventPredicate<&'static str, &'static str>,
            field_profile: &TestProfile,
        ) -> Result<Self::Plan, RuleCompilationErrorKind> {
            self.calls.set(self.calls.get() + 1);

            Ok(ExecutionPlan {
                plan_id: "opaque-event-plan",
                rule_id: RuleId::from_str("CER-DET-000001").expect("valid rule id"),
                rule_version: "opaque-rule-version",
                backend,
                required_fields: field_profile.required_fields().clone(),
                predicates: predicate.clone(),
                grouping: (),
                window: (),
                resource_limits: (),
                plan_hash: "opaque-plan-hash",
            })
        }
    }

    fn field_resolver() -> TestFieldResolver {
        TestFieldResolver {
            fields: HashMap::from([
                ("process.name", TestType::Text),
                ("process.pid", TestType::Number),
                ("user.name", TestType::Text),
            ]),
        }
    }

    fn valid_expression() -> Expression {
        Expression::Boolean(BooleanExpression::And(vec![
            Expression::Comparison(ComparisonExpression {
                field: "process.name",
                operator: ComparisonOperator::Eq,
                value: "powershell.exe",
            }),
            Expression::Exists(ExistsExpression { field: "user.name" }),
        ]))
    }

    #[test]
    fn valid_event_ast_materializes_plan_with_field_profile() {
        let profile_resolver = TestProfileResolver {
            calls: Cell::new(0),
        };
        let plan_builder = TestPlanCompiler {
            calls: Cell::new(0),
        };
        let optimizer = optimizer();

        let compiled_event = compile_event_execution_plan(
            &valid_expression(),
            ExecutionBackend::ClickHouse,
            &field_resolver(),
            &TestValueResolver,
            &TestComparisonPolicy,
            &EventCompilationStages::new(&optimizer, &profile_resolver, &plan_builder),
        )
        .expect("valid EVENT plan");

        assert_eq!(optimizer.calls.get(), 1);
        assert_eq!(profile_resolver.calls.get(), 1);
        assert_eq!(plan_builder.calls.get(), 1);
        assert_eq!(compiled_event.plan.rule_id.as_str(), "CER-DET-000001");
        assert_eq!(compiled_event.plan.backend, ExecutionBackend::ClickHouse);
        assert!(
            compiled_event
                .supported_backends
                .supports(ExecutionBackend::ClickHouse)
        );
        assert!(
            !compiled_event
                .supported_backends
                .supports(ExecutionBackend::Stream)
        );
        assert_eq!(compiled_event.plan.required_fields, vec!["process.name"]);
        assert_eq!(
            compiled_event.plan.predicates,
            EventPredicate::And(vec![
                EventPredicate::Comparison {
                    field: "process.name",
                    operator: ComparisonOperator::Eq,
                    value: "powershell.exe",
                },
                EventPredicate::Exists { field: "user.name" },
            ])
        );
        assert_eq!(
            compiled_event.field_profile.optional_fields(),
            &vec!["user.name"]
        );
        assert_eq!(
            compiled_event.field_profile.field_types(),
            &vec![
                ("process.name", TestType::Text),
                ("user.name", TestType::Text),
            ]
        );
    }

    #[test]
    fn event_ast_lowering_preserves_nested_boolean_semantics() {
        let profile_resolver = TestProfileResolver {
            calls: Cell::new(0),
        };
        let plan_builder = TestPlanCompiler {
            calls: Cell::new(0),
        };
        let optimizer = optimizer();
        let expression = Expression::Boolean(BooleanExpression::Or(vec![
            Expression::Exists(ExistsExpression { field: "user.name" }),
            Expression::Boolean(BooleanExpression::Not(Box::new(Expression::Comparison(
                ComparisonExpression {
                    field: "process.name",
                    operator: ComparisonOperator::Eq,
                    value: "cmd.exe",
                },
            )))),
        ]));

        let compiled_event = compile_event_execution_plan(
            &expression,
            ExecutionBackend::ClickHouse,
            &field_resolver(),
            &TestValueResolver,
            &TestComparisonPolicy,
            &EventCompilationStages::new(&optimizer, &profile_resolver, &plan_builder),
        )
        .expect("valid nested EVENT plan");

        assert_eq!(optimizer.calls.get(), 1);
        assert_eq!(
            compiled_event.plan.predicates,
            EventPredicate::Or(vec![
                EventPredicate::Exists { field: "user.name" },
                EventPredicate::Not(Box::new(EventPredicate::Comparison {
                    field: "process.name",
                    operator: ComparisonOperator::Eq,
                    value: "cmd.exe",
                })),
            ])
        );
    }

    #[test]
    fn invalid_structure_fails_before_profile_or_plan_materialization() {
        let profile_resolver = TestProfileResolver {
            calls: Cell::new(0),
        };
        let compiler = TestPlanCompiler {
            calls: Cell::new(0),
        };
        let optimizer = optimizer();
        let expression = Expression::Boolean(BooleanExpression::And(Vec::new()));

        assert_eq!(
            compile_event_execution_plan(
                &expression,
                ExecutionBackend::ClickHouse,
                &field_resolver(),
                &TestValueResolver,
                &TestComparisonPolicy,
                &EventCompilationStages::new(&optimizer, &profile_resolver, &compiler),
            ),
            Err(RuleCompilationErrorKind::SyntaxError)
        );
        assert_eq!(optimizer.calls.get(), 0);
        assert_eq!(profile_resolver.calls.get(), 0);
        assert_eq!(compiler.calls.get(), 0);
    }

    #[test]
    fn type_failure_fails_before_profile_or_plan_materialization() {
        let profile_resolver = TestProfileResolver {
            calls: Cell::new(0),
        };
        let compiler = TestPlanCompiler {
            calls: Cell::new(0),
        };
        let optimizer = optimizer();
        let expression = Expression::Comparison(ComparisonExpression {
            field: "process.pid",
            operator: ComparisonOperator::Eq,
            value: "powershell.exe",
        });

        assert_eq!(
            compile_event_execution_plan(
                &expression,
                ExecutionBackend::ClickHouse,
                &field_resolver(),
                &TestValueResolver,
                &TestComparisonPolicy,
                &EventCompilationStages::new(&optimizer, &profile_resolver, &compiler),
            ),
            Err(RuleCompilationErrorKind::TypeError)
        );
        assert_eq!(optimizer.calls.get(), 0);
        assert_eq!(profile_resolver.calls.get(), 0);
        assert_eq!(compiler.calls.get(), 0);
    }

    #[test]
    fn unknown_field_fails_before_profile_or_plan_materialization() {
        let profile_resolver = TestProfileResolver {
            calls: Cell::new(0),
        };
        let compiler = TestPlanCompiler {
            calls: Cell::new(0),
        };
        let optimizer = optimizer();
        let expression = Expression::Exists(ExistsExpression {
            field: "unknown.field",
        });

        assert_eq!(
            compile_event_execution_plan(
                &expression,
                ExecutionBackend::ClickHouse,
                &field_resolver(),
                &TestValueResolver,
                &TestComparisonPolicy,
                &EventCompilationStages::new(&optimizer, &profile_resolver, &compiler),
            ),
            Err(RuleCompilationErrorKind::UnknownField)
        );
        assert_eq!(optimizer.calls.get(), 0);
        assert_eq!(profile_resolver.calls.get(), 0);
        assert_eq!(compiler.calls.get(), 0);
    }

    #[test]
    fn optimizer_failure_fails_before_profile_or_plan_materialization() {
        let profile_resolver = TestProfileResolver {
            calls: Cell::new(0),
        };
        let compiler = TestPlanCompiler {
            calls: Cell::new(0),
        };
        let optimizer = TestOptimizer {
            calls: Cell::new(0),
            failure: Some(RuleCompilationErrorKind::ResourcePolicyViolation),
        };

        assert_eq!(
            compile_event_execution_plan(
                &valid_expression(),
                ExecutionBackend::ClickHouse,
                &field_resolver(),
                &TestValueResolver,
                &TestComparisonPolicy,
                &EventCompilationStages::new(&optimizer, &profile_resolver, &compiler),
            ),
            Err(RuleCompilationErrorKind::ResourcePolicyViolation)
        );
        assert_eq!(optimizer.calls.get(), 1);
        assert_eq!(profile_resolver.calls.get(), 0);
        assert_eq!(compiler.calls.get(), 0);
    }

    #[test]
    fn unsupported_backend_fails_before_plan_materialization() {
        let profile_resolver = TestProfileResolver {
            calls: Cell::new(0),
        };
        let compiler = TestPlanCompiler {
            calls: Cell::new(0),
        };
        let optimizer = optimizer();

        assert_eq!(
            compile_event_execution_plan(
                &valid_expression(),
                ExecutionBackend::Stream,
                &field_resolver(),
                &TestValueResolver,
                &TestComparisonPolicy,
                &EventCompilationStages::new(&optimizer, &profile_resolver, &compiler),
            ),
            Err(RuleCompilationErrorKind::UnsupportedBackend)
        );
        assert_eq!(optimizer.calls.get(), 1);
        assert_eq!(profile_resolver.calls.get(), 1);
        assert_eq!(compiler.calls.get(), 0);
    }
}
