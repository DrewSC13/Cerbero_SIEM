use crate::{
    BooleanExpression, ComparisonOperator, DetectionExpression, FieldTypeResolver,
    RuleCompilationErrorKind, require_field_type,
};

/// Boundary used to derive the schema-facing type of an AST value.
pub trait ValueTypeResolver<Value> {
    type ValueType;

    fn resolve_value_type(&self, value: &Value) -> Self::ValueType;
}

/// Schema-owned compatibility policy for comparison expressions.
pub trait ComparisonTypePolicy<FieldType, ValueType> {
    /// Validates whether an operator can compare the resolved field and value types.
    ///
    /// # Errors
    ///
    /// Returns [`RuleCompilationErrorKind::InvalidOperator`] when the operator is
    /// not valid for the resolved field type, or
    /// [`RuleCompilationErrorKind::TypeError`] when field and value types are
    /// incompatible. Schema-specific policy may use another frozen compilation
    /// error kind when required by a later contract.
    fn validate_comparison(
        &self,
        field_type: &FieldType,
        operator: ComparisonOperator,
        value_type: &ValueType,
    ) -> Result<(), RuleCompilationErrorKind>;
}

/// Type-checks the currently materialized detection AST against schema-owned policy.
///
/// Boolean nodes are traversed recursively. `EXISTS` requires a resolvable field.
/// Comparison nodes resolve the field type and value type before delegating
/// operator/type compatibility to [`ComparisonTypePolicy`].
///
/// # Errors
///
/// Returns [`RuleCompilationErrorKind::UnknownField`] for unresolved fields and
/// propagates compatibility errors returned by [`ComparisonTypePolicy`].
pub fn type_check_expression<Field, Value, FieldResolver, ValueResolver, Policy>(
    expression: &DetectionExpression<Field, Value>,
    field_resolver: &FieldResolver,
    value_resolver: &ValueResolver,
    policy: &Policy,
) -> Result<(), RuleCompilationErrorKind>
where
    Field: AsRef<str>,
    FieldResolver: FieldTypeResolver,
    ValueResolver: ValueTypeResolver<Value>,
    Policy: ComparisonTypePolicy<FieldResolver::FieldType, ValueResolver::ValueType>,
{
    match expression {
        DetectionExpression::Boolean(boolean) => match boolean {
            BooleanExpression::And(children) | BooleanExpression::Or(children) => {
                for child in children {
                    type_check_expression(child, field_resolver, value_resolver, policy)?;
                }

                Ok(())
            }
            BooleanExpression::Not(child) => {
                type_check_expression(child, field_resolver, value_resolver, policy)
            }
        },
        DetectionExpression::Comparison(comparison) => {
            let field_type = require_field_type(field_resolver, comparison.field.as_ref())?;
            let value_type = value_resolver.resolve_value_type(&comparison.value);

            policy.validate_comparison(&field_type, comparison.operator, &value_type)
        }
        DetectionExpression::Exists(exists) => {
            require_field_type(field_resolver, exists.field.as_ref()).map(|_| ())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{
        BooleanExpression, ComparisonExpression, ComparisonOperator, DetectionExpression,
        ExistsExpression, FieldTypeResolver, RuleCompilationErrorKind,
    };

    use super::{ComparisonTypePolicy, ValueTypeResolver, type_check_expression};

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

    fn field_resolver() -> TestFieldResolver {
        TestFieldResolver {
            fields: HashMap::from([
                ("process.name", TestType::Text),
                ("process.pid", TestType::Number),
            ]),
        }
    }

    #[test]
    fn comparison_type_check_accepts_schema_compatible_types() {
        let expression = Expression::Comparison(ComparisonExpression {
            field: "process.pid",
            operator: ComparisonOperator::Eq,
            value: "42",
        });

        assert_eq!(
            type_check_expression(
                &expression,
                &field_resolver(),
                &TestValueResolver,
                &TestComparisonPolicy,
            ),
            Ok(())
        );
    }

    #[test]
    fn comparison_type_check_rejects_incompatible_types() {
        let expression = Expression::Comparison(ComparisonExpression {
            field: "process.pid",
            operator: ComparisonOperator::Eq,
            value: "powershell.exe",
        });

        assert_eq!(
            type_check_expression(
                &expression,
                &field_resolver(),
                &TestValueResolver,
                &TestComparisonPolicy,
            ),
            Err(RuleCompilationErrorKind::TypeError)
        );
    }

    #[test]
    fn comparison_type_check_rejects_invalid_operator_for_field_type() {
        let expression = Expression::Comparison(ComparisonExpression {
            field: "process.pid",
            operator: ComparisonOperator::Contains,
            value: "42",
        });

        assert_eq!(
            type_check_expression(
                &expression,
                &field_resolver(),
                &TestValueResolver,
                &TestComparisonPolicy,
            ),
            Err(RuleCompilationErrorKind::InvalidOperator)
        );
    }

    #[test]
    fn type_check_rejects_unknown_fields_in_exists_and_comparison() {
        let cases = [
            Expression::Exists(ExistsExpression {
                field: "unknown.field",
            }),
            Expression::Comparison(ComparisonExpression {
                field: "unknown.field",
                operator: ComparisonOperator::Eq,
                value: "value",
            }),
        ];

        for expression in cases {
            assert_eq!(
                type_check_expression(
                    &expression,
                    &field_resolver(),
                    &TestValueResolver,
                    &TestComparisonPolicy,
                ),
                Err(RuleCompilationErrorKind::UnknownField)
            );
        }
    }

    #[test]
    fn type_check_recurses_through_boolean_tree() {
        let expression = Expression::Boolean(BooleanExpression::And(vec![
            Expression::Exists(ExistsExpression {
                field: "process.name",
            }),
            Expression::Boolean(BooleanExpression::Not(Box::new(Expression::Comparison(
                ComparisonExpression {
                    field: "process.pid",
                    operator: ComparisonOperator::Eq,
                    value: "powershell.exe",
                },
            )))),
        ]));

        assert_eq!(
            type_check_expression(
                &expression,
                &field_resolver(),
                &TestValueResolver,
                &TestComparisonPolicy,
            ),
            Err(RuleCompilationErrorKind::TypeError)
        );
    }
}
