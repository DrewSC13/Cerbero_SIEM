use std::fmt;

use crate::{BooleanExpression, BooleanOperator, DetectionExpression};

/// Structural validation failures for the backend-neutral detection AST.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AstValidationError {
    EmptyBooleanExpression { operator: BooleanOperator },
}

impl fmt::Display for AstValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyBooleanExpression { operator } => {
                write!(
                    formatter,
                    "{} detection expression requires at least one child",
                    operator.as_str()
                )
            }
        }
    }
}

impl std::error::Error for AstValidationError {}

/// Validates structural invariants of the currently materialized detection AST.
///
/// This stage is intentionally backend-neutral. It does not resolve OCSF fields,
/// type-check values, or validate operator/type compatibility.
///
/// # Errors
///
/// Returns [`AstValidationError::EmptyBooleanExpression`] when an `AND` or `OR`
/// expression has no children. Nested boolean expressions are validated
/// recursively.
pub fn validate_expression<Field, Value>(
    expression: &DetectionExpression<Field, Value>,
) -> Result<(), AstValidationError> {
    match expression {
        DetectionExpression::Boolean(boolean) => validate_boolean_expression(boolean),
        DetectionExpression::Comparison(_) | DetectionExpression::Exists(_) => Ok(()),
    }
}

fn validate_boolean_expression<Field, Value>(
    expression: &BooleanExpression<Field, Value>,
) -> Result<(), AstValidationError> {
    let operator = expression.operator();

    match expression {
        BooleanExpression::And(children) | BooleanExpression::Or(children) => {
            if children.is_empty() {
                return Err(AstValidationError::EmptyBooleanExpression { operator });
            }

            for child in children {
                validate_expression(child)?;
            }

            Ok(())
        }
        BooleanExpression::Not(child) => validate_expression(child),
    }
}

#[cfg(test)]
mod tests {
    use crate::{BooleanExpression, BooleanOperator, DetectionExpression, ExistsExpression};

    use super::{AstValidationError, validate_expression};

    type Expression = DetectionExpression<&'static str, &'static str>;

    fn exists() -> Expression {
        Expression::Exists(ExistsExpression { field: "user.name" })
    }

    #[test]
    fn empty_and_or_fail_closed() {
        let cases = [
            (
                Expression::Boolean(BooleanExpression::And(Vec::new())),
                BooleanOperator::And,
            ),
            (
                Expression::Boolean(BooleanExpression::Or(Vec::new())),
                BooleanOperator::Or,
            ),
        ];

        for (expression, operator) in cases {
            assert_eq!(
                validate_expression(&expression),
                Err(AstValidationError::EmptyBooleanExpression { operator })
            );
        }
    }

    #[test]
    fn single_child_and_or_remain_valid_until_stricter_policy_is_defined() {
        let cases = [
            Expression::Boolean(BooleanExpression::And(vec![exists()])),
            Expression::Boolean(BooleanExpression::Or(vec![exists()])),
        ];

        for expression in cases {
            assert_eq!(validate_expression(&expression), Ok(()));
        }
    }

    #[test]
    fn validator_recurses_through_boolean_tree() {
        let nested_empty = Expression::Boolean(BooleanExpression::Not(Box::new(
            Expression::Boolean(BooleanExpression::And(Vec::new())),
        )));

        assert_eq!(
            validate_expression(&nested_empty),
            Err(AstValidationError::EmptyBooleanExpression {
                operator: BooleanOperator::And,
            })
        );
    }

    #[test]
    fn leaf_nodes_are_structurally_valid_before_type_checking() {
        assert_eq!(validate_expression(&exists()), Ok(()));
    }
}
