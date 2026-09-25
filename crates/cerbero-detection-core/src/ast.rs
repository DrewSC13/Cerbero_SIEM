/// Frozen detection-expression categories from Detection & Correlation v1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DetectionExpressionKind {
    Boolean,
    Comparison,
    Exists,
    Selection,
    Threshold,
    Aggregation,
    Sequence,
    Join,
    Absence,
}

impl DetectionExpressionKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Boolean => "BOOLEAN",
            Self::Comparison => "COMPARISON",
            Self::Exists => "EXISTS",
            Self::Selection => "SELECTION",
            Self::Threshold => "THRESHOLD",
            Self::Aggregation => "AGGREGATION",
            Self::Sequence => "SEQUENCE",
            Self::Join => "JOIN",
            Self::Absence => "ABSENCE",
        }
    }
}

/// Boolean operators supported by the v1 detection AST.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BooleanOperator {
    And,
    Or,
    Not,
}

impl BooleanOperator {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::And => "AND",
            Self::Or => "OR",
            Self::Not => "NOT",
        }
    }
}

/// Comparison operators supported by the v1 detection AST.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ComparisonOperator {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    In,
    NotIn,
    Contains,
    StartsWith,
    EndsWith,
    Matches,
}

impl ComparisonOperator {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Eq => "EQ",
            Self::Ne => "NE",
            Self::Gt => "GT",
            Self::Gte => "GTE",
            Self::Lt => "LT",
            Self::Lte => "LTE",
            Self::In => "IN",
            Self::NotIn => "NOT_IN",
            Self::Contains => "CONTAINS",
            Self::StartsWith => "STARTS_WITH",
            Self::EndsWith => "ENDS_WITH",
            Self::Matches => "MATCHES",
        }
    }
}

/// Backend-neutral comparison node.
///
/// `Field` and `Value` remain generic until OCSF field resolution and type
/// checking are introduced by later compiler stages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComparisonExpression<Field, Value> {
    pub field: Field,
    pub operator: ComparisonOperator,
    pub value: Value,
}

/// Backend-neutral existence check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExistsExpression<Field> {
    pub field: Field,
}

/// Boolean composition over detection expressions.
///
/// `AND` and `OR` are structurally variadic; the validator rejects empty
/// child lists while allowing a single child. `NOT` carries exactly one child
/// by construction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BooleanExpression<Field, Value> {
    And(Vec<DetectionExpression<Field, Value>>),
    Or(Vec<DetectionExpression<Field, Value>>),
    Not(Box<DetectionExpression<Field, Value>>),
}

impl<Field, Value> BooleanExpression<Field, Value> {
    #[must_use]
    pub const fn operator(&self) -> BooleanOperator {
        match self {
            Self::And(_) => BooleanOperator::And,
            Self::Or(_) => BooleanOperator::Or,
            Self::Not(_) => BooleanOperator::Not,
        }
    }
}

/// Initial structural detection AST.
///
/// Only boolean, comparison, and existence nodes are materialized in this
/// slice. The remaining frozen expression kinds stay represented by
/// [`DetectionExpressionKind`] until their contracts are implemented.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DetectionExpression<Field, Value> {
    Boolean(BooleanExpression<Field, Value>),
    Comparison(ComparisonExpression<Field, Value>),
    Exists(ExistsExpression<Field>),
}

impl<Field, Value> DetectionExpression<Field, Value> {
    #[must_use]
    pub const fn kind(&self) -> DetectionExpressionKind {
        match self {
            Self::Boolean(_) => DetectionExpressionKind::Boolean,
            Self::Comparison(_) => DetectionExpressionKind::Comparison,
            Self::Exists(_) => DetectionExpressionKind::Exists,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BooleanExpression, BooleanOperator, ComparisonExpression, ComparisonOperator,
        DetectionExpression, DetectionExpressionKind, ExistsExpression,
    };

    #[test]
    fn detection_expression_kinds_match_frozen_contract() {
        let values = [
            (DetectionExpressionKind::Boolean, "BOOLEAN"),
            (DetectionExpressionKind::Comparison, "COMPARISON"),
            (DetectionExpressionKind::Exists, "EXISTS"),
            (DetectionExpressionKind::Selection, "SELECTION"),
            (DetectionExpressionKind::Threshold, "THRESHOLD"),
            (DetectionExpressionKind::Aggregation, "AGGREGATION"),
            (DetectionExpressionKind::Sequence, "SEQUENCE"),
            (DetectionExpressionKind::Join, "JOIN"),
            (DetectionExpressionKind::Absence, "ABSENCE"),
        ];

        for (value, expected) in values {
            assert_eq!(value.as_str(), expected);
        }
    }

    #[test]
    fn boolean_operators_match_frozen_contract() {
        assert_eq!(BooleanOperator::And.as_str(), "AND");
        assert_eq!(BooleanOperator::Or.as_str(), "OR");
        assert_eq!(BooleanOperator::Not.as_str(), "NOT");
    }

    #[test]
    fn structural_nodes_preserve_backend_neutral_shape() {
        type Expression = DetectionExpression<&'static str, &'static str>;

        let comparison = Expression::Comparison(ComparisonExpression {
            field: "process.name",
            operator: ComparisonOperator::Eq,
            value: "powershell.exe",
        });
        let exists = Expression::Exists(ExistsExpression { field: "user.name" });

        assert_eq!(comparison.kind(), DetectionExpressionKind::Comparison);
        assert_eq!(exists.kind(), DetectionExpressionKind::Exists);

        let expression = Expression::Boolean(BooleanExpression::And(vec![
            comparison.clone(),
            exists.clone(),
        ]));

        assert_eq!(expression.kind(), DetectionExpressionKind::Boolean);

        let Expression::Boolean(boolean) = expression else {
            panic!("expected boolean expression");
        };
        assert_eq!(boolean.operator(), BooleanOperator::And);

        let BooleanExpression::And(children) = boolean else {
            panic!("expected AND expression");
        };
        assert_eq!(children, vec![comparison, exists]);
    }

    #[test]
    fn not_structurally_contains_exactly_one_child() {
        type Expression = DetectionExpression<&'static str, &'static str>;

        let child = Expression::Exists(ExistsExpression { field: "user.name" });
        let expression = Expression::Boolean(BooleanExpression::Not(Box::new(child.clone())));

        let Expression::Boolean(BooleanExpression::Not(actual_child)) = expression else {
            panic!("expected NOT expression");
        };

        assert_eq!(*actual_child, child);
    }

    #[test]
    fn comparison_operators_match_frozen_contract() {
        let values = [
            (ComparisonOperator::Eq, "EQ"),
            (ComparisonOperator::Ne, "NE"),
            (ComparisonOperator::Gt, "GT"),
            (ComparisonOperator::Gte, "GTE"),
            (ComparisonOperator::Lt, "LT"),
            (ComparisonOperator::Lte, "LTE"),
            (ComparisonOperator::In, "IN"),
            (ComparisonOperator::NotIn, "NOT_IN"),
            (ComparisonOperator::Contains, "CONTAINS"),
            (ComparisonOperator::StartsWith, "STARTS_WITH"),
            (ComparisonOperator::EndsWith, "ENDS_WITH"),
            (ComparisonOperator::Matches, "MATCHES"),
        ];

        for (value, expected) in values {
            assert_eq!(value.as_str(), expected);
        }
    }
}
