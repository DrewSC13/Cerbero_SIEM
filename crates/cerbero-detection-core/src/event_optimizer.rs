use crate::{DetectionExpression, RuleCompilationErrorKind};

/// Backend-neutral optimizer boundary between the Detection AST and execution planning.
///
/// Detection & Correlation v1 freezes an Optimizer stage but does not freeze
/// concrete EVENT rewrite rules. This contract keeps the stage explicit without
/// introducing transformations that are not yet governed.
pub trait EventAstOptimizer<Field, Value> {
    /// Optimizes a validated and type-checked EVENT AST while preserving semantics.
    ///
    /// # Errors
    ///
    /// Returns a frozen [`RuleCompilationErrorKind`] when optimization cannot
    /// complete deterministically or safely.
    fn optimize_event_ast(
        &self,
        expression: DetectionExpression<Field, Value>,
    ) -> Result<DetectionExpression<Field, Value>, RuleCompilationErrorKind>;
}

/// Identity optimizer used until concrete governed EVENT rewrites are defined.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IdentityEventAstOptimizer;

impl<Field, Value> EventAstOptimizer<Field, Value> for IdentityEventAstOptimizer {
    fn optimize_event_ast(
        &self,
        expression: DetectionExpression<Field, Value>,
    ) -> Result<DetectionExpression<Field, Value>, RuleCompilationErrorKind> {
        Ok(expression)
    }
}

#[cfg(test)]
mod tests {
    use crate::{ComparisonExpression, ComparisonOperator, DetectionExpression};

    use super::{EventAstOptimizer, IdentityEventAstOptimizer};

    #[test]
    fn identity_event_optimizer_preserves_ast() {
        let expression = DetectionExpression::Comparison(ComparisonExpression {
            field: "process.name",
            operator: ComparisonOperator::Eq,
            value: "powershell.exe",
        });

        assert_eq!(
            IdentityEventAstOptimizer.optimize_event_ast(expression.clone()),
            Ok(expression)
        );
    }
}
