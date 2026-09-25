#![forbid(unsafe_code)]

mod ast;
mod compiler_contract;
mod rule_contract;
mod type_checker;
mod validator;

pub use ast::{
    BooleanExpression, BooleanOperator, ComparisonExpression, ComparisonOperator,
    DetectionExpression, DetectionExpressionKind, ExistsExpression,
};
pub use compiler_contract::{FieldTypeResolver, RuleCompilationErrorKind, require_field_type};
pub use rule_contract::{
    AutomatedTestCoverage, RuleContractError, RuleId, RuleStatus, validate_rule_status,
};
pub use type_checker::{ComparisonTypePolicy, ValueTypeResolver, type_check_expression};
pub use validator::{AstValidationError, validate_expression};

/// Stable component identity used for logging and provenance wiring.
#[must_use]
pub const fn component_name() -> &'static str {
    "cerbero-detection-core"
}

#[cfg(test)]
mod tests {
    use super::component_name;

    #[test]
    fn component_identity_is_stable() {
        assert_eq!(component_name(), "cerbero-detection-core");
    }
}
