#![forbid(unsafe_code)]

mod ast;
mod compiler_contract;
mod correlation;
mod event_compiler;
mod event_evaluator;
mod event_optimizer;
mod event_signal;
mod event_signal_materializer;
mod execution_plan;
mod normalized_event;
mod rule_contract;
mod threshold;
mod type_checker;
mod validator;

pub use ast::{
    BooleanExpression, BooleanOperator, ComparisonExpression, ComparisonOperator,
    DetectionExpression, DetectionExpressionKind, ExistsExpression,
};
pub use compiler_contract::{FieldTypeResolver, RuleCompilationErrorKind, require_field_type};
pub use correlation::{
    CompiledSequenceCorrelationPlan, SequenceCorrelationEvaluationError,
    SequenceCorrelationEvaluationPolicy, SequenceCorrelationMatch, SequenceCorrelationRule,
    compile_sequence_correlation_plan, evaluate_sequence_signals,
};
pub use event_compiler::{
    CompiledEventExecutionPlan, EventCompilationStages, EventExecutionPlanCompiler,
    EventFieldProfile, EventFieldProfileContract, EventFieldProfileResolver,
    compile_event_execution_plan,
};
pub use event_evaluator::{
    EventEvaluationResult, EventExecutionPlanView, EventFieldState, EventFieldView,
    EventLeafEvaluationPolicy, EventPredicate, evaluate_event_execution_plan,
};
pub use event_optimizer::{EventAstOptimizer, IdentityEventAstOptimizer};
pub use event_signal::{
    EventSignalEvaluationError, EventSignalEvaluationResult, EventSignalExecutionPlanView,
    EventSignalProvenance, evaluate_normalized_event_for_signal,
};
pub use event_signal_materializer::{EventSignalMaterializationContext, materialize_event_signal};
pub use execution_plan::{ExecutionBackend, ExecutionBackendCapabilities, ExecutionPlan};
pub use normalized_event::NormalizedEventFieldView;
pub use rule_contract::{
    AutomatedTestCoverage, RuleContractError, RuleId, RuleStatus, validate_rule_status,
};
pub use threshold::{
    CompiledThresholdExecutionPlan, ThresholdAggregation, ThresholdEvaluationError,
    ThresholdEvaluationPolicy, ThresholdLateEventPolicy, ThresholdRule,
    ThresholdSignalMaterializationContext, ThresholdSignalProvenance, ThresholdTimeBasis,
    ThresholdWindow, compile_threshold_execution_plan, evaluate_threshold_normalized_events,
    materialize_threshold_signal,
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
