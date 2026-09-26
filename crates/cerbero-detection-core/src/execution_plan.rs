use crate::RuleId;

/// Execution backends frozen by Detection & Correlation v1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExecutionBackend {
    ClickHouse,
    Stream,
}

impl ExecutionBackend {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ClickHouse => "CLICKHOUSE",
            Self::Stream => "STREAM",
        }
    }
}

/// Declared backend capabilities for a compiled detection rule.
///
/// Detection & Correlation v1 requires compiled rules to know which of the
/// initial backends can preserve their semantics. The representation stays
/// deliberately minimal while those two backends remain the frozen v1 set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExecutionBackendCapabilities {
    clickhouse: bool,
    stream: bool,
}

impl ExecutionBackendCapabilities {
    #[must_use]
    pub const fn new(clickhouse: bool, stream: bool) -> Self {
        Self { clickhouse, stream }
    }

    #[must_use]
    pub const fn supports(self, backend: ExecutionBackend) -> bool {
        match backend {
            ExecutionBackend::ClickHouse => self.clickhouse,
            ExecutionBackend::Stream => self.stream,
        }
    }
}

/// Backend-neutral execution-plan contract.
///
/// Detection & Correlation v1 fixes the logical fields carried by an
/// `ExecutionPlan`, while leaving the concrete representation of plan IDs,
/// rule versions, required-field collections, predicates, grouping, windows,
/// resource limits, and plan hashes to later governed contracts. Those
/// components therefore remain generic at this boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionPlan<
    PlanId,
    RuleVersion,
    RequiredFields,
    Predicates,
    Grouping,
    Window,
    ResourceLimits,
    PlanHash,
> {
    pub plan_id: PlanId,
    pub rule_id: RuleId,
    pub rule_version: RuleVersion,
    pub backend: ExecutionBackend,
    pub required_fields: RequiredFields,
    pub predicates: Predicates,
    pub grouping: Grouping,
    pub window: Window,
    pub resource_limits: ResourceLimits,
    pub plan_hash: PlanHash,
}

#[cfg(test)]
mod tests {
    use std::str::FromStr as _;

    use crate::RuleId;

    use super::{ExecutionBackend, ExecutionBackendCapabilities, ExecutionPlan};

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestGrouping;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestWindow;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestResourceLimits;

    #[test]
    fn execution_backend_strings_match_frozen_contract() {
        assert_eq!(ExecutionBackend::ClickHouse.as_str(), "CLICKHOUSE");
        assert_eq!(ExecutionBackend::Stream.as_str(), "STREAM");
    }

    #[test]
    fn execution_backend_capabilities_explicitly_declare_support() {
        let clickhouse_only = ExecutionBackendCapabilities::new(true, false);
        let both = ExecutionBackendCapabilities::new(true, true);

        assert!(clickhouse_only.supports(ExecutionBackend::ClickHouse));
        assert!(!clickhouse_only.supports(ExecutionBackend::Stream));
        assert!(both.supports(ExecutionBackend::ClickHouse));
        assert!(both.supports(ExecutionBackend::Stream));
    }

    #[test]
    fn execution_plan_preserves_backend_neutral_contract_shape() {
        let plan = ExecutionPlan {
            plan_id: "opaque-plan-id",
            rule_id: RuleId::from_str("CER-DET-000001").expect("valid rule id"),
            rule_version: "opaque-rule-version",
            backend: ExecutionBackend::ClickHouse,
            required_fields: vec!["process.name", "process.command_line"],
            predicates: "opaque-predicates",
            grouping: TestGrouping,
            window: TestWindow,
            resource_limits: TestResourceLimits,
            plan_hash: "opaque-plan-hash",
        };

        assert_eq!(plan.plan_id, "opaque-plan-id");
        assert_eq!(plan.rule_id.as_str(), "CER-DET-000001");
        assert_eq!(plan.rule_version, "opaque-rule-version");
        assert_eq!(plan.backend, ExecutionBackend::ClickHouse);
        assert_eq!(
            plan.required_fields,
            vec!["process.name", "process.command_line"]
        );
        assert_eq!(plan.predicates, "opaque-predicates");
        assert_eq!(plan.grouping, TestGrouping);
        assert_eq!(plan.window, TestWindow);
        assert_eq!(plan.resource_limits, TestResourceLimits);
        assert_eq!(plan.plan_hash, "opaque-plan-hash");
    }
}
