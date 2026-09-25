use std::fmt;
use std::str::FromStr;

const RULE_ID_PREFIX: &str = "CER-DET-";
const RULE_ID_DIGITS: usize = 6;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RuleId(String);

impl RuleId {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for RuleId {
    type Err = RuleContractError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let Some(suffix) = value.strip_prefix(RULE_ID_PREFIX) else {
            return Err(RuleContractError::InvalidRuleId);
        };

        if suffix.len() != RULE_ID_DIGITS || !suffix.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(RuleContractError::InvalidRuleId);
        }

        Ok(Self(value.to_owned()))
    }
}

impl fmt::Display for RuleId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RuleStatus {
    Experimental,
    Testing,
    Stable,
    Deprecated,
    Disabled,
}

impl RuleStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Experimental => "EXPERIMENTAL",
            Self::Testing => "TESTING",
            Self::Stable => "STABLE",
            Self::Deprecated => "DEPRECATED",
            Self::Disabled => "DISABLED",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AutomatedTestCoverage {
    positive: bool,
    negative: bool,
    edge_cases: bool,
}

impl AutomatedTestCoverage {
    #[must_use]
    pub const fn new(positive: bool, negative: bool, edge_cases: bool) -> Self {
        Self {
            positive,
            negative,
            edge_cases,
        }
    }

    #[must_use]
    pub const fn complete() -> Self {
        Self::new(true, true, true)
    }

    #[must_use]
    pub const fn is_complete(self) -> bool {
        self.positive && self.negative && self.edge_cases
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleContractError {
    InvalidRuleId,
    StableRequiresAutomatedTests,
}

impl fmt::Display for RuleContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRuleId => formatter.write_str(
                "detection rule id must match CER-DET-XXXXXX with exactly six ASCII digits",
            ),
            Self::StableRequiresAutomatedTests => formatter.write_str(
                "STABLE detection rules require positive, negative, and edge-case automated tests",
            ),
        }
    }
}

impl std::error::Error for RuleContractError {}

/// Validates status-specific invariants for a detection rule.
///
/// # Errors
///
/// Returns [`RuleContractError::StableRequiresAutomatedTests`] when a `STABLE`
/// rule does not have positive, negative, and edge-case automated test coverage.
pub fn validate_rule_status(
    status: RuleStatus,
    test_coverage: AutomatedTestCoverage,
) -> Result<(), RuleContractError> {
    if status == RuleStatus::Stable && !test_coverage.is_complete() {
        return Err(RuleContractError::StableRequiresAutomatedTests);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr as _;

    use super::{
        AutomatedTestCoverage, RuleContractError, RuleId, RuleStatus, validate_rule_status,
    };

    #[test]
    fn rule_id_requires_stable_cerbero_detection_format() {
        for value in ["CER-DET-000001", "CER-DET-999999"] {
            let rule_id = RuleId::from_str(value).expect("valid rule id");
            assert_eq!(rule_id.as_str(), value);
            assert_eq!(rule_id.to_string(), value);
        }

        for value in [
            "CER-DET-00001",
            "CER-DET-0000001",
            "CER-DET-00A001",
            "cer-det-000001",
            " CER-DET-000001",
            "CER-DET-000001 ",
        ] {
            assert_eq!(
                RuleId::from_str(value),
                Err(RuleContractError::InvalidRuleId),
                "unexpectedly accepted {value:?}"
            );
        }
    }

    #[test]
    fn rule_status_strings_match_frozen_contract() {
        assert_eq!(RuleStatus::Experimental.as_str(), "EXPERIMENTAL");
        assert_eq!(RuleStatus::Testing.as_str(), "TESTING");
        assert_eq!(RuleStatus::Stable.as_str(), "STABLE");
        assert_eq!(RuleStatus::Deprecated.as_str(), "DEPRECATED");
        assert_eq!(RuleStatus::Disabled.as_str(), "DISABLED");
    }

    #[test]
    fn stable_rules_require_complete_automated_test_coverage() {
        let incomplete = AutomatedTestCoverage::new(true, true, false);

        assert_eq!(
            validate_rule_status(RuleStatus::Stable, incomplete),
            Err(RuleContractError::StableRequiresAutomatedTests)
        );
        assert_eq!(
            validate_rule_status(RuleStatus::Stable, AutomatedTestCoverage::complete()),
            Ok(())
        );
    }

    #[test]
    fn pre_stable_rules_may_have_incomplete_test_coverage() {
        let no_tests = AutomatedTestCoverage::default();

        assert_eq!(
            validate_rule_status(RuleStatus::Experimental, no_tests),
            Ok(())
        );
        assert_eq!(validate_rule_status(RuleStatus::Testing, no_tests), Ok(()));
    }
}
