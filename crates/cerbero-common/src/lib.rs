#![forbid(unsafe_code)]

/// Canonical product name.
pub const PROJECT_NAME: &str = "CERBERO";
/// Architectural baseline implemented by this repository bootstrap.
pub const ARCHITECTURE_BASELINE: &str = "v1.0";

/// Returns the immutable bootstrap identity used by smoke tests.
#[must_use]
pub const fn repository_baseline() -> &'static str {
    ARCHITECTURE_BASELINE
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_identity_is_stable() {
        assert_eq!(PROJECT_NAME, "CERBERO");
        assert_eq!(repository_baseline(), "v1.0");
    }
}
