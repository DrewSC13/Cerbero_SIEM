#![forbid(unsafe_code)]

/// Stable component identity used for logging and provenance wiring.
#[must_use]
pub const fn component_name() -> &'static str {
    "cerbero-integrity"
}

#[cfg(test)]
mod tests {
    use super::component_name;

    #[test]
    fn component_identity_is_stable() {
        assert_eq!(component_name(), "cerbero-integrity");
    }
}
