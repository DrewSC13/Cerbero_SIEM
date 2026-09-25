/// Initial rule-compilation error categories frozen by Detection & Correlation v1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RuleCompilationErrorKind {
    SyntaxError,
    UnknownField,
    TypeError,
    InvalidOperator,
    InvalidWindow,
    UnsupportedBackend,
    InvalidMapping,
    ResourcePolicyViolation,
}

impl RuleCompilationErrorKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SyntaxError => "SYNTAX_ERROR",
            Self::UnknownField => "UNKNOWN_FIELD",
            Self::TypeError => "TYPE_ERROR",
            Self::InvalidOperator => "INVALID_OPERATOR",
            Self::InvalidWindow => "INVALID_WINDOW",
            Self::UnsupportedBackend => "UNSUPPORTED_BACKEND",
            Self::InvalidMapping => "INVALID_MAPPING",
            Self::ResourcePolicyViolation => "RESOURCE_POLICY_VIOLATION",
        }
    }
}

/// Boundary used by detection compilation to resolve canonical normalized fields.
///
/// The concrete `FieldType` is deliberately owned by the schema/mapping layer.
/// `cerbero-detection-core` does not freeze the OCSF/Cerbero field-type taxonomy.
pub trait FieldTypeResolver {
    type FieldType;

    fn resolve_field_type(&self, field: &str) -> Option<Self::FieldType>;
}

/// Resolves the type of a canonical normalized field.
///
/// # Errors
///
/// Returns [`RuleCompilationErrorKind::UnknownField`] when the schema/mapping
/// layer cannot resolve the requested field.
pub fn require_field_type<R>(
    resolver: &R,
    field: &str,
) -> Result<R::FieldType, RuleCompilationErrorKind>
where
    R: FieldTypeResolver,
{
    resolver
        .resolve_field_type(field)
        .ok_or(RuleCompilationErrorKind::UnknownField)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{FieldTypeResolver, RuleCompilationErrorKind, require_field_type};

    struct TestFieldResolver {
        fields: HashMap<&'static str, &'static str>,
    }

    impl FieldTypeResolver for TestFieldResolver {
        type FieldType = &'static str;

        fn resolve_field_type(&self, field: &str) -> Option<Self::FieldType> {
            self.fields.get(field).copied()
        }
    }

    #[test]
    fn compilation_error_strings_match_frozen_contract() {
        let values = [
            (RuleCompilationErrorKind::SyntaxError, "SYNTAX_ERROR"),
            (RuleCompilationErrorKind::UnknownField, "UNKNOWN_FIELD"),
            (RuleCompilationErrorKind::TypeError, "TYPE_ERROR"),
            (
                RuleCompilationErrorKind::InvalidOperator,
                "INVALID_OPERATOR",
            ),
            (RuleCompilationErrorKind::InvalidWindow, "INVALID_WINDOW"),
            (
                RuleCompilationErrorKind::UnsupportedBackend,
                "UNSUPPORTED_BACKEND",
            ),
            (RuleCompilationErrorKind::InvalidMapping, "INVALID_MAPPING"),
            (
                RuleCompilationErrorKind::ResourcePolicyViolation,
                "RESOURCE_POLICY_VIOLATION",
            ),
        ];

        for (value, expected) in values {
            assert_eq!(value.as_str(), expected);
        }
    }

    #[test]
    fn field_type_resolution_is_schema_owned() {
        let resolver = TestFieldResolver {
            fields: HashMap::from([
                ("process.name", "schema-string-type"),
                ("event_time", "schema-time-type"),
            ]),
        };

        assert_eq!(
            require_field_type(&resolver, "process.name"),
            Ok("schema-string-type")
        );
        assert_eq!(
            require_field_type(&resolver, "event_time"),
            Ok("schema-time-type")
        );
    }

    #[test]
    fn unknown_field_fails_closed() {
        let resolver = TestFieldResolver {
            fields: HashMap::new(),
        };

        assert_eq!(
            require_field_type(&resolver, "source_specific.hidden_field"),
            Err(RuleCompilationErrorKind::UnknownField)
        );
    }
}
