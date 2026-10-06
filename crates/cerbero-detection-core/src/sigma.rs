use std::{collections::BTreeMap, fmt, str::FromStr as _};

use cerbero_common::contracts::sha256_lower_hex;
use serde::Deserialize;

use crate::{
    BooleanExpression, ComparisonExpression, ComparisonOperator, DetectionExpression, RuleId,
    RuleStatus, ThresholdAggregation, ThresholdLateEventPolicy, ThresholdRule, ThresholdTimeBasis,
    ThresholdWindow,
};

/// Imported first-slice Sigma THRESHOLD rule with source provenance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportedSigmaThresholdRule {
    pub rule_id: RuleId,
    pub rule_version: String,
    pub status: RuleStatus,
    pub title: String,
    pub severity: String,
    pub content_hash: String,
    pub threshold_rule: ThresholdRule<String, String>,
}

/// Fail-closed Sigma import error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SigmaImportError(String);

impl fmt::Display for SigmaImportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for SigmaImportError {}

#[derive(Debug, Clone, Deserialize)]
struct SigmaDocument {
    title: String,
    id: Option<String>,
    name: Option<String>,
    taxonomy: Option<String>,
    status: Option<String>,
    logsource: Option<SigmaLogSource>,
    detection: Option<SigmaDetection>,
    correlation: Option<SigmaCorrelation>,
    level: Option<String>,
    cerbero_rule_id: Option<String>,
    cerbero_rule_version: Option<String>,
    cerbero_time_basis: Option<String>,
    cerbero_time_field: Option<String>,
    cerbero_late_event_policy: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct SigmaLogSource {
    product: Option<String>,
    service: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct SigmaDetection {
    condition: String,
    #[serde(flatten)]
    selections: BTreeMap<String, BTreeMap<String, String>>,
}

#[derive(Debug, Clone, Deserialize)]
struct SigmaCorrelation {
    #[serde(rename = "type")]
    kind: String,
    rules: Vec<String>,
    #[serde(rename = "group-by")]
    group_by: Vec<String>,
    timespan: String,
    condition: SigmaCountCondition,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SigmaCountCondition {
    gte: u64,
}

/// Parse the governed Sigma subset used by the first STABLE CERBERO rule.
///
/// This initial adapter accepts a multi-document pack containing one OCSF base
/// detection and one `event_count` correlation. It maps the selection to the
/// existing backend-neutral detection AST and maps the correlation to the
/// existing [`ThresholdRule`]. Unsupported shapes fail closed rather than being
/// approximated.
///
/// # Errors
///
/// Returns [`SigmaImportError`] for YAML syntax failures, unresolved rule
/// references, unsupported Sigma constructs, invalid Cerbero identity/version,
/// or invalid time/correlation metadata.
#[allow(clippy::too_many_lines)]
pub fn parse_sigma_threshold_rule(
    source: &str,
) -> Result<ImportedSigmaThresholdRule, SigmaImportError> {
    let documents = serde_yaml::Deserializer::from_str(source)
        .map(|document| {
            SigmaDocument::deserialize(document)
                .map_err(|error| SigmaImportError(format!("Sigma YAML syntax error: {error}")))
        })
        .collect::<Result<Vec<_>, _>>()?;

    if documents.len() < 2 {
        return Err(SigmaImportError(
            "Sigma threshold pack requires a base detection and correlation document".to_string(),
        ));
    }

    let correlations = documents
        .iter()
        .filter(|document| document.correlation.is_some())
        .collect::<Vec<_>>();
    if correlations.len() != 1 {
        return Err(SigmaImportError(
            "Sigma threshold pack requires exactly one correlation document".to_string(),
        ));
    }
    let correlation_document = correlations.first().copied().ok_or_else(|| {
        SigmaImportError("Sigma threshold pack lost its correlation document".to_string())
    })?;
    let correlation = correlation_document.correlation.as_ref().ok_or_else(|| {
        SigmaImportError("internal Sigma import selection lost correlation document".to_string())
    })?;

    require_value(
        correlation_document.taxonomy.as_deref(),
        "ocsf",
        "correlation taxonomy",
    )?;
    require_value(
        correlation_document.status.as_deref(),
        "stable",
        "correlation status",
    )?;
    if correlation.kind != "event_count" {
        return Err(SigmaImportError(
            "first Sigma threshold slice supports event_count only".to_string(),
        ));
    }
    if correlation.rules.len() != 1 {
        return Err(SigmaImportError(
            "first Sigma threshold slice requires exactly one referenced base rule".to_string(),
        ));
    }
    if correlation.group_by.is_empty() {
        return Err(SigmaImportError(
            "Sigma event_count correlation requires non-empty group-by".to_string(),
        ));
    }
    if correlation.condition.gte == 0 {
        return Err(SigmaImportError(
            "Sigma event_count gte threshold must be greater than zero".to_string(),
        ));
    }

    let referenced = correlation.rules.first().ok_or_else(|| {
        SigmaImportError(
            "first Sigma threshold slice requires one referenced base rule".to_string(),
        )
    })?;
    let base_document = documents
        .iter()
        .find(|document| {
            document.detection.is_some()
                && (document.name.as_deref() == Some(referenced.as_str())
                    || document.id.as_deref() == Some(referenced.as_str()))
        })
        .ok_or_else(|| {
            SigmaImportError(format!(
                "Sigma correlation references unresolved base rule {referenced}"
            ))
        })?;

    require_value(base_document.taxonomy.as_deref(), "ocsf", "base taxonomy")?;
    let logsource = base_document
        .logsource
        .as_ref()
        .ok_or_else(|| SigmaImportError("Sigma base detection requires logsource".to_string()))?;
    require_value(logsource.product.as_deref(), "linux", "logsource.product")?;
    require_value(logsource.service.as_deref(), "sshd", "logsource.service")?;

    let detection = base_document
        .detection
        .as_ref()
        .ok_or_else(|| SigmaImportError("Sigma base detection requires detection".to_string()))?;
    let selection = detection
        .selections
        .get(&detection.condition)
        .ok_or_else(|| {
            SigmaImportError(
                "first Sigma slice requires condition to name one selection".to_string(),
            )
        })?;
    if selection.is_empty() {
        return Err(SigmaImportError(
            "Sigma selection must contain at least one field".to_string(),
        ));
    }
    if selection.keys().any(|field| field.contains('|')) {
        return Err(SigmaImportError(
            "Sigma field modifiers are not supported by the first governed slice".to_string(),
        ));
    }

    let comparisons = selection
        .iter()
        .map(|(field, value)| {
            DetectionExpression::Comparison(ComparisonExpression {
                field: field.clone(),
                operator: ComparisonOperator::Eq,
                value: value.clone(),
            })
        })
        .collect::<Vec<_>>();
    let filter = if comparisons.len() == 1 {
        comparisons.into_iter().next().ok_or_else(|| {
            SigmaImportError("Sigma selection unexpectedly became empty".to_string())
        })?
    } else {
        DetectionExpression::Boolean(BooleanExpression::And(comparisons))
    };

    let rule_id_text = correlation_document
        .cerbero_rule_id
        .as_deref()
        .ok_or_else(|| SigmaImportError("missing cerbero_rule_id".to_string()))?;
    let rule_id = RuleId::from_str(rule_id_text)
        .map_err(|error| SigmaImportError(format!("invalid cerbero_rule_id: {error}")))?;

    let rule_version = correlation_document
        .cerbero_rule_version
        .clone()
        .ok_or_else(|| SigmaImportError("missing cerbero_rule_version".to_string()))?;
    if rule_version
        .parse::<u64>()
        .ok()
        .is_none_or(|value| value == 0)
    {
        return Err(SigmaImportError(
            "cerbero_rule_version must be a positive integer string".to_string(),
        ));
    }

    let time_basis = match correlation_document.cerbero_time_basis.as_deref() {
        Some("EVENT_TIME") => ThresholdTimeBasis::EventTime,
        Some(_) => {
            return Err(SigmaImportError(
                "first Sigma threshold slice supports EVENT_TIME only".to_string(),
            ));
        }
        None => return Err(SigmaImportError("missing cerbero_time_basis".to_string())),
    };

    let time_field = correlation_document
        .cerbero_time_field
        .clone()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| SigmaImportError("missing cerbero_time_field".to_string()))?;

    let late_event_policy = match correlation_document.cerbero_late_event_policy.as_deref() {
        Some("ACCEPT") => ThresholdLateEventPolicy::Accept,
        Some(_) => {
            return Err(SigmaImportError(
                "first Sigma threshold slice supports ACCEPT late-event policy only".to_string(),
            ));
        }
        None => {
            return Err(SigmaImportError(
                "missing cerbero_late_event_policy".to_string(),
            ));
        }
    };

    let severity = correlation_document
        .level
        .as_deref()
        .ok_or_else(|| SigmaImportError("missing Sigma level".to_string()))?
        .to_ascii_uppercase();

    Ok(ImportedSigmaThresholdRule {
        rule_id,
        rule_version,
        status: RuleStatus::Stable,
        title: correlation_document.title.clone(),
        severity,
        content_hash: sha256_lower_hex(source.as_bytes()),
        threshold_rule: ThresholdRule {
            filter,
            group_by: correlation.group_by.clone(),
            aggregation: ThresholdAggregation::Count,
            threshold: correlation.condition.gte,
            window: ThresholdWindow {
                duration_millis: parse_timespan_millis(&correlation.timespan)?,
                time_basis,
                time_field,
                late_event_policy,
            },
        },
    })
}

fn require_value(
    actual: Option<&str>,
    expected: &str,
    field: &str,
) -> Result<(), SigmaImportError> {
    if actual == Some(expected) {
        Ok(())
    } else {
        Err(SigmaImportError(format!(
            "{field} must be {expected:?}, observed {actual:?}"
        )))
    }
}

fn parse_timespan_millis(value: &str) -> Result<u64, SigmaImportError> {
    if value.len() < 2 {
        return Err(SigmaImportError("invalid Sigma timespan".to_string()));
    }
    let (number, unit) = value.split_at(value.len() - 1);
    let amount = number
        .parse::<u64>()
        .ok()
        .filter(|amount| *amount > 0)
        .ok_or_else(|| SigmaImportError("invalid Sigma timespan amount".to_string()))?;
    let multiplier = match unit {
        "s" => 1_000_u64,
        "m" => 60_000_u64,
        "h" => 3_600_000_u64,
        "d" => 86_400_000_u64,
        _ => {
            return Err(SigmaImportError(
                "unsupported Sigma timespan unit".to_string(),
            ));
        }
    };
    amount
        .checked_mul(multiplier)
        .ok_or_else(|| SigmaImportError("Sigma timespan overflows u64 milliseconds".to_string()))
}

#[cfg(test)]
mod tests {
    use super::parse_sigma_threshold_rule;
    use crate::{RuleStatus, ThresholdAggregation, ThresholdLateEventPolicy, ThresholdTimeBasis};

    const STABLE_RULE: &str =
        include_str!("../../../rules/sigma/linux_sshd_failed_login_burst.yml");

    #[test]
    fn stable_sigma_rule_maps_to_governed_threshold_contract() {
        let imported = parse_sigma_threshold_rule(STABLE_RULE).expect("valid stable Sigma rule");

        assert_eq!(imported.rule_id.as_str(), "CER-DET-000001");
        assert_eq!(imported.rule_version, "1");
        assert_eq!(imported.status, RuleStatus::Stable);
        assert_eq!(imported.severity, "HIGH");
        assert_eq!(imported.content_hash.len(), 64);
        assert_eq!(
            imported.threshold_rule.aggregation,
            ThresholdAggregation::Count
        );
        assert_eq!(imported.threshold_rule.threshold, 10);
        assert_eq!(
            imported.threshold_rule.group_by,
            vec!["user.name".to_string(), "src_endpoint.ip".to_string()]
        );
        assert_eq!(imported.threshold_rule.window.duration_millis, 300_000);
        assert_eq!(
            imported.threshold_rule.window.time_basis,
            ThresholdTimeBasis::EventTime
        );
        assert_eq!(imported.threshold_rule.window.time_field, "time");
        assert_eq!(
            imported.threshold_rule.window.late_event_policy,
            ThresholdLateEventPolicy::Accept
        );
    }

    #[test]
    fn sigma_import_fails_closed_for_unsupported_correlation_type() {
        let invalid = STABLE_RULE.replace("type: event_count", "type: temporal");
        let error = parse_sigma_threshold_rule(&invalid).expect_err("temporal must fail closed");
        assert!(error.to_string().contains("event_count only"));
    }

    #[test]
    fn sigma_import_fails_closed_for_non_ocsf_taxonomy() {
        let invalid = STABLE_RULE.replacen("taxonomy: ocsf", "taxonomy: sigma", 1);
        let error = parse_sigma_threshold_rule(&invalid).expect_err("taxonomy must fail closed");
        assert!(error.to_string().contains("base taxonomy"));
    }

    #[test]
    fn sigma_import_rejects_invalid_cerbero_rule_identity() {
        let invalid = STABLE_RULE.replace("CER-DET-000001", "CER-DET-1");
        let error = parse_sigma_threshold_rule(&invalid).expect_err("bad ID must fail");
        assert!(error.to_string().contains("invalid cerbero_rule_id"));
    }
}
