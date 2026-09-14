use std::collections::BTreeMap;
use std::fmt;

use cerbero_common::contracts::v1::RawEventPersisted;
use prost_types::Timestamp;
use time::{Date, Month, OffsetDateTime, PrimitiveDateTime, Time, UtcOffset};

use crate::ParsedEvent;
use crate::parser_formats::SYSLOG_RFC3164_PARSER_ID;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceTimeError {
    pub message: String,
}

impl fmt::Display for SourceTimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for SourceTimeError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceTimePolicy {
    offset: UtcOffset,
    offset_text: String,
}

impl SourceTimePolicy {
    #[must_use]
    pub fn canonical(&self) -> String {
        format!(
            "fixed_utc_offset={};rfc3164_year=nearest_ingest_year",
            self.offset_text
        )
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SourceTimePolicyRegistry {
    policies: BTreeMap<String, SourceTimePolicy>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventTimeContext {
    pub event_time: Option<Timestamp>,
    pub source: String,
    pub assumption: Option<String>,
}

impl EventTimeContext {
    #[must_use]
    pub fn from_persisted(persisted: &RawEventPersisted) -> Self {
        if let Some(event_time) = &persisted.event_time {
            Self {
                event_time: Some(*event_time),
                source: "event_time".to_string(),
                assumption: None,
            }
        } else {
            Self::unresolved()
        }
    }

    #[must_use]
    pub fn unresolved() -> Self {
        Self {
            event_time: None,
            source: "ingest_time_fallback".to_string(),
            assumption: None,
        }
    }
}

impl SourceTimePolicyRegistry {
    pub fn parse_spec(spec: &str) -> Result<Self, SourceTimeError> {
        let mut policies = BTreeMap::new();
        let spec = spec.trim();
        if spec.is_empty() {
            return Ok(Self { policies });
        }
        for entry in spec.split(',') {
            let (source_id, offset_text) = entry.split_once('=').ok_or_else(|| {
                source_time_error(
                    "source time policy entries must use source_id=+HH:MM or source_id=-HH:MM",
                )
            })?;
            let source_id = source_id.trim();
            if source_id.is_empty() || source_id.contains(',') || source_id.contains('=') {
                return Err(source_time_error(
                    "source time policy source_id must be non-empty and cannot contain ',' or '='",
                ));
            }
            if policies.contains_key(source_id) {
                return Err(source_time_error(format!(
                    "duplicate source time policy for source_id {source_id}"
                )));
            }
            let (offset, canonical) = parse_offset(offset_text.trim())?;
            policies.insert(
                source_id.to_string(),
                SourceTimePolicy {
                    offset,
                    offset_text: canonical,
                },
            );
        }
        Ok(Self { policies })
    }

    #[must_use]
    pub fn policy_canonical(&self, source_id: &str) -> Option<String> {
        self.policies
            .get(source_id)
            .map(SourceTimePolicy::canonical)
    }

    pub fn resolve(
        &self,
        persisted: &RawEventPersisted,
        parsed: &ParsedEvent,
    ) -> Result<EventTimeContext, SourceTimeError> {
        if persisted.event_time.is_some() {
            return Ok(EventTimeContext::from_persisted(persisted));
        }
        if parsed.parser_id != SYSLOG_RFC3164_PARSER_ID {
            return Ok(EventTimeContext::unresolved());
        }
        let Some(policy) = self.policies.get(&persisted.source_id) else {
            return Ok(EventTimeContext::unresolved());
        };
        let Some(candidate) = parsed
            .timestamp_candidates
            .iter()
            .find(|candidate| candidate.precision == "second_without_year_or_timezone")
        else {
            return Ok(EventTimeContext::unresolved());
        };
        let Some(ingest_time) = persisted.ingest_time.as_ref() else {
            return Ok(EventTimeContext::unresolved());
        };
        let event_time = resolve_rfc3164_nearest_year(&candidate.value, ingest_time, policy)?;
        Ok(EventTimeContext {
            event_time: Some(event_time),
            source: "source_time_policy".to_string(),
            assumption: Some(format!(
                "{};candidate={}",
                policy.canonical(),
                candidate.source_field
            )),
        })
    }
}

fn resolve_rfc3164_nearest_year(
    value: &str,
    ingest: &Timestamp,
    policy: &SourceTimePolicy,
) -> Result<Timestamp, SourceTimeError> {
    let (month, day, hour, minute, second) = parse_rfc3164_components(value)?;
    let ingest_utc = OffsetDateTime::from_unix_timestamp(ingest.seconds)
        .map_err(|error| source_time_error(format!("invalid ingest_time: {error}")))?;
    let local_ingest = ingest_utc.to_offset(policy.offset);
    let base_year = local_ingest.year();

    let mut best: Option<(u64, OffsetDateTime)> = None;
    for year in [base_year - 1, base_year, base_year + 1] {
        let Ok(date) = Date::from_calendar_date(year, month, day) else {
            continue;
        };
        let time = Time::from_hms(hour, minute, second)
            .map_err(|error| source_time_error(format!("invalid RFC3164 time: {error}")))?;
        let candidate = PrimitiveDateTime::new(date, time).assume_offset(policy.offset);
        let distance = candidate
            .unix_timestamp()
            .abs_diff(ingest_utc.unix_timestamp());
        if best.as_ref().is_none_or(|(current, _)| distance < *current) {
            best = Some((distance, candidate));
        }
    }
    let (_, best) = best.ok_or_else(|| {
        source_time_error("RFC3164 timestamp cannot be resolved under the source time policy")
    })?;
    Ok(Timestamp {
        seconds: best.unix_timestamp(),
        nanos: 0,
    })
}

fn parse_rfc3164_components(value: &str) -> Result<(Month, u8, u8, u8, u8), SourceTimeError> {
    if value.len() != 15 {
        return Err(source_time_error(
            "RFC3164 candidate must match 'Mmm dd HH:MM:SS'",
        ));
    }
    let bytes = value.as_bytes();
    let month = match &bytes[..3] {
        b"Jan" => Month::January,
        b"Feb" => Month::February,
        b"Mar" => Month::March,
        b"Apr" => Month::April,
        b"May" => Month::May,
        b"Jun" => Month::June,
        b"Jul" => Month::July,
        b"Aug" => Month::August,
        b"Sep" => Month::September,
        b"Oct" => Month::October,
        b"Nov" => Month::November,
        b"Dec" => Month::December,
        _ => return Err(source_time_error("invalid RFC3164 month")),
    };
    let day = parse_component_bytes(&bytes[4..6], "day")?;
    let hour = parse_component_bytes(&bytes[7..9], "hour")?;
    let minute = parse_component_bytes(&bytes[10..12], "minute")?;
    let second = parse_component_bytes(&bytes[13..15], "second")?;
    Ok((month, day, hour, minute, second))
}

fn parse_component_bytes(value: &[u8], name: &str) -> Result<u8, SourceTimeError> {
    let value = std::str::from_utf8(value)
        .map_err(|_| source_time_error(format!("invalid RFC3164 {name}")))?;
    value
        .trim()
        .parse::<u8>()
        .map_err(|_| source_time_error(format!("invalid RFC3164 {name}")))
}

fn parse_offset(value: &str) -> Result<(UtcOffset, String), SourceTimeError> {
    let bytes = value.as_bytes();
    if bytes.len() != 6
        || !matches!(bytes[0], b'+' | b'-')
        || bytes[3] != b':'
        || !bytes[1..3].iter().all(u8::is_ascii_digit)
        || !bytes[4..6].iter().all(u8::is_ascii_digit)
    {
        return Err(source_time_error(
            "source UTC offset must use exact +HH:MM or -HH:MM form",
        ));
    }
    let sign: i8 = if bytes[0] == b'-' { -1 } else { 1 };
    let hours = value[1..3]
        .parse::<i8>()
        .map_err(|_| source_time_error("invalid source UTC offset hour"))?;
    let minutes = value[4..6]
        .parse::<i8>()
        .map_err(|_| source_time_error("invalid source UTC offset minute"))?;
    let offset = UtcOffset::from_hms(sign * hours, sign * minutes, 0)
        .map_err(|error| source_time_error(format!("invalid source UTC offset: {error}")))?;
    Ok((offset, value.to_string()))
}

fn source_time_error(message: impl Into<String>) -> SourceTimeError {
    SourceTimeError {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ParsedValue, ParsingStatus, TimestampCandidate};

    fn parsed_rfc3164() -> ParsedEvent {
        ParsedEvent {
            parser_id: "cerbero.parser.syslog.rfc3164".to_string(),
            parser_version: "1".to_string(),
            source_event_type: "syslog.rfc3164".to_string(),
            fields: ParsedValue::Null,
            timestamp_candidates: vec![TimestampCandidate {
                value: "Oct 11 22:14:15".to_string(),
                source_field: "timestamp".to_string(),
                timezone_assumption: None,
                precision: "second_without_year_or_timezone".to_string(),
                confidence: "medium".to_string(),
            }],
            warnings: Vec::new(),
            status: ParsingStatus::Partial,
            selection: None,
        }
    }

    #[test]
    fn configured_offset_resolves_rfc3164_without_mutating_ingest_time() {
        let registry = SourceTimePolicyRegistry::parse_spec("source-a=-04:00").unwrap();
        let offset = UtcOffset::from_hms(-4, 0, 0).unwrap();
        let expected = PrimitiveDateTime::new(
            Date::from_calendar_date(2026, Month::October, 11).unwrap(),
            Time::from_hms(22, 14, 15).unwrap(),
        )
        .assume_offset(offset);
        let ingest = expected + time::Duration::hours(2);
        let persisted = RawEventPersisted {
            source_id: "source-a".to_string(),
            ingest_time: Some(Timestamp {
                seconds: ingest.unix_timestamp(),
                nanos: 123,
            }),
            ..Default::default()
        };
        let resolution = registry.resolve(&persisted, &parsed_rfc3164()).unwrap();
        assert_eq!(
            resolution.event_time.unwrap().seconds,
            expected.unix_timestamp()
        );
        assert_eq!(persisted.ingest_time.unwrap().nanos, 123);
        assert_eq!(resolution.source, "source_time_policy");
        assert!(
            resolution
                .assumption
                .unwrap()
                .contains("rfc3164_year=nearest_ingest_year")
        );
    }

    #[test]
    fn rfc3164_policy_does_not_resolve_another_parser_with_matching_precision_text() {
        let registry = SourceTimePolicyRegistry::parse_spec("source-a=-04:00").unwrap();
        let persisted = RawEventPersisted {
            source_id: "source-a".to_string(),
            ingest_time: Some(Timestamp {
                seconds: 1_800_000_000,
                nanos: 0,
            }),
            ..Default::default()
        };
        let mut parsed = parsed_rfc3164();
        parsed.parser_id = "cerbero.parser.future.example".to_string();
        parsed.source_event_type = "future.example".to_string();

        let resolution = registry.resolve(&persisted, &parsed).unwrap();
        assert!(resolution.event_time.is_none());
        assert_eq!(resolution.source, "ingest_time_fallback");
    }

    #[test]
    fn missing_source_policy_keeps_timestamp_unresolved() {
        let registry = SourceTimePolicyRegistry::default();
        let persisted = RawEventPersisted {
            source_id: "source-a".to_string(),
            ingest_time: Some(Timestamp {
                seconds: 1_800_000_000,
                nanos: 0,
            }),
            ..Default::default()
        };
        let resolution = registry.resolve(&persisted, &parsed_rfc3164()).unwrap();
        assert!(resolution.event_time.is_none());
        assert_eq!(resolution.source, "ingest_time_fallback");
    }

    #[test]
    fn policy_spec_rejects_duplicate_source_ids() {
        let error =
            SourceTimePolicyRegistry::parse_spec("source-a=-04:00,source-a=+00:00").unwrap_err();
        assert!(error.message.contains("duplicate"));
    }
}
