use cerbero_common::contracts::v1::RawEventPersisted;
use cerbero_normalizer::{
    GENERIC_JSON_PARSER_ID, JOURNALD_CANONICAL_PARSER_ID, ParsedValue, ParserInput, ParserRegistry,
    ParsingStatus, SYSLOG_RFC3164_PARSER_ID, SYSLOG_RFC5424_PARSER_ID,
};

fn persisted(content_type: &str) -> RawEventPersisted {
    RawEventPersisted {
        content_type: content_type.to_string(),
        encoding: "utf-8".to_string(),
        ..Default::default()
    }
}

fn parse(raw: &[u8], content_type: &str) -> cerbero_normalizer::ParsedEvent {
    let persisted = persisted(content_type);
    ParserRegistry::with_defaults()
        .parse(&ParserInput {
            raw,
            persisted: &persisted,
            configured_parser_id: None,
        })
        .unwrap()
}

#[test]
fn generic_json_golden_preserves_types_and_large_number_precision() {
    let parsed = parse(
        include_bytes!("fixtures/parsers/generic.json"),
        "application/json",
    );
    assert_eq!(parsed.parser_id, GENERIC_JSON_PARSER_ID);
    assert_eq!(parsed.status, ParsingStatus::Success);
    assert_eq!(
        parsed.field("count").and_then(ParsedValue::as_number_str),
        Some("123456789012345678901234567890")
    );
    assert_eq!(
        parsed.field("ok").and_then(ParsedValue::as_bool),
        Some(true)
    );
}

#[test]
fn rfc5424_golden_extracts_governed_header_fields() {
    let parsed = parse(include_bytes!("fixtures/parsers/rfc5424.log"), "text/plain");
    assert_eq!(parsed.parser_id, SYSLOG_RFC5424_PARSER_ID);
    assert_eq!(parsed.status, ParsingStatus::Success);
    assert_eq!(parsed.u64_field("pri"), Some(34));
    assert_eq!(parsed.u64_field("facility"), Some(4));
    assert_eq!(parsed.u64_field("severity"), Some(2));
    assert_eq!(parsed.u64_field("version"), Some(1));
    assert_eq!(parsed.string_field("hostname"), Some("mymachine"));
    assert_eq!(parsed.string_field("app_name"), Some("su"));
    assert_eq!(parsed.string_field("procid"), Some("123"));
    assert_eq!(parsed.string_field("msgid"), Some("ID47"));
    assert_eq!(parsed.timestamp_candidates.len(), 1);
    assert_eq!(parsed.timestamp_candidates[0].timezone_assumption, None);
}

#[test]
fn rfc3164_golden_records_timestamp_ambiguity_without_inventing_utc() {
    let parsed = parse(include_bytes!("fixtures/parsers/rfc3164.log"), "text/plain");
    assert_eq!(parsed.parser_id, SYSLOG_RFC3164_PARSER_ID);
    assert_eq!(parsed.status, ParsingStatus::Partial);
    assert_eq!(parsed.string_field("hostname"), Some("mymachine"));
    assert_eq!(parsed.string_field("app_name"), Some("su"));
    assert_eq!(parsed.string_field("procid"), Some("123"));
    assert_eq!(parsed.timestamp_candidates.len(), 1);
    assert_eq!(parsed.timestamp_candidates[0].value, "Oct 11 22:14:15");
    assert_eq!(parsed.timestamp_candidates[0].timezone_assumption, None);
    assert!(
        parsed
            .warnings
            .iter()
            .any(|warning| warning.contains("UTC is not assumed"))
    );
}

#[test]
fn journald_golden_preserves_binary_fields_and_absolute_timestamp_candidate() {
    let parsed = parse(
        include_bytes!("fixtures/parsers/journald.json"),
        "application/json",
    );
    assert_eq!(parsed.parser_id, JOURNALD_CANONICAL_PARSER_ID);
    assert_eq!(parsed.status, ParsingStatus::Success);
    assert_eq!(parsed.timestamp_candidates.len(), 1);
    assert_eq!(
        parsed.timestamp_candidates[0].source_field,
        "__REALTIME_TIMESTAMP"
    );
    assert_eq!(parsed.timestamp_candidates[0].timezone_assumption, None);

    let entries = parsed
        .field("journal_fields")
        .and_then(ParsedValue::as_array)
        .unwrap();
    let boot_id = entries
        .iter()
        .find(|entry| entry.get("name").and_then(ParsedValue::as_str) == Some("_BOOT_ID"))
        .unwrap();
    assert_eq!(
        boot_id.get("value").and_then(ParsedValue::as_bytes),
        Some(&[0, 1, 2, 255][..])
    );
}

#[test]
fn malformed_rfc5424_timestamp_fails_explicitly() {
    let persisted = persisted("text/plain");
    let error = ParserRegistry::with_defaults()
        .parse(&ParserInput {
            raw: include_bytes!("fixtures/parsers/invalid-rfc5424.log"),
            persisted: &persisted,
            configured_parser_id: None,
        })
        .unwrap_err();
    assert_eq!(error.code, "CER-PARSE-INVALID-TIMESTAMP");
    assert!(!error.retryable);
}

#[test]
fn parser_selection_trace_records_candidates_policy_and_choice() {
    let parsed = parse(
        include_bytes!("fixtures/parsers/journald.json"),
        "application/json",
    );
    let trace = parsed.selection.unwrap();
    assert_eq!(trace.policy, "auto:tier-then-confidence");
    assert_eq!(trace.selected_parser_id, JOURNALD_CANONICAL_PARSER_ID);
    assert!(trace.candidates.iter().any(|candidate| {
        candidate.parser_id == JOURNALD_CANONICAL_PARSER_ID && candidate.confidence > 0
    }));
    assert!(trace.candidates.iter().any(|candidate| {
        candidate.parser_id == GENERIC_JSON_PARSER_ID && candidate.confidence > 0
    }));
}

#[test]
fn malformed_rfc3164_timestamp_is_not_hidden_as_no_match() {
    let persisted = persisted("text/plain");
    let error = ParserRegistry::with_defaults()
        .parse(&ParserInput {
            raw: b"<34>Oct 32 22:14:15 mymachine su: bad timestamp",
            persisted: &persisted,
            configured_parser_id: None,
        })
        .unwrap_err();
    assert_eq!(error.code, "CER-PARSE-INVALID-TIMESTAMP");
}

#[test]
fn generic_json_input_limit_fails_before_syntax_processing() {
    let persisted = persisted("application/json");
    let mut raw = vec![b'x'; 1_048_577];
    raw[0] = b'{';
    let error = ParserRegistry::with_defaults()
        .parse(&ParserInput {
            raw: &raw,
            persisted: &persisted,
            configured_parser_id: None,
        })
        .unwrap_err();
    assert_eq!(error.code, "CER-PARSE-LIMIT-EXCEEDED");
}
