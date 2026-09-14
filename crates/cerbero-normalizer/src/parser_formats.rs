use std::collections::BTreeMap;

use base64::Engine as _;
use serde_json::Value;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::parser::{
    ParseError, ParsedEvent, ParsedValue, Parser, ParserInput, ParserLimits, ParserTier,
    ParsingStatus, TimestampCandidate,
};

pub const GENERIC_JSON_PARSER_ID: &str = "cerbero.parser.json.generic";
pub const GENERIC_JSON_PARSER_VERSION: &str = "1";
pub const SYSLOG_RFC3164_PARSER_ID: &str = "cerbero.parser.syslog.rfc3164";
pub const SYSLOG_RFC3164_PARSER_VERSION: &str = "1";
pub const SYSLOG_RFC5424_PARSER_ID: &str = "cerbero.parser.syslog.rfc5424";
pub const SYSLOG_RFC5424_PARSER_VERSION: &str = "1";
pub const JOURNALD_CANONICAL_PARSER_ID: &str = "cerbero.parser.journald.canonical";
pub const JOURNALD_CANONICAL_PARSER_VERSION: &str = "1";

const JSON_LIMITS: ParserLimits = ParserLimits {
    max_input_bytes: 1_048_576,
    max_depth: 64,
    max_fields: 8_192,
    max_string_bytes: 262_144,
    max_collection_length: 4_096,
};

const SYSLOG_LIMITS: ParserLimits = ParserLimits {
    max_input_bytes: 65_536,
    max_depth: 8,
    max_fields: 128,
    max_string_bytes: 65_536,
    max_collection_length: 128,
};

#[derive(Clone, Copy, Debug, Default)]
pub struct GenericJsonParser;

impl Parser for GenericJsonParser {
    fn id(&self) -> &'static str {
        GENERIC_JSON_PARSER_ID
    }

    fn version(&self) -> &'static str {
        GENERIC_JSON_PARSER_VERSION
    }

    fn tier(&self) -> ParserTier {
        ParserTier::Generic
    }

    fn limits(&self) -> ParserLimits {
        JSON_LIMITS
    }

    fn probe(&self, input: &ParserInput<'_>) -> u8 {
        let first = input
            .raw
            .iter()
            .copied()
            .find(|byte| !byte.is_ascii_whitespace());
        if input.persisted.content_type.contains("json") {
            match first {
                Some(b'{' | b'[') => 60,
                _ => 35,
            }
        } else if matches!(first, Some(b'{' | b'[')) {
            30
        } else {
            0
        }
    }

    fn parse(&self, input: &ParserInput<'_>) -> Result<ParsedEvent, ParseError> {
        ensure_input_limit(input.raw, self.limits())?;
        let value: Value = serde_json::from_slice(input.raw)
            .map_err(|error| malformed(format!("invalid JSON syntax: {error}")))?;
        validate_json_tree(&value, self.limits())?;
        let (timestamp_candidates, warnings) = json_timestamp_candidates(&value);
        let status = if warnings.is_empty() {
            ParsingStatus::Success
        } else {
            ParsingStatus::Partial
        };
        Ok(ParsedEvent {
            parser_id: self.id().to_string(),
            parser_version: self.version().to_string(),
            source_event_type: "generic.json".to_string(),
            fields: json_to_parsed(&value),
            timestamp_candidates,
            warnings,
            status,
            selection: None,
        })
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct JournaldCanonicalParser;

impl Parser for JournaldCanonicalParser {
    fn id(&self) -> &'static str {
        JOURNALD_CANONICAL_PARSER_ID
    }

    fn version(&self) -> &'static str {
        JOURNALD_CANONICAL_PARSER_VERSION
    }

    fn tier(&self) -> ParserTier {
        ParserTier::SourceSpecific
    }

    fn limits(&self) -> ParserLimits {
        JSON_LIMITS
    }

    fn probe(&self, input: &ParserInput<'_>) -> u8 {
        let trimmed = trim_ascii_whitespace(input.raw);
        if trimmed.first() != Some(&b'{') {
            return 0;
        }
        if contains_bytes(trimmed, b"\"cerbero_journald_version\"")
            && contains_bytes(trimmed, b"\"fields\"")
        {
            100
        } else {
            0
        }
    }

    fn parse(&self, input: &ParserInput<'_>) -> Result<ParsedEvent, ParseError> {
        ensure_input_limit(input.raw, self.limits())?;
        let value: Value = serde_json::from_slice(input.raw)
            .map_err(|error| malformed(format!("invalid journald canonical JSON: {error}")))?;
        validate_json_tree(&value, self.limits())?;
        let root = value
            .as_object()
            .ok_or_else(|| malformed("journald canonical representation must be a JSON object"))?;
        ensure_journald_keys(root, &["cerbero_journald_version", "cursor", "fields"])?;
        let version = root
            .get("cerbero_journald_version")
            .and_then(Value::as_u64)
            .ok_or_else(|| malformed("cerbero_journald_version must be an unsigned integer"))?;
        if version != 1 {
            return Err(ParseError {
                code: "CER-PARSE-UNSUPPORTED-VERSION",
                message: format!("unsupported journald canonical version {version}"),
                retryable: false,
            });
        }

        let cursor = match root.get("cursor") {
            None | Some(Value::Null) => ParsedValue::Null,
            Some(Value::String(value)) => ParsedValue::String(value.clone()),
            Some(_) => return Err(malformed("journald cursor must be a string or null")),
        };
        let fields = root
            .get("fields")
            .and_then(Value::as_array)
            .ok_or_else(|| malformed("journald canonical fields must be an array"))?;

        let mut parsed_fields = Vec::with_capacity(fields.len());
        let mut timestamp_candidates = Vec::new();
        for field in fields {
            let (parsed_field, timestamp_candidate) = parse_journald_field(field, self.limits())?;
            parsed_fields.push(parsed_field);
            if let Some(candidate) = timestamp_candidate {
                timestamp_candidates.push(candidate);
            }
        }

        Ok(ParsedEvent {
            parser_id: self.id().to_string(),
            parser_version: self.version().to_string(),
            source_event_type: "journald.entry".to_string(),
            fields: ParsedValue::object([
                ("cursor", cursor),
                ("journal_fields", ParsedValue::Array(parsed_fields)),
            ]),
            timestamp_candidates,
            warnings: Vec::new(),
            status: ParsingStatus::Success,
            selection: None,
        })
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SyslogRfc5424Parser;

impl Parser for SyslogRfc5424Parser {
    fn id(&self) -> &'static str {
        SYSLOG_RFC5424_PARSER_ID
    }

    fn version(&self) -> &'static str {
        SYSLOG_RFC5424_PARSER_VERSION
    }

    fn tier(&self) -> ParserTier {
        ParserTier::ContentProtocol
    }

    fn limits(&self) -> ParserLimits {
        SYSLOG_LIMITS
    }

    fn probe(&self, input: &ParserInput<'_>) -> u8 {
        let Ok(text) = std::str::from_utf8(input.raw) else {
            return 0;
        };
        let Ok((_, rest)) = parse_pri_prefix(text.trim_end_matches(&['\r', '\n'][..])) else {
            return 0;
        };
        let version = rest.split(' ').next().unwrap_or_default();
        if !version.is_empty() && version.bytes().all(|byte| byte.is_ascii_digit()) {
            95
        } else {
            0
        }
    }

    fn parse(&self, input: &ParserInput<'_>) -> Result<ParsedEvent, ParseError> {
        ensure_input_limit(input.raw, self.limits())?;
        let text = std::str::from_utf8(input.raw)
            .map_err(|error| malformed(format!("RFC5424 input must be UTF-8: {error}")))?;
        parse_rfc5424(text.trim_end_matches(&['\r', '\n'][..]))
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SyslogRfc3164Parser;

impl Parser for SyslogRfc3164Parser {
    fn id(&self) -> &'static str {
        SYSLOG_RFC3164_PARSER_ID
    }

    fn version(&self) -> &'static str {
        SYSLOG_RFC3164_PARSER_VERSION
    }

    fn tier(&self) -> ParserTier {
        ParserTier::ContentProtocol
    }

    fn limits(&self) -> ParserLimits {
        SYSLOG_LIMITS
    }

    fn probe(&self, input: &ParserInput<'_>) -> u8 {
        let Ok(text) = std::str::from_utf8(input.raw) else {
            return 0;
        };
        let Ok((_, rest)) = parse_pri_prefix(text.trim_end_matches(&['\r', '\n'][..])) else {
            return 0;
        };
        if looks_like_rfc3164_timestamp_shape(rest) {
            90
        } else {
            0
        }
    }

    fn parse(&self, input: &ParserInput<'_>) -> Result<ParsedEvent, ParseError> {
        ensure_input_limit(input.raw, self.limits())?;
        let text = std::str::from_utf8(input.raw)
            .map_err(|error| malformed(format!("RFC3164 input must be UTF-8: {error}")))?;
        parse_rfc3164(text.trim_end_matches(&['\r', '\n'][..]))
    }
}

fn parse_rfc5424(text: &str) -> Result<ParsedEvent, ParseError> {
    let (pri, rest) = parse_pri_prefix(text)?;
    let (version_text, rest) = take_token(rest, "VERSION")?;
    let version = version_text
        .parse::<u16>()
        .map_err(|_| malformed("RFC5424 VERSION must be an unsigned integer"))?;
    if version != 1 {
        return Err(ParseError {
            code: "CER-PARSE-UNSUPPORTED-VERSION",
            message: format!("unsupported RFC5424 VERSION {version}"),
            retryable: false,
        });
    }
    let (timestamp, rest) = take_token(rest, "TIMESTAMP")?;
    let (hostname, rest) = take_token(rest, "HOSTNAME")?;
    let (app_name, rest) = take_token(rest, "APP-NAME")?;
    let (procid, rest) = take_token(rest, "PROCID")?;
    let (msgid, rest) = take_token(rest, "MSGID")?;
    let (structured_data, message) = split_structured_data(rest)?;

    let (timestamp_candidates, warnings, status) = if timestamp == "-" {
        (
            Vec::new(),
            vec!["RFC5424 TIMESTAMP is NILVALUE; event_time candidate unavailable".to_string()],
            ParsingStatus::Partial,
        )
    } else {
        OffsetDateTime::parse(timestamp, &Rfc3339).map_err(|error| {
            invalid_timestamp(format!("invalid RFC5424 TIMESTAMP {timestamp:?}: {error}"))
        })?;
        (
            vec![TimestampCandidate {
                value: timestamp.to_string(),
                source_field: "timestamp".to_string(),
                timezone_assumption: None,
                precision: "rfc3339".to_string(),
                confidence: "high".to_string(),
            }],
            Vec::new(),
            ParsingStatus::Success,
        )
    };

    Ok(ParsedEvent {
        parser_id: SYSLOG_RFC5424_PARSER_ID.to_string(),
        parser_version: SYSLOG_RFC5424_PARSER_VERSION.to_string(),
        source_event_type: "syslog.rfc5424".to_string(),
        fields: syslog_fields(SyslogFieldSet {
            pri,
            version: Some(version),
            timestamp: token_value(timestamp),
            hostname: token_value(hostname),
            app_name: token_value(app_name),
            procid: token_value(procid),
            msgid: token_value(msgid),
            structured_data: if structured_data == "-" {
                ParsedValue::Null
            } else {
                ParsedValue::String(structured_data.to_string())
            },
            message,
        }),
        timestamp_candidates,
        warnings,
        status,
        selection: None,
    })
}

fn parse_rfc3164(text: &str) -> Result<ParsedEvent, ParseError> {
    let (pri, rest) = parse_pri_prefix(text)?;
    if rest.len() < 16 {
        return Err(malformed("RFC3164 message is missing timestamp/hostname"));
    }
    let timestamp = rest
        .get(..15)
        .ok_or_else(|| malformed("RFC3164 timestamp is not ASCII-compatible"))?;
    validate_rfc3164_timestamp(timestamp)?;
    let after_timestamp = rest
        .get(15..)
        .ok_or_else(|| malformed("RFC3164 timestamp boundary is invalid"))?;
    let after_timestamp = after_timestamp
        .strip_prefix(' ')
        .ok_or_else(|| malformed("RFC3164 timestamp must be followed by a space"))?;
    let (hostname, remainder) = take_token_allow_terminal(after_timestamp, "HOSTNAME")?;

    let (app_name, procid, message) = parse_rfc3164_tag_and_message(remainder);
    let warning = "RFC3164 timestamp has no year or timezone; source policy is required and UTC is not assumed";

    Ok(ParsedEvent {
        parser_id: SYSLOG_RFC3164_PARSER_ID.to_string(),
        parser_version: SYSLOG_RFC3164_PARSER_VERSION.to_string(),
        source_event_type: "syslog.rfc3164".to_string(),
        fields: syslog_fields(SyslogFieldSet {
            pri,
            version: None,
            timestamp: ParsedValue::String(timestamp.to_string()),
            hostname: ParsedValue::String(hostname.to_string()),
            app_name: app_name.map_or(ParsedValue::Null, |value| {
                ParsedValue::String(value.to_string())
            }),
            procid: procid.map_or(ParsedValue::Null, |value| {
                ParsedValue::String(value.to_string())
            }),
            msgid: ParsedValue::Null,
            structured_data: ParsedValue::Null,
            message,
        }),
        timestamp_candidates: vec![TimestampCandidate {
            value: timestamp.to_string(),
            source_field: "timestamp".to_string(),
            timezone_assumption: None,
            precision: "second_without_year_or_timezone".to_string(),
            confidence: "medium".to_string(),
        }],
        warnings: vec![warning.to_string()],
        status: ParsingStatus::Partial,
        selection: None,
    })
}

struct SyslogFieldSet<'a> {
    pri: u8,
    version: Option<u16>,
    timestamp: ParsedValue,
    hostname: ParsedValue,
    app_name: ParsedValue,
    procid: ParsedValue,
    msgid: ParsedValue,
    structured_data: ParsedValue,
    message: &'a str,
}

fn syslog_fields(fields: SyslogFieldSet<'_>) -> ParsedValue {
    ParsedValue::object([
        ("pri", ParsedValue::Number(fields.pri.to_string())),
        (
            "facility",
            ParsedValue::Number((fields.pri / 8).to_string()),
        ),
        (
            "severity",
            ParsedValue::Number((fields.pri % 8).to_string()),
        ),
        (
            "version",
            fields.version.map_or(ParsedValue::Null, |value| {
                ParsedValue::Number(value.to_string())
            }),
        ),
        ("timestamp", fields.timestamp),
        ("hostname", fields.hostname),
        ("app_name", fields.app_name),
        ("procid", fields.procid),
        ("msgid", fields.msgid),
        ("structured_data", fields.structured_data),
        ("message", ParsedValue::String(fields.message.to_string())),
    ])
}

fn parse_pri_prefix(text: &str) -> Result<(u8, &str), ParseError> {
    let rest = text
        .strip_prefix('<')
        .ok_or_else(|| malformed("syslog PRI must start with '<'"))?;
    let end = rest
        .find('>')
        .ok_or_else(|| malformed("syslog PRI is missing '>'"))?;
    let pri_text = &rest[..end];
    if pri_text.is_empty()
        || pri_text.len() > 3
        || !pri_text.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(malformed("syslog PRI must contain 1-3 decimal digits"));
    }
    let pri = pri_text
        .parse::<u16>()
        .map_err(|_| malformed("syslog PRI is not a valid integer"))?;
    if pri > 191 {
        return Err(malformed("syslog PRI must be between 0 and 191"));
    }
    Ok((
        u8::try_from(pri).expect("validated PRI fits u8"),
        &rest[end + 1..],
    ))
}

fn take_token<'a>(input: &'a str, name: &str) -> Result<(&'a str, &'a str), ParseError> {
    let (token, rest) = input
        .split_once(' ')
        .ok_or_else(|| malformed(format!("RFC5424 {name} is missing")))?;
    if token.is_empty() {
        return Err(malformed(format!("RFC5424 {name} is empty")));
    }
    Ok((token, rest))
}

fn take_token_allow_terminal<'a>(
    input: &'a str,
    name: &str,
) -> Result<(&'a str, &'a str), ParseError> {
    if let Some((token, rest)) = input.split_once(' ') {
        if token.is_empty() {
            return Err(malformed(format!("RFC3164 {name} is empty")));
        }
        Ok((token, rest))
    } else if input.is_empty() {
        Err(malformed(format!("RFC3164 {name} is missing")))
    } else {
        Ok((input, ""))
    }
}

fn split_structured_data(input: &str) -> Result<(&str, &str), ParseError> {
    if let Some(rest) = input.strip_prefix('-') {
        return if rest.is_empty() {
            Ok(("-", ""))
        } else if let Some(message) = rest.strip_prefix(' ') {
            Ok(("-", message))
        } else {
            Err(malformed(
                "RFC5424 NILVALUE structured data must be followed by a space",
            ))
        };
    }
    if !input.starts_with('[') {
        return Err(malformed(
            "RFC5424 STRUCTURED-DATA must be '-' or start with '['",
        ));
    }

    let bytes = input.as_bytes();
    let mut index = 0_usize;
    while index < bytes.len() && bytes[index] == b'[' {
        index += 1;
        let mut in_quotes = false;
        let mut escaped = false;
        let mut closed = false;
        while index < bytes.len() {
            let byte = bytes[index];
            if in_quotes {
                if escaped {
                    escaped = false;
                } else if byte == b'\\' {
                    escaped = true;
                } else if byte == b'"' {
                    in_quotes = false;
                }
            } else if byte == b'"' {
                in_quotes = true;
            } else if byte == b']' {
                index += 1;
                closed = true;
                break;
            }
            index += 1;
        }
        if !closed || in_quotes || escaped {
            return Err(malformed("RFC5424 STRUCTURED-DATA is not balanced"));
        }
    }

    let structured_data = &input[..index];
    let rest = &input[index..];
    if rest.is_empty() {
        Ok((structured_data, ""))
    } else if let Some(message) = rest.strip_prefix(' ') {
        Ok((structured_data, message))
    } else {
        Err(malformed(
            "RFC5424 STRUCTURED-DATA must be followed by a space",
        ))
    }
}

fn parse_rfc3164_tag_and_message(input: &str) -> (Option<&str>, Option<&str>, &str) {
    let Some((tag, message)) = input.split_once(':') else {
        return (None, None, input);
    };
    let message = message.strip_prefix(' ').unwrap_or(message);
    if let Some(open) = tag.rfind('[')
        && tag.ends_with(']')
        && open > 0
    {
        let app = &tag[..open];
        let procid = &tag[open + 1..tag.len() - 1];
        if !procid.is_empty() {
            return (Some(app), Some(procid), message);
        }
    }
    if tag.is_empty() {
        (None, None, message)
    } else {
        (Some(tag), None, message)
    }
}

fn validate_rfc3164_timestamp(value: &str) -> Result<(), ParseError> {
    let bytes = value.as_bytes();
    if bytes.len() != 15
        || bytes[3] != b' '
        || bytes[6] != b' '
        || bytes[9] != b':'
        || bytes[12] != b':'
    {
        return Err(invalid_timestamp(
            "RFC3164 timestamp must match 'Mmm dd HH:MM:SS'",
        ));
    }
    let month = &bytes[..3];
    if !matches!(
        month,
        b"Jan"
            | b"Feb"
            | b"Mar"
            | b"Apr"
            | b"May"
            | b"Jun"
            | b"Jul"
            | b"Aug"
            | b"Sep"
            | b"Oct"
            | b"Nov"
            | b"Dec"
    ) {
        return Err(invalid_timestamp("invalid RFC3164 month"));
    }
    let day = parse_ascii_u8(&bytes[4..6], "day")?;
    let hour = parse_ascii_u8(&bytes[7..9], "hour")?;
    let minute = parse_ascii_u8(&bytes[10..12], "minute")?;
    let second = parse_ascii_u8(&bytes[13..15], "second")?;
    if !(1..=31).contains(&day) || hour > 23 || minute > 59 || second > 59 {
        return Err(invalid_timestamp(
            "RFC3164 timestamp component out of range",
        ));
    }
    Ok(())
}

fn parse_ascii_u8(bytes: &[u8], component: &str) -> Result<u8, ParseError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| invalid_timestamp(format!("invalid RFC3164 {component}")))?;
    text.trim()
        .parse::<u8>()
        .map_err(|_| invalid_timestamp(format!("invalid RFC3164 {component}")))
}

fn looks_like_rfc3164_timestamp_shape(input: &str) -> bool {
    let Some(timestamp) = input.as_bytes().get(..15) else {
        return false;
    };
    timestamp[3] == b' '
        && timestamp[6] == b' '
        && timestamp[9] == b':'
        && timestamp[12] == b':'
        && timestamp[..3].iter().all(u8::is_ascii_alphabetic)
}

fn token_value(value: &str) -> ParsedValue {
    if value == "-" {
        ParsedValue::Null
    } else {
        ParsedValue::String(value.to_string())
    }
}

fn json_timestamp_candidates(value: &Value) -> (Vec<TimestampCandidate>, Vec<String>) {
    let Some(object) = value.as_object() else {
        return (Vec::new(), Vec::new());
    };
    let mut candidates = Vec::new();
    let mut warnings = Vec::new();
    for field in ["event_time", "timestamp", "@timestamp"] {
        let Some(value) = object.get(field) else {
            continue;
        };
        match value {
            Value::String(text) => {
                if OffsetDateTime::parse(text, &Rfc3339).is_ok() {
                    candidates.push(TimestampCandidate {
                        value: text.clone(),
                        source_field: field.to_string(),
                        timezone_assumption: None,
                        precision: "rfc3339".to_string(),
                        confidence: "high".to_string(),
                    });
                } else {
                    candidates.push(TimestampCandidate {
                        value: text.clone(),
                        source_field: field.to_string(),
                        timezone_assumption: None,
                        precision: "source_text".to_string(),
                        confidence: "low".to_string(),
                    });
                    warnings.push(format!(
                        "JSON timestamp candidate {field} is not an absolute RFC3339 value; timezone is not assumed"
                    ));
                }
            }
            Value::Number(number) => {
                candidates.push(TimestampCandidate {
                    value: number.to_string(),
                    source_field: field.to_string(),
                    timezone_assumption: None,
                    precision: "source_numeric_unspecified_unit".to_string(),
                    confidence: "low".to_string(),
                });
                warnings.push(format!(
                    "JSON timestamp candidate {field} has no governed numeric unit; no event_time is inferred"
                ));
            }
            _ => {}
        }
    }
    (candidates, warnings)
}

fn validate_json_tree(value: &Value, limits: ParserLimits) -> Result<(), ParseError> {
    fn visit(
        value: &Value,
        limits: ParserLimits,
        depth: usize,
        field_count: &mut usize,
    ) -> Result<(), ParseError> {
        if depth > limits.max_depth {
            return Err(limit_exceeded(format!(
                "JSON nesting depth exceeds {}",
                limits.max_depth
            )));
        }
        match value {
            Value::Null | Value::Bool(_) => Ok(()),
            Value::Number(number) => {
                if number.to_string().len() > limits.max_string_bytes {
                    Err(limit_exceeded("JSON number representation is too large"))
                } else {
                    Ok(())
                }
            }
            Value::String(text) => {
                if text.len() > limits.max_string_bytes {
                    Err(limit_exceeded(format!(
                        "JSON string exceeds {} bytes",
                        limits.max_string_bytes
                    )))
                } else {
                    Ok(())
                }
            }
            Value::Array(values) => {
                if values.len() > limits.max_collection_length {
                    return Err(limit_exceeded(format!(
                        "JSON array exceeds {} elements",
                        limits.max_collection_length
                    )));
                }
                for value in values {
                    visit(value, limits, depth + 1, field_count)?;
                }
                Ok(())
            }
            Value::Object(object) => {
                if object.len() > limits.max_collection_length {
                    return Err(limit_exceeded(format!(
                        "JSON object exceeds {} fields",
                        limits.max_collection_length
                    )));
                }
                *field_count = field_count.saturating_add(object.len());
                if *field_count > limits.max_fields {
                    return Err(limit_exceeded(format!(
                        "JSON field count exceeds {}",
                        limits.max_fields
                    )));
                }
                for (key, value) in object {
                    if key.len() > limits.max_string_bytes {
                        return Err(limit_exceeded("JSON object key is too large"));
                    }
                    visit(value, limits, depth + 1, field_count)?;
                }
                Ok(())
            }
        }
    }

    let mut field_count = 0_usize;
    visit(value, limits, 0, &mut field_count)
}

fn json_to_parsed(value: &Value) -> ParsedValue {
    match value {
        Value::Null => ParsedValue::Null,
        Value::Bool(value) => ParsedValue::Bool(*value),
        Value::Number(value) => ParsedValue::Number(value.to_string()),
        Value::String(value) => ParsedValue::String(value.clone()),
        Value::Array(values) => ParsedValue::Array(values.iter().map(json_to_parsed).collect()),
        Value::Object(object) => ParsedValue::Object(
            object
                .iter()
                .map(|(key, value)| (key.clone(), json_to_parsed(value)))
                .collect::<BTreeMap<_, _>>(),
        ),
    }
}

fn parse_journald_field(
    field: &Value,
    limits: ParserLimits,
) -> Result<(ParsedValue, Option<TimestampCandidate>), ParseError> {
    let object = field
        .as_object()
        .ok_or_else(|| malformed("each journald field must be an object"))?;
    ensure_journald_keys(object, &["name", "encoding", "value"])?;
    let name = required_json_string(object, "name")?;
    if !valid_journal_field_name(name) {
        return Err(malformed(format!("invalid journald field name {name:?}")));
    }
    let encoding = required_json_string(object, "encoding")?;
    let raw_value = required_json_string(object, "value")?;
    let parsed_value = match encoding {
        "utf8" => ParsedValue::String(raw_value.to_string()),
        "base64" => {
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(raw_value.as_bytes())
                .map_err(|error| malformed(format!("invalid journald base64 value: {error}")))?;
            if decoded.len() > limits.max_string_bytes {
                return Err(limit_exceeded(format!(
                    "decoded journald field exceeds {} bytes",
                    limits.max_string_bytes
                )));
            }
            ParsedValue::Bytes(decoded)
        }
        other => {
            return Err(malformed(format!(
                "journald field encoding must be utf8 or base64, got {other:?}"
            )));
        }
    };

    let timestamp_candidate =
        if matches!(name, "__REALTIME_TIMESTAMP" | "_SOURCE_REALTIME_TIMESTAMP")
            && encoding == "utf8"
        {
            if raw_value.is_empty() || !raw_value.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(invalid_timestamp(format!(
                    "journald {name} must be Unix epoch microseconds"
                )));
            }
            Some(TimestampCandidate {
                value: raw_value.to_string(),
                source_field: name.to_string(),
                timezone_assumption: None,
                precision: "microsecond".to_string(),
                confidence: "high".to_string(),
            })
        } else {
            None
        };

    Ok((
        ParsedValue::object([
            ("name", ParsedValue::String(name.to_string())),
            ("encoding", ParsedValue::String(encoding.to_string())),
            ("value", parsed_value),
        ]),
        timestamp_candidate,
    ))
}

fn ensure_journald_keys(
    object: &serde_json::Map<String, Value>,
    allowed: &[&str],
) -> Result<(), ParseError> {
    if let Some(key) = object.keys().find(|key| !allowed.contains(&key.as_str())) {
        return Err(malformed(format!(
            "unexpected journald canonical member {key:?}"
        )));
    }
    Ok(())
}

fn required_json_string<'a>(
    object: &'a serde_json::Map<String, Value>,
    field: &str,
) -> Result<&'a str, ParseError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| malformed(format!("journald field {field} must be a string")))
}

fn valid_journal_field_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte == b'_' || byte.is_ascii_uppercase() || byte.is_ascii_digit())
}

fn ensure_input_limit(raw: &[u8], limits: ParserLimits) -> Result<(), ParseError> {
    if raw.len() > limits.max_input_bytes {
        Err(limit_exceeded(format!(
            "parser input exceeds {} bytes",
            limits.max_input_bytes
        )))
    } else {
        Ok(())
    }
}

fn trim_ascii_whitespace(mut value: &[u8]) -> &[u8] {
    while value.first().is_some_and(u8::is_ascii_whitespace) {
        value = &value[1..];
    }
    while value.last().is_some_and(u8::is_ascii_whitespace) {
        value = &value[..value.len() - 1];
    }
    value
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

fn malformed(message: impl Into<String>) -> ParseError {
    ParseError {
        code: "CER-PARSE-MALFORMED",
        message: message.into(),
        retryable: false,
    }
}

fn invalid_timestamp(message: impl Into<String>) -> ParseError {
    ParseError {
        code: "CER-PARSE-INVALID-TIMESTAMP",
        message: message.into(),
        retryable: false,
    }
}

fn limit_exceeded(message: impl Into<String>) -> ParseError {
    ParseError {
        code: "CER-PARSE-LIMIT-EXCEEDED",
        message: message.into(),
        retryable: false,
    }
}
