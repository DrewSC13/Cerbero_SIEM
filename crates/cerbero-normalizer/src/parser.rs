use std::collections::BTreeMap;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};

use cerbero_common::contracts::v1::RawEventPersisted;

use crate::parser_formats::{
    GenericJsonParser, JournaldCanonicalParser, SyslogRfc3164Parser, SyslogRfc5424Parser,
};

pub const LINUX_SSHD_PARSER_ID: &str = "linux/sshd";
pub const LINUX_SSHD_PARSER_VERSION: &str = "1";

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ParserTier {
    Generic,
    ContentProtocol,
    SourceSpecific,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParserLimits {
    pub max_input_bytes: usize,
    pub max_depth: usize,
    pub max_fields: usize,
    pub max_string_bytes: usize,
    pub max_collection_length: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParsingStatus {
    Success,
    Partial,
    Failed,
    Unsupported,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TimestampCandidate {
    pub value: String,
    pub source_field: String,
    pub timezone_assumption: Option<String>,
    pub precision: String,
    pub confidence: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParsedValue {
    Null,
    Bool(bool),
    Number(String),
    String(String),
    Bytes(Vec<u8>),
    Array(Vec<ParsedValue>),
    Object(BTreeMap<String, ParsedValue>),
}

impl ParsedValue {
    #[must_use]
    pub fn object<K, I>(entries: I) -> Self
    where
        K: Into<String>,
        I: IntoIterator<Item = (K, ParsedValue)>,
    {
        Self::Object(
            entries
                .into_iter()
                .map(|(key, value)| (key.into(), value))
                .collect(),
        )
    }

    #[must_use]
    pub fn get(&self, key: &str) -> Option<&Self> {
        self.as_object()?.get(key)
    }

    #[must_use]
    pub fn as_object(&self) -> Option<&BTreeMap<String, ParsedValue>> {
        match self {
            Self::Object(value) => Some(value),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_array(&self) -> Option<&[ParsedValue]> {
        match self {
            Self::Array(value) => Some(value),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(value) => Some(*value),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_number_str(&self) -> Option<&str> {
        match self {
            Self::Number(value) => Some(value),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_u64(&self) -> Option<u64> {
        self.as_number_str()?.parse().ok()
    }

    #[must_use]
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Bytes(value) => Some(value),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParserCandidate {
    pub parser_id: String,
    pub parser_version: String,
    pub tier: ParserTier,
    pub confidence: u8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParserSelectionTrace {
    pub policy: String,
    pub candidates: Vec<ParserCandidate>,
    pub selected_parser_id: String,
    pub selected_parser_version: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedEvent {
    pub parser_id: String,
    pub parser_version: String,
    pub source_event_type: String,
    pub fields: ParsedValue,
    pub timestamp_candidates: Vec<TimestampCandidate>,
    pub warnings: Vec<String>,
    pub status: ParsingStatus,
    pub selection: Option<ParserSelectionTrace>,
}

impl ParsedEvent {
    #[must_use]
    pub fn field(&self, key: &str) -> Option<&ParsedValue> {
        self.fields.get(key)
    }

    #[must_use]
    pub fn string_field(&self, key: &str) -> Option<&str> {
        self.field(key)?.as_str()
    }

    #[must_use]
    pub fn bool_field(&self, key: &str) -> Option<bool> {
        self.field(key)?.as_bool()
    }

    #[must_use]
    pub fn u64_field(&self, key: &str) -> Option<u64> {
        self.field(key)?.as_u64()
    }
}

pub struct ParserInput<'a> {
    pub raw: &'a [u8],
    pub persisted: &'a RawEventPersisted,
    pub configured_parser_id: Option<&'a str>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParseError {
    pub code: &'static str,
    pub message: String,
    pub retryable: bool,
}

impl fmt::Display for ParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for ParseError {}

pub trait Parser: Send + Sync {
    fn id(&self) -> &'static str;
    fn version(&self) -> &'static str;
    fn tier(&self) -> ParserTier;
    fn limits(&self) -> ParserLimits;
    fn probe(&self, input: &ParserInput<'_>) -> u8;
    fn parse(&self, input: &ParserInput<'_>) -> Result<ParsedEvent, ParseError>;
}

pub struct ParserSelection<'a> {
    parser: &'a dyn Parser,
    trace: ParserSelectionTrace,
}

impl ParserSelection<'_> {
    #[must_use]
    pub fn parser(&self) -> &dyn Parser {
        self.parser
    }

    #[must_use]
    pub fn trace(&self) -> &ParserSelectionTrace {
        &self.trace
    }
}

#[derive(Default)]
pub struct ParserRegistry {
    parsers: Vec<Box<dyn Parser>>,
}

impl ParserRegistry {
    #[must_use]
    pub fn with_defaults() -> Self {
        Self {
            parsers: vec![
                Box::new(LinuxSshdParser),
                Box::new(JournaldCanonicalParser),
                Box::new(SyslogRfc5424Parser),
                Box::new(SyslogRfc3164Parser),
                Box::new(GenericJsonParser),
            ],
        }
    }

    pub fn register(&mut self, parser: Box<dyn Parser>) {
        self.parsers.push(parser);
    }

    pub fn select(&self, input: &ParserInput<'_>) -> Result<ParserSelection<'_>, ParseError> {
        if let Some(configured) = input.configured_parser_id {
            let parser = self
                .parsers
                .iter()
                .map(Box::as_ref)
                .find(|parser| parser.id() == configured)
                .ok_or_else(|| ParseError {
                    code: "CER-PARSE-NO-MATCH",
                    message: format!("configured parser {configured} is not registered"),
                    retryable: false,
                })?;
            let confidence = probe_parser(parser, input)?;
            return Ok(ParserSelection {
                parser,
                trace: ParserSelectionTrace {
                    policy: "configured-parser".to_string(),
                    candidates: vec![candidate(parser, confidence)],
                    selected_parser_id: parser.id().to_string(),
                    selected_parser_version: parser.version().to_string(),
                },
            });
        }

        let mut candidates = Vec::with_capacity(self.parsers.len());
        for parser in &self.parsers {
            candidates.push(candidate(
                parser.as_ref(),
                probe_parser(parser.as_ref(), input)?,
            ));
        }
        let mut best: Option<(usize, ParserTier, u8)> = None;
        let mut tied = Vec::new();
        for (index, entry) in candidates.iter().enumerate() {
            if entry.confidence == 0 {
                continue;
            }
            let key = (entry.tier, entry.confidence);
            match best {
                None => {
                    best = Some((index, entry.tier, entry.confidence));
                    tied.clear();
                    tied.push(index);
                }
                Some((_, incumbent_tier, incumbent_confidence))
                    if key > (incumbent_tier, incumbent_confidence) =>
                {
                    best = Some((index, entry.tier, entry.confidence));
                    tied.clear();
                    tied.push(index);
                }
                Some((_, incumbent_tier, incumbent_confidence))
                    if key == (incumbent_tier, incumbent_confidence) =>
                {
                    tied.push(index);
                }
                _ => {}
            }
        }

        let Some((best_index, _, _)) = best else {
            return Err(ParseError {
                code: "CER-PARSE-NO-MATCH",
                message: "no registered parser supports the input".to_string(),
                retryable: false,
            });
        };
        if tied.len() > 1 {
            let ids = tied
                .iter()
                .map(|index| candidates[*index].parser_id.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(ParseError {
                code: "CER-PARSE-NO-MATCH",
                message: format!(
                    "parser selection is ambiguous at the highest tier/confidence: {ids}"
                ),
                retryable: false,
            });
        }

        let parser = self.parsers[best_index].as_ref();
        Ok(ParserSelection {
            parser,
            trace: ParserSelectionTrace {
                policy: "auto:tier-then-confidence".to_string(),
                candidates,
                selected_parser_id: parser.id().to_string(),
                selected_parser_version: parser.version().to_string(),
            },
        })
    }

    pub fn parse(&self, input: &ParserInput<'_>) -> Result<ParsedEvent, ParseError> {
        let selection = self.select(input)?;
        let trace = selection.trace().clone();
        let parsed = catch_unwind(AssertUnwindSafe(|| selection.parser().parse(input))).map_err(
            |_| ParseError {
                code: "CER-PARSE-INTERNAL",
                message: format!("parser {} panicked", selection.parser().id()),
                retryable: false,
            },
        )??;
        if parsed.parser_id != selection.parser().id()
            || parsed.parser_version != selection.parser().version()
        {
            return Err(ParseError {
                code: "CER-PARSE-INTERNAL",
                message: "parser output identity differs from selected parser".to_string(),
                retryable: false,
            });
        }
        Ok(ParsedEvent {
            selection: Some(trace),
            ..parsed
        })
    }
}

fn candidate(parser: &dyn Parser, confidence: u8) -> ParserCandidate {
    ParserCandidate {
        parser_id: parser.id().to_string(),
        parser_version: parser.version().to_string(),
        tier: parser.tier(),
        confidence,
    }
}

fn probe_parser(parser: &dyn Parser, input: &ParserInput<'_>) -> Result<u8, ParseError> {
    catch_unwind(AssertUnwindSafe(|| parser.probe(input))).map_err(|_| ParseError {
        code: "CER-PARSE-INTERNAL",
        message: format!("parser {} panicked while probing", parser.id()),
        retryable: false,
    })
}

#[derive(Clone, Copy, Debug, Default)]
pub struct LinuxSshdParser;

impl Parser for LinuxSshdParser {
    fn id(&self) -> &'static str {
        LINUX_SSHD_PARSER_ID
    }

    fn version(&self) -> &'static str {
        LINUX_SSHD_PARSER_VERSION
    }

    fn tier(&self) -> ParserTier {
        ParserTier::SourceSpecific
    }

    fn limits(&self) -> ParserLimits {
        ParserLimits {
            max_input_bytes: 65_536,
            max_depth: 8,
            max_fields: 64,
            max_string_bytes: 65_536,
            max_collection_length: 64,
        }
    }

    fn probe(&self, input: &ParserInput<'_>) -> u8 {
        let Ok(text) = std::str::from_utf8(input.raw) else {
            return 0;
        };
        let text = text.trim();
        if text.contains("Failed password for ")
            && text.contains(" from ")
            && text.contains(" port ")
        {
            100
        } else {
            0
        }
    }

    fn parse(&self, input: &ParserInput<'_>) -> Result<ParsedEvent, ParseError> {
        if input.raw.len() > self.limits().max_input_bytes {
            return Err(ParseError {
                code: "CER-PARSE-LIMIT-EXCEEDED",
                message: format!(
                    "linux/sshd input exceeds {} bytes",
                    self.limits().max_input_bytes
                ),
                retryable: false,
            });
        }
        let text = std::str::from_utf8(input.raw).map_err(|error| ParseError {
            code: "CER-PARSE-MALFORMED",
            message: format!("linux/sshd v1 requires UTF-8 input: {error}"),
            retryable: false,
        })?;
        parse_failed_password(text.trim())
    }
}

fn parse_failed_password(text: &str) -> Result<ParsedEvent, ParseError> {
    let marker = "Failed password for ";
    let Some(after_prefix) = text.find(marker).map(|index| &text[index + marker.len()..]) else {
        return malformed("missing 'Failed password for' marker");
    };
    let (identity, source) = after_prefix
        .split_once(" from ")
        .ok_or_else(|| malformed_error("missing source address"))?;
    let (source_ip, port_and_rest) = source
        .split_once(" port ")
        .ok_or_else(|| malformed_error("missing source port"))?;
    let source_port_text = port_and_rest
        .split_whitespace()
        .next()
        .ok_or_else(|| malformed_error("empty source port"))?;
    let source_port = source_port_text
        .parse::<u16>()
        .map_err(|_| malformed_error("source port is not a valid unsigned 16-bit integer"))?;

    let (invalid_user, username) = identity
        .strip_prefix("invalid user ")
        .map_or((false, identity), |user| (true, user));
    if username.is_empty() || source_ip.is_empty() {
        return malformed("username and source address are required");
    }
    let message = format!("SSH authentication failed for user {username} from {source_ip}");

    Ok(ParsedEvent {
        parser_id: LINUX_SSHD_PARSER_ID.to_string(),
        parser_version: LINUX_SSHD_PARSER_VERSION.to_string(),
        source_event_type: "linux.ssh.authentication".to_string(),
        fields: ParsedValue::object([
            ("username", ParsedValue::String(username.to_string())),
            ("source_ip", ParsedValue::String(source_ip.to_string())),
            ("source_port", ParsedValue::Number(source_port.to_string())),
            ("authentication_succeeded", ParsedValue::Bool(false)),
            ("invalid_user", ParsedValue::Bool(invalid_user)),
            ("message", ParsedValue::String(message)),
        ]),
        timestamp_candidates: Vec::new(),
        warnings: Vec::new(),
        status: ParsingStatus::Success,
        selection: None,
    })
}

fn malformed<T>(message: &str) -> Result<T, ParseError> {
    Err(malformed_error(message))
}

fn malformed_error(message: &str) -> ParseError {
    ParseError {
        code: "CER-PARSE-MALFORMED",
        message: message.to_string(),
        retryable: false,
    }
}

#[cfg(test)]
mod tests {
    use cerbero_common::contracts::v1::RawEventPersisted;

    use super::*;

    fn input<'a>(bytes: &'a [u8], persisted: &'a RawEventPersisted) -> ParserInput<'a> {
        ParserInput {
            raw: bytes,
            persisted,
            configured_parser_id: None,
        }
    }

    #[test]
    fn sshd_parser_handles_governed_vertical_fixture() {
        let persisted = RawEventPersisted::default();
        let raw = b"Failed password for invalid user admin from 10.0.0.8 port 50341 ssh2";
        let parser = LinuxSshdParser;
        let parsed = parser.parse(&input(raw, &persisted)).unwrap();
        assert_eq!(parsed.string_field("username"), Some("admin"));
        assert_eq!(parsed.string_field("source_ip"), Some("10.0.0.8"));
        assert_eq!(parsed.u64_field("source_port"), Some(50341));
        assert_eq!(parsed.bool_field("invalid_user"), Some(true));
        assert_eq!(parsed.bool_field("authentication_succeeded"), Some(false));
    }

    #[test]
    fn malformed_sshd_is_permanent() {
        let persisted = RawEventPersisted::default();
        let error = LinuxSshdParser
            .parse(&input(b"Failed password for admin", &persisted))
            .unwrap_err();
        assert_eq!(error.code, "CER-PARSE-MALFORMED");
        assert!(!error.retryable);
    }

    #[test]
    fn registry_selection_is_explainable_and_deterministic() {
        let persisted = RawEventPersisted::default();
        let registry = ParserRegistry::with_defaults();
        let selection = registry
            .select(&input(
                b"Failed password for admin from 10.0.0.8 port 22 ssh2",
                &persisted,
            ))
            .unwrap();
        assert_eq!(selection.parser().id(), LINUX_SSHD_PARSER_ID);
        assert_eq!(selection.parser().version(), LINUX_SSHD_PARSER_VERSION);
        assert_eq!(selection.trace().policy, "auto:tier-then-confidence");
    }

    #[test]
    fn configured_parser_has_priority() {
        let persisted = RawEventPersisted {
            content_type: "application/json".to_string(),
            ..Default::default()
        };
        let raw = br#"{"cerbero_journald_version":1,"fields":[]}"#;
        let registry = ParserRegistry::with_defaults();
        let configured = ParserInput {
            raw,
            persisted: &persisted,
            configured_parser_id: Some(crate::GENERIC_JSON_PARSER_ID),
        };
        let selection = registry.select(&configured).unwrap();
        assert_eq!(selection.parser().id(), crate::GENERIC_JSON_PARSER_ID);
        assert_eq!(selection.trace().policy, "configured-parser");
    }

    #[test]
    fn equal_highest_candidates_fail_closed() {
        #[derive(Clone, Copy)]
        struct TieParser(&'static str);

        impl Parser for TieParser {
            fn id(&self) -> &'static str {
                self.0
            }

            fn version(&self) -> &'static str {
                "1"
            }

            fn tier(&self) -> ParserTier {
                ParserTier::Generic
            }

            fn limits(&self) -> ParserLimits {
                ParserLimits {
                    max_input_bytes: 64,
                    max_depth: 1,
                    max_fields: 1,
                    max_string_bytes: 64,
                    max_collection_length: 1,
                }
            }

            fn probe(&self, _input: &ParserInput<'_>) -> u8 {
                50
            }

            fn parse(&self, _input: &ParserInput<'_>) -> Result<ParsedEvent, ParseError> {
                unreachable!("selection must fail before parsing")
            }
        }

        let persisted = RawEventPersisted::default();
        let mut registry = ParserRegistry::default();
        registry.register(Box::new(TieParser("test/a")));
        registry.register(Box::new(TieParser("test/b")));
        let Err(error) = registry.select(&input(b"x", &persisted)) else {
            panic!("ambiguous parser selection unexpectedly succeeded");
        };
        assert_eq!(error.code, "CER-PARSE-NO-MATCH");
        assert!(error.message.contains("ambiguous"));
    }
    #[test]
    fn parser_panics_are_contained() {
        #[derive(Clone, Copy)]
        struct PanicParser;

        impl Parser for PanicParser {
            fn id(&self) -> &'static str {
                "test/panic"
            }

            fn version(&self) -> &'static str {
                "1"
            }

            fn tier(&self) -> ParserTier {
                ParserTier::SourceSpecific
            }

            fn limits(&self) -> ParserLimits {
                ParserLimits {
                    max_input_bytes: 64,
                    max_depth: 1,
                    max_fields: 1,
                    max_string_bytes: 64,
                    max_collection_length: 1,
                }
            }

            fn probe(&self, _input: &ParserInput<'_>) -> u8 {
                100
            }

            fn parse(&self, _input: &ParserInput<'_>) -> Result<ParsedEvent, ParseError> {
                panic!("parser bug")
            }
        }

        let persisted = RawEventPersisted::default();
        let mut registry = ParserRegistry::default();
        registry.register(Box::new(PanicParser));
        let error = registry.parse(&input(b"x", &persisted)).unwrap_err();
        assert_eq!(error.code, "CER-PARSE-INTERNAL");
        assert!(error.message.contains("panicked"));
    }

    #[test]
    fn probe_panics_are_contained() {
        #[derive(Clone, Copy)]
        struct PanicProbeParser;

        impl Parser for PanicProbeParser {
            fn id(&self) -> &'static str {
                "test/panic-probe"
            }

            fn version(&self) -> &'static str {
                "1"
            }

            fn tier(&self) -> ParserTier {
                ParserTier::Generic
            }

            fn limits(&self) -> ParserLimits {
                ParserLimits {
                    max_input_bytes: 64,
                    max_depth: 1,
                    max_fields: 1,
                    max_string_bytes: 64,
                    max_collection_length: 1,
                }
            }

            fn probe(&self, _input: &ParserInput<'_>) -> u8 {
                panic!("probe bug")
            }

            fn parse(&self, _input: &ParserInput<'_>) -> Result<ParsedEvent, ParseError> {
                unreachable!("probe must fail first")
            }
        }

        let persisted = RawEventPersisted::default();
        let mut registry = ParserRegistry::default();
        registry.register(Box::new(PanicProbeParser));
        let Err(error) = registry.select(&input(b"x", &persisted)) else {
            panic!("panicking probe unexpectedly selected");
        };
        assert_eq!(error.code, "CER-PARSE-INTERNAL");
        assert!(error.message.contains("probing"));
    }
}
