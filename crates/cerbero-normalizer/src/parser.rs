use std::fmt;

use cerbero_common::contracts::v1::RawEventPersisted;

pub const LINUX_SSHD_PARSER_ID: &str = "linux/sshd";
pub const LINUX_SSHD_PARSER_VERSION: &str = "1";

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
pub struct ParsedEvent {
    pub parser_id: String,
    pub parser_version: String,
    pub status: ParsingStatus,
    pub username: String,
    pub source_ip: String,
    pub source_port: u16,
    pub authentication_succeeded: bool,
    pub invalid_user: bool,
    pub message: String,
    pub timestamp_candidates: Vec<TimestampCandidate>,
    pub warnings: Vec<String>,
}

pub struct ParserInput<'a> {
    pub raw: &'a [u8],
    pub persisted: &'a RawEventPersisted,
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
    fn probe(&self, input: &ParserInput<'_>) -> u8;
    fn parse(&self, input: &ParserInput<'_>) -> Result<ParsedEvent, ParseError>;
}

#[derive(Default)]
pub struct ParserRegistry {
    parsers: Vec<Box<dyn Parser>>,
}

impl ParserRegistry {
    #[must_use]
    pub fn with_defaults() -> Self {
        Self {
            parsers: vec![Box::new(LinuxSshdParser)],
        }
    }

    pub fn register(&mut self, parser: Box<dyn Parser>) {
        self.parsers.push(parser);
    }

    pub fn select(&self, input: &ParserInput<'_>) -> Result<&dyn Parser, ParseError> {
        let mut scored: Vec<_> = self
            .parsers
            .iter()
            .map(|parser| (parser.probe(input), parser.as_ref()))
            .filter(|(score, _)| *score > 0)
            .collect();
        scored.sort_by(|left, right| {
            right
                .0
                .cmp(&left.0)
                .then_with(|| left.1.id().cmp(right.1.id()))
        });
        let Some((best_score, best)) = scored.first().copied() else {
            return Err(ParseError {
                code: "CER-NORM-PARSER-UNSUPPORTED",
                message: "no registered parser supports the input".to_string(),
                retryable: false,
            });
        };
        if scored.get(1).is_some_and(|(score, _)| *score == best_score) {
            return Err(ParseError {
                code: "CER-NORM-PARSER-AMBIGUOUS",
                message: format!("multiple parsers returned probe score {best_score}"),
                retryable: false,
            });
        }
        Ok(best)
    }
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
        } else if text.contains("sshd") {
            40
        } else {
            0
        }
    }

    fn parse(&self, input: &ParserInput<'_>) -> Result<ParsedEvent, ParseError> {
        let text = std::str::from_utf8(input.raw).map_err(|error| ParseError {
            code: "CER-NORM-INVALID-ENCODING",
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

    Ok(ParsedEvent {
        parser_id: LINUX_SSHD_PARSER_ID.to_string(),
        parser_version: LINUX_SSHD_PARSER_VERSION.to_string(),
        status: ParsingStatus::Success,
        username: username.to_string(),
        source_ip: source_ip.to_string(),
        source_port,
        authentication_succeeded: false,
        invalid_user,
        message: format!("SSH authentication failed for user {username} from {source_ip}"),
        timestamp_candidates: Vec::new(),
        warnings: Vec::new(),
    })
}

fn malformed<T>(message: &str) -> Result<T, ParseError> {
    Err(malformed_error(message))
}

fn malformed_error(message: &str) -> ParseError {
    ParseError {
        code: "CER-NORM-PARSE-FAILED",
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
        }
    }

    #[test]
    fn sshd_parser_handles_governed_vertical_fixture() {
        let persisted = RawEventPersisted::default();
        let raw = b"Failed password for invalid user admin from 10.0.0.8 port 50341 ssh2";
        let parser = LinuxSshdParser;
        let parsed = parser.parse(&input(raw, &persisted)).unwrap();
        assert_eq!(parsed.username, "admin");
        assert_eq!(parsed.source_ip, "10.0.0.8");
        assert_eq!(parsed.source_port, 50341);
        assert!(parsed.invalid_user);
        assert!(!parsed.authentication_succeeded);
    }

    #[test]
    fn malformed_sshd_is_permanent() {
        let persisted = RawEventPersisted::default();
        let error = LinuxSshdParser
            .parse(&input(b"Failed password for admin", &persisted))
            .unwrap_err();
        assert_eq!(error.code, "CER-NORM-PARSE-FAILED");
        assert!(!error.retryable);
    }

    #[test]
    fn registry_selection_is_explainable_and_deterministic() {
        let persisted = RawEventPersisted::default();
        let registry = ParserRegistry::with_defaults();
        let parser = registry
            .select(&input(
                b"Failed password for admin from 10.0.0.8 port 22 ssh2",
                &persisted,
            ))
            .unwrap();
        assert_eq!(parser.id(), LINUX_SSHD_PARSER_ID);
        assert_eq!(parser.version(), LINUX_SSHD_PARSER_VERSION);
    }
}
