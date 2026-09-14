use std::panic::{AssertUnwindSafe, catch_unwind};
use std::{fs, path::PathBuf};

use cerbero_common::contracts::v1::RawEventPersisted;
use cerbero_normalizer::{ParserInput, ParserRegistry};

fn run_one(raw: &[u8], content_type: &str, parser_version: &str) {
    let original = raw.to_vec();
    let persisted = RawEventPersisted {
        content_type: content_type.to_string(),
        ..Default::default()
    };
    let registry = ParserRegistry::with_linux_sshd_version(parser_version).unwrap();
    let input = ParserInput {
        raw,
        persisted: &persisted,
        configured_parser_id: None,
    };
    let outcome = catch_unwind(AssertUnwindSafe(|| registry.parse(&input)));
    assert!(
        outcome.is_ok(),
        "parser registry panicked for version {parser_version} and {} bytes",
        raw.len()
    );
    assert_eq!(raw, original.as_slice(), "parser mutated raw input");
}

#[test]
fn tracked_fuzz_corpus_never_panics_or_mutates_raw() {
    let corpus = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fuzz");
    let mut paths = fs::read_dir(corpus)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    paths.sort();

    for path in paths {
        let raw = fs::read(&path).unwrap();
        for version in ["1", "2"] {
            for content_type in ["text/plain", "application/json", "application/octet-stream"] {
                run_one(&raw, content_type, version);
            }
        }
    }
}

#[test]
fn deterministic_arbitrary_byte_fuzz_smoke_never_panics_or_mutates_raw() {
    let mut state = 0xC3E2_BA5E_5EED_1701_u64;
    for case in 0..1024_usize {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let len = usize::try_from(state % 513).unwrap();
        let mut raw = Vec::with_capacity(len);
        for index in 0..len {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            raw.push(u8::try_from((state ^ u64::try_from(index).unwrap()) & 0xff).unwrap());
        }
        let content_type = match case % 3 {
            0 => "text/plain",
            1 => "application/json",
            _ => "application/octet-stream",
        };
        run_one(&raw, content_type, "1");
        run_one(&raw, content_type, "2");
    }
}

#[test]
fn source_specific_parser_boundary_stays_contained() {
    let mut raw = b"Failed password for invalid user admin from 10.0.0.8 port 22 ssh2".to_vec();
    raw.resize(65_537, b'x');
    for version in ["1", "2"] {
        run_one(&raw, "text/plain", version);
    }
}
