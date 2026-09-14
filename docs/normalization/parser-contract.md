# Milestone 4 parser contract and minimum parser set

This increment expands the internal parser layer without changing the durable first OCSF vertical. Parsing remains separate from normalization:

```text
raw bytes + RawEventPersisted context
  -> explainable parser selection
  -> versioned parser
  -> ParsedEvent
  -> versioned OCSF mapping when one exists
```

A valid parse does not imply a valid OCSF normalization. At this stage only `linux/sshd@1` has a governed OCSF mapping. Generic JSON, RFC3164, RFC5424, and journald canonical inputs can produce `ParsedEvent`, but the normalizer must fail permanently at mapping selection rather than emit a false `NormalizedEvent SUCCESS` when no mapping exists.

## Minimum parser identities

| Family | Parser ID | Version | Selection tier |
| --- | --- | ---: | --- |
| Linux SSH authentication | `linux/sshd` | 1 | source-specific |
| Journald canonical | `cerbero.parser.journald.canonical` | 1 | source-specific |
| Syslog RFC5424 | `cerbero.parser.syslog.rfc5424` | 1 | content/protocol |
| Syslog RFC3164-compatible | `cerbero.parser.syslog.rfc3164` | 1 | content/protocol |
| Generic JSON | `cerbero.parser.json.generic` | 1 | generic |

The existing `linux/sshd@1` identity remains unchanged because it is already part of the first historical OCSF derivation contract. New parser families use the canonical `cerbero.parser.<family>.<name>` namespace.

## ParsedEvent

`ParsedEvent` is internal and contains:

```text
parser_id
parser_version
source_event_type
fields                 typed ParsedValue tree
timestamp_candidates
warnings
parsing_status
selection              candidate/score/policy trace
```

`ParsedValue` distinguishes null, bool, exact number text, UTF-8 string, byte string, array, and object. JSON numbers are parsed with `serde_json` arbitrary-precision support and retained as their exact number representation rather than silently converting through `f64`.

## Parser selection

The automatic selection hierarchy is explicit:

```text
configured parser
  -> source-specific parser
  -> content/protocol parser
  -> generic parser
  -> unsupported
```

When no parser is configured, every registered parser is probed. Selection first compares tier and then confidence inside that tier. Equal highest tier/confidence is ambiguous and fails closed with `CER-PARSE-NO-MATCH`; registration order is never a silent tie-breaker.

A higher-tier parser must return zero confidence unless it recognizes an input that it can actually parse. Process or application-name hints alone, such as the string `sshd` inside a syslog envelope, are insufficient to preempt a content/protocol parser.

The resulting `ParsedEvent` records candidate parser IDs/versions, tier, confidence, the selected parser, and the selection policy. Source configuration is not yet wired into the DEVELOPMENT runtime, so current runtime calls automatic selection with no configured parser. The registry API already accepts a configured parser ID for the future governed Source configuration path.

## Timestamp candidates

Parsers may expose candidate source timestamps but never mutate `ingest_time`.

- RFC5424 requires an absolute RFC3339 timestamp when TIMESTAMP is not NILVALUE. No timezone assumption is recorded because the offset is explicit.
- RFC3164 contains neither year nor timezone. It is therefore `PARTIAL`, records the original timestamp text as a candidate, keeps `timezone_assumption = None`, and emits a warning that UTC was not assumed.
- Journald `__REALTIME_TIMESTAMP` and `_SOURCE_REALTIME_TIMESTAMP` are absolute Unix epoch microseconds and require no timezone assumption.
- Generic JSON only treats top-level `event_time`, `timestamp`, or `@timestamp` as candidates. Absolute RFC3339 strings are high confidence; ambiguous strings or numeric values remain low-confidence candidates with warnings and do not invent a timezone or numeric unit.

ADR-0014 now governs Source-time resolution for supported ambiguous candidates. The normalizer may apply an explicit per-`source_id` fixed UTC offset and records the assumption; without a matching policy the candidate remains unresolved. No global normalizer timezone exists and UTC is never assumed.

## Parser safety limits

The architecture leaves global numeric parser limits open. The following are implementation guardrails for parser version 1, not global production policy:

| Parser family | Maximum input | Max depth | Max fields | Max string/decoded bytes | Max collection |
| --- | ---: | ---: | ---: | ---: | ---: |
| JSON / journald canonical | 1 MiB | 64 | 8192 | 256 KiB | 4096 |
| Syslog / linux-sshd | 64 KiB | 8 | 128 or less | 64 KiB | 128 or less |

Limit violations fail permanently with `CER-PARSE-LIMIT-EXCEEDED`. Parser execution through the registry is wrapped with panic containment and converts an unwind into `CER-PARSE-INTERNAL`; one bad event must not terminate the normalizer process when Rust unwinding is available.

Exact memory and wall-clock sandbox limits remain open architectural decisions and are not silently frozen here.

## Stable initial parser errors

The implementation uses the governed initial families:

```text
CER-PARSE-NO-MATCH
CER-PARSE-MALFORMED
CER-PARSE-LIMIT-EXCEEDED
CER-PARSE-INVALID-TIMESTAMP
CER-PARSE-UNSUPPORTED-VERSION
CER-PARSE-INTERNAL
```

These failures are non-retryable at the parser layer. Raw evidence remains preserved. Governed normalization DLQ payloads, metrics, fuzz targets, replay/renormalization, and additional OCSF mappings remain later Milestone 4 increments.

## Journald canonical v1

ADR-0012 governs the byte-safe JSON representation accepted by `cerbero.parser.journald.canonical`. The ordered `fields` array supports repeated names, `utf8` preserves exact text, and `base64` round-trips binary bytes. Version 1 rejects unknown canonical members instead of silently discarding them. The cursor is preserved as source metadata and is not treated as CERBERO sequence identity.
