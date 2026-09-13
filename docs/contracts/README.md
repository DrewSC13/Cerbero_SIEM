# Contract governance

Milestone 1 introduces the first real CERBERO wire contracts under `schemas/protobuf/cerbero/contracts/v1/`.

## Canonical v1 objects

The governed source defines:

- `CerberoEnvelope` and `Producer`;
- `RawEvent`;
- `NormalizedEvent`;
- `Transformation`;
- `CerberoError`;
- shared integrity, normalization, transformation, execution-mode, and error-category enums.

The envelope contract version is `"1"`. Schema names are versioned, and incompatible semantic changes require a new major contract version rather than reinterpretation of existing fields.

## Locked invariants represented in the schema

- CERBERO-owned identifiers use UUIDv7 where the baseline requires them.
- Protobuf timestamps represent internal time values; REST/JSON uses RFC 3339 UTC.
- `event_time` is source-declared metadata and remains distinct from `ingest_time`.
- `RawEvent.raw_payload` contains exact received bytes.
- Raw evidence SHA-256 is calculated before decoding, parsing, whitespace changes, line-ending conversion, or character conversion.
- `sequence_number` remains optional and is never fabricated for a source that does not provide one.
- `RawEvent` remains immutable and may have multiple historical `NormalizedEvent` derivations.
- `Transformation` records provenance and distinguishes `LIVE`, `REPLAY`, and `TEST` execution.
- Error codes are stable machine identifiers; human `message` text is not a programmatic identifier.
- Retryability is explicit and is reserved for failures classified as transient by the owning workflow.

## TransformationStatus closure

CONTRACTS v1.0 references `TransformationStatus` without assigning enum values. ADR-0005 closes that gap minimally with `UNSPECIFIED`, `SUCCESS`, and `FAILED`; it does not add speculative lifecycle states. New enums use type-prefixed Protobuf symbols to avoid package-scope collisions. Locked `IntegrityStatus` and `ErrorCategory` symbols retain their exact original names.

## Tooling

Buf `1.72.0` is pinned for schema checks. The generation template pins:

- Go protobuf plugin `v1.36.12`;
- `neoeinstein-prost` plugin `v0.5.0`.

`make contracts` performs repository contract-root verification, semantic field/number checks, Buf lint/build, and a pinned regeneration drift check. `make contracts-generate` creates the Rust and Go bindings from the same canonical source. Generated output is committed but never edited by hand.

## Runtime validation

ADR-0006 keeps runtime validation outside generated files. Rust exposes generated types through `cerbero_common::contracts::v1`; Go exposes generated types from `cerbero/services/internal/contracts/v1` and keeps validators in the sibling `validation` package.

M1 runtime validation covers UUIDv7 syntax, Protobuf timestamp ranges, envelope v1 identity/producer/payload presence, exact raw byte count, lowercase SHA-256 over exact raw bytes, and transformation status/execution-mode/error provenance. Source-declared `event_time` is validated when present but never fabricated.

The exact normalized-hash field scope and first OCSF version remain open in the parsing/OCSF baseline, so M1 deliberately does not add policy for them.

The shared wire fixture under `tests/fixtures/contracts/v1/` is decoded and re-encoded by both Rust and Go to detect language-binding incompatibility. Duplicate delivery, replay, parser failure, invalid raw metadata, and hash mismatch are covered by executable tests.
