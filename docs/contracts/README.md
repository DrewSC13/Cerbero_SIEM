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

`make contracts` performs repository contract-root verification, semantic field/number checks, Buf lint, and Buf build. `make contracts-generate` creates the Rust and Go bindings from the same canonical source.

Binding validation, serialization/deserialization fixtures, UUIDv7/timestamp/hash validators, duplicate/replay/parser-failure/hash-mismatch tests are completed in the generated-binding portion of Milestone 1 before the milestone is merged.
