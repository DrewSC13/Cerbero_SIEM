# ADR-0006 — Contract v1 runtime validation

## Status

Accepted

## Context

CONTRACTS v1.0 fixes the wire fields and several semantic invariants, but generated Protobuf bindings alone do not enforce those invariants. In particular, the baseline requires UUIDv7 identities, valid timestamps, SHA-256 over exact raw bytes, immutable RawEvent evidence, stable message identity across redelivery, explicit LIVE/REPLAY/TEST execution provenance, and preserved error provenance for failed transformations.

The generated Rust and Go bindings must remain generated artifacts rather than separate hand-maintained domain models. Validation therefore needs to wrap the generated types without editing generated files.

The ingest baseline also defines `raw_size` alongside `raw_payload` and requires the exact accepted bytes to be recoverable and to reproduce the stored SHA-256. Runtime validation must reject self-inconsistent raw metadata rather than silently reinterpret it.

## Decision

1. Rust exposes generated `cerbero.contracts.v1` types through `cerbero-common::contracts::v1` and keeps validators in non-generated source.
2. Go keeps generated types in `services/internal/contracts/v1` and places validators in the sibling `services/internal/contracts/validation` package so generated declarations are never hand-extended.
3. The first runtime validators enforce only invariants already established or directly implied by the governed fields:
   - canonical RFC-variant UUIDv7 syntax for CERBERO-owned IDs checked in M1;
   - valid Protobuf Timestamp ranges;
   - `contract_version == "1"` for v1 envelopes;
   - mandatory message type, producer component/version, emitted timestamp, payload schema, and payload for event-bus envelopes;
   - `raw_size == len(raw_payload)`;
   - `raw_hash_algorithm == "sha256"` and lowercase hexadecimal SHA-256 matching the exact raw bytes;
   - non-unspecified transformation status and execution mode;
   - a `CerberoError` when a transformation is `FAILED`.
4. `event_time` remains source-declared, untrusted metadata. It is range-validated when present but is not fabricated when absent.
5. M1 does not freeze the normalized-hash field scope or an OCSF version. Those remain governed by the parsing/OCSF milestone and are intentionally not invented by these validators.
6. The generated bindings are committed and CI regenerates them with pinned Buf plugins; any generated drift fails the contract gate.
7. Rust dependencies required by generated code and raw SHA-256 validation are exact-pinned in `cerbero-common`. Go uses the same `protoc-gen-go` runtime version as the pinned generator.

## Consequences

- Invalid raw size/hash metadata is rejected before downstream code can treat it as trustworthy evidence metadata.
- Parser/normalizer failures can be represented without mutating the original RawEvent.
- Duplicate delivery can retain the same `message_id`; deduplication storage/consumer behavior remains an owning-service concern rather than a contract-library side effect.
- Replay semantics are testable at the wire level through `execution_mode`.
- Generated bindings remain replaceable artifacts; regeneration cannot silently alter committed output.
- CI requires access to the pinned Buf remote plugins for the generated-drift check.
- This ADR does not alter any `[LOCKED]` field number, enum meaning, NATS subject, persistence rule, or normalized-hash scope.

## Alternatives considered

### Hand-maintain Rust and Go domain copies

Rejected. Separate copies would create schema drift and violate the requirement that bindings derive from a common contract source.

### Put helper declarations directly in the generated Go package

Rejected. Generator upgrades may add declarations, and hand-authored code mixed into the generated package would create collision and ownership ambiguity.

### Validate only at ingest/service boundaries

Rejected for M1. Shared invariant helpers reduce divergent interpretations across components while still leaving service-specific policy with the owning service.

### Freeze normalized hash scope now

Rejected. PARSING & OCSF explicitly leaves the exact normalized-hash field scope open.

## References

- CONTRACTS v1.0 — UUIDv7, timestamp model, envelope, RawEvent, provenance, idempotency, replay, error contract, and Definition of Done.
- INGEST & EVENT BUS v1.0 — exact raw-byte boundary, SHA-256, RawEvent validation, duplicate redelivery behavior.
- PARSING & OCSF v1.0 — parser failure preservation and open normalized-hash scope.
