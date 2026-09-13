# ADR-0005: Contract v1 enum closure and code generation

## Status

Accepted

## Context

`CERBERO — CONTRACTS v1.0` defines the v1 message shapes, requires Rust and Go bindings to derive from the same contract source where possible, and uses `TransformationStatus` in `Transformation`. The baseline defines the semantic normalization outcomes `SUCCESS`, `PARTIAL`, and `FAILED`, and locks execution modes `LIVE`, `REPLAY`, and `TEST`.

The baseline does not enumerate the numeric wire values for `TransformationStatus`. Protobuf enum values are package-scoped for generated C++ compatibility, so reusing bare names such as `SUCCESS` in multiple enums would also create a schema-level collision. `ErrorCategory`, by contrast, already has exact locked unprefixed value names and numeric values.

This is a contract gap that must be closed explicitly rather than independently by Rust and Go implementations.

## Decision

The canonical source lives in `schemas/protobuf/cerbero/contracts/v1/` and uses package `cerbero.contracts.v1`.

New v1 enum symbols use type-specific prefixes and stable numeric values:

```text
NormalizationStatus
  NORMALIZATION_STATUS_UNSPECIFIED = 0
  NORMALIZATION_STATUS_SUCCESS       = 1
  NORMALIZATION_STATUS_PARTIAL       = 2
  NORMALIZATION_STATUS_FAILED        = 3

TransformationStatus
  TRANSFORMATION_STATUS_UNSPECIFIED = 0
  TRANSFORMATION_STATUS_SUCCESS       = 1
  TRANSFORMATION_STATUS_FAILED        = 2

ExecutionMode
  EXECUTION_MODE_UNSPECIFIED = 0
  EXECUTION_MODE_LIVE        = 1
  EXECUTION_MODE_REPLAY      = 2
  EXECUTION_MODE_TEST        = 3
```

`TransformationStatus` intentionally contains only the terminal outcomes required by current architecture. Additional lifecycle states require a later compatibility-reviewed decision.

`ErrorCategory` preserves the exact names and values already locked by CONTRACTS v1.0. The Buf `ENUM_VALUE_PREFIX` lint rule is exempted for `common.proto` and `error.proto` only because the locked `IntegrityStatus` and `ErrorCategory` symbols predate that naming rule; no locked wire symbol is renamed merely to satisfy style tooling.

`Transformation.execution_mode` is added as field `13`. This satisfies the later locked replay requirement without renumbering or changing any earlier field.

Buf CLI `1.72.0` is the pinned schema tool. Generated bindings use the pinned remote plugins:

- `buf.build/protocolbuffers/go:v1.36.12`
- `buf.build/community/neoeinstein-prost:v0.5.0`

Generated outputs are repository artifacts, but the `.proto` files are authoritative.

## Consequences

- Rust and Go consume one source contract instead of redefining wire shapes.
- Enum numeric values become explicit compatibility commitments.
- Existing locked `ErrorCategory` names are preserved exactly.
- Replay/test provenance is represented directly in `Transformation`.
- Contract generation requires the pinned Buf version and access to the pinned remote plugins.
- Future incompatible field/enum semantic changes require a new contract major version.

## Alternatives considered

- Leave `TransformationStatus` implementation-defined: rejected because it would create incompatible Rust/Go contracts.
- Add speculative states such as `STARTED` or `SKIPPED`: rejected because the baseline does not require them.
- Rename locked `ErrorCategory` values to satisfy Buf style: rejected because this would silently alter a locked wire contract.
- Maintain independent Rust and Go models: rejected because it violates the common-source contract goal.

## References

- `CERBERO — CONTRACTS v1.0`, sections 11, 12, 21, 22, 33, 37, 39, 40.
- `CERBERO — PARSING & OCSF v1.0`, normalization status semantics.
