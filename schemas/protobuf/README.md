# Protobuf contracts

Milestone 1 establishes `schemas/protobuf/cerbero/contracts/v1/` as the canonical internal wire-contract source for package `cerbero.contracts.v1`.

The v1 source set is deliberately split by domain object:

- `common.proto` — `Producer` and shared enums;
- `envelope.proto` — `CerberoEnvelope`;
- `raw_event.proto` — immutable raw evidence contract;
- `raw_event_persisted.proto` — durable Raw Store locator/metadata handoff for `raw.persisted`;
- `normalized_event.proto` — OCSF-derived event contract;
- `transformation.proto` — provenance and execution-mode contract;
- `error.proto` — `CerberoError` and locked error categories.

`buf.yaml` enforces schema lint/build policy. The `ENUM_VALUE_PREFIX` exception is limited to `common.proto` and `error.proto`, because CONTRACTS v1.0 locks the existing `IntegrityStatus` and `ErrorCategory` names and changing those names for style would alter the wire contract.

`buf.gen.yaml` pins the Go and Prost remote plugins used to generate bindings. Run from the repository root:

```bash
make contracts
make contracts-generate
```

The canonical `.proto` files are authoritative. Generated Rust and Go artifacts must be regenerated after source changes and must never become an independent contract definition.

## Generated output ownership

Pinned generation produces:

- Rust: `crates/cerbero-common/src/generated/cerbero/contracts/v1/cerbero.contracts.v1.rs`;
- Go: `services/internal/contracts/v1/*.pb.go`.

Hand-authored validation code lives outside those generated roots. `make contracts` regenerates both outputs and fails if Git observes drift.
