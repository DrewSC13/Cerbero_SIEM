# Changelog

All notable project changes are recorded here. CERBERO uses immutable release tags once published.

## Unreleased

### Added

- Milestone 1 canonical `cerbero.contracts.v1` Protobuf source for envelope, raw/normalized events, transformations, errors, and shared enums.
- Pinned Buf schema validation and Rust/Go code-generation configuration.
- Semantic contract-source verification for field numbers, optional presence, enum values, and replay execution mode.
- ADR-0005 documenting the minimal `TransformationStatus` closure and locked `ErrorCategory` lint exception.
- Runtime Rust and Go validation around generated v1 bindings for UUIDv7, Protobuf timestamps, raw byte counts, exact raw SHA-256, envelope identity, and transformation provenance.
- Shared cross-language Protobuf wire fixture and M1 tests for serialization/deserialization, duplicate delivery identity, replay mode, parser failure preservation, invalid raw metadata, and hash mismatch.
- Generated-binding drift verification and recursive Go module discovery so the shared contracts module participates in formatting, vet, build, and tests.
- ADR-0006 documenting v1 runtime-validation semantics without freezing open normalized-hash/OCSF decisions.
- Milestone 0 repository bootstrap.
- Rust, Go, and Python workspace skeletons with executable smoke tests.
- Versioned PostgreSQL and ClickHouse migration roots.
- Pinned Docker Compose development infrastructure for PostgreSQL, ClickHouse, and NATS JetStream.
- Filesystem-backed development Raw Store boundary.
- Local/CI parity through GNU Make and GitHub Actions.
- Indexed the external architectural baseline without committing source PDF documents.
- Architecture, development, operations, security, testing, and ADR documentation roots.

### Fixed

- Prevent the Go build gate from leaving service executables in the repository working tree.
- Make the remote-readiness gate reject untracked files as well as tracked or staged changes.
- Make development health checks retry bounded service readiness and use a NATS CLI-compatible JetStream probe.
