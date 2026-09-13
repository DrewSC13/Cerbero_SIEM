# Changelog

All notable project changes are recorded here. CERBERO uses immutable release tags once published.

## Unreleased

### Added

- Milestone 3 end-to-end raw-preserver runtime integration across least-privilege NATS identities, durable consumer redelivery, exact filesystem evidence, PostgreSQL published outbox state, and duplicate-safe `raw.persisted` emission.
- Milestone 3 executable development raw-preserver runtime with a durable `raw-preserver` pull consumer, explicit ACK/retry/isolate transport semantics, least-privilege NATS consumer API permissions, service-specific PostgreSQL login bootstrap, and UUIDv7 runtime identity generation.
- Milestone 3 real PostgreSQL `MetadataStore` integration coverage with the least-privilege application role, full uint64 locator round-trip, idempotent publication completion, conflict rollback, and forbidden-delete verification.
- Milestone 3 JetStream `raw.persisted` publisher with stable `Nats-Msg-Id`, ADR-0007 request correlation, strict PubAck validation, duplicate-safe recovery semantics, and a real JetStream integration test.
- Milestone 3 development filesystem Raw Store adapter with exact-byte SHA-256 verification, durable no-overwrite publication, deterministic locator recovery, and concurrent redelivery convergence.
- Milestone 3 PostgreSQL `MetadataStore` adapter with atomic raw-locator/idempotency/outbox commits, stable duplicate recovery, conflict detection, uint64-safe locator decoding, and idempotent publication completion.
- Milestone 3 PostgreSQL raw-preservation schema for immutable raw locators, critical-consumer processed-message idempotency, transactional outbox state, and a least-privilege `cerbero_raw_preserver` role with Compose integration coverage.
- Milestone 3 concrete `RawEventPersisted` outbox builder producing deterministic `CerberoEnvelope` bytes with stable derived UUIDv7 identity, causation/trace propagation, raw locator metadata, and ADR-0007 request correlation.
- Canonical `RawEventPersisted` v1 Protobuf source, generated Rust/Go bindings, shared wire fixture, semantic contract checks, and runtime validators for the durable Raw Store handoff.
- ADR-0009 closing the missing v1 `RawEventPersisted` payload contract and its raw-locator/normalizer handoff semantics without retransmitting authoritative raw bytes.
- Milestone 3 raw-preserver Go service skeleton and preservation core with exact-byte Raw Store boundary, transport-idempotency state, stable outbox publication recovery, and explicit ACK/retry/isolate dispositions.
- ADR-0008 defining the standalone Go raw-preserver boundary, transport idempotency, PostgreSQL locator/processed-message ownership, and transactional-outbox publication of `raw.persisted`.
- Milestone 2 closure gates for backend-neutral ingest metrics, native-sequence gap detection fixtures, and NATS-outage behavior proving NOT READY plus retryable non-acceptance.
- Milestone 2 syslog adapter skeleton and journald collector contract, preserving source evidence without freezing open syslog framing/UDP policy or journald canonicalization decisions.
- Milestone 2 DEVELOPMENT runtime composition for JSON/HTTP → staged IngestCore → JetStream, with fail-closed production startup, loopback-only insecure development binding, explicit frontend limits, live/ready health endpoints, graceful shutdown, and full HTTP-to-JetStream integration coverage.
- Milestone 2 synchronous JetStream `DurableAcceptor` for `cerbero.v1.raw.received`, with Protobuf envelope publication, `Nats-Msg-Id` deduplication identity, ADR-0007 request metadata, PubAck enforcement, unit coverage, and live JetStream integration coverage.
- ADR-0007 defining `Cerbero-Request-Id` as the v1 NATS trace-metadata carrier for request correlation without altering the locked `CerberoEnvelope` schema.
- Milestone 2 JSON/HTTP ingest adapter with exact-byte JSON preservation, UUIDv7 request correlation, stable HTTP error mapping, and an injected durable-admission boundary required before any `2xx` response.
- Milestone 2 common Go `IngestCore` with injectable authentication/authorization hooks, explicit payload-size policy, source metadata validation, UUIDv7 event/message/trace generation, gateway `ingest_time`, exact-byte SHA-256, `RawEvent` construction, and `CerberoEnvelope` wrapping.
- RFC 9562 UUIDv7 generation using the Unix-millisecond layout and cryptographic randomness, with deterministic unit coverage.
- Ingest-core tests for exact-byte immutability, event/ingest time separation, sensor policy, unknown encoding preservation, stable Cerbero errors, payload limits, authn/authz failures, and protobuf payload round-trip.
- Milestone 2 ingest implementation/testing documentation that explicitly withholds HTTP acceptance until durable JetStream admission exists.
- Milestone 1 canonical `cerbero.contracts.v1` Protobuf source for envelope, raw/normalized events, transformations, errors, and shared enums.
- Pinned Buf schema validation and Rust/Go code-generation configuration.
- Semantic contract-source verification for field numbers, optional presence, enum values, and replay execution mode.
- ADR-0005 documenting the minimal `TransformationStatus` closure and locked `ErrorCategory` lint exception.
- Runtime Rust and Go validation around generated v1 bindings for UUIDv7, Protobuf timestamps, raw byte counts, exact raw SHA-256, envelope identity, and transformation provenance.
- Shared cross-language Protobuf wire fixture and M1 tests for serialization/deserialization, duplicate delivery identity, replay mode, parser failure preservation, invalid raw metadata, and hash mismatch.
- Generated-binding drift verification and recursive Go module discovery so the shared contracts module participates in formatting, vet, build, and tests.
- ADR-0006 documenting v1 runtime-validation semantics without freezing open normalized-hash/OCSF decisions.
- CONTRACTS v1 event-bus semantics for versioned NATS subjects, ACK-after-durable-effect, transient-only retry, DLQ routing, and the ingest-to-durable-ACK scenario.
- Shared valid and intentionally invalid RawEvent wire fixtures exercised by both Rust and Go validators.
- Milestone 0 repository bootstrap.
- Rust, Go, and Python workspace skeletons with executable smoke tests.
- Versioned PostgreSQL and ClickHouse migration roots.
- Pinned Docker Compose development infrastructure for PostgreSQL, ClickHouse, and NATS JetStream.
- Filesystem-backed development Raw Store boundary.
- Local/CI parity through GNU Make and GitHub Actions.
- Indexed the external architectural baseline without committing source PDF documents.
- Architecture, development, operations, security, testing, and ADR documentation roots.

### Fixed

- Enforce configured ingest event-admission rate limits after authentication/authorization and before HTTP payload receipt.
- Stage ingest authentication/authorization before HTTP body receipt and bind authorized admission metadata so source identity cannot change between authorization and RawEvent construction.
- Avoid copying generated Protobuf messages by value in JSON/HTTP adapter tests so Go `vet` copylock analysis remains clean.
- Resolve the shared contracts module through the root Go workspace instead of declaring an invalid versioned repository-local module requirement in `cerbero-ingest`.
- Prevent the Go build gate from leaving service executables in the repository working tree.
- Make the remote-readiness gate reject untracked files as well as tracked or staged changes.
- Make development health checks retry bounded service readiness and use a NATS CLI-compatible JetStream probe.
