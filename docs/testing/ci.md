# Testing and CI

CERBERO uses local commands as the source of CI behavior. GitHub Actions invokes the same Make targets rather than hiding correctness checks in hosted-only scripts.

## Repository gates

```text
verify
  -> repository invariants
  -> baseline source-policy verification
  -> tracked text hygiene

format
  -> cargo fmt
  -> gofmt
  -> Python repository formatter checks

lint
  -> cargo clippy
  -> go vet
  -> Python AST/style lint

build
  -> Rust workspace
  -> every Go service module using an ephemeral build-output directory
  -> Python compileall

test
  -> Rust unit tests
  -> Go unit tests
  -> Python unit tests

security-check
  -> forbidden secret-bearing filenames
  -> high-signal committed-secret patterns

contracts
  -> governed v1 source shape and field-number invariants
  -> Buf STANDARD lint with the locked IntegrityStatus/ErrorCategory exceptions
  -> Buf schema build
  -> pinned Rust/Go binding regeneration with zero Git drift

contract runtime tests
  -> Rust and Go Protobuf serialization/deserialization
  -> shared valid/invalid cross-language wire fixtures
  -> UUIDv7 and Protobuf timestamp validation
  -> exact raw SHA-256 / byte-count validation
  -> duplicate message identity, replay, and parser-failure preservation

Milestone 2 JSON/HTTP component tests (through the Go gates)
  -> exact-byte JSON body preservation
  -> request-id generation/propagation
  -> HTTP method/media-type/body-size validation
  -> 4xx source error mapping
  -> durable-admission required before 202
  -> 503 on durable-admission failure

Milestone 2 JetStream admission tests
  -> envelope Protobuf publication on cerbero.v1.raw.received
  -> message_id as Nats-Msg-Id transport deduplication identity
  -> ADR-0007 Cerbero-Request-Id propagation
  -> request metadata validation before publish
  -> publish error / missing PubAck rejection
  -> duplicate PubAck accepted as durable success
  -> live development JetStream storage verification

Milestone 2 ingest-core unit tests (through the Go gates)
  -> UUIDv7 generation and secure-random failure behavior
  -> exact-byte RawEvent construction and immutable input-copy semantics
  -> event_time / ingest_time separation
  -> payload limits and source-identity policy
  -> authentication + events.ingest authorization hooks
  -> stable CerberoError mapping and envelope payload round-trip

integration
  -> Compose config
  -> PostgreSQL
  -> ClickHouse
  -> NATS JetStream + v1 stream topology
  -> M2 synchronous RawEvent envelope durable publication + stored header verification
```

The Go gates discover every `go.mod` recursively below `services/`, including the shared contracts module. Repository-local Go modules are linked by the root `go.work`; they are not represented as synthetic `v0.0.0` requirements in sibling `go.mod` files. External dependencies remain pinned in the owning module. The build gate must not write executables into `services/` or otherwise dirty the repository working tree. Build artifacts are written to a temporary directory and deleted when the gate exits.

Infrastructure health checks use bounded retries because PostgreSQL and ClickHouse can transiently reject requests while their fresh development volumes are initialized. PostgreSQL must also be running its final PID 1 `postgres` process before `pg_isready` can satisfy the gate; this excludes the temporary server started by the image entrypoint during `initdb`. NATS readiness is validated with `stream ls`, which simultaneously verifies connectivity, authentication, and JetStream availability without relying on version-specific `server ping` flags.

Buf CLI `1.72.0` is installed in GitHub Actions and is required locally for `make contracts`. Contract generation uses plugin versions pinned in `schemas/protobuf/buf.gen.yaml`. The contract gate regenerates committed bindings and rejects any tracked or untracked drift under the generated output roots.

Rust contract code is checked with Clippy under `-D warnings`; documentation comments must therefore satisfy the active Clippy documentation lints as part of the local and remote CI gate.
Contract tests must also satisfy the active Clippy style lints; concrete default constructors are used where type inference would otherwise trigger `clippy::default_trait_access`.

## E2E honesty

Milestones 0–1 and M2 Steps 1–2 do not claim durable ingest. M2 Step 3 proves durable JetStream admission but still does not claim Raw Preservation or an analytical E2E pipeline. `make e2e` is only an explicit gate documenting that fact. A real ingest E2E requires JetStream admission and raw preservation; the full analytical E2E becomes mandatory when enough implemented stages exist to exercise the source-to-TUI path.

## Future hierarchy

The baseline testing hierarchy remains:

```text
unit -> contract -> component -> integration -> E2E -> performance/resilience
```

Parser fuzzing, detection fixtures, service-level idempotency/retry, security, resilience, and end-to-end replay tests are introduced with the owning functionality rather than as empty test names. M1 already enforces wire compatibility, duplicate-message identity, and execution-mode replay semantics at the contract layer.
