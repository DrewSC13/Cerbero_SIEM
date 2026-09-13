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
  -> shared cross-language wire fixture
  -> UUIDv7 and Protobuf timestamp validation
  -> exact raw SHA-256 / byte-count validation
  -> duplicate message identity, replay, and parser-failure preservation

integration
  -> Compose config
  -> PostgreSQL
  -> ClickHouse
  -> NATS JetStream + v1 stream topology
```

The Go gates discover every `go.mod` recursively below `services/`, including the shared contracts module. The build gate must not write executables into `services/` or otherwise dirty the repository working tree. Build artifacts are written to a temporary directory and deleted when the gate exits.

Infrastructure health checks use bounded retries because PostgreSQL and ClickHouse can transiently reject requests while their fresh development volumes are initialized. PostgreSQL must also be running its final PID 1 `postgres` process before `pg_isready` can satisfy the gate; this excludes the temporary server started by the image entrypoint during `initdb`. NATS readiness is validated with `stream ls`, which simultaneously verifies connectivity, authentication, and JetStream availability without relying on version-specific `server ping` flags.

Buf CLI `1.72.0` is installed in GitHub Actions and is required locally for `make contracts`. Contract generation uses plugin versions pinned in `schemas/protobuf/buf.gen.yaml`. The contract gate regenerates committed bindings and rejects any tracked or untracked drift under the generated output roots.

## E2E honesty

Milestones 0–1 do not claim an analytical E2E pipeline. `make e2e` is only an explicit gate documenting that fact. A real E2E becomes mandatory when enough implemented stages exist to exercise the source-to-TUI path.

## Future hierarchy

The baseline testing hierarchy remains:

```text
unit -> contract -> component -> integration -> E2E -> performance/resilience
```

Parser fuzzing, contract compatibility, detection fixtures, idempotency/retry, security, resilience, and replay tests are introduced with the owning functionality rather than as empty test names.
