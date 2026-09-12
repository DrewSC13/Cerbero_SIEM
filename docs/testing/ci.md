# Testing and CI

CERBERO uses local commands as the source of CI behavior. GitHub Actions invokes the same Make targets rather than hiding correctness checks in hosted-only scripts.

## Milestone 0 gates

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
  -> every Go service module
  -> Python compileall

test
  -> Rust unit tests
  -> Go unit tests
  -> Python unit tests

security-check
  -> forbidden secret-bearing filenames
  -> high-signal committed-secret patterns

integration
  -> Compose config
  -> PostgreSQL
  -> ClickHouse
  -> NATS JetStream + v1 stream topology
```

## E2E honesty

Milestone 0 does not claim an analytical E2E pipeline. `make e2e` is only an explicit gate documenting that fact. A real E2E becomes mandatory when enough implemented stages exist to exercise the source-to-TUI path.

## Future hierarchy

The baseline testing hierarchy remains:

```text
unit -> contract -> component -> integration -> E2E -> performance/resilience
```

Parser fuzzing, contract compatibility, detection fixtures, idempotency/retry, security, resilience, and replay tests are introduced with the owning functionality rather than as empty test names.
