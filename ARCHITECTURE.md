# CERBERO architecture implementation index

This file is an implementation-facing index, not a replacement for the external architectural baseline listed in `docs/architecture/source-of-truth.md`. The original PDF source documents are intentionally not stored in Git.

## Product model

CERBERO is open source, terminal-first, API-first, modular, traceable, reproducible, auditable, interoperable, and secure by default.

The target lifecycle is:

```text
INGEST -> PRESERVE -> NORMALIZE -> STORE -> SEARCH
       -> DETECT -> CORRELATE -> INVESTIGATE -> RESPOND -> AUDIT
```

The canonical analytical objects remain distinct:

```text
RawEvent -> NormalizedEvent -> Signal -> Finding -> Incident -> Case
```

`alert` is not a canonical internal substitute for these objects.

## Component ownership

- Rust: `cerbero-common`, `cerbero-tui`, `cerbero-normalizer`, `cerbero-integrity`, `cerbero-detection-core`, `cerbero-agent`, critical parsers.
- Go: `cerbero-ingest`, `cerbero-api`, `cerbero-coordinator`, `cerbero-scheduler`, `cerbero-worker`.
- Python: tooling, datasets, detection engineering, STIX/TAXII, validation, experimentation, analytics; not the mass-ingest critical path.
- C++: excluded unless a measured/native/interoperability constraint is documented.

## Storage authority

```text
Raw payload           -> Raw Store
Raw locator           -> PostgreSQL
NormalizedEvent       -> ClickHouse
Control plane         -> PostgreSQL
Rules/entities/risk   -> PostgreSQL
Findings/incidents    -> PostgreSQL
Cases/audit           -> PostgreSQL
Archive/datasets      -> Parquet/files
```

CERBERO does not assume a global distributed ACID transaction across stores. Workflows rely on stable IDs, idempotency, retries, explicit state, reconciliation, outbox where appropriate, and provenance.

## Event bus

NATS JetStream is the initial durable bus. The v1 stream boundaries are:

- `CERBERO_RAW` -> `cerbero.v1.raw.*`
- `CERBERO_ANALYTICS` -> normalized/signal/finding/incident/case subjects
- `CERBERO_SYSTEM` -> system/audit subjects
- `CERBERO_DLQ` -> `cerbero.v1.dlq.*`

Delivery is at-least-once. Consumers must be idempotent and ACK only after a durable side effect (or confirmed prior idempotent completion). The normalizer consumes `raw.persisted`, never `raw.received`.

## Interface isolation

```text
TUI -> REST API -> services/query layer -> storage
```

The supported TUI never reaches PostgreSQL or ClickHouse directly.

## Bootstrap decisions

Milestone-specific implementation decisions are documented in `docs/architecture/bootstrap-decisions.md`; architectural decisions are recorded in `docs/adr/`.
