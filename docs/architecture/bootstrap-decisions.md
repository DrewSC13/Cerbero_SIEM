# Milestone 0 bootstrap decisions

## Decisions inherited from the v1.0 baseline

- Git versions code, contracts, schemas, migrations, rules, datasets, documentation, tests, and ADRs.
- Pull requests, CI, CODEOWNERS, reviews, immutable release tags, changelog, and signed releases are the governance direction.
- CI must have locally executable equivalents.
- Dependency locks and database migrations are reproducibility artifacts.
- PostgreSQL is control-plane/relational authority; ClickHouse is normalized telemetry authority; Raw Store is separate; Parquet/files are archive/dataset authority.
- NATS JetStream is the initial durable event bus with separate RAW/ANALYTICS/SYSTEM/DLQ streams.
- Raw evidence must be persisted before normalization; at-least-once + idempotent consumers + ACK after durable effect are invariants.
- Development credentials may be local/non-production only and must be clearly marked.
- Kubernetes, HA, clustering, ML/LLMs, web dashboard, and autonomous SOAR remain outside the MVP.

## New decisions recorded by ADR before implementation

- ADR-0001: Apache-2.0 for CERBERO-authored code/documentation.
- ADR-0002: GNU Make as the thin local/CI task runner.
- ADR-0003: filesystem-backed Raw Store for local development only.
- ADR-0004: exact bootstrap toolchain and development image pins.

## Deliberate non-decisions

Milestone 0 does **not** freeze:

- production Raw Store/object-storage provider;
- production retention durations or storage thresholds;
- exact human login mechanism;
- TLS cipher/minimum-version policy still marked open by the baseline;
- Go import host and the final repository hosting state; the GitHub owner is now `DrewSC13` and the intended repository is `DrewSC13/cerbero`;
- OCSF version used by the first normalization implementation;
- deployment beyond Docker Compose.

These are not filled with guesses.
