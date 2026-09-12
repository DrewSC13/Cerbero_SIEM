# Development secrets and identities

## Rule

Operational secrets are never committed, hard-coded, or logged. Git may contain placeholders, examples, and secret references.

## Compose development

`.env.example` is intentionally populated with obvious DEVELOPMENT ONLY values. `make dev-init` copies it to ignored `.env` and sets mode `0600`.

These values are suitable only for a localhost development network. Production startup must not rely on these universal values.

## NATS identities

Milestone 0 already separates development NATS identities for:

- administration/bootstrap;
- ingest;
- raw preservation;
- normalization;
- detection/correlation.

Permissions are narrowed to their expected subject families. Later service implementations may tighten them further based on exact contracts.

## Databases

The PostgreSQL bootstrap creates NOLOGIN service role groups so grants can evolve without sharing one application identity. Actual per-service login credentials are introduced when a service first needs database access.

The current Compose database users are development bootstrap administrators only; no application service is wired to them in Milestone 0.
