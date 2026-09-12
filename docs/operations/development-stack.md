# Development stack operations

Milestone 0 runs PostgreSQL, ClickHouse, and NATS JetStream through Docker Compose. The filesystem-backed development Raw Store lives under `var/raw/`.

## Authority boundaries

- PostgreSQL bootstrap creates only locked schema namespaces and a migration ledger plus non-login service role groups.
- ClickHouse bootstrap creates its database and migration ledger; normalized event tables arrive with the storage/query milestone.
- NATS creates the locked v1 stream families through an explicit bootstrap job.
- Raw data is not stored in PostgreSQL or ClickHouse as authoritative evidence.

## Development-only capacity limits

JetStream stream byte limits are local safeguards to avoid unbounded workstation disk consumption. They are not retention-policy decisions and must not be copied into a production policy without operational evidence.

## Health

`make dev-health` checks PostgreSQL readiness, ClickHouse queryability, NATS reachability, and stream existence. Application health/readiness contracts are introduced with each service implementation.

## Reset

`make dev-reset` deletes local Compose volumes. Raw Store runtime data under `var/raw/` is also deleted except for tracked documentation/ignore files.
