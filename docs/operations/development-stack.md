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

`make dev-health` checks PostgreSQL readiness, ClickHouse queryability, NATS authentication/JetStream reachability, and stream existence. Each infrastructure probe is retried with a bounded 60-attempt, two-second interval so fresh local volumes can finish initialization without weakening the final readiness requirement. PostgreSQL readiness additionally requires the container PID 1 process to be the final `postgres` server, which prevents the temporary server used by `initdb` from producing a false-positive readiness result. The NATS probe uses `stream ls` rather than version-specific `server ping` flags. Application health/readiness contracts are introduced with each service implementation.

## Reset

`make dev-reset` deletes local Compose volumes. Raw Store runtime data under `var/raw/` is also deleted except for tracked documentation/ignore files.
