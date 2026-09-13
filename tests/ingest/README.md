# Ingest tests

Milestone 2 unit tests currently live with the Go implementation under `services/cerbero-ingest/internal/ingestcore` so `make go-check` executes them for the owning module.

Step 1 covers:

- exact-byte raw preservation and SHA-256 construction;
- generated UUIDv7 event/message/trace identities;
- distinct source `event_time` and gateway `ingest_time` semantics;
- `RawEvent` and `CerberoEnvelope` validation and payload round-trip;
- payload-size rejection before CERBERO IDs are allocated;
- authentication and `events.ingest` authorization hook failures;
- source policy for absent `sensor_id` without invented identity;
- unfamiliar encoding preserved as metadata;
- stable `CerberoError` mapping for ingest-core failures;
- UUIDv7 version/variant/timestamp layout and secure-random failure behavior.

HTTP acceptance, JetStream durability, NATS outages, retry/DLQ, and raw-preserver crash/idempotency tests remain future milestone work and must not be reported as passing yet.
