# Ingest tests

Milestone 2 tests live with the Go implementation so `make go-check` executes them for the owning module.

Step 2 JSON/HTTP component coverage:

- preserves the exact JSON request-body bytes across syntax validation;
- generates or propagates UUIDv7 `X-Request-ID`;
- rejects invalid method, media type, malformed JSON, and oversized bodies before durable admission;
- maps ingest-core source errors to `4xx`;
- requires the injected durable acceptor before reporting `202 Accepted`;
- maps durable-admission failure to retryable `CER-BUS-PUBLISH-FAILED` and HTTP `503`.

Step 1 common-core coverage:

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

The HTTP component tests use an injected durable-acceptor fake and therefore do not claim real JetStream durability. Production JetStream admission, NATS outage/readiness behavior, retry/DLQ, and raw-preserver crash/idempotency tests remain future work and must not be reported as passing yet.
