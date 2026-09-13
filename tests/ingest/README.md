# Ingest tests

Milestone 2 tests live with the Go implementation so `make go-check` executes them for the owning module.

Step 3 JetStream admission coverage:

- serializes and publishes the prepared `CerberoEnvelope` on `cerbero.v1.raw.received`;
- sets `Nats-Msg-Id` from the stable `message_id`;
- propagates validated ADR-0007 `Cerbero-Request-Id` metadata when present;
- rejects invalid request metadata before publish;
- fails durable admission on publish error or missing `PubAck`;
- accepts duplicate `PubAck` as the same transport identity already admitted;
- verifies a real stored message and both headers against the development JetStream during `make integration`.

Step 4B runtime-composition coverage:

- rejects production startup while production PKI source authentication remains unimplemented;
- requires explicit insecure DEVELOPMENT mode and loopback binding;
- requires all locked frontend limit categories as runtime configuration;
- rate-limits admitted events after authn/authz and before payload receipt;
- exposes liveness/readiness with readiness dependent on NATS + `CERBERO_RAW`;
- integration-tests JSON/HTTP through the composed runtime to stored JetStream `RawEvent` exact bytes.

Step 4A staged-admission coverage:

- begins common-core authentication/authorization before HTTP body reads;
- binds authorized admission metadata through payload preparation;
- rejects post-authorization source-identity substitution before IDs are allocated.

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

Step 3 supplies and integration-tests the real JetStream durable acceptor, so HTTP's durability boundary now has an executable production implementation. Process wiring, NATS outage/readiness behavior, retry policy, DLQ, and raw-preserver crash/idempotency tests remain future work and must not be reported as passing yet.
