# NATS JetStream development configuration

This configuration implements the v1 subject/stream boundaries and distinct development identities for planned consumers. Credentials are injected from `.env` and are DEVELOPMENT ONLY.

The stream topology is created by `scripts/dev/nats-bootstrap.sh` only after the server is reachable. Development byte limits are deployment safeguards, not CERBERO retention policy.

The canonical v1 wire subject namespace and ACK/retry/DLQ contract semantics are documented in `docs/contracts/event-bus-semantics.md`; this development configuration implements those boundaries without redefining them.

M2 durable ingest publishes a Protobuf `CerberoEnvelope<RawEvent>` synchronously to `cerbero.v1.raw.received`. The publisher sets `Nats-Msg-Id` to the envelope `message_id` for JetStream transport deduplication and, for request/response-originated traffic, propagates the ADR-0007 `Cerbero-Request-Id` header. These headers are transport metadata and do not change the v1 payload contract.
