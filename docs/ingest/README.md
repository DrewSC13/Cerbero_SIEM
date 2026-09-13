# Ingest implementation

Milestone 2 turns the locked `RawEvent` contract into executable ingest behavior. This document tracks the implementation boundary without replacing the external architectural source of truth.

## M2 Step 1: common IngestCore

`services/cerbero-ingest/internal/ingestcore` implements the frontend-neutral portion of the locked acceptance sequence:

1. authenticate the source through an injected hook;
2. authorize the locked `events.ingest` capability through an injected hook;
3. enforce an explicitly configured `max_payload_size`;
4. validate/capture source metadata without inventing sensor identity;
5. generate CERBERO-owned UUIDv7 `event_id`, `message_id`, and `trace_id` values;
6. capture `ingest_time` at the ingest boundary;
7. calculate lowercase SHA-256 over an immutable copy of the exact received bytes;
8. construct and validate `RawEvent`;
9. wrap that `RawEvent` in a validated `CerberoEnvelope` using `RawEventReceived` and `cerbero.raw_event.v1`.

The core copies `raw_payload` before hashing and contract construction. Callers may mutate their input buffer after `Prepare` returns without changing the evidence bytes retained by the returned `RawEvent`.

## Time semantics

`cerbero-ingest` is authoritative for `ingest_time`, not `event_time`. A source-provided event timestamp is normalized to UTC for the Protobuf representation when present. If the frontend cannot extract a source timestamp safely, `event_time` remains absent; the core never substitutes `ingest_time`.

## Encoding semantics

`encoding` is metadata. Step 1 does not decode or normalize raw bytes, and it does not reject an unfamiliar non-empty encoding merely because the core does not understand it. Frontend-specific policy may reject unsupported encodings only when that policy is explicit; rejection must never mutate or truncate the original bytes.

## Identity and authorization hooks

Authentication and authorization remain transport/security integrations rather than hard-coded credentials. The common core requires both hooks and enforces authorization for `events.ingest`. The complete mTLS enrollment/certificate lifecycle remains owned by the security milestone and must not be improvised in ingest code.

`sensor_id` is required by default. A configured source type may allow it to be absent, in which case the core preserves the absence instead of fabricating an identifier.

## Error contract

Request failures returned by the core use `CerberoError` through `ingestcore.Error` with stable initial codes:

- `CER-ING-INVALID-PAYLOAD` for structural or constructed-contract validation failure;
- `CER-ING-PAYLOAD-TOO-LARGE` when the explicit payload limit is exceeded;
- `CER-AUTH-UNAUTHENTICATED` for authentication failure;
- `CER-AUTH-FORBIDDEN` for authorization failure;
- `CER-SYSTEM-INTERNAL` for local failures such as UUID generation or protobuf packing.

Implementation causes remain available through Go error unwrapping but are not copied into the stable wire-facing human message.

## Deliberately not implemented in Step 1

`Prepare` is not a durable-acceptance operation and must not be exposed as an HTTP `2xx` success by itself. The architecture requires durable JetStream admission before reporting acceptance. Therefore these responsibilities remain outside this commit:

- JSON/HTTP request/response adapter and `X-Request-ID` handling;
- connection/rate/timeouts and per-frontend limit configuration;
- syslog adapter skeleton and journald collector contract;
- JetStream publish to `cerbero.v1.raw.received`;
- NATS outage behavior and readiness degradation;
- raw-preserver, Raw Store persistence, `raw.persisted`, ACK/retry/DLQ behavior (Milestone 3).

This boundary prevents M2 unit code from claiming the stronger acceptance guarantee that only the durable event bus can provide.
