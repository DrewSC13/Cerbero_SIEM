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

The core exposes `Begin` for streaming/request-response frontends. `Begin` performs authentication, authorization, and source-policy validation before payload receipt and returns an admission bound to the authorized metadata. That admission then prepares the payload without re-running auth hooks and rejects any identity-metadata substitution. The one-shot `Core.Prepare` path remains as a compatibility wrapper for callers that already hold the complete payload.

The core copies `raw_payload` before hashing and contract construction. Callers may mutate their input buffer after preparation returns without changing the evidence bytes retained by the returned `RawEvent`.

## Time semantics

`cerbero-ingest` is authoritative for `ingest_time`, not `event_time`. A source-provided event timestamp is normalized to UTC for the Protobuf representation when present. If the frontend cannot extract a source timestamp safely, `event_time` remains absent; the core never substitutes `ingest_time`.

## Encoding semantics

`encoding` is metadata. Step 1 does not decode or normalize raw bytes, and it does not reject an unfamiliar non-empty encoding merely because the core does not understand it. Frontend-specific policy may reject unsupported encodings only when that policy is explicit; rejection must never mutate or truncate the original bytes.

## Identity and authorization hooks

Authentication and authorization remain transport/security integrations rather than hard-coded credentials. The common core requires both hooks and enforces authorization for `events.ingest`. The complete mTLS enrollment/certificate lifecycle remains owned by the security milestone and must not be improvised in ingest code.

`sensor_id` is required by default. A configured source type may allow it to be absent, in which case the core preserves the absence instead of fabricating an identifier.

## Go workspace dependency boundary

`cerbero-ingest` consumes the shared contracts module through the repository `go.work` workspace. The local module path `cerbero/services/internal/contracts` must not be added as a versioned `require` entry in `services/cerbero-ingest/go.mod`; Go treats versioned module requirements as downloadable module paths and rejects this repository-local path because its first element is not a DNS-style module host. The ingest module pins only external runtime dependencies such as `google.golang.org/protobuf`.

## Error contract

Request failures returned by the core use `CerberoError` through `ingestcore.Error` with stable initial codes:

- `CER-ING-INVALID-PAYLOAD` for structural or constructed-contract validation failure;
- `CER-ING-PAYLOAD-TOO-LARGE` when the explicit payload limit is exceeded;
- `CER-AUTH-UNAUTHENTICATED` for authentication failure;
- `CER-AUTH-FORBIDDEN` for authorization failure;
- `CER-SYSTEM-INTERNAL` for local failures such as UUID generation or protobuf packing.

Implementation causes remain available through Go error unwrapping but are not copied into the stable wire-facing human message.

## M2 Step 2: JSON/HTTP adapter

`services/cerbero-ingest/internal/httpingest` adds the transport adapter without weakening the durability contract.

The adapter:

- accepts only `POST` requests with JSON media type;
- resolves source metadata through an injected transport resolver rather than hard-coding credentials or tenant/source identity;
- calls `IngestCore.Begin` to authenticate and authorize `events.ingest` before reading the request body;
- bounds the HTTP body independently after admission has succeeded;
- validates JSON syntax without decoding and reserializing the body, preserving the exact bytes used by the raw hash;
- uses a valid client `X-Request-ID` UUIDv7 when supplied, otherwise generates a CERBERO-owned UUIDv7;
- always returns the request ID in `X-Request-ID`;
- calls the authorized admission's `Prepare` method and then the injected `DurableAcceptor`;
- emits `202 Accepted` only after `DurableAcceptor.Accept` returns success;
- maps source/client failures to `4xx` and durable-admission/internal failures to `5xx`.

`DurableAcceptor` is deliberately an interface in Step 2. The production JetStream implementation arrives in the following increment. Until that implementation is wired into the process, the adapter is component-testable but is not exposed as a production listening endpoint.

### HTTP response model

Successful durable admission returns JSON containing `request_id`, `event_id`, and `message_id`. Error responses expose the stable `CerberoError` shape and do not serialize wrapped implementation causes.

A durable-admission failure is represented as `CER-BUS-PUBLISH-FAILED`, `TRANSPORT`, retryable `true`, and HTTP `503 Service Unavailable`.

## Request correlation across the event bus

ADR-0007 closes the v1 representation gap between the required `request_id` propagation and the locked `CerberoEnvelope` shape. Request/response-originated bus publications carry a validated UUIDv7 request identifier in the NATS header:

```text
Cerbero-Request-Id: <request_id>
```

The header is observability/audit correlation metadata only. It does not replace `trace_id`, `causation_id`, or `correlation_id`, and it must never be used as an authentication or authorization input. Request-associated downstream publications should propagate the validated header while the request context remains relevant.

## M2 Step 3: durable JetStream admission

`services/cerbero-ingest/internal/eventbus.JetStreamAcceptor` implements the durable-admission boundary used by the HTTP adapter:

1. validate the prepared `CerberoEnvelope`;
2. validate ADR-0007 request metadata when present;
3. serialize the envelope as Protobuf bytes;
4. publish synchronously to `cerbero.v1.raw.received`;
5. set `Nats-Msg-Id` to the stable envelope `message_id`;
6. propagate `Cerbero-Request-Id` when the ingest request has one;
7. return success only after a non-null JetStream `PubAck`.

A duplicate JetStream acknowledgement is successful durable admission: it means the same transport identity was already accepted within JetStream's deduplication window. No content-hash deduplication is introduced.

Step 3 deliberately does not add internal publish retries. Retry attempts, backoff, jitter, and retry budgets remain open policy and are not frozen by this implementation. The caller's `context.Context` bounds the synchronous publish operation.

`make integration` now exercises the acceptor against the real development JetStream using the least-privilege `cerbero_ingest` identity, verifies the stored subject and transport headers with the development admin identity, and removes the integration message afterward.

## M2 Step 4A: staged admission before payload receipt

Before the JSON/HTTP adapter is wired into a listening process, the common-core boundary is staged so the locked acceptance order is enforceable for streaming transports. The adapter resolves presented transport metadata, calls `IngestCore.Begin` for authentication and `events.ingest` authorization, and only then reads the bounded HTTP body. The returned admission is bound to the authorized request/source/sensor/remote-identity/transport metadata; any substitution before `RawEvent` construction is rejected before CERBERO event/message/trace IDs are allocated.

The existing one-shot `Core.Prepare` method remains a compatibility wrapper for non-streaming callers that already hold the complete payload. Production request/response frontends must use the staged `Begin` flow.

## M2 Step 4B: DEVELOPMENT runtime composition

`cerbero-ingest` now composes the JSON/HTTP adapter, staged `IngestCore`, and synchronous JetStream durable acceptor into an executable DEVELOPMENT runtime.

The security boundary is intentionally fail-closed:

- `CERBERO_SECURITY_PROFILE=DEVELOPMENT` is currently the only runnable profile;
- plain HTTP additionally requires `CERBERO_INGEST_INSECURE_DEVELOPMENT=1`;
- insecure DEVELOPMENT binding must use an explicit loopback IP;
- the development source identity is static configuration and is visibly warned at startup;
- `PRODUCTION` refuses startup until a production source authenticator conforming to the PKI/security baseline is implemented;
- no API key, bearer-token scheme, certificate subject profile, or other production credential format is invented by M2.

Every frontend limit required by the ingest baseline is explicit runtime configuration: `max_payload_size`, `max_connection_rate`, `max_events_per_second`, `read_timeout`, `idle_timeout`, and `concurrent_connections`. Values in `.env.example` are DEVELOPMENT examples only and do not freeze global production policy.

The event-admission rate limiter runs inside `IngestCore.Begin` after authentication/authorization and before body receipt. Socket connection-rate and concurrent-connection controls remain transport-frontier controls. `/livez` reports process liveness; `/readyz` requires the NATS connection and the `CERBERO_RAW` stream to be available. The development `cerbero_ingest` NATS identity receives only the additional `$JS.API.STREAM.INFO.CERBERO_RAW` metadata-query permission needed for that check. The configured JSON ingest path is DEVELOPMENT-only and is not frozen as the production external API contract.

`make integration` now performs a real JSON/HTTP request through the composed runtime, verifies `202 Accepted`, reads the resulting `cerbero.v1.raw.received` message from JetStream, validates request/message correlation, and verifies that the `RawEvent` contains the exact HTTP bytes.

## Deliberately not implemented after Step 4B

Payload preparation is not a durable-acceptance operation and must not be exposed as an HTTP `2xx` success by itself. The architecture requires durable JetStream admission before reporting acceptance. Therefore these responsibilities remain outside this commit:

- syslog adapter skeleton and journald collector contract;
- production TLS/mTLS source authentication, certificate/revocation integration, and production security-profile wiring;
- raw-preserver, Raw Store persistence, `raw.persisted`, ACK/retry/DLQ behavior (Milestone 3).

This boundary prevents M2 unit code from claiming the stronger acceptance guarantee that only the durable event bus can provide.
