# ADR-0007 — Request correlation metadata on the v1 event bus

## Status

Accepted

## Context

CONTRACTS v1.0 and INGEST & EVENT BUS v1.0 require every request/response operation to carry a `request_id` for traceability. INGEST further requires the request identifier to propagate into CerberoEnvelope trace metadata.

The locked v1 `CerberoEnvelope` contains `trace_id`, `causation_id`, and `correlation_id`, but it has no `request_id` field. Those existing fields already have distinct meanings:

- `trace_id` groups a distributed processing flow;
- `causation_id` identifies the message that directly caused another message;
- `correlation_id` represents an explicit functional/security correlation when one exists.

Overloading any of those fields with `request_id` would silently change its locked meaning. Adding `request_id` as a new field to the exact v1 envelope would change the governed v1 contract and is outside the M2 implementation authority.

The HTTP adapter already preserves a UUIDv7 `request_id` in `ingestcore.Result`, but M2 Step 3 must serialize the envelope and publish it to JetStream. A v1-compatible carrier is therefore required before durable publication is implemented.

## Decision

1. The canonical `CerberoEnvelope` v1 protobuf remains unchanged.
2. `trace_id`, `causation_id`, and `correlation_id` retain their existing CONTRACTS v1 semantics and MUST NOT be overloaded with `request_id`.
3. For event-bus messages that originate from a request/response transport and have a request identifier, CERBERO v1 carries that identifier in the NATS message header:

   ```text
   Cerbero-Request-Id: <canonical UUIDv7 request_id>
   ```

4. `Cerbero-Request-Id` is transport trace metadata adjacent to the serialized `CerberoEnvelope`; it is not part of the payload schema and it does not alter the envelope wire contract.
5. Producers MUST validate the header value as canonical RFC-variant UUIDv7 before publication.
6. Consumers MUST treat `Cerbero-Request-Id` as observability/audit correlation metadata only. It MUST NOT grant authentication, authorization, tenant selection, source identity, or data-integrity trust.
7. When a component emits another bus message as part of the same request-associated flow, it SHOULD propagate the validated `Cerbero-Request-Id` header. M3 raw preservation will preserve it when emitting `raw.persisted`.
8. A transport with no request/response identifier omits the header rather than inventing one solely for the bus.
9. A future contract version may add an explicit request-context field. Such a change requires separate contract governance and a compatibility plan; v1 consumers must continue to understand the header while v1 is supported.

## Consequences

- M2 can implement durable JetStream publication without modifying the locked v1 Protobuf envelope.
- `request_id` survives the HTTP-to-event-bus boundary and can be linked to logs, metrics, audit context, and downstream request-associated messages.
- Distributed tracing remains independent because `trace_id` is not repurposed.
- The bus now has one stable CERBERO-owned NATS header that must be covered by producer/consumer tests.
- Consumers must validate the header before using it for observability context and must never treat it as an authorization primitive.
- M3 must copy the header from `raw.received` to the causally produced `raw.persisted` message when present.
- This ADR closes a representation gap; it does not change any `[LOCKED]` v1 field number, subject, ACK rule, retry rule, or RawEvent field.

## Alternatives considered

### Add `request_id` to CerberoEnvelope v1

Rejected. The v1 envelope shape is governed and locked. Adding a field during M2 would silently change that contract and generated bindings.

### Reuse `trace_id` as `request_id`

Rejected. `trace_id` has a distributed-trace meaning independent from the request identifier. Conflating them would erase the distinction already present in the architecture.

### Reuse `correlation_id` or `causation_id`

Rejected. Both fields have explicit meanings that are unrelated to request correlation.

### Drop `request_id` at the bus boundary

Rejected. INGEST explicitly requires preservation and propagation of request correlation metadata.

## References

- CONTRACTS v1.0 — Event Envelope, trace/correlation identifier semantics, API `request_id`, and stable error contract.
- INGEST & EVENT BUS v1.0 — ingest identifiers, HTTP semantics, request correlation, durable publish ordering, and v1 event-bus flow.
- ADR-0006 — Contract v1 runtime validation.
