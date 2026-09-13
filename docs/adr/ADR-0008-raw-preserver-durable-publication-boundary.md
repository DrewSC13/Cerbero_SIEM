# ADR-0008 — Raw-preserver service and durable publication boundary

## Status

Accepted

## Context

Milestone 3 implements the locked preservation path:

```text
cerbero.v1.raw.received
  -> raw-preserver
  -> validate envelope
  -> check idempotency
  -> persist exact raw bytes
  -> persist raw locator metadata
  -> verify durable write
  -> publish raw.persisted
  -> ACK raw.received
```

The external architecture fixes the component boundary (`raw-preserver` owns evidence persistence), at-least-once delivery, transport deduplication by `message_id`, functional RawEvent identity by `event_id`, and ACK only after the durable side effect or confirmed prior idempotent completion.

STORAGE v1.0 separately fixes the RawEvent payload authority in Raw Store, the RawEvent locator authority in PostgreSQL, `system.processed_messages` for critical consumer idempotency with `UNIQUE (consumer_name, message_id)`, no global ACID transaction across stores, the PostgreSQL transactional outbox as an allowed reliable DB-to-bus pattern, and a segmented Raw Store with manifests while leaving provider and segment sizing open.

The implementation index did not yet assign `raw-preserver` to a language or decide whether it should be hosted inside the generic worker. M3 must also close a crash window: if evidence and locator state are durable but the process exits around `raw.persisted` publication, recovery must not lose the derived event or create duplicate evidence.

## Decision

1. CERBERO will implement raw preservation as a standalone Go service named `cerbero-raw-preserver`, rooted at `services/cerbero-raw-preserver/`.
2. It is a dedicated durable consumer of `cerbero.v1.raw.received`, not a generic `cerbero-worker` job.
3. The service owns orchestration of evidence persistence, idempotency, publication recovery, retry classification, and final ACK eligibility; storage engines and NATS remain behind explicit interfaces.
4. Raw Store remains authoritative for exact raw payload bytes. PostgreSQL stores locator/control metadata and must never become the authoritative raw-payload store.
5. Transport idempotency is keyed by `consumer_name + raw.received.message_id` using `system.processed_messages` semantics. Equal `raw_hash` values alone do not identify duplicate events.
6. `event_id` remains the functional identity of the preserved RawEvent. Redelivery of the same `message_id` must resolve to the same logical RawEvent without duplicate evidence.
7. The Raw Store write must be idempotent/reconcilable by stable RawEvent identity and verified hash. A crash after raw write but before PostgreSQL completion must recover by rediscovering/verifying the existing object.
8. After exact bytes are durably stored and verified, PostgreSQL records the raw locator and critical-consumer state. The same PostgreSQL transaction creates a transactional-outbox record for the causally produced `cerbero.v1.raw.persisted` event.
9. The outbox stores or references one stable serialized derived publication, including one stable derived `message_id`; retries republish that same logical publication.
10. `raw.persisted` is considered published only after JetStream durable publication acknowledgement; the outbox record is then marked published.
11. `raw.received` may be ACKed only when exact bytes are durable and hash-verified, locator/idempotency state is durable, and the corresponding `raw.persisted` outbox publication is confirmed published—or recovery proves those same effects already complete.
12. Transient Raw Store, PostgreSQL, or NATS failures cause retry/NAK behavior. Exact retry counts and backoff intervals remain open configuration.
13. Permanent invalid/unsupported messages are candidates for `cerbero.v1.dlq.raw`; DLQ details are implemented later in M3 and must preserve locked original-message/failure metadata.
14. ADR-0007 `Cerbero-Request-Id` is propagated from `raw.received` to the stable `raw.persisted` outbox publication when present and valid.
15. M3 does not change the locked Protobuf v1 envelope or RawEvent schema. Any new functional payload contract requires normal contract governance first.
16. Raw Store remains segmented with manifests; this ADR does not select an object-storage vendor, freeze segment size, or redefine the locked manifest baseline.

## Processing state model

```text
RECEIVED
  -> RAW_DURABLE
  -> METADATA_DURABLE_OUTBOX_PENDING
  -> RAW_PERSISTED_PUBLISHED
  -> ACK_ELIGIBLE
```

A restart may discover completed stages and continue forward. Receipt alone is never completion.

## Consequences

- Raw preservation has an independently deployable lifecycle, service identity, NATS permissions, health/readiness surface, and failure domain.
- The generic worker cannot accidentally ACK raw evidence using unrelated job semantics.
- Raw Store remains evidence authority while PostgreSQL remains locator/idempotency/control authority.
- `system.processed_messages` plus outbox make recovery explicit instead of in-memory.
- The outbox closes the DB-to-NATS publication-loss window without claiming a distributed transaction.
- Redelivery can recover from crashes after raw storage, PostgreSQL commit, or around publication.
- Duplicate transport delivery can still occur; downstream consumers remain idempotent by `message_id`.
- M3 must add PostgreSQL migrations for raw locators, processed-message state, and outbox records before claiming full preservation durability.
- M3 must add failure-injection tests for Raw Store outage, PostgreSQL failure, NATS publication failure, crash-before-ACK, and duplicate delivery.
- The normalizer remains blocked from `raw.received`; it consumes only `raw.persisted`.

## Alternatives considered

### Run raw preservation inside `cerbero-worker`

Rejected. Raw preservation is a named component with dedicated durable-consumer, evidence-durability, ACK, retry, backlog, and least-privilege requirements.

### Publish `raw.persisted` directly after PostgreSQL commit without an outbox

Rejected. A crash between DB commit and NATS publication could leave durable evidence with no recoverable derived event.

### Publish before durable locator/idempotency state

Rejected. The architecture requires locator metadata and verified durable preservation before the event that triggers normalization.

### ACK immediately after the Raw Store write

Rejected. The locked flow places locator persistence, durable verification, and `raw.persisted` publication before ACK.

### Deduplicate using only `raw_hash`

Rejected. Equal bytes may be legitimate repeated observations; `message_id` is the transport-redelivery identity.

### Depend on exactly-once delivery or a distributed transaction

Rejected. CERBERO explicitly assumes at-least-once delivery and idempotent side effects.

## References

- INGEST & EVENT BUS v1.0 — raw-preserver flow, `raw.persisted`, ACK semantics, retry/DLQ, idempotency, duplicate delivery and crash scenarios.
- STORAGE v1.0 — Raw Store authority/layout, raw locator authority, `system.processed_messages`, transaction boundaries, outbox pattern, recovery, segmented Raw Store/manifests.
- CONTRACTS v1.0 — `CerberoEnvelope`, `RawEvent`, message/event identity semantics.
- ADR-0007 — request correlation metadata on the v1 event bus.
