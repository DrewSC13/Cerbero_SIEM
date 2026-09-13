# Raw preservation

Milestone 3 turns durable bus admission into durable evidence preservation.

The locked processing order is:

```text
cerbero.v1.raw.received
  -> validate envelope / RawEvent
  -> check transport idempotency
  -> persist exact raw bytes
  -> persist RawEvent locator metadata
  -> verify durable write
  -> publish cerbero.v1.raw.persisted
  -> ACK cerbero.v1.raw.received
```

`cerbero-raw-preserver` is a standalone Go service per ADR-0008 and will use the shared v1 contract bindings plus a dedicated durable JetStream consumer.

## Authorities

- Raw Store: authoritative exact raw payload bytes.
- PostgreSQL: RawEvent locator/control metadata, critical-consumer idempotency, transactional outbox.
- NATS JetStream: durable transport, not historical evidence authority.

No implementation may silently copy raw payload authority into PostgreSQL or ClickHouse.

## Idempotency

Transport redelivery is identified by incoming `message_id`; the critical-consumer key is conceptually `(raw-preserver, message_id)`. `event_id` remains the functional RawEvent identity. `raw_hash` verifies bytes but is not a duplicate-event key.

A repeated delivery must discover and continue an existing preservation operation without creating a second logical RawEvent.

## Durable publication boundary

Raw Store and PostgreSQL/NATS do not participate in one distributed transaction. After the raw write is durable and verified, PostgreSQL records locator/idempotency state and a stable outbox publication for `raw.persisted`. The derived publication uses one stable `message_id` across retries.

The incoming delivery becomes ACK-eligible only after the derived `raw.persisted` publication is durably acknowledged, or when recovery proves that the same effects were already completed.

## M3 Step 2: preservation core

`services/cerbero-raw-preserver/internal/preserver` implements the storage/bus-neutral orchestration boundary.

The core:

- accepts only validated `RawEventReceived` / `cerbero.raw_event.v1` envelopes;
- validates envelope/RawEvent tenant consistency and ADR-0007 request IDs when present;
- checks `(consumer_name, message_id)` state before touching Raw Store;
- passes an immutable copy of the exact raw bytes to `RawStore.EnsureDurable`;
- requires the Raw Store locator to report the same byte length and SHA-256 as the RawEvent;
- commits locator, processed-message, and stable outbox state through one `MetadataStore.CommitPreservation` boundary;
- republishes the same stored outbox `message_id` when recovering an unpublished record;
- returns ACK eligibility only after durable publication and `MarkPublished` complete;
- classifies validation/identity conflicts as isolate candidates and dependency failures as retryable.

The core deliberately returns a disposition instead of ACKing JetStream itself. The future transport adapter owns the actual ACK/NAK/DLQ operation and must obey that disposition.

The `PublicationBuilder` payload remains opaque in Step 2. This prevents implementation code from inventing a `RawEventPersisted` wire payload before contract governance defines it.

## M3 Step 3A: RawEventPersisted contract decision

ADR-0009 closes the missing v1 payload semantics for `cerbero.v1.raw.persisted`. The governed payload is a metadata/locator projection of the preserved RawEvent plus `persisted_at`; it never retransmits `raw_payload`.

## M3 Step 3B: executable RawEventPersisted contract

The canonical source now defines `RawEventPersisted` as `cerbero.raw_event_persisted.v1`, with generated Rust/Go bindings and runtime validation. A shared deterministic wire fixture proves cross-language serialization compatibility. Validation requires UUIDv7 event identity, required tenant/source/locator/pipeline metadata, valid timestamps, lowercase SHA-256 metadata, and `length == raw_size`; the semantic source gate also rejects any future `raw_payload` field in this message.

## M3 Step 4: concrete RawEventPersisted outbox builder

`RawPersistedBuilder` now implements the Step 2 publication boundary using the governed Step 3B contract. After the Raw Store reports verified durable evidence, the builder:

- projects immutable RawEvent metadata without copying `raw_payload`;
- attaches the verified `storage_uri`, `segment_id`, `offset`, and `length`;
- records `persisted_at` as raw-preserver processing time;
- generates one new UUIDv7 `message_id` for the derived publication;
- preserves `tenant_id`, incoming `trace_id`, and `correlation_id`;
- sets `causation_id` to the incoming `raw.received` `message_id`;
- emits `message_type=RawEventPersisted` and `payload_schema=cerbero.raw_event_persisted.v1`;
- serializes the complete `CerberoEnvelope<RawEventPersisted>` deterministically for the transactional outbox;
- propagates the validated ADR-0007 request ID in `Publication.RequestID` for the future NATS publisher.

The builder is invoked only when `(consumer_name, incoming message_id)` has no durable preservation record. The outbox stores the returned derived `message_id` and serialized envelope bytes; retry/recovery republishes that stored publication instead of rebuilding it.

## Still open after M3 Step 4

- concrete object-storage provider;
- exact raw segment size/rotation;
- exact retry count;
- exact backoff intervals;
- DLQ retention;
- final segment-manifest schema beyond the locked STORAGE baseline.
