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

## Still open after M3 Step 1

- concrete object-storage provider;
- exact raw segment size/rotation;
- exact retry count;
- exact backoff intervals;
- DLQ retention;
- final segment-manifest schema beyond the locked STORAGE baseline.
