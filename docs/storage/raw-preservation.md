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

## M3 Step 5A: PostgreSQL durable-state schema

Migration `000002_raw_preservation.sql` establishes the control-plane tables used by the raw-preserver:

- `system.raw_objects` stores `event_id`, `storage_uri`, `segment_id`, byte offset/length, the lowercase SHA-256 locator hash, and creation time. It never stores `raw_payload`.
- `system.processed_messages` materializes critical-consumer idempotency with primary key `(consumer_name, message_id)` and links one processed delivery to its functional `event_id` and stable outbox publication.
- `system.outbox` stores the derived publication `message_id`, subject, optional ADR-0007 request ID, serialized envelope bytes, creation time, and nullable `published_at`.
- SQL `byte_offset` and `byte_length` use `numeric(20,0)` with explicit `uint64` bounds so the storage layer does not silently narrow the governed Protobuf `uint64` range.

`cerbero_raw_preserver` is a `NOLOGIN` least-privilege database role. It may read/insert locator and processed-message rows, read/insert outbox rows, and update only `system.outbox.published_at`. It receives no delete permission and cannot mutate raw locator rows.

The Step 5B adapter must create locator, processed-message, and outbox state in one PostgreSQL transaction after the Raw Store write is verified. Marking `published_at` remains a separate idempotent operation performed only after JetStream confirms the derived publication.

The development Compose init directory runs migrations on fresh PostgreSQL volumes. The integration gate also reapplies the idempotent Step 5A migration explicitly before validating schema/grants, so an existing development volume does not masquerade as migration coverage. No production migration runner is selected by this increment.

## M3 Step 5B: PostgreSQL MetadataStore adapter

`PostgresMetadataStore` is the concrete `MetadataStore` implementation for the Step 5A schema. It receives an already-open `*sql.DB`; database driver choice, credentials, and connection lifecycle remain part of the future runtime composition rather than the preservation core.

`CommitPreservation` uses one PostgreSQL transaction to create or verify the immutable raw locator, persist the exact stable outbox publication, and claim `(consumer_name, incoming message_id)` in `processed_messages`. `byte_offset` and `byte_length` cross the SQL boundary as decimal strings and are decoded with `strconv.ParseUint(..., 64)` so the adapter preserves the complete governed Protobuf `uint64` range.

The adapter verifies existing `event_id` locator metadata and existing derived `message_id` outbox contents before accepting them. If a concurrent transaction wins the same critical-consumer key after tentative locator/outbox inserts, the losing transaction reloads the winner and rolls back its tentative rows before returning the durable record. The core then performs its existing identity/provenance validation, so incompatible duplicates remain isolate candidates rather than silently succeeding.

`MarkPublished` is deliberately outside `CommitPreservation`: it runs only after the publisher reports durable JetStream admission and sets `published_at` with `COALESCE(published_at, now())`, preserving the first completion timestamp across repeated calls. No adapter operation deletes rows, rewrites raw locator metadata, stores raw bytes in PostgreSQL, or claims a distributed transaction with Raw Store/NATS.

The adapter introduces no PostgreSQL driver dependency in Step 5B because it does not open connections. Production connection wiring and the durable JetStream consumer remain later M3 increments.

## M3 Step 6: development filesystem Raw Store adapter

`FilesystemRawStore` implements ADR-0003 for local development under the configured Raw Store root (`var/raw/` in the repository). `RawEvidence` now carries the governed RawEvent `ingest_time`, allowing the adapter to derive a stable UTC hierarchy without substituting local processing time.

The development locator is `raw:///<tenant>/YYYY/MM/DD/HH/<segment_id>/raw.bin`. For this backend only, `segment_id = event_id` and each provisional segment directory contains one RawEvent object at offset `0`. This is an implementation detail of the development backend, not a v1 rule that every RawEvent has its own file or segment; production segmentation size/rotation remains open.

Before any filesystem mutation the adapter validates UUIDv7 event identity, a safe tenant path component, `sha256`, exact byte length, and the hash over the untouched raw bytes. A new object is written to a `0600` temporary file, synchronized, then linked atomically into the final name without overwrite. The containing directory is synchronized and the final file is re-read to verify length and SHA-256 before `EnsureDurable` returns. Concurrent/redelivered calls converge on the same locator; an existing object is accepted only after verification and conflicting bytes are never rewritten.

Directory creation follows the locked tenant/UTC hierarchy and rejects symlink/non-directory path components. Runtime files remain ignored by Git. The adapter uses the local filesystem directly and therefore adds no Compose service or production object-store dependency.

Step 6 deliberately does **not** close the provisional segment or emit a segment manifest. STORAGE v1 requires manifest + manifest hash when a segment is closed, while the definitive manifest schema is still open. Segment closure/chaining is therefore a separate M3 increment rather than an ad-hoc schema invented by this adapter.

## M3 Step 7: JetStream raw.persisted publisher

`JetStreamPublisher` is the concrete preservation `Publisher`. It synchronously publishes the exact serialized outbox payload to `cerbero.v1.raw.persisted`, sets `Nats-Msg-Id` to the already-persisted derived `Publication.MessageID`, and propagates ADR-0007 `Cerbero-Request-Id` transport metadata when present. The adapter does not rebuild or reserialize the envelope.

Success requires a non-nil JetStream PubAck from the locked `CERBERO_RAW` stream with a non-zero stream sequence. A duplicate PubAck is success: this is the required recovery path when JetStream accepted the first publication but the raw-preserver crashed before PostgreSQL `MarkPublished`. Reusing the stable outbox `message_id` allows JetStream duplicate suppression while PostgreSQL remains the authority for whether publication completion has been marked.

The adapter validates the locked subject, UUIDv7 publication identity, optional UUIDv7 request correlation, and non-empty payload before transport. It does not ACK `raw.received`; ACK/NAK/DLQ behavior remains the responsibility of the future durable-consumer runtime. The integration test uses the existing least-privilege `NATS_RAW_PRESERVER_*` identity and verifies the real `CERBERO_RAW` boundary.

## M3 Step 8: real PostgreSQL adapter integration

The PostgreSQL metadata adapter is now exercised against the real development PostgreSQL boundary using `pgx/v5` through `database/sql`. This dependency is introduced for integration coverage only; `PostgresMetadataStore` still receives an already-open `*sql.DB`, so production runtime connection ownership/credential delivery is not selected by this increment.

The integration test creates a temporary login role that inherits the migration-owned `cerbero_raw_preserver` NOLOGIN role, then verifies the adapter using the same least-privilege grants intended for the service. It covers atomic preservation commit, complete `uint64` offset/length round-trip through `numeric(20,0)`, idempotent same-key commit, repeated `MarkPublished`, conflict rollback without orphan locator state, and the expected failure of an application-role `DELETE`. Test records and the temporary login role are removed using the development administrator after the test.

This closes the SQL-semantic gap between the package-local transaction fakes and PostgreSQL 18 while preserving the architecture rule that raw bytes never enter PostgreSQL and that Raw Store/PostgreSQL/NATS do not form one distributed ACID transaction.

## Still open after M3 Step 8

- production object-storage provider;
- exact raw segment size/rotation;
- exact retry count;
- exact backoff intervals;
- DLQ retention;
- final segment-manifest schema beyond the locked STORAGE baseline.
