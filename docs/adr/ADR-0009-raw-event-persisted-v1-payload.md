# ADR-0009 — RawEventPersisted v1 payload closure

## Status

Accepted

## Context

CONTRACTS v1.0 locks `RawEventPersisted` as an initial domain event and `cerbero.v1.raw.persisted` as its v1 subject, but the canonical Protobuf source currently defines no `RawEventPersisted` payload message. M3 cannot safely implement the outbox builder or normalizer handoff by inventing that payload inside service code.

INGEST & EVENT BUS v1.0 locks `raw.persisted` after exact-byte persistence, raw-locator persistence, and durable-write verification; the normalizer consumes `raw.persisted`, never `raw.received`.

PARSING & OCSF v1.0 defines the logical normalizer input as `CerberoEnvelope<RawPersisted>` and requires it to permit recovery of the RawEvent, raw storage location, raw hash, source metadata, sensor metadata, and pipeline version.

STORAGE v1.0 keeps exact raw bytes authoritative in Raw Store and defines PostgreSQL raw-locator metadata including `event_id`, `storage_uri`, `segment_id`, `offset`, `length`, and hash. Re-embedding `raw_payload` in `raw.persisted` would duplicate evidence on the event bus and blur the Raw Store authority.

## Decision

1. CERBERO v1 adds a new compatible payload message named `RawEventPersisted` in `schemas/protobuf/cerbero/contracts/v1/raw_event_persisted.proto`. Existing v1 message field numbers and meanings remain unchanged.
2. The event-bus mapping is fixed as:

   ```text
   subject          = cerbero.v1.raw.persisted
   message_type     = RawEventPersisted
   payload_schema   = cerbero.raw_event_persisted.v1
   payload          = google.protobuf.Any<RawEventPersisted>
   ```

3. `RawEventPersisted` is a durable-evidence reference plus the immutable RawEvent metadata required by the normalization boundary. It MUST NOT contain `raw_payload`.
4. The v1 field layout will be:

   ```proto
   message RawEventPersisted {
     string event_id = 1;
     string tenant_id = 2;
     string source_id = 3;
     string sensor_id = 4;
     google.protobuf.Timestamp event_time = 5;
     google.protobuf.Timestamp ingest_time = 6;
     string content_type = 7;
     string encoding = 8;
     uint64 raw_size = 9;
     string raw_hash_algorithm = 10;
     string raw_hash = 11;
     string transport = 12;
     string remote_identity = 13;
     optional uint64 sequence_number = 14;
     IntegrityStatus integrity_status = 15;
     string pipeline_version = 16;
     string storage_uri = 17;
     string segment_id = 18;
     uint64 offset = 19;
     uint64 length = 20;
     google.protobuf.Timestamp persisted_at = 21;
   }
   ```

5. Fields 1–16 are a payload-free projection of the immutable RawEvent metadata. The builder MUST copy them from the validated `RawEvent`; it must not reinterpret source metadata or timestamps.
6. `storage_uri` is an internal opaque locator. Consumers must not infer authorization or trust from its contents. `segment_id`, `offset`, and `length` locate the event inside the segmented Raw Store.
7. `length` MUST equal `raw_size`. `raw_hash_algorithm` remains `sha256`; `raw_hash` remains the lowercase SHA-256 of the exact original bytes. The raw-preserver MUST verify these invariants before emitting the event.
8. `persisted_at` records when the raw-preserver confirmed durable preservation. It is processing metadata and MUST NOT replace or modify `event_time` or `ingest_time`.
9. The derived `CerberoEnvelope` MUST use a new stable UUIDv7 `message_id`, preserve `tenant_id`, preserve the incoming `trace_id`, set `causation_id` to the `raw.received` `message_id`, and preserve `correlation_id` when present. The stable derived `message_id` is created once when the outbox record is built and reused for all publication retries.
10. ADR-0007 `Cerbero-Request-Id` remains NATS transport metadata and is propagated when valid; it is not added to the payload.
11. The normalizer may use the locator/hash/metadata in `RawEventPersisted` to retrieve and verify the authoritative raw bytes. It MUST verify accessibility before declaring normalization success.
12. `RawEventPersisted` is a notification that durable evidence exists, not a second authoritative copy of the RawEvent bytes.
13. This is a compatible v1 contract closure because it adds a new message for an already-locked domain event/subject without changing any existing message field or enum meaning.

## Validation invariants

Runtime validation of `RawEventPersisted` must require at least:

- canonical UUIDv7 `event_id`;
- non-empty `tenant_id`, `source_id`, `storage_uri`, `segment_id`, and `pipeline_version`;
- valid optional `event_time`;
- valid required `ingest_time` and `persisted_at`;
- `raw_hash_algorithm == "sha256"`;
- lowercase 64-character hexadecimal `raw_hash`;
- `length == raw_size`;
- no raw payload field.

`sensor_id` remains subject to the source policy already established for RawEvent and is therefore not made globally mandatory by this new contract.

## Consequences

- M3 can replace the opaque Step 2 `PublicationBuilder` payload with a governed v1 message.
- The normalizer receives enough metadata to locate and integrity-check the preserved evidence without retransmitting raw bytes through JetStream.
- Raw Store remains the authoritative raw-payload store; PostgreSQL remains the locator/control authority.
- `raw.persisted` publications remain small relative to raw evidence size.
- Rust and Go generated bindings, semantic contract checks, and runtime validators must be extended in the implementation increment following this ADR.
- Future locator evolution must preserve v1 compatibility or introduce an appropriately versioned payload contract.

## Alternatives considered

### Reuse `RawEvent` as the `raw.persisted` payload

Rejected. `RawEvent` contains `raw_payload`; republishing it would duplicate the full evidence bytes on the bus and obscure the Raw Store authority.

### Publish only `event_id`

Rejected. PARSING & OCSF requires the trigger to permit recovery of storage location, hash, source/sensor context, and pipeline version. An ID-only event would force an undocumented coupling and would not express the required integrity context.

### Publish only the storage URI

Rejected. A URI alone loses the stable RawEvent identity, hash, source metadata, and pipeline provenance required by downstream processing.

### Add locator fields to `RawEvent`

Rejected. `RawEvent` describes evidence at the ingest boundary. Adding post-persistence locator state would change the meaning of the immutable acquisition contract.

### Put `request_id` in `RawEventPersisted`

Rejected. ADR-0007 already defines request correlation as NATS transport metadata for v1 and explicitly keeps it out of the locked envelope/payload model.

## References

- CONTRACTS v1.0 — domain events, common envelope, versioned payload schemas, trace/causation semantics.
- INGEST & EVENT BUS v1.0 — `raw.received` → raw-preserver → `raw.persisted`, durable-write/ACK ordering.
- STORAGE v1.0 — Raw Store authority, segmented layout, PostgreSQL raw locator metadata.
- PARSING & OCSF v1.0 — `CerberoEnvelope<RawPersisted>` logical normalizer input and required recoverability.
- ADR-0007 — request correlation event-bus metadata.
- ADR-0008 — raw-preserver service and durable publication boundary.
