# ADR-0010: Raw segment manifest v1

- Status: Accepted
- Date: 2026-09-13
- Baseline: STORAGE v1.0, SECURITY / PKI / SECRETS v1.0

## Context

STORAGE v1 locks a segmented Raw Store. Every closed segment must produce segment data, a manifest, and a manifest hash. The baseline also gives a structural manifest example and requires the definitive fields to live in a dedicated JSON Schema. The development filesystem Raw Store currently persists exact `raw.bin` evidence but deliberately leaves its provisional one-event segment without closure artifacts.

The storage baseline does not freeze the production object-store provider, raw segment size, rotation policy, or a globally ordered multi-event segment writer. Those choices must not be smuggled into the first manifest implementation.

## Decision

CERBERO defines `schemas/jsonschema/raw-segment-manifest-v1.schema.json` as the governed v1 contract for a closed raw segment manifest.

A closed segment has at least these sibling artifacts:

```text
<segment_id>/
├── raw.bin
├── manifest.json
└── manifest.sha256
```

`manifest.json` contains exactly:

- `manifest_version`
- `segment_id`
- `tenant_id`
- `event_count`
- `first_event_id`
- `last_event_id`
- `created_at`
- `previous_segment_hash`
- `content_hash`

`manifest_hash` is represented by the sibling `manifest.sha256` artifact rather than embedded in `manifest.json`. Embedding a digest of the manifest inside the bytes being hashed would introduce a self-reference without adding integrity. `manifest.sha256` is the lowercase SHA-256 of the exact persisted `manifest.json` bytes followed by one LF in the hash file.

`content_hash` is `sha256:<lowercase-hex>` over the exact segment-data object bytes. For the current DEVELOPMENT filesystem backend, the segment data object is `raw.bin` and therefore the content hash is the RawEvent exact-byte hash.

`previous_segment_hash` is either `null` or `sha256:<lowercase-hex>`. A non-null value identifies the previous closed segment manifest hash in an ordered chain. The current DEVELOPMENT one-event backend writes `null`: inter-segment ordering/chaining remains deferred until a multi-event rotation/order policy is governed. This does not redefine the baseline chain or prohibit a future chained implementation.

The development backend may continue to use `segment_id = event_id` as an implementation detail. This ADR does not make one-event segments a v1 storage requirement.

## Serialization and hashing

The producer writes UTF-8 JSON without a BOM and with exactly one trailing LF. The manifest hash covers every byte of that file, including the trailing LF. Consumers/verifiers must hash the persisted bytes; they must not parse and reserialize JSON before hash verification.

`created_at` is the UTC time at which the segment manifest wins immutable publication. Idempotent retries reuse the already-persisted manifest and do not rewrite its timestamp.

## Durability and idempotency

A filesystem segment is closed only after:

1. exact segment data is durable and hash-verified;
2. `manifest.json` is durably published under an immutable name;
3. `manifest.sha256` is durably published and matches the exact manifest bytes;
4. the segment directory is synchronized;
5. a final read verifies all three artifacts.

Redelivery must discover and verify matching artifacts. Existing conflicting data, manifest content, or manifest hash is an integrity conflict and must never be overwritten.

## Consequences

- Raw Preservation can prove segment closure without placing raw bytes in PostgreSQL.
- Manifest semantics are executable through the normal contract gate.
- Crash recovery can complete a partially closed segment idempotently.
- The production object-store implementation, segment rotation/size, and cross-segment chain coordinator remain open and require separate decisions.
