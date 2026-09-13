# Raw segment manifest v1

`STORAGE v1.0` requires every **closed** Raw Store segment to have segment data, a manifest, and a manifest hash. ADR-0010 governs the first executable representation.

## Closed-segment artifacts

The DEVELOPMENT filesystem backend closes its provisional one-event segment as:

```text
raw/<tenant>/YYYY/MM/DD/HH/<segment_id>/
├── raw.bin
├── manifest.json
└── manifest.sha256
```

`raw.bin` remains the authoritative exact evidence bytes. `manifest.json` describes the closed segment. `manifest.sha256` contains the SHA-256 of the exact `manifest.json` file bytes as 64 lowercase hexadecimal characters followed by LF.

The locator published through `RawEventPersisted.storage_uri` continues to point to `raw.bin`; manifests are adjacent integrity metadata and do not replace the raw locator.

## Governed manifest

The schema is `schemas/jsonschema/raw-segment-manifest-v1.schema.json`.

The current fields are:

```text
manifest_version
segment_id
tenant_id
event_count
first_event_id
last_event_id
created_at
previous_segment_hash
content_hash
```

For the DEVELOPMENT one-event segment:

- `event_count = 1`;
- `first_event_id = last_event_id = event_id`;
- `content_hash = sha256:<RawEvent.raw_hash>`;
- `previous_segment_hash = null` until ordered multi-event rotation/chaining is governed;
- `segment_id = event_id` remains a development implementation detail, not a global v1 rule.

## Integrity verification

Verification order is:

```text
raw.bin exists and is regular
  -> raw size/hash matches RawEvent
manifest.json exists and validates semantically
  -> content_hash matches raw.bin
manifest.sha256 exists
  -> SHA-256(exact manifest.json bytes) matches
segment directory sync completed
```

No verifier may normalize, pretty-print, trim, or otherwise rewrite either raw evidence or manifest bytes before hashing.

## Still open

This contract does not select:

- production object storage;
- raw segment maximum size;
- event/time-based rotation thresholds;
- a cross-process coordinator for `previous_segment_hash` chaining;
- raw retention or legal-hold policy.
