# Development Raw Store

This directory is the filesystem-backed Raw Store selected for local development in ADR-0003.

The durable layout introduced with raw preservation will follow the locked shape:

```text
raw/<tenant>/YYYY/MM/DD/HH/<segment_id>/
```

M3 Step 6 implements the ADR-0003 development backend here. It uses the RawEvent `ingest_time` in UTC and, for this backend only, creates one provisional segment directory per `event_id` with `raw.bin` as the exact evidence object. The logical locator is `raw:///<tenant>/YYYY/MM/DD/HH/<event_id>/raw.bin`.

This one-event development representation does not freeze production segment sizing or rotation. The directory is not considered a closed segment yet: manifest generation/hash chaining remains a separate M3 increment because the definitive manifest schema is still open.

Runtime content is ignored by Git; only this documentation and the ignore policy are versioned.
