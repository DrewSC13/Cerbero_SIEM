# Development Raw Store

This directory is the filesystem-backed Raw Store selected for local development in ADR-0003.

The durable layout introduced with raw preservation will follow the locked shape:

```text
raw/<tenant>/YYYY/MM/DD/HH/<segment_id>/
```

Milestone 0 does not manufacture raw segments. Runtime content is ignored by Git; only this documentation and the ignore policy are versioned.
