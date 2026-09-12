# ADR-0003 — Filesystem-backed development Raw Store

## Status

Accepted

## Context

STORAGE v1.0 locks the Raw Store as object/file-based and separate from PostgreSQL/ClickHouse, but explicitly leaves the concrete object-storage provider open. Milestone 0 needs a reproducible development Raw Store without prematurely making MinIO, S3, Ceph, or another provider an architectural dependency.

## Decision

Use the local filesystem under `var/raw/` as the **development-only** Raw Store backend.

The runtime layout will follow the locked segmented hierarchy when Milestone 3 implements raw preservation:

```text
raw/<tenant>/YYYY/MM/DD/HH/<segment_id>/
```

Milestone 0 tracks only the boundary/documentation; it does not generate fake raw segments.

## Consequences

- Development requires no additional object-storage daemon.
- Exact original bytes can later be persisted and hashed without changing the RawEvent conceptual contract.
- Production object storage remains an open deployment decision.
- Code must depend on a Raw Store abstraction rather than filesystem-only assumptions.

## Alternatives considered

- MinIO/S3-compatible service in Milestone 0: rejected as premature provider selection.
- PostgreSQL byte storage: rejected by the locked storage authority separation.
- ClickHouse raw authority: rejected by the locked storage model.

## References

- CERBERO — STORAGE v1.0, Raw Store physical layout and development deployment sections
