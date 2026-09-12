# Contract governance

Milestone 0 creates governance roots only. Milestone 1 delivers the first real wire contracts and generated bindings.

The first contract set will implement:

- `CerberoEnvelope`
- `RawEvent`
- `NormalizedEvent`
- `Transformation`
- `CerberoError`

with UUIDv7 identifiers, protobuf timestamps, RFC3339 UTC externally, SHA-256 over exact raw bytes, contract versioning, producer identity, pipeline version, provenance, and compatibility tests.

No placeholder schema is labeled as a contract merely to make the bootstrap look complete.
