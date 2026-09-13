# Contract tests

Milestone 1 contract verification is split into source-schema, generated-binding drift, and runtime compatibility tests.

The source gate (`make contracts`) verifies:

- exact v1 message field names, types, optional presence, and field numbers;
- exact locked enum values and the ADR-0005 enum closure;
- the `Transformation.execution_mode` replay/test provenance field;
- Buf STANDARD lint (with the documented locked `ErrorCategory` exception);
- successful Protobuf schema compilation via `buf build`;
- byte-for-byte regeneration cleanliness of committed Rust and Go bindings.

Rust and Go runtime tests cover:

- Protobuf serialization/deserialization;
- one shared cross-language RawEvent wire fixture;
- UUIDv7 validation for CERBERO-owned IDs;
- Protobuf timestamp validity;
- exact raw SHA-256 and raw byte-count consistency;
- duplicate delivery retaining the same `message_id`;
- explicit REPLAY execution mode surviving serialization;
- parser failure represented as `Transformation FAILED` with error provenance while RawEvent remains unchanged;
- invalid raw metadata and raw hash mismatch rejection.

Normalized hash scope, OCSF structural validation, parser fuzzing, persistence behavior, ACK/retry/DLQ integration, and service-level deduplication remain with their owning milestones rather than being simulated inside the common contract library.
