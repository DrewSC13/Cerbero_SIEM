# Contract tests

Milestone 1 contract verification is split into source-schema checks and generated-binding tests.

The source gate (`make contracts`) verifies:

- exact v1 message field names, types, optional presence, and field numbers;
- exact locked enum values and the ADR-0005 enum closure;
- the `Transformation.execution_mode` replay/test provenance field;
- Buf STANDARD lint (with the documented locked `ErrorCategory` exception);
- successful Protobuf schema compilation via `buf build`.

Generated-binding tests added in the second Milestone 1 step cover serialization/deserialization, compatibility fixtures, UUIDv7 and timestamp validation, raw SHA-256 mismatch, duplicate delivery identity, replay execution mode, parser failure, and invalid payload behavior.
