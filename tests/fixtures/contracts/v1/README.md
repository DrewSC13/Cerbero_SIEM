# CONTRACTS v1 wire fixtures

These files are committed golden Protobuf wire fixtures shared by the Rust and Go contract tests.

- `raw_event_minimal.hex` is a valid `RawEvent` containing raw payload `abc`, `raw_size = 3`, and the SHA-256 of those exact bytes.
- `raw_event_invalid_size.hex` is intentionally invalid: it carries the same payload and hash but declares `raw_size = 4`. Both language validators must reject it as a `raw_size` contract violation.

The invalid fixture changes contract metadata only; the raw evidence bytes themselves remain unchanged.
