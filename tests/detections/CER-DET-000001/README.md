# CER-DET-000001 — Linux SSH authentication burst

This is the automated STABLE-rule dataset for the first CERBERO detection vertical.

Governed semantics:

- source format: Sigma;
- Cerbero rule ID: `CER-DET-000001`;
- rule version: `1`;
- base event: OCSF `activity_name == "Logon"` and `status == "Failure"`;
- grouping: `user.name` + `src_endpoint.ip`;
- threshold: `COUNT >= 10`;
- window: five minutes using event time;
- deterministic batch late-event policy: `ACCEPT`;
- output: one `Signal` carrying all ten unique contributing NormalizedEvent IDs.

The fixtures intentionally use semantic event labels instead of runtime UUIDs. Rust tests
materialize valid UUIDv7 identities before exercising the real threshold evaluator.

Edge cases cover the frozen baseline set: missing fields, invalid/strange timestamps,
duplicates, out-of-order/late events, and unusual Unicode input.
