# Milestone 4 normalization runtime governance

This increment closes the first operational-governance layer around the durable normalizer without changing the first SSH-to-OCSF parser/mapping identities.

## Durable permanent-failure boundary

A permanent normalizer failure is no longer terminated directly. The runtime first writes `cerbero.normalization_dlq.v1` to `cerbero.v1.dlq.normalization`, verifies a `CERBERO_DLQ` JetStream PubAck, records `dlq_normalization_total`, and only then sends `AckKind::Term` for the original `raw.persisted` delivery.

If DLQ publication fails, the original delivery is NAKed and remains recoverable. Raw Store remains authoritative evidence.

## Failure classification

Dead letters use stable low-cardinality error categories and stages. Error codes remain the primary machine-readable classifier.

## Retry behavior

Retryable failures continue to NAK with exponential backoff bounded by the configured minimum and maximum. A deterministic 80–120 percent jitter window is derived from stable delivery identity and attempt number, then clamped to the configured bounds.

The exact global production retry budget remains open. This increment does not invent a persistent retry-attempt ledger.

## Backend-neutral metrics

The normalizer exposes semantic metric names and a concurrency-safe in-memory recorder. No Prometheus/OpenTelemetry backend is selected because the baseline leaves the exporter open.

The recorder covers events parsed/normalized, parse and normalization status counters, parser/normalization latency observations, governed parser/mapping ID counts, and `dlq_normalization_total`. No event ID, username, IP address, raw source value, or other high-cardinality event field is used as a metric key.

## Source timestamp policy

`CERBERO_NORMALIZER_SOURCE_TIME_OFFSETS` is a DEVELOPMENT-only representation for ADR-0014:

```text
source-a=-04:00,source-b=+05:30
```

The normalizer may resolve an RFC3164 candidate only when the selected parser is `cerbero.parser.syslog.rfc3164` and the matching `source_id` has configured policy. Candidate precision text alone never activates the RFC3164 policy. The persisted raw handoff is not mutated. Changing the policy changes effective configuration hash and derivation identity.

## Still open in Milestone 4

- persistent retry-budget state across process restarts;
- automatic DLQ reprocessing;
- IANA/DST-aware Source timezone policies;
- REPLAY/TEST runtime isolation;
- historical parser-v2 renormalization;
- fuzz-target expansion.
