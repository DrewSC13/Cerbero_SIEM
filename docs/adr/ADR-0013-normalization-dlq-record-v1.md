# ADR-0013 — Normalization DLQ record v1

## Status

Accepted

## Context

The normalizer consumes `cerbero.v1.raw.persisted` at-least-once. Before this decision a permanent parsing, mapping, contract, or integrity failure was terminated at the JetStream consumer with `AckKind::Term` but no durable normalization dead-letter artifact.

The event-bus baseline requires permanent failures to preserve operational context in `cerbero.v1.dlq.normalization`. A DLQ record must also be able to represent an input whose CERBERO envelope itself is malformed, so the DLQ transport cannot require that original envelope to validate successfully.

## Decision

`cerbero.v1.dlq.normalization` is persisted in `CERBERO_DLQ` as a versioned UTF-8 JSON operational record governed by:

```text
schema_version = cerbero.normalization_dlq.v1
schema         = schemas/jsonschema/normalization-dlq-v1.schema.json
```

The record preserves, when available, the original CERBERO message ID, raw event ID, tenant, request/trace/correlation IDs, selected parser identity, JetStream stream/consumer sequence, attempt count, error code/category/stage, error message, and first/last observed failure timestamps. It always stores the original subject and SHA-256 of the original transport payload. It does not duplicate Raw Store evidence.

The runtime ordering for a permanent failure is:

```text
processing failure
  -> build normalization DLQ record
  -> publish cerbero.v1.dlq.normalization
  -> verify CERBERO_DLQ PubAck
  -> increment dlq_normalization_total
  -> Term original raw.persisted delivery
```

If DLQ publication or its PubAck fails, the original delivery is NAKed and retained for retry. `Term` is forbidden before durable DLQ publication.

JetStream deduplication uses a stable `Nats-Msg-Id` derived from the original CERBERO `message_id` together with the original payload SHA-256 when a message ID is available, then stream sequence, then payload hash. Binding the message ID to the payload hash prevents a malformed envelope that reuses an invalid or duplicated ID from collapsing a distinct dead letter. This provides idempotent durable side effects without claiming distributed exactly-once delivery.

The v1 runtime dead-letters permanent failures. A persisted retry-budget ledger across process restarts remains separate work; retryable failures continue to use bounded exponential delay with deterministic jitter and are not silently converted into permanent failures.

## Consequences

- Permanent normalization failures become durable and inspectable.
- A malformed CERBERO envelope can still be represented in the DLQ because the DLQ record is not nested inside the invalid envelope.
- Raw Store remains the authoritative evidence boundary.
- A DLQ outage cannot cause silent event loss because the original consumer delivery is not terminated first.
- Reprocessing from the DLQ remains an explicit REPLAY operation and is not automatic in this increment.

## Alternatives considered

### Wrap every dead letter in `CerberoEnvelope`

Rejected because an invalid source envelope may not provide the fields required to construct a valid replacement envelope without inventing tenant or message identity.

### Term first and publish the DLQ best-effort

Rejected because a DLQ outage would permanently discard the active consumer delivery before durable failure evidence exists.

### Copy raw bytes into the DLQ record

Rejected because Raw Store is the authoritative raw evidence boundary.

## References

- `PARSING & OCSF v1.0`
- `INGEST & EVENT BUS v1.0`
- `OPERATIONS - OBSERVABILITY - RETENTION v1.0`
- ADR-0007 — request correlation metadata on the v1 event bus
- ADR-0011 — first OCSF normalization vertical
