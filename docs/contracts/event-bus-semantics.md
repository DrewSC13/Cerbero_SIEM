# Event-bus contract semantics v1

This document records the Milestone 1 event-bus semantics required by CONTRACTS v1.0. It freezes contract behavior only; transport consumers, persistence, retry scheduling, and DLQ handling are implemented by their owning milestones.

## Versioned NATS subjects

CERBERO event-bus subjects use the locked namespace:

```text
cerbero.v1.<domain>.<event>
```

The v1 lifecycle subjects are:

```text
cerbero.v1.raw.received
cerbero.v1.raw.persisted
cerbero.v1.normalized.created
cerbero.v1.signal.created
cerbero.v1.finding.created
cerbero.v1.incident.created
cerbero.v1.incident.updated
cerbero.v1.case.created
cerbero.v1.case.updated
cerbero.v1.audit.created
```

`cerbero.v1.system.*` is reserved for operational events. Dead-letter routing uses `cerbero.v1.dlq.<domain>`; initial domain examples include `cerbero.v1.dlq.raw`, `cerbero.v1.dlq.normalization`, and `cerbero.v1.dlq.detection`.

The development NATS configuration may use wildcard subscriptions or stream subjects to implement these boundaries, but wildcard configuration does not redefine the wire subject namespace.

## ACK policy

A consumer MUST acknowledge a message only after the durable effect required from that consumer has been confirmed.

For Raw Preservation, the contract sequence is:

```text
receive
  -> validate envelope
  -> check idempotency
  -> persist RawEvent
  -> persist required metadata
  -> ACK
```

The inverse ordering is forbidden: acknowledging first and attempting persistence afterward can lose evidence if the consumer fails between those operations.

A redelivery after a durable write but before ACK must be handled idempotently. Reprocessing the same `message_id` must not create a duplicate durable side effect; once the existing effect is recognized, the message may be acknowledged.

## Retry policy

Automatic retry is permitted only for failures classified as transient. Typical transient classes include temporary storage unavailability, temporary network failure, dependent-service unavailability, and resource pressure.

Permanent failures such as an invalid schema, unsupported encoding, payload limit violation, invalid contract version, or invalid required field MUST NOT enter an unbounded automatic retry loop.

Retries use backoff. CONTRACTS v1.0 deliberately leaves the exact attempt count, intervals, jitter, and DLQ retention duration open; implementations must not invent those values as wire-contract guarantees.

`CerberoError.retryable` communicates the classification used by the owning workflow; it does not override the rule that only transient failures may be retried automatically.

## DLQ policy

A message that cannot be processed through the normal retry policy may be routed to the versioned dead-letter namespace:

```text
cerbero.v1.dlq.<domain>
```

DLQ routing is for non-recoverable processing after the applicable retry policy has been evaluated. Exact retention and retry-count thresholds remain open decisions and are not frozen by Milestone 1.

## Contract scenario: input to durable ACK

Milestone 1 freezes the following scenario as the required behavior for the ingest/raw-preservation path:

```text
exact input bytes
  -> assign CERBERO identities and ingest_time
  -> calculate SHA-256 over the exact raw bytes
  -> construct and validate RawEvent
  -> wrap RawEvent in CerberoEnvelope
  -> durable publish to cerbero.v1.raw.received
  -> Raw Preservation validates envelope and idempotency
  -> persist exact raw evidence and required metadata
  -> durable confirmation
  -> ACK
```

The scenario is a contract requirement, not a claim that Milestone 1 already implements ingest or Raw Preservation. The executable persistence/ACK path belongs to Milestones 2 and 3. Until then, the contract tests cover the wire objects, exact raw bytes/hash, duplicate message identity, and validation behavior that those components must use.
