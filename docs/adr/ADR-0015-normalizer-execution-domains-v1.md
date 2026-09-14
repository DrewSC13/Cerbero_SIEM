# ADR-0015 — Normalizer execution domains v1

## Status

Accepted

## Context

CERBERO must distinguish LIVE, REPLAY, and TEST processing. A replay must never silently replace LIVE history, while TEST activity must not contaminate production analytical risk by default. The v1 event namespace already locks normalized lifecycle publication to `cerbero.v1.normalized.created`; creating parallel replay/test domain subjects would change that contract without need.

JetStream also provides durable replay semantics. The normalizer therefore needs execution-domain isolation without inventing a second event model or a second normalized-event store.

## Decision

The normalizer keeps the canonical lifecycle subject:

```text
cerbero.v1.normalized.created
```

Every normalized lifecycle publication carries:

```text
Cerbero-Execution-Mode: LIVE | REPLAY | TEST
```

The durable RawEvent consumers are mode-specific:

```text
LIVE   -> normalizer
REPLAY -> normalizer-replay
TEST   -> normalizer-test
```

`normalizer` is long-lived. REPLAY and TEST consumers are durable while a run is active, so an unclean process failure can resume from the durable consumer state. After an explicit clean shutdown the normalizer deletes the REPLAY/TEST durable consumer. A later explicit replay/test run can therefore traverse retained RawEvent history again and converge through logical idempotency.

Only one active REPLAY worker and one active TEST worker per environment are supported by this v1 consumer naming scheme.

The ClickHouse `normalized_events` authority remains shared. Isolation is historical and semantic, not a separate database: each row stores the Transformation execution mode, and the logical derivation identity contains `execution_mode`. Consequently a REPLAY or TEST derivation cannot collide with a LIVE derivation.

Downstream analytical consumers MUST treat LIVE as the production default. They may consume REPLAY or TEST only when the execution domain is explicitly enabled. Until the detection runtime is implemented, this requirement is carried by the publication header and stored Transformation provenance.

## Historical renormalization

The logical derivation key is computed from the parser and mapping identities actually executed, including parser and mapping versions. Parser v2 is therefore a new derivation, not a duplicate delivery.

The current governed proof is:

```text
RawEvent R1
├── N1  linux/sshd@1  LIVE
└── N2  linux/sshd@2  REPLAY
```

N1 remains byte-for-byte represented by its original stored identity/hash row. No update or delete is used to manufacture N2.

## Consequences

- The locked subject namespace remains unchanged.
- LIVE, REPLAY, and TEST have distinct durable consumer progress.
- Replay/test processing remains observable on the same canonical lifecycle subject.
- Historical normalized rows coexist in ClickHouse.
- Execution mode remains provenance rather than source event time.
- A crash preserves REPLAY/TEST progress; a clean end resets that explicit execution domain for a later run.
- Changing replay configuration while resuming an unclean existing replay consumer is an operator error in v1; finish or delete that execution-domain consumer before starting a materially different replay.

## Alternatives considered

### Separate replay/test NATS subjects

Rejected because the v1 normalized lifecycle subject is already locked and execution mode is provenance, not a different domain event.

### Separate ClickHouse tables

Rejected because RawEvent 1:N NormalizedEvent history is a first-class property and `execution_mode` already distinguishes derivations.

### Ephemeral replay consumers

Rejected because an interrupted replay would lose durable progress and violate the durable-consumer operating model.

## References

- `CONTRACTS v1.0`
- `PARSING & OCSF v1.0`
- `INGEST & EVENT BUS v1.0`
- `ANALYTICAL MODEL v1.0`
- ADR-0011 — first OCSF normalization vertical
- ADR-0013 — normalization DLQ record v1
