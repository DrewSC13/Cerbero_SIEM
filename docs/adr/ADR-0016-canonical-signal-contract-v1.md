# ADR-0016 — Canonical Signal contract v1

## Status

Accepted

## Context

`ANALYTICAL MODEL v1.0` locks Signal as the elementary output of Detection and locks the logical
fields `signal_id`, `tenant_id`, `rule_id`, `rule_version`, `rule_type`, `severity`, `confidence`,
`status`, `first_observed_at`, `last_observed_at`, `created_at`, `execution_mode`, `source_count`,
`event_count`, `summary`, and `provenance`. It also locks `SignalStatus` and `SignalInput` semantics.

`DETECTION & CORRELATION v1.0` additionally requires EVENT/THRESHOLD matches to produce Signals,
UUIDv7 Signal identity, duplicate-safe processing, rule/version/input/execution provenance, and the
ability to explain which execution backend produced a match.

`CONTRACTS v1.0` deliberately leaves the definitive Signal wire schema open and requires that the
gap be closed by a later governed decision rather than independently inside services. Step 23
therefore stopped at `EventSignalProvenance` and intentionally did not materialize a canonical
Signal object.

Several logical fields do not yet have a frozen value taxonomy. In particular, the baseline defines
the meaning of analytical `severity` and `confidence` and demonstrates categorical values such as
`HIGH` and `LOW`, but it does not lock a complete enum, numeric scale, or scoring formula. The
baseline also permits source `event_time` to be absent and explicitly forbids substituting
`ingest_time` for it.

## Decision

The canonical internal v1 wire source is
`schemas/protobuf/cerbero/contracts/v1/signal.proto` in package `cerbero.contracts.v1`.

### Signal identity and rule provenance

`Signal.signal_id` and `Signal.tenant_id` are canonical UUIDv7 strings.

`rule_id` is the stable `CER-DET-XXXXXX` identifier. `rule_version` is encoded as a non-empty string
that preserves the exact governed rule-version token without inventing a numeric-only version
contract at the wire boundary.

`SignalRuleType` freezes the logical vocabulary already present in the analytical model:

```text
EVENT
THRESHOLD
CORRELATION
```

The presence of `CORRELATION` in this enum preserves the analytical-model vocabulary. It does not
change the Detection & Correlation runtime rule that EVENT/THRESHOLD produce Signal while
CORRELATION produces Finding.

### Severity and confidence

`severity` is a required non-empty string token. `confidence` is an optional non-empty string token.

This decision freezes presence and wire representation only. It does not invent a closed enum,
percentage, floating-point score, or cross-domain conversion policy that the baseline has not
specified. Normalized-event severity, rule severity, Signal/Finding severity, confidence, risk, and
Case priority remain semantically distinct.

A future closed vocabulary may restrict accepted tokens through a compatibility-reviewed decision;
services must not invent such a vocabulary independently.

### Signal status

`SignalStatus` has stable wire values:

```text
SIGNAL_STATUS_UNSPECIFIED = 0
SIGNAL_STATUS_ACTIVE = 1
SIGNAL_STATUS_SUPPRESSED = 2
SIGNAL_STATUS_PROMOTED = 3
SIGNAL_STATUS_INVALIDATED = 4
```

`UNSPECIFIED` is a protobuf sentinel and is invalid for a canonical Signal.

### Temporal semantics

`created_at` is required and records when Cerbero created the Signal.

`first_observed_at` and `last_observed_at` retain the analytical-model meaning: minimum and maximum
contributing source `event_time`. Because source `event_time` can legitimately be absent, both wire
fields use protobuf message presence. They MUST either both be present or both be absent. When
present, `first_observed_at <= last_observed_at`.

A producer MUST NOT substitute `ingest_time`, `normalized_at`, `evaluated_at`, or processing time for
missing source `event_time`. Runtime materialization may obtain authoritative event-time context
from the stored normalized-event record; that plumbing is outside this contract step.

### Execution provenance

`execution_mode` reuses the existing `ExecutionMode` contract and MUST distinguish LIVE, REPLAY, or
TEST.

`SignalProvenance` contains only execution facts not already represented by Signal fields or inputs:

```text
evaluated_at
execution_backend
```

`DetectionExecutionBackend` is:

```text
CLICKHOUSE
STREAM
```

Rule ID/version, execution mode, and input identities are not duplicated inside
`SignalProvenance`; they are canonical top-level Signal/Input fields. This maps the Step 23 EVENT
boundary into a generic Signal contract without making EVENT-only provenance the universal shape.

### Inputs and ordering

A canonical Signal embeds `repeated SignalInput inputs` so a `SignalCreated` payload is independently
traceable to the objects that caused the match. The relational storage model may persist the same
logical relations in `detection.signal_inputs`.

`SignalInput` preserves:

```text
signal_id
input_type
input_id
relation
ordinal
```

Input types are `NORMALIZED_EVENT`, `SIGNAL`, and `ENTITY`. `input_id` is a CERBERO-owned UUIDv7.
`relation` is required but its vocabulary is not frozen by this ADR. `ordinal` is zero-based and MUST
match the position of the input in the repeated wire list. Duplicate `(input_type, input_id)` pairs
are invalid within one canonical Signal.

For `EVENT`, exactly one direct `NORMALIZED_EVENT` input contributes and `event_count == 1`.
Additional non-event inputs may be retained when they are genuine contributing objects. For
`THRESHOLD`, every directly contributing normalized event is represented once and `event_count`
equals that normalized-event input count. This ADR does not authorize a CORRELATION runtime to emit
Signal.

`source_count` and `event_count` use `uint64` and must be positive for canonical Signals. For EVENT
and THRESHOLD, `source_count <= event_count`.

### Lifecycle payload

The already-locked lifecycle subject remains:

```text
cerbero.v1.signal.created
```

The v1 event-bus mapping is:

```text
message_type   = SignalCreated
payload_schema = cerbero.signal.v1
payload        = Signal
```

No additional wrapper message is introduced.

### Validation and code generation

Rust and Go bindings continue to be generated from the same canonical protobuf source. Hand-authored
validators reject invalid UUIDv7 identities, invalid rule IDs, unspecified/unknown enums, missing
required text, invalid timestamps, inconsistent observed-time presence/order, invalid input
ordering, duplicate inputs, and EVENT/THRESHOLD input/count inconsistencies.

## Consequences

- Step 23 can feed a later materialization layer without redefining Signal semantics.
- Signal wire identity, status, input provenance, temporal semantics, backend, and execution mode are
  now explicit and language-neutral.
- Missing source event time remains missing instead of being silently replaced by transport or
  processing time.
- Severity/confidence remain typed as explicit tokens without freezing an unsupported taxonomy.
- Generated Rust/Go bindings remain derived artifacts.
- The `SignalCreated` runtime publisher, persistence transaction, outbox, deduplication store/key
  implementation, and PostgreSQL migrations remain separate implementation steps.

## Alternatives considered

### Infer a severity/confidence enum now

Rejected. The baseline defines meanings and examples but not a complete closed taxonomy or numeric
scale.

### Use floating point for confidence

Rejected. No baseline scale is frozen, and choosing one would make a scoring policy look like a wire
fact.

### Substitute ingest/evaluation time when event time is absent

Rejected. CERBERO explicitly distinguishes source event time from ingest and processing time.

### Store provenance as `google.protobuf.Struct`

Rejected. Core analytical provenance must be inspectable and type-checked rather than an opaque bag
of implementation-specific metadata.

### Derive the public `signal_id` from the logical deduplication key

Rejected. Detection & Correlation requires UUIDv7 public identity and permits a separate logical
deduplication key.

## References

- `CERBERO — CONTRACTS v1.0`
- `CERBERO — ANALYTICAL MODEL v1.0`
- `CERBERO — DETECTION & CORRELATION v1.0`
- `CERBERO — STORAGE v1.0`
- ADR-0005 — contract v1 enum closure and code generation
- ADR-0006 — contract v1 runtime validation
- ADR-0014 — Source timestamp policy v1
- ADR-0015 — normalizer execution domains v1
