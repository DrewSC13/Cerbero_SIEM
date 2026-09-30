# ADR-0017 — Canonical Finding contract v1

## Status

Accepted

## Context

`ANALYTICAL MODEL v1.0` locks Finding as an analytically relevant observation distinct from Signal,
Incident, and Case. It locks the logical Finding fields `finding_id`, `tenant_id`, `finding_type`,
`status`, `severity`, `confidence`, `title`, `description`, `first_seen`, `last_seen`, `created_at`,
`updated_at`, `primary_rule_id`, `primary_rule_version`, `correlation_rule_id`,
`correlation_rule_version`, `disposition`, and `metadata`. It also locks the Finding type vocabulary,
`FindingStatus`, disposition vocabulary, preservation of `FindingInput[]`, LIVE/REPLAY/TEST isolation,
and the rule that Correlation Engine produces Finding.

`STORAGE v1.0` originally marked the complete Finding table shape open, but it already locks the
generic `investigation.finding_inputs` relation to `finding_id`, `input_type`, `input_id`, and
`relation`. The later Analytical Model closes the semantic Finding schema while preserving the
storage relation shape.

`DETECTION & CORRELATION v1.0` locks correlation provenance to
`correlation_rule_id`, `correlation_rule_version`, `window_start`, `window_end`, `input_ids`,
`output_finding_id`, `execution_backend`, `execution_mode`, and `configuration_hash`. Step 27
intentionally stopped at `SequenceCorrelationMatch` because no canonical Finding wire contract had
yet been frozen.

The baseline does not freeze a complete severity or confidence taxonomy, does not freeze a closed
vocabulary for `FindingInput.input_type` or `FindingInput.relation`, and does not define a global
correlation-rule identifier format analogous to `CER-DET-XXXXXX`. Those gaps must not be filled by
service-local assumptions.

## Decision

The canonical internal v1 wire source is
`schemas/protobuf/cerbero/contracts/v1/finding.proto` in package `cerbero.contracts.v1`.

### Identity and analytical type

`Finding.finding_id` and `Finding.tenant_id` are canonical UUIDv7 strings. This follows the global
CERBERO identifier contract, which explicitly includes `finding_id` among CERBERO-owned UUIDv7
objects.

`FindingType` has stable values:

```text
DETECTION
THRESHOLD
CORRELATION
ANALYTICAL
```

`UNSPECIFIED` is a protobuf sentinel and is invalid for a canonical Finding.

### Status and disposition

`FindingStatus` freezes:

```text
OPEN
ACKNOWLEDGED
SUPPRESSED
RESOLVED
INVALIDATED
```

`FindingDisposition` freezes:

```text
UNDETERMINED
TRUE_POSITIVE
BENIGN_TRUE_POSITIVE
FALSE_POSITIVE
DUPLICATE
TEST_ACTIVITY
```

Status and disposition remain separate. `UNSPECIFIED` is a protobuf sentinel; `UNDETERMINED` is the
valid unresolved analytical disposition.

This ADR validates current values only. State-transition authorization, suppression policy, audit
side effects, and persistence history remain separate implementation concerns.

### Severity, confidence, title, and description

`severity`, `title`, and `description` are required non-empty strings. `confidence` is an optional
non-empty string.

As with Signal v1, this freezes wire representation and presence only. It does not invent a closed
severity/confidence enum, numeric scale, or conversion policy that the baseline does not specify.

### Temporal semantics and execution domains

`created_at` and `updated_at` are required valid protobuf timestamps.

`first_seen` and `last_seen` preserve observation bounds. They MUST either both be present or both be
absent; when present `first_seen <= last_seen`. Absence remains representable rather than silently
substituting ingest or processing time.

`execution_mode` reuses `ExecutionMode` and MUST distinguish LIVE, REPLAY, or TEST. This satisfies
the Analytical Model requirement that replay/test-derived Findings remain isolated from LIVE
history.

### Rule references

`primary_rule_id`/`primary_rule_version` and
`correlation_rule_id`/`correlation_rule_version` are optional presence-aware pairs. When one member
of a pair is present, both are required and non-empty.

DETECTION and THRESHOLD Findings require the primary rule pair. CORRELATION Findings require the
correlation rule pair. This ADR does not invent a syntax validator for correlation-rule IDs because
no such format is frozen in the baseline.

### Finding inputs

A canonical Finding embeds `repeated FindingInput inputs`. The wire shape is exactly the generic
relation already locked by Storage v1:

```text
finding_id
input_type
input_id
relation
```

`finding_id` MUST equal the containing Finding. `input_id` is a CERBERO UUIDv7. `input_type` and
`relation` are required non-empty tokens, but their closed vocabularies are deliberately not frozen
here.

No `ordinal` field is invented. Generic Finding-input ordering is not defined by Storage or the
Analytical Model. Exact duplicate `(input_type, input_id, relation)` entries are invalid, while the
same input may carry distinct explicit relations when a later governed producer requires it.

### Correlation provenance

CORRELATION Findings embed a required `FindingCorrelationProvenance` carrying exactly the provenance
facts locked by Detection & Correlation v1:

```text
correlation_rule_id
correlation_rule_version
window_start
window_end
input_ids
output_finding_id
execution_backend
execution_mode
configuration_hash
```

The correlation rule/version MUST equal the corresponding top-level Finding pair.
`output_finding_id` MUST equal `Finding.finding_id`. `execution_mode` MUST equal the top-level
Finding execution mode. `window_start <= window_end`. Every correlation `input_id` MUST be a valid
UUIDv7 and MUST be backed by at least one explicit `FindingInput` relation.

`input_ids` remains an ordered repeated field so a SEQUENCE materializer can preserve stage order
without inventing a generic FindingInput ordinal that the storage contract does not define.

`execution_backend` reuses the already-frozen `DetectionExecutionBackend` (`CLICKHOUSE`, `STREAM`).
This ADR does not authorize streaming correlation state semantics; Step 27 continues to reject
STREAM until checkpoint/watermark/state decisions are frozen.

Non-CORRELATION Findings MUST NOT carry `FindingCorrelationProvenance`.

### Metadata

The locked `metadata` field uses `google.protobuf.Struct` as an extension bag. It MUST NOT replace
core typed fields, Finding inputs, execution mode, or correlation provenance. Core analytical
provenance remains structurally typed and validated.

### Generated bindings and validation

Rust and Go bindings are generated from the same canonical protobuf source. Hand-authored Rust and
Go validators reject invalid UUIDv7 identities, unknown/unspecified enums, missing required text,
inconsistent temporal pairs, invalid execution domains, invalid optional rule pairs, missing inputs,
exact duplicate input relations, and inconsistent correlation provenance.

A repository contract gate verifies the exact Finding v1 message/enum field numbers and prevents
correlation provenance from degrading into an opaque `Struct` or `Any` payload.

## Consequences

- Step 27 `SequenceCorrelationMatch` now has a governed canonical object boundary to target.
- Step 29 can materialize the first SEQUENCE correlation result into a canonical CORRELATION Finding
  without inventing FindingInput or provenance fields.
- Finding identity, lifecycle state, disposition, input traceability, replay/test isolation, and
  correlation provenance become language-neutral.
- Generic FindingInput does not pretend that an input-type taxonomy or ordering contract exists when
  the baseline has not frozen one.
- Correlation stage order remains preservable through correlation provenance `input_ids`.
- PostgreSQL migrations, durable persistence, outbox/publisher behavior, logical deduplication key,
  suppression workflows, Finding ↔ Entity, risk contributions, API resources, and TUI views remain
  separate implementation steps.

## Alternatives considered

### Add an ordinal to FindingInput

Rejected. Storage v1 locks the generic FindingInput relation to four fields and does not define a
generic ordering semantic. Sequence order is preserved in the explicitly governed correlation
provenance instead.

### Freeze FindingInput.input_type as an enum now

Rejected. The Analytical Model allows correlation inputs from Signal/Finding/Event collections and
other Finding derivations, but no complete closed input-type taxonomy is frozen.

### Store correlation provenance only in metadata

Rejected. Correlation provenance is a core forensic contract and must remain structurally typed and
validated.

### Infer a correlation-rule ID syntax from detection-rule IDs

Rejected. `CER-DET-XXXXXX` is frozen for detection rules; no equivalent global validator is frozen
for correlation-rule IDs.

### Materialize SequenceCorrelationMatch in the same step

Rejected. Contract closure and producer materialization are kept as separate reviewable boundaries.
Step 29 will perform that mapping against this frozen contract.

## References

- `CERBERO — CONTRACTS v1.0`
- `CERBERO — STORAGE v1.0`
- `CERBERO — ANALYTICAL MODEL v1.0`
- `CERBERO — DETECTION & CORRELATION v1.0`
- ADR-0005 — contract v1 enum closure and code generation
- ADR-0006 — contract v1 runtime validation
- ADR-0015 — normalizer execution domains v1
- ADR-0016 — canonical Signal contract v1
