# ADR-0011 — First OCSF normalization vertical

## Status

Accepted

## Context

Milestone 4 must turn the existing `cerbero-normalizer` Rust bootstrap into the first governed raw-to-OCSF derivation without weakening the invariants already established by Raw Preservation. `PARSING & OCSF v1.0` locks parsing and normalization as separate stages, requires versioned parser and mapping identities, historical 1:N normalization, canonical normalized hashing, explicit LIVE/REPLAY/TEST execution modes, and a first vertical based on Linux `sshd` authentication.

The baseline leaves the exact first OCSF version and normalized-hash field scope open. Those decisions must be made before implementation can produce durable normalized rows.

## Decision

The first implementation pins:

```text
OCSF version       = 1.9.0
parser             = linux/sshd@1
mapping            = linux.ssh.authentication@1
OCSF category_uid  = 3 (Identity & Access Management)
OCSF class_uid     = 3002 (Authentication)
OCSF activity_id   = 1 (Logon)
auth_protocol_id   = 99 (Other)
auth_protocol      = SSH
```

`normalized_hash` is:

```text
SHA-256(canonical JSON bytes of NormalizedEvent.ocsf_event)
```

The canonical JSON representation recursively sorts object keys, preserves array order, emits UTF-8 without insignificant whitespace, and hashes only the OCSF event object. Transport metadata, protobuf envelope bytes, `normalized_event_id`, and `normalized_at` are outside this hash scope.

Parser selection is explicit and explainable through a parser registry. A tie at the highest probe score is a permanent error; selection order is never used as an implicit tie-breaker.

The first SSH mapping represents failed authentication as OCSF Authentication/Logon with failure status. SSH is represented using the OCSF `Other` protocol identifier plus the source-specific `SSH` label.

If authoritative `event_time` is absent, the RawEvent remains unchanged and no timestamp is invented. Because OCSF Base Event requires `time`, the first mapping may use `ingest_time` only as an explicitly recorded fallback and must emit `NormalizationStatus.PARTIAL` with `metadata.cerbero_time_source = ingest_time_fallback`.

Logical normalization identity includes at least:

```text
raw_event_id
parser_id + parser_version
mapping_id + mapping_version
ocsf_version
pipeline_version
configuration_hash
execution_mode
```

Changing any of these is intentional re-normalization, not duplicate processing.

## Consequences

- Raw evidence is never mutated or replaced by OCSF output.
- The same RawEvent can have multiple historical NormalizedEvents when interpretation changes.
- Duplicate delivery of one logical derivation can be recognized independently of the public UUIDv7.
- Hash verification is reproducible across runtimes because the exact byte scope is governed.
- The first mapping is intentionally narrow; generic JSON, RFC3164/RFC5424, journald and broader Linux authentication coverage remain Milestone 4 work.

## Alternatives considered

### Hash the complete NormalizedEvent protobuf

Rejected because protobuf envelope/identity/timestamp changes would alter the hash even if canonical analytical meaning stayed identical.

### Track OCSF `main`

Rejected because the development branch is not a stable schema contract. CERBERO pins the released 1.9.0 schema for this vertical.

### Treat missing `event_time` as `ingest_time` silently

Rejected because it violates the locked timestamp provenance rule.

## References

- `PARSING & OCSF v1.0`
- `CONTRACTS v1.0`
- `STORAGE v1.0`
- OCSF Schema 1.9.0
