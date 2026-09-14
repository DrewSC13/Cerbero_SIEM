# ADR-0012 — Journald canonical raw representation v1

## Status

Accepted

## Context

Milestone 2 deliberately left journald canonical serialization open. The ingest boundary may preserve the original journald field set with byte values or accept a future source-specific canonical raw representation, but it does not invent that representation itself.

Milestone 4 requires a minimum `journald canonical representation` parser. A normalizer parser therefore needs one deterministic, byte-safe representation that can preserve repeated fields, distinguish textual from binary values, carry the journald cursor without converting it into CERBERO sequence semantics, and remain independent from OCSF.

## Decision

CERBERO journald canonical raw representation v1 is UTF-8 JSON with this logical shape:

```json
{
  "cerbero_journald_version": 1,
  "cursor": "optional journald cursor or null",
  "fields": [
    {
      "name": "MESSAGE",
      "encoding": "utf8",
      "value": "text value"
    },
    {
      "name": "_BINARY_FIELD",
      "encoding": "base64",
      "value": "AAEC/w=="
    }
  ]
}
```

`fields` is an ordered array rather than a JSON object. This preserves field order and permits repeated journald field names without silently collapsing values.

Each field has exactly one byte representation:

- `encoding = utf8`: `value` is the exact UTF-8 text value;
- `encoding = base64`: `value` is standard RFC 4648 Base64 and is decoded back to exact bytes by the parser.

Version 1 rejects unknown top-level members and unknown field-object members. This prevents parser upgrades from silently discarding source metadata; adding new canonical members requires an explicit compatible contract decision or a new representation version.

The representation does not map fields to OCSF and does not alter the RawEvent. The authoritative evidence remains the exact raw bytes stored by Raw Store.

The parser identity is:

```text
parser_id      = cerbero.parser.journald.canonical
parser_version = 1
```

`__REALTIME_TIMESTAMP` and `_SOURCE_REALTIME_TIMESTAMP`, when present as UTF-8 decimal values, are exposed as high-confidence timestamp candidates in Unix epoch microseconds. They are absolute epoch values and therefore require no timezone assumption. The parser does not overwrite `RawEvent.event_time` or `ingest_time`.

The journald cursor remains source metadata. It is not converted into `RawEvent.sequence_number` by this decision.

## Consequences

- A future journald collector has a governed byte-safe representation it may emit without OCSF coupling.
- Binary field values are preserved without lossy Unicode conversion.
- Repeated journald fields are representable.
- Generic JSON remains a distinct parser; a document carrying `cerbero_journald_version` is selected by the source-specific journald parser before generic JSON under automatic selection.
- Changing this representation in a way that changes parsed meaning requires a new journald canonical/parser version rather than rewriting historical interpretation.

## Alternatives considered

### Treat `journalctl -o json` as the CERBERO canonical representation

Rejected because its handling of binary values and duplicate-field semantics is not a CERBERO-owned stable contract.

### Store journald fields as a JSON object

Rejected because duplicate names would collapse and binary values would require an implicit encoding convention.

### Normalize journald directly to OCSF at ingest

Rejected because parsing and normalization are separate locked stages and RawEvent remains authoritative evidence.

## References

- `PARSING & OCSF v1.0`
- `INGEST & EVENT BUS v1.0`
- `docs/ingest/README.md`
- ADR-0011 — first OCSF normalization vertical
