# ADR-0014 — Source timestamp policy v1

## Status

Accepted

## Context

Source timestamps are not always absolute. RFC3164 contains month/day/time but no year or timezone. The parsing baseline forbids inventing UTC and permits a timezone configured on the Source to resolve ambiguity when the assumption is explicitly recorded.

The normalizer must keep `event_time` distinct from `ingest_time`.

## Decision

The initial normalizer Source-time policy is an explicit per-`source_id` fixed UTC offset:

```text
source_id=+HH:MM
source_id=-HH:MM
```

The DEVELOPMENT runtime accepts a comma-separated registry through `CERBERO_NORMALIZER_SOURCE_TIME_OFFSETS`. An empty registry means no assumption is available.

For an event parsed specifically by `cerbero.parser.syslog.rfc3164`, an RFC3164 timestamp candidate with no year or timezone is eligible for this policy. Matching the timestamp-candidate precision string alone is insufficient. v1 reconstructs a complete local datetime by evaluating the ingest-local year and its adjacent years under the configured fixed offset, then selecting the candidate nearest to `ingest_time`. This policy is named:

```text
fixed_utc_offset=<offset>;rfc3164_year=nearest_ingest_year
```

The result becomes the normalized derivation's `event_time`; `RawEventPersisted.event_time` and `ingest_time` are never mutated. OCSF metadata records `cerbero_time_source = source_time_policy` and `cerbero_time_assumption = <canonical policy>`.

If `RawEventPersisted.event_time` already exists, it wins. If the Source has no policy, the candidate cannot be resolved, or the parser does not expose a supported candidate, normalization retains partial/fallback behavior and UTC is not assumed.

The effective transformation configuration hash and logical derivation key include the canonical policy for the applicable `source_id`, so changing Source-time policy creates a distinct historical derivation.

This v1 decision uses fixed offsets only. IANA timezone databases and DST-aware policy remain future controlled expansion.

## Consequences

- Ambiguous RFC3164 timestamps can be resolved only with explicit Source policy.
- The assumption is machine-visible in normalized provenance.
- `ingest_time` remains transport chronology, not a substitute for source event time.
- Policy changes preserve historical non-destructive semantics.
- DST-sensitive Sources require a future IANA-zone policy.

## Alternatives considered

### Assume UTC when timezone is missing

Rejected by the locked baseline.

### Always use the ingest year without checking adjacent years

Rejected because year rollover around New Year can produce an obviously wrong year.

### Use host local timezone

Rejected because host configuration is not Source provenance and would make results deployment-dependent.

## References

- `PARSING & OCSF v1.0`
- `CONTRACTS v1.0`
- ADR-0011 — first OCSF normalization vertical
- ADR-0012 — journald canonical raw representation v1
