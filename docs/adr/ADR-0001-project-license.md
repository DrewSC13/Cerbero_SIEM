# ADR-0001 — Project license

## Status

Accepted

## Context

The architectural baseline requires CERBERO to be open source and requires a root `LICENSE`, but it does not select the license for CERBERO-authored code/documentation. A real repository cannot defer the legal terms of contributions and redistribution indefinitely.

Third-party content (for example external rule sets, datasets, schemas, and tooling) can carry separate licenses and attribution requirements.

## Decision

License CERBERO-authored code and documentation under **Apache License 2.0** unless a file or imported component explicitly states another compatible license.

Third-party material is not relicensed by this ADR.

## Consequences

- Contributors and downstream users receive a permissive license with an explicit patent grant.
- License notices and third-party attribution must be preserved where required.
- Any future license change is a governance/legal decision and requires a new ADR plus compatibility review.

## Alternatives considered

- MIT: simpler text but lacks Apache-2.0's explicit patent grant.
- GPL-family copyleft: valid open-source option but imposes redistribution obligations not established by the project baseline.
- Keep license unspecified: rejected because it would make the repository legally ambiguous despite the open-source requirement.

## References

- CERBERO — ARCHITECTURE v1.0 (open-source requirement and repository structure)
- Cerbero project proposal (repository structure includes `LICENSE`)
