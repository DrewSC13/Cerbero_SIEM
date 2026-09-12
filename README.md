# CERBERO

CERBERO is an open-source, terminal-first, API-first SIEM focused on traceability, reproducibility, auditability, interoperability, and least privilege.

**Repository status:** Milestone 0 — Repository Bootstrap. The repository is intentionally not yet a functional SIEM. Milestone 0 establishes the reproducible build/test/documentation/infra foundation required to implement the real pipeline without disposable prototypes.

## Architectural source of truth

The authoritative v1.0 baseline is indexed in `docs/architecture/source-of-truth.md`. The original source documents are intentionally kept outside Git; this repository stores only implementation-facing documentation and decision records. The implementation must not silently change `[LOCKED]` decisions.

Core invariants include:

- original evidence is never silently overwritten;
- transformations carry provenance and versions;
- delivery may repeat, so consumers are idempotent;
- raw evidence is durably preserved before normalization;
- `RawEvent`, `NormalizedEvent`, `Signal`, `Finding`, `Incident`, and `Case` are distinct objects;
- the TUI is an API client, never a privileged database shortcut;
- PostgreSQL, ClickHouse, Raw Store, and Parquet have separate authorities;
- correctness > functionality > performance > scale.

## Milestone 0 toolchains

Reference versions are pinned for reproducibility:

```text
Rust      1.98.1
Go        1.27.1
Python    3.14.7
uv        0.12.13
GNU Make  local/CI task runner
Docker    Compose v2-capable engine for integration checks
```

Run `make doctor` to inspect the current workstation.

## Bootstrap verification

```bash
make verify
make rust-check
make go-check
make python-check
make security-check
```

With Docker Compose available:

```bash
make dev-init
make dev-up
make dev-bootstrap
make dev-health
make integration
```

`make ci` is the local equivalent of the static/unit GitHub Actions gate. Integration is a distinct Docker-backed gate because a workstation without Docker must not be reported as having passed infrastructure tests.

## Repository map

```text
crates/                 Rust dataplane/client component roots
services/               Go network/control services
python/cerbero-tooling/ Python tooling only; not critical ingest path
schemas/                Protobuf, JSON Schema, OCSF governance roots
migrations/             Versioned PostgreSQL and ClickHouse migrations
configs/                Runtime configuration tracked without secrets
deploy/compose/          Pinned local development infrastructure
scripts/                 Local/CI/dev/security/Git automation
parsers/                 Parser implementations from Milestone 4
internal/ocsf-mappings/ Versioned OCSF mappings from Milestone 4
rules/                   Detection rules from Milestone 7
datasets/                Reproducible analytical datasets
tests/                   Cross-component test assets and suites
docs/                    Architecture, contracts, dev, ops, security, testing, ADRs
var/raw/                 Git-ignored development Raw Store runtime data
```

## Planned implementation sequence

1. Common contracts and generated bindings.
2. `RawEvent` + ingest.
3. NATS + raw preservation.
4. Parsing + OCSF normalization.
5. ClickHouse persistence + query API.
6. Minimal Rust TUI.
7. Detection Engine.
8. Correlation + Findings.
9. Entities + Risk.
10. Incidents + Cases.
11. Audit + security hardening.
12. Full reproducible E2E MVP.

The sequence may change only for a documented technical reason that preserves architectural dependencies.

## Git

The repository uses Conventional Commits and topic branches. See `CONTRIBUTING.md` and `docs/development/git-workflow.md` before publishing changes.

The GitHub owner is `DrewSC13`; `CODEOWNERS` is configured for `@DrewSC13`. The intended remote is `https://github.com/DrewSC13/cerbero.git`. The repository does not yet exist through the connected GitHub installation, so remote creation remains an external hosting step before the first push.

## License

CERBERO-authored code and documentation are licensed under Apache License 2.0 as recorded in ADR-0001. Third-party datasets, rules, schemas, and other content retain their own licenses and attribution requirements.
