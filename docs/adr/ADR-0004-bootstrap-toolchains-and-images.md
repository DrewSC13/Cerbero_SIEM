# ADR-0004 — Bootstrap toolchains and development image pins

## Status

Accepted

## Context

The baseline requires dependency locks, container image digest identification, database migrations, and reproducible local/CI execution, but it does not freeze language patch versions or development database/bus image versions.

Milestone 0 therefore needs explicit reference versions rather than “latest”.

## Decision

Reference toolchains:

- Rust 1.98.1
- Go 1.27.1
- Python 3.14.7
- uv 0.12.13

Development infrastructure image locks are recorded in `deploy/compose/images.lock` and referenced by tag **and** registry digest from Compose.

Version upgrades are ordinary dependency maintenance when semantics remain compatible; an ADR is required only when an upgrade changes architecture, contracts, security guarantees, storage semantics, or supported platform assumptions.

## Consequences

- CI and local workstations can target the same language patch versions.
- Development images do not drift silently.
- Upgrades become explicit diffs that can be tested.
- Container/runtime availability remains an integration-test prerequisite; lack of Docker must be reported as pending, never as a passed test.

## Alternatives considered

- Floating `stable`, `latest`, or unpinned images: rejected because they defeat reproducibility.
- Pin only major versions: rejected because patch drift can change build/test behavior.
- Vendor all toolchains in Git: rejected as unnecessary and operationally heavy.

## References

- CERBERO — TESTING / CI / REPRODUCIBILITY v1.0, dependency locks and container image requirements
- CERBERO — SECURITY / PKI / SECRETS v1.0, supply-chain/release controls
