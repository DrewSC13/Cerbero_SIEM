# ADR-0002 — Local task runner

## Status

Accepted

## Context

TESTING / CI / REPRODUCIBILITY v1.0 locks local parity with GitHub Actions but explicitly leaves the task runner open. Milestone 0 needs one stable command surface for formatting, linting, builds, tests, security checks, and Compose integration.

## Decision

Use **GNU Make** as a thin orchestration layer. Make targets delegate to normal language-native and shell/Python tools; business logic does not live in Make.

GitHub Actions invokes the same Make targets.

## Consequences

- Local and hosted CI commands remain visible and reproducible.
- No custom task-runner dependency is required.
- Platform-specific shell assumptions are limited to the Linux distributions named by the baseline.
- Replacing Make later is possible without changing component contracts because it is orchestration only.

## Alternatives considered

- Task/Just: capable, but would add another bootstrap binary before a demonstrated need.
- GitHub Actions-only scripts: rejected by the locked local-parity requirement.
- Ad-hoc developer commands only: rejected because they drift and are difficult to audit.

## References

- CERBERO — TESTING / CI / REPRODUCIBILITY v1.0, CI platform and local parity sections
