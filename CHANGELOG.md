# Changelog

All notable project changes are recorded here. CERBERO uses immutable release tags once published.

## Unreleased

### Added

- Milestone 0 repository bootstrap.
- Rust, Go, and Python workspace skeletons with executable smoke tests.
- Versioned PostgreSQL and ClickHouse migration roots.
- Pinned Docker Compose development infrastructure for PostgreSQL, ClickHouse, and NATS JetStream.
- Filesystem-backed development Raw Store boundary.
- Local/CI parity through GNU Make and GitHub Actions.
- Indexed the external architectural baseline without committing source PDF documents.
- Architecture, development, operations, security, testing, and ADR documentation roots.

### Fixed

- Prevent the Go build gate from leaving service executables in the repository working tree.
- Make the remote-readiness gate reject untracked files as well as tracked or staged changes.
