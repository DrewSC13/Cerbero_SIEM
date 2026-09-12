# Component ownership

## Rust

Owns components that process untrusted input, require memory safety/performance, or implement the terminal client:

- `cerbero-common`
- `cerbero-tui`
- `cerbero-normalizer`
- `cerbero-integrity`
- `cerbero-detection-core`
- `cerbero-agent`
- critical parsers

Rust code forbids `unsafe_code` by default. Any future critical exception requires explicit justification, review, tests/fuzzing, and an ADR when it changes the critical attack surface.

## Go

Owns network/control coordination services:

- `cerbero-ingest`
- `cerbero-api`
- `cerbero-coordinator`
- `cerbero-scheduler`
- `cerbero-worker`

Milestone 0 uses one Go module per service and a root `go.work` so services remain independently buildable without inventing a public module host before the remote exists.

## Python

Owns tooling, datasets, detection engineering, STIX/TAXII, validation, experimentation, and analytics. Python does not enter the mass-ingest critical path without a new documented architectural reason.

## C++

No C++ component exists in Milestone 0. Introduction requires measured/native/interoperability justification under the baseline.
