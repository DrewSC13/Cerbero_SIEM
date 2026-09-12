# Local development

## Required reference toolchains

- Rust 1.98.1 (`rust-toolchain.toml`)
- Go 1.27.1 (`go.work` and service `go.mod` files)
- Python 3.14.7 (`.python-version`)
- uv 0.12.13 (`pyproject.toml` required version)
- GNU Make
- Docker Engine/compatible runtime with Compose v2 for infrastructure integration

Run:

```bash
make doctor
```

`make doctor` is read-only: it reports installed local tooling and never installs or downloads a Rust toolchain. If the Rust version pinned in `rust-toolchain.toml` is not installed, the command reports it as `MISSING`.

## Static/unit loop

```bash
make verify
make rust-check
make go-check
make python-check
make security-check
```

or the combined local gate:

```bash
make ci
```

## Development infrastructure

```bash
make dev-init
make dev-up
make dev-bootstrap
make dev-health
```

Stop without deleting data:

```bash
make dev-down
```

Destructive local reset:

```bash
make dev-reset
```

`dev-reset` deletes only Docker development volumes and generated files under `var/raw/`; it is not a production operation.
