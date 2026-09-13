# Local development

## Required reference toolchains

- Rust 1.98.1 (`rust-toolchain.toml`)
- Go 1.27.1 (`go.work` and service `go.mod` files)
- Python 3.14.7 (`.python-version`)
- uv 0.12.13 (`pyproject.toml` required version)
- Buf 1.72.0 (Milestone 1 contract generation)
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


## DEVELOPMENT JSON/HTTP ingest runtime

The runtime added in M2 is intentionally development-only until the production PKI source-authentication profile is implemented. `.env.example` enables an explicit insecure DEVELOPMENT profile bound to loopback. These values are not production defaults.

For an existing `.env` created before M2 Step 4B, review the new `CERBERO_INGEST_*`, `CERBERO_SECURITY_PROFILE`, and `CERBERO_NATS_URL` entries in `.env.example` and copy the development values intentionally.

With NATS running and bootstrapped:

```bash
make dev-ingest
```

The default DEVELOPMENT endpoints from `.env.example` are:

```text
POST /ingest/v1/events
GET  /livez
GET  /readyz
```

The ingest path is a DEVELOPMENT runtime path, not a frozen production API endpoint. Plain HTTP and static source identity are rejected outside the explicit insecure DEVELOPMENT profile.

## Contract tooling

Milestone 1 requires Buf `1.72.0` for schema validation and generation. Install the pinned CLI without changing the system Go installation:

```bash
mkdir -p "$HOME/.local/bin"
GOBIN="$HOME/.local/bin" go install github.com/bufbuild/buf/cmd/buf@v1.72.0
rehash  # zsh; use `hash -r` in bash if needed
buf --version
```

`make doctor` reports the installed Buf version. `make contracts` requires the exact pin. `make contracts-generate` uses the remote plugins pinned in `schemas/protobuf/buf.gen.yaml`.

## Contract runtime dependency metadata

The generated Rust binding is compiled through `cerbero-common` using exact-pinned `prost`, `prost-types`, and `sha2` dependencies. The generated Go binding is its own workspace module at `services/internal/contracts` and pins `google.golang.org/protobuf` to the same version as the code generator.

After changing those dependency pins, regenerate package-manager metadata before using the locked CI gates:

```bash
cargo generate-lockfile
(cd services/internal/contracts && go mod tidy)
go work sync
```

Ordinary development uses the committed `Cargo.lock` and Go checksum files; dependency resolution is not part of `make ci`.
