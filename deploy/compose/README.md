# Development Compose stack

The MVP development composition contains the shared infrastructure and the CERBERO application services that have executable runtimes:

- PostgreSQL
- ClickHouse
- NATS JetStream
- filesystem Raw Store volume
- `cerbero-ingest` JSON/HTTP frontend
- `cerbero-syslog` RFC6587 TCP frontend
- `cerbero-raw-preserver`
- `cerbero-normalizer`
- `cerbero-worker`
- `cerbero-api`

Coordinator/scheduler placeholders and a fictitious detection daemon are intentionally not added. Detection remains exercised through the implemented detection runtime and the Step33 E2E gate.

External infrastructure images referenced directly by Compose remain tag+digest pinned in `images.lock`. Application images are built from the checked-out source and dependency locks; their Go/Rust/Debian builder/runtime bases are pinned by digest in the Dockerfiles and supplied through explicit build arguments. Step34 records the resulting application image IDs but claims environment/semantic reproducibility, not bit-for-bit image equality.

The development Raw Store is the named `raw-store` volume shared read/write by the raw-preserver and read-only by the normalizer. This preserves the STORAGE v1 object/file authority boundary without selecting a production object-store provider.

## Security boundary

This is an explicit **DEVELOPMENT ONLY** composition. JSON and syslog containers may bind to an unspecified address inside the Compose network only when their dedicated container-development opt-in is present. Published host ports remain bound to `127.0.0.1`. This does not implement or claim production mTLS/enrollment or a production secrets backend.

## Start infrastructure only

```bash
make dev-init
make dev-up
make dev-bootstrap
make dev-health
```

## MVP composed acceptance

```bash
make compose-e2e
make reproducibility
```

`make compose-e2e` builds the source-pinned application images and proves the composed syslog vertical through Raw Store, OCSF normalization, ClickHouse and API retrieval. `make source-ingest` separately proves JSON, real TCP syslog and journald durable source admission.
