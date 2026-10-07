# Step34 reproducibility evidence

`make reproducibility` runs the Step34 composed source-to-API E2E twice from clean Compose project state and requires identical **semantic evidence**.

The gate records:

- dataset and raw SHA-256 evidence;
- dependency lock hashes;
- the digest-pinned external/container base image lock;
- source and rendered Compose hashes;
- application Dockerfile hashes;
- application image IDs observed in each run;
- PostgreSQL and ClickHouse migration versions;
- contract version and Git commit.

CERBERO classifies this gate as **environment-reproducible / semantic**. It does not claim bit-for-bit reproducible application images or artifacts. Runtime UUIDv7 values, timestamps and other nondeterministic runtime identifiers are deliberately excluded from semantic equivalence. Application image IDs are evidence and may be compared, but equality is not promoted to a bit-for-bit guarantee.

The earlier Step33 analytical E2E remains available through `make e2e`; Step34 does not weaken or replace its Signal/TUI/provenance coverage.
