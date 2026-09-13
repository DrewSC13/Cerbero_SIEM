# Explicit deferred work

This file lists known work that is intentionally outside repository bootstrap. These items are visible debt/scope boundaries, not hidden placeholders.

| Item | Why deferred | Required milestone / gate |
| --- | --- | --- |
| Pin GitHub Actions by immutable commit SHA | Action SHAs are hosting/supply-chain hardening beyond the local bootstrap; major tags are used initially | Before first public release; security hardening |
| Implement production syslog transport and journald host collector | M2 defines the common core, JSON HTTP runtime, syslog adapter skeleton, and journald collector contract; exact syslog TCP framing, UDP support policy, and source-specific host integration remain intentionally open | Before enabling those source types in an MVP deployment |
| Select ingest metrics exporter/backend | M2 records backend-neutral ingest metric semantics in-process; OPERATIONS v1 explicitly leaves the metrics backend and tool-specific naming convention open | Operations/observability implementation before production |
| Define/publish the source-gap bus payload contract | M2 detects native-sequence gaps and provides a reproducible fixture without inventing a v1 payload schema; the `cerbero.v1.system.source.gap_detected` subject remains reserved | Contract governance before emitting gap events on the bus |
| Implement raw-preserver and segment manifests | Requires real contracts and event flow | Milestone 3 |
| Select/pin OCSF version | Baseline makes OCSF canonical but implementation version belongs with mappings | Milestone 4 |
| Add application service containers to Compose | Fake containers are intentionally avoided until each service exists | Owning implementation milestone |
| Define production secret backend | Baseline leaves backend concrete choice open | Security/deployment milestone |
| Define production Raw Store provider | STORAGE v1 leaves object-store implementation open | Deployment evidence/requirements |
| Define retention durations | Baseline explicitly leaves exact retention open | Capacity/operations evidence |
| Generate SBOM and sign releases | Required for distributable releases, but there is no release artifact in Milestone 0 | Release pipeline before first release |
