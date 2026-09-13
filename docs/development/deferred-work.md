# Explicit deferred work

This file lists known work that is intentionally outside repository bootstrap. These items are visible debt/scope boundaries, not hidden placeholders.

| Item | Why deferred | Required milestone / gate |
| --- | --- | --- |
| Pin GitHub Actions by immutable commit SHA | Action SHAs are hosting/supply-chain hardening beyond the local bootstrap; major tags are used initially | Before first public release; security hardening |
| Complete ingest frontends and durable acceptance wiring | Common IngestCore now constructs validated RawEvent/envelopes; JSON HTTP, syslog/journald boundaries, request IDs, rate/timeouts, and durable bus admission remain intentionally separate | Milestone 2 for frontends; Milestone 3 for JetStream/raw preservation |
| Implement raw-preserver and segment manifests | Requires real contracts and event flow | Milestone 3 |
| Select/pin OCSF version | Baseline makes OCSF canonical but implementation version belongs with mappings | Milestone 4 |
| Add application service containers to Compose | Fake containers are intentionally avoided until each service exists | Owning implementation milestone |
| Define production secret backend | Baseline leaves backend concrete choice open | Security/deployment milestone |
| Define production Raw Store provider | STORAGE v1 leaves object-store implementation open | Deployment evidence/requirements |
| Define retention durations | Baseline explicitly leaves exact retention open | Capacity/operations evidence |
| Generate SBOM and sign releases | Required for distributable releases, but there is no release artifact in Milestone 0 | Release pipeline before first release |
