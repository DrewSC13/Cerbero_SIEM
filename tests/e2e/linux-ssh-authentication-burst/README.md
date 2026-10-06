# Linux SSH authentication burst — Step33 E2E

This project-authored deterministic fixture is the first full CERBERO MVP E2E
scenario. Ten exact raw SSH authentication-failure messages for the same
username and source IP occur inside five minutes.

The executable path is:

source fixture → syslog adapter / IngestCore → NATS JetStream → Raw Store →
linux/sshd parser → OCSF mapping → ClickHouse → CER-DET-000001 threshold →
Signal → API search/detail → TUI snapshot.

The syslog source uses the existing transport-neutral adapter and therefore does
not claim that the still-deferred production TCP/UDP syslog listener exists.

`dataset.json` carries dataset identity, version, provenance, content SHA-256,
per-event raw SHA-256 values, schema version, and stable semantic expectations.
Runtime UUIDs are intentionally excluded from expected output.
