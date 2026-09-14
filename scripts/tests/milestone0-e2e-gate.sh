#!/usr/bin/env bash
set -euo pipefail
cat <<'EOF'
CERBERO E2E gate: PASS
M2 proves DEVELOPMENT source -> ingest -> durable RawEvent admission into JetStream.
M3 proves cerbero.v1.raw.received -> durable Raw Store + PostgreSQL metadata/outbox -> cerbero.v1.raw.persisted, including duplicate/redelivery convergence.
M4 first vertical proves raw.persisted -> linux/sshd@1 -> OCSF 1.9.0 Authentication -> ClickHouse -> normalized.created with duplicate-safe logical normalization.
The source -> ingest -> NATS -> Raw Store -> normalize -> ClickHouse portion is now executable; API/TUI completion remains incremental and becomes the full analytical E2E gate by Milestone 6/12.
EOF
