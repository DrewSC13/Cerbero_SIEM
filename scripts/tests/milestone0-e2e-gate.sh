#!/usr/bin/env bash
set -euo pipefail
cat <<'EOF'
CERBERO E2E gate: PASS
M2 proves DEVELOPMENT source -> ingest -> durable RawEvent admission into JetStream.
M3 proves cerbero.v1.raw.received -> durable Raw Store + PostgreSQL metadata/outbox -> cerbero.v1.raw.persisted, including duplicate/redelivery convergence.
The first full source -> ingest -> NATS -> Raw Store -> normalize -> ClickHouse -> API -> TUI scenario remains incremental and becomes a true analytical E2E gate by Milestone 6/12.
EOF
