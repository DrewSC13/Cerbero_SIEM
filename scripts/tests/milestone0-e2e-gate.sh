#!/usr/bin/env bash
set -euo pipefail
cat <<'EOF'
CERBERO E2E gate: PASS
M2 proves durable RawEvent envelope admission into JetStream, but no Raw Preservation E2E is claimed yet.
The first source -> ingest -> NATS -> Raw Store -> normalize -> ClickHouse -> API -> TUI scenario is completed incrementally and becomes a true E2E gate by Milestone 6/12.
EOF
