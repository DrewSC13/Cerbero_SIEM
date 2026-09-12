#!/usr/bin/env bash
set -euo pipefail
cat <<'EOF'
Milestone 0 E2E gate: PASS
No analytical E2E is claimed in repository bootstrap.
The first source -> ingest -> NATS -> Raw Store -> normalize -> ClickHouse -> API -> TUI scenario is completed incrementally and becomes a true E2E gate by Milestone 6/12.
EOF
