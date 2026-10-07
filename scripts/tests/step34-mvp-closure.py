#!/usr/bin/env python3
import json
from pathlib import Path

matrix = json.loads(Path("tests/e2e/mvp-gap-matrix.json").read_text(encoding="utf-8"))
criteria = matrix["criteria"]

assert len(criteria) == 15, f"expected 15 criteria, got {len(criteria)}"
assert [row["criterion_id"] for row in criteria] == list(range(1, 16))

not_pass = [row for row in criteria if row["status"] != "PASS"]
assert not not_pass, f"MVP criteria not PASS: {not_pass}"

by_id = {row["criterion_id"]: row for row in criteria}
criterion_1 = by_id[1]
criterion_12 = by_id[12]

assert criterion_1["automated_gate"] == "make source-ingest"
assert "services/cerbero-ingest/internal/syslogingest/tcp.go" in criterion_1["evidence"]
assert "services/cerbero-ingest/internal/journald/export.go" in criterion_1["evidence"]
assert "scripts/tests/step34-source-ingest.sh" in criterion_1["evidence"]
assert "blocker_reason" not in criterion_1

assert criterion_12["automated_gate"] == "make reproducibility"
assert "deploy/compose/compose.yaml" in criterion_12["evidence"]
assert "deploy/docker/Dockerfile.go-service" in criterion_12["evidence"]
assert "deploy/docker/Dockerfile.normalizer" in criterion_12["evidence"]
assert "scripts/tests/step34-reproducibility.sh" in criterion_12["evidence"]
assert "blocker_reason" not in criterion_12

print("STEP34_MVP_CLOSURE_MATRIX_PASS")
print("MVP_REQUIRED_BLOCKERS=0")
print("MVP_COMPLETE=YES")
