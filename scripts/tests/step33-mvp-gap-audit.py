#!/usr/bin/env python3
import json
from pathlib import Path

matrix_path = Path("tests/e2e/mvp-gap-matrix.json")
data = json.loads(matrix_path.read_text(encoding="utf-8"))
criteria = data["criteria"]

assert len(criteria) == 15, f"expected 15 criteria, got {len(criteria)}"
assert [row["criterion_id"] for row in criteria] == list(range(1, 16))

allowed = {"PASS", "BLOCKED", "DEFERRED"}
for row in criteria:
    assert row["status"] in allowed
    assert row["requirement"]
    if row["status"] == "PASS":
        assert row.get("evidence")
        assert row.get("automated_gate")

blocked = [row for row in criteria if row["status"] == "BLOCKED"]
print("STEP33_GAP_AUDIT_PASS")
print(f"MVP_REQUIRED_BLOCKERS={len(blocked)}")
print("MVP_COMPLETE=" + ("YES" if not blocked else "NO"))
for row in blocked:
    reason = row.get("blocker_reason", "")
    print(f"BLOCKED[{row['criterion_id']}]: {row['requirement']} :: {reason}")
