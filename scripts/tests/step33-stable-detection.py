#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RULE = ROOT / "rules/sigma/linux_sshd_failed_login_burst.yml"
SUITE = ROOT / "tests/detections/CER-DET-000001"

required = [
    RULE,
    SUITE / "positive/ten_failed.jsonl",
    SUITE / "negative/nine_failed.jsonl",
    SUITE / "edge_cases/duplicates.jsonl",
    SUITE / "edge_cases/out_of_order.jsonl",
    SUITE / "edge_cases/late_events.jsonl",
    SUITE / "edge_cases/missing_fields.jsonl",
    SUITE / "edge_cases/strange_encoding.jsonl",
    SUITE / "edge_cases/invalid_timestamp.jsonl",
    SUITE / "expected.json",
    SUITE / "README.md",
]

missing = [path.relative_to(ROOT) for path in required if not path.is_file()]
if missing:
    raise SystemExit("missing STABLE detection artifacts: " + ", ".join(map(str, missing)))

expected = json.loads((SUITE / "expected.json").read_text(encoding="utf-8"))
assert expected["rule_id"] == "CER-DET-000001"
assert expected["rule_version"] == "1"
assert expected["status"] == "STABLE"
assert expected["positive"] == {"signals": 1, "matched_events": 10}
assert expected["negative"] == {"signals": 0}

expected_edges = {
    "duplicates",
    "out_of_order",
    "late_events",
    "missing_fields",
    "strange_encoding",
    "invalid_timestamp",
}
assert set(expected["edge_cases"]) == expected_edges

def load_jsonl(path: Path) -> list[dict[str, object]]:
    rows = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise AssertionError(f"{path}:{line_number}: invalid JSON: {exc}") from exc
        assert isinstance(row, dict)
        rows.append(row)
    return rows

positive = load_jsonl(SUITE / "positive/ten_failed.jsonl")
negative = load_jsonl(SUITE / "negative/nine_failed.jsonl")
duplicates = load_jsonl(SUITE / "edge_cases/duplicates.jsonl")
out_of_order = load_jsonl(SUITE / "edge_cases/out_of_order.jsonl")
late_events = load_jsonl(SUITE / "edge_cases/late_events.jsonl")
missing_fields = load_jsonl(SUITE / "edge_cases/missing_fields.jsonl")
unicode_rows = load_jsonl(SUITE / "edge_cases/strange_encoding.jsonl")
invalid_timestamp = load_jsonl(SUITE / "edge_cases/invalid_timestamp.jsonl")

assert len(positive) == 10
assert len(negative) == 9
assert len({row["event_id"] for row in duplicates}) == 10
assert len(duplicates) == 11
assert [row["event_time_ms"] for row in out_of_order] != sorted(
    row["event_time_ms"] for row in out_of_order
)
assert late_events[-1]["event_time_ms"] < late_events[0]["event_time_ms"]
assert any("source_ip" not in row for row in missing_fields)
assert any("\ufffd" in str(row.get("user", "")) for row in unicode_rows)
assert any(not isinstance(row["event_time_ms"], int) for row in invalid_timestamp)

rule_text = RULE.read_text(encoding="utf-8")
for marker in [
    "status: stable",
    "type: event_count",
    "timespan: 5m",
    "gte: 10",
    "cerbero_rule_id: CER-DET-000001",
    "cerbero_rule_version: '1'",
    "cerbero_time_basis: EVENT_TIME",
    "cerbero_late_event_policy: ACCEPT",
]:
    assert marker in rule_text, f"missing governed Sigma marker: {marker}"

print("STEP33_STABLE_DETECTION_ASSET_PASS")
print("RULE_ID=CER-DET-000001")
print("RULE_VERSION=1")
print("RULE_STATUS=STABLE")
print("POSITIVE_FIXTURES=1")
print("NEGATIVE_FIXTURES=1")
print("EDGE_CASE_FIXTURES=6")
