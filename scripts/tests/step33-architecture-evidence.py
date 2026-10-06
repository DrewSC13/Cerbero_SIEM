#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
threat = ROOT / "docs/threat-model/README.md"
required_mappings = [
    ROOT / "compliance/nist-csf/initial-mapping-v1.json",
    ROOT / "compliance/nist-800-53/initial-mapping-v1.json",
    ROOT / "compliance/nist-800-92/initial-mapping-v1.json",
    ROOT / "compliance/iso-27001/initial-mapping-v1.json",
    ROOT / "compliance/mitre-attack/initial-mapping-v1.json",
]

text = threat.read_text(encoding="utf-8")
for marker in [
    "TB-01", "TB-02", "TB-03", "TB-04", "TB-05", "TB-06", "TB-07", "TB-08",
    "Spoofing", "Tampering", "Repudiation", "Information Disclosure",
    "Denial of Service", "Elevation of Privilege",
    "event forgery", "replay", "parser exploitation", "detection poisoning",
    "time manipulation", "supply-chain",
]:
    assert marker in text, marker

for path in required_mappings:
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["schema_version"] == "cerbero.compliance.mapping.v1"
    assert data["mapping_version"] == "1"
    assert data["mappings"]
    rendered = json.dumps(data).lower()
    assert "compliant" not in rendered
    assert "satisfies_control" not in rendered

attack = json.loads(required_mappings[-1].read_text(encoding="utf-8"))
assert attack["attack_version"]
assert attack["mappings"][0]["cerbero_rule_id"] == "CER-DET-000001"

print("STEP33_ARCHITECTURE_EVIDENCE_PASS")
print("THREAT_MODEL=PASS")
print("INITIAL_MAPPINGS=PASS")
