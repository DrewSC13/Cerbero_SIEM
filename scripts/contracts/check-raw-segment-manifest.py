#!/usr/bin/env python3
from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[2]
SCHEMA = ROOT / "schemas/jsonschema/raw-segment-manifest-v1.schema.json"
VALID_FIXTURE = ROOT / "tests/contracts/raw-segment-manifest-v1.valid.json"
INVALID_FIXTURE = ROOT / "tests/contracts/raw-segment-manifest-v1.invalid.json"

UUIDV7 = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
HASH = re.compile(r"^sha256:[0-9a-f]{64}$")
FIELDS = {
    "manifest_version",
    "segment_id",
    "tenant_id",
    "event_count",
    "first_event_id",
    "last_event_id",
    "created_at",
    "previous_segment_hash",
    "content_hash",
}


def fail(message: str) -> None:
    print(f"raw segment manifest contract verification failed: {message}", file=sys.stderr)
    raise SystemExit(1)


def load_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        fail(f"cannot parse {path.relative_to(ROOT)}: {exc}")
    if not isinstance(value, dict):
        fail(f"{path.relative_to(ROOT)} must contain a JSON object")
    return value


def verify_schema(schema: dict[str, object]) -> None:
    if schema.get("$schema") != "https://json-schema.org/draft/2020-12/schema":
        fail("schema must use JSON Schema draft 2020-12")
    if schema.get("$id") != "urn:cerbero:schema:raw-segment-manifest:v1":
        fail("schema $id drifted")
    if schema.get("type") != "object" or schema.get("additionalProperties") is not False:
        fail("schema must be a closed object")
    if set(schema.get("required", [])) != FIELDS:
        fail("schema required-field set drifted")
    properties = schema.get("properties")
    if not isinstance(properties, dict) or set(properties) != FIELDS:
        fail("schema property set drifted")
    if properties.get("manifest_version") != {"const": 1}:
        fail("manifest_version must remain const 1")
    if "manifest_hash" in properties:
        fail("manifest_hash must remain an adjacent artifact, not a self-referential JSON field")


def validate_manifest(value: dict[str, object]) -> list[str]:
    errors: list[str] = []
    if set(value) != FIELDS:
        errors.append("field set differs from governed schema")
    if value.get("manifest_version") != 1:
        errors.append("manifest_version must equal 1")
    for name in ("segment_id", "first_event_id", "last_event_id"):
        candidate = value.get(name)
        if not isinstance(candidate, str) or UUIDV7.fullmatch(candidate) is None:
            errors.append(f"{name} must be canonical lowercase UUIDv7")
    tenant = value.get("tenant_id")
    if not isinstance(tenant, str) or not tenant:
        errors.append("tenant_id must be non-empty")
    count = value.get("event_count")
    if not isinstance(count, int) or isinstance(count, bool) or count < 1:
        errors.append("event_count must be an integer >= 1")
    created_at = value.get("created_at")
    if not isinstance(created_at, str):
        errors.append("created_at must be a string")
    else:
        try:
            parsed = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                raise ValueError("timezone is required")
        except ValueError:
            errors.append("created_at must be an RFC3339 date-time with timezone")
    previous = value.get("previous_segment_hash")
    if previous is not None and (not isinstance(previous, str) or HASH.fullmatch(previous) is None):
        errors.append("previous_segment_hash must be null or sha256:<lowercase-hex>")
    content = value.get("content_hash")
    if not isinstance(content, str) or HASH.fullmatch(content) is None:
        errors.append("content_hash must be sha256:<lowercase-hex>")
    return errors


def main() -> int:
    schema = load_json(SCHEMA)
    verify_schema(schema)

    valid = load_json(VALID_FIXTURE)
    errors = validate_manifest(valid)
    if errors:
        fail(f"valid fixture rejected: {errors}")

    invalid = load_json(INVALID_FIXTURE)
    if not validate_manifest(invalid):
        fail("invalid fixture unexpectedly satisfied governed manifest semantics")

    print("raw segment manifest contract: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
