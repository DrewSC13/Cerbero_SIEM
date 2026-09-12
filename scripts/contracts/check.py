#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[2]
PROTO_ROOT = ROOT / "schemas/protobuf/cerbero/contracts/v1"


@dataclass(frozen=True)
class Field:
    type_name: str
    name: str
    number: int
    optional: bool = False


EXPECTED_MESSAGES: dict[str, tuple[Field, ...]] = {
    "Producer": (
        Field("string", "component", 1),
        Field("string", "component_version", 2),
        Field("string", "instance_id", 3),
    ),
    "CerberoEnvelope": (
        Field("string", "contract_version", 1),
        Field("string", "message_id", 2),
        Field("string", "message_type", 3),
        Field("string", "tenant_id", 4),
        Field("Producer", "producer", 5),
        Field("google.protobuf.Timestamp", "emitted_at", 6),
        Field("string", "trace_id", 7),
        Field("string", "causation_id", 8),
        Field("string", "correlation_id", 9),
        Field("string", "payload_schema", 10),
        Field("google.protobuf.Any", "payload", 11),
    ),
    "RawEvent": (
        Field("string", "event_id", 1),
        Field("string", "tenant_id", 2),
        Field("string", "source_id", 3),
        Field("string", "sensor_id", 4),
        Field("google.protobuf.Timestamp", "event_time", 5),
        Field("google.protobuf.Timestamp", "ingest_time", 6),
        Field("string", "content_type", 7),
        Field("string", "encoding", 8),
        Field("bytes", "raw_payload", 9),
        Field("uint64", "raw_size", 10),
        Field("string", "raw_hash_algorithm", 11),
        Field("string", "raw_hash", 12),
        Field("string", "transport", 13),
        Field("string", "remote_identity", 14),
        Field("uint64", "sequence_number", 15, optional=True),
        Field("IntegrityStatus", "integrity_status", 16),
        Field("string", "pipeline_version", 17),
    ),
    "NormalizedEvent": (
        Field("string", "normalized_event_id", 1),
        Field("string", "raw_event_id", 2),
        Field("string", "tenant_id", 3),
        Field("google.protobuf.Timestamp", "normalized_at", 4),
        Field("string", "ocsf_version", 5),
        Field("uint32", "class_uid", 6),
        Field("uint32", "category_uid", 7),
        Field("uint32", "severity", 8, optional=True),
        Field("uint32", "activity_id", 9, optional=True),
        Field("google.protobuf.Struct", "ocsf_event", 10),
        Field("string", "parser_id", 11),
        Field("string", "parser_version", 12),
        Field("NormalizationStatus", "normalization_status", 13),
        Field("string", "normalized_hash_algorithm", 14),
        Field("string", "normalized_hash", 15),
        Field("string", "pipeline_version", 16),
    ),
    "Transformation": (
        Field("string", "transformation_id", 1),
        Field("string", "input_object_id", 2),
        Field("string", "input_object_type", 3),
        Field("string", "output_object_id", 4),
        Field("string", "output_object_type", 5),
        Field("string", "component", 6),
        Field("string", "component_version", 7),
        Field("string", "configuration_hash", 8),
        Field("google.protobuf.Timestamp", "started_at", 9),
        Field("google.protobuf.Timestamp", "completed_at", 10),
        Field("TransformationStatus", "status", 11),
        Field("CerberoError", "error", 12, optional=True),
        Field("ExecutionMode", "execution_mode", 13),
    ),
    "CerberoError": (
        Field("string", "code", 1),
        Field("ErrorCategory", "category", 2),
        Field("string", "message", 3),
        Field("bool", "retryable", 4),
        Field("string", "component", 5),
        Field("string", "request_id", 6),
        Field("map<string, string>", "metadata", 7),
    ),
}

EXPECTED_ENUMS: dict[str, tuple[tuple[str, int], ...]] = {
    "IntegrityStatus": (
        ("INTEGRITY_STATUS_UNSPECIFIED", 0),
        ("INTEGRITY_UNVERIFIED", 1),
        ("INTEGRITY_VALID", 2),
        ("INTEGRITY_INVALID", 3),
    ),
    "NormalizationStatus": (
        ("NORMALIZATION_STATUS_UNSPECIFIED", 0),
        ("NORMALIZATION_STATUS_SUCCESS", 1),
        ("NORMALIZATION_STATUS_PARTIAL", 2),
        ("NORMALIZATION_STATUS_FAILED", 3),
    ),
    "TransformationStatus": (
        ("TRANSFORMATION_STATUS_UNSPECIFIED", 0),
        ("TRANSFORMATION_STATUS_SUCCESS", 1),
        ("TRANSFORMATION_STATUS_FAILED", 2),
    ),
    "ExecutionMode": (
        ("EXECUTION_MODE_UNSPECIFIED", 0),
        ("EXECUTION_MODE_LIVE", 1),
        ("EXECUTION_MODE_REPLAY", 2),
        ("EXECUTION_MODE_TEST", 3),
    ),
    "ErrorCategory": (
        ("ERROR_CATEGORY_UNSPECIFIED", 0),
        ("VALIDATION", 1),
        ("AUTHENTICATION", 2),
        ("AUTHORIZATION", 3),
        ("RATE_LIMIT", 4),
        ("CONFLICT", 5),
        ("PARSING", 6),
        ("NORMALIZATION", 7),
        ("STORAGE", 8),
        ("TRANSPORT", 9),
        ("DEPENDENCY", 10),
        ("INTEGRITY", 11),
        ("INTERNAL", 12),
    ),
}

MESSAGE_RE = re.compile(r"\bmessage\s+(\w+)\s*\{(.*?)\n\}", re.DOTALL)
ENUM_RE = re.compile(r"\benum\s+(\w+)\s*\{(.*?)\n\}", re.DOTALL)
FIELD_RE = re.compile(
    r"^\s*(optional\s+)?(map<\s*string\s*,\s*string\s*>|[.\w]+)\s+(\w+)\s*=\s*(\d+)\s*;",
    re.MULTILINE,
)
ENUM_VALUE_RE = re.compile(r"^\s*(\w+)\s*=\s*(\d+)\s*;", re.MULTILINE)


def fail(message: str) -> None:
    print(f"contract verification failed: {message}", file=sys.stderr)
    raise SystemExit(1)


def normalized_type(type_name: str) -> str:
    if type_name.startswith("."):
        return type_name[1:]
    return re.sub(r"\s+", " ", type_name.replace(" ,", ",").replace(", ", ", "))


def load_sources() -> str:
    files = sorted(PROTO_ROOT.glob("*.proto"))
    expected_files = {
        "common.proto",
        "envelope.proto",
        "error.proto",
        "normalized_event.proto",
        "raw_event.proto",
        "transformation.proto",
    }
    found = {path.name for path in files}
    if found != expected_files:
        fail(f"unexpected v1 proto set: expected {sorted(expected_files)}, found {sorted(found)}")

    chunks: list[str] = []
    for path in files:
        text = path.read_text(encoding="utf-8")
        if 'syntax = "proto3";' not in text:
            fail(f"{path.relative_to(ROOT)} is not proto3")
        if "package cerbero.contracts.v1;" not in text:
            fail(f"{path.relative_to(ROOT)} has the wrong package")
        chunks.append(text)
    return "\n".join(chunks)


def verify_messages(source: str) -> None:
    parsed: dict[str, tuple[Field, ...]] = {}
    for message_name, body in MESSAGE_RE.findall(source):
        fields: list[Field] = []
        for optional, type_name, name, number in FIELD_RE.findall(body):
            fields.append(Field(normalized_type(type_name), name, int(number), bool(optional)))
        parsed[message_name] = tuple(fields)

    for name, expected in EXPECTED_MESSAGES.items():
        actual = parsed.get(name)
        if actual != expected:
            fail(f"message {name} differs from governed field/type/number contract: {actual!r}")


def verify_enums(source: str) -> None:
    parsed: dict[str, tuple[tuple[str, int], ...]] = {}
    for enum_name, body in ENUM_RE.findall(source):
        parsed[enum_name] = tuple((name, int(number)) for name, number in ENUM_VALUE_RE.findall(body))

    for name, expected in EXPECTED_ENUMS.items():
        actual = parsed.get(name)
        if actual != expected:
            fail(f"enum {name} differs from governed value contract: {actual!r}")


def verify_semantics(source: str) -> None:
    if 'ExecutionMode execution_mode = 13;' not in source:
        fail("Transformation must distinguish LIVE/REPLAY/TEST execution")
    if 'optional uint64 sequence_number = 15;' not in source:
        fail("RawEvent sequence_number must remain optional")
    if 'optional uint32 severity = 8;' not in source or 'optional uint32 activity_id = 9;' not in source:
        fail("NormalizedEvent optional scalar presence must be preserved")


def main() -> int:
    source = load_sources()
    verify_messages(source)
    verify_enums(source)
    verify_semantics(source)
    print("canonical contract semantics: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
