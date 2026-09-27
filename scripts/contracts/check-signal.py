#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[2]
SIGNAL_PROTO = ROOT / "schemas/protobuf/cerbero/contracts/v1/signal.proto"


@dataclass(frozen=True)
class Field:
    label: str
    type_name: str
    name: str
    number: int


EXPECTED_MESSAGES: dict[str, tuple[Field, ...]] = {
    "SignalProvenance": (
        Field("", "google.protobuf.Timestamp", "evaluated_at", 1),
        Field("", "DetectionExecutionBackend", "execution_backend", 2),
    ),
    "SignalInput": (
        Field("", "string", "signal_id", 1),
        Field("", "SignalInputType", "input_type", 2),
        Field("", "string", "input_id", 3),
        Field("", "string", "relation", 4),
        Field("", "uint32", "ordinal", 5),
    ),
    "Signal": (
        Field("", "string", "signal_id", 1),
        Field("", "string", "tenant_id", 2),
        Field("", "string", "rule_id", 3),
        Field("", "string", "rule_version", 4),
        Field("", "SignalRuleType", "rule_type", 5),
        Field("", "string", "severity", 6),
        Field("optional", "string", "confidence", 7),
        Field("", "SignalStatus", "status", 8),
        Field("", "google.protobuf.Timestamp", "first_observed_at", 9),
        Field("", "google.protobuf.Timestamp", "last_observed_at", 10),
        Field("", "google.protobuf.Timestamp", "created_at", 11),
        Field("", "ExecutionMode", "execution_mode", 12),
        Field("", "uint64", "source_count", 13),
        Field("", "uint64", "event_count", 14),
        Field("", "string", "summary", 15),
        Field("", "SignalProvenance", "provenance", 16),
        Field("repeated", "SignalInput", "inputs", 17),
    ),
}

EXPECTED_ENUMS: dict[str, tuple[tuple[str, int], ...]] = {
    "SignalRuleType": (
        ("SIGNAL_RULE_TYPE_UNSPECIFIED", 0),
        ("SIGNAL_RULE_TYPE_EVENT", 1),
        ("SIGNAL_RULE_TYPE_THRESHOLD", 2),
        ("SIGNAL_RULE_TYPE_CORRELATION", 3),
    ),
    "SignalStatus": (
        ("SIGNAL_STATUS_UNSPECIFIED", 0),
        ("SIGNAL_STATUS_ACTIVE", 1),
        ("SIGNAL_STATUS_SUPPRESSED", 2),
        ("SIGNAL_STATUS_PROMOTED", 3),
        ("SIGNAL_STATUS_INVALIDATED", 4),
    ),
    "SignalInputType": (
        ("SIGNAL_INPUT_TYPE_UNSPECIFIED", 0),
        ("SIGNAL_INPUT_TYPE_NORMALIZED_EVENT", 1),
        ("SIGNAL_INPUT_TYPE_SIGNAL", 2),
        ("SIGNAL_INPUT_TYPE_ENTITY", 3),
    ),
    "DetectionExecutionBackend": (
        ("DETECTION_EXECUTION_BACKEND_UNSPECIFIED", 0),
        ("DETECTION_EXECUTION_BACKEND_CLICKHOUSE", 1),
        ("DETECTION_EXECUTION_BACKEND_STREAM", 2),
    ),
}

MESSAGE_RE = re.compile(r"\bmessage\s+(\w+)\s*\{(.*?)\n\}", re.DOTALL)
ENUM_RE = re.compile(r"\benum\s+(\w+)\s*\{(.*?)\n\}", re.DOTALL)
FIELD_RE = re.compile(
    r"^\s*(?:(optional|repeated)\s+)?([.\w]+)\s+(\w+)\s*=\s*(\d+)\s*;",
    re.MULTILINE,
)
ENUM_VALUE_RE = re.compile(r"^\s*(\w+)\s*=\s*(\d+)\s*;", re.MULTILINE)


def fail(message: str) -> None:
    print(f"signal contract verification failed: {message}", file=sys.stderr)
    raise SystemExit(1)


def main() -> int:
    source = SIGNAL_PROTO.read_text(encoding="utf-8")
    if 'syntax = "proto3";' not in source:
        fail("signal.proto is not proto3")
    if "package cerbero.contracts.v1;" not in source:
        fail("signal.proto has the wrong package")
    for required_import in (
        'import "cerbero/contracts/v1/common.proto";',
        'import "google/protobuf/timestamp.proto";',
    ):
        if required_import not in source:
            fail(f"missing required import: {required_import}")

    parsed_messages: dict[str, tuple[Field, ...]] = {}
    for message_name, body in MESSAGE_RE.findall(source):
        parsed_messages[message_name] = tuple(
            Field(label, type_name, name, int(number))
            for label, type_name, name, number in FIELD_RE.findall(body)
        )
    if parsed_messages != EXPECTED_MESSAGES:
        fail(f"message schema differs from governed Signal v1 contract: {parsed_messages!r}")

    parsed_enums: dict[str, tuple[tuple[str, int], ...]] = {}
    for enum_name, body in ENUM_RE.findall(source):
        parsed_enums[enum_name] = tuple(
            (name, int(number)) for name, number in ENUM_VALUE_RE.findall(body)
        )
    if parsed_enums != EXPECTED_ENUMS:
        fail(f"enum schema differs from governed Signal v1 contract: {parsed_enums!r}")

    if "google.protobuf.Struct" in source or "google.protobuf.Any" in source:
        fail("Signal provenance must remain structurally typed, not an opaque Struct/Any payload")

    print("canonical Signal v1 semantics: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
