#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[2]
FINDING_PROTO = ROOT / "schemas/protobuf/cerbero/contracts/v1/finding.proto"


@dataclass(frozen=True)
class Field:
    label: str
    type_name: str
    name: str
    number: int


EXPECTED_MESSAGES: dict[str, tuple[Field, ...]] = {
    "FindingInput": (
        Field("", "string", "finding_id", 1),
        Field("", "string", "input_type", 2),
        Field("", "string", "input_id", 3),
        Field("", "string", "relation", 4),
    ),
    "FindingCorrelationProvenance": (
        Field("", "string", "correlation_rule_id", 1),
        Field("", "string", "correlation_rule_version", 2),
        Field("", "google.protobuf.Timestamp", "window_start", 3),
        Field("", "google.protobuf.Timestamp", "window_end", 4),
        Field("repeated", "string", "input_ids", 5),
        Field("", "string", "output_finding_id", 6),
        Field("", "DetectionExecutionBackend", "execution_backend", 7),
        Field("", "ExecutionMode", "execution_mode", 8),
        Field("", "string", "configuration_hash", 9),
    ),
    "Finding": (
        Field("", "string", "finding_id", 1),
        Field("", "string", "tenant_id", 2),
        Field("", "FindingType", "finding_type", 3),
        Field("", "FindingStatus", "status", 4),
        Field("", "string", "severity", 5),
        Field("optional", "string", "confidence", 6),
        Field("", "string", "title", 7),
        Field("", "string", "description", 8),
        Field("", "google.protobuf.Timestamp", "first_seen", 9),
        Field("", "google.protobuf.Timestamp", "last_seen", 10),
        Field("", "google.protobuf.Timestamp", "created_at", 11),
        Field("", "google.protobuf.Timestamp", "updated_at", 12),
        Field("optional", "string", "primary_rule_id", 13),
        Field("optional", "string", "primary_rule_version", 14),
        Field("optional", "string", "correlation_rule_id", 15),
        Field("optional", "string", "correlation_rule_version", 16),
        Field("", "FindingDisposition", "disposition", 17),
        Field("", "google.protobuf.Struct", "metadata", 18),
        Field("", "ExecutionMode", "execution_mode", 19),
        Field("repeated", "FindingInput", "inputs", 20),
        Field("", "FindingCorrelationProvenance", "correlation_provenance", 21),
    ),
}

EXPECTED_ENUMS: dict[str, tuple[tuple[str, int], ...]] = {
    "FindingType": (
        ("FINDING_TYPE_UNSPECIFIED", 0),
        ("FINDING_TYPE_DETECTION", 1),
        ("FINDING_TYPE_THRESHOLD", 2),
        ("FINDING_TYPE_CORRELATION", 3),
        ("FINDING_TYPE_ANALYTICAL", 4),
    ),
    "FindingStatus": (
        ("FINDING_STATUS_UNSPECIFIED", 0),
        ("FINDING_STATUS_OPEN", 1),
        ("FINDING_STATUS_ACKNOWLEDGED", 2),
        ("FINDING_STATUS_SUPPRESSED", 3),
        ("FINDING_STATUS_RESOLVED", 4),
        ("FINDING_STATUS_INVALIDATED", 5),
    ),
    "FindingDisposition": (
        ("FINDING_DISPOSITION_UNSPECIFIED", 0),
        ("FINDING_DISPOSITION_UNDETERMINED", 1),
        ("FINDING_DISPOSITION_TRUE_POSITIVE", 2),
        ("FINDING_DISPOSITION_BENIGN_TRUE_POSITIVE", 3),
        ("FINDING_DISPOSITION_FALSE_POSITIVE", 4),
        ("FINDING_DISPOSITION_DUPLICATE", 5),
        ("FINDING_DISPOSITION_TEST_ACTIVITY", 6),
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
    print(f"finding contract verification failed: {message}", file=sys.stderr)
    raise SystemExit(1)


def main() -> int:
    source = FINDING_PROTO.read_text(encoding="utf-8")
    if 'syntax = "proto3";' not in source:
        fail("finding.proto is not proto3")
    if "package cerbero.contracts.v1;" not in source:
        fail("finding.proto has the wrong package")
    for required_import in (
        'import "cerbero/contracts/v1/common.proto";',
        'import "cerbero/contracts/v1/signal.proto";',
        'import "google/protobuf/struct.proto";',
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
        fail(f"message schema differs from governed Finding v1 contract: {parsed_messages!r}")

    parsed_enums: dict[str, tuple[tuple[str, int], ...]] = {}
    for enum_name, body in ENUM_RE.findall(source):
        parsed_enums[enum_name] = tuple(
            (name, int(number)) for name, number in ENUM_VALUE_RE.findall(body)
        )
    if parsed_enums != EXPECTED_ENUMS:
        fail(f"enum schema differs from governed Finding v1 contract: {parsed_enums!r}")

    if "google.protobuf.Any" in source:
        fail("Finding must not hide analytical provenance in google.protobuf.Any")
    correlation = re.search(
        r"message\s+FindingCorrelationProvenance\s*\{(.*?)\n\}", source, re.DOTALL
    )
    if correlation is None:
        fail("FindingCorrelationProvenance is required")
    if "google.protobuf.Struct" in correlation.group(1):
        fail("correlation provenance must remain structurally typed")

    print("canonical Finding v1 semantics: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
