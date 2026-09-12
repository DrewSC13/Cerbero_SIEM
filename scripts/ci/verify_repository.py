#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import re
import sys
import tomllib

ROOT = Path(__file__).resolve().parents[2]

RUST_VERSION = "1.98.1"
GO_VERSION = "1.27.1"
PYTHON_VERSION = "3.14.7"
UV_VERSION = "0.12.13"
BUF_VERSION = "1.72.0"

REQUIRED_CONTRACT_ROOTS = [
    "schemas/protobuf/README.md",
    "schemas/protobuf/buf.yaml",
    "schemas/protobuf/buf.gen.yaml",
    "schemas/protobuf/cerbero/contracts/v1/common.proto",
    "schemas/protobuf/cerbero/contracts/v1/envelope.proto",
    "schemas/protobuf/cerbero/contracts/v1/raw_event.proto",
    "schemas/protobuf/cerbero/contracts/v1/normalized_event.proto",
    "schemas/protobuf/cerbero/contracts/v1/transformation.proto",
    "schemas/protobuf/cerbero/contracts/v1/error.proto",
    "schemas/jsonschema/README.md",
    "schemas/ocsf/README.md",
]

REQUIRED = [
    ".editorconfig",
    ".gitattributes",
    ".gitignore",
    ".env.example",
    "ARCHITECTURE.md",
    "CHANGELOG.md",
    "CODEOWNERS",
    "CODE_OF_CONDUCT.md",
    "CONTRIBUTING.md",
    "LICENSE",
    "README.md",
    "SECURITY.md",
    "Cargo.toml",
    "Cargo.lock",
    "go.work",
    "rust-toolchain.toml",
    ".python-version",
    "python/cerbero-tooling/pyproject.toml",
    "python/cerbero-tooling/uv.lock",
    "deploy/compose/compose.yaml",
    "deploy/compose/images.lock",
    "migrations/postgres/000001_bootstrap.sql",
    "migrations/clickhouse/000001_bootstrap.sql",
    "docs/adr/ADR-0001-project-license.md",
    "docs/adr/ADR-0002-local-task-runner.md",
    "docs/adr/ADR-0003-development-raw-store.md",
    "docs/adr/ADR-0004-bootstrap-toolchains-and-images.md",
    "docs/adr/ADR-0005-contract-v1-enum-closure-and-code-generation.md",
    *REQUIRED_CONTRACT_ROOTS,
]


def fail(message: str) -> None:
    print(f"repository verification failed: {message}", file=sys.stderr)
    raise SystemExit(1)


def load_toml(relative_path: str) -> dict[str, object]:
    path = ROOT / relative_path
    with path.open("rb") as handle:
        return tomllib.load(handle)


def verify_contracts() -> None:
    for rel in REQUIRED_CONTRACT_ROOTS:
        if not (ROOT / rel).is_file():
            fail(f"missing contract root file: {rel}")


def verify_toolchain_pins() -> None:
    cargo = load_toml("Cargo.toml")
    workspace_package = cargo.get("workspace", {}).get("package", {})
    if workspace_package.get("rust-version") != RUST_VERSION:
        fail("Cargo.toml Rust version does not match bootstrap pin")

    rust_toolchain = load_toml("rust-toolchain.toml")
    if rust_toolchain.get("toolchain", {}).get("channel") != RUST_VERSION:
        fail("rust-toolchain.toml does not match bootstrap Rust pin")

    go_work = (ROOT / "go.work").read_text(encoding="utf-8")
    if not re.search(rf"(?m)^go\s+{re.escape(GO_VERSION)}\s*$", go_work):
        fail("go.work does not match bootstrap Go pin")
    for go_mod in sorted((ROOT / "services").rglob("go.mod")):
        text = go_mod.read_text(encoding="utf-8")
        if not re.search(rf"(?m)^go\s+{re.escape(GO_VERSION)}\s*$", text):
            fail(f"{go_mod.relative_to(ROOT)} does not match bootstrap Go pin")

    python_version = (ROOT / ".python-version").read_text(encoding="utf-8").strip()
    if python_version != PYTHON_VERSION:
        fail(".python-version does not match bootstrap Python pin")

    pyproject = load_toml("python/cerbero-tooling/pyproject.toml")
    if pyproject.get("project", {}).get("requires-python") != ">=3.14,<3.15":
        fail("pyproject.toml Python range is not the expected 3.14 series")
    if pyproject.get("tool", {}).get("uv", {}).get("required-version") != f"=={UV_VERSION}":
        fail("pyproject.toml uv version does not match bootstrap pin")

    uv_lock = load_toml("python/cerbero-tooling/uv.lock")
    if uv_lock.get("version") != 1 or uv_lock.get("revision") != 3:
        fail("uv.lock format metadata is unexpected")
    if uv_lock.get("requires-python") != ">=3.14,<3.15":
        fail("uv.lock Python range does not match pyproject.toml")

    workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    if f"github.com/bufbuild/buf/cmd/buf@v{BUF_VERSION}" not in workflow:
        fail("GitHub Actions does not install the pinned Buf version")


def verify_image_locks() -> None:
    lock_lines = [
        line.strip()
        for line in (ROOT / "deploy/compose/images.lock").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    compose = (ROOT / "deploy/compose/compose.yaml").read_text(encoding="utf-8")
    if len(lock_lines) != len(set(lock_lines)):
        fail("deploy/compose/images.lock contains duplicate image entries")
    for image in lock_lines:
        if "@sha256:" not in image:
            fail(f"container image is not digest-pinned: {image}")
        if image not in compose:
            fail(f"locked image is not referenced by compose.yaml: {image}")


def verify_debt_markers() -> None:
    forbidden: list[str] = []
    debt_markers = ("TO" + "DO", "FIX" + "ME")
    for path in ROOT.rglob("*"):
        if ".git" in path.parts or not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for marker in debt_markers:
            if re.search(rf"\b{marker}\b", text):
                forbidden.append(f"{path.relative_to(ROOT)} contains {marker}")
    if forbidden:
        fail("; ".join(forbidden))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--contracts-only", action="store_true")
    args = parser.parse_args()
    verify_contracts()
    if args.contracts_only:
        print("contract roots: PASS")
        return 0

    for rel in REQUIRED:
        if not (ROOT / rel).is_file():
            fail(f"missing required file: {rel}")

    for script in (ROOT / "scripts").rglob("*.sh"):
        if not script.stat().st_mode & 0o111:
            fail(f"shell script is not executable: {script.relative_to(ROOT)}")

    if list(ROOT.rglob("*.pdf")):
        fail("PDF files are not permitted in the repository working tree")

    verify_toolchain_pins()
    verify_image_locks()
    verify_debt_markers()

    print("repository verification: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
