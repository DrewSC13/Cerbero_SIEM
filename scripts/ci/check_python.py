#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
TARGETS = [ROOT / "python" / "cerbero-tooling", ROOT / "scripts"]


def python_files() -> list[Path]:
    files: list[Path] = []
    for base in TARGETS:
        files.extend(path for path in base.rglob("*.py") if ".venv" not in path.parts)
    return sorted(files)


def check_format(files: list[Path]) -> list[str]:
    errors: list[str] = []
    for path in files:
        text = path.read_text(encoding="utf-8")
        if not text.endswith("\n"):
            errors.append(f"{path.relative_to(ROOT)}: missing final newline")
        for number, line in enumerate(text.splitlines(), start=1):
            if line.endswith((" ", "\t")):
                errors.append(f"{path.relative_to(ROOT)}:{number}: trailing whitespace")
            if "\t" in line:
                errors.append(f"{path.relative_to(ROOT)}:{number}: tab character")
    return errors


def check_lint(files: list[Path]) -> list[str]:
    errors: list[str] = []
    for path in files:
        try:
            ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except SyntaxError as exc:
            errors.append(f"{path.relative_to(ROOT)}:{exc.lineno}: {exc.msg}")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--format-only", action="store_true")
    parser.add_argument("--lint-only", action="store_true")
    args = parser.parse_args()
    files = python_files()
    errors: list[str] = []
    if not args.lint_only:
        errors.extend(check_format(files))
    if not args.format_only:
        errors.extend(check_lint(files))
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"python checks: PASS ({len(files)} files)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
