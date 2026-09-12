#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
BINARY_SUFFIXES = {".pdf", ".parquet", ".png", ".jpg", ".jpeg", ".gif", ".ico"}


def tracked_files() -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "-z"], cwd=ROOT, check=True, capture_output=True
    )
    return [ROOT / item.decode() for item in result.stdout.split(b"\0") if item]


def main() -> int:
    errors: list[str] = []
    for path in tracked_files():
        if path.suffix.lower() in BINARY_SUFFIXES:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        rel = path.relative_to(ROOT)
        if text and not text.endswith("\n"):
            errors.append(f"{rel}: missing final newline")
        for number, line in enumerate(text.splitlines(), start=1):
            if line.endswith((" ", "\t")):
                errors.append(f"{rel}:{number}: trailing whitespace")
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print("tracked text files: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
