#!/usr/bin/env bash
set -euo pipefail
if [[ $# -ne 1 || "$1" != @* ]]; then
  echo "usage: $0 @github-user-or-team" >&2
  exit 2
fi
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
owner="$1"
python3 - "$repo_root/CODEOWNERS" "$owner" <<'PYCODEOWNERS'
from pathlib import Path
import sys
path = Path(sys.argv[1])
owner = sys.argv[2]
text = path.read_text(encoding="utf-8")
path.write_text(text.replace("@OWNER", owner), encoding="utf-8")
PYCODEOWNERS
echo "CODEOWNERS configured for $owner"
