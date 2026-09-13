#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

readonly expected_buf_version="1.72.0"

if ! command -v buf >/dev/null 2>&1; then
  echo "buf ${expected_buf_version} is required; run: GOBIN=\"$HOME/.local/bin\" go install github.com/bufbuild/buf/cmd/buf@v${expected_buf_version}" >&2
  exit 1
fi

actual_buf_version="$(buf --version)"
if [[ "$actual_buf_version" != "$expected_buf_version" ]]; then
  echo "buf version mismatch: expected ${expected_buf_version}, got ${actual_buf_version}" >&2
  exit 1
fi

python3 scripts/contracts/check.py
python3 scripts/contracts/check-raw-segment-manifest.py
(
  cd schemas/protobuf
  buf lint
  buf build >/dev/null
)

echo "contract source gate: PASS"
