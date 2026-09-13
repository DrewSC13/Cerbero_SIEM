#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

readonly expected_buf_version="1.72.0"
if ! command -v buf >/dev/null 2>&1; then
  echo "buf ${expected_buf_version} is required" >&2
  exit 1
fi
if [[ "$(buf --version)" != "$expected_buf_version" ]]; then
  echo "buf version mismatch: expected ${expected_buf_version}, got $(buf --version)" >&2
  exit 1
fi

rm -rf services/internal/contracts/v1 crates/cerbero-common/src/generated
mkdir -p services/internal/contracts crates/cerbero-common/src/generated
(
  cd schemas/protobuf
  buf generate --template buf.gen.yaml
)

echo "generated Rust and Go contract bindings from canonical protobuf source"
