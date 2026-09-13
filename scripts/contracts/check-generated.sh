#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

./scripts/contracts/generate.sh >/dev/null

mapfile -t drift < <(git status --porcelain --untracked-files=all -- \
  crates/cerbero-common/src/generated \
  services/internal/contracts/v1)
if ((${#drift[@]} != 0)); then
  echo "generated contract bindings are stale:" >&2
  printf '%s\n' "${drift[@]}" >&2
  echo "run 'make contracts-generate' and commit the generated changes" >&2
  exit 1
fi

echo "generated contract bindings: PASS"
