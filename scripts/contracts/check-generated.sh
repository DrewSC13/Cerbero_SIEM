#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

rust_root="crates/cerbero-common/src/generated"
go_root="services/internal/contracts/v1"
snapshot="$(mktemp -d)"

restore_generated() {
  rm -rf "$rust_root" "$go_root"
  mkdir -p "$(dirname "$rust_root")" "$(dirname "$go_root")"
  cp -a "$snapshot/rust" "$rust_root"
  cp -a "$snapshot/go" "$go_root"
  rm -rf "$snapshot"
}
trap restore_generated EXIT

cp -a "$rust_root" "$snapshot/rust"
cp -a "$go_root" "$snapshot/go"

./scripts/contracts/generate.sh >/dev/null

drift=0
if ! diff -qr "$snapshot/rust" "$rust_root" >/dev/null; then
  echo "generated Rust contract bindings are stale" >&2
  drift=1
fi
if ! diff -qr "$snapshot/go" "$go_root" >/dev/null; then
  echo "generated Go contract bindings are stale" >&2
  drift=1
fi
if ((drift != 0)); then
  echo "run 'make contracts-generate' and commit the generated changes" >&2
  exit 1
fi

echo "generated contract bindings: PASS"
