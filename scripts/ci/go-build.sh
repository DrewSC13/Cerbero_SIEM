#!/usr/bin/env bash
set -euo pipefail

build_root="$(mktemp -d "${TMPDIR:-/tmp}/cerbero-go-build.XXXXXX")"
cleanup() {
  rm -rf "$build_root"
}
trap cleanup EXIT

for module in services/*/go.mod; do
  dir="${module%/go.mod}"
  output_dir="$build_root/$(basename "$dir")"
  mkdir -p "$output_dir"

  echo "go build: $dir"
  (cd "$dir" && go build -o "$output_dir/" ./...)
done
