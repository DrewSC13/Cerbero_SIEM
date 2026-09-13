#!/usr/bin/env bash
set -euo pipefail

build_root="$(mktemp -d "${TMPDIR:-/tmp}/cerbero-go-build.XXXXXX")"
cleanup() {
  rm -rf "$build_root"
}
trap cleanup EXIT

mapfile -t modules < <(find services -type f -name go.mod -print | sort)
for module in "${modules[@]}"; do
  dir="${module%/go.mod}"
  echo "go build: $dir"

  package_list="$(cd "$dir" && go list -f $'{{.ImportPath}}\t{{.Name}}' ./...)"
  while IFS=$'\t' read -r import_path package_name; do
    [[ -n "$import_path" ]] || continue
    if [[ "$package_name" == "main" ]]; then
      output="$build_root/${import_path//\//_}"
      (cd "$dir" && go build -o "$output" "$import_path")
    else
      (cd "$dir" && go build "$import_path")
    fi
  done <<< "$package_list"
done
