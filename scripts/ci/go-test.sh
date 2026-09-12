#!/usr/bin/env bash
set -euo pipefail
for module in services/*/go.mod; do
  dir="${module%/go.mod}"
  echo "go test: $dir"
  (cd "$dir" && go test ./...)
done
