#!/usr/bin/env bash
set -euo pipefail
for module in services/*/go.mod; do
  dir="${module%/go.mod}"
  echo "go vet: $dir"
  (cd "$dir" && go vet ./...)
done
