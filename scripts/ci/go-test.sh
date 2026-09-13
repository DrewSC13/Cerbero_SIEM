#!/usr/bin/env bash
set -euo pipefail
mapfile -t modules < <(find services -type f -name go.mod -print | sort)
for module in "${modules[@]}"; do
  dir="${module%/go.mod}"
  action="test"
  echo "go ${action}: $dir"
  (cd "$dir" && go "${action}" ./...)
done
