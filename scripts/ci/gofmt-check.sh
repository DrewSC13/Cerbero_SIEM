#!/usr/bin/env bash
set -euo pipefail
mapfile -t files < <(find services -type f -name '*.go' -print | sort)
if ((${#files[@]} == 0)); then
  exit 0
fi
unformatted="$(gofmt -l "${files[@]}")"
if [[ -n "$unformatted" ]]; then
  echo "gofmt required:" >&2
  echo "$unformatted" >&2
  exit 1
fi
