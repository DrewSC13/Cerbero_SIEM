#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

if [[ ! -f .env ]]; then
  cp .env.example .env
  chmod 600 .env
  echo "created .env from DEVELOPMENT ONLY template"
else
  added=0
  while IFS= read -r line; do
    [[ "$line" =~ ^[A-Z0-9_]+= ]] || continue
    key="${line%%=*}"
    if ! grep -qE "^${key}=" .env; then
      printf '%s\n' "$line" >> .env
      added=$((added + 1))
    fi
  done < .env.example
  chmod 600 .env
  if (( added > 0 )); then
    echo "existing .env preserved; appended ${added} missing DEVELOPMENT template keys"
  else
    echo ".env already contains all DEVELOPMENT template keys"
  fi
fi

mkdir -p var/raw
chmod 700 var/raw

echo "development runtime directories ready"
