#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

if [[ ! -f .env ]]; then
  cp .env.example .env
  chmod 600 .env
  echo "created .env from DEVELOPMENT ONLY template"
else
  echo ".env already exists; left unchanged"
fi

mkdir -p var/raw
chmod 700 var/raw

echo "development runtime directories ready"
