#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

if git ls-files | grep -Eq '(^|/)(\.env|id_rsa|id_ed25519|[^/]+\.(pem|key|p12|pfx))$'; then
  echo "forbidden secret-bearing filename is tracked" >&2
  git ls-files | grep -E '(^|/)(\.env|id_rsa|id_ed25519|[^/]+\.(pem|key|p12|pfx))$' >&2
  exit 1
fi

patterns=(
  '-----BEGIN [A-Z ]*PRIVATE KEY-----'
  'AKIA[0-9A-Z]{16}'
  'ghp_[A-Za-z0-9]{30,}'
  'github_pat_[A-Za-z0-9_]{30,}'
  'xox[baprs]-[A-Za-z0-9-]{20,}'
)

for pattern in "${patterns[@]}"; do
  if git grep -I -n -E "$pattern" >/tmp/cerbero-secret-match 2>/dev/null; then
    echo "possible committed secret detected by pattern: $pattern" >&2
    cat /tmp/cerbero-secret-match >&2
    rm -f /tmp/cerbero-secret-match
    exit 1
  fi
done
rm -f /tmp/cerbero-secret-match

echo "repository secret scan: PASS"
