#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

if git ls-files '*.pdf' | grep -q .; then
  echo "repository policy violation: PDF files must not be tracked" >&2
  git ls-files '*.pdf' >&2
  exit 1
fi

required_documents=(
  'CERBERO — ARCHITECTURE v1.0'
  'CERBERO — CONTRACTS v1.0'
  'CERBERO — STORAGE v1.0'
  'CERBERO — ANALYTICAL MODEL v1.0'
  'CERBERO — DETECTION & CORRELATION v1.0'
  'CERBERO — INGEST & EVENT BUS v1.0'
  'CERBERO — PARSING & OCSF v1.0'
  'CERBERO — API + AUTH/RBAC v1.0'
  'CERBERO — SECURITY / PKI / SECRETS v1.0'
  'CERBERO — TUI & QUERY v1.0'
  'CERBERO — TESTING / CI / REPRODUCIBILITY v1.0'
  'CERBERO — OPERATIONS / OBSERVABILITY / RETENTION v1.0'
  'Cerbero_Propuesta_de_Proyecto'
  'Documentación fundacional'
)

index='docs/architecture/source-of-truth.md'
for document in "${required_documents[@]}"; do
  if ! grep -Fq "$document" "$index"; then
    echo "baseline index is missing: $document" >&2
    exit 1
  fi
done

echo "baseline source policy: PASS"
