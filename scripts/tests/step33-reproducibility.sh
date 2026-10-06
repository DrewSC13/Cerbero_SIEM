#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

tmp="$(mktemp -d)"
cleanup() {
  rm -rf "$tmp"
}
trap cleanup EXIT

first="$tmp/e2e-first.json"
second="$tmp/e2e-second.json"

STEP33_E2E_EVIDENCE_OUT="$first" ./scripts/tests/step33-full-e2e.sh
STEP33_E2E_EVIDENCE_OUT="$second" ./scripts/tests/step33-full-e2e.sh

python3 - "$first" "$second" >"$tmp/semantic-compare.json" <<'PY'
import json
import sys

first = json.load(open(sys.argv[1], encoding="utf-8"))
second = json.load(open(sys.argv[2], encoding="utf-8"))
assert first["semantic"] == second["semantic"], (first["semantic"], second["semantic"])
assert first["semantic_fingerprint_sha256"] == second["semantic_fingerprint_sha256"]
print(json.dumps({
    "reproducibility_class": "environment-reproducible-semantic",
    "bit_for_bit_reproducible_claimed": False,
    "semantic_fingerprint_sha256": first["semantic_fingerprint_sha256"],
}, sort_keys=True))
PY

python3 - "$tmp/semantic-compare.json" "${STEP33_REPRO_EVIDENCE_OUT:-}" <<'PY'
import datetime
import hashlib
import json
import pathlib
import subprocess
import sys

root = pathlib.Path.cwd()
comparison = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

go_sums = sorted(root.glob("services/**/go.sum"))
locks = {
    "Cargo.lock": sha(root / "Cargo.lock"),
    "python/cerbero-tooling/uv.lock": sha(root / "python/cerbero-tooling/uv.lock"),
}
for path in go_sums:
    locks[str(path.relative_to(root))] = sha(path)

images_lock = root / "deploy/compose/images.lock"
images_text = images_lock.read_text(encoding="utf-8")
assert "@sha256:" in images_text

pg_migrations = sorted((root / "migrations/postgres").glob("*.sql"))
ch_migrations = sorted((root / "migrations/clickhouse").glob("*.sql"))
assert pg_migrations and ch_migrations

commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
evidence = {
    "schema_version": "cerbero.step33.reproducibility.v1",
    "reproducibility_class": comparison["reproducibility_class"],
    "bit_for_bit_reproducible_claimed": False,
    "semantic_fingerprint_sha256": comparison["semantic_fingerprint_sha256"],
    "cerbero_version": "0.0.0",
    "git_commit": commit,
    "build_timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "dependency_lock_sha256": locks,
    "container_images_lock_sha256": sha(images_lock),
    "latest_postgres_migration": pg_migrations[-1].name,
    "latest_clickhouse_migration": ch_migrations[-1].name,
    "contract_version": "1",
}
out = pathlib.Path(sys.argv[2]) if sys.argv[2] else None
rendered = json.dumps(evidence, indent=2, sort_keys=True) + "\n"
if out:
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(rendered, encoding="utf-8")
print(rendered, end="")
PY

echo "STEP33_REPRODUCIBILITY_PASS"
