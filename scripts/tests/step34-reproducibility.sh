#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

tmp="$(mktemp -d)"
cleanup() {
  rm -rf "$tmp"
}
trap cleanup EXIT

first="$tmp/compose-first.json"
second="$tmp/compose-second.json"

STEP34_COMPOSE_PROJECT_NAME="cerbero-step34-repro-a" \
STEP34_COMPOSE_E2E_EVIDENCE_OUT="$first" \
  ./scripts/tests/step34-compose-e2e.sh

STEP34_COMPOSE_PROJECT_NAME="cerbero-step34-repro-b" \
STEP34_COMPOSE_E2E_EVIDENCE_OUT="$second" \
  ./scripts/tests/step34-compose-e2e.sh

python3 - "$first" "$second" "$tmp/semantic.json" <<'PY'
import json
import pathlib
import sys

first = json.load(open(sys.argv[1], encoding="utf-8"))
second = json.load(open(sys.argv[2], encoding="utf-8"))
assert first["semantic"] == second["semantic"], (first["semantic"], second["semantic"])
assert first["semantic_fingerprint_sha256"] == second["semantic_fingerprint_sha256"]
first_image_ids = first.get("runtime_objects", {}).get("application_image_ids", {})
second_image_ids = second.get("runtime_objects", {}).get("application_image_ids", {})
pathlib.Path(sys.argv[3]).write_text(
    json.dumps({
        "reproducibility_class": "environment-reproducible-semantic",
        "bit_for_bit_reproducible_claimed": False,
        "semantic_fingerprint_sha256": first["semantic_fingerprint_sha256"],
        "application_image_ids_first": first_image_ids,
        "application_image_ids_second": second_image_ids,
        "application_image_ids_equal": first_image_ids == second_image_ids,
    }, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
PY

docker compose --env-file .env -f deploy/compose/compose.yaml --profile app \
  config >"$tmp/compose-rendered.yaml"

python3 - "$tmp/semantic.json" "$tmp/compose-rendered.yaml" \
  "${STEP34_REPRO_EVIDENCE_OUT:-}" <<'PY'
import datetime
import hashlib
import json
import pathlib
import subprocess
import sys

root = pathlib.Path.cwd()
semantic = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
compose_rendered = pathlib.Path(sys.argv[2])

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

lock = root / "deploy/compose/images.lock"
lock_lines = [
    line.strip()
    for line in lock.read_text(encoding="utf-8").splitlines()
    if line.strip() and not line.lstrip().startswith("#")
]
assert lock_lines
assert all("@sha256:" in line for line in lock_lines), lock_lines

env_values = {}
for line in (root / ".env").read_text(encoding="utf-8").splitlines():
    if not line or line.lstrip().startswith("#") or "=" not in line:
        continue
    key, value = line.split("=", 1)
    env_values[key.strip()] = value.strip()
dockerfiles = [
    root / "deploy/docker/Dockerfile.go-service",
    root / "deploy/docker/Dockerfile.normalizer",
]
dockerfile_text = "\n".join(
    dockerfile.read_text(encoding="utf-8") for dockerfile in dockerfiles
)
for dockerfile in dockerfiles:
    text = dockerfile.read_text(encoding="utf-8")
    assert "@sha256:" in text, dockerfile

for name in [
    "CERBERO_GO_BUILD_IMAGE",
    "CERBERO_RUST_BUILD_IMAGE",
    "CERBERO_DEBIAN_RUNTIME_IMAGE",
]:
    value = env_values.get(name, "")
    assert value and "@sha256:" in value, (name, value)
    assert value in dockerfile_text, (name, value)

locks = {
    "Cargo.lock": sha(root / "Cargo.lock"),
    "python/cerbero-tooling/uv.lock": sha(root / "python/cerbero-tooling/uv.lock"),
}
for path in sorted(root.glob("services/**/go.sum")):
    locks[str(path.relative_to(root))] = sha(path)

pg_migrations = sorted((root / "migrations/postgres").glob("*.sql"))
ch_migrations = sorted((root / "migrations/clickhouse").glob("*.sql"))

evidence = {
    "schema_version": "cerbero.step34.reproducibility.v1",
    "reproducibility_class": semantic["reproducibility_class"],
    "bit_for_bit_reproducible_claimed": False,
    "semantic_fingerprint_sha256": semantic["semantic_fingerprint_sha256"],
    "application_image_ids_first": semantic["application_image_ids_first"],
    "application_image_ids_second": semantic["application_image_ids_second"],
    "application_image_ids_equal": semantic["application_image_ids_equal"],
    "cerbero_version": "0.0.0",
    "git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
    "build_timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "dependency_lock_sha256": locks,
    "container_images_lock_sha256": sha(lock),
    "compose_source_sha256": sha(root / "deploy/compose/compose.yaml"),
    "compose_rendered_sha256": sha(compose_rendered),
    "dockerfile_sha256": {
        str(path.relative_to(root)): sha(path)
        for path in dockerfiles
    },
    "latest_postgres_migration": pg_migrations[-1].name,
    "latest_clickhouse_migration": ch_migrations[-1].name,
    "contract_version": "1",
}
rendered = json.dumps(evidence, indent=2, sort_keys=True) + "\n"
if sys.argv[3]:
    out = pathlib.Path(sys.argv[3])
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(rendered, encoding="utf-8")
print(rendered, end="")
PY

echo "STEP34_REPRODUCIBILITY_PASS"
