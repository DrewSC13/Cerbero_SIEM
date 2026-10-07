#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

for command in docker curl python3 sha256sum; do
  command -v "$command" >/dev/null 2>&1 || {
    echo "$command is required for Step34 composed E2E" >&2
    exit 2
  }
done
docker compose version >/dev/null

[[ -f .env ]] || ./scripts/dev/init.sh

set -a
# shellcheck disable=SC1091
source .env
set +a

: "${CERBERO_SYSLOG_HOST_PORT:?CERBERO_SYSLOG_HOST_PORT is required}"
: "${CERBERO_API_HOST_PORT:?CERBERO_API_HOST_PORT is required}"
: "${CERBERO_INGEST_HOST_PORT:?CERBERO_INGEST_HOST_PORT is required}"
: "${CERBERO_SYSLOG_DEV_TENANT_ID:?CERBERO_SYSLOG_DEV_TENANT_ID is required}"

export COMPOSE_PROJECT_NAME="${STEP34_COMPOSE_PROJECT_NAME:-cerbero-step34-compose-e2e}"
export CERBERO_APP_IMAGE_TAG="${CERBERO_APP_IMAGE_TAG:-step34}"

compose=(docker compose --env-file .env -f deploy/compose/compose.yaml --profile app)
tmp="$(mktemp -d)"

cleanup() {
  "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$tmp"
}
trap cleanup EXIT

"${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
"${compose[@]}" build \
  cerbero-ingest \
  cerbero-syslog \
  cerbero-raw-preserver \
  cerbero-normalizer \
  cerbero-worker \
  cerbero-api

docker compose --env-file .env -f deploy/compose/compose.yaml up -d postgres clickhouse nats
./scripts/dev/bootstrap-nats.sh
./scripts/dev/health.sh
./scripts/dev/bootstrap-postgres.sh
./scripts/dev/bootstrap-clickhouse.sh

"${compose[@]}" up -d \
  cerbero-ingest \
  cerbero-syslog \
  cerbero-raw-preserver \
  cerbero-normalizer \
  cerbero-worker \
  cerbero-api

wait_http() {
  local name="$1"
  local url="$2"
  local expected="$3"
  local attempt code
  for attempt in $(seq 1 120); do
    code="$(curl -sS -o /dev/null -w '%{http_code}' "$url" 2>/dev/null || true)"
    if [[ "$code" == "$expected" ]]; then
      return 0
    fi
    sleep 0.25
  done
  echo "$name did not become ready at $url" >&2
  "${compose[@]}" ps >&2 || true
  return 1
}

wait_http \
  "cerbero-ingest" \
  "http://127.0.0.1:${CERBERO_INGEST_HOST_PORT}/readyz" \
  "200"

python3 - "$CERBERO_SYSLOG_HOST_PORT" <<'PY'
import socket
import sys
import time

port = int(sys.argv[1])
for _ in range(120):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.25):
            print("STEP34_SYSLOG_SOCKET_READY")
            raise SystemExit(0)
    except OSError:
        time.sleep(0.25)
raise SystemExit("cerbero-syslog TCP listener did not become ready")
PY

required_services=(
  cerbero-ingest
  cerbero-syslog
  cerbero-raw-preserver
  cerbero-normalizer
  cerbero-worker
  cerbero-api
)
running="$("${compose[@]}" ps --status running --services)"
for service in "${required_services[@]}"; do
  grep -Fxq "$service" <<<"$running" || {
    echo "Step34 service is not running: $service" >&2
    "${compose[@]}" ps >&2
    false
  }
done
echo "STEP34_COMPOSE_APPLICATION_SERVICES_READY"

dataset="$repo_root/tests/e2e/linux-ssh-authentication-burst/events.jsonl"
metadata="$repo_root/tests/e2e/linux-ssh-authentication-burst/dataset.json"

python3 - "$CERBERO_SYSLOG_HOST_PORT" "$dataset" <<'PY'
import json
import socket
import sys

port = int(sys.argv[1])
events = [
    json.loads(line)
    for line in open(sys.argv[2], encoding="utf-8")
    if line.strip()
]
with socket.create_connection(("127.0.0.1", port), timeout=5) as stream:
    stream.settimeout(5)
    for event in events:
        raw = event["raw"].encode("utf-8")
        stream.sendall(str(len(raw)).encode("ascii") + b" " + raw)
print(f"STEP34_SYSLOG_FRAMES_SENT={len(events)}")
PY

raw_count=""
for _ in $(seq 1 200); do
  raw_count="$(
    docker compose --env-file .env -f deploy/compose/compose.yaml exec -T \
      -e PGPASSWORD="$POSTGRES_PASSWORD" postgres \
      psql -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At \
      -c "SELECT count(*) FROM system.raw_objects" 2>/dev/null || true
  )"
  raw_count="$(printf '%s' "$raw_count" | tr -d '[:space:]')"
  [[ "$raw_count" == "10" ]] && break
  sleep 0.1
done
[[ "$raw_count" == "10" ]] || {
  echo "Step34 expected 10 composed Raw Store objects, observed ${raw_count:-unknown}" >&2
  "${compose[@]}" logs cerbero-raw-preserver >&2 || true
  false
}
echo "STEP34_COMPOSE_RAW_COUNT_PASS count=$raw_count"

docker compose --env-file .env -f deploy/compose/compose.yaml exec -T \
  -e PGPASSWORD="$POSTGRES_PASSWORD" postgres \
  psql -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At \
  -c "SELECT raw_hash FROM system.raw_objects ORDER BY raw_hash" >"$tmp/raw-hashes.txt"

python3 - "$metadata" "$tmp/raw-hashes.txt" <<'PY'
import json
import pathlib
import sys

metadata = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
actual = sorted(
    line.strip()
    for line in pathlib.Path(sys.argv[2]).read_text(encoding="utf-8").splitlines()
    if line.strip()
)
expected = sorted(metadata["event_raw_sha256"])
assert actual == expected, (actual, expected)
print("STEP34_COMPOSE_RAW_HASH_PASS")
PY

normalized_count=""
for _ in $(seq 1 300); do
  normalized_count="$(
    docker compose --env-file .env -f deploy/compose/compose.yaml exec -T clickhouse \
      clickhouse-client \
        --user "$CLICKHOUSE_USER" \
        --password "$CLICKHOUSE_PASSWORD" \
        --query "SELECT count() FROM ${CLICKHOUSE_DB}.normalized_events WHERE tenant_id = '${CERBERO_SYSLOG_DEV_TENANT_ID}'" \
        </dev/null 2>/dev/null || true
  )"
  normalized_count="$(printf '%s' "$normalized_count" | tr -d '[:space:]')"
  [[ "$normalized_count" == "10" ]] && break
  sleep 0.1
done
[[ "$normalized_count" == "10" ]] || {
  echo "Step34 expected 10 composed normalized events, observed ${normalized_count:-unknown}" >&2
  "${compose[@]}" logs cerbero-normalizer >&2 || true
  false
}
echo "STEP34_COMPOSE_NORMALIZED_PASS count=$normalized_count"

event_id="$(
  docker compose --env-file .env -f deploy/compose/compose.yaml exec -T clickhouse \
    clickhouse-client \
      --user "$CLICKHOUSE_USER" \
      --password "$CLICKHOUSE_PASSWORD" \
      --query "SELECT toString(normalized_event_id) FROM ${CLICKHOUSE_DB}.normalized_events WHERE tenant_id = '${CERBERO_SYSLOG_DEV_TENANT_ID}' ORDER BY normalized_at_seconds, normalized_event_id LIMIT 1" \
      </dev/null |
    tr -d '[:space:]'
)"
[[ -n "$event_id" ]] || {
  echo "Step34 could not recover a normalized_event_id" >&2
  false
}

api_url="http://127.0.0.1:${CERBERO_API_HOST_PORT}"
api_ready=0
for _ in $(seq 1 120); do
  code="$(
    curl -sS -o "$tmp/event.json" -w '%{http_code}' \
      -H "X-Cerbero-Tenant-ID: $CERBERO_SYSLOG_DEV_TENANT_ID" \
      "$api_url/api/v1/events/$event_id" 2>/dev/null || true
  )"
  if [[ "$code" == "200" ]]; then
    api_ready=1
    break
  fi
  sleep 0.25
done
[[ "$api_ready" == "1" ]] || {
  "${compose[@]}" logs cerbero-api >&2 || true
  echo "Step34 API detail retrieval did not become ready" >&2
  false
}

python3 - "$tmp/event.json" <<'PY'
import json
import sys

event = json.load(open(sys.argv[1], encoding="utf-8"))
assert event["parser_id"] == "linux/sshd", event
assert event["parser_version"] == "1", event
assert event["mapping_id"] == "linux.ssh.authentication", event
assert event["mapping_version"] == "1", event
assert event["ocsf_version"] == "1.9.0", event
assert event["ocsf_event"]["status"] == "Failure", event
print("STEP34_COMPOSE_API_RETRIEVAL_PASS")
PY

cat >"$tmp/app-images.txt" <<EOF
cerbero-ingest|$(docker image inspect --format '{{.Id}}' "cerbero/ingest:${CERBERO_APP_IMAGE_TAG}")
cerbero-syslog|$(docker image inspect --format '{{.Id}}' "cerbero/syslog:${CERBERO_APP_IMAGE_TAG}")
cerbero-raw-preserver|$(docker image inspect --format '{{.Id}}' "cerbero/raw-preserver:${CERBERO_APP_IMAGE_TAG}")
cerbero-normalizer|$(docker image inspect --format '{{.Id}}' "cerbero/normalizer:${CERBERO_APP_IMAGE_TAG}")
cerbero-worker|$(docker image inspect --format '{{.Id}}' "cerbero/worker:${CERBERO_APP_IMAGE_TAG}")
cerbero-api|$(docker image inspect --format '{{.Id}}' "cerbero/api:${CERBERO_APP_IMAGE_TAG}")
EOF

python3 - "$metadata" "$tmp/raw-hashes.txt" "$event_id" \
  "${STEP34_COMPOSE_E2E_EVIDENCE_OUT:-}" \
  "$CERBERO_SYSLOG_DEV_TENANT_ID" "$tmp/app-images.txt" <<'PY'
import hashlib
import json
import pathlib
import subprocess
import sys

metadata = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
raw_hashes = sorted(
    line.strip()
    for line in pathlib.Path(sys.argv[2]).read_text(encoding="utf-8").splitlines()
    if line.strip()
)
event_id = sys.argv[3]
output = pathlib.Path(sys.argv[4]) if sys.argv[4] else None
tenant = sys.argv[5]
images_path = pathlib.Path(sys.argv[6])

services = [
    "cerbero-ingest",
    "cerbero-syslog",
    "cerbero-raw-preserver",
    "cerbero-normalizer",
    "cerbero-worker",
    "cerbero-api",
]
semantic = {
    "source_transport": "syslog",
    "syslog_framing": "RFC6587-octet-counting",
    "tenant_id": tenant,
    "raw_objects": 10,
    "raw_sha256": raw_hashes,
    "normalized_events": 10,
    "parser_id": "linux/sshd",
    "parser_version": "1",
    "mapping_id": "linux.ssh.authentication",
    "mapping_version": "1",
    "ocsf_version": "1.9.0",
    "api_retrieval": True,
    "compose_application_services": services,
    "dataset_id": metadata["dataset_id"],
    "dataset_version": metadata["version"],
    "dataset_sha256": metadata["content_sha256"],
}
canonical = json.dumps(semantic, sort_keys=True, separators=(",", ":")).encode()
app_image_ids = {}
for line in images_path.read_text(encoding="utf-8").splitlines():
    if not line.strip():
        continue
    service, image_id = line.split("|", 1)
    app_image_ids[service] = image_id
assert set(app_image_ids) == set(services), app_image_ids
assert all(app_image_ids.values()), app_image_ids

evidence = {
    "schema_version": "cerbero.step34.compose-e2e.v1",
    "semantic": semantic,
    "semantic_fingerprint_sha256": hashlib.sha256(canonical).hexdigest(),
    "runtime_objects": {
        "sample_normalized_event_id": event_id,
        "application_image_ids": app_image_ids,
    },
    "git_commit": subprocess.check_output(
        ["git", "rev-parse", "HEAD"], text=True
    ).strip(),
}
rendered = json.dumps(evidence, indent=2, sort_keys=True) + "\n"
if output:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(rendered, encoding="utf-8")
print(rendered, end="")
PY

echo "STEP34_COMPOSE_E2E_PASS"
