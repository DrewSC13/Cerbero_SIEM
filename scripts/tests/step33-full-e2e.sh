#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

for command in docker curl python3 cargo go sha256sum; do
  command -v "$command" >/dev/null 2>&1 || {
    echo "$command is required for Step33 full E2E" >&2
    exit 2
  }
done
docker compose version >/dev/null

[[ -f .env ]] || { echo "missing .env; run make dev-init" >&2; exit 2; }

set -a
# shellcheck disable=SC1091
source .env
set +a

export COMPOSE_PROJECT_NAME="cerbero-step33-e2e"
export CERBERO_NATS_URL="nats://127.0.0.1:${NATS_PORT}"
export STEP33_E2E_TENANT_ID="018f47a2-4b00-7a00-8000-00000000d033"
export STEP33_E2E_START_MILLIS="1999999999000"
export STEP33_E2E_END_MILLIS="2000000300000"
export STEP33_E2E_DATASET="$repo_root/tests/e2e/linux-ssh-authentication-burst/events.jsonl"

e2e_tmp="$(mktemp -d)"
export CERBERO_RAW_STORE_PATH="$e2e_tmp/raw"
mkdir -p "$CERBERO_RAW_STORE_PATH"

raw_pid=""
normalizer_pid=""
api_pid=""

cleanup() {
  for pid in "$api_pid" "$normalizer_pid" "$raw_pid"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
      wait "$pid" >/dev/null 2>&1 || true
    fi
  done
  docker compose --env-file .env -f deploy/compose/compose.yaml \
    down --volumes --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$e2e_tmp"
}
trap cleanup EXIT

docker compose --env-file .env -f deploy/compose/compose.yaml \
  down --volumes --remove-orphans >/dev/null 2>&1 || true

./scripts/dev/init.sh
./scripts/dev/up.sh
./scripts/dev/bootstrap-nats.sh
./scripts/dev/health.sh
./scripts/dev/bootstrap-postgres.sh
./scripts/dev/bootstrap-clickhouse.sh

(
  cd services/cerbero-raw-preserver
  go build -o "$e2e_tmp/cerbero-raw-preserver" .
)
(
  cd services/cerbero-api
  go build -o "$e2e_tmp/cerbero-api" .
)
cargo build -p cerbero-normalizer -p cerbero-tui --locked
normalizer_bin="$repo_root/target/debug/cerbero-normalizer"
tui_bin="$repo_root/target/debug/cerbero-tui"

"$e2e_tmp/cerbero-raw-preserver" >"$e2e_tmp/raw-preserver.log" 2>&1 &
raw_pid=$!
"$normalizer_bin" >"$e2e_tmp/normalizer.log" 2>&1 &
normalizer_pid=$!

sleep 1
kill -0 "$raw_pid" >/dev/null 2>&1 || {
  cat "$e2e_tmp/raw-preserver.log" >&2
  echo "Step33 raw-preserver exited before source injection" >&2
  false
}
kill -0 "$normalizer_pid" >/dev/null 2>&1 || {
  cat "$e2e_tmp/normalizer.log" >&2
  echo "Step33 normalizer exited before source injection" >&2
  false
}

(
  cd services/cerbero-ingest
  go test -tags=integration ./internal/syslogingest \
    -run '^TestStep33SyslogSourceToJetStream$' -count=1 -v
) | tee "$e2e_tmp/source.log"
grep -Fq "STEP33_SOURCE_INGEST_PASS" "$e2e_tmp/source.log"

normalized_ready=0
count=""
count_error="$e2e_tmp/clickhouse-count.err"
for _ in $(seq 1 200); do
  if count="$(
    docker compose --env-file .env -f deploy/compose/compose.yaml exec -T clickhouse \
      clickhouse-client \
        --user "$CLICKHOUSE_USER" \
        --password "$CLICKHOUSE_PASSWORD" \
        --query "SELECT count() FROM ${CLICKHOUSE_DB}.normalized_events WHERE tenant_id = '${STEP33_E2E_TENANT_ID}'" \
        </dev/null 2>"$count_error"
  )"; then
    count="$(printf '%s' "$count" | tr -d '[:space:]')"
    if [[ "$count" == "10" ]]; then
      normalized_ready=1
      break
    fi
  fi
  sleep 0.1
done
if [[ "$normalized_ready" != "1" ]]; then
  cat "$count_error" >&2 || true
  cat "$e2e_tmp/raw-preserver.log" >&2
  cat "$e2e_tmp/normalizer.log" >&2
  echo "Step33 E2E expected 10 normalized events, observed ${count:-unknown}" >&2
  false
fi
echo "STEP33_NORMALIZED_EVENTS_PASS count=$count"

python3 - "$STEP33_E2E_DATASET" \
  "$repo_root/tests/e2e/linux-ssh-authentication-burst/dataset.json" \
  "$CERBERO_RAW_STORE_PATH" >"$e2e_tmp/raw-verification.json" <<'PY'
import hashlib
import json
import pathlib
import sys

dataset_path = pathlib.Path(sys.argv[1])
metadata = json.loads(pathlib.Path(sys.argv[2]).read_text(encoding="utf-8"))
raw_root = pathlib.Path(sys.argv[3])
events = [
    json.loads(line)
    for line in dataset_path.read_text(encoding="utf-8").splitlines()
    if line.strip()
]
raw_files = sorted(raw_root.rglob("raw.bin"))
assert len(raw_files) == 10, [str(path) for path in raw_files]
expected_bytes = sorted(event["raw"].encode("utf-8") for event in events)
actual_bytes = sorted(path.read_bytes() for path in raw_files)
assert actual_bytes == expected_bytes
actual_hashes = sorted(hashlib.sha256(value).hexdigest() for value in actual_bytes)
assert actual_hashes == sorted(metadata["event_raw_sha256"])
assert hashlib.sha256(dataset_path.read_bytes()).hexdigest() == metadata["content_sha256"]
print(json.dumps({
    "raw_objects": len(raw_files),
    "raw_sha256": actual_hashes,
    "dataset_sha256": metadata["content_sha256"],
}, sort_keys=True))
PY
echo "STEP33_RAW_PRESERVATION_PASS"

cargo test -p cerbero-detection-runtime \
  --test step33_source_to_tui_e2e --locked \
  -- --ignored --nocapture | tee "$e2e_tmp/detection.log"
signal_id="$(
  sed -n 's/.*STEP33_DETECTION_PASS.*signal=\([^ ]*\).*/\1/p' \
    "$e2e_tmp/detection.log" | tail -n 1
)"
[[ -n "$signal_id" ]] || {
  cat "$e2e_tmp/detection.log" >&2
  echo "Step33 E2E could not recover threshold Signal ID" >&2
  false
}

"$e2e_tmp/cerbero-api" >"$e2e_tmp/api.log" 2>&1 &
api_pid=$!
api_url="http://${CERBERO_API_LISTEN_ADDRESS}"
from="2033-05-18T03:33:00Z"
to="2033-05-18T03:38:00Z"

api_ready=0
for _ in $(seq 1 100); do
  code="$(
    curl -sS -o "$e2e_tmp/events-ready.json" -w '%{http_code}' \
      -H "X-Cerbero-Tenant-ID: $STEP33_E2E_TENANT_ID" \
      --get \
      --data-urlencode "from=$from" \
      --data-urlencode "to=$to" \
      --data-urlencode 'query=user.name == "admin"' \
      "$api_url/api/v1/events" 2>/dev/null || true
  )"
  if [[ "$code" == "200" ]]; then
    api_ready=1
    break
  fi
  if ! kill -0 "$api_pid" >/dev/null 2>&1; then
    cat "$e2e_tmp/api.log" >&2
    false
  fi
  sleep 0.1
done
[[ "$api_ready" == "1" ]] || {
  cat "$e2e_tmp/api.log" >&2
  echo "Step33 API readiness failed" >&2
  false
}

signal_code="$(
  curl -sS -o "$e2e_tmp/signal.json" -w '%{http_code}' \
    -H "X-Cerbero-Tenant-ID: $STEP33_E2E_TENANT_ID" \
    "$api_url/api/v1/signals/$signal_id"
)"
[[ "$signal_code" == "200" ]] || {
  cat "$e2e_tmp/signal.json" >&2
  false
}

cat >"$e2e_tmp/search.json" <<JSON
{
  "query": "user.name == \"admin\"",
  "time_range": {"from": "$from", "to": "$to"},
  "limit": 20,
  "sort": [{"field": "event_time", "direction": "asc"}]
}
JSON
search_code="$(
  curl -sS -o "$e2e_tmp/search-response.json" -w '%{http_code}' \
    -H 'Content-Type: application/json' \
    -H "X-Cerbero-Tenant-ID: $STEP33_E2E_TENANT_ID" \
    --data-binary @"$e2e_tmp/search.json" \
    "$api_url/api/v1/search"
)"
[[ "$search_code" == "200" ]] || {
  cat "$e2e_tmp/search-response.json" >&2
  false
}

python3 - "$e2e_tmp/signal.json" "$e2e_tmp/search-response.json" \
  "$CERBERO_RAW_STORE_PATH" >"$e2e_tmp/provenance.json" <<'PY'
import json
import pathlib
import sys

signal = json.load(open(sys.argv[1], encoding="utf-8"))
search = json.load(open(sys.argv[2], encoding="utf-8"))
raw_root = pathlib.Path(sys.argv[3])

items = search["items"]
assert len(items) == 10, len(items)
assert signal["rule_id"] == "CER-DET-000001"
assert signal["rule_version"] == "1"
assert int(signal["event_count"]) == 10
input_ids = {entry["input_id"] for entry in signal["inputs"]}
normalized_ids = {item["normalized_event_id"] for item in items}
assert input_ids == normalized_ids, (input_ids, normalized_ids)

raw_ids = {item["raw_event_id"] for item in items}
raw_parent_ids = {path.parent.name for path in raw_root.rglob("raw.bin")}
assert raw_ids <= raw_parent_ids, (raw_ids, raw_parent_ids)
assert {item["parser_id"] for item in items} == {"linux/sshd"}
assert {item["parser_version"] for item in items} == {"1"}
assert {item["mapping_id"] for item in items} == {"linux.ssh.authentication"}
assert {item["mapping_version"] for item in items} == {"1"}
assert {item["ocsf_version"] for item in items} == {"1.9.0"}
assert all(item["ocsf_event"]["status"] == "Failure" for item in items)
assert all(item["ocsf_event"]["user"]["name"] == "admin" for item in items)
print(json.dumps({
    "normalized_events": len(items),
    "signal_inputs": len(input_ids),
    "raw_ids": sorted(raw_ids),
    "parser_id": "linux/sshd",
    "parser_version": "1",
    "mapping_id": "linux.ssh.authentication",
    "mapping_version": "1",
    "ocsf_version": "1.9.0",
}, sort_keys=True))
PY
echo "STEP33_PROVENANCE_PASS"

CERBERO_TUI_API_URL="$api_url" \
CERBERO_TUI_TENANT_ID="$STEP33_E2E_TENANT_ID" \
  "$tui_bin" \
    --snapshot \
    --query 'user.name == "admin"' \
    --from "$from" \
    --to "$to" \
    --limit 20 >"$e2e_tmp/tui-snapshot.txt"

grep -Fq "CERBERO Search snapshot" "$e2e_tmp/tui-snapshot.txt"
tui_rows="$(grep -Fc 'user=admin' "$e2e_tmp/tui-snapshot.txt")"
[[ "$tui_rows" == "10" ]] || {
  cat "$e2e_tmp/tui-snapshot.txt" >&2
  echo "Step33 TUI expected 10 rows, observed $tui_rows" >&2
  false
}
echo "STEP33_TUI_INSPECTION_PASS"

evidence_out="${STEP33_E2E_EVIDENCE_OUT:-$e2e_tmp/e2e-evidence.json}"
mkdir -p "$(dirname "$evidence_out")"

python3 - \
  "$repo_root/tests/e2e/linux-ssh-authentication-burst/dataset.json" \
  "$repo_root/rules/sigma/linux_sshd_failed_login_burst.yml" \
  "$e2e_tmp/raw-verification.json" \
  "$e2e_tmp/provenance.json" \
  "$signal_id" \
  "$tui_rows" \
  "$evidence_out" <<'PY'
import hashlib
import json
import pathlib
import sys

dataset = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
rule_bytes = pathlib.Path(sys.argv[2]).read_bytes()
raw = json.loads(pathlib.Path(sys.argv[3]).read_text(encoding="utf-8"))
prov = json.loads(pathlib.Path(sys.argv[4]).read_text(encoding="utf-8"))
signal_id = sys.argv[5]
tui_rows = int(sys.argv[6])
out = pathlib.Path(sys.argv[7])

semantic = {
    "dataset_id": dataset["dataset_id"],
    "dataset_version": dataset["version"],
    "dataset_sha256": dataset["content_sha256"],
    "raw_objects": raw["raw_objects"],
    "raw_sha256": raw["raw_sha256"],
    "normalized_events": prov["normalized_events"],
    "parser_id": prov["parser_id"],
    "parser_version": prov["parser_version"],
    "mapping_id": prov["mapping_id"],
    "mapping_version": prov["mapping_version"],
    "ocsf_version": prov["ocsf_version"],
    "rule_id": "CER-DET-000001",
    "rule_version": "1",
    "rule_content_sha256": hashlib.sha256(rule_bytes).hexdigest(),
    "threshold_signals": 1,
    "matched_events": prov["signal_inputs"],
    "findings": 0,
    "api_retrieval": True,
    "tui_search_rows": tui_rows,
}
canonical = json.dumps(semantic, sort_keys=True, separators=(",", ":")).encode()
evidence = {
    "schema_version": "cerbero.step33.e2e.evidence.v1",
    "semantic": semantic,
    "semantic_fingerprint_sha256": hashlib.sha256(canonical).hexdigest(),
    "runtime_objects": {"threshold_signal_id": signal_id},
}
out.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
print(f"STEP33_E2E_EVIDENCE={out}")
print(f"STEP33_E2E_SEMANTIC_SHA256={evidence['semantic_fingerprint_sha256']}")
PY

echo "STEP33_FULL_E2E_PASS"
