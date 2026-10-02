#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

command -v curl >/dev/null 2>&1 || { echo "curl is required for Step 32 integration" >&2; exit 2; }
command -v python3 >/dev/null 2>&1 || { echo "python3 is required for Step 32 integration" >&2; exit 2; }

set -a
# shellcheck disable=SC1091
source .env
set +a

: "${CLICKHOUSE_USER:?CLICKHOUSE_USER is required}"
: "${CLICKHOUSE_PASSWORD:?CLICKHOUSE_PASSWORD is required}"
: "${CLICKHOUSE_DB:?CLICKHOUSE_DB is required}"
: "${CERBERO_API_LISTEN_ADDRESS:?CERBERO_API_LISTEN_ADDRESS is required}"

api_url="${STEP32_API_E2E_URL:-http://${CERBERO_API_LISTEN_ADDRESS}}"
tenant_a="018f47a2-4b00-7a00-8000-00000000f001"
tenant_b="018f47a2-4b00-7a00-8000-00000000f002"
event_a1="018f47a2-4b00-7a00-8000-00000000f101"
event_a2="018f47a2-4b00-7a00-8000-00000000f102"
event_b1="018f47a2-4b00-7a00-8000-00000000f201"
raw_a1="018f47a2-4b00-7a00-8000-00000000f301"
raw_a2="018f47a2-4b00-7a00-8000-00000000f302"
raw_b1="018f47a2-4b00-7a00-8000-00000000f401"
from="2033-05-18T03:33:00Z"
to="2033-05-18T03:34:00Z"

api_tmp="$(mktemp -d)"
api_pid=""
cleanup_step32() {
  if [[ -n "${api_pid:-}" ]] && kill -0 "$api_pid" >/dev/null 2>&1; then
    kill "$api_pid" >/dev/null 2>&1 || true
    wait "$api_pid" >/dev/null 2>&1 || true
  fi
  rm -rf "$api_tmp"
}
trap cleanup_step32 EXIT

insert_sql="$(cat <<SQL
INSERT INTO ${CLICKHOUSE_DB}.normalized_events
(
  logical_key, tenant_id, normalized_event_id, raw_event_id,
  event_time_present, event_time_seconds, event_time_nanos,
  ingest_time_seconds, ingest_time_nanos,
  normalized_at_seconds, normalized_at_nanos,
  ocsf_version, class_uid, category_uid, activity_id, severity,
  parser_id, parser_version, mapping_id, mapping_version,
  normalization_status, normalized_hash_algorithm, normalized_hash,
  pipeline_version, ocsf_event_json, transformation_id, configuration_hash,
  execution_mode, publication_message_id, causation_message_id,
  trace_id, correlation_id, producer_component_version, producer_instance_id
)
VALUES
(
  repeat('a', 64), '$tenant_a', toUUID('$event_a1'), toUUID('$raw_a1'),
  1, 2000000000, 0, 2000000000, 0, 2000000000, 0,
  '1.9.0', 3002, 3, 1, 2,
  'cerbero.parser.linux.sshd', '1', 'linux.ssh.auth', '1',
  1, 'SHA-256', repeat('b', 64), 'step32-fixture',
  '{"activity_id":1,"activity_name":"Logon","category_uid":3,"class_uid":3002,"message":"Step32 failed SSH login A1","severity":"Low","severity_id":2,"src_endpoint":{"ip":"10.32.0.1"},"status":"Failure","user":{"name":"step32-admin"}}',
  toUUID('018f47a2-4b00-7a00-8000-00000000f501'), repeat('c', 64), 1,
  toUUID('018f47a2-4b00-7a00-8000-00000000f601'), toUUID('018f47a2-4b00-7a00-8000-00000000f701'),
  'step32-trace-a1', 'step32-correlation-a1', 'step32', 'step32-fixture'
),
(
  repeat('d', 64), '$tenant_a', toUUID('$event_a2'), toUUID('$raw_a2'),
  1, 2000000001, 0, 2000000001, 0, 2000000001, 0,
  '1.9.0', 3002, 3, 1, 4,
  'cerbero.parser.linux.sshd', '1', 'linux.ssh.auth', '1',
  1, 'SHA-256', repeat('e', 64), 'step32-fixture',
  '{"activity_id":1,"activity_name":"Logon","category_uid":3,"class_uid":3002,"message":"Step32 failed SSH login A2","severity":"High","severity_id":4,"src_endpoint":{"ip":"10.32.0.2"},"status":"Failure","user":{"name":"step32-admin"}}',
  toUUID('018f47a2-4b00-7a00-8000-00000000f502'), repeat('f', 64), 1,
  toUUID('018f47a2-4b00-7a00-8000-00000000f602'), toUUID('018f47a2-4b00-7a00-8000-00000000f702'),
  'step32-trace-a2', 'step32-correlation-a2', 'step32', 'step32-fixture'
),
(
  repeat('1', 64), '$tenant_b', toUUID('$event_b1'), toUUID('$raw_b1'),
  1, 2000000002, 0, 2000000002, 0, 2000000002, 0,
  '1.9.0', 3002, 3, 1, 5,
  'cerbero.parser.linux.sshd', '1', 'linux.ssh.auth', '1',
  1, 'SHA-256', repeat('2', 64), 'step32-fixture',
  '{"activity_id":1,"activity_name":"Logon","category_uid":3,"class_uid":3002,"message":"Step32 tenant B isolation fixture","severity":"Critical","severity_id":5,"src_endpoint":{"ip":"10.32.9.9"},"status":"Failure","user":{"name":"step32-admin"}}',
  toUUID('018f47a2-4b00-7a00-8000-00000000f503'), repeat('3', 64), 1,
  toUUID('018f47a2-4b00-7a00-8000-00000000f603'), toUUID('018f47a2-4b00-7a00-8000-00000000f703'),
  'step32-trace-b1', 'step32-correlation-b1', 'step32', 'step32-fixture'
);
SQL
)"

docker compose --env-file .env -f deploy/compose/compose.yaml exec -T clickhouse \
  clickhouse-client \
    --user "$CLICKHOUSE_USER" \
    --password "$CLICKHOUSE_PASSWORD" \
    --query "$insert_sql" \
    </dev/null

echo "Step 32 deterministic ClickHouse search fixture: PASS"

(
  cd services/cerbero-api
  go build -o "$api_tmp/cerbero-api" .
)
cargo build -p cerbero-tui --locked --bin cerbero-tui
tui_bin="$repo_root/target/debug/cerbero-tui"

"$api_tmp/cerbero-api" >"$api_tmp/api.log" 2>&1 &
api_pid=$!

ready=0
for _ in $(seq 1 50); do
  if ! kill -0 "$api_pid" >/dev/null 2>&1; then
    cat "$api_tmp/api.log" >&2
    echo "Step 32 API exited before readiness" >&2
    false
  fi
  code="$(
    curl -sS -o "$api_tmp/ready.json" -w '%{http_code}' \
      -H "X-Cerbero-Tenant-ID: $tenant_a" \
      --get \
      --data-urlencode "from=$from" \
      --data-urlencode "to=$to" \
      --data-urlencode 'query=user.name == "step32-admin"' \
      "$api_url/api/v1/events" 2>/dev/null || true
  )"
  if [[ "$code" == "200" ]]; then
    ready=1
    break
  fi
  sleep 0.2
done
if [[ "$ready" != "1" ]]; then
  cat "$api_tmp/api.log" >&2
  cat "$api_tmp/ready.json" >&2 || true
  echo "Step 32 API readiness failed" >&2
  false
fi

cat >"$api_tmp/search.json" <<JSON
{
  "query": "user.name == \\"step32-admin\\" AND severity_id >= 2",
  "time_range": {"from": "$from", "to": "$to"},
  "limit": 10,
  "sort": [{"field": "event_time", "direction": "desc"}]
}
JSON
search_code="$(
  curl -sS -o "$api_tmp/search-response.json" -w '%{http_code}' \
    -H 'Content-Type: application/json' \
    -H "X-Cerbero-Tenant-ID: $tenant_a" \
    --data-binary @"$api_tmp/search.json" \
    "$api_url/api/v1/search"
)"
[[ "$search_code" == "200" ]] || { cat "$api_tmp/search-response.json" >&2; false; }
python3 - "$api_tmp/search-response.json" "$tenant_a" "$event_a1" "$event_a2" <<'PY'
import json, sys
body=json.load(open(sys.argv[1]))
tenant, first, second=sys.argv[2:]
ids={item["normalized_event_id"] for item in body["items"]}
assert ids == {first, second}, (ids, body)
assert all(item["tenant_id"] == tenant for item in body["items"])
assert body["execution"]["request_id"]
assert body["execution"]["partial_result"] is False
assert body["execution"]["truncated"] is False
PY

echo "Step 32 canonical query + tenant scope: PASS"

cat >"$api_tmp/page1.json" <<JSON
{"query":"user.name == \\"step32-admin\\"","time_range":{"from":"$from","to":"$to"},"limit":1,"sort":[{"field":"event_time","direction":"desc"}]}
JSON
page1_code="$(curl -sS -o "$api_tmp/page1-response.json" -w '%{http_code}' -H 'Content-Type: application/json' -H "X-Cerbero-Tenant-ID: $tenant_a" --data-binary @"$api_tmp/page1.json" "$api_url/api/v1/search")"
[[ "$page1_code" == "200" ]] || { cat "$api_tmp/page1-response.json" >&2; false; }
cursor="$(python3 - "$api_tmp/page1-response.json" <<'PY'
import json, sys
body=json.load(open(sys.argv[1]))
assert len(body["items"]) == 1
assert body["next_cursor"]
assert body["execution"]["partial_result"] is True
assert body["execution"]["truncated"] is True
print(body["next_cursor"])
PY
)"
echo "Step 32 cursor page 1 + truncation metadata: PASS"

python3 - "$api_tmp/page1.json" "$cursor" "$api_tmp/page2.json" <<'PY'
import json, sys
body=json.load(open(sys.argv[1]))
body["cursor"]=sys.argv[2]
json.dump(body, open(sys.argv[3], "w"))
PY
page2_code="$(curl -sS -o "$api_tmp/page2-response.json" -w '%{http_code}' -H 'Content-Type: application/json' -H "X-Cerbero-Tenant-ID: $tenant_a" --data-binary @"$api_tmp/page2.json" "$api_url/api/v1/search")"
[[ "$page2_code" == "200" ]] || { cat "$api_tmp/page2-response.json" >&2; false; }
python3 - "$api_tmp/page1-response.json" "$api_tmp/page2-response.json" <<'PY'
import json, sys
one=json.load(open(sys.argv[1]))["items"][0]["normalized_event_id"]
two=json.load(open(sys.argv[2]))["items"][0]["normalized_event_id"]
assert one != two, (one, two)
PY

echo "Step 32 opaque cursor pagination + explicit truncation: PASS"

cat >"$api_tmp/tenant-b.json" <<JSON
{"query":"user.name == \\"step32-admin\\"","time_range":{"from":"$from","to":"$to"},"limit":10}
JSON
tenant_b_code="$(curl -sS -o "$api_tmp/tenant-b-response.json" -w '%{http_code}' -H 'Content-Type: application/json' -H "X-Cerbero-Tenant-ID: $tenant_b" --data-binary @"$api_tmp/tenant-b.json" "$api_url/api/v1/search")"
[[ "$tenant_b_code" == "200" ]] || { cat "$api_tmp/tenant-b-response.json" >&2; false; }
python3 - "$api_tmp/tenant-b-response.json" "$tenant_b" "$event_b1" <<'PY'
import json, sys
body=json.load(open(sys.argv[1]))
assert [item["normalized_event_id"] for item in body["items"]] == [sys.argv[3]]
assert body["items"][0]["tenant_id"] == sys.argv[2]
PY

echo "Step 32 tenant B search isolation: PASS"

detail_a_code="$(curl -sS -o "$api_tmp/detail-a.json" -w '%{http_code}' -H "X-Cerbero-Tenant-ID: $tenant_a" "$api_url/api/v1/events/$event_a1")"
[[ "$detail_a_code" == "200" ]] || { cat "$api_tmp/detail-a.json" >&2; false; }
echo "Step 32 event detail same-tenant lookup: PASS"

cross_tenant_code="$(curl -sS -o "$api_tmp/detail-cross.json" -w '%{http_code}' -H "X-Cerbero-Tenant-ID: $tenant_b" "$api_url/api/v1/events/$event_a1")"
[[ "$cross_tenant_code" == "404" ]] || { cat "$api_tmp/detail-cross.json" >&2; false; }

echo "Step 32 event detail tenant isolation: PASS"

cat >"$api_tmp/injection.json" <<JSON
{"query":"user.name == \\"x\\"; SELECT 1","time_range":{"from":"$from","to":"$to"},"limit":10}
JSON
injection_code="$(curl -sS -o "$api_tmp/injection-response.txt" -w '%{http_code}' -H 'Content-Type: application/json' -H "X-Cerbero-Tenant-ID: $tenant_a" --data-binary @"$api_tmp/injection.json" "$api_url/api/v1/search")"
[[ "$injection_code" == "400" ]] || { cat "$api_tmp/injection-response.txt" >&2; false; }

cat >"$api_tmp/unknown-field.json" <<JSON
{"query":"physical_column == \\"secret\\"","time_range":{"from":"$from","to":"$to"},"limit":10}
JSON
unknown_code="$(curl -sS -o "$api_tmp/unknown-response.txt" -w '%{http_code}' -H 'Content-Type: application/json' -H "X-Cerbero-Tenant-ID: $tenant_a" --data-binary @"$api_tmp/unknown-field.json" "$api_url/api/v1/search")"
[[ "$unknown_code" == "400" ]] || { cat "$api_tmp/unknown-response.txt" >&2; false; }

missing_range_code="$(curl -sS -o "$api_tmp/missing-range.txt" -w '%{http_code}' -H 'Content-Type: application/json' -H "X-Cerbero-Tenant-ID: $tenant_a" --data-binary '{"query":"user.name == \"step32-admin\""}' "$api_url/api/v1/search")"
[[ "$missing_range_code" == "400" ]] || { cat "$api_tmp/missing-range.txt" >&2; false; }

echo "Step 32 parser/type/bounds/injection fail-closed: PASS"

CERBERO_TUI_API_URL="$api_url" \
CERBERO_TUI_TENANT_ID="$tenant_a" \
  "$tui_bin" \
    --snapshot \
    --query 'user.name == "step32-admin"' \
    --from "$from" \
    --to "$to" \
    --limit 10 \
    >"$api_tmp/tui-snapshot.txt"

grep -Fq 'CERBERO Search snapshot' "$api_tmp/tui-snapshot.txt"
grep -Fq "$event_a1" "$api_tmp/tui-snapshot.txt"
grep -Fq "$event_a2" "$api_tmp/tui-snapshot.txt"
grep -Fq 'user=step32-admin' "$api_tmp/tui-snapshot.txt"

echo "STEP32_TUI_SNAPSHOT_PASS tenant=$tenant_a"
echo "STEP32_SEARCH_API_E2E_PASS tenant=$tenant_a events=$event_a1,$event_a2 isolated_tenant=$tenant_b"
