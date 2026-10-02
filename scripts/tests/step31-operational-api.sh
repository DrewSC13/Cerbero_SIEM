#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

command -v curl >/dev/null 2>&1 || { echo "curl is required for Step 31 operational API integration" >&2; exit 2; }
command -v python3 >/dev/null 2>&1 || { echo "python3 is required for Step 31 operational API integration" >&2; exit 2; }

set -a
# shellcheck disable=SC1091
source .env
set +a

: "${POSTGRES_DB:?POSTGRES_DB is required}"
: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}"
: "${POSTGRES_API_USER:?POSTGRES_API_USER is required}"
: "${POSTGRES_API_PASSWORD:?POSTGRES_API_PASSWORD is required}"

if [[ -z "${STEP31_API_E2E_URL:-}" ]]; then
  : "${CERBERO_API_LISTEN_ADDRESS:?CERBERO_API_LISTEN_ADDRESS is required}"
  STEP31_API_E2E_URL="http://${CERBERO_API_LISTEN_ADDRESS}"
fi

api_tmp="$(mktemp -d)"
api_pid=""
cleanup_step31_api() {
  if [[ -n "${api_pid:-}" ]] && kill -0 "$api_pid" >/dev/null 2>&1; then
    kill "$api_pid" >/dev/null 2>&1 || true
    wait "$api_pid" >/dev/null 2>&1 || true
  fi
  rm -rf "$api_tmp"
}
trap cleanup_step31_api EXIT

runtime_ids="$(
  docker compose --env-file .env -f deploy/compose/compose.yaml exec -T \
    -e PGPASSWORD="$POSTGRES_PASSWORD" postgres \
    psql -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At \
    -c "WITH runtime_tenant AS (
          SELECT tenant_id
          FROM detection.signals
          GROUP BY tenant_id
          HAVING count(*) >= 22
          ORDER BY max(created_at) DESC
          LIMIT 1
        )
        SELECT rt.tenant_id::text || '|' || f.finding_id::text
        FROM runtime_tenant AS rt
        JOIN LATERAL (
          SELECT finding_id
          FROM investigation.findings
          WHERE tenant_id = rt.tenant_id
          ORDER BY created_at DESC, finding_id DESC
          LIMIT 1
        ) AS f ON TRUE"
)"
IFS='|' read -r tenant_id finding_id <<<"$runtime_ids"
[[ -n "$tenant_id" ]] || { echo "Step 31 integration: missing Step 30 runtime tenant" >&2; false; }
[[ -n "$finding_id" ]] || { echo "Step 31 integration: missing durable Step 30 Finding" >&2; false; }

entity_id="018f47a2-4b00-7a00-8000-00000000e101"
contribution_id="018f47a2-4b00-7a00-8000-00000000e201"
incident_id="018f47a2-4b00-7a00-8000-00000000e301"
incident_audit_id="018f47a2-4b00-7a00-8000-00000000e401"
actor_id="018f47a2-4b00-7a00-8000-00000000e501"
wrong_tenant_id="018f47a2-4b00-7a00-8000-00000000e601"

seed_fixture() {
  docker compose --env-file .env -f deploy/compose/compose.yaml exec -T \
    -e PGPASSWORD="$POSTGRES_PASSWORD" postgres \
    psql -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
      --set ON_ERROR_STOP=1 \
      --set tenant_id="$tenant_id" \
      --set finding_id="$finding_id" \
      --set entity_id="$entity_id" \
      --set contribution_id="$contribution_id" \
      --set incident_id="$incident_id" \
      --set incident_audit_id="$incident_audit_id" \
      --set actor_id="$actor_id" >/dev/null <<'SQL'
INSERT INTO risk.entities(
  entity_id, tenant_id, entity_type, canonical_key, first_seen, last_seen,
  criticality, attributes, risk_score
) VALUES (
  :'entity_id'::uuid, :'tenant_id'::uuid, 'user', 'user:alice',
  now() - interval '10 minutes', now(), 'HIGH', '{"name":"alice"}'::jsonb, 0
)
ON CONFLICT (entity_id) DO NOTHING;

INSERT INTO investigation.finding_entities(
  tenant_id, finding_id, entity_id, role, confidence
) VALUES (
  :'tenant_id'::uuid, :'finding_id'::uuid, :'entity_id'::uuid, 'ACTOR', 'HIGH'
)
ON CONFLICT (finding_id, entity_id, role) DO NOTHING;

INSERT INTO risk.contributions(
  contribution_id, tenant_id, entity_id, finding_id, amount, severity,
  confidence, reason, created_at
) VALUES (
  :'contribution_id'::uuid, :'tenant_id'::uuid, :'entity_id'::uuid,
  :'finding_id'::uuid, 25, 'HIGH', 'MEDIUM',
  'explicit Step 31 integration risk contribution', now()
)
ON CONFLICT (contribution_id) DO NOTHING;

INSERT INTO investigation.incidents(
  incident_id, tenant_id, status, severity, confidence, title, description,
  first_seen, last_seen, correlation_reason, version
) VALUES (
  :'incident_id'::uuid, :'tenant_id'::uuid, 'OPEN', 'HIGH', 'MEDIUM',
  'Step 31 account compromise incident',
  'Operational Incident fixture linked to the durable Step 30 Finding',
  now() - interval '10 minutes', now(),
  'explicit integration promotion; automatic policy remains deferred', 1
)
ON CONFLICT (incident_id) DO NOTHING;

INSERT INTO investigation.incident_findings(
  tenant_id, incident_id, finding_id, relation, added_by
) VALUES (
  :'tenant_id'::uuid, :'incident_id'::uuid, :'finding_id'::uuid,
  'PROMOTED_FINDING', :'actor_id'::uuid
)
ON CONFLICT (incident_id, finding_id) DO NOTHING;

INSERT INTO audit.events(
  audit_event_id, tenant_id, event_type, actor_type, actor_id, action,
  object_type, object_id, occurred_at, reason, before_state, after_state,
  result, metadata
) VALUES (
  :'incident_audit_id'::uuid, :'tenant_id'::uuid, 'incident.created',
  'INTEGRATION_FIXTURE', :'actor_id', 'create', 'incident', :'incident_id',
  now(), 'Step 31 deterministic integration fixture', NULL,
  jsonb_build_object('status', 'OPEN', 'finding_id', :'finding_id'),
  'SUCCESS', '{}'::jsonb
)
ON CONFLICT (audit_event_id) DO NOTHING;
SQL
}

seed_fixture
seed_fixture

fixture_counts="$(
  docker compose --env-file .env -f deploy/compose/compose.yaml exec -T \
    -e PGPASSWORD="$POSTGRES_PASSWORD" postgres \
    psql -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At \
      --set ON_ERROR_STOP=1 \
      --set tenant_id="$tenant_id" \
      --set finding_id="$finding_id" \
      --set entity_id="$entity_id" \
      --set contribution_id="$contribution_id" \
      --set incident_id="$incident_id" <<'SQL'
SELECT
  (SELECT count(*) FROM risk.contributions
   WHERE tenant_id = :'tenant_id'::uuid
     AND contribution_id = :'contribution_id'::uuid)::text || '|' ||
  (SELECT count(*) FROM investigation.finding_entities
   WHERE tenant_id = :'tenant_id'::uuid
     AND finding_id = :'finding_id'::uuid
     AND entity_id = :'entity_id'::uuid)::text || '|' ||
  (SELECT count(*) FROM investigation.incident_findings
   WHERE tenant_id = :'tenant_id'::uuid
     AND incident_id = :'incident_id'::uuid
     AND finding_id = :'finding_id'::uuid)::text;
SQL
)"
[[ "$fixture_counts" == "1|1|1" ]] || {
  echo "Step 31 integration: retry/idempotency fixture counts were $fixture_counts" >&2
  false
}

echo "Step 31 Entity/Risk/Incident durable fixture idempotency: PASS"

(
  cd services/cerbero-api
  go build -o "$api_tmp/cerbero-api" .
)
"$api_tmp/cerbero-api" >"$api_tmp/api.log" 2>&1 &
api_pid=$!

api_ready=0
for _ in $(seq 1 50); do
  if ! kill -0 "$api_pid" >/dev/null 2>&1; then
    cat "$api_tmp/api.log" >&2
    echo "Step 31 integration: cerbero-api exited before readiness" >&2
    false
  fi
  api_code="$(
    curl -sS -o "$api_tmp/entities-list.json" -w '%{http_code}' \
      -H "X-Cerbero-Tenant-ID: $tenant_id" \
      "$STEP31_API_E2E_URL/api/v1/entities" 2>/dev/null || true
  )"
  if [[ "$api_code" == "200" ]]; then
    api_ready=1
    break
  fi
  sleep 0.2
done
[[ "$api_ready" == "1" ]] || {
  cat "$api_tmp/api.log" >&2
  echo "Step 31 integration: API readiness failed" >&2
  false
}

grep -Fq "$entity_id" "$api_tmp/entities-list.json" || { cat "$api_tmp/entities-list.json" >&2; false; }

entity_code="$(curl -sS -o "$api_tmp/entity.json" -w '%{http_code}' -H "X-Cerbero-Tenant-ID: $tenant_id" "$STEP31_API_E2E_URL/api/v1/entities/$entity_id")"
[[ "$entity_code" == "200" ]] || { cat "$api_tmp/entity.json" >&2; false; }
grep -Fq '"canonical_key":"user:alice"' "$api_tmp/entity.json" || { cat "$api_tmp/entity.json" >&2; false; }

risk_code="$(curl -sS -o "$api_tmp/risk.json" -w '%{http_code}' -H "X-Cerbero-Tenant-ID: $tenant_id" "$STEP31_API_E2E_URL/api/v1/entities/$entity_id/risk-contributions")"
[[ "$risk_code" == "200" ]] || { cat "$api_tmp/risk.json" >&2; false; }
grep -Fq "$contribution_id" "$api_tmp/risk.json" || { cat "$api_tmp/risk.json" >&2; false; }
grep -Fq '"amount":"25.0000"' "$api_tmp/risk.json" || { cat "$api_tmp/risk.json" >&2; false; }

finding_entities_code="$(curl -sS -o "$api_tmp/finding-entities.json" -w '%{http_code}' -H "X-Cerbero-Tenant-ID: $tenant_id" "$STEP31_API_E2E_URL/api/v1/findings/$finding_id/entities")"
[[ "$finding_entities_code" == "200" ]] || { cat "$api_tmp/finding-entities.json" >&2; false; }
grep -Fq "$entity_id" "$api_tmp/finding-entities.json" || { cat "$api_tmp/finding-entities.json" >&2; false; }

incident_code="$(curl -sS -o "$api_tmp/incident.json" -w '%{http_code}' -H "X-Cerbero-Tenant-ID: $tenant_id" "$STEP31_API_E2E_URL/api/v1/incidents/$incident_id")"
[[ "$incident_code" == "200" ]] || { cat "$api_tmp/incident.json" >&2; false; }
grep -Fq "$finding_id" "$api_tmp/incident.json" || { cat "$api_tmp/incident.json" >&2; false; }

wrong_tenant_code="$(curl -sS -o "$api_tmp/wrong-tenant.json" -w '%{http_code}' -H "X-Cerbero-Tenant-ID: $wrong_tenant_id" "$STEP31_API_E2E_URL/api/v1/incidents/$incident_id")"
[[ "$wrong_tenant_code" == "404" ]] || { cat "$api_tmp/wrong-tenant.json" >&2; echo "expected wrong-tenant 404, got $wrong_tenant_code" >&2; false; }

incident_patch_code="$(
  curl -sS -o "$api_tmp/incident-patched.json" -w '%{http_code}' \
    -X PATCH \
    -H "Content-Type: application/json" \
    -H "X-Cerbero-Tenant-ID: $tenant_id" \
    -H "X-Cerbero-Actor-ID: $actor_id" \
    -d '{"expected_version":1,"status":"TRIAGED","reason":"Step 31 triage"}' \
    "$STEP31_API_E2E_URL/api/v1/incidents/$incident_id"
)"
[[ "$incident_patch_code" == "200" ]] || { cat "$api_tmp/incident-patched.json" >&2; false; }
grep -Fq '"status":"TRIAGED"' "$api_tmp/incident-patched.json" || { cat "$api_tmp/incident-patched.json" >&2; false; }
grep -Fq '"version":2' "$api_tmp/incident-patched.json" || { cat "$api_tmp/incident-patched.json" >&2; false; }

incident_stale_code="$(
  curl -sS -o "$api_tmp/incident-stale.json" -w '%{http_code}' \
    -X PATCH \
    -H "Content-Type: application/json" \
    -H "X-Cerbero-Tenant-ID: $tenant_id" \
    -H "X-Cerbero-Actor-ID: $actor_id" \
    -d '{"expected_version":1,"status":"INVESTIGATING"}' \
    "$STEP31_API_E2E_URL/api/v1/incidents/$incident_id"
)"
[[ "$incident_stale_code" == "409" ]] || { cat "$api_tmp/incident-stale.json" >&2; echo "expected Incident stale-version 409, got $incident_stale_code" >&2; false; }

echo "Step 31 Incident optimistic concurrency + tenant isolation: PASS"

case_create_code="$(
  curl -sS -o "$api_tmp/case-created.json" -w '%{http_code}' \
    -X POST \
    -H "Content-Type: application/json" \
    -H "X-Cerbero-Tenant-ID: $tenant_id" \
    -H "X-Cerbero-Actor-ID: $actor_id" \
    -d "{\"title\":\"Step 31 account compromise investigation\",\"description\":\"Durable Case linked to Incident and Finding provenance\",\"priority\":\"P2_HIGH\",\"incident_id\":\"$incident_id\"}" \
    "$STEP31_API_E2E_URL/api/v1/cases"
)"
[[ "$case_create_code" == "201" ]] || {
  cat "$api_tmp/case-created.json" >&2
  cat "$api_tmp/api.log" >&2
  echo "Step 31 integration: Case creation HTTP $case_create_code" >&2
  false
}
case_id="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["case_id"])' "$api_tmp/case-created.json")"
[[ -n "$case_id" ]] || { cat "$api_tmp/case-created.json" >&2; false; }
grep -Fq "$incident_id" "$api_tmp/case-created.json" || { cat "$api_tmp/case-created.json" >&2; false; }
grep -Fq "$finding_id" "$api_tmp/case-created.json" || { cat "$api_tmp/case-created.json" >&2; false; }
grep -Fq "$entity_id" "$api_tmp/case-created.json" || { cat "$api_tmp/case-created.json" >&2; false; }

patch_case() {
  local expected_version="$1"
  local body="$2"
  local output="$3"
  local code
  code="$(
    curl -sS -o "$output" -w '%{http_code}' \
      -X PATCH \
      -H "Content-Type: application/json" \
      -H "X-Cerbero-Tenant-ID: $tenant_id" \
      -H "X-Cerbero-Actor-ID: $actor_id" \
      -d "{\"expected_version\":$expected_version,$body}" \
      "$STEP31_API_E2E_URL/api/v1/cases/$case_id"
  )"
  [[ "$code" == "200" ]] || { cat "$output" >&2; echo "Case patch HTTP $code" >&2; false; }
}

patch_case 1 '"status":"TRIAGE","reason":"initial triage"' "$api_tmp/case-v2.json"
grep -Fq '"version":2' "$api_tmp/case-v2.json" || { cat "$api_tmp/case-v2.json" >&2; false; }

case_stale_code="$(
  curl -sS -o "$api_tmp/case-stale.json" -w '%{http_code}' \
    -X PATCH \
    -H "Content-Type: application/json" \
    -H "X-Cerbero-Tenant-ID: $tenant_id" \
    -H "X-Cerbero-Actor-ID: $actor_id" \
    -d '{"expected_version":1,"status":"INVESTIGATING"}' \
    "$STEP31_API_E2E_URL/api/v1/cases/$case_id"
)"
[[ "$case_stale_code" == "409" ]] || { cat "$api_tmp/case-stale.json" >&2; echo "expected Case stale-version 409, got $case_stale_code" >&2; false; }

patch_case 2 '"status":"INVESTIGATING","reason":"triage accepted"' "$api_tmp/case-v3.json"
patch_case 3 '"status":"RESPONSE","reason":"response coordination"' "$api_tmp/case-v7.json"
patch_case 4 '"status":"RESOLVED","reason":"investigation resolved"' "$api_tmp/case-v7.json"
patch_case 5 '"status":"CLOSED","disposition":"CONFIRMED_INCIDENT","closure_reason":"validated Step 31 integration incident"' "$api_tmp/case-v7.json"
grep -Fq '"status":"CLOSED"' "$api_tmp/case-v7.json" || { cat "$api_tmp/case-v7.json" >&2; false; }
grep -Fq '"version":6' "$api_tmp/case-v7.json" || { cat "$api_tmp/case-v7.json" >&2; false; }

patch_case 6 '"status":"INVESTIGATING","reopen_reason":"new evidence requires renewed investigation"' "$api_tmp/case-v7.json"
grep -Fq '"status":"INVESTIGATING"' "$api_tmp/case-v7.json" || { cat "$api_tmp/case-v7.json" >&2; false; }
grep -Fq '"version":7' "$api_tmp/case-v7.json" || { cat "$api_tmp/case-v7.json" >&2; false; }
grep -Fq '"disposition":"UNDETERMINED"' "$api_tmp/case-v7.json" || { cat "$api_tmp/case-v7.json" >&2; false; }

echo "Step 31 Case state machine + close/reopen + optimistic concurrency: PASS"

case_get_code="$(curl -sS -o "$api_tmp/case-detail.json" -w '%{http_code}' -H "X-Cerbero-Tenant-ID: $tenant_id" "$STEP31_API_E2E_URL/api/v1/cases/$case_id")"
[[ "$case_get_code" == "200" ]] || { cat "$api_tmp/case-detail.json" >&2; false; }
for expected_id in "$incident_id" "$finding_id" "$entity_id"; do
  grep -Fq "$expected_id" "$api_tmp/case-detail.json" || { cat "$api_tmp/case-detail.json" >&2; false; }
done

case_wrong_tenant_code="$(curl -sS -o "$api_tmp/case-wrong-tenant.json" -w '%{http_code}' -H "X-Cerbero-Tenant-ID: $wrong_tenant_id" "$STEP31_API_E2E_URL/api/v1/cases/$case_id")"
[[ "$case_wrong_tenant_code" == "404" ]] || { cat "$api_tmp/case-wrong-tenant.json" >&2; false; }

timeline_code="$(curl -sS -o "$api_tmp/timeline.json" -w '%{http_code}' -H "X-Cerbero-Tenant-ID: $tenant_id" "$STEP31_API_E2E_URL/api/v1/cases/$case_id/timeline")"
[[ "$timeline_code" == "200" ]] || { cat "$api_tmp/timeline.json" >&2; false; }
for marker in '"entry_type":"INCIDENT"' '"entry_type":"FINDING"' '"entry_type":"AUDIT"'; do
  grep -Fq "$marker" "$api_tmp/timeline.json" || { cat "$api_tmp/timeline.json" >&2; false; }
done
if grep -Fq '"entry_type":"ENTITY"' "$api_tmp/timeline.json"; then
  cat "$api_tmp/timeline.json" >&2
  false
fi

audit_code="$(curl -sS -o "$api_tmp/audit.json" -w '%{http_code}' -H "X-Cerbero-Tenant-ID: $tenant_id" "$STEP31_API_E2E_URL/api/v1/audit?object_type=case&object_id=$case_id")"
[[ "$audit_code" == "200" ]] || { cat "$api_tmp/audit.json" >&2; false; }
for event_type in 'case.created' 'case.status_changed' 'case.closed' 'case.reopened'; do
  grep -Fq "$event_type" "$api_tmp/audit.json" || { cat "$api_tmp/audit.json" >&2; false; }
done

wrong_audit_code="$(curl -sS -o "$api_tmp/audit-wrong-tenant.json" -w '%{http_code}' -H "X-Cerbero-Tenant-ID: $wrong_tenant_id" "$STEP31_API_E2E_URL/api/v1/audit?object_type=case&object_id=$case_id")"
[[ "$wrong_audit_code" == "200" ]] || { cat "$api_tmp/audit-wrong-tenant.json" >&2; false; }
if grep -Fq "$case_id" "$api_tmp/audit-wrong-tenant.json"; then
  cat "$api_tmp/audit-wrong-tenant.json" >&2
  echo "Step 31 integration: audit tenant isolation leak" >&2
  false
fi

if docker compose --env-file .env -f deploy/compose/compose.yaml exec -T \
  -e PGPASSWORD="$POSTGRES_API_PASSWORD" postgres \
  psql -h 127.0.0.1 -U "$POSTGRES_API_USER" -d "$POSTGRES_DB" \
    --set ON_ERROR_STOP=1 \
    --set tenant_id="$tenant_id" \
    >/dev/null 2>&1 <<'SQL'
UPDATE audit.events
SET result = 'TAMPERED'
WHERE tenant_id = :'tenant_id'::uuid;
SQL
then
  echo "Step 31 integration: cerbero_api unexpectedly mutated append-only audit" >&2
  false
fi

echo "Step 31 append-only AuditEvent + tenant isolation: PASS"
echo "STEP31_OPERATIONAL_API_E2E_PASS tenant=$tenant_id finding=$finding_id entity=$entity_id incident=$incident_id case=$case_id"
