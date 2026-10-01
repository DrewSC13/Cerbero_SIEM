#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

command -v docker >/dev/null 2>&1 || { echo "Docker is required for integration tests" >&2; exit 2; }
docker compose version >/dev/null

cleanup() {
  docker compose --env-file .env -f deploy/compose/compose.yaml down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

./scripts/dev/init.sh
docker compose --env-file .env -f deploy/compose/compose.yaml config --quiet
./scripts/dev/up.sh
./scripts/dev/bootstrap-nats.sh
./scripts/dev/health.sh
./scripts/dev/bootstrap-postgres.sh
./scripts/dev/bootstrap-clickhouse.sh
./scripts/tests/raw-preservation-postgres.sh

set -a
# shellcheck disable=SC1091
source .env
set +a
export CERBERO_NATS_URL="nats://127.0.0.1:${NATS_PORT}"

(
  cd services/cerbero-ingest
  go test -tags=integration ./internal/eventbus -run '^TestJetStreamAcceptorIntegration$' -count=1
  go test -tags=integration ./internal/ingestapp -run '^TestDevelopmentRuntime(HTTPToJetStream|NATSOutageRejectsAcceptance)$' -count=1
)

(
  cd services/cerbero-raw-preserver
  go test -tags=integration ./internal/preserver -run '^TestDevelopmentRawPreserverRuntime$' -count=1
)

cargo test -p cerbero-normalizer --test runtime_integration --locked -- --ignored --nocapture --test-threads=1
cargo test -p cerbero-detection-runtime --test postgres_integration --locked -- --ignored --nocapture --test-threads=1

cargo test -p cerbero-detection-runtime --test mvp_batch_integration --locked -- --ignored --nocapture --test-threads=1

echo "Step 30 MVP batch EVENT/THRESHOLD/SEQUENCE durable integration: PASS"
(
  command -v curl >/dev/null 2>&1 || {
    echo "curl is required for Step 30 API integration" >&2
    false
  }

  if [[ -z "${STEP30_API_E2E_URL:-}" ]]; then
    : "${CERBERO_API_LISTEN_ADDRESS:?CERBERO_API_LISTEN_ADDRESS is required}"
    STEP30_API_E2E_URL="http://${CERBERO_API_LISTEN_ADDRESS}"
  fi
  : "${STEP30_API_E2E_URL:?STEP30_API_E2E_URL is required}"

  api_tmp="$(mktemp -d)"
  api_pid=""

  cleanup_step30_api() {
    if [[ -n "${api_pid:-}" ]] && kill -0 "$api_pid" >/dev/null 2>&1; then
      kill "$api_pid" >/dev/null 2>&1 || true
      wait "$api_pid" >/dev/null 2>&1 || true
    fi
    rm -rf "$api_tmp"
  }
  trap cleanup_step30_api EXIT

  analytical_ids="$(
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
          SELECT rt.tenant_id::text || '|' || s.signal_id::text || '|' || f.finding_id::text
          FROM runtime_tenant AS rt
          JOIN LATERAL (
            SELECT signal_id
            FROM detection.signals
            WHERE tenant_id = rt.tenant_id
            ORDER BY created_at DESC, signal_id DESC
            LIMIT 1
          ) AS s ON TRUE
          JOIN LATERAL (
            SELECT finding_id
            FROM investigation.findings
            WHERE tenant_id = rt.tenant_id
            ORDER BY created_at DESC, finding_id DESC
            LIMIT 1
          ) AS f ON TRUE"
  )"

  IFS='|' read -r api_tenant_id api_signal_id api_finding_id <<<"$analytical_ids"

  [[ -n "$api_tenant_id" ]] || { echo "Step 30 API integration: missing runtime tenant" >&2; false; }
  [[ -n "$api_signal_id" ]] || { echo "Step 30 API integration: missing runtime Signal" >&2; false; }
  [[ -n "$api_finding_id" ]] || { echo "Step 30 API integration: missing runtime Finding" >&2; false; }

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
      echo "Step 30 API integration: cerbero-api exited before readiness" >&2
      false
    fi

    api_code="$(
      curl -sS -o "$api_tmp/signals-list.json" -w '%{http_code}' \
        -H "X-Cerbero-Tenant-ID: $api_tenant_id" \
        "$STEP30_API_E2E_URL/api/v1/signals" 2>/dev/null || true
    )"
    if [[ "$api_code" == "200" ]]; then
      api_ready=1
      break
    fi
    sleep 0.2
  done

  if [[ "$api_ready" != "1" ]]; then
    cat "$api_tmp/api.log" >&2
    echo "Step 30 API integration: API readiness failed" >&2
    false
  fi

  grep -Fq "$api_signal_id" "$api_tmp/signals-list.json" || {
    cat "$api_tmp/signals-list.json" >&2
    echo "Step 30 API integration: Signal missing from list endpoint" >&2
    false
  }

  signal_code="$(
    curl -sS -o "$api_tmp/signal-detail.json" -w '%{http_code}' \
      -H "X-Cerbero-Tenant-ID: $api_tenant_id" \
      "$STEP30_API_E2E_URL/api/v1/signals/$api_signal_id"
  )"
  [[ "$signal_code" == "200" ]] || {
    cat "$api_tmp/signal-detail.json" >&2
    echo "Step 30 API integration: Signal detail HTTP $signal_code" >&2
    false
  }
  grep -Fq "$api_signal_id" "$api_tmp/signal-detail.json" || {
    cat "$api_tmp/signal-detail.json" >&2
    echo "Step 30 API integration: wrong Signal detail payload" >&2
    false
  }

  finding_list_code="$(
    curl -sS -o "$api_tmp/findings-list.json" -w '%{http_code}' \
      -H "X-Cerbero-Tenant-ID: $api_tenant_id" \
      "$STEP30_API_E2E_URL/api/v1/findings"
  )"
  [[ "$finding_list_code" == "200" ]] || {
    cat "$api_tmp/findings-list.json" >&2
    echo "Step 30 API integration: Finding list HTTP $finding_list_code" >&2
    false
  }
  grep -Fq "$api_finding_id" "$api_tmp/findings-list.json" || {
    cat "$api_tmp/findings-list.json" >&2
    echo "Step 30 API integration: Finding missing from list endpoint" >&2
    false
  }

  finding_code="$(
    curl -sS -o "$api_tmp/finding-detail.json" -w '%{http_code}' \
      -H "X-Cerbero-Tenant-ID: $api_tenant_id" \
      "$STEP30_API_E2E_URL/api/v1/findings/$api_finding_id"
  )"
  [[ "$finding_code" == "200" ]] || {
    cat "$api_tmp/finding-detail.json" >&2
    echo "Step 30 API integration: Finding detail HTTP $finding_code" >&2
    false
  }
  grep -Fq "$api_finding_id" "$api_tmp/finding-detail.json" || {
    cat "$api_tmp/finding-detail.json" >&2
    echo "Step 30 API integration: wrong Finding detail payload" >&2
    false
  }

  missing_tenant_code="$(
    curl -sS -o "$api_tmp/missing-tenant.json" -w '%{http_code}' \
      "$STEP30_API_E2E_URL/api/v1/signals"
  )"
  [[ "$missing_tenant_code" != "200" ]] || {
    cat "$api_tmp/missing-tenant.json" >&2
    echo "Step 30 API integration: tenant header was not enforced" >&2
    false
  }

  echo "STEP30_API_E2E_PASS tenant=$api_tenant_id signal=$api_signal_id finding=$api_finding_id"
)

echo "Step 30 Signal/Finding HTTP API visibility integration: PASS"
echo "Milestone 2 NATS-outage integration: PASS"
echo "Milestone 2 JSON/HTTP durable-ingest runtime integration: PASS"
echo "Milestone 3 durable raw-preservation runtime integration: PASS"
echo "Milestone 4 normalizer ClickHouse + governed DLQ + LIVE/REPLAY/TEST history integration: PASS"
echo "Step 30 v1 durable Signal/Finding integration: PASS"
