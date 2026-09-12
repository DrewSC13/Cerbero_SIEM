#!/usr/bin/env sh
set -eu

: "${NATS_URL:?NATS_URL is required}"
: "${NATS_ADMIN_USER:?NATS_ADMIN_USER is required}"
: "${NATS_ADMIN_PASSWORD:?NATS_ADMIN_PASSWORD is required}"

nats_cmd() {
  nats --server "$NATS_URL" --user "$NATS_ADMIN_USER" --password "$NATS_ADMIN_PASSWORD" "$@"
}

ensure_stream() {
  name="$1"
  subjects="$2"
  max_bytes="$3"

  if nats_cmd stream info "$name" >/dev/null 2>&1; then
    echo "stream $name already exists"
    return 0
  fi

  nats_cmd stream add "$name" \
    --subjects "$subjects" \
    --storage file \
    --retention limits \
    --discard old \
    --replicas 1 \
    --max-bytes "$max_bytes" \
    --defaults
}

ensure_stream CERBERO_RAW "cerbero.v1.raw.*" 1073741824
ensure_stream CERBERO_ANALYTICS "cerbero.v1.normalized.*,cerbero.v1.signal.*,cerbero.v1.finding.*,cerbero.v1.incident.*,cerbero.v1.case.*" 1073741824
ensure_stream CERBERO_SYSTEM "cerbero.v1.system.*,cerbero.v1.audit.*" 268435456
ensure_stream CERBERO_DLQ "cerbero.v1.dlq.*" 536870912

nats_cmd stream ls
