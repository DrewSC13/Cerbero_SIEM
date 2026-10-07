# ADR-0013 — Syslog TCP framing v1

## Status

Accepted

## Context

INGEST & EVENT BUS v1 requires a TCP syslog frontend to use unambiguous framing and requires the network boundary to preserve the received syslog message as raw evidence without rewriting RFC fields. The existing `syslogingest.Adapter` already enforces common IngestCore admission and durable JetStream publication after a frame exists, but it deliberately left the network framing contract open.

Step34 closes the MVP source-ingest blocker without broadening scope into multiple framing modes or UDP semantics.

## Decision

CERBERO syslog TCP v1 supports **RFC 6587 octet-counting only**.

The wire form for each event is:

```text
<decimal-octet-count><SP><exact-message-bytes>
```

The length counts only the message bytes after the single framing space. The framing prefix is transport metadata and is not part of `RawEvent.raw_payload`.

The frontend:

1. authenticates/authorizes the configured source admission before reading each frame;
2. applies explicit connection-rate, concurrent-connection, frame-size and read-timeout limits;
3. accepts only a non-empty decimal length prefix followed by one space;
4. reads exactly the declared number of bytes;
5. does not trim line endings, parse PRI/facility/severity, repair malformed syslog, or otherwise rewrite the message;
6. submits the exact frame bytes to the existing `syslogingest.Adapter`;
7. does not read the next frame on a connection until the current frame has received durable JetStream admission or failed.

A malformed or oversized frame terminates that client connection. It does not terminate the listener.

The Step34 executable runtime remains an explicit DEVELOPMENT composition with static configured source identity. This ADR defines the transport/framing behavior, not production authentication. Production mTLS/enrollment remains governed separately by the security baseline.

## UDP

UDP syslog is not enabled by v1. INGEST & EVENT BUS v1 makes UDP conditional and requires any future implementation to expose its inherently lossy semantics rather than claiming transport guarantees equivalent to confirmed TCP/durable admission.

## Consequences

- TCP framing is deterministic and testable.
- Raw evidence hashes cover the exact syslog message bytes, not the RFC6587 length prefix.
- Parser/normalizer ownership is preserved.
- Supporting non-transparent framing, newline framing, TLS syslog, or UDP requires a separate explicit contract rather than silent fallback.

## References

- `INGEST & EVENT BUS v1.0`
- `docs/ingest/README.md`
- `services/cerbero-ingest/internal/syslogingest/`
