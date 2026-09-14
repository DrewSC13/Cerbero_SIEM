# Milestone 4 first normalization vertical

The first executable normalization path is intentionally narrow and forensic-first:

```text
cerbero.v1.raw.persisted
  -> verify durable raw locator bytes and SHA-256
  -> parser registry selects linux/sshd@1
  -> ParsedEvent
  -> mapping registry selects linux.ssh.authentication@1
  -> OCSF 1.9.0 Authentication / Logon
  -> canonical OCSF JSON + SHA-256
  -> NormalizedEvent + Transformation provenance
```

The governed fixture is:

```text
Failed password for invalid user admin from 10.0.0.8 port 50341 ssh2
```

Expected normalized semantics include:

```text
category_uid     = 3
class_uid        = 3002
activity_id      = 1
status_id        = 2
user.name        = admin
src_endpoint.ip  = 10.0.0.8
src_endpoint.port= 50341
auth_protocol_id = 99
auth_protocol    = SSH
```

`NormalizedEvent` is a derivation and never replaces `RawEvent`. `normalized_hash` is SHA-256 over the canonical JSON bytes of `ocsf_event` only. Mapping identity/version and parser identity/version participate in logical derivation identity so intentional re-normalization produces history rather than overwrite.

When `event_time` is unavailable, the mapping does not mutate or synthesize RawEvent metadata. It may use `ingest_time` as the required OCSF `time` fallback only while marking the output `PARTIAL` and recording `metadata.cerbero_time_source = ingest_time_fallback`.

The runtime/durability increment that follows this core is responsible for ClickHouse persistence, stable duplicate recovery, `normalized.created`, durable ACK/retry/isolate handling, and real NATS/ClickHouse integration.
