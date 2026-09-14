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

## Durable DEVELOPMENT runtime

The first runtime uses the existing DEVELOPMENT filesystem Raw Store as read-only evidence input and ClickHouse as the normalized-event authority. Processing order is:

```text
raw.persisted
  -> validate envelope + RawEventPersisted
  -> derive logical normalization key
  -> recover existing ClickHouse row if already processed
  -> read exact raw locator bytes
  -> verify byte length + SHA-256
  -> parse + map + canonical hash
  -> synchronous ClickHouse INSERT with insert_deduplication_token=logical_key
  -> read back the authoritative stored row
  -> publish normalized.created with stable stored publication_message_id
  -> durable ACK raw.persisted
```

The ClickHouse table enables a bounded non-replicated insert-deduplication window for the DEVELOPMENT single-node topology. Application-level lookup by logical key happens before insert and the row is always read back after insert, so a retry after an ambiguous ClickHouse response converges on the already stored derivation rather than generating a new public identity.

The physical schema stores source `event_time` presence/seconds/nanos separately from `ingest_time`, so absence remains distinguishable and the normalizer never rewrites source time. No daily/monthly ClickHouse partition policy is frozen by this milestone.

`normalized.created` uses:

```text
subject        = cerbero.v1.normalized.created
message_type   = NormalizedEventCreated
payload_schema = cerbero.normalized_event.v1
payload        = NormalizedEvent
```

A permanent parsing/contract error is terminated at the durable consumer rather than entering infinite retry. The fully governed `cerbero.v1.dlq.normalization` payload and retry-budget transition remain a later Milestone 4 increment; exact retry counts/backoff values remain deployment policy, not contract values.

Publication semantics are at-least-once. After a durable ClickHouse write, a redelivery may retry `normalized.created` with the same stored `publication_message_id`; the `Nats-Msg-Id` deduplication boundary is JetStream storage, not Core NATS fanout. A Core NATS subscriber can therefore observe more than one publication attempt even when JetStream stores one logical `normalized.created` message. Downstream durable consumers must remain idempotent, and CERBERO does not claim distributed exactly-once delivery. The integration gate waits for the duplicate `raw.persisted` delivery to be ACKed and then requires exactly one ClickHouse derivation row plus exactly one stored `cerbero.v1.normalized.created` message in `CERBERO_ANALYTICS`.
