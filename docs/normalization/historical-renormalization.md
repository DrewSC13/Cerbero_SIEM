# Milestone 4 historical renormalization and parser test expansion

CERBERO preserves normalization history rather than rewriting it.

## Governed parser-v2 proof

The exact first vertical RawEvent can now be interpreted with two governed parser versions:

```text
Failed password for invalid user admin from 10.0.0.8 port 50341 ssh2
```

`linux/sshd@1` preserves the original Step 13 interpretation:

```text
message = SSH authentication failed for user admin from 10.0.0.8
```

`linux/sshd@2` improves the derived message without changing the underlying RawEvent:

```text
message = SSH authentication failed for invalid user admin from 10.0.0.8
```

The semantic change intentionally increments `parser_version`. Mapping identity remains `linux.ssh.authentication@1` and OCSF remains `1.9.0`.

## Derivation identity

The logical key is derived from the identities actually executed:

- raw_event_id;
- parser_id and parser_version;
- mapping_id and mapping_version;
- OCSF version;
- pipeline version;
- effective configuration hash;
- execution mode.

The configuration hash likewise includes the actual parser/mapping versions. The default v1 formula remains byte-compatible with the previous first vertical.

## Runtime history proof

The integration gate demonstrates on one immutable R1:

```text
LIVE   + linux/sshd@1 -> N1
REPLAY + linux/sshd@2 -> N2
TEST   + linux/sshd@1 -> N3
```

After all three passes:

- ClickHouse contains three historical derivations for R1;
- N1's normalized_event_id, logical_key, normalized_hash, and configuration_hash remain intact;
- N2 has parser_version=2 and a different normalized hash/key;
- TEST has a distinct execution-mode derivation;
- REPLAY and TEST durable consumers are removed only after clean shutdown;
- the LIVE durable consumer remains.

## Golden and fuzz/property regressions

Versioned golden fixtures pin the canonical OCSF JSON for sshd v1 and v2. The test suite also runs a deterministic arbitrary-byte fuzz smoke and a tracked corpus against both governed SSH parser versions, asserting:

- parser registry never panics for arbitrary input;
- raw bytes are never mutated;
- source-specific size-boundary input remains contained.

These tests are reproducible CI evidence. They do not claim to replace future coverage-guided fuzzing; a dedicated coverage-guided harness remains M4 hardening work.
