# Raw preservation tests

Milestone 3 preservation-core tests live with the Go service so `make go-check` discovers them automatically.

M3 Step 4 adds concrete raw.persisted builder coverage:

- governed `RawEventPersisted` metadata is copied from the validated RawEvent without raw bytes;
- durable Raw Store locator fields are copied exactly into the derived payload;
- the derived envelope preserves tenant/trace/correlation and sets causation to the incoming message ID;
- `persisted_at` and envelope `emitted_at` use the raw-preserver processing clock;
- one generated UUIDv7 becomes both the outbox publication ID and envelope `message_id`;
- the validated request ID remains transport metadata on the outbox record;
- fixed identity/time inputs produce deterministic serialized envelope bytes;
- generator failures or invalid generated UUIDs fail before outbox persistence.

M3 Step 2 proves:

- invalid raw.received envelopes produce an isolate disposition before storage effects;
- transport idempotency is checked before Raw Store writes;
- exact raw bytes reach the Raw Store boundary unchanged;
- Raw Store failure is retryable and cannot create metadata/outbox state;
- locator/outbox commit occurs only after a verified durable raw write;
- durable publication occurs before ACK eligibility;
- a publish failure remains retryable and cannot mark the outbox as published;
- an already-published redelivery is ACK-eligible without duplicate storage/publication;
- a pending outbox redelivery republishes the same derived message ID and then becomes ACK-eligible;
- conflicting `(consumer_name, message_id)` state is isolated instead of silently accepted.

## PostgreSQL MetadataStore adapter

M3 Step 5B adds package-level tests around the concrete `database/sql` adapter without selecting a production PostgreSQL driver yet. The tests prove:

- `Find` reconstructs stable locator/outbox state and preserves the full `uint64` offset/length range;
- `CommitPreservation` commits raw locator, outbox, and processed-message state through one transaction;
- an already-existing critical-consumer key returns the stored record without writes;
- a concurrent loser rolls back tentative locator/outbox rows instead of leaving orphans;
- conflicting immutable raw locator metadata is rejected and rolled back;
- `MarkPublished` is repeatable and reports missing durable state rather than fabricating it.

These are adapter component tests. Real connection creation/driver selection and end-to-end raw-preserver ↔ PostgreSQL runtime wiring remain separate increments so Step 5B does not prematurely compose the service.

## PostgreSQL schema integration

`make integration` runs `scripts/tests/raw-preservation-postgres.sh` after infrastructure health succeeds. The check reapplies migration `000002_raw_preservation.sql`, verifies all three control-plane tables plus the `NOLOGIN` service role, exercises a locator + outbox + processed-message transaction under `SET ROLE cerbero_raw_preserver`, marks the outbox publication complete, rolls the fixture back, and proves the service role cannot update raw locators or delete outbox rows.

These are component tests for orchestration semantics. PostgreSQL migrations, filesystem/object Raw Store implementation, JetStream durable-consumer wiring, actual ACK/NAK, DLQ, and crash-injection integration remain later M3 increments.
