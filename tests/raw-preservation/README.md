# Raw preservation tests

Milestone 3 preservation-core tests live with the Go service so `make go-check` discovers them automatically.

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

These are component tests for orchestration semantics. PostgreSQL migrations, filesystem/object Raw Store implementation, JetStream durable-consumer wiring, actual ACK/NAK, DLQ, and crash-injection integration remain later M3 increments.
