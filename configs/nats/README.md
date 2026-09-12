# NATS JetStream development configuration

This configuration implements the v1 subject/stream boundaries and distinct development identities for planned consumers. Credentials are injected from `.env` and are DEVELOPMENT ONLY.

The stream topology is created by `scripts/dev/nats-bootstrap.sh` only after the server is reachable. Development byte limits are deployment safeguards, not CERBERO retention policy.
