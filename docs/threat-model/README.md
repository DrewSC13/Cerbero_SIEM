# CERBERO threat model — implementation view

Status: Step34 MVP-closure implementation view derived from Architecture v1.0 and
Security/PKI/Secrets v1.0. Methodology: **STRIDE**.

## Trust boundaries

| ID | Boundary | Primary concern |
| --- | --- | --- |
| TB-01 | External Sources | All payloads are untrusted even when the source is authenticated. |
| TB-02 | Endpoint Agent | Per-agent identity and compromise containment. |
| TB-03 | Ingestion Gateway | Authentication/authorization, limits, exact-byte RawEvent construction. |
| TB-04 | Event Bus | Message identity, schema/version, durable delivery, replay and deduplication. |
| TB-05 | Processing Services | Parser/mapping/rule inputs remain untrusted; fail closed. |
| TB-06 | Storage Layer | Raw immutability, tenant boundaries, least privilege, integrity. |
| TB-07 | Control Plane | Explicit authentication/authorization and append-only audit. |
| TB-08 | Analyst Environment | TUI/API boundary; no direct database or privileged bypass. |

## STRIDE register

| Category | Representative CERBERO threats | Baseline mitigations/evidence |
| --- | --- | --- |
| Spoofing | forged source/service identity | service identities; source auth hooks; production mTLS remains explicit production hardening |
| Tampering | changed raw evidence, bus payload or analytical inputs | SHA-256 raw hash; immutable RawEvent; canonical normalized hash; versioned contracts |
| Repudiation | unaudited case/rule/admin actions | append-only audit model; actor/request IDs; state-machine audit tests |
| Information Disclosure | raw evidence, secrets, tenant data or internal errors exposed | tenant scoping; API boundary; secret checks; controlled raw/export model |
| Denial of Service | oversized payloads, expensive rules/queries, parser abuse, queue pressure | size/rate/query limits; bounded rules; backpressure; parser limits; DLQ |
| Elevation of Privilege | TUI/clients bypass API/RBAC or services overreach storage permissions | TUI→API invariant; least-privilege DB/NATS identities; deny/fail-closed boundaries |

CERBERO additionally treats event forgery, replay, parser exploitation,
detection poisoning, time manipulation and supply-chain compromise as explicit
threats. Existing duplicate/replay tests, parser validation, STABLE-rule tests,
lockfiles and image digests provide current evidence.

## Known residual post-MVP risks

1. Production PKI/mTLS, enrollment/revocation and complete production RBAC are not implied by the DEVELOPMENT acceptance profile.
2. UDP syslog and additional syslog framing modes are not enabled by the MVP transport contract.
3. Production secret delivery, production Raw Store provider selection and retention values remain deployment/operations work.
4. Release signing/SBOM technology remains governed release work and is not claimed by Step34.

MVP completion means the frozen functional Definition of Done is demonstrated; it does not convert these residual deployment/security items into completed controls.


## Step34 source-boundary delta

The existing source→ingest trust boundary now has executable TCP syslog and host-journald frontends. RFC6587 length parsing is treated as untrusted input and is bounded by frame-size, timeout, connection-rate and concurrency limits before exact bytes enter RawEvent. Journald export parsing is likewise bounded and byte-safe; its durable cursor advances only after JetStream acceptance, favoring at-least-once replay over silent loss. The runnable source profiles remain DEVELOPMENT-only with static configured identities. Production mTLS/enrollment, certificate revocation and secret delivery remain residual deployment/security work and are not implied by MVP completion.
