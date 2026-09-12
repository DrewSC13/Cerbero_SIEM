# CERBERO security policy

Security is a structural requirement from the first milestone, not a post-MVP retrofit.

## Current support status

CERBERO is in pre-release development. There is no supported production release yet. Security defects in `main` are still treated as first-class defects.

## Reporting a vulnerability

Do not disclose a suspected vulnerability, leaked secret, private key, or exploitable detail in a public issue.

Because Milestone 0 does not invent a GitHub organization, security email address, or remote repository, the final private reporting channel cannot be truthfully named yet. When the remote is created, repository security settings must enable a private vulnerability-reporting path and this section must be updated in the same change.

Until then, report privately to the project owner through the private collaboration channel used to provide this repository.

## Baseline controls

- least privilege and separate service identities;
- mTLS for managed agents and service-to-service trust-boundary crossings when applicable;
- deny-by-default authorization and tenant isolation;
- no operational secrets in Git or logs;
- rotatable/revocable credentials;
- exact raw-byte hashes and provenance;
- dependency locks and pinned development container digests;
- static/security tests and secret scanning;
- SBOM and signed release gates before a distributable release.

## Development credentials

`.env.example` contains known local credentials marked DEVELOPMENT ONLY. `make dev-init` copies that file to ignored `.env` with restrictive permissions. These values are prohibited in production.

See `docs/security/development-secrets.md`.
