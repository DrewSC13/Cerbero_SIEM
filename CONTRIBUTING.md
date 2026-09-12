# Contributing to CERBERO

CERBERO changes must preserve correctness, traceability, reproducibility, and reviewability. Code and documentation evolve together.

## Branches

`main` represents stable, reproducible state. Significant work uses one of:

```text
feature/<name>
fix/<name>
docs/<name>
refactor/<name>
security/<name>
```

Do not force-push `main`, rewrite published `main`, or move a published release tag.

## Commits

Use Conventional Commits:

```text
<type>(<scope>): <description>
```

Allowed baseline types:

```text
feat fix refactor perf test docs build ci chore security revert
```

Commits should be atomic, coherent, reviewable, and functionally complete for one logical purpose.

Examples:

```text
chore(repo): initialize cerbero monorepo
feat(contracts): add raw event protobuf contract
fix(normalizer): preserve source timestamp on partial parsing
security(pki): enforce agent certificate validation
```

## Architectural changes

Do not silently change a `[LOCKED]` decision. For a new important architectural decision, contradiction, or required change to a locked decision:

1. identify the decision and impact;
2. add/update an ADR in `docs/adr/`;
3. update affected architecture/security/operations/testing documentation;
4. implement only after the decision is recorded.

## Required checks

For code-bearing changes, run the applicable subset and normally the full local gate:

```bash
make verify
make ci
```

For infrastructure changes, also run:

```bash
make integration
```

For analytical milestones, the required E2E gates expand as real pipeline stages exist. Tests must not be disabled merely to obtain green CI.

## Documentation rule

Every completed milestone/module updates its affected Markdown documentation in the same logical change. Known stale documentation is a defect.

## Security

Never commit operational secrets, private keys, production tokens, or production credentials. `.env.example` values are explicitly DEVELOPMENT ONLY.

## Pull requests

A PR should explain purpose, architecture/ADR implications, tests actually executed, documentation changed, and any deferred work. Critical paths are governed by `CODEOWNERS` once a real repository owner is configured.

See `docs/development/git-workflow.md` for the exact pre-push checklist.
