# Git workflow

## Initial repository

Milestone 0 is developed on `feature/repository-bootstrap` and then fast-forwarded locally to `main` only after the bootstrap commits are complete. This is a one-time pre-remote bootstrap flow; after `main` is published, significant work uses topic branches and review.

The verified GitHub owner is `DrewSC13`, and the intended remote is:

```text
https://github.com/DrewSC13/cerbero.git
```

The connected GitHub capability can operate on existing repositories but does not expose repository creation. Therefore the remote repository must exist before the first push. Once it exists:

```bash
make ci
make integration
make remote-readiness
git remote add origin https://github.com/DrewSC13/cerbero.git
git push -u origin main
```

Never rewrite published `main` history merely to reconcile a local bootstrap assumption.

## Branches

```text
main
feature/<name>
fix/<name>
docs/<name>
refactor/<name>
security/<name>
```

Use a branch for significant work. `main` is stable and reproducible.

## Commit format

```text
<type>(<scope>): <description>
```

Types:

```text
feat fix refactor perf test docs build ci chore security revert
```

One commit should represent one logical purpose. Do not mix unrelated cleanup, feature changes, migration changes, and docs unless they are necessary parts of the same functional change.

## Before every important push

Record and review:

```text
branch actual
commits a enviar
tests ejecutados
estado de CI/local checks
documentación actualizada
```

Commands:

```bash
git branch --show-current
git log --oneline --decorate origin/main..HEAD   # after a remote exists
git status --short
git diff --check
make ci
make integration                                # when Docker-backed boundaries changed
```

Then push the current topic branch:

```bash
git push -u origin <branch>
```

`make remote-readiness` treats tracked, staged, and untracked files as a dirty working tree. Local build artifacts must never be silently ignored by the publication gate.

No force-push to `main`; no history rewriting of published `main`; published release tags are immutable.

## Branch protection

Once the GitHub remote exists, protect `main` with successful CI, required review(s), and CODEOWNERS-sensitive paths. The exact reviewer count remains an open project-governance parameter until explicitly selected.
