# Architectural source of truth

The initial CERBERO baseline consists of the following external source documents. The original PDF files are intentionally kept outside Git and are not part of the repository contents:

1. CERBERO — ARCHITECTURE v1.0
2. CERBERO — CONTRACTS v1.0
3. CERBERO — STORAGE v1.0
4. CERBERO — ANALYTICAL MODEL v1.0
5. CERBERO — DETECTION & CORRELATION v1.0
6. CERBERO — INGEST & EVENT BUS v1.0
7. CERBERO — PARSING & OCSF v1.0
8. CERBERO — API + AUTH/RBAC v1.0
9. CERBERO — SECURITY / PKI / SECRETS v1.0
10. CERBERO — TUI & QUERY v1.0
11. CERBERO — TESTING / CI / REPRODUCIBILITY v1.0
12. CERBERO — OPERATIONS / OBSERVABILITY / RETENTION v1.0
13. `Cerbero_Propuesta_de_Proyecto`
14. `Documentación fundacional`

## Repository boundary

The repository stores implementation-facing Markdown, ADRs, schemas, code, tests, migrations, configuration examples, and other implementation artifacts. It does **not** store the baseline PDF source files.

The external baseline remains authoritative. A repository summary is a convenience index and cannot silently redefine a `[LOCKED]` decision.

## Precedence and change discipline

- Explicit `[LOCKED]`/`[LOCK]` decisions are implementation constraints.
- Open implementation details may be selected only when needed; important choices are recorded in ADRs.
- A contradiction is surfaced, not silently reconciled.
- Changing a locked decision requires an ADR that states the impact and affected documentation before implementation.
- Repository summaries such as root `ARCHITECTURE.md` are indexes; they do not override the external baseline or accepted ADRs.
