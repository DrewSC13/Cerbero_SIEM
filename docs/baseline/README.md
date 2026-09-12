# Architectural baseline policy

The authoritative CERBERO v1.0 source documents are intentionally **not stored in this Git repository**.

`docs/architecture/source-of-truth.md` records the authoritative document set and the change-discipline rules used by the implementation. The original source files remain external project inputs and must be reviewed from their approved storage location when implementation work depends on them.

Repository policy:

- do not commit the baseline PDF source documents;
- do not commit generated copies of those PDFs under another path or filename;
- do not treat repository summaries as replacements for the source documents;
- preserve `[LOCKED]` decisions through implementation-facing Markdown and ADRs;
- when a baseline source document changes, update the source-of-truth index and affected repository documentation in the same milestone.
