# docs/archive

Historical artifacts kept for reference but no longer informing live decisions.
Files here are frozen point-in-time snapshots, not maintained.

## Index

- `market-research-sql-engines-iceberg.md` — March 2026 evaluation of 14 open-source SQL engines for Iceberg. Informed the decision to build SQE on DataFusion. Selection complete.
- `matrix-parity-tracking-issue.md` — Tracking issue body for the iceberg-matrix-parity workstream (target: lift parity from 31% to 83%). Goal exceeded; the matrix is at 167/189 = 88%.
- `matrix-parity-workflow.md` — Branching strategy for the 8-phase iceberg-matrix-parity work (Phases A through H). All phases shipped.

## When to archive

Move a doc here when:

- It is a one-time decision artifact (market research, RFC, retro) and the decision is made.
- It tracks a workstream that is fully done.
- It references a process or version that has been superseded.

Don't archive:

- Operational reference (deployment, runbooks, troubleshooting).
- Architecture overviews.
- Audit trails worth keeping (e.g., `docs/issues.md`).
- Live design specs in `docs/specs/` or `docs/superpowers/specs/`.
