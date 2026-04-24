# Iceberg matrix parity: branching and tracking

Reference for anyone working on the `iceberg-matrix-parity` openspec change.

## Feature branch naming

Each of the eight phases (A through H) plus the setup phase ships on its own feature branch and merge request. Use the prefix and the phase letter so it is obvious which phase an MR belongs to.

```
feat/matrix-phase-1-setup        # Phase 1 (this doc, the xtask skeleton, the state file)
feat/matrix-phase-a-catalog-glue
feat/matrix-phase-a-catalog-hms
feat/matrix-phase-a-catalog-sql
feat/matrix-phase-a-catalog-hadoop
feat/matrix-phase-a-unity-oidc-m2m
feat/matrix-phase-b-maintenance-rewrite
feat/matrix-phase-b-maintenance-expire
feat/matrix-phase-b-maintenance-orphan
feat/matrix-phase-b-maintenance-manifests
feat/matrix-phase-c-branching-ddl
feat/matrix-phase-c-tagging-ddl
feat/matrix-phase-d-v3-nanosec
feat/matrix-phase-d-v3-column-defaults
feat/matrix-phase-e-row-delta-action
feat/matrix-phase-e-mor-dispatch
feat/matrix-phase-f-parquet-bloom
feat/matrix-phase-f-puffin-ndv
feat/matrix-phase-g-cdc-range-scan
feat/matrix-phase-g-cdc-meta-columns
feat/matrix-phase-h-mor-update
feat/matrix-phase-h-mor-merge
```

Phases A through D are parallel-safe. Phases E through H have the dependencies documented in `openspec/changes/iceberg-matrix-parity/design.md`.

## One merge request per branch

Small MRs reviewed quickly. No mega-branches. If a phase turns out to need more than two or three MRs, split it further.

## Matrix state file updates

Every MR that changes support level SHALL update `docs/iceberg-matrix-state.json` in the same commit. CI job `matrix-score` asserts the computed percentage is at or above `MATRIX_MIN_PERCENT` (see `.gitlab-ci.yml`). Update `MATRIX_MIN_PERCENT` when a phase tag ships.

Planned baseline steps:

| Tag                | MATRIX_MIN_PERCENT | Phase |
|---|---:|---|
| v0.15.0 (now)      | 30 | baseline |
| v0.16.0-catalogs   | 43 | after Phase A |
| v0.17.0-maintenance | 50 | after Phase B |
| v0.18.0-branching  | 56 | after Phase C |
| v0.19.0-v3-types   | 65 | after Phase D |
| v0.20.0-row-deltas | 70 | after Phase E |
| v0.21.0-puffin     | 73 | after Phase F |
| v0.22.0-cdc        | 75 | after Phase G |
| v0.23.0-mor        | 83 | after Phase H |

The percentages are rounded. Actual values come out of `cargo xtask matrix-report`.

## Running the matrix report

```bash
# Print current score
cargo xtask matrix-report

# Fail if below threshold
cargo xtask matrix-report --min-percent 30

# Point at a non-default state file
cargo xtask matrix-report --path some/other/state.json
```

## Submission to the public matrix

After Phase A tags, open a PR to [Neuw84/iceberg-matrix](https://github.com/Neuw84/iceberg-matrix) per the checklist in `openspec/changes/iceberg-matrix-parity/tasks.md` section 2.22 through 2.26. Update the PR after every subsequent phase tag.
