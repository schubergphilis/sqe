**STATUS 2026-10-23: Iceberg matrix pages re-rendered on one scale (`docs/iceberg-matrix-refresh`, #855).**

- `render-iceberg-matrix-compare.py` rewritten for upstream's per-platform tree (it still fetched the old `oss.json` / `aws.json` files). It scores every upstream engine on the rubric's 54 applicable cells and writes `docs/evidence/iceberg-matrix-rubric.json` (upstream `d14d0f1341`, MIT).
- `render-iceberg-matrix.py` takes features, categories and peers from that snapshot and refuses to render if the state file and rubric disagree on the cells. On the same 54 cells SQE (self-rated, 150) leads AWS EMR 8.1 and Starburst (147) and Dremio (146); OSS Spark 4.1 is 138. The old page ranked SQE below EMR's 180/189 from a 2026-04 snapshot.
- State file: rubric pin moved to `d14d0f1341` (features.json byte-identical to `2dd75e0aa2`; the new upstream commits add Dremio and catalog-side entries), `generatedAt` 2026-10-23.
- Book index, roadmap and Iceberg feature page: 147/162 -> 150/162 (92.6%). `roadmap-tracker.md` no longer claims 167/189, fifth.

**NEXT:** run the leak-gated site sync from a clean main after this merges (the published comparison should use the same 54-cell scale), then #866 (Nessie / HMS / JDBC live tests).
