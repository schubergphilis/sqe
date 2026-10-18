**STATUS 2026-10-18: Iceberg matrix rescored against the current upstream rubric, 144/162 (88.9%) (`docs/iceberg-matrix-rescore`).**

`docs/evidence/iceberg-matrix-state.json` is rebuilt against upstream `features.json` (rubric commit `2dd75e0a`, 32 features). Only cells that apply to a format version are scored, as the site shows them: position-deletes is V2-only now, so 54 cells, max 162. The old 167/189 is not comparable.

- Removed upstream and dropped here: hive-metastore, nessie, polaris, hadoop-catalog, jdbc-catalog, vector-type, cdc-support.
- New: deletion-vectors (V3 full), unknown-type, google-lakehouse (partial).
- Gains: deletion vectors and row lineage on V3, unshredded VARIANT (partial; evidence now includes the Spark 4.1 interop and worker-path tests from #849).
- Downgrade: unity-catalog to partial (read-only smoke).
- Platform: `SQE (0.51.0)`, docUrl `https://docs.getsqe.com`.

Every evidence path and test symbol in the state file resolves on main. `docs/internal/design-archive/iceberg-matrix.md` is regenerated; `README.md`, the book index and `features/iceberg.md` carry the new score. The "fifth on the scoreboard" rank is dropped: it was measured on the old rubric. `scripts/render-iceberg-matrix-compare.py` was not rerun (its `gh api` call against the upstream repo 404s), so `iceberg-matrix-compare.md` still shows the old comparison.

**NEXT:**
- Refresh the docs.getsqe.com pages the docUrl points at before submitting the state upstream.
- Rerun `render-iceberg-matrix-compare.py` once its upstream fetch works, and re-derive the public rank.
- Variant: typed path access (`variant_get`), a shredded-file read test, compaction of a variant table.
