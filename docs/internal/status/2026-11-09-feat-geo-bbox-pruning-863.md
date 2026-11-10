**STATUS 2026-11-09: geometry bounding-box pruning (`feat/geo-bbox-pruning-863`, #863).**

The last caveat on the `geometry-type:v3` matrix cell was "no bounding-box
pruning". `crates/sqe-catalog/src/geo_prune.rs` (modelled on
`variant_prune.rs`) reads `ST_Intersects / Contains / Within / Equals /
Touches / Crosses / Overlaps(col, const)` in either argument order and
`ST_DWithin(col, const, d)` (box grown by `d`). The column must be a bare
`geoarrow.wkb` column that is not GEOGRAPHY, so a policy `CASE` guard never
matches; the constant is the folded binary literal of `ST_MakeEnvelope`,
`ST_Point` or `ST_GeomFromText`. `ST_Disjoint` and `NOT` are refused.

- Files: the manifest lower/upper bounds of a `geometry` field (16/24/32-byte
  little-endian x,y[,z][,m] corners) are compared on both planning paths
  (`plan_static_scan` and the distributed `plan_data_files`). Metric
  `files_pruned_geo`.
- Row groups: parquet `geo_statistics().bounding_box()`, plus all-NULL
  chunks. The vendored scan builder takes one `RowGroupPruner`, so
  `ChainedRowGroupPruner` runs the variant and geo pruners in turn. Metric
  `row_groups_pruned_geo`.
- Workers: new `ScanTask::geo_prunes` (`geo_prunes_b64`), ticket version 7.
  Workers with another version are refused at registration, as before.
- The filter stays in the plan (`Inexact`) and leaves `df_filters`, which
  workers decode without the `ST_*` functions.

Matrix: `geometry-type:v3` partial -> full (159/162).

**NEXT:** #870 shredded variant: writer-side variant manifest bounds for
nested and double paths, then typed field access on read (plan first).
