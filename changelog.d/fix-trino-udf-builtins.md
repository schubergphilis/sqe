### Fixed

- `arbitrary()` is now a real aggregate
  `arbitrary(x)` was a scalar function that returned the first non-NULL
  value of the whole batch, so `SELECT k, arbitrary(v) FROM t GROUP BY k`
  failed to plan and other uses gave one value for every group. It is now
  an alias of DataFusion's `any_value` aggregate, which matches Trino: one
  non-NULL value per group, NULL when the group has none.
