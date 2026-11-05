### Changed

- `avg(decimal(p,s))` returns `decimal(p,s)`, as in Trino (#785)

    DataFusion widened it to `decimal(p+4,s+4)`: `avg` of 0.01, 0.01, 0.00 over a `decimal(7,2)` column gave 0.006667 as `decimal(11,6)`. It is now 0.01 as `decimal(7,2)`, rounded half away from zero. `avg` keeps DataFusion's count and sum state and computes only the final division. `avg` over integers still returns a double.
