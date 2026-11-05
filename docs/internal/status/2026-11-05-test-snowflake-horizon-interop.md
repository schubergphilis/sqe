**STATUS 2026-11-05: Snowflake Horizon interop harness, matrix cells measured, blog (`test/snowflake-horizon-interop`, stacked on `fix/snowflake-horizon-interop` !1660, needs `fix/parquet-delta-miniblock-skip` !1662).**

Adds `scripts/interop/snowflake-horizon/` (phases A-F, modelled on the
Databricks UC harness) and the results of the 2026-11-05 run: 40 of 40 reads
equal in embedded, coordinator-local and distributed mode (the worker scanned
all 40). The engine fixes the run needed are in !1660 and !1662; this branch
carries the harness and the docs.

- Iceberg matrix: `snowflake-horizon-catalog` v2 and v3 move from partial to
  full (158/162, 97.5%). State JSON, rendered matrix, compare page, README,
  roadmap tracker and book pages updated.
- Book: Snowflake Horizon row in the catalog table and a "Snowflake Horizon
  interop" section in `features/iceberg.md`.
- Blog: `docs/site/blog/2026-11-05-snowflake-and-sqe-read-each-others-tables.md`.
- Snowflake delete files: small tables are always copy-on-write, so the
  harness adds `SF_RUN_MOR_V2` / `SF_RUN_MOR_V3` (1M rows,
  `ICEBERG_MERGE_ON_READ_BEHAVIOR = 'enabled'`). Snowflake's DELETE writes a
  position delete file (V2) and a deletion vector (V3); seven `mor_*` compare
  rows check SQE applies them. Reading them needs parquet 59.3.2 (!1662).
  `sqe_v3_mor` covers the reverse: Snowflake applies SQE's DV.
- The Databricks harness reran green on the same build (27/27, all modes,
  with !1660's fill change and parquet 59.3.2). The scratch catalog there is
  now `pg3-bronze`.

Open: `$files` lists data files only (deletes show in `$snapshots`); a
`content` column and delete-file rows are a separate change. Case folding
(#337) makes Snowflake names need quotes; a follow-up issue is drafted, not
filed. Leftover `P_*` / `SF_HELLO*` tables in the scratch schema predate the
harness and can be dropped by hand.

**NEXT:** merge !1660 and !1662 first, then this MR.
