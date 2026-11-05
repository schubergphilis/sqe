**STATUS 2026-11-05: Snowflake Horizon interop harness, matrix cells measured, blog (`test/snowflake-horizon-interop`, stacked on `fix/snowflake-horizon-interop` !1660).**

Adds `scripts/interop/snowflake-horizon/` (phases A-F, modelled on the
Databricks UC harness) and the results of the 2026-11-05 run: 33 of 33 reads
equal in embedded, coordinator-local and distributed mode. The engine fixes
the run needed are in !1660; this branch carries the harness and the docs.

- Iceberg matrix: `snowflake-horizon-catalog` v2 and v3 move from partial to
  full (158/162, 97.5%). State JSON, rendered matrix, compare page, README,
  roadmap tracker and book pages updated.
- Book: Snowflake Horizon row in the catalog table and a "Snowflake Horizon
  interop" section in `features/iceberg.md`.
- Blog: `docs/site/blog/2026-11-05-snowflake-and-sqe-read-each-others-tables.md`.
- Snowflake DML is copy-on-write on V2 and V3, so SQE reading a Snowflake
  deletion vector stays untested. `sqe_v3_mor` covers the reverse: Snowflake
  applies SQE's DV.
- The Databricks harness reran green on the same build (27/27, all modes).
  The scratch catalog there is now `pg3-bronze`.

Open: `$files` lists data files only (deletes show in `$snapshots`); case
folding (#337) makes Snowflake names need quotes; workers older than scan
ticket V4 refuse projected scans; the unprojected scan path still sends no
fill. Leftover `P_*` / `SF_HELLO*` tables in the scratch schema predate the
harness and can be dropped by hand.

**NEXT:** merge !1660 first, then this MR; follow-up for the unprojected
no-fill worker path.
