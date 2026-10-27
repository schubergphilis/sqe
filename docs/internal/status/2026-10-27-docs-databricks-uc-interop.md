**STATUS 2026-10-27: Databricks Unity Catalog interop run, both directions, 25/25 in every SQE mode (`docs/databricks-uc-interop`).**

Adds `scripts/interop/databricks-uc/`: a Python driver plus SQL phases where Databricks and SQE each write Iceberg tables in one Unity schema, run DML on the other's tables, and compare 25 reads value by value. Live run on 2026-10-27: embedded 25/25, coordinator 25/25, coordinator + worker 23/25 before !1570 and 25/25 after (workers ignored delete files). Matrix `unity-catalog` v2/v3 partial -> full, score 154 -> 156/162 (rubric pin moved to 027f1513, peer rows only). Book (iceberg, catalogs, worker, roadmap, index), README, ebook 16c and a blog post updated. worker.md, iceberg.md and the ebook describe !1570's behaviour, so !1570 merges first.

Gaps recorded, not fixed: embedded CLI has no DDL on an attached catalog; DESCRIBE shows GEOMETRY as varbinary; SHOW CREATE TABLE echoes Unity's internal properties and no clustering; no ST_SRID. The run is manual (needs a workspace token, ~1h lifetime).

**NEXT:** carry delete files in the worker scan ticket so merge-on-read tables with live deletes distribute again; then DDL on attached catalogs in the embedded CLI.
