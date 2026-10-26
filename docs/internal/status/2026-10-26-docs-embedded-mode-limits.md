# docs/embedded-mode-limits

**Date:** 2026-10-26

## What changed

`docs/site/book/src/features/embedded.md` rewritten to what `sqe-cli --embedded`
runs, checked against a local probe (`/tmp/emb1-3.sql` against `--warehouse`)
during the Databricks playground3 testing:

- `--warehouse` is a SQLite catalog named `iceberg` (`PATH/sqe.db`), not a
  catalog-free Hadoop path scan; `--catalog-backend sqlite` does not exist.
- Removed claims: UPDATE / DELETE / MERGE, ALTER SET TBLPROPERTIES, branches,
  tags, time travel, "CTAS streams", "same SQL surface as the coordinator",
  "REST catalogs need the cluster path" (ATTACH TYPE iceberg_rest works).
- Added a statement support table (embedded vs cluster).
- CTAS and `SHOW TABLES IN` are documented as working: they land in
  `fix/embedded-ctas-show-tables`, which must merge first.

## Gate

- `make leak-scan`: clean. Forbidden-character and word greps: clean.

**NEXT:** after both MRs merge, the embedded parity issue (route embedded
statements through QueryHandler/WriteHandler) and `feat/oauth2-secret`.
