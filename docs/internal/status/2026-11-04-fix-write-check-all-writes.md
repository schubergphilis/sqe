**STATUS 2026-11-04: a caller without write access is refused INSERT / DELETE / UPDATE / MERGE / TRUNCATE / ingest before any file is written (`fix/write-check-all-writes`, #880 follow-up to !1652).**

`SessionCatalog::check_write_access` runs right after the target loads in every write handler: the seven DML handlers (`handle_delete`, `_mor`, `_equality`, `handle_update`, `_equality`, `handle_merge`, `_equality`, so the dispatcher fallbacks are covered too), `handle_insert_streaming_on` and `handle_ingest_streaming`. Two halves, both as the caller:

1. Catalog, every REST catalog: a commit with `set-snapshot-ref main` to the current snapshot (no updates when there is no `main`) and an `assert-table-uuid` with the table uuid XOR `u128::MAX`. Polaris 1.7 authorizes the fine-grained op (`SET_TABLE_SNAPSHOT_REF` / `UPDATE_TABLE`) in `IcebergCatalogHandler.updateTable` before `CatalogHandlerUtils.commit` validates requirements, so a reader gets 403 and a writer 409 with nothing committed. A 200 logs an error (the catalog skips requirements) and passes; the update is a no-op even then.
2. Storage, only when the catalog vends credentials: write and delete an empty `.sqe-write-check-<nanos>` under `write.data.path` or `<location>/data` with the table's FileIO. `TABLE_WRITE_PROPERTIES` is a super-privilege of both snapshot ops, so the catalog half lets such a principal through; its vended keys are read-only and the probe's `AccessDenied` refuses.

Only a refusal refuses; anything else warns and the real commit decides. Non-refusals are cached 60 s (moka, 10k) by token fingerprint, catalog URL, warehouse and table uuid.

Verified in `dml_write_precheck_e2e`: a `TABLE_READ_DATA` principal is refused all five statements by the catalog half, a `TABLE_READ_DATA` + `TABLE_WRITE_PROPERTIES` principal by the storage half (log shows `write check at 's3://…'`), the object listing under the table location is unchanged (no probe object left), root's DELETE still commits. Earlier portal-data probe: analyst 403, engineer and admin pass on Polaris 1.7.0 + Ranger 2.8.

Limits: CTAS / CTAS OR REPLACE are not checked (the staged create is already refused by the catalog). Databricks UC was not probed; on a catalog that answers neither 403 nor 409 the check is inconclusive and behaviour is as before.

**NEXT:** probe Databricks UC's answer to the uuid-mismatch commit; the 60 s pass cache could move to the session if a revoked grant must bite sooner.
