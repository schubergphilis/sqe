## 0. Decision

- [ ] 0.1 Owner confirms D3 (refresh as caller; definer or admin only) or picks impersonation

## 1. Parsing and dispatch

- [ ] 1.1 Pre-parse `REFRESH MATERIALIZED VIEW` and `SHOW CREATE MATERIALIZED VIEW` in `sqe-sql`
- [ ] 1.2 Extend `classify_materialized_view` (`query_handler.rs:5938`); remove the DROP no-op and the CREATE refusal

## 2. Storage and refresh

- [ ] 2.1 Property names per apache/iceberg#11041 (D1); `DROP TABLE` / `DROP MATERIALIZED VIEW` cross-refusal
- [ ] 2.2 CREATE: plan, policy probe (D4), write via the one-commit replace path
- [ ] 2.3 REFRESH: pin source snapshots, read at them, replace commit with summary fields (D2); definer/admin check (D3)
- [ ] 2.4 `SHOW CREATE MATERIALIZED VIEW`

## 3. Metadata, lineage, dbt

- [ ] 3.1 `build_materialized_views_table` rows + `freshness` + `sources`; update its doc and test (`system_metadata.rs:352`)
- [ ] 3.2 OpenLineage event per refresh
- [ ] 3.3 dbt-sqe `materialized_view` materialization; check `list_relations_without_caching`

## 4. Tests and docs

- [ ] 4.1 Integration: acceptance 2-4; Spark interop test for acceptance 1
- [ ] 4.2 Ranger e2e: MV over a masked source refused
- [ ] 4.3 Book: SQL reference page for the four statements
