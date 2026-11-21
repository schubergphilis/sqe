**STATUS 2026-11-21: tag policies scoped to a catalog (`feat/catalog-scoped-tag-policies-919`, #919).**

- Under `[policy.ranger] catalog_scoped`, a column of catalog `c` tagged `pii` resolves tag policies on `pii` and `c.pii` (`crates/sqe-policy/src/tag_scope.rs`).
  - `c.pii` governs catalog `c` only.
  - Bare `pii` keeps applying everywhere, so the setting never unmasks.
  - The catalog's mask wins over the bare one. Filters from both apply. A tag unmappable under either name restricts.
- Column tags stay bare. The catalog lives in the policy's tag value, because Ranger's `tag` service definition has only a `tag` resource, and Spark evaluates neither a new resource nor a policy label.
- `project_table_tags` writes both names on the scoped resource, so Spark applies the same policies.
- Every caller of `resolve_effective_policy` now passes the table's catalog.
- Pre-existing gap fixed on the way: `table_files` / `table_partitions` keyed their policy on the namespace only, even under `catalog_scoped`. They now key `<catalog>.<namespace>` on the TVF's catalog argument, falling back to the session's.
- OpenSpec: `openspec/changes/catalog-scoped-tag-policies/`.

Gate:
- `cargo test -p sqe-policy -p sqe-catalog --lib --test it`: exit 0 (859, 249, 354, 103 passed).
- New: `tag_scope::tests::*` (3), `tag_projector::tests::scoped_projection_adds_the_catalog_qualified_tag`, `ranger_store_wiremock::catalog_scoped_tag_policy_through_the_rewriter_covers_one_catalog` (real `RangerStore` over HTTP, through the rewriter: scoped, bare, off).
- `cargo test -p sqe-write --lib -- tag`: exit 0, 43 passed.
- `cargo clippy -p sqe-policy -p sqe-catalog -p sqe-coordinator --all-targets -- -D warnings`: exit 0.
- `make test-integration FILTER=tag`: exit 0, 26 passed (live Ranger tests skip there).
- `scripts/access-control-test.sh catalog_scoped_tag` against the live Ranger stack: exit 0, 2 passed. New: `access_control_e2e::catalog_scoped_tag_policy_masks_its_catalog_only`. The projector imports `sales_wh.PII` into Ranger. A tag mask policy on `ops_wh.PII` leaves `sales_wh` raw, and one on `sales_wh.PII` masks it. An unscoped handler matches neither.

Left open:
- The live test creates the qualified tag policy through the Ranger REST API. `CREATE POLICY ... ON TAG <catalog>.<tag>` in SQL is not run live.
- Projector resources written before this change carry only the bare tag. They pick up the qualified name on the next tag write or `CALL reproject_column_tags`.
- `SET MASKING POLICY` with a qualified name is not stripped to the bare tag. It still masks, because the bare lookup matches.
- Rewriter metadata-TVF access gate (`plan_rewriter.rs`, metadata scan branch) takes the catalog from the session, not from the TVF's catalog argument. That is the next branch.

**NEXT:** fix the metadata-TVF gate catalog keying, then #920 part 1.
