**STATUS 2026-11-22: a tag write needs USE on the schema first (`fix/tag-write-needs-use-941`, #941, openspec tag-authority-apply-tag 6.x).**

Under `tag_authority = "ranger-delegate"` a non-admin column, table or schema tag write now first asks `PolicyStore::grants_schema_use` for the schema's policy key. In Ranger that is a `select` (or `all`) allow on any table of the schema; a `select` deny on the whole schema refuses whatever else is allowed, a table or column deny does not, and the last-component fallback only ever narrows. `apply_tag` / `remove_tag` / `alter` alone do not count. Under `catalog_scoped` the key is `<catalog>.<ns>`, which makes it the catalog check too; without it Polaris decides the catalog on the load. Catalog tags keep their gate (Polaris). A store error refuses. The commit-lock re-check (`TagWriteAuth`) re-checks column privileges only; USE is not re-checked there.

Tests run: `cargo test -p sqe-policy --lib ranger_store` (Ranger unit: deny over allow, other schema, apply_tag alone, unscoped path) and the coordinator `grant_introspection_gate_test` with `test-sqlite`. Not run: the live Ranger suite (`make test-access-control`); its stewards already hold `select` through `GRANT SELECT` and the quickstart's `public` item.

Owner option, not built: require `select` on `table *` (the whole schema) instead of on any one table.

**NEXT:** live Ranger acceptance for a steward with `apply_tag` and no `select` (openspec 6.4); owner decision on the stricter `table *` rule.
