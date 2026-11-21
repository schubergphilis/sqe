**STATUS 2026-11-21: refuse undefined tag names (`feat/tag-names-ranger-tagdefs`, #926).**

New setting `[access_control] tag_names = "any" | "ranger-tagdefs"` (default `any`).

- `PolicyStore::tag_definitions(fresh)`: trait default errors (only Ranger can list tag definitions). `RangerStore` reads `GET /service/tags/tagdefs` (a bare JSON array of `RangerTagDef`, global to the Ranger instance, not per tag service, not paged), caches the names for `cache_ttl_secs`, single-flights concurrent misses, drops the cache in `invalidate_all`, and keeps fetch failures off the policy circuit breaker.
- `require_tag_authority`: after the privilege and column checks, every tag a `Set` op names must be defined; a miss in the cached list is re-read once before the refusal. Undefined -> `InvalidArguments` "tag 'x' is not defined in Ranger"; list unreadable or no store -> 403 "cannot check the tag names".
- Raw `sqe.column-tags` writes (`SET TBLPROPERTIES`, `CREATE ... TBLPROPERTIES`, plain `CREATE TABLE`): only the tag keys a column gains are checked, under both `admin-role` and `ranger-delegate`.
- `catalog_scoped`: `<catalog>.<tag>` (the name the tag projector writes, as in !1888) also defines `<tag>` for a table of that catalog; the exact name still matches.
- Parser fix found on the way: the native `SET TAGS (col = ('k' = 'v'))` list has no value syntax, and main stored the junk key `k' = 'v`. It is now refused with a pointer to the `ALTER COLUMN ... SET TAGS ('k' = 'v')` form.
- Not checked: `SET MASKING POLICY` (its tag comes from an existing policy), UNSET, admins.

Gates: `cargo test -p sqe-core --lib tag_names` (1), `cargo test -p sqe-policy --lib ranger_store` (106), `cargo test -p sqe-sql --lib tags` (44), `cargo test -p sqe-coordinator --lib tag` (9), `cargo check-server`, `cargo test -p sqe-coordinator --test it tag` (35 passed, 26 ignored, 7 new), clippy `-D warnings` on the four crates. Not run: an e2e against the compose Ranger (the parser was written against a body captured from it).

**NEXT:** object-level tags (#922: SET/UNSET TAGS on tables, schemas and catalogs) so the `table_tags` / `schema_tags` / `catalog_tags` views of !1890 return rows; those writes should take the same name check.
