**STATUS 2026-11-21: main compiles again after #925 crossed #919 and !1891 (`fix/tag-views-catalog-arg`).**

- `session_context.rs`: the tag-view access closure passes `Some(&catalog)` to `resolve_effective_policy`, whose `catalog` argument #919 added, so catalog-scoped tag policies apply to tag-view visibility like they do to reads.
- `tag_views.rs`: `MetadataCatalogResolver` returns `(catalog, registered name)` since !1891; the view takes the registered name as `catalog_name`, so a case-folded or default catalog is reported and keyed under the name it is registered as (before: the argument's spelling, or the warehouse name without an argument).
- Test resolvers in `tag_views.rs` and `audit_resources.rs` updated to the tuple.

Gate: `cargo check-server` exit 0; `cargo clippy -p sqe-catalog -p sqe-coordinator -p sqe-cli -p sqe-policy --all-targets -- -D warnings` exit 0; `cargo test -p sqe-catalog --lib tag_views` 9 passed; `cargo test -p sqe-coordinator --lib -- audit_resources tag_view metadata_tvf` 10 passed.

Not run: the live tag-view integration tests.

**NEXT:** merge this first; branches cut from ca59929b do not compile until it lands.
