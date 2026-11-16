**STATUS 2026-11-16: `CREATE OR REPLACE VIEW` keeps other engines' texts (`fix/view-replace-keeps-dialects-900`).**

#900, part 10.1. A replace on a view whose *current* version has a representation in a dialect other than `sqe` is refused (`NotSupported`). The error names each dialect it would drop and the opt-in. `WITH (drop_dialects = 'spark, trino')` lets the replace go ahead for the named dialects.

- `crates/sqe-catalog/src/rest_catalog.rs`: `create_or_replace_view` takes `drop_dialects`. `refuse_dropping_dialects` checks `existing.current_version()` before any commit, comparing dialects case-insensitively.
- `crates/sqe-write/src/catalog_ops.rs`: `view_drop_dialects` parses the option from the `WITH`/`OPTIONS` list, and `view_options` is shared with `view_properties`. `view_properties` reads only `sqe_view_security`, so the option is never persisted.
- Finding from the gate: Polaris (the Iceberg Java view builder) already refuses a commit that loses a dialect, but as an HTTP 500 (`Cannot replace view due to loss of view dialects (replace.drop-dialect.allowed=false)`). So the issue's "nobody is told" was really "told with an opaque 500, and no way through". With the opt-in, the replace commit also sets `replace.drop-dialect.allowed = true`. A second, best-effort commit removes it again (a warning is logged if that fails), unless the view already carried it. Otherwise another engine's later replace could drop SQE's text unasked.
- A replace whose definition equals an existing version still re-points to it (`view_replace_updates`). The check runs before that, so the opt-in is needed there too.

Gate:
- `cargo test -p sqe-catalog -p sqe-write --lib`: all passed. The new tests are `replace_refuses_to_drop_spark_representation`, `replace_with_drop_dialects_writes_sqe_only`, `replace_names_only_the_uncovered_dialects`, `replace_keeps_an_existing_drop_dialect_property` and `view_drop_dialects_is_parsed_and_not_a_property`.
- `make test-integration FILTER=view_replace_`: 2 passed, exit 0. `view_replace_keeps_spark_text_unless_dropped` commits a `spark` text through raw Polaris REST, then covers all four acceptance tests and checks the waiver property is gone. `views_over_views_and_same_schema_replace` also runs and passes.
- `cargo clippy -p sqe-catalog -p sqe-write --all-targets -- -D warnings`: exit 0.

Left open: part 10.2 (SQE writing a `spark` representation) waits on this.

**NEXT:** #900 part 10.2: write a `spark` representation on replace.
