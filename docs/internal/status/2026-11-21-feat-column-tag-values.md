**STATUS 2026-11-21: column tag values (`feat/column-tag-values`, stacked on `feat/databricks-column-tag-syntax`).**

New `crates/sqe-core/src/column_tags.rs` owns the `sqe.column-tags` shape.
A list entry is a string (key only) or a one-entry object `{"key": "value"}`;
an object with zero or several entries is an error, so the property reads as
unknown tag state and fails closed. `parse_keys` gives the old key-only map,
and every policy-facing reader (TagSource, scan tags, Hive manifest,
`iceberg_metadata` TVF, Ranger projection, SHOW STATS) uses keys, so policy
behaviour does not change.

`ColumnTagOp` carries `values` (key -> value). The Databricks and Snowflake
parsers fill it. `apply_tag_ops` sets by key and replaces the value,
`rewrite_tag_keys` keeps the target's value on a rename union, and
`changed_tag_columns` compares (key, value) pairs, so a value-only change
needs tag authority. `'k' = ''` is now a key-only tag in the Databricks form
(it was an error in the first MR); `'k' =` with nothing after it is still an
error.

The values live in `sqe.column-tags` itself, not in a sibling property: the
cross-engine tests parse that property, and the portal's Iceberg gate guards
only that key. #920 (field-id keying) can move the value into its per-field
entry.

Mixed versions: an older SQE fails closed on a table with a value. The
portal's `tags.py column_tags` must read both shapes before tag values are
written there.

Not done: SHOW TAGS still lists keys only; the `information_schema` tag
views (next MR) show values.

Tests: `cargo test -p sqe-core -p sqe-sql -p sqe-write -p sqe-catalog --lib`
and `cargo test -p sqe-coordinator --lib tag`, all passing. No compose stack.

**NEXT:** `information_schema` tag views (`column_tags` with `tag_value`).
