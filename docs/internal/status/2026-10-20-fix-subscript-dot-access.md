**STATUS 2026-10-20: dot access after a subscript (`fix/subscript-dot-access`).**

Found while gating #859: DataFusion 55.2 (`datafusion-sql`
`expr/mod.rs` `sql_compound_field_access_to_expr`) folds the leading
`Dot(Identifier)` run of a `CompoundFieldAccess` into a compound identifier
and plans a later dot only when it holds a string literal. `l[1].b`,
`l[1].b.c`, `m['k'].b` and `(s).a` therefore failed with "Dot access not
supported for non-string expr". Trino and Spark accept them.

`rewrite_dot_after_subscript` in `sqe-sql/src/trino_compat.rs` turns every
`Dot(Identifier)` after that leading run (all of them on a non-identifier
root) into `Subscript(Index('name'))`, which DataFusion plans as the same
named struct field access. Unquoted names are lowercased as DataFusion
folds identifiers; quoted names keep their case. The fast path triggers on
`].` or `).`. The rewrite runs in `apply_trino_compat`, so every wire gets it.

Gate: `make test-integration FILTER=subscript_dot_access` -> 1 passed
(`dot_after_a_subscript_reads_the_struct_field`: `l[1].b`, `l[1].c.d`,
`l[1].C.D`, `t.l[1].b`, `(s).a`, `named_struct('z', id).z` each equal the subscript form on an Iceberg
table, and `WHERE l[1].b = 20` filters), none skipped. Unit tests:
`sqe-sql trino_compat` 71 passed (2 new). The existing rewriter ITs still pass:
`cargo test -p sqe-catalog --test it trino_row_compat -- --include-ignored`
-> 12 passed. Clippy clean on sqe-sql and the
sqe-coordinator tests.

Docs: `sql-reference/operators.md` field-access table.

**NEXT:** #858 (shredded variant write).
