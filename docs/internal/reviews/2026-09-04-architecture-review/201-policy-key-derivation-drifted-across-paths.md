# 201: Policy lookup key is derived in four places and they disagree on the namespace

- Scope: sqe-policy, sqe-coordinator write path, sqe-catalog information_schema
- Severity: high
- Category: security
- Effort: M (days)
- Status: open

## Problem

The read path keys a policy by the FULL dotted namespace (issue #205 fix):

```rust
// crates/sqe-policy/src/plan_rewriter.rs:829-839
fn resolve_policy_key(table_ref, hive_catalogs) -> Option<(String, String)> {
    ...
    let namespace = match table_ref.schema() {
        Some(schema) if !schema.is_empty() => schema.to_string(),
```

Three other callers still key by the LAST component and each carries a
comment claiming it matches the rewriter:

```rust
// crates/sqe-coordinator/src/write_handler.rs:754-762  (DELETE / UPDATE)
// The rewriter keys policy lookups by the last namespace component
// and the table name. Flatten the multi-level namespace to match.
let namespace_key = table_ident.namespace().as_ref().last().cloned()
    .unwrap_or_else(|| "default".to_string());
```

```rust
// crates/sqe-catalog/src/info_schema.rs:207-211
// Match plan_rewriter's policy key: the namespace is the LAST dotted
// component (resolve_policy_key uses rsplit('.').next()).
let policy_ns = ns.rsplit('.').next().unwrap_or(ns);
```

```rust
// crates/sqe-policy/src/write_predicates.rs:20-23
//! Multi-level namespaces are flattened to the last component, matching
//! the SELECT-side rewriter that does the same when keying its policy lookup.
```

`crates/sqe-policy/src/ranger_store.rs:381-385` documents the same stale
last-component convention for `hive_database`.

## Why it matters

For any table in a multi-level namespace (`a.b.t`):

- SELECT resolves `("a.b", "t")`; DELETE and UPDATE resolve `("b", "t")`. A
  Ranger row filter written against database `a.b` applies to reads but not to
  DML, so a user can delete or update rows they cannot see. A filter written
  against `b` applies to DML but not to reads, and collides across tenants
  (the exact bug #205 fixed on the read side).
- `information_schema.columns` resolves `("b", "t")`, so a column restricted
  under `("a.b", "t")` is listed by name while the rewriter NULLs it. The
  "denied columns are invisible" guarantee is broken for every multi-level
  namespace. `SHOW COLUMNS`, `DESCRIBE` and `SHOW CREATE TABLE` all go through
  `information_schema.columns` (`query_handler.rs:4703-4740`), so they leak too.

The drift happened once already and will happen again: the key rule is
copied, not called.

## Proposed change

1. Make `resolve_policy_key` in `plan_rewriter.rs` the single public function
   (drop the `_for_test` shim) and give it a second entry point that takes
   `(catalog: Option<&str>, namespace: &[String], table: &str)` for callers
   that hold an Iceberg `TableIdent` rather than a `TableReference`.
2. Call it from `WriteHandler::compute_write_predicates`,
   `info_schema.rs` (columns provider) and `write_predicates.rs`.
3. Delete the three stale comments and the `hive_database` note in
   `ranger_store.rs:381-385`.
4. Add a `#[deny]`-style guard: a unit test in `sqe-policy` that feeds the same
   `a.b.t` reference through the read key function and the ident-based key
   function and asserts equality.

## Acceptance criteria

- Integration test (`make test-integration FILTER=policy_dml_key`): create
  table `ns1.ns2.t`, add an InMemory row filter under namespace `ns1.ns2`,
  assert SELECT, DELETE and UPDATE all see the same filtered row set, and that
  `information_schema.columns` omits a restricted column.
- `grep -rn "rsplit('.')" crates/sqe-catalog/src/info_schema.rs` returns
  nothing; `grep -rn "namespace().as_ref().last()" crates/sqe-coordinator/src/write_handler.rs`
  returns nothing.

## Related

- 200 (subquery bypass), 202 (MERGE target not gated), 205 (SHOW STATS leak).
- Issue #205 (read-side fix that created the drift).
