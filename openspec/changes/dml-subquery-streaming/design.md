## Context

SQE compiles `UPDATE`/`DELETE` with a WHERE clause into a Copy-on-Write (CoW) rewrite: per-file batches are read as Arrow RecordBatches, registered as MemTables under `datafusion.public.__update_<table>` / `__delete_<table>`, and evaluated via a `SELECT ... FROM <scratch> AS "<orig_name>"` that either filters (DELETE) or projects `CASE WHEN <where> THEN <new_expr> ELSE <col> END` (UPDATE). The result RecordBatches are written back as new Iceberg data files and committed via `rewrite_files`.

`IN (subquery)` WHERE clauses crash this pipeline. The root cause is documented in the proposal. This design covers how to lift the subquery out of the WHERE and into a joined relation so the plan stays bounded in size regardless of cardinality.

## Goals / Non-Goals

**Goals:**

- CoW `UPDATE` and `DELETE` with `(cols) IN (SELECT cols FROM ...)` WHERE clauses complete without stack overflow at any cardinality the subquery evaluates to.
- Plan size after rewriting is O(1) in subquery cardinality (only the scratch MemTable data scales, not the SQL text).
- Single materialisation of the subquery result per DML statement. Per-batch SELECTs reuse the materialised MemTable via semi-join.
- Correctness preserved for `IN`, `NOT IN`, single-column, multi-column tuple IN, empty result, NULL handling. Matches current behaviour for NULL: rows with any NULL column are skipped from the matcher (already current rewriter behaviour at line 1946-1948).

**Non-Goals:**

- Correlated IN subqueries (subquery references outer target columns). Neither the current rewriter nor the new one supports this; the current code executes the subquery as `SELECT * FROM ({subquery}) AS __sq` which ignores correlation. No regression, no new capability.
- Scalar subquery handling (the separate `decorrelate_scalar_subqueries` path is untouched).
- MERGE INTO source IN-subquery handling (MERGE uses a different code path; scoped out of this change).
- MoR UPDATE (not implemented yet in SQE; when added, reuse this machinery).

## Architecture

```
            WHERE "(col1, col2) IN (SELECT c1, c2 FROM big)"
                               |
                               v
            +---------------------------------------+
            |    lift_in_subqueries(where, ctx)     |
            |                                       |
            |  1. Parse WHERE into Expr             |
            |  2. Walk AST; for each InSubquery:    |
            |       a. ctx.sql(DISTINCT + matched)  |
            |       b. collect RecordBatches        |
            |       c. MemTable + register scratch  |
            |       d. emit LEFT JOIN clause        |
            |       e. replace node with COALESCE   |
            |  3. format! on rewritten WHERE (tiny) |
            +---------------------------------------+
                               |
        +------------+---------+--------+-----------+
        |            |                  |           |
        v            v                  v           v
  rewritten_where  joins_sql     InSubqueryCleanup  (drops on scope end)
        |            |
        +-----+------+
              |
              v
+------------------------------------------------------+
|  Per-batch evaluator (filter_batch_negate,            |
|  filter_batch_match, apply_update, count_matching)    |
|                                                       |
|  Builds:  SELECT <proj> FROM __scratch AS "target"    |
|           <joins_sql>   <- LEFT JOIN __sqN ON ...     |
|           WHERE <rewritten_where>                     |
|                                                       |
|  DataFusion optimiser lowers LEFT JOIN + flag check   |
|  to a HashSemi/HashAnti join over the small scratch   |
|  MemTable. Plan depth is bounded. Hash recursion is   |
|  bounded. Stack is bounded.                           |
+------------------------------------------------------+
                               |
                               v
           DropGuard -> ctx.deregister_table(__sqN)
```

## Key Design Decisions

### Materialise the subquery once, not per batch

`DataFrame::into_view()` exists in DataFusion and would push the subquery's `LogicalPlan` into each batch-SELECT. That re-runs the subquery for every CoW data file batch. TPC-E `trade_result_update_holding` scans holding_summary, which has many data files; a naive view would re-execute the `SELECT ... FROM trade WHERE t_st_id = 'PNDG'` scan on every batch.

Instead we execute the subquery once via `ctx.sql(...).await?.collect().await?`, pipe the RecordBatches into a `MemTable`, and register that. Per-batch CoW SELECTs read from the MemTable. The subquery runs once per DML statement.

Trade-off: O(subquery cardinality) coordinator memory. For TPC-E SF10 at 34,496 tuples of `(i64, varchar(15))` that is ~1.3 MB. For 1M tuples, ~40 MB. Acceptable for a query engine. If a future workload needs unbounded subquery cardinality, the same mechanism admits a `DataFrame::into_view()` path behind a config flag — behaviour-preserving because the outer JOIN shape is identical.

### Replace `IN (subquery)` with `COALESCE(__matched, FALSE)`, not with a direct IN against the scratch

```sql
-- option A (chosen): stable across DataFusion versions
LEFT JOIN (SELECT DISTINCT c1 AS __col0, c2 AS __col1, TRUE AS __matched FROM ...) v
  ON v.__col0 = target.col1 AND v.__col1 = target.col2
WHERE COALESCE(v.__matched, FALSE)

-- option B (rejected): relies on optimiser decorrelating InSubquery inside CASE WHEN
WHERE (col1, col2) IN (SELECT __col0, __col1 FROM __sqN)
```

Option B sits inside CASE WHEN in the UPDATE path (see `apply_update` at line 1774). DataFusion's `DecorrelatePredicateSubquery` / `ScalarSubqueryToJoin` rules typically target WHERE-level subqueries; projection-embedded InSubquery inside a CASE WHEN is what motivates the existing `decorrelate_scalar_subqueries` helper in the same file. Rather than extend the decorrelator to handle `Expr::InSubquery`, we avoid the problem: the rewriter emits a plain boolean column reference which DataFusion's projection pushdown handles trivially.

### Why `SELECT DISTINCT` in the scratch materialiser

The outer LEFT JOIN on `(c1, c2)` can produce row multiplicity if the subquery has duplicates. `COALESCE(v.__matched, FALSE)` tolerates that (any match flips the flag true), but row multiplicity distorts CASE WHEN projection in the UPDATE path: the outer SELECT would emit duplicate rows for the target table. `DISTINCT` in the scratch materialiser collapses duplicates up front so every target row matches at most once. Matches current rewriter semantics (it produced `IN (v1, v2, ...)` which is naturally duplicate-insensitive).

### NOT IN and NULLs

Standard SQL `x NOT IN (values)` is NULL if any value in the list is NULL. The current rewriter skips NULL subquery rows (line 1946-1948), so `NOT IN` already behaves as `NOT EXISTS` — a match from the non-NULL part of the keyset. We preserve that: `SELECT DISTINCT` on the subquery naturally drops rows that are NULL in all columns, and `WHERE __col IS NOT NULL` is added to the scratch materialiser to drop rows with NULL on any matcher column. The equality join then matches only non-NULL tuples, and `NOT COALESCE(__matched, FALSE)` returns TRUE for unmatched rows.

This is a deliberate semantic: TPC-E and real-world DML depend on this behaviour. Switching to strict SQL NOT IN semantics would break the current benchmark. Documented in the spec.

### RAII cleanup

Scratch MemTable names are globally unique per-statement: `__sqe_in_subq_<u64>` where the u64 is bumped from a `static AtomicU64`. Collisions across concurrent statements or nested DML are impossible.

The cleanup guard wraps a `Vec<String>` of registered names plus a `Weak<DFSessionContext>` equivalent (via the existing ctx handle). `Drop` iterates and calls `ctx.deregister_table(name)`. Returned from `lift_in_subqueries` and held for the full DML statement lifetime by the handler.

Deregister errors inside `Drop` are logged at `warn!` and swallowed — matches current behaviour at write_handler.rs lines 1650, 1709, 1808, 1847 (`let _ = ctx.deregister_table(...)`).

### How the outer SELECT consumes `joins_sql`

Current pattern in `apply_update` (lines 1796-1800):

```rust
let select_sql = format!(
    "SELECT {cols} FROM datafusion.public.{table_name} AS \"{orig_name}\"{joins}",
    cols = columns.join(", "),
    joins = joins_sql,  // decorrelator LEFT JOINs
);
```

The decorrelator already injects `joins_sql` here (as `extra_joins.join(" ")`). We extend the same mechanism: the IN-subquery lifter appends its LEFT JOIN clauses to the same string. Decorrelator joins remain first; IN-subquery joins follow.

`filter_batch_negate`, `filter_batch_match`, `count_matching_rows` have simpler SELECTs that currently do not inject joins. They gain a `joins_sql: &str` parameter and splice it in the same position.

## Rust Shapes

```rust
/// RAII handle that deregisters a set of scratch MemTables on drop.
///
/// Returned by `lift_in_subqueries` and bound in DML handlers to outlive
/// the per-batch SELECT loop.
pub(crate) struct InSubqueryCleanup {
    ctx: DFSessionContext,
    scratch_tables: Vec<String>,
}

impl Drop for InSubqueryCleanup {
    fn drop(&mut self) {
        for name in &self.scratch_tables {
            if let Err(e) = self.ctx.deregister_table(name.as_str()) {
                tracing::warn!(table = %name, error = %e, "in-subquery scratch deregister failed");
            }
        }
    }
}

impl WriteHandler {
    /// Lift every `IN (subquery)` in `where_sql` into a LEFT JOIN over a
    /// pre-materialised DISTINCT keyset.
    ///
    /// Returns:
    /// - the rewritten WHERE string (O(1) in subquery cardinality)
    /// - a JOIN clause string to append after the outer SELECT's FROM table
    /// - a cleanup guard deregistering all scratch MemTables on drop
    async fn lift_in_subqueries(
        &self,
        where_sql: &str,
        ctx: &DFSessionContext,
    ) -> sqe_core::Result<(String, String, InSubqueryCleanup)>;
}
```

Call sites update from:

```rust
let where_sql = self.rewrite_in_subquery_where(&raw_where, ctx).await?;
// ... per-batch loop passes where_sql
```

to:

```rust
let (where_sql, joins_sql, _in_subq_guard) =
    self.lift_in_subqueries(&raw_where, ctx).await?;
// _in_subq_guard outlives the per-batch loop; dropped at end of handler.
// joins_sql is appended to each per-batch SELECT's FROM clause.
```

## Data Flow Example

Input DML (TPC-E `trade_result_update_holding`):

```sql
UPDATE holding_summary
SET hs_qty = hs_qty + (
    SELECT CASE WHEN tt.tt_is_sell THEN -t.t_qty ELSE t.t_qty END
    FROM trade t JOIN trade_type tt ON tt.tt_id = t.t_tt_id
    WHERE t.t_ca_id = holding_summary.hs_ca_id
      AND t.t_s_symb = holding_summary.hs_s_symb
      AND t.t_st_id = 'PNDG'
    LIMIT 1)
WHERE (hs_ca_id, hs_s_symb) IN (
    SELECT t.t_ca_id, t.t_s_symb FROM trade t WHERE t.t_st_id = 'PNDG'
);
```

After `lift_in_subqueries` (WHERE portion only):

Rewritten WHERE:
```sql
COALESCE("__sqe_in_subq_42"."__matched", FALSE)
```

Joins:
```sql
LEFT JOIN (
  SELECT DISTINCT t.t_ca_id AS __col0, t.t_s_symb AS __col1, TRUE AS __matched
  FROM trade t
  WHERE t.t_st_id = 'PNDG'
    AND t.t_ca_id IS NOT NULL
    AND t.t_s_symb IS NOT NULL
) AS "__sqe_in_subq_42"
  ON "__sqe_in_subq_42"."__col0" = holding_summary.hs_ca_id
 AND "__sqe_in_subq_42"."__col1" = holding_summary.hs_s_symb
```

The DISTINCT SELECT runs once as part of `lift_in_subqueries`, materialising into a MemTable registered as `__sqe_in_subq_42`. The outer CoW SELECT at `apply_update` then sees this scratch MemTable as a regular table and DataFusion plans the join using a hash semi-join with the small side on the right.

Plan text for the outer SELECT stays ~500 bytes regardless of how many rows the trade PNDG subquery returns.

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Multi-column tuple IN where LHS uses qualified names (e.g. `tpce_sf10.holding_summary.hs_ca_id`) fails to resolve in the JOIN ON | The lifter reuses the exact text of each LHS column ref from the parsed AST; the outer SELECT's `AS "{orig_name}"` alias already handles the qualified-name case for the existing decorrelator |
| Subquery references outer target (correlated IN) | Out of scope, same as current rewriter. If a user submits one, the subquery SELECT will fail to resolve the outer column and surface DataFusion's "column not found" error. Documented. |
| Memory pressure from very large subqueries (100M+ rows) | A configuration knob `dml.in_subquery.max_materialised_rows` (default unlimited, override per environment) will cap the materialised keyset; over the cap, fallback to `df.into_view()` with re-execution per batch. Added as follow-up; not required for SF100. |
| Scratch table name collision with user tables | Prefix `__sqe_in_subq_` is well-outside any sensible table naming convention; name includes a globally unique u64 counter |
| `DISTINCT` on wide tuples is expensive | DataFusion's HashAggregate handles this efficiently; the subquery had to be executed anyway, DISTINCT adds a hash-set pass not a sort |
| Regression on TPC-H / TPC-DS which do not use IN subqueries in DML | Fast path: `lift_in_subqueries` checks `where_sql.to_uppercase().contains("SELECT")` first (already in current rewriter at line 1885); returns the input unchanged and empty joins/guard |

## Open Questions

None remaining after the proposal review. Decisions captured above.
