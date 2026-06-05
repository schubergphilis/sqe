# Findings — Planner & Core (`sqe-planner`, `sqe-core`, light `sqe-cli`/`sqe-metrics`)

**Scope:** `crates/sqe-planner/` (entire crate, 12 files: LogicalPlan -> PhysicalPlan, partition splitting,
distributed-execution scaffolding), `crates/sqe-core/` (config + secret types + shared parsers), and a light pass
over `crates/sqe-cli/` and `crates/sqe-metrics/`. The decisive lens was a **reachability split**: of the
planner's optimizer rules, only `StarSchemaReorderRule` is wired into the coordinator
(`query_handler.rs:1697`) and is ON by default, so its bug is live (PLAN-01). The distributed-execution layer
(`decompose_plan`, the shuffle execs, `DistributedAggregateRule`, `BroadcastJoinRule`) has no callers outside
the planner crate and its `execute()` paths are stubs, so those issues are latent (PLAN-02..04, rated `low`).
`bin_pack_files`/`ScanTask` (the hot scan-planning path) are bounded and sound. `secret.rs`/`secret_string.rs`
are correct; several credential fields in `config.rs` bypass them (CORE-01). Metrics cardinality and the CLI are
clean for this scope (MET-01, CLI-01).

> **COORD-01 (in findings-coordinator-core.md) was re-verified during this pass**: store-side
> `extract_table_names` (extract/mod.rs:25) emits qualified `table_name.to_string()`; invalidate-side
> (query_handler.rs:1267) uses bare `ins.table.to_string()`. The keys differ. invalidation is a no-op.

---

### PLAN-01 — high — StarSchemaReorderRule can rebuild joins on the wrong column (silent wrong results), and the rule is ON by default

- **Dimension:** security
- **Status:** NEW surface
- **Location:** `crates/sqe-planner/src/star_schema_reorder.rs:550` (and the by-name fallback at `:516-526`), invoked at `crates/sqe-coordinator/src/query_handler.rs:1697`, default at `crates/sqe-core/src/config.rs:227`
- **Evidence:**
  ```rust
  // resolve_join_keys, line 550 — re-binds the join key by NAME into the accumulated schema:
  let left_idx = accumulated_schema.index_of(&left_col_name).ok()?;
  let new_left = Arc::new(Column::new(&left_col_name, left_idx));
  ```
  ```rust
  // star_schema_reorder defaults ON: config.rs:227
  star_schema_reorder: default_true(),
  ```
- **Impact:** When the rule reorders a chain of 3+ inner joins, it rebuilds each `HashJoinExec` by re-resolving
  the original key columns *by name* against the accumulated left subtree's schema. After joins concatenate
  columns, that schema commonly carries duplicate column names (a shared surrogate key such as `id`/`sk` across
  dimension tables is the star-schema norm). `Schema::index_of(name)` returns the **first** field of that name, so
  the rebuilt join binds the key to the wrong table's column. The result is a syntactically valid join with the
  same output schema but **wrong rows**, shipped silently to the client. `schema_check()` (line 262) validates
  only output *shape*, which a wrong-but-same-shape join preserves, so nothing catches it. Runs on every
  multi-join query in the default configuration. A *failed* lookup is safe (keeps the original plan); the hazard is
  a *successful but wrong* resolution. The by-name fallback (516-526) widens the same hazard.
- **Fix:** Re-bind join keys by the original `Column` *index* tracked through the flattening, not by name; or
  before reordering, detect duplicate column names in any accumulated schema and bail (`Transformed::no`) when keys
  cannot be disambiguated. At minimum, gate the fallback (516-526) so it only fires when the matched name is unique
  in both schemas. Consider treating this as critical during triage. it is silent data-correctness corruption on a
  default-on path.
- **Effort:** medium

---

### PLAN-02 — low — `decompose_plan` produces overlapping plan fragments with no shuffle wiring (unwired scaffolding)

- **Dimension:** reliability
- **Status:** NEW surface
- **Location:** `crates/sqe-planner/src/stage_planner.rs:183-326`
- **Evidence:**
  ```rust
  fn decompose_recursive(plan, builder) -> HashMap<usize, String> {
      decompose_recursive(left, builder);   // emits stages for nested boundaries
      builder.stages.push(QueryStage {       // ALSO emits `left` whole as a stage
          plan_fragment: Arc::clone(left),
          input_stages: vec![],              // never wired to the nested stages
      });
      extracted.insert(0, left_stage_id);    // return value discarded by callers
  }
  ```
- **Impact:** The returned `extracted` map is discarded by every caller, and the function never substitutes
  `ShuffleReaderExec` placeholders for extracted children. When a join's `left` is itself a join, the nested
  boundary is emitted as a stage AND `left` is re-emitted whole, yielding overlapping/duplicated fragments; the
  intermediate stage gets `input_stages: vec![]`, so DAG edges are missing. If the distributed scheduler ever
  consumed this it would execute overlapping work and/or a disconnected stage graph. `low` because `decompose_plan`
  has no callers outside the planner crate today. unwired scaffolding, latent until distributed execution lands.
- **Fix:** Return and consume the `extracted` map: replace each extracted child with a `ShuffleReaderExec`
  placeholder, set the parent stage's `input_stages` to the extracted IDs, emit each subtree once. Add a test that
  asserts the stage DAG is connected and fragments are disjoint.
- **Effort:** large

---

### PLAN-03 — low — `estimate_group_cardinality` reads the first input column's stats, not the GROUP BY column (latent)

- **Dimension:** performance
- **Status:** NEW surface
- **Location:** `crates/sqe-planner/src/distributed_aggregate.rs:543-549`
- **Evidence:**
  ```rust
  let col_stats = stats.column_statistics.first()?;   // always column 0
  col_stats.distinct_count.get_value().copied()
  ```
- **Impact:** Picks `column_statistics.first()` regardless of which column the first GROUP BY expression
  references. When the key is not column 0, the distinct-count estimate is for the wrong column, so the strategy
  selector may pick `CoordinatorMerge` for a high-cardinality group-by (coordinator bottleneck/OOM) or
  `ShuffleMerge` for a low-cardinality one (needless shuffle). A perf-decision bug, not wrong results. `low`:
  `DistributedAggregateRule` is not registered on the coordinator (latent).
- **Fix:** Map the first group-by expression to its input column index and index `column_statistics` by that
  index; return `None` if the expression is not a plain column.
- **Effort:** small

---

### PLAN-04 — low — `BroadcastJoinPlan::from_hash_join` uses `.expect()` reachable during optimization (latent)

- **Dimension:** reliability
- **Status:** NEW surface
- **Location:** `crates/sqe-planner/src/distributed_join.rs:208-211`
- **Evidence:**
  ```rust
  let inner = hash_join.builder().build()
      .expect("BroadcastJoinPlan: failed to reconstruct HashJoinExec");
  ```
- **Impact:** If `HashJoinExec::builder().build()` ever returns `Err` (e.g. a join shape DataFusion's builder
  rejects after an upgrade), this panics inside a `PhysicalOptimizerRule`, crashing the planning thread for that
  query rather than degrading gracefully. `low`: `BroadcastJoinRule` is not registered on the coordinator (latent).
- **Fix:** Return `Result` from `from_hash_join` (or fall back to `Transformed::no(node)` on `build()` error)
  instead of `.expect()`.
- **Effort:** small

---

### PLAN-05 — info — Micro-allocations on the planning path (consolidated)

- **Dimension:** performance
- **Status:** NEW surface
- **Location:** `crates/sqe-planner/src/join_strategy.rs:233`, `distributed_join.rs:730`, `predicate_transfer.rs:186`, `star_schema_reorder.rs:474-526`
- **Evidence:**
  ```rust
  // join_strategy.rs:233 — two String allocations per sort-expr comparison:
  if format!("{existing}") != format!("{required_expr}") { return false; }
  ```
- **Impact:** Wasted allocations/CPU on the planning path; all bounded (plans are small; predicate-transfer aborts
  past 10k distinct values), so impact is negligible. Grouped as one `info` note to avoid triage noise.
- **Fix:** Compare `PhysicalSortExpr` structurally instead of via `format!`; check the distinct-value size bound
  per-insert rather than per-batch. Low priority.
- **Effort:** small

---

### CORE-01 — medium — OAuth client secrets and worker secret stored as plain `String` under `#[derive(Debug)]` (redaction bypass)

- **Dimension:** security
- **Status:** REGRESSION of resolved finding (prior audit: "credential redaction in Debug impls")
- **Location:** `crates/sqe-core/src/config.rs:642` (`AuthProviderConfig`, secret fields at `:654`, `:667`), `config.rs:550` (`WorkerConfig.worker_secret: String` at `:575`); also `azure_sas_token` at `:1160`, `api_key` at `:1643`
- **Evidence:**
  ```rust
  #[derive(Debug, Deserialize, Clone)]            // config.rs:642
  pub enum AuthProviderConfig {
      OidcPassword { ... client_secret: String, ... }     // :654
      ClientCredentials { ... client_secret: String }     // :667
  ```
  ```rust
  #[derive(Debug, Deserialize, Clone)]            // config.rs:550
  pub struct WorkerConfig { pub worker_secret: String, /* :575, plain String */ }
  ```
- **Impact:** The codebase introduced `SecretString` and a custom `CoordinatorConfig` Debug impl (`config.rs:452`,
  with `worker_secret: SecretString` at `:315`) precisely so credentials cannot leak through `{:?}`. These structs
  bypass that: `AuthProviderConfig` derives default `Debug` over plain-`String` OAuth `client_secret`s, and
  `WorkerConfig.worker_secret` is plain `String` (the coordinator's counterpart uses `SecretString`). Any
  `debug!("{:?}", provider)`, an `anyhow` chain capturing the config, or a panic message formatting these structs
  prints the live secret in plaintext to logs. No active wholesale `{:?}` of the full config was found in hot
  paths, so this is a latent defense-in-depth gap on live credential fields (hence `medium`).
- **Fix:** Change these fields to `SecretString` (consistent with `CoordinatorConfig`/`StorageConfig`), or give
  `AuthProviderConfig` and `WorkerConfig` hand-written redacting `Debug` impls. Add a test asserting
  `format!("{:?}", provider)` never contains the secret material.
- **Effort:** small

---

### CORE-02 — low — `parse_memory_limit` silently saturates to `usize::MAX` on oversized input

- **Dimension:** reliability
- **Status:** NEW surface
- **Location:** `crates/sqe-core/src/config.rs:635`
- **Evidence:**
  ```rust
  Ok((num * multiplier) as usize)   // f64 -> usize saturating cast
  ```
- **Impact:** `num: f64 * multiplier: f64` then `as usize`. An oversized config value (e.g. `"99999999TB"`)
  overflows and the `as usize` cast **saturates to `usize::MAX`** rather than erroring. The parsed value feeds
  `target_task_size`, per-user memory budgets, and pool sizing, so a fat-fingered config becomes an effectively
  unbounded byte count downstream code may try to honor. No panic and operator-controlled (limited blast radius),
  hence `low`.
- **Fix:** Parse the numeric part as integer where possible, compute in checked `u64`/`u128`, and return
  `SqeError::Config` when the product exceeds `usize::MAX` (or a sane cap) instead of casting through `f64`.
- **Effort:** small

---

### CORE-03 — info — `SecretString::ct_eq` short-circuits on length mismatch (length oracle; accepted tradeoff)

- **Dimension:** security
- **Status:** NEW surface
- **Location:** `crates/sqe-core/src/secret_string.rs:73-75`
- **Evidence:**
  ```rust
  if a.len() != b.len() { return false; }   // early return reveals length inequality via timing
  ```
- **Impact:** The constant-time comparator returns early when lengths differ, leaking (via timing) whether the
  candidate's length matches. This is the standard accepted tradeoff for variable-length secret comparison (the
  `subtle` crate behaves similarly), so impact is minimal. The rest of the type (Debug redaction, no
  `Display`/`Deref`, Zeroize-on-drop) is sound and well-tested.
- **Fix:** Optional: compare against fixed-length digests (SHA-256 of each side) so the loop is always over equal
  lengths, or document the length-leak as accepted.
- **Effort:** trivial

---

### MET-01 — info — Metrics label cardinality is bounded (verified clean — coverage result)

- **Dimension:** sustainability
- **Status:** verified safe (no finding)
- **Location:** `crates/sqe-metrics/src/lib.rs:103,111,151,337,372,392,403,416,428,443`
- **Evidence:**
  ```rust
  &["status", "statement_type", "error_code"],   // lib.rs:103
  ```
- **Impact:** None. positive coverage result. All metric label values are bounded enum names (`status`,
  `kind.name()`, `error_code` = `err.error_code().name()`, `decision`, `backend`, `provider`, `operation`). No
  label carries `query_id`, username, token, or SQL text, so there is no Prometheus cardinality-explosion vector.
- **Fix:** None.
- **Effort:** trivial

---

### CLI-01 — info — CLI is clean for this scope (verified clean — coverage result)

- **Dimension:** reliability
- **Status:** verified safe (no finding)
- **Location:** `crates/sqe-cli/src/` (all files)
- **Evidence:**
  ```rust
  // flight.rs:93 — guarded by an is_empty() check at line 86:
  let schema = batches[0].schema();
  ```
- **Impact:** None of note. Every `unwrap()`/`expect()`/`panic!` in the CLI is inside `#[cfg(test)]`, or guarded,
  or operates on the local interactive user's own stdin where blast radius is the user's own terminal. No command
  injection, no process spawning, no path/URL handling beyond the already-covered `next_uri` same-origin check. The
  embedded.rs TVF local-path matter is covered as CAT-01 context (not restated).
- **Fix:** None.
- **Effort:** trivial
