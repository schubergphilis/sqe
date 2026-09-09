# Where the coordinator's compile time goes, and the four boxes that cut it 68 s to 18 s

Date: 2026-09-07. Build-time program item C2 follow-up; supersedes the
"split the coordinator" reasoning in `2026-09-07-coordinator-crate-split.md`
as the first lever. Branch `perf/compile-time-async-hubs`.

## The measurement that went wrong first

The crate-split plan was built on `touch crates/sqe-core/src/lib.rs && cargo
build --workspace --timings` = 154 s with the coordinator library at 96 s.
That number came from a warm *incremental* cache in an unknown state. The
same protocol with `CARGO_INCREMENTAL=0` (what CI runs, and the only way two
branches compare) gives 173 s on `main`, and repeating it after moving the
16k-line write path into its own crate (`refactor/extract-sqe-write`) gives
177 s: the new `sqe-write` crate compiles in 2.8 s and the coordinator
library stays at 68 to 70 s. Lines are not the cost. The split buys structure
(#636) and a 3 s test loop for the 187 write tests, not compile time.

## Where the 68 s actually go

`scripts/compile-profile.sh sqe-coordinator` (stable toolchain,
`RUSTC_BOOTSTRAP=1 cargo rustc -p … -- -Zself-profile -Ztime-passes`, then
measureme's `summarize` and `crox`, then `scripts/compile-profile-attribute.py`):

| pass (`-Ztime-passes`) | seconds |
|---|---|
| type_check_crate | 22.1 |
| MIR_borrow_checking | 20.5 |
| monomorphization_collector_graph_walk | 22.0 |
| generate_crate_metadata | 22.4 |
| codegen + LLVM | ~4 |

Those passes overlap in one query: `evaluate_obligation`, 58.1 s self time,
197,778 evaluations, 70 % of the compile. Attributing every evaluation to
the function whose type-check, borrow-check or MIR optimisation asked for it:

| root function | seconds | why |
|---|---|---|
| `flight_sql::{FlightSqlService impl}` (21 methods, 1.3 s each) | 32.8 | each `async_trait` method proves `SqeFlightSqlService: Sync`; the walk reaches `PreparedStatement.plan: Option<LogicalPlan>` through the moka cache and traverses DataFusion's whole plan/expression type graph (1,646 sub-obligations, 1.1 s of the 1.3 s), once per method because each method has its own param-env and the solver cache does not span them |
| `quack_executor::execute`, `mcp_server::execute` | 10.6 + 10.3 | `async_trait` methods awaiting `QueryHandler::execute` (a 2,000-line `async fn`) and `execute_stream`; each re-proves `Send` over that state machine |
| `flight_sql::do_get_statement`, `get_flight_info_statement` | 6.8 + 3.6 | same, via `execute_stream_with_id` |
| `session_context::PolarisCatalogDiscovery::invalidate_session_cache` | 3.7 | awaits moka `remove` futures over `(SessionContext, Arc<SessionCatalog>)` values |
| everything else (write path, maintenance, distribution, DDL) | < 1 | |

`sqe-catalog` has the same shape: 43 s of its 47 s are `evaluate_obligation`,
rooted in the `async_trait` `TableProvider` impls (`hive::provider` 17 s,
`table_provider` 5 s, `info_schema` 4 s, `iceberg_metadata_tvf` 4 s,
`schema_provider` 3.5 s, `read_json_buffer` 3.5 s). Not touched by this
branch; next candidate.

## The fix: prove once, at the definition

Rust proves auto traits structurally. An `async fn`'s future is a state
machine whose type mentions every local held across an `.await`, so proving
it `Send` walks all of them, transitively, and does so again in every caller
that needs the proof (any `async_trait` method, `tokio::spawn`, `BoxFuture`
conversion). A `Pin<Box<dyn Future + Send>>` at the definition is opaque: the
proof happens once there, callers use the declared bound.

| change | mechanism |
|---|---|
| `QueryHandler::execute` returns `BoxFuture` (body renamed `execute_inner`, `#[instrument]` moved with it) | one `Send` proof of the 2,000-line state machine instead of one per executor |
| `QueryHandler::execute_stream_with_id` returns `BoxFuture` | same for the streaming path |
| `SqeFlightSqlService::get_session_from_request` returns `BoxFuture` | the auth chain's state machine proved once instead of 21 times |
| `session_context::invalidate_session_cache` returns `BoxFuture` | moka's futures over the session-cache value type proved once |
| `PreparedStatement.plan: Option<StoredPlan>` where `StoredPlan(Arc<dyn PlanHolder>)`, `PlanHolder: Send + Sync` | the `Sync` walk of the Flight service stops at the trait object instead of traversing `LogicalPlan` |

Behaviour is unchanged. One heap allocation per query for the boxed futures,
which the code already pays many times over per query; `StoredPlan` adds one
`Arc` per prepared statement. No public signature outside the crate changes
except `PreparedStatement.plan`'s type (field is `pub`, read via `.is_some()`
and `bound_plan()`).

## Result, coordinator library alone (`cargo rustc -p sqe-coordinator --lib`, incremental off)

| step | total | type_check | borrowck | mono collect | metadata |
|---|---|---|---|---|---|
| before | 75.1 (68 unprofiled) | 22.1 | 20.5 | 22.0 | 22.4 |
| + box `execute`, `get_session_from_request` | 46.0 | 20.3 | 10.4 | 11.8 | 12.1 |
| + box `execute_stream_with_id`, `invalidate_session_cache` | 36.4 | 20.3 | 5.6 | 7.0 | 7.3 |
| + `StoredPlan` | **18.4** | 4.4 | 5.2 | 5.5 | 5.8 |

Cold chain (`touch sqe-core`, `CARGO_INCREMENTAL=0`, `cargo build --workspace`,
same laptop, same session):

| unit | main | this branch |
|---|---|---|
| sqe-policy | 22.1 | 23.6 |
| sqe-catalog | 47.5 | 52.5 |
| sqe-coordinator lib | 67.9 | 22.9 |
| sqe-server bin | 19.7 | 15.4 |
| **wall** | **173.2** | **131.3** |

The coordinator is no longer the long pole: `sqe-policy` -> `sqe-catalog`
(76 s in sequence, both with the same async-hub profile) now is.

Gate: `cargo check --workspace --all-targets --all-features`, `cargo clippy
--workspace --all-targets --all-features -- -D warnings`, coordinator lib
tests with `test-sqlite` (954 passed), the Flight SQL and prepared-statement
integration tests (8 passed).

## What this changes in the plan

- C2 (crate split) stops being the compile-time lever. `sqe-write` (MR B,
  branch `refactor/extract-sqe-write`, gate green, test parity 187 + 767 =
  954) is held as a structural change for #636; merge it when that is wanted,
  not for build time.
- C1 (catalog waits for policy) is real again: with incremental off, the
  critical path is `sqe-policy` 22 s → `sqe-catalog` 47 s → coordinator →
  binary. The catalog's 47 s has the same async-hub shape and should get the
  same treatment before any dependency surgery.
- Method for the next crates: run `scripts/compile-profile.sh <crate>`, read
  the attribution table, box the top roots, re-measure. Stop when
  `evaluate_obligation` is under a quarter of the compile.

## sqe-catalog: one cut, then diminishing returns

`HiveTableProvider` held `Arc<ListingTable>`; every `TableProvider` method
re-walked `ListingOptions -> SortExpr -> Expr -> LogicalPlan` for the
`Self: Sync` proof (3.5 s each). As `Arc<dyn TableProvider>` (plus the local
`ListingTable` held across the scan `.await`) the library goes 56 s -> 43 s,
type_check_crate 13.8 s -> 6.5 s.

What remains (40 s) is spread over ten `scan`/`table`/`delete_from` methods
at 1.2 to 2.2 s each in `check_coroutine_obligations` and again in
`optimized_mir`, and it is a different shape: not one deep type reached
through a struct, but hundreds of flat, individually cheap obligations, one
per component of each coroutine witness (`Vec<Expr>`, `Expr`, `Alias`,
`ScalarValue`, `SessionState`, `Analyzer`, `Optimizer`, ...). Those are the
DataFusion parameter and argument types every `TableProvider` method takes
(`filters: &[Expr]`, `state: &dyn Session`) or passes on, and they are live
across the awaits by construction. Even a one-line delegating
`delete_from` on a `dyn` inner pays 2.4 s. There is nothing local to erase
without changing DataFusion's trait signatures, so the catalog stops here at
43 s; the next lever for it is the C1 dependency edge (it waits 22 s for
`sqe-policy` before it can start).

## sqe-catalog, second look (2026-09-08): the coroutine, not the trait

The paragraph above was wrong about "nothing local to erase". Aggregating
the profile by *predicate* (the `arg0` of every `evaluate_obligation` event
in `chrome_profiler.json`) showed the 39.5 s as thousands of small
`<datafusion_expr::... as Send/Sync>` proofs, each solved about 26 times: once
per async method, because every method has its own parameter environment and
rustc does not share auto-trait results across them. The cost is therefore
per *coroutine that captures an `Expr`-bearing value*, and `async_trait`
makes every `async fn` in a trait impl such a coroutine: it moves all the
arguments (`filters: &[Expr]`, a `Vec<Expr>`) into the boxed `async move`
block whether the body uses them across an await or not.

Three shapes remove the coroutine or its `Expr` capture without touching the
DataFusion trait:

1. **Pass-through wrappers** written in `async_trait`'s desugared form
   (`fn scan<'life0, ..., 'async_trait>(...) -> BoxFuture<'async_trait, _>`
   with the `'lifeN: 'async_trait` bounds) that return the inner `dyn`
   provider's already-boxed future directly. No coroutine at all.
   `ReservedMemTable::scan` 4.1 s -> 1.1 s; `HiveTableProvider`'s
   `insert_into` / `delete_from` / `update` / `truncate` 2.2 s -> 0.
2. **Hubs boxed at the definition** (the coordinator recipe):
   `SessionCatalog::load_table`, `SqeTableProvider::scan_impl`,
   `InformationSchemaProvider::build_columns_batch`,
   `SqeSchemaProvider::plan_view`. Their callers then await a `BoxFuture`
   and own no `Expr`/`LogicalPlan`. `SqeSchemaProvider::table` 3.8 s -> 0.3 s;
   `SqeTableProvider::scan` 2.7 s -> 0, `scan_with_args` 2.6 s -> 0 (`ScanArgs`
   hands out its borrows with the outer lifetime, so the boxed future can be
   built and `map_ok`'d without a coroutine).
3. **Erase what the block holds across an await.** A `MemTable` carries
   `Expr` (column defaults); holding one while awaiting its `scan` puts the
   whole graph back in. `Arc<dyn TableProvider>` before the await:
   `information_schema.columns` scan 4.7 s -> 0, the metadata TVF scan
   4.4 s -> 0. `ListingOptions` carries `Expr` (`file_sort_order`); the Hive
   `table()` method now lists files for statistics *before* it builds the
   `ListingTable`, so its last await holds none (3.9 s -> 2.3 s).

4. **State the fact once when the block must own the value.** Three methods
   have no choice: `HiveTableProvider::scan` / `scan_with_args` must own the
   rewritten `Vec<Expr>` the inner scan borrows, `ScopedTable::scan` must own
   the scoped `SessionState` (and needs the `&[Expr]`). Both types are
   `Send + Sync`; only the proof is expensive. `crate::send_erased::SendErased`
   is a newtype with an `unsafe impl Send + Sync` gated on an `unsafe` marker
   trait implemented for exactly those types, and a unit test that proves
   the marker's claim the ordinary way. The coroutine holds the wrapper and
   the proof stops there: 4.5 s + 2.4 s -> 0. DataFusion is not vendored in
   this repo (only iceberg-rust is), so an owned-argument trait variant was
   never on the table; this is the substitute.

Per-crate `evaluate_obligation`: 39.5 s -> 7.2 s; the profiled crate build
42 s -> 19 s. What remains: the Hive `table()` tail (2.3 s) and 1.2 s on
`ReservedMemTable::scan` that the pass-through did not remove, plus the long
tail of `SessionCatalog` methods at 0.2 to 0.4 s each.

The predicate aggregation is a two-line addition to the attribution script's
input and is worth running first next time: it tells you *what type* is being
walked, which is what decides between the three shapes above.

## Progress log

- 2026-09-07: measured, fixed, documented (this file); chain 173 s -> 131 s.
- 2026-09-08: C1 policy half (!963) and C4 coordinator (!961) on main: chain
  108.8 s. sqe-catalog C4 on `perf/catalog-async-hubs`: obligations
  39.5 s -> 7.2 s (section above); chain 108.8 s -> 84.9 s
  (`CARGO_BUILD_JOBS=4`, `touch sqe-core`, `CARGO_INCREMENTAL=0`).
