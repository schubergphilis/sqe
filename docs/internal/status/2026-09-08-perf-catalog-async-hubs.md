**STATUS 2026-09-08 (late): build-time C4 for `sqe-catalog` on `perf/catalog-async-hubs`: trait solving 39.5 s -> 7.2 s; cold chain 108.8 s (main) -> 84.9 s.**

Re-measured main first (`touch sqe-core`, `CARGO_INCREMENTAL=0`): 108.8 s,
down from 173 s at the start of the program (!961 + !963). Then profiled the
catalog and aggregated `evaluate_obligation` by *predicate*: 39.5 s of
`<datafusion_expr::... as Send/Sync>` proofs, each solved once per async
method because every method has its own parameter environment. The cost is
per coroutine that captures an `Expr`-bearing value, and `async_trait` makes
every trait `async fn` such a coroutine. Three shapes fix it without touching
DataFusion's traits: pass-through wrappers written in `async_trait`'s
desugared form (no coroutine: `ReservedMemTable::scan`, the Hive DML hooks),
hubs boxed at the definition (`SessionCatalog::load_table`,
`SqeTableProvider::scan_impl`, `build_columns_batch`, `plan_view`), and
erasing what a block holds across an await (`MemTable` carries `Expr` in its
column defaults, so `Arc<dyn TableProvider>` first; `ListingOptions` carries
`Expr`, so the Hive `table()` lists files before it builds the
`ListingTable`). The residue (`HiveTableProvider::scan`/`scan_with_args`
must own the rewritten `Vec<Expr>` the inner scan borrows; `ScopedTable::scan`
holds a `SessionState`) is a fourth shape: `send_erased::SendErased<T>`
wraps the value in a type whose `Send + Sync` is stated once (unsafe impls
for `Vec<Expr>`, `&[Expr]`, `SessionState`, each checked structurally by a
unit test), so the coroutine proof never descends into DataFusion. That
took obligations 13.9 s -> 7.2 s and the chain 93.8 s -> 84.9 s. Plan:
docs/internal/plans/2026-09-07-compile-time-async-hubs.md, "second look".
Gates: sqe-catalog all test binaries 846, `it` 150, live Hive e2e 5, clippy
clean. Behaviour
unchanged; the metadata TVF scan stops forwarding `filters` to its
`MemTable`, which never received any (default pushdown is unsupported).
**NEXT:** the Flight `USE` / default-schema gap (closes #627), #659 partials,
the docker-capable CI runner (807 item 3). Parked, not in this repo: A2
sccache bucket on object02 prd (lifecycle rule + RO key, then the five CI
variables); C3 runner with a persistent target dir (platform team).
