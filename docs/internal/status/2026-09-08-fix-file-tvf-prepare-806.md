**STATUS 2026-09-08 (afternoon): finding 806 (#638), file table functions, on `fix/file-tvf-prepare-806`: `read_parquet` / `read_csv` / `read_json` infer their schema on the async path; `call` only looks the provider up.**

`TableFunctionImpl::call` is synchronous and DataFusion invokes it while
planning, so the three file readers bridged onto object storage from
inside the planner (`runtime_bridge::block_on_compat`, five sites). New
module `crates/sqe-catalog/src/file_tvf_prepare.rs`: each reader
implements `PreparableTableFunction::prepare(exprs)` (parse, path policy,
build the `ListingTable`, store it in the function's `PreparedProviders`
keyed by the literal arguments) and its `call` re-runs the policy and
looks the provider up. A call that was never prepared is a planning error
that names the function and `prepare_statement`, not a fallback: that is
how a new planner entry point that forgets the step gets noticed (it
caught the CTAS path on the first live run). `FileTableFunctions::register`
replaces `ctx.register_udtf` for the readers and records them in a
`SessionConfig` extension; `prepare_statement(&ctx, sql)` parses the final
SQL, converts the file-function arguments (`'k=v'` literals, `k => v`,
`k = v`, numbers, booleans) to the `Expr`s the planner will pass, and
awaits each `prepare`. Hooked before every `ctx.sql` in the coordinator
(execute, the Flight stream planner, `get_schema`, parameter typing, the
three EXPLAIN paths, the Hive CTAS schema probe) plus once in the
statement dispatcher, gated on the text naming a file function, which
covers the write handler's many derived sub-statements (CTAS / INSERT ...
SELECT / MERGE) through the session-cached context. The embedded CLI
prepares before its `ctx.sql`. `read_delta.rs` keeps its bridge: the module
is unwired pending a DataFusion-55 delta-rs. Gates: catalog unit 637 (six
new for the module, the `read_json` tests now plan through `prepare`),
`it` 150, the three `test_read_*_local_file` CTAS cases against the live
stack, sqe-cli 109, clippy clean.

Left of 806 after this: `SqeCatalogProvider::schema()`'s namespace re-list
on miss (#368) and `WritableIcebergCatalog`'s create/drop-namespace hooks
(three sites); both already report the bridge error as an error, and the
coordinator routes CREATE/DROP SCHEMA through `catalog_ops` asynchronously,
so the remaining consumer is the embedded CLI's `ctx.sql("CREATE SCHEMA")`.
**NEXT:** those three sites (then #638 closes), 205/212 (#653), 610 (#657),
627, #659 partials; the docker-capable CI runner for the `polaris` group.
