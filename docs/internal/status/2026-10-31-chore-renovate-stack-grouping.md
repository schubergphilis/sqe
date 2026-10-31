**STATUS 2026-10-31: Renovate stack grouping (`chore/renovate-stack-grouping`).**

Three red Renovate MRs, all waiting on the same upstream release:

- !1133 arrow/parquet 59 -> 60 (major group). Its `Cargo.toml` also lost
  `parquet` from `[workspace.dependencies]`, so `cargo metadata` fails.
- !1465 `parquet-variant-*` / `parquet-geospatial` 59 -> 60. Not matched by
  the stack rule (`parquet` only), so the preset "arrow-rs monorepo" group
  took it; two `arrow_schema` versions in the graph.
- !1107 sqlparser 0.62 -> 0.63 (minor group). `Expr::Cast` lost its `array`
  field (`sqe-sql` `trino_compat.rs`, `v3_types.rs`).

DataFusion 55.2.0 (latest on crates.io, 2026-10-07) requires arrow/parquet
`^59.2` and sqlparser `^0.62`; arrow 60.1.0 shipped 2026-10-28. No MR can
merge until a DataFusion release on arrow 60 exists.

Change: the stack rule matches `parquet-*` and sets
`separateMajorMinor: false`, so the next run folds all three into one MR.
Validated with `renovate-config-validator --strict` (Homebrew node@24 is
broken by a missing simdjson dylib; run the validator under node@22).

Arrow 59.3.2 (2026-10-26) is a patch on our line; the 7-day floor clears
2026-11-02 and the minor-and-patch group picks it up.

**NEXT:** when the grouped MR appears, close !1133, !1465 and !1107. When a
DataFusion release on arrow 60 lands: bump the stack together, adapt
`Expr::Cast` for sqlparser 0.63, re-check the vendored iceberg-rust and the
`object_store` quick-xml patch.
