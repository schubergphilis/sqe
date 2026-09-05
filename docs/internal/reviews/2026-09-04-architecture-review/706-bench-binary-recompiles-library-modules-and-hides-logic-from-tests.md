# 706: sqe-bench declares its modules twice (bin and lib), so the binary carries a private copy of the generators and most bench logic is unreachable from tests

- Scope: sqe-bench (main.rs, lib.rs)
- Severity: medium
- Category: maintainability
- Effort: S (hours)
- Status: open

## Problem

`crates/sqe-bench/src/lib.rs:8-9` exports:

```rust
pub mod generate;
pub mod sink;
```

and `crates/sqe-bench/src/main.rs:1-8` declares the same files again as private modules of the binary crate:

```rust
mod cli;
mod client;
mod comparison;
mod execute;
mod generate;
mod load;
mod profile;
mod sink;
```

`main.rs` never references `sqe_bench::` (verified by grep), so the binary compiles `generate/` (about 13,000 lines: `tpcds.rs` 4148, `tpce.rs` 2679, `tpch.rs` 1602, `tpcc.rs` 1273, `ssb.rs` 1282, `bank.rs` 1049, `clickbench.rs` 1024) and `sink/` (1300 lines) a second time as a different crate. `generate::TableDef` in the binary is a distinct type from `sqe_bench::generate::TableDef` used by `tests/generate_parallel.rs:75` and `tests/direct_iceberg_sink.rs:27`.

Everything the binary alone declares (`cli`, `client`, `comparison`, `execute`, `load`, `profile`, `query`, `report`, `run`, `status`, `suite`, `test`) is invisible to `tests/`. That includes `comparison::classify::classify_status` (the compare verdict logic), `query::prefix_tables` (SQL rewriting of benchmark queries), and `load::load_benchmark` (CTAS generation with table properties, bloom columns, partition specs). Their only tests are `#[cfg(test)]` blocks inside the same file, and `tests/single_pass_compare_gated.rs` cannot call the compare code at all.

## Why it matters

Two module trees for one file set means a `pub(crate)` or `#[allow(dead_code)]` decision made for the lib is wrong for the bin and vice versa, which is why `client/mod.rs:3` and `generate/mod.rs:22-24`, `:51`, `:62` carry blanket `allow(dead_code)`. The compare harness is the correctness oracle for the engine and its verdict logic cannot be integration-tested. Any refactor touching the generators is compiled twice, and a type mismatch between the two copies only shows up as a confusing "expected TableDef, found TableDef" if someone ever tries to bridge them.

Build time is out of scope for this review; this issue is about the split itself.

## Proposed change

1. Move every module into `lib.rs` as `pub mod` (or `pub(crate)` where no external user exists) and reduce `main.rs` to `fn main() { sqe_bench::cli::run() }`.
2. Delete the blanket `#![allow(dead_code)]` in `client/mod.rs` and the per-item allows in `generate/mod.rs`; fix or delete what is actually dead.
3. Add integration tests under `tests/` for `comparison::classify::classify_status` (all arms), `query::prefix_tables` (see issue 707), and `load` CTAS text generation (snapshot the SQL for one table per suite).

## Acceptance criteria

- `crates/sqe-bench/src/main.rs` contains no `mod` declarations.
- `cargo test -p sqe-bench` includes at least one test in `tests/` that imports from `sqe_bench::comparison` and one from `sqe_bench::load`.
- `grep -rn 'allow(dead_code)' crates/sqe-bench/src` returns nothing.

## Related

- 705 (shared clients).
- 707 (benchmark SQL table qualification).
- Memory: Unified bench harness (!653/!654); sqe-bench build pending.
