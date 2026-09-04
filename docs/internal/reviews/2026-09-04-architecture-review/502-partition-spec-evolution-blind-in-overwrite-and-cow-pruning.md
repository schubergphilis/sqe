# 502: Dynamic INSERT OVERWRITE and CoW partition pruning ignore the data file's partition spec id

- Scope: sqe-coordinator write path (write_handler `commit_written_files`, `partition_prune_cow_files`)
- Severity: high
- Category: reliability
- Effort: M (days)
- Status: open

## Problem

Two places compare or interpret a data file's `partition()` struct as if every
live file were written under the table's current default spec. After
`ALTER TABLE ... ADD/DROP/REPLACE PARTITION FIELD` (supported in
`catalog_ops.rs`, see the `PartitionEvolution` handler around line 1600) a
table holds files from several specs, and `DataFile::partition_spec_id()` is
never consulted.

Dynamic overwrite, `crates/sqe-coordinator/src/write_handler.rs:4372-4396`:

```rust
let touched_partitions: Vec<iceberg::spec::Struct> = if partitioned { ... f.partition().clone() ... };
...
all_old.into_iter().filter(|df| touched_partitions.contains(df.partition())).collect()
```

A file written under an older spec has a struct with a different arity or
field order, so it never equals a new file's struct and is never removed. The
"replaced" partition keeps its old-spec rows next to the new ones.

CoW pruning, `write_handler.rs:6273-6308` takes
`table.metadata().default_partition_spec()` once and applies it to every file;
`partition_could_match` (`:6240-6270`) then indexes
`partition.fields().get(idx)` with the default spec's field index. For an
old-spec file that index can land on a different partition column, and when
the literal there "provably differs" the file is pruned from the rewrite. A
`DELETE ... WHERE region = 'EU'` can therefore leave EU rows alive in files
written before the spec change. The function's doc comment promises the
opposite ("false negatives would silently lose data").

`collect_data_files` (`write_handler.rs:4489-4531`) carries no spec id either,
so callers cannot notice.

The compaction crate already keys grouping on `partition_spec_id()`
(`crates/sqe-compaction/src/rewrite.rs:205-207`), so the codebase knows the
rule; the DML path does not apply it.

## Why it matters

Partition evolution is a shipped feature with an e2e suite
(`tests/it/partition_evolution_e2e.rs`, all `#[ignore]`) and is exactly the
situation in which operators run `INSERT OVERWRITE` (repartition a table) and
bulk `DELETE`. Both produce silently wrong table contents: duplicated rows after
overwrite, surviving rows after delete. Nothing errors, and the affected-row
count reported to the client is internally consistent with the wrong result.

## Proposed change

1. In `commit_written_files` overwrite branch: a live file whose
   `partition_spec_id()` differs from the default spec is treated as touched
   whenever any of the new files' partition values, projected through the old
   spec's source columns, could overlap. Simplest correct first cut: if any
   live file has a non-default spec id, fall back to removing all files whose
   spec differs (full-replace semantics for legacy-spec files) and log it; or
   refuse dynamic overwrite on mixed-spec tables with a clear error pointing at
   `rewrite_data_files` to normalise the layout first.
2. In `partition_prune_cow_files`: look up the file's own spec via
   `table.metadata().partition_spec_by_id(df.partition_spec_id())` and evaluate
   `partition_could_match` against that spec; files with an unknown spec are
   kept (never pruned).
3. Add `spec_id` to the file summary logged at `write_handler.rs:2210-2220`.

## Acceptance criteria

- Unit tests with fabricated `DataFile`s (pattern at
  `crates/sqe-compaction/src/rewrite.rs:634-647`, `data_file_part(..., spec_id, part)`):
  a file with `spec_id = 0` under a table whose default spec is 1 is never
  pruned by a WHERE on the new partition column.
- Un-ignore or add a sqlite-backed variant of the partition-evolution overwrite
  scenario: add a partition field, `INSERT OVERWRITE` one partition, assert no
  duplicates across old- and new-spec files.
- `DELETE` after partition evolution removes every matching row in both spec
  generations.

## Related

- 513 (sqlite write-test rig makes the above runnable in `cargo test`).
- Memory note "Partition-on-write gap" in `nextsteps.md` history.
