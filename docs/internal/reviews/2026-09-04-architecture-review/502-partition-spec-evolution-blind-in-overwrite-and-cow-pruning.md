# 502: Dynamic INSERT OVERWRITE and CoW partition pruning ignore the data file's partition spec id

- Scope: sqe-coordinator write path (write_handler `commit_written_files`, `partition_prune_cow_files`)
- Severity: high
- Category: reliability
- Effort: M (days)
- Status: fixed (branch `fix/data-consistency-p1`, 2026-09-05)
- Issue: #594

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

## Resolution (2026-09-05)

Both places now consult `DataFile::partition_spec_id()` instead of assuming
every live file was written under the table's current default spec.

**What changed** (`crates/sqe-coordinator/src/write_handler.rs`)

- `partition_prune_cow_files` looks each file's own spec up with
  `table.metadata().partition_spec_by_id(df.partition_spec_id())` and evaluates
  `partition_could_match` against *that* spec. A file whose spec id is not in
  the metadata is kept, and so is one written under a spec with no fields. The
  old code took `default_partition_spec()` once and indexed every file's
  partition struct with the default spec's field index, so an old-spec file's
  value for one column was compared against a constraint on another — a
  `DELETE ... WHERE region = 'EU'` pruned files that did hold EU rows.
- `commit_written_files`' dynamic `INSERT OVERWRITE` branch reads the default
  spec from the *reloaded* snapshot inside the retry closure and treats every
  live file whose `partition_spec_id()` differs from it as touched: legacy-spec
  files are removed wholesale (full-replace semantics for the generations the
  writer cannot address partition-by-partition), with a `warn!` naming the spec
  ids and pointing at `rewrite_data_files` to normalise the layout. Before this
  an old-spec file's partition struct never compared equal to a new file's, so
  the "replaced" partition kept its old-spec rows next to the new ones.
- `distinct_spec_ids(&[DataFile])` feeds a `spec_ids` field into the CoW
  DELETE/UPDATE rewrite summaries, the truncate summary and the INSERT
  OVERWRITE swap summary (which also logs `default_spec_id`), so a rewrite
  spanning spec generations is visible in the log.

**Tests** (`cargo test -p sqe-coordinator --lib -- write_handler`, 132 passed /
0 failed)

- `write_handler::tests::partition_prune_keeps_files_from_an_older_partition_spec`
  — a table whose spec 0 partitions by `country` and whose default spec 1
  partitions by `region` (both string, both at index 0: the shape that made the
  old code misread). `DELETE ... WHERE region = 'EU'` keeps the spec-0
  `country=US` file and the spec-1 `region=EU` file, prunes only the spec-1
  `region=US` file. The test also pins the bug's fingerprint: evaluated against
  the DEFAULT spec, that same spec-0 file *does* look provably different, which
  is exactly what used to prune it.
- `write_handler::tests::partition_prune_keeps_files_with_an_unknown_spec_id`
- `write_handler::tests::distinct_spec_ids_reports_every_generation_once`

The fabricated `DataFile`s follow the `data_file_part(..., spec_id, part)`
pattern from `crates/sqe-compaction/src/rewrite.rs`; the table is built from
JSON metadata with a local fs `FileIO`, so no catalog is involved.

**Not covered by unit tests.** The two end-to-end scenarios (add a partition
field, `INSERT OVERWRITE` one partition, assert no duplicates across spec
generations; `DELETE` after evolution removing matching rows in both
generations) need the docker rig — the `#[ignore]`d
`tests/it/partition_evolution_e2e.rs` is where they belong.
