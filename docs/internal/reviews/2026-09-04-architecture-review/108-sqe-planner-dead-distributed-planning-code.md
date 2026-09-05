# 108: More than half of `sqe-planner` is distributed-planning code no binary calls

- Scope: sqe-planner
- Severity: medium
- Category: maintainability
- Effort: M (days)
- Status: open
- Issue: #551

## Problem

`CLAUDE.md` describes `sqe-planner` as "LogicalPlan -> PhysicalPlan,
partition-aware splitting". The crate's public surface (`lib.rs:19-62`)
exports a stage planner, shuffle operators, distributed sort/join/aggregate
rules, a predicate-transfer builder, an external aggregate and a Grace hash
join. Grepping every other crate's `src/` for those symbols (bare name and
`sqe_planner::` path, non-test code) finds no callers for:

| Module | Lines | Unused exports |
|---|---|---|
| `stage_planner.rs` | 848 | `decompose_plan`, `compute_waves`, `QueryStage`, `ShuffleType` |
| `distributed_join.rs` | 1343 | `BroadcastJoinRule`, `PreSortedJoinRule`, `BroadcastJoinPlan`, `ShuffleHashJoinPlan` |
| `shuffle_exec.rs` | 1073 | `ShuffleWriterExec`, `ShuffleReaderExec`, `ShufflePartitioning` (the only hit is a doc comment in `sqe-worker/src/shuffle.rs:176`) |
| `distributed_sort.rs` | 943 | `DistributedSortRule`, `DistributedSortExec`, `compute_range_boundaries`, `sample_based_boundaries` |
| `distributed_aggregate.rs` | 797 | `DistributedAggregateRule`, `PartialAggregateExec`, `FinalAggregateExec` |
| `grace_hash_join.rs` | 758 | everything except `choose_local_join_strategy` (called from `join_strategy.rs:126`) |
| `external_aggregate.rs` | 586 | `external_hash_aggregate`, `ensure_decomposable`, all types |
| `predicate_transfer.rs` | 528 | `build_predicate_transfer`, `extract_distinct_values`, `PredicateTransfer` |
| `sort_memory.rs` | 197 | `SortConsumer`; `SortMemoryPolicy` is used only by `sort_memory_rule.rs` |
| `splitter.rs` | 241 | `split_files` (`bin_pack_files` is live) |

That is roughly 6,900 of 11,431 lines. The live surface is `ScanTask`,
`HiveScanDescriptor`, `scan_morsel`, `bin_pack_files`, `JoinStrategyRule`,
`StarSchemaReorderRule`, `SortMemoryRule`, `SingleDistinctCountCompanionRule`
and `PuffinStatisticsRequestRule`.

The coordinator's actual distribution model has no stages or shuffles:
`sqe-coordinator/src/scheduler.rs:1-6` is a "Fragment scheduler - assigns
scan tasks to workers", `codec.rs:40-77` can encode exactly one node type
(`DistributedScanExec`), and `grep -rl 'stage_id\|ShuffleManager' sqe-coordinator/src`
is empty. The worker has its own `ExchangeDescriptor`/`ShuffleReceiver`
(`sqe-worker/src/shuffle.rs`) that nothing on the coordinator side drives.
The design note `docs/internal/...` memory entries describe the stage-based
MPP as a roadmap item, and these modules are that roadmap's scaffolding from
the Ballista period.

## Why it matters

The dead code is not free. Each of these modules implements DataFusion
`ExecutionPlan` / `PhysicalOptimizerRule` traits, which change on every minor
DataFusion release. The DF55/arrow59 upgrade is currently blocked on trait
migrations; part of that work is being spent on operators that never run.
Reviewers and new contributors read `lib.rs` and reasonably believe SQE has
range-partitioned distributed sort and broadcast joins. The 148 unit tests in
these files run in every `cargo test --all` and give a false sense of coverage
for a feature that does not exist in the shipped binary. `ShuffleType::Hash`
keys are `Vec<String>` produced by `format!("{}", expr)` (`stage_planner.rs:370`),
so if this code is ever revived it will need a redesign anyway.

## Proposed change

1. Decide per module: delete, or move behind a `distributed-mpp` cargo feature
   that is off by default and excluded from the workspace default members.
   Deleting is recommended for `stage_planner`, `distributed_*`, `shuffle_exec`
   and `split_files`; git history keeps them and the memory notes point at the
   commits.
2. Keep `predicate_transfer`, `external_aggregate` and `grace_hash_join` only
   if there is a dated plan entry that will wire them in; otherwise move them
   to `docs/internal/plans` as design notes with a pointer to the last commit
   that contained the code.
3. After removal, trim `sqe-planner/Cargo.toml` (`base64`, `serde_json`,
   `futures`, `tokio` may become unnecessary) and re-check
   `cargo tree -p sqe-planner`.
4. Update `CLAUDE.md` and the crate-level doc to describe the live surface:
   scan-task model, morsel planning, and the five optimizer rules.

## Acceptance criteria

- `cargo +nightly udeps -p sqe-planner` or a manual grep shows every `pub`
  item in `lib.rs` has at least one non-test caller outside the crate, or is
  behind the feature.
- `wc -l crates/sqe-planner/src/*.rs` total under 5,000 lines with the feature
  off.
- `cargo build --all` and `make test-distributed` unchanged.
- `CLAUDE.md` crate table row for `sqe-planner` matches the code.

## Related

- Memory notes on the Ballista evaluation and the "Distributed MPP
  architecture" roadmap.
- `docs/site/book/src/design-notes/ballista-evaluation-learnings.md`.
- `109-scan-task-version-invariant-not-enforced.md` (the live part of the crate).
- The DF55 upgrade blocker (memory: `!886 blocked by apply_expressions migration`).
- `600-doexchange-shuffle-is-unreachable-but-exposed.md` is the worker-side half of the same dead shuffle path.
