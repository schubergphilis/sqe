## Context

SQE's Iceberg feature coverage gap is not a build problem. Most of what's missing already exists in the vendored `iceberg-rust` tree (`vendor/iceberg-rust/crates/iceberg/src/`): position delete writer, equality delete writer, Puffin reader/writer, `rewrite_files`/`rewrite_manifests`/`remove_orphan_files`/`remove_snapshots` transaction actions, `TimestampNs`/`TimestamptzNs` primitive types, `NestedField::initial_default`/`write_default`. The gap is that these capabilities have no SQL surface, no planner integration, and no tests.

The other half of the gap is catalog coverage. Apache iceberg-rust ships `iceberg-catalog-glue`, `iceberg-catalog-hms`, and `iceberg-catalog-sql` as production-ready workspace crates that SQE has never pulled in.

Full reference roadmap: `docs/superpowers/plans/2026-04-24-iceberg-matrix-parity.md` (the source this openspec change was converted from).

## Goals / Non-Goals

**Goals:**
- Lift SQE from 58/189 (31%) to ~156/189 (83%) on the icebergmatrix.org rubric within 4-6 months.
- Land Phase A-H as an atomic matrix-visible tier. Each phase is independently shippable and mergeable.
- Keep all Phase changes upstream-compatible so we can push patches back to apache/iceberg-rust where relevant.
- Submit SQE to the public matrix after Phase A, update submission after each subsequent phase.

**Non-Goals:**
- Variant type, shredded Variant, geometry type, vector type, multi-argument partition transforms, lineage. These are deferred with documented reasons.
- Catching up to Spark 93% in this cycle. We stop at ~83% because the remaining gap is V3 types blocked on upstream (arrow-rs #9790 for shredding, datafusion #12644 for UDT).
- Rewriting the CoW DML path. Phase H adds MoR as an alternative, not a replacement. CoW remains the default.
- Redesigning the pluggable-catalogs trait hierarchy. Phase A extends the existing `CatalogBackend` trait from the active `pluggable-catalogs` change rather than redesigning it.

## Decisions

### 1. One umbrella change, eight phases

We use a single openspec change `iceberg-matrix-parity` with phased tasks.md rather than eight separate changes. Rationale: the phases share a unified success criterion (matrix score) and it is easier to reason about the target tier as one tracked artifact. Each phase is still a separate git branch and PR. Phase A overlaps with the existing `pluggable-catalogs` change, which retains authority on catalog trait design; Phase A here tracks only the adoption-and-wire work.

Alternative considered: eight separate openspec changes. Rejected because the matrix score is the cross-cutting metric and would require a ninth "rollup" change anyway.

### 2. Cherry-pick upstream where draft; build wrappers where missing

**Cherry-pick:** iceberg-rust PR #2203 (RowDeltaAction) for Phase E. The PR is a draft but the design is settled. Risk of upstream re-work is low. If upstream rebases, we rebase our cherry-pick; that is the standard cost of vendoring.

**Build wrappers:** Branch/tag transactions (Phase C), table maintenance procedures (Phase B), Puffin emission (Phase F). In all three cases the low-level primitives exist in vendored iceberg-rust (`TableUpdate::SetSnapshotRef`, the four `rewrite_*`/`remove_*` actions, `puffin::writer::PuffinWriter`). We only add thin convenience APIs.

**Wait:** DataFusion `StatisticsSource` trait (datafusion#21157, DF 54) for Phase F consumer side. MERGE INTO native plan (datafusion#20746, DF 55+) for Phase H simplification. In both cases we can ship without them and upgrade later.

Alternative considered: wait for all upstream PRs to merge. Rejected because timelines are unpredictable and the matrix score is blocked.

### 3. Catalog sweep via apache/iceberg-rust workspace crates, not third-party

Phase A adopts `iceberg-catalog-glue`, `iceberg-catalog-hms`, `iceberg-catalog-sql` from the apache/iceberg-rust workspace. Alternative: JanKaul/iceberg-rust parallel ecosystem (`iceberg-sql-catalog`, `iceberg-file-catalog`). Rejected because: (a) apache crates are ASF-governed and more likely to stay maintained, (b) using the apache workspace keeps us aligned with the vendored `iceberg` crate's lock-step versioning, (c) fewer forks to track.

### 4. MoR and CoW coexist; table property decides

Phase H adds MoR as a second DML strategy, keyed off table properties `write.update.mode`, `write.merge.mode`, `write.delete.mode` per the Iceberg spec. Default remains `copy-on-write` for compat. Benchmarks (TPC-C SF100 `trade_result_update_holding`) will determine when MoR becomes the recommended default.

Alternative considered: replace CoW with MoR once MoR lands. Rejected because CoW is currently the only path that produces Spark-readable outputs with simple schemas; some users prefer its read performance.

### 5. Puffin stats writer ships ahead of the DataFusion consumer

Phase F emits Puffin sidecars on commit even before DataFusion #21157 lands. Reason: the data is cheap to produce (theta sketch per column is O(rows) amortised) and forward-compatible. When DF 54 ships `StatisticsSource`, we wire the consumer side without revisiting writes.

### 6. CDC range scan first, changelog view second

Phase G ships snapshot-range incremental scan (`FOR INCREMENTAL BETWEEN SNAPSHOT x AND SNAPSHOT y`) before the full changelog view with `_change_type`/`_change_ordinal`. Range scan covers the common dbt use case (incremental models). Full changelog requires upstream iceberg-rust #1636 and adds significant test surface.

### 7. Nanosecond and column defaults exposed together in Phase D

Both primitives already exist in vendored iceberg-rust. The exposure work is a shared Phase D because they share DDL parser changes and test harness. Splitting would duplicate effort.

### 8. SQL surface consistent with Trino where it exists

New SQL syntax follows Trino first, then Spark. Examples:

- `CALL system.rewrite_data_files(table => 'schema.table')` (Trino-style procedure calls)
- `ALTER TABLE t CREATE BRANCH name` (Spark/Flink-compatible)
- `SELECT ... FOR VERSION AS OF 'branch_name'` (Iceberg spec)
- `SELECT ... FOR INCREMENTAL BETWEEN SNAPSHOT x AND SNAPSHOT y` (SQE-specific; no Trino equivalent)

Rationale: most SQE users migrate from Trino. When Trino and Spark diverge, follow Trino unless Spark is the canonical reference (branching syntax is).

## Risks / Trade-offs

**[Risk] Upstream cherry-pick conflicts with RisingWave fork rebases.** Phase E cherry-picks iceberg-rust #2203 from apache/iceberg-rust main. The RisingWave fork rebases irregularly. Each rebase risks conflicts in our cherry-picked patch.
Mitigation: pin RisingWave HEAD at cherry-pick time. Track upstream weekly. Own the rebase work as part of Phase E maintenance.

**[Risk] DataFusion 54 rebase gated by RisingWave fork.** Phase F consumer side and general planner improvements are blocked on DF 54, which requires RisingWave to rebase off DF 52.1.
Mitigation: Phase F writer side ships on DF 53 today. Consumer side waits. Track RisingWave fork monthly.

**[Risk] Matrix submission gets contested.** Our scoring may disagree with the matrix maintainers. Example: we rate `partial` where they'd rate `none`.
Mitigation: err conservative on the PR. Let the maintainers upgrade ratings after review. Link to integration tests in the support entry `links` field.

**[Risk] MoR writes produce files unreadable by older Spark versions.** Spark 3.x readers with position-delete support bounded to specific format versions.
Mitigation: Phase H default remains CoW. Document the compat matrix in `docs/iceberg-writes.md`. Add integration test that round-trips MoR output through Spark 4.1 and Trino 465.

**[Risk] Catalog sweep (Phase A) bloats dependency tree with AWS SDK, Thrift, multiple DB drivers.** Each new catalog pulls transitive deps.
Mitigation: make each catalog a Cargo feature flag. Default build includes only REST (polaris + nessie). Users opt into `glue`, `hms`, `sql` features.

**[Risk] Branching and tagging SQL diverges from upstream if Iceberg spec clarifies later.** Iceberg V3 may tighten branch/tag semantics.
Mitigation: track iceberg-rust #1939 and any spec clarifications. The SQL syntax is reversible; internal data is standard TableUpdate entries.

**[Trade-off] Matrix score is not our primary goal.** We optimise for feature parity with Spark 4.1 where it makes sense, not for matrix points. Some rows score `.` intentionally (e.g., Hadoop catalog) because the use case is marginal.

## Migration Plan

**Rollout:** Each Phase lands as one or more PRs against `main`. No long-lived feature branches. Behind feature flags where applicable (Cargo features for Phase A catalogs, table properties for Phase H mode).

**Rollback:** Per-phase. Phase A catalogs are additive and can be removed by disabling their Cargo feature. Phase B-G introduce new SQL syntax; parser tolerates the syntax being disabled via config flag. Phase H is table-property-gated; setting mode back to CoW returns to prior behaviour.

**Versioning:** Each phase increments minor version. Phase A = v0.16.0, Phase B = v0.17.0, etc. Matrix submission follows the v0.20.0 release after Phase H.

## Open Questions

1. **Do we submit SQE to icebergmatrix.org after Phase A (partial), after Phase D (substantive), or after Phase H (complete)?** Current plan: submit after Phase A with honest partial ratings, update the PR as each subsequent phase lands. Feedback from the matrix maintainers may suggest waiting until later.

2. **Does the existing `pluggable-catalogs` change absorb Phase A, or does Phase A remain tracked here?** Pragmatic answer: Phase A tasks live here but implementation changes reference the pluggable-catalogs design doc. `pluggable-catalogs` remains the canonical design artifact. When implementation completes, archive `pluggable-catalogs` and point future readers at the merged state.

3. **Do we build OpenLineage emission as part of this change (Phase G adjacency) or defer?** Current plan: defer to next cycle. Lineage-as-feature is matrix row `lineage`, not `cdc-support`.

4. **RisingWave fork ownership post-matrix-parity.** We're carrying more local patches (Phase C branch transaction API, Phase E RowDeltaAction). When does it make sense to send these upstream or own a real fork?

5. **Vector type: reconcile with Step 6c (Lance-based vector search).** Current position: Lance is the primary path for real vector workloads; Iceberg V3 vector type is deferred until the spec finalises. Revisit when a user asks for Iceberg-native vectors.
