# Findings: 11-why-distribute-at-all.md

## Thesis
Distribution is a measured trade-off, not a default. Start single-node, find the empirical crossover point where I/O parallelism beats coordination overhead, and let per-query guard clauses decide whether to distribute.

## Opening
> The fastest distributed query is the one that runs on a single node.
>
> SQE worked. By the end of Part III, we had a query engine that authenticated users via OIDC...

Verdict: strong hook. The epigraph is a sharp inversion, and "SQE worked. ... One binary. One process. No cluster." lands with rhythm before pivoting to the real question.

## Closing
> The fastest distributed query is the one that runs on a single node. Don't distribute until you must. And when you must, the next three chapters will show you how.

Verdict: lands it. Callback to the epigraph, a directive, and a forward pointer to Part IV. No trailing summary.

## Voice & editorial issues
1. L16 "This is the part of the story where many teams make their first mistake." Followed by "So you start planning distributed execution because you assume you'll need it eventually." The sentence is the longest run-on in the chapter (six chained verbs: "spin up... deploy... add... build... handle... and six months later"). It works as deliberate piling-on. No change required; flagging as the only borderline case.
2. L259 "I want to be explicit about this, because the rest of Part IV is about distributed execution and it would be easy to lose the thread." Mild throat-clearing / meta-commentary ("I want to be explicit about this"). The voice guide prefers stating the thing directly. Rewrite: "The rest of Part IV is about distributed execution, so the counter-case is easy to lose. Here it is." Minor.
3. L299 "without the complexity of optimal scheduling algorithms that NP-hard problems would require." Slightly tangled. The optimal scheduling problem IS NP-hard; the wording reads as if NP-hard problems impose the requirement. Rewrite: "without the complexity of optimal bin-packing, which is NP-hard." Minor clarity fix.
4. Positive note (not a defect): L250 "Honest beats ambitious." and the L253 field report are exactly on-voice (dry, earned). Load-bearing for the chapter's ethic.

No forbidden words found (checked "leverage", "utilize", "robust", "comprehensive", "essentially", "arguably", "delve", "facilitate", "this approach ensures", "this enables", "this allows for").

## Mechanical violations (PROSE only)
none

## Exclamation marks in prose
none. (Two `!` hits at L124 `return vec![];` and L280 `tokio::select!` are both code, excluded.)

## Continuity data
### Concepts INTRODUCED / defined here
- Crossover point -> data volume where distribution pays
- `split_files` -> round-robin file splitter (sqe-planner)
- `WeightedScheduler` -> largest-first bin-packing balancer
- `try_distribute` -> coordinator's five guard clauses
- `DistributedScanExec` -> fan-out scan node replacing IcebergScanExec
- Mode toggle / `mode = "coordinator"` -> one binary, optional workers
- Phase A (spill first) / Phase B (push down) -> hybrid memory strategy
- `FairSpillPool` -> watermark memory-pressure pool
- The distribution tax -> per-query overhead (30-50ms)

### Concepts ASSUMED (used as if already known)
- `IcebergScanExec` (existing physical-plan node; likely Part II/III planner chapter)
- Plan rewriting / policy rewrites (security chapter)
- CTAS, INSERT INTO, Arrow Flight SQL, OIDC, Polaris, Prometheus metrics (Part III recap, L5)
- Heartbeats / WorkerRegistry / worker health (deepened in ch12-14)
- `DoExchange`, two-phase aggregation, SortMergeJoin, GroupedHashAggregate (DataFusion knowledge assumed)
- information_schema virtual providers, `system.runtime.queries` (earlier chapters)

### Key factual / numeric claims
- Single-node hardware: 16 cores, 64GB RAM (L12, L90)
- 5GB Iceberg scan + aggregation: under two seconds; analyst query: 200ms (L14)
- Distribution was Phase 3 in week-one architecture docs (L18)
- TPC-H scale factors: SF1 ~1GB, SF10 ~10GB, SF50 ~50GB, SF100 ~100GB (L27, L94-98)
- Phase breakdown SF-100: parse+plan <1%, metadata+pruning 2-5%, Parquet scan 60-75%, filter+projection 5-10%, agg/join 10-20%, serialization <1% (L35-42)
- Parallel fraction 60-75% of wall clock (L69)
- Amdahl table @70% parallel: theoretical 1.0/1.5/2.1/2.6/2.9x vs actual 1.0/1.4/1.8/2.1/2.2x for 1/2/4/8/16 workers (L71-77)
- Three configs: single 16c/64GB; two workers 8c/32GB; four workers 4c/16GB; all 10Gbps (L90-92)
- SF100: two workers beat single node 30-40% scan-heavy, four workers 50-60% (L98)
- Crossover point: ~30-50GB scanned data per query (L100, L362)
- Unhealthy after 3 missed heartbeats (15 seconds) (L244)
- Fragment retry: up to two attempts per fragment (L296, L312)
- Serialization tax 1-5ms/fragment; network 5-20ms/fragment; total 4-worker tax ~30-50ms (L276, L278, L284)
- Small-query penalty: 100MB/200ms query => 15-25% penalty (L284)
- 1TB lineitem / 8 workers => 125GB per worker (L324, L326)
- DataFusion `GroupedHashAggregate` "does not spill today" (L328)
- S3 endpoints: RustFS in test, AWS S3 in production (L305)
- Config: `mode = "coordinator"`, worker ports 50052; legacy aliases hybrid/local/distributed (L182-187)
- Distributed joins/aggregation/locality/dynamic-scaling deferred; "joins -- not yet supported" (L250, L301-306)
- Trino spill opt-in/later versions; Spark spills aggressively (tungsten/ExternalSorter); DuckDB out-of-core 1TB on 16GB; ClickHouse materialized views + shards (L332-338)

### Cross-references
- L5 "By the end of Part III" (back ref)
- L46 "the key insight that drives everything in Part IV"
- L315 "what Chapters 12 through 14 are about"
- L359 "the next three chapters will show you how"
- L344-348 Phase A/Phase B (forward to distributed-execution chapters)

## Pacing
Flows well. Strong header-as-outline structure; broad-principle -> implementation -> edge-case progression is on-voice. Tables carry the comparisons (Amdahl, crossover, decision framework, phase breakdown). The "What We Built (And What We Deferred)" + "1TB Problem" sections are the densest stretch and run long, but the bullet inventory format keeps them scannable rather than a wall of text. No section drags or rushes.

## Grade
Voice adherence: A. Clean mechanics (zero emdash/arrow/emoji/exclamation in prose, no forbidden words), strong hook and callback close, consistent short-long rhythm, opinionated with shown reasoning, honest about deferrals. Only nits: one meta throat-clear (L259) and one tangled NP-hard clause (L299).
