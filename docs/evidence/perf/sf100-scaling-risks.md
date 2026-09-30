# SF100 readiness and qualification

Updated: 2026-09-30. Reviewed code: `29aa2b0d189bf7ceb5def3eb9289e27c57bafe88`.

SF100 readiness is not established by the available SF1 results. The next work is
bounded generation and execution memory, smaller join intermediates, and repeatable
qualification across separate hosts. No new benchmarks were run for this update.

The [historical scaling review](sf100-scaling-risks-2026-06-historical.md) retains
the earlier DataFusion 53 measurements. Its implementation status and failure-mode
claims describe that period and must not be applied to the current DataFusion 55
build without verification.

## Current implementation and remaining evidence

| Area | Current code evidence | Remaining work |
|---|---|---|
| Generation | Local implementation now lazily produces SSB, TPC-DS and TPCBB batches; direct Iceberg writer consumes its bounded channel without collecting a shard | Value fingerprints and staged round trips pass; complete SF100 loading still needs qualification |
| Scan parallelism | `ParallelProbeScanRule` and `ParallelScanRule` exist; Iceberg scan has fetch-ahead and decode admission | Measure throughput and byte budgets; do not treat single output as a universal current bottleneck |
| Large joins | `JoinStrategyRule` rewrites only exact over-threshold build estimates; absent/inexact estimates keep hash joins | Bound uncertain large builds without repeating the historical coalesce regression |
| Sort memory | `SortMemoryRule` provides fail-closed merge-headroom admission; adaptive sort and spill protections exist | Qualify concurrent sort/join/aggregate completion and fairness under constrained memory |
| Worker transport | Worker service has finite Flight-frame budgets and scan budget wiring | Prove end-to-end decode/transport accounting under slow consumers; track [#407](https://sbp.gitlab.schubergphilis.com/vpf-data-ai/chameleon/applications/sqlengine/-/work_items/407) |
| Result collection | Local implementation uses actual retained Arrow byte reservations and the configured row cap | Lifetime and shared-budget tests pass; qualify concurrent workloads for [#780](https://sbp.gitlab.schubergphilis.com/vpf-data-ai/chameleon/applications/sqlengine/-/work_items/780) |
| Distributed execution | Worker scan/distribution paths exist | Verify which operators execute on each node and qualify 1/2/4 workers on separate hosts; track [#416](https://sbp.gitlab.schubergphilis.com/vpf-data-ai/chameleon/applications/sqlengine/-/work_items/416) |

This inspection does not establish that every operator or shuffle path spills.
Successful plan admission is not proof of bounded process RSS or completion under
concurrency. Older predictions of universal OOM and claims that parallel scan is
unimplemented are superseded by the table above.

Source references:

- [SSB generation](../../../crates/sqe-bench/src/generate/ssb.rs) and
  [TPC-DS generation](../../../crates/sqe-bench/src/generate/tpcds.rs).
- [Streaming generator infrastructure](../../../crates/sqe-bench/src/generate/mod.rs).
- [Scan tuning](../../../crates/sqe-catalog/src/iceberg_scan.rs) and
  [physical rule pipeline](../../../crates/sqe-coordinator/src/post_planning.rs).
- [Join decision](../../../crates/sqe-planner/src/join_strategy.rs) and
  [sort memory guard](../../../crates/sqe-planner/src/sort_memory_rule.rs).
- [Worker Flight budgets](../../../crates/sqe-worker/src/flight_service.rs).

## Prioritized experiments

1. **Lazy fact generation.** Reuse bounded batch/range iterators for staged
   Parquet and direct Iceberg loading. Preserve deterministic relationships and
   planted rows. Audit inventory and dimension formulas: a suite's scale label
   alone does not specify its memory footprint.
2. **Uncertain join builds.** Capture statistics precision and actual build
   bytes on TPC-H q09/q18 and TPC-DS q39/q72. Prototype conservative admission or
   a partition-preserving spill fallback. Blanket conversion of unknown builds
   previously serialized joins and is not a safe default.
3. **TPCBB q01 pre-aggregation.** Its item-only fact-to-fact join creates a
   many-to-many intermediate before aggregation. Aggregate facts by item first
   in a separate prototype, preserving multiplicities and non-null AVG counts.
   Keep the original query as the benchmark baseline. Item domains also scale;
   quadratic growth is not established. Measure actual cardinalities.
4. **Scan concurrency.** Counterbalance a fetch-ahead/output-partition sweep at
   SF10. Capture storage request latency/count, bytes, decode CPU, stream waits,
   repartition work and RSS. Choose byte-bounded concurrency before scaling.
5. **TopK and layout.** Trace ClickBench q23-q26 filters through SQE's custom
   scan and physical rewrites. Compare identical rows in unsorted,
   EventTime-clustered and SearchPhrase-clustered layouts. For wide q23,
   investigate delayed payload decode. Include write/layout cost.
6. **Concurrent spill.** Stress sorts, joins and aggregates with constrained
   memory and slow clients. Account for merge buffers, retained output, staging
   and transport together. Test cancellation, disk saturation and cleanup.

Upstream DataFusion exposes TopK dynamic filters and sort pushdown
([configuration documentation](https://datafusion.apache.org/user-guide/configs.html)).
Their availability does not prove SQE's custom scan applies them or benefits on
these layouts; verify plans and scan counters before changing defaults.

## Existing SF1 observations

The September 30 artifacts report:

| Suite | SQE total | Trino total | Trino/SQE | Comparison status |
|---|---|---|---|---|
| Bank | 3,305 ms | 8,655 ms | 2.62x | 8 Match |
| ClickBench | 1,716 ms | 7,850 ms | 4.57x | 41 Match, 2 Vacuous |
| TPCBB | 31,285 ms | 183,653 ms | 5.87x | 10 Match |

TPCBB q01 is 18,522 ms, 59.2% of SQE's suite time. ClickBench q23-q26 are
slower than Trino. These single-artifact observations prioritize profiling; they
are not repeated-run speedup estimates, full value-level oracles, or SF100 evidence.

Artifacts are `benchmarks/results/compare-{bank,clickbench,tpcbb}-sf1-2026-09-30*.json`.
The [August parallel-scan A/B](scan-parallelism-sf1-ab-2026-08-18.md) and
[September NDV A/B](puffin-ndv-ab-2026-09-04.md) show why plan changes and repeated
counterbalanced runs matter. Do not enable statistics or increase filter caps
solely because a configuration sounds beneficial.

## Qualification sequence and gates

1. Establish SF10 on separate Linux hosts with identical data, comparable
   storage routes, pinned engine/config versions, and result cache disabled.
   Record file/row-group sizes, clustering, compression, hardware, memory limits
   and network capacity. Separate cold and warm runs; counterbalance run order.
2. Use SSB q4.1/q4.3, TPC-H q09/q18, TPC-DS q39/q72, ClickBench q23-q26 and
   TPCBB q01 as the diagnostic set. Capture physical plans and actual operator
   rows alongside scan bytes, CPU, peak RSS, spill, network and worker imbalance.
3. Repeat at SF30, then SF100, first serially and then with concurrent workloads.
   Run at least three measured repetitions per setting and report medians/ranges;
   use a longer run for meaningful tail latency under sustained concurrency.
4. Compare 1/2/4 workers, reporting latency and total CPU/I/O/network resource
   use. Do not assume scan distribution also distributes large joins/aggregates;
   document their execution location from the actual plans.
5. Require full result-value checks with appropriate floating-point tolerances,
   bounded RSS below configured process limits, no process OOM, and released
   permits/reservations/spill files after cancellation. Resource exhaustion must
   be a clean error. Record spill disk capacity and throughput.

Choose latency and throughput targets from the intended deployment before declaring
readiness. No universal SF100 speedup or linear-scaling target is implied here.

## Tracking

Existing work: [#416](https://sbp.gitlab.schubergphilis.com/vpf-data-ai/chameleon/applications/sqlengine/-/work_items/416) (separate-host qualification), [#407](https://sbp.gitlab.schubergphilis.com/vpf-data-ai/chameleon/applications/sqlengine/-/work_items/407) (worker backpressure),
[#380](https://sbp.gitlab.schubergphilis.com/vpf-data-ai/chameleon/applications/sqlengine/-/work_items/380) (memory governor), [#382](https://sbp.gitlab.schubergphilis.com/vpf-data-ai/chameleon/applications/sqlengine/-/work_items/382) (spill-store safety), [#755](https://sbp.gitlab.schubergphilis.com/vpf-data-ai/chameleon/applications/sqlengine/-/work_items/755) (cardinality admission),
and [#780](https://sbp.gitlab.schubergphilis.com/vpf-data-ai/chameleon/applications/sqlengine/-/work_items/780) (buffered result byte accounting). New experiment issues:

- [#783 — [PERF][SF100] Stream SSB, TPC-DS and TPCBB fact generation](https://sbp.gitlab.schubergphilis.com/vpf-data-ai/chameleon/applications/sqlengine/-/work_items/783)

- [#784 — [PERF][SF100] Bound large hash builds when statistics are inexact or absent](https://sbp.gitlab.schubergphilis.com/vpf-data-ai/chameleon/applications/sqlengine/-/work_items/784)

- [#785 — [PERF][SF100] Prototype semantics-preserving pre-aggregation for TPCBB q01](https://sbp.gitlab.schubergphilis.com/vpf-data-ai/chameleon/applications/sqlengine/-/work_items/785)

- [#786 — [PERF][SF100] Qualify scan fetch-ahead and output parallelism under byte budgets](https://sbp.gitlab.schubergphilis.com/vpf-data-ai/chameleon/applications/sqlengine/-/work_items/786)

- [#787 — [PERF][SF100] Verify TopK pushdown and clustered layouts for ClickBench q23-q26](https://sbp.gitlab.schubergphilis.com/vpf-data-ai/chameleon/applications/sqlengine/-/work_items/787)

- [#788 — [PERF][SF100] Qualify concurrent sort, join and aggregate spill with bounded memory](https://sbp.gitlab.schubergphilis.com/vpf-data-ai/chameleon/applications/sqlengine/-/work_items/788)

## Local implementation follow-up (2026-09-30)

See [implementation evidence](sf100-performance-implementation-2026-09-30.md) for
changes, tests, measurements and limitations. Generation (#783), buffered output
(#780), and buffered JSON scheduling (#782) now have local implementations. The
TPCBB candidate (#785) has a separate SQL variant, a repeatable experiment and an
engine equivalence test. These changes are published for review in the merge requests linked in the implementation evidence; they have not been merged or deployed.
Join strategy (#784), scan tuning (#786), TopK/layout (#787), concurrent spill
qualification (#788) and separate-host qualification (#416) remain open. No
unmeasured default join-mode, pool-policy or concurrency change is included.
