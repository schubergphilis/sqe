# SF10 resource controls — 2026-10-02

Base: `7be79b90`. Existing integration stack: Polaris, RustFS, namespace `tpch_sf10`. Data was attached read-only; nothing was regenerated. SF100 is deferred at the user's request.

## Per-execution admission (#798)

Streaming and buffered read execution receive a fresh query memory pool. Atomic admission enforces `query.max_query_memory` and delegates reservations to the shared coordinator pool, preserving its global limit. Session state retains its catalogs, object-store registry, caches and disk manager. Buffered output reserves once through this layered pool. Resource refusals retain RESOURCE_EXHAUSTED through Flight and query/audit tracking.

The cap bounds fallible DataFusion operator reservations, including hash builds without dependable estimates. It is not a process RSS limit: untracked allocations and MemoryPool's contractual infallible grow remain outside hard admission. Write-specific execution and a general CPU/cardinality admission gate remain separate work. This advances #784/#755 without changing uncertain joins to serial sort-merge plans.

Tests: 766 coordinator library tests pass, one ignored, with `RUST_MIN_STACK=33554432` for the local test harness. Coordinator library Clippy with `-D warnings` passes. New cases cover independent query limits, global refusal rollback, concurrent consumers, shared runtime resources, failed hash-build cleanup/light-query recovery and typed Flight resource refusals.

## Bounded live SF10 probes

Debug coordinator: global pool 4 GiB, query cap 1 GiB, timeout 300 seconds. q03 returns 10 rows; q09 returns 175 rows. Both pass individually and concurrently. Under an intentionally small 1 MiB query cap, SF10 q09 is refused at hash-build admission after 1.36 s (901,752 bytes reserved, 278,584 requested); SELECT 1 immediately afterward returns one row successfully.

| Run | q03 | q09 | Sampled peak coordinator RSS |
|---|---:|---:|---:|
| Current baseline, single query | 7.417 s | 18.421 s | not sampled |
| Current baseline, concurrent | 14.279 s | 31.003 s | 753.8 MiB |
| Query cap enforced, concurrent | 16.160 s | 43.560 s | 726.5 MiB |

These are safety probes, not a controlled timing A/B: compilation ran concurrently, the build is debug, and cache state differs. No throughput improvement or regression is inferred. The live low-cap probe preceded the final typed Flight error mapping; its allocation refusal/recovery is valid, while typed status is covered by the new encoder regression.

## Historical performance comparison

| Recorded run | q03 | q09 | All 22 queries |
|---|---:|---:|---:|
| 2026-07-21 | 6.835 s | 12.149 s | 196.409 s |
| 2026-09-28 | 3.025 s | 9.305 s | 66.052 s |
| 2026-09-29 | 1.671 s | 4.973 s | 46.065 s |

Source: checked-in `benchmarks/results/tpch-sf10-flight-<timestamp>.json`. These reports do not record build mode, machine, memory limits, cache state or dataset snapshot. July's q11/q20 row counts differ from September's, so those datasets are not assumed identical. Today's debug timings are slower than the September records, but cannot establish a code regression. A quiescent optimized full-suite run is needed before judging current performance.

## Worker byte backpressure (#407)

The live implementation already has a worker-wide shared ScanBudget with separate fetch/decode lanes and byte permits retained through Flight consumption (commit `f35ed904`). The shared-budget tests pass: eight concurrent scans complete without deadlock, and cancellation releases the service-wide charge. All 26 worker integration tests pass, including credential refresh, cancellation and zero-pruning scans. Both slow-consumer gates pass: queued peak 5,570,560 bytes and tracked peak 8,847,360 bytes under a 64 MiB budget, including a paused client.

This is local integration qualification. It does not establish separate-host worker scaling or SF100 scan/spill qualification for #786/#788.

## Removed governor admission (#380)

The proposed governor admission code was removed in `255f2daf`; `sqe-spill::governor` now sizes the memory pool. #380 duplicates closed #384 and refers to that removed path. The live query admission gap is tracked separately as #798.
