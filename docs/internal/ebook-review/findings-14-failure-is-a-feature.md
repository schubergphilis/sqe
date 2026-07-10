# Findings: 14-failure-is-a-feature.md

## Thesis
A 50-client load test broke distributed SQE in a dozen ways at once; the chapter walks each failure (gRPC hang, empty schema, S3 throttle, OOM, opaque errors) to its fix, arguing recovery mechanisms beat prevention and that error handling is part of the API contract.

## Opening
> The question is not whether workers will fail. The question is what happens to the query when they do.
Verdict: strong hook. Epigraph reframes failure as inevitable, then "Everything broke." (L10) lands the stakes in two words. No throat-clearing.

## Closing
> It doesn't catch everything -- you'd need hundreds of concurrent clients to reproduce the S3 throttle -- but it catches the failures that appear first.
Verdict: lands it. The prose ends honest about the test's limits rather than on a victory lap. The two callout boxes after (field report L883, AI logbook L889) are structural appendices, not the narrative ending, and the field report's "That's the point." (L887) closes the theme cleanly.

## Voice & editorial issues
1. **L15** `Before we talk about what failed, let's talk about how we tested.` -- borderline filler transition ("let's talk about"). Voice guide forbids throat-clearing intros. Rewrite: `What failed only makes sense once you know how we tested.`
2. **L555** `The question is: what should happen when memory runs out?` -- rhetorical question used as a transition (forbidden pattern). Rewrite: `The real question was what should happen when memory runs out, and the answer depends on the operator.`
3. **L227** `This seems obvious in retrospect.` -- sentence-initial "This" referring to the prior clause without naming the subject (CLAUDE.md: never start a sentence with "This" referring to the previous sentence). Rewrite: `The fix seems obvious in retrospect.`
4. **L589** `This is the same trick that MapReduce uses for large aggregations, applied at the query operator level.` -- same "This" rule. Rewrite: `Two-phase aggregation is the same trick MapReduce uses for large aggregations, applied at the query operator level.`
5. **L258** `This is one of those properties of async Rust that you appreciate most when you need it at 2am.` -- same "This" rule (subject = drop-cancels-future). Rewrite: `Drop-cancels-the-future is one of those properties of async Rust you appreciate most at 2am.`
6. **L708 / L116** `This degrades performance` and `This is the output that tells you` -- both lead with unanchored "This". Name the subject (`Local fallback degrades performance`; `That query tells you`).
7. **L477 / L704 / L281** chapter leans on the "X is the right primitive here" / "X isn't just a timeout mechanism" construction repeatedly. Mild repetition, not a hard rule break. Consider varying one.
8. **L307** `We considered making the budget configurable. We didn't, because two retries is enough...` -- exactly the right opinionated voice. No change; flagged as a positive anchor.

## Mechanical violations (PROSE only)
None. Grep for U+2014/2013/2192/2190/25B6 returns zero. Author uses `--` and ASCII `->` (the approved replacements) throughout, including the L670-677 bullet list.

## Exclamation marks in prose
None. Every `!` in the chapter is inside code fences (`tokio::select!`, `eprintln!`, `warn!`, `anyhow!`, `info!`). No prose exclamation.

## Continuity data
### Concepts INTRODUCED / defined here
- Concurrent load test rig -> `scripts/concurrent-test.sh`, N parallel CLI clients
- HTTP/2 stream accumulation hang -> reused gRPC channel wedges after ~30 queries
- Fresh-connection-per-query -> `FlightSqlBenchClient` avoids stream accumulation
- Empty-result schema bug -> `do_get` must return query schema, not `Schema::empty()`
- Fragment retry semantics -> `DistributedScanExec`, 2 attempts, `failed_workers` exclusion
- `mark_unhealthy` immediate demotion -> execution failure vs 3-strike health probe
- Local execution fallback -> coordinator runs scan when all workers fail
- Credential refresh push -> `CredentialRefreshTracker`, `do_action("refresh_credentials")`, watch channels
- FairSpillPool + watermark model -> green/yellow/orange/red utilization tiers
- External merge sort / k-way merge -> spill runs, bounded merge memory
- Two-phase aggregation -> q18 fix, hash-partition groups across workers
- Heartbeat over Flight -> `do_action("heartbeat")`, dynamic worker discovery via `or_insert_with`
- `SqeErrorCode` -> 27-variant typed error taxonomy
- Error classifier -> `classify_execution_error` / `classify_catalog_error`, order-dependent decision tree
- Failure taxonomy table -> 7 failure modes with detection + recovery

### Concepts ASSUMED (used as if already known)
- Distributed coordinator/worker split, fragment scheduling, `DoExchange` (ch13)
- OIDC bearer passthrough, per-user STS credential vending via Polaris (ch4, explicitly back-ref'd)
- DataFusion `SortExec`, `GroupedHashAggregate`, `MemoryConsumer`, `MemoryPool`
- Arrow Flight SQL `get_flight_info` / `do_get` contract, `do_action`
- Iceberg catalog commit / position-delete write path, RustFS, Polaris in-memory mode
- dbt + Trino error-code-aware retry behavior
- TPC-H queries, scale factor

### Key factual / numeric claims
- "TPC-H at scale factor 0.01 across two workers" (L6)
- Load test: 50 concurrent clients; 10 pass, 20 intermittent, 50 nothing reliable (L10, L118)
- gRPC hang "after about 30 queries" / "approximately 30 queries" (L125, L157)
- HTTP/2 max stream ID = 2^31 - 1, "roughly 2.1 billion" (L159)
- Fresh-connection overhead "about 1-2ms per query" (L202, L702)
- `build_channel`: keepalive 10s, keepalive timeout 20s, timeout 300s, connect_timeout 10s (L189-199)
- S3 503 SlowDown thresholds: 3,500 PUT/COPY/POST/DELETE or 5,500 GET/HEAD per second per prefix (L241)
- Query timeout deadline 120s (L252, L254)
- Heartbeat: 3 missed (15s) before unhealthy; credential background task every 60s; refresh buffer 300s / 5 min (L292, L420, L411, L416)
- Retry: `DEFAULT_MAX_RETRIES: u32 = 2`; backoff `50 * (1 << attempt)` = 100/200/400ms (L312, L361)
- Default worker memory limit 8GB (L533); FairSpillPool example "1TB sort with 512MB" -> ~2,000 runs of 512KB (L577)
- Watermarks: green <60%, yellow 60-75%, orange 75-90%, red >90% (L564-569)
- q18 GROUP BY HAVING SUM(l_quantity) > 300; breaks on 512MB at SF1; passes at SF0.1 within 512MB (L583, L591)
- 8 workers -> per-worker hash table ~1/8 size (L587)
- "twelve distinct failure modes ... fixed eight ... accepted four" (L668, L679); intro "a dozen things broke" (L118)
- 27-code error taxonomy `SqeErrorCode` (L744); Trino codes: TableNotFound=11, TypeMismatch=7, SyntaxError=1 (L774, L796)
- Spill dir `/tmp/sqe-spill` (L549)
- Load test cost: "one day to write and three days to work through" (L876); gRPC debug "four hours" (L127, L874)
- Crates touched: `sqe-coordinator`, `sqe-worker`, `sqe-bench`, `sqe-cli`, `sqe-core` (L876)
- Final run: 50 clients, all 50 passed, wall 14.2s, per-query avg 7.8s, throughput 3.5 q/s, two 200K-row scans (L883-887)
- Test table: 200K rows across two Parquet files (L78, L886)
- Pre-merge check: 10 clients mixed mode (L880)

### Cross-references
- L6: "Chapter 13 ends with queries being split across coordinator and workers" (back-ref, explicit)
- L401: "In Chapter 4, we established that every query runs as the authenticated user" (back-ref, explicit)
- L585: hash-aggregate spill "tracked in the DataFusion issue tracker" (external, unversioned)
- L587/L591: "SQE's Phase B" / "Phase A only (single-node)" used as known terms -- verify Phase A/B is defined in an earlier chapter (likely ch13). If not, dangling reference.

## Pacing
Flows well; strong short/long rhythm and the failure-by-failure structure is scannable. Two soft spots: "Testing Infrastructure" (L13-118) front-loads three code blocks before any failure appears, slightly delaying the payoff the opening promised. "Designing for Recovery" (L694-714) is six bolded principles back-to-back; inventories well but reads list-heavy after a code-dense chapter. No paragraph exceeds 5 sentences.

## Grade
Voice adherence: A-. Clean mechanically (zero emdash/arrows/prose-exclamations), strongly opinionated, honest about accepted failures and dead ends; held back only by ~5 sentence-initial unanchored "This" instances and two mild transition tics (L15, L555).
