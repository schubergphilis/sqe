# Findings: 16-benchmarks-dont-lie.md

## Thesis
A benchmark suite is only useful if the measurement is honest: this chapter documents SQE's seven-suite `sqe-bench` harness, the SQL-compatibility and caching bugs it surfaced, the SQE-vs-Trino results, and the hard-won lesson that agreement between two engines is not validation.

## Opening
> We said SQE was fast. The team believed it. The architecture diagrams looked right. DataFusion is fast. Rust is fast.
Verdict: strong hook. Short declaratives building to "None of that matters until you run the queries and measure." sets up the whole chapter cleanly.

## Closing
> The hard part is knowing which numbers to look at. The harder part is knowing when a clean report means nothing happened.
Verdict: lands it. Earned callback that escalates the prior chapter-internal refrain instead of summarizing. Note: this is the close of "Agreement Is Not Validation," but a later section ("The Rig Was the Bug") follows it, so the book's actual last line is the AI Logbook at L795. See Pacing.

## Voice & editorial issues
1. **L631** "SQE is 40% faster than Trino on single-table scans. SQE is 30% slower than Trino on complex multi-way joins." These two specific percentages appear nowhere in the chapter's tables. The scan tables show 2.5x-4.6x (not 40%) and the join regressions are stated as multiples (q72 ~13x, q39 OOM), not "30% slower." The round numbers read as invented illustrative figures and clash with the chapter's "222 queries of truth" ethos. Rewrite to reference the real figures, e.g. "SQE is 2.5x faster than Trino on single-table scans. SQE is 13x slower on q72's ten-table join."
2. **L375** "Auth overhead." as a standalone two-word sentence fragment opening a paragraph. The preceding paragraphs in that section flow as prose; this fragment reads like a leftover heading or table cell. Rewrite: "Then there is auth overhead." or fold into the sentence that follows.
3. **L739 and L772** both end sections with the identical line "The hard part is knowing which numbers to look at." Intentional refrain, but the second occurrence (L772) immediately extends it ("The harder part..."), so the bare L739 repeat feels like a stutter rather than a deliberate echo. Consider varying L739 or cutting it.
4. **L391** "It opened one." and "from 'competitive' to 'dominant'" -- the word "dominant" recurs in the section title L506 and at L391. Mild but the self-congratulatory register ("turned SQE from competitive to dominant") sits oddly against the chapter's repeated humility about misleading benchmarks. Not forbidden, but watch the tonal whiplash between "benchmarks mislead" and "dominant."
5. **L217** "Sometimes the pragmatic solution is the right one." Borderline trailing-summary platitude closing the section. The dead-end callout right below (L219-225) already makes the point harder and with specifics. Consider cutting the platitude.
6. **L78** "That constant looks whimsical. It is." Good dry voice, keep. (Noted as a positive, not an issue.)

## Mechanical violations (PROSE only)
none

## Exclamation marks in prose
none. All `!` occurrences are the Rust macro `tokio::select!` (L118, L121, L130, in inline code or fences) or `format!`/`eprintln!`/`select!` inside code blocks (L124, L428, L429, L457, L462).

## Continuity data
### Concepts INTRODUCED / defined here
- sqe-bench -> standalone benchmark binary
- BenchmarkGenerator trait -> per-suite data generator
- TableDef -> Arrow schema + row_count fn
- BenchClient trait -> dual-protocol (Flight/Trino) abstraction
- prefix_tables / table qualification -> namespace-prefix query rewriter
- requires/timeout/name query header metadata -> graceful skip + deadline
- Five result statuses -> Pass/Fail/Diff/Skip/Error
- Vacuous status -> zero-rows-on-both, not Match
- Five-layer caching -> RestCatalog/table-meta/manifest/SessionContext/OAuth caches
- compare.rs / epsilon tolerance -> type-aware result comparator
- DuckDB dsdgen oracle -> off-data-path referee

### Concepts ASSUMED (used as if already known)
- Flight SQL, gRPC, HTTP/2 keepalive (assumes ch on wire protocol / ch14)
- CTAS, CoW (copy-on-write) DELETE/UPDATE/MERGE
- Iceberg manifest files, snapshot summary, partition pruning
- OIDC bearer passthrough, JTI claim, token fingerprint (security chapters)
- SessionContext, UDF/TVF, JoinSelection, FairSpillPool, late materialization, predicate transfer (ch13 streaming execution, explicitly cross-ref'd)
- DataFusion optimizer, broadcast threshold, dynamic filter

### Key factual / numeric claims
- 7 suites, 222 queries, 82 tables (L31). Suite table L19-27: TPC-H 22q/8t, TPC-DS 99q/24t, SSB 13q/5t, ClickBench 43q/1t, TPC-E 18q/33t, TPC-BB 10q/2t, TPC-C 17q/9t. (22+99+13+43+18+10+17 = 222 OK; 8+24+5+1+33+2+9 = 82 OK)
- Parquet split 128 MB per file (L80), "aligns with Iceberg default target". Cross-check other chapters for 128 MB vs other split sizes.
- RestCatalog ~250ms, OAuth token ~120ms, SessionContext 70+ UDFs ~50ms (L324). Server-side dropped ~540ms -> under 1ms; SELECT 1 = 0.4ms (L342). Caches: RestCatalog 5-min TTL, table-meta 30s TTL global, manifest no-TTL LRU 512MB, SessionContext 5-min TTL per username, OAuth in-process (L328-336).
- April 2026 compare (SF0.01) L357-365: TPC-H 1646/10796 8.8x 22/22; SSB 710/2045 3.2x 13/13; TPC-DS 19650/46989 2.6x 93/99; TPC-C 304/1528 5.5x 8/8; TPC-E 474/2175 5.3x 11/11; TPC-BB 1223/2193 3.1x 10/10; ClickBench 904/2205 2.5x 43/43.
- TPC-H per-query range 1.9x (q15) to 66.9x (q01); q01 34ms SQE vs 2275ms Trino (L367).
- ClickBench single table 105 columns (L369).
- 6 TPC-DS DIFF = q18,q27,q36,q67,q70,q86 ROLLUP; apache/datafusion#21570 (L373).
- TPC-DS q07,q84 = SQE 0.6x (L371).
- April timeline L510-529: Apr 2 = 192/222, 126s; Apr 10 = 218/222, 154s; Apr 12 = 221/222, 19s. Per-suite Apr12: TPC-H 1.6s 8.5x, SSB 0.7s 11x, TPC-DS 13.0s 5.3x, ClickBench 0.6s 39x, TPC-C 0.9s 3.1x, TPC-E 1.0s 3.6x, TPC-BB 1.1s 6.3x, total 19s 6.7x.
- Apr10 vs Trino losses: TPC-H 0.6x, SSB 0.3x, TPC-DS 0.5x, ClickBench 0.1x, TPC-C 0.5x, TPC-E 0.4x, TPC-BB 0/10 (L533-541).
- SF1 May 7 L547-555: TPC-H 19.3/26.6 2.3x; SSB 7.6/8.3 1.1x; TPC-DS 57.1/39.7 1.4x Mixed (Trino wins total); TPC-C 0.45/3.4 9.6x; TPC-E 10.4/138.8 7.8x; TPC-BB 36.9/323.6 5.5x; ClickBench 1.7/6.3 4.6x. q72: 10-table join, 11.7M-row inventory, 16s SQE vs 1.2s Trino; DF#3843; w/o q72 SQE wins TPC-DS. Column-stats dropped TPC-DS SF1 21%, q72 24.8s->April baseline.
- Streaming/distributed TPC-H SF1 L644-648: single-8GB 22/22 37.5s; single-512MB+spill 21/22 33.3s; distributed 2w 22/22 12.0s, 3.1x. q18 fails at 512MB (DF#17334), 0.74s with 2 workers.
- Per-query distributed table L657-682 totals 37.5s -> 12.0s 3.1x.
- Five-suite x three-config matrix L704-712 totals 169 queries: 512mb 146(86%), 8gb 164(97%), dist 162(96%). Spill table L717-721: 512mb 30/1.1GB, 8gb 128/27.7GB, dist 3/49MB. TPC-E 33 brokerage tables, 27GB intermediate (L723). SF1 distributed ran all local, scheduler_decisions{local}=120+, distribution threshold 4 files, tables 1-2 files (L727).
- Validation: SF0.1 TPC-DS 29 vacuous; 16 of 29 failed DuckDB oracle (L748-752). dsdgen has 92 colors, 8 invented (L754). q63 brand=f(category,class). TPC-C `scale as i32` truncates to 0 at 0.1 (L756). q75 57 rows SQE / 55 Trino; ratios 0.8983/0.8984; DuckDB = 57 (L758).
- SF10 rig: q06 6.3s of 6.4s in scan, 8.5M rows one core (L778); single GET 96MB/s host, 8 parallel 163MB/s flatlined, 2x inside Docker net (L780); TPC-H 0.57x->win distributed, SSB 0.26x->0.7x (L782). q39 OOM 8GB pool Trino did in 5GB; aggregate held 84MB; ~90 consumers, 95MB each; real demand 3GB (L784).
- Final summary table L599-613 (SF0.01): tpch 22/1.6s, ssb 13/.6s, tpcds 99/12.9s, tpcc 17/.8s, tpce 17pass/1error/18 1.0s, tpcbb 10/1.0s, clickbench 43/.6s; TOTAL 221/1err/222 18.8s. TPC-E error = trade_result_update_holding (L616).

### Cross-references
- L130, L302: "Chapter 14" for stuck gRPC streams / HTTP/2 stream accumulation (back-ref).
- L638: "Chapter 13: coordinator spill-to-disk, late materialization, file-level pruning, S3 I/O pipeline, distributed shuffle" (back-ref).
- No forward refs.

## INTERNAL CONSISTENCY FLAGS (cross-check priority)
1. **TPC-E query count is internally contradictory.** L25 suite table = 18 queries. L363 April compare = "TPC-E (11 queries)" 11/11. L710 matrix = TPC-E (18) 12/13/10. L608/L616 final = tpce 17 pass + 1 error = 18 total. So TPC-E is variously 11 and 18. The "11 queries" at L363 likely should reconcile with the 18-query suite (a read subset, like "TPC-C (8 read queries)" at L362, but TPC-E's subset is not called out). Recommend: state the read-subset count explicitly for TPC-E the way TPC-C does, or fix to 18.
2. **L144 sample output is self-inconsistent / stale.** Header "TPCH SF1", body lists q01-q22, "Results: 20 pass, 0 fail, 1 diff, 1 skip" (20+1+1=22 OK). But L142 shows "- q14 ... requires: lateral_join" skipped, while current TPC-H is 22/22 (L367/L373) with ROLLUP/DML enabled (L116). Looks like a historical snapshot; mark it as such or update.
3. **TPC-C 8 vs 17.** L362 "TPC-C (8 read queries)" 8/8 vs 17/17 full (L27, L116, L526, L607, L616). Subset is explained; verify other chapters agree TPC-C is 17 total.
4. **TPC-DS DIFF count.** L373 six diffs, L361 93/99 match (6 diffs = 93 match). Consistent.
5. **L342 "both caches hitting"** but five layers described. "both" likely = SessionContext + OAuth (landed that afternoon). Ambiguous; consider "with the warm caches hitting."
6. **128 MB file split (L80) vs other split sizes elsewhere** (e.g. task_split_target_size intra-file). Different concept (write file size vs read task size) but a reader may conflate; cross-check ch13/scan chapter.

## Pacing
Flows well; sections scannable, field-report/ai-log callouts pace the data tables nicely. Chapter is LONG (~796 lines) with multiple endings: "What We Learned" (L621, three-takeaways recap) reads as a chapter close, then four more substantial sections follow ("The Streaming Execution Effect," "The Benchmark as Regression Suite," "Agreement Is Not Validation," "The Rig Was the Bug"). Result: two strong endings buried mid-chapter. L621-633 is the most summary-like and would be a natural finale; the validation/rig material after it is excellent but arrives post-wrap-up. Consider reordering so the recap is last. No single wall-of-text paragraph; L754 and L784 are densest but stay readable.

## Grade
Voice adherence: A-. Clean mechanically (zero emdash/arrow/emoji, no prose exclamations), strong hook, dry humour landed, no forbidden words. Docked for the two invented-feeling round percentages at L631 that contradict the chapter's own tables, the "Auth overhead." fragment, and the structural double-ending that dilutes an otherwise excellent close.
