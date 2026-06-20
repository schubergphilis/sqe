# Findings: 12-standing-on-ballistas-shoulders.md

## Thesis
Building a distributed query engine from scratch is months of work, and using Ballista as-is is incompatible with SQE's auth/policy pipeline. The right move is a "surgical fork": take Ballista's serialisation and execution patterns, reimplement the parts you need (scheduler, codec, heartbeat) with SQE's constraints baked in.

## Opening
> "Apache Ballista exists for a reason. Building a distributed query execution framework is years of work."
Verdict: strong hook. Opens with a flat claim then immediately enumerates the work involved; anaphora ("You need...") builds momentum. No throat-clearing.

## Closing
> "The hard part of distributed execution isn't the execution itself. It's the plumbing: serialisation, health monitoring, credential management, plan manipulation. We built that plumbing in twelve days, because Ballista showed us which pipes to lay."
Verdict: lands it. The "which pipes to lay" image ties back to the plumbing metaphor and the chapter title. The actual last paragraph (L729) is a forward-ref to ch13, acceptable as a transition but weaker than the plumbing close that precedes it.

## Voice & editorial issues
1. The "N lines of logic, N days of debugging" beat appears four times. L404 "This test exists because we learned the hard way that it needed to." L421 "The fix was small. The time to find it was not." L623 "Three lines of logic in a recursive function. Two days of debugging wrong query results to get there." L714 "We hit that point in two days." The pattern is on-voice (cf. voice.md "The fix was one line. The debugging was four hours.") but four times in one chapter dulls it. Keep two, vary or cut the others. Concrete: at L623 the construction is near-identical to L421; consider cutting the L623 recap since the field report already made the point.

2. Mild redundancy between "The Cost of the Fork" and "What Ballista Taught Us": scan-only simplicity is relitigated. L688 "These are deliberate trade-offs. The scanner-only distribution model is simpler to reason about, simpler to debug, and simpler to operate." then L710 "start with the simplest distribution model that solves your problem." Tighten one.

3. L702-714 "What Ballista Taught Us" uses "The second lesson... The third lesson... The fourth lesson..." Four sequential "the Nth lesson" openers read as a listicle in prose; the voice guide prefers prose for reasoning. Convert to a short bulleted inventory or merge lessons two and three (both are "start simple").

4. L363 "Is this elegant? No. It's practical." A rhetorical question, but self-answered immediately and on-voice (dry, direct). Not a violation; noting only that it skirts the rhetorical-question rule. Fine because it is answered in the same breath, not used to pivot topics.

No hedging, no forbidden words, no throat-clearing intros, no filler transitions found.

## Mechanical violations (PROSE only)
none

## Exclamation marks in prose
none (all `!` occurrences are inside code fences: `format!`, `vec!`, `assert_eq!`, `warn!`, `info!`, `!Arc::ptr_eq`)

## Continuity data
### Concepts INTRODUCED / defined here
- Surgical fork -> study, reimplement, not clone
- DistributedScanExec -> custom scan ExecutionPlan node
- WeightedScheduler -> largest-first bin-packing, 170 lines
- FragmentScheduler -> trait assigning tasks to workers
- ScanTask -> fragment + S3 credentials struct
- SqePhysicalCodec -> custom PhysicalExtensionCodec
- WorkerRegistry -> heartbeat-based worker health
- CredentialRefreshTracker -> mid-scan credential push
- RefreshableCredentials -> coordinator-to-worker cred payload
- replace_scan_in_plan -> recursive plan-tree leaf swap
- Two-tier health model -> soft heartbeat miss vs hard exec failure
- Mode enum -> Coordinator | Worker single binary

### Concepts ASSUMED (used as if already known)
- IcebergScanExec (SQE's local scan node, used as if introduced earlier)
- Polaris (catalog, credential vending)
- bearer token passthrough / auth model (referenced as "SQE's auth model")
- policy engine / plan rewriting before optimisation (L21, L25)
- Arrow Flight, do_get/do_action/do_exchange, RecordBatch, gRPC (assumed competence)
- coordinator/worker split (architecture assumed from earlier chapters)
- datafusion-proto, PhysicalExtensionCodec, with_new_children (DataFusion API)

### Key factual / numeric claims
- L8: Ballista is "DataFusion's official distributed execution layer"
- L35: "three to four months" estimate for from-scratch distributed layer
- L37: "DataFusion 52 has over forty physical plan node types"
- L39: ambition "zero to distributed execution in under two weeks"
- L58: `datafusion-proto = "53"`; "moved through 52 and 53.0 to 53.1 over the course of this book"
- L128: "Our scheduler is 170 lines"
- L198: home-worker fallback "exceeds the minimum-load worker by more than 20%"
- L207-219: ScanTask struct field list (fragment_id, data_file_paths, file_sizes_bytes, projected_columns, s3_endpoint/region/access_key/secret_key/session_token/path_style/allow_http)
- L462/L483: 3 consecutive misses, 5-second heartbeat interval = 15 seconds before removal
- L519: MAX_CONSECUTIVE_FAILURES
- L531: STS session token lifetime "typically an hour"
- L535: refresh "within five minutes of expiring"
- L549: executor checks refreshed creds "between files" via `tokio::sync::watch`
- L570: try_distribute "does this in twelve steps"
- L621/L726: commit `3a123ea` "fix: replace scan leaf in plan tree instead of replacing entire plan"
- L631-634: skip distribution if file count < worker count
- L645-668: ports 60051:50051 (Flight SQL), 28080:8080 (Trino HTTP), 29090:9090 (Prometheus), workers 60061/60062:50052
- L723: "We built that plumbing in twelve days"
- L726: replace_scan_in_plan "took the AI four attempts"

NOTE for cross-check: chapter says DataFusion 52 (L37) and datafusion-proto 53/53.1 (L58) coexist intentionally. Verify version numbers against other chapters (CLAUDE.md/memory reference DF52/53/54 at various points; some notes mention DF54). The "over forty physical plan node types" for DF52 should be checked against any other chapter quoting a node count.
NOTE: "twelve days" (L723) vs "under two weeks" ambition (L39) vs "three to four months" from-scratch estimate (L35) are internally consistent. "twelve steps" (L570) is a separate count, do not conflate with twelve days.

### Cross-references
- L729: forward-ref to next chapter (ch13) "we'll walk through the coordinator and worker in detail"
- L58: self-ref "over the course of this book" (version progression)
- Implicit back-refs: assumes IcebergScanExec, Polaris credential vending, auth/policy pipeline defined earlier
- Callouts used: .deadend, .antipattern, .datafusion, .fieldreport, .ailog (consistent with book conventions)

## Pacing
Flows well overall; strong short/long sentence rhythm; code blocks land right after the concept. Two soft spots: the scheduler code block (L130-194, ~65 lines) is long for a prose book and could be trimmed to the assign loop; and the codec deep-dive (L271-358) plus round-trip test (L365-402) is a ~90-line code stretch where prose nearly disappears. No prose wall-of-text paragraphs. The "What Ballista Taught Us" + "Looking Forward" + final forward-ref stack three closing-flavored sections in a row, which slightly dilutes the ending.

## Grade
Voice adherence: A-. Clean mechanically (zero emdash/arrow/emoji/prose-exclamation), strong hook and close, directness and dead-end honesty are textbook Jacob. Held back from A by the four-times-repeated "N lines / N days" beat and the listicle-style "Nth lesson" closing section.
