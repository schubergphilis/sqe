# Findings: 13-neither-trusts-the-other.md

## Thesis
Distributed execution in SQE pushes only the scan to workers while the coordinator keeps filters, aggregations, and projections; the design is built around an asymmetric, minimal-trust relationship where neither coordinator nor worker holds more state or capability than the current query requires.

## Opening
> "We have a physical plan. We have workers registered and sending heartbeats. Chapter 12 built the infrastructure for distributed execution"

Verdict: strong hook. Three short declaratives establish state, then poses the mechanical question the chapter answers. (Note: the epigraph at L4-L5 has a duplicated line, flagged below.)

## Closing
> "The coordinator decides. The worker executes. Neither trusts the other more than necessary. That is the contract. Everything else follows from it."

Verdict: lands it. Callback to the epigraph; short cadence closes the trust thesis cleanly. (The `.ailog` callout at L727-729 follows the prose close, which is the book's structural convention.)

## Voice & editorial issues
1. **L9** `"And the trust boundaries, it turns out, are the interesting part."` -- "it turns out" is mild filler. Rewrite: "The trust boundaries are the interesting part." The earned single-sentence emphasis is stronger without the hedge.
2. **L319** `"Two things stand out here."` followed by "First," / "Second," at L321/L323 -- minor enumerate-then-list scaffolding, acceptable but slightly mechanical. Not worth changing; noted for completeness.
3. No other soft-voice issues. The punchy "This is deliberate." (L92), "This distinction matters." (L18), "This matters." (L95), "Round-robin." (L120) sentences are good rhythm, not violations. The rubric's forbidden "This"-patterns are only the specific phrases (this enables / this allows for / this approach ensures), none of which appear.

## Mechanical violations (PROSE only)
None. Grep for emdash/endash/unicode-arrow/emoji returns zero hits. All ` -- ` are correct double-hyphens; `<--` at L31-L34 is inside the code/tree fence.

## Exclamation marks in prose
None. All `!` occurrences are in code fences (Rust macros: `debug!`, `format!`, `warn!`, `info!`) or the image alt-text at L11. Zero in prose sentences.

## Continuity data

### Concepts INTRODUCED / defined here
- DistributedScanExec -> replaces IcebergScanExec, fans scan
- try_distribute -> decides whether to distribute
- split_files -> round-robin file splitter
- WeightedScheduler -> largest-first bin-packing assign
- ScanTask -> self-contained scan unit
- WorkerFlightService -> worker do_get/do_action server
- execute_scan -> Parquet bytes to batches
- FairSpillPool / memory_limit -> bounded worker memory
- WorkerRegistry / heartbeats -> liveness tracking
- CredentialRefreshTracker -> push-model STS refresh
- watch channel credential push -> latest-value credential delivery
- Late materialization -> two-phase RowFilter scan
- Memory watermarks (green/yellow/orange/red) -> admission control
- SortMergeJoin fallback -> spill-safe join
- DoExchange shuffle -> worker-to-worker exchange

### Concepts ASSUMED (used as if already known)
- Logical plan / physical plan / DataFusion optimizer (treated as known; tied to ch8)
- Policy enforcement via plan rewriting, row filters, column masks (explicitly ch8)
- Worker registration, protobuf serialization, "Ballista heritage" (explicitly ch12)
- Arrow Flight (do_get, do_action, do_exchange, Ticket, FlightDataEncoderBuilder), gRPC/tonic, OIDC/JWT, Polaris, S3/STS, Iceberg, Parquet -- assumed competence, not re-explained (consistent with voice guide)
- OpenTelemetry trace context / spans
- `system.runtime.tasks` virtual table (referenced L621 as if previously known)

### Key factual / numeric claims
- Worker `memory_limit` default `8GB` (L328)
- `FairSpillPool` for worker memory (L334, L353)
- OOM example: 200-column table, 2 concurrent queries -> 14GB, OOM-killed by Kubernetes (L358)
- Heartbeat: 3 consecutive missed = unhealthy; 1 success recovers; 15 seconds at default 5-second interval (L441)
- Health-check loop is coordinator-initiated, separate from worker heartbeat (L445)
- STS credential TTL "typically 15 minutes to 1 hour" (L505); also "expire in 15 minutes" (L388)
- Credential refresh background task runs every 60 seconds, buffer "within 5 minutes of expiry" (L321, L524); refresh window stated as "5 minutes before expiry" (L321)
- Round-robin split: 12 files / 3 workers = 4 each (L121, L631)
- estimate_cost = file count, minimum 1 (L201)
- Field report: round-robin bottleneck = 300MB vs 2MB partition, idle worker 12 seconds (L208)
- `hash_join_memory_threshold` default 256MB (L700)
- Late materialization: 20-column table, 5% match -> "up to 19x fewer column-chunk reads" (L681)
- Watermarks: green <60%, yellow 60-75%, orange 75-90%, red >90% (L689-L692)
- Prometheus metric: `sqe_memory_utilization_ratio` (L694)
- Security review caught credential issue "in twelve minutes" (L386)
- Deployment endpoint: RustFS/MinIO, private/internal (L388)
- `Partitioning::UnknownPartitioning(n)` (L499)
- DataFusion does not yet support hash join spill-to-disk upstream (L698, L702)

### Cross-references
- L7 "Chapter 12 built the infrastructure for distributed execution" (back-ref)
- L16, L95 "enforces policies (Chapter 8)" / "Policy enforcement happens on the logical plan (Chapter 8)" (back-ref)
- L723 "Chapter 14 is about all the ways things go wrong" (forward-ref)
- L646, L648 "Future optimization could push the filter predicate..." / "That is a future optimization" (forward intent, no chapter named)
- L702 "tracked in the DataFusion issue tracker" (external ref)

## Pacing
Flows well. Progressive disclosure from scan-split -> scheduler -> worker -> credentials -> trust model -> streaming future. Code blocks are sized and follow each concept. No walls of text; paragraphs stay 3-5 sentences. The "Putting It All Together" numbered walkthrough (L626-L648) is a good consolidation, not filler. The chapter is long but earns its length; the late "Beyond the Scan Boundary" section (L671+) shifts to future/aspirational work and could feel like a second chapter, but the trust-model framing holds it together.

## Internal consistency (drift to resolve)
1. **Coordinator S3 access contradiction.** L37 "The coordinator never touches S3." and L653 "In distributed mode, it has no S3 connectivity (by design -- it does not need it)." But the local-fallback path repeatedly has the coordinator perform the scan: L90 "the coordinator handles the scan itself," L355 "fall back to local execution where the coordinator's larger memory pool might absorb the load," L619 "the fragment runs on the coordinator itself." A coordinator running the scan fallback must have S3 access. The "in distributed mode" qualifier at L653/L366 only partially reconciles this, since L90/L355 are inside the distributed code path's fallback. Recommend a one-line clarification (e.g. coordinator has S3 access but does not use it when healthy workers handle all fragments).
2. **Credential-in-ScanTask: claimed wrong, then retained.** L382 "This worked. It was also wrong." vs L388 "In the current implementation, the coordinator still sends credentials in the `ScanTask`." Resolved in-text as acceptable for the private-endpoint deployment, so not a true contradiction, but the nuance (static creds + internal network is acceptable; STS-vended is the production design) lands by L390. Acceptable as written.
3. **STS TTL figure varies.** "expire in 15 minutes" (L388) vs "typically 15 minutes to 1 hour" (L505). Not contradictory (15 min is the low end), but worth aligning for cross-chapter consistency.

## Epigraph defect
**L4-L5** the line `"Neither trusts the other more than necessary."` appears twice consecutively in the opening block quote. Delete the duplicate (keep L4, remove L5).

## Grade
Voice adherence: A. Clean mechanics (zero emdash/forbidden-word/prose-exclamation hits), strong hook and close, consistent short/long rhythm, opinionated and direct. Only soft flag is "it turns out" at L9. The substantive editorial issues are the duplicated epigraph line and the coordinator-S3 internal contradiction (continuity, not voice).
