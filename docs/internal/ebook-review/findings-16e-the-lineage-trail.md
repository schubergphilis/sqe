# Findings: 16e-the-lineage-trail.md

## Thesis
SQE adds OpenLineage emission (column-level by default, multi-catalog dataset URIs, disk-spooled HTTP delivery) so operators can trace where a number came from; the chapter is a scope-creep narrative justifying each "yes" that grew a one-afternoon task into a new crate.

## Opening
> Lineage is the question every data team asks at 3 AM: "Where did this number come from?"
Verdict: strong hook. Concrete, human, sets the recurring "3 AM question" motif that pays off in the close.

## Closing
> Either the engine emits OL or it does not. SQE emits OL.
Verdict: lands it. Three-word final line, earned by the "can't retrofit lineage" argument that precedes it. Clean.

## Voice & editorial issues
1. L10 "...and a configuration block that operators could understand in five minutes." Reads as mild self-praise smuggled into an inventory list. Minor. Could trim to "...a disk spool, and a small config block." Not blocking.
2. L43 "They cost us the right to claim 'we knew what we were building' later." Good line but oblique; on first pass reads as if the decisions were a mistake when the point is the opposite. Consider "They are the reason we cannot honestly claim we knew the size of what we were building." Style nit only.
3. L186 "That is the 3 AM answer." Second payoff of the same phrase (first L37); with the close re-using the beat (L190) the motif is slightly over-spent. Keep opening+closing uses; consider cutting/varying L186.
4. No hedging, no forbidden words, no throat-clearing, no trailing summary, no filler transitions found. Headers ("The plan walk that mostly worked", "The disk spool we did not want to write") form a scannable, honest outline. Strong adherence overall.

## Mechanical violations (PROSE only)
none

## Exclamation marks in prose
none

## Continuity data
### Concepts INTRODUCED / defined here
- `sqe-lineage` crate -> new OpenLineage crate
- OpenLineage (OL) emitter -> emits RunEvents per query
- Column-level lineage -> per-output-column dependency trace
- `ColumnDep` / per-column trace -> tracks column origin + transform
- `Transformation` enum -> IDENTITY/COMPUTED/AGGREGATION/GROUP_BY/JOIN/WINDOW/FILTER
- Multi-catalog dataset URI -> `iceberg://catalog-name/warehouse`
- Disk spool -> JSONL replay buffer on HTTP failure
- `emit_selects` config flag -> SELECT lineage off by default
- `unwrap_alias` helper -> strip Expr::Alias before match
- DIRECT/INDIRECT transformation subtypes -> column vs filter dependency
- `auth_mode` (bearer / user_token) -> OL collector auth modes

### Concepts ASSUMED (used as if already known)
- DataFusion `LogicalPlan` tree, node types, `Expr`, `Expr::column_refs()`
- Audit log + query history table (earlier audit-logging chapter, Phase 2)
- `QueryHandler::execute_statement`, coordinator vs embedded mode
- Bearer token passthrough / no service account / per-user catalog calls
- MERGE / INSERT / OPTIMIZE / VACUUM / REWRITE_MANIFESTS write-path statements
- `RustlsConfig` infrastructure (TLS/hardening chapter)
- SQL classifier (`should_emit`, target-table extraction) - used without definition
- Matrix-parity plan / matrix score (prior parity-matrix chapter)
- OpenTelemetry analogy (assumed reader knowledge)
- Marquez, DataHub as OL consumers (lightly introduced inline)

### Key factual / numeric claims
- OL stable spec "2-0-2"; SCHEMA_URL `https://openlineage.io/spec/2-0-2/OpenLineage.json` (L22,L25-27,L29); future "2-1-0" (L29)
- Constant location: `crates/sqe-lineage/src/event.rs` (L28)
- DataFusion 53.1 has no `LogicalPlan::Merge` node (L77,L119)
- "Eleven node types fell out in two weeks"; "Each rule is twenty to forty lines" (L62)
- Node list (L47): Projection, Filter, Aggregate, Join, Window, Sort, Union, Limit, Distinct, SubqueryAlias, TableScan = 11, matches "eleven"
- Window fix "three lines and an hour of debugging" (L64)
- Eight catalog backends (L41): Polaris, Nessie, Unity, Glue, S3 Tables, HMS, JDBC, Hadoop = 8, matches "eight"
- Initial channel design "about six hundred lines" (L83); disk spool "four hundred lines" (L103)
- Marquez sidecar restart ~"ninety seconds" outage (L87,L89)
- Spool config: spool_path=/var/spool/sqe-ol, spool_max_bytes=104857600 (100MB), replay_interval_secs=30 (L96-99)
- Heartbeat work "a week of work" (L109)
- Subquery nesting: two levels work, three coarser, four hits a wall (L115)
- Shipped around v0.16 (L194)
- Example event: eventTime 2026-05-09T08:31:42.103Z, job namespace `sqe-prod`, dataset namespace `iceberg://polaris/warehouse` (L137-184)
- Closing scenario: "order count dropped by 30 per cent" (L190)

### Cross-references
- L10 "the matrix-parity plan" (back-ref to a parity-matrix chapter)
- L79 "The chapter on what we did not ship has the rest." (forward/cross-ref to a deferred-work chapter) - VERIFY it exists; vague reference
- L194 "v0.16" / "The matrix score does not move." (ties to versioned roadmap chapters)
- No explicit "as we saw in ch X" numbered refs.

## Pacing
Flows well. Narrative arc (small ask -> scope creep -> shipped) carries the technical detail. No walls of text; longest paragraphs (L77 MERGE, L89 drop-policy) stay near the 5-sentence guide and earn their length. "What we still do not ship" is a long bolded list but reads as inventory, which the voice guide permits.

## Grade
Voice adherence: A. Strong hook and close, honest dead-end/deferred disclosure (signature trait), zero forbidden words, zero mechanical violations, good short/long rhythm; only nits are a slightly over-used "3 AM answer" motif and one faintly self-congratulatory inventory phrase.
