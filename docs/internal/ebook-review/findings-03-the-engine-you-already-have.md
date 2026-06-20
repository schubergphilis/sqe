# Findings: 03-the-engine-you-already-have.md

## Thesis
DataFusion is a library, not a service, and that single distinction (you own the process, the catalog, the credentials) is what made a sovereign query engine buildable by two people in three months. SQE supplies only the 20% DataFusion doesn't: auth, catalog integration, policy enforcement, distribution.

## Opening
> "We needed a query engine. Not a toy. Not a prototype. A real SQL engine that could parse complex queries, optimize them, push predicates into Iceberg manifests..."
Verdict: strong hook. Short declaratives set stakes immediately, then the "three months and two people" line (L8) lands the tension. Textbook Jacob rhythm.

## Closing
> "Staying current is not a milestone you reach. It is a position you hold, and sometimes holding it means being the furthest forward in your own dependency graph."
Verdict: lands it. Earns a general principle out of the specific DF 54 fork story without trailing-summary repetition.

## Voice & editorial issues
1. **L602** AI Logbook is grammatically broken from mechanical emdash removal: "The `create_session_context` method. one `SessionContext` per user with per-session credentials. was specified by the human..." Those periods create sentence fragments around an appositive that was clearly once set off by emdashes. Rewrite: "The `create_session_context` method, one `SessionContext` per user with per-session credentials, was specified by the human as the architectural constraint. The AI implemented it correctly because Rust's ownership model made the isolation boundaries explicit in the type signatures."
2. **L19 vs L40** Section header "The Fifty-Line Query Engine" but the code is ~12 lines and the prose says "Twelve lines of code." Fifty appears nowhere in the section. Either retitle to "The Twelve-Line Query Engine" or make the "fifty" deliberate (counting the full runnable program with imports/comments) and say so. As written it reads as an editing artifact.
3. **L44, L350, L505** Bare "This is..." openers referring to the previous sentence, which CLAUDE.md forbids ("Never start a sentence with 'This' referring to the previous sentence. Name the subject."). L44 "This is the property that made SQE possible." -> "Per-session isolation is the property that made SQE possible." L505 "This is why the '80/20' framing is accurate." -> "The trait model is why the '80/20' framing is accurate." (Pick 2-3; not every instance needs surgery, but these three are the clearest.)
4. **L411** "At scale, this cost compounds in both directions." Vague filler clause appended to an otherwise sharp tradeoff paragraph. The preceding sentences already make the point concretely (30 seconds vs four hours). Cut the sentence or replace with the specific: "The more crates and CI runs you have, the more both sides of that trade grow."
5. **L60 / L346 / L350** Forward/back chapter refs are all clean; no action, noted for the cross-ref ledger.

## Mechanical violations (PROSE only)
None. The only emdash in the file is L399 inside the TOML code fence (`# Skip dsymutil on macOS — saves 20-30% link time`), which the rules and CLAUDE.md's grep check explicitly permit inside code blocks. No endashes, unicode arrows, or emoji in prose.

## Exclamation marks in prose
None.

## Continuity data
### Concepts INTRODUCED / defined here
- SessionContext -> per-user query environment
- Library vs service -> own-the-process distinction
- Five-stage pipeline -> parse/logical/optimize/physical/execute
- Pull-based / pipeline breakers -> lazy stream, eager joins
- CatalogProvider/SchemaProvider/TableProvider -> three-level data hierarchy
- IcebergScanExec -> leaf scan ExecutionPlan
- 80/20 framing -> DataFusion 80%, your product 20%
- Catalog/storage/compute separation -> sovereignty mechanism
### Concepts ASSUMED (used as if already known)
- bearer token passthrough ("bearer passthrough architecture from Chapter 4", L346)
- Polaris REST catalog (named without re-definition)
- vended/vendable S3 credentials (L287, L346)
- policy enforcement / plan rewriting (ch8 forward-ref but used freely here)
- Trino DCAF fork (L472)
- distributed workers / DistributedScanExec (ch13 forward-ref)
- Arrow RecordBatch, Send + Sync, tokio (assumed Rust competence, correct per voice guide)
### Key factual / numeric claims
- "three months and two people" (L8)
- "built SQE in 15 days" (L365)
- "zero to a working single-node query engine... in three days" (L570)
- "first integration test... passed on March 14, 2025... It took less than a day" (L592-597) -- NOTE: same day as crate scaffolding; three overlapping timeline figures (15 days / three days / less than a day) that should be reconciled or clearly distinguished across chapters
- "DataFusion 52 ships with over 100 optimizer rules" (L120)
- "all 12 crates" (L382) and "scaffolded all six initial crates" (L602) -- CROSS-CHECK: CLAUDE.md documents a 10-crate table (sqe-bench would make 11); "12 crates" needs verification against the canonical crate list
- "CI build went from 4 minutes to 18 minutes" then "sccache... 18 minutes to 6 minutes" (L388/L409/L414)
- "Docker image is 47MB", "binary starts in under a second" (L363)
- "vendor/iceberg-rust/ (4.6 MB)" (L583/L588)
- "DataFusion 53 shipped on April 2, 2026" (L577); "hash join dynamic filters (5-25x for star-schema joins)", "40x faster query planning" (L577)
- "upstream apache/iceberg-rust (v0.9.0)" (L579); RisingWave fork rev `1978911ec4` (L518-519)
- "upstream PR #2206", "Ten files in the fork, forty-four sites in SQE" (L583)
- "TPC-DS dropped from 19.3 seconds to 12.2 seconds. TPC-H from 1.8 to 1.1" (L585)
- "DataFusion 52, Arrow 57, Iceberg 0.9" (L522); Cargo.toml: datafusion "52", arrow "57" (L510-516)
- "we have since moved to DF 53.1" (L507) -- STALE: contradicts the later DF 54 section (L607-625) which says "our in-tree port runs on 54 while both apache main and the RisingWave rebase branch are still on 53." The 53.1 parenthetical predates the DF 54 addition and should be updated to 54.
- DF 54: "swapped its hash backend from ahash to foldhash", "fixed seed-0 state", "all 222 benchmark queries distributed" (L611) -- 222 is a new count to cross-check against benchmark chapters
- DF 54 perf: "TPC-H runs 89 seconds against Trino's 106 and TPC-DS runs 234 against 448" (L623)
- DF 54: LATERAL parses but physical planning fails; QUALIFY works (was wrongly doc'd unsupported); PIVOT/UNPIVOT/ASOF JOIN still rejected (L613-617)
- "delta-rs dependency for `read_delta` has no DF 54 release" (L625)
### Cross-references
- L60 "More on this in Chapter 8." (policy enforcement, forward)
- L346 "the bearer passthrough architecture from Chapter 4" (back)
- L350 "In Chapter 13, we replace the local `IcebergScanExec` with a `DistributedScanExec`" (forward)
- L433 dev.to callout: "DuckDB S3 Tables with Iceberg using Iceberg Rest API" (2025)
- L460 IOMete reference; L472 Trino "DCAF branch" fork

## Pacing
Flows well. Short-long rhythm is consistent and the section headers read as a story outline. The two appended sections (L575 DF 53 fork, L605 DF 54) are the slowest stretch but earn it as narrative payoff; no walls of text. Longest paragraph (L365, "AI agents write Rust well") runs ~7 sentences and is the one spot that could be split for breathing room.

## Grade
Voice adherence: A-. Hook and close are strong, rhythm is on-voice, transparency about what fails (LATERAL, SSB gap, stale QUALIFY doc) is exemplary. Held back from A by the broken AI Logbook grammar (L602), the title/line-count mismatch (L19 vs L40), and a few bare "This is..." openers the project's own rules forbid.
