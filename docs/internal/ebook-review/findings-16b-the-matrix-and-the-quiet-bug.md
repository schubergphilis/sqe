# Findings: 16b-the-matrix-and-the-quiet-bug.md

## Thesis
Benchmark green-counts measure performance, not capability; getting honest about Iceberg V3 support meant writing integration tests against a real Polaris, which surfaced two silent bugs and one runtime gap that no unit test could catch and moved the matrix score from 99/189 to 129/189.

## Opening
> The benchmark says you are fast. The matrix says what you actually do.

Verdict: strong hook. Two short epigraph lines that set the whole chapter's tension, then "Benchmarks are the public face of a query engine. The matrix is the back of the kitchen." lands the metaphor immediately.

## Closing
> The only number worth quoting is the one earned by tests that ran against a real stack today.

Verdict: lands it. A single declarative line that distills the chapter's argument without restating the section list.

## Voice & editorial issues
1. L242: "The next time someone asks 'is your engine production-ready,' you will not need to answer with adjectives." Slight shift into second-person sermon ("you will not need to") just before the closing. Minor. The "Let the reader decide." cadence is on-voice, so this is a soft flag, not a rewrite demand. Could tighten to: "Ask whether the engine is production-ready and you can skip the adjectives. Show the matrix score and the benchmark score side by side."
2. L208: "That difference between a status board and a punch list is the difference between a marketing artifact and an engineering tool." Sentence repeats the "is the difference" structure ("That difference... is the difference") which reads slightly tangled. Rewrite: "A status board markets. A punch list works. The matrix is the second."
3. L154: "That sounds obvious in writing. It was not happening in code." Good short-short rhythm, on voice. No change. (Noting as a positive anchor, not an issue.)
4. No forbidden words found. No hedging, no throat-clearing, no rhetorical-question transitions, no trailing summaries, no filler transitions. "What comes next" is a forward punch list, not a summary, so it does not trip the trailing-summary rule.

## Mechanical violations (PROSE only)
none

## Exclamation marks in prose
none

## Continuity data
### Concepts INTRODUCED / defined here
- Iceberg matrix -> public capability scoreboard (icebergmatrix.org)
- matrix cell levels -> full / partial / none
- `format-version` table property -> reserved REST property carrying v3
- `format_version_properties` -> helper building format-version map
- `merge_user_table_properties` -> reads TBLPROPERTIES into props
- v3_e2e integration tests -> 11/13 ignore-gated catalog tests
- status board vs punch list -> "exact line to change" distinction

### Concepts ASSUMED (used as if already known)
- TPC-H / TPC-DS / ClickBench suites and the "222 of 222" green count
- TVFs `table_files`, `table_snapshots` (cited as already-existing matrix evidence)
- `FOR VERSION AS OF`, `FOR INCREMENTAL`, `FOR SYSTEM_TIME AS OF` time-travel clauses
- pre-classifier that strips clauses before sqlparser-rs
- `replace_table_reference` helper, MemoryCatalog / `datafusion.public` writable schema
- iceberg-rust `TableCreation`, `CreateTableRequest`, `TableMetadataBuilder`
- position deletes / equality deletes / copy-on-write / merge-on-read (Iceberg fundamentals)
- Polaris REST catalog, S3 backing, docker-compose test stack

### Key factual / numeric claims
- "Twenty-two TPC-H queries pass. Ninety-nine TPC-DS queries pass. Forty-three ClickBench queries pass. Two hundred and twenty-two queries across seven suites" (L8)
- "Sixty-three cells per engine" (L16) -- internal-consistency concern: scores are out of 189 points, and 189 = 63 x 3, so "cells" vs "points" should be reconciled (a cell scored none=0 / partial / full=3). Prose says "sixteen matrix cells flipped" (L181) and the table lists 16 rows, while score moves +30 points (99->129), consistent with partial(1)->full(3) and none(0)->full(3). Coherent, but the cell/point distinction is never stated and a reader trips on "63 cells" vs "189 points."
- Start: "99 out of 189 points ... Roughly 52%" (L24); later "(52.4%)" (L202)
- End: "129/189 (68.3%)" (L202)
- "Three commits. Thirteen integration tests ... sixteen matrix cells flipped" (L181) -- but L48 says "I wrote eleven integration tests" and L61 "Eleven failures." 11 (v3_e2e) vs 13 total claimed -- the extra 2 are presumably the bloom/time-travel tests; never reconciled. Worth a one-line bridge.
- File/crate: `crates/sqe-coordinator/tests/v3_e2e.rs` (L48)
- Commands: `docker-compose.test.yml`, `./scripts/bootstrap-test.sh`, `cargo test --package sqe-coordinator --test v3_e2e -- --ignored --test-threads=1` (L52-57)
- "The fix is one line" (L96); "Six lines to fix" (L137); "Five-minute change" (L175)
- 16-row cell-flip table (L184-199)
- `bloom-filters:v3` stayed partial; worker write path does not wire bloom property (L206, L232)
- Spark test ships `#[ignore]` (L232); HMS/Glue config scaffolding only; variant/geometry/vector blocked on upstream iceberg-rust + arrow-rs; hidden partitioning needs PARTITIONED BY in CREATE TABLE (L236-238)

### Cross-references
- "the same TVFs ... the matrix evidence column already cited" (L48) -- implicit back-ref to an earlier matrix/TVF chapter; no explicit "as we saw in ch X."
- No explicit forward refs. Chapter is largely self-contained.

## Pacing
Flows well. Short-long sentence alternation is strong throughout (L8, L44, L110, L154). No walls of text; longest paragraph is L42 at ~4 sentences. The three-bug structure (first failure / wire protocol / second silent bug / third gap) builds momentum, and "Three rules from this work" pays it off without dragging. Numbered fixes ("one line", "six lines", "five-minute") give a satisfying escalation rhythm.

## Grade
Voice adherence: A. On-voice throughout: confessional honesty ("That is not a status. That is an admission."), short-sentence landings, no forbidden words, no mechanical violations, no exclamation marks. Only nits are a slight second-person drift near the close and an unstated cells-vs-points / 11-vs-13-tests reconciliation that a reader will notice.
