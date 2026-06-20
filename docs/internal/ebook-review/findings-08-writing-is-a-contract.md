# Findings: 08-writing-is-a-contract.md

## Thesis
Building the Iceberg write path (CTAS, INSERT, DoPut, then DELETE/UPDATE/MERGE) is harder than the read path not because the commit protocol is complex but because writing is a contract across four parties (engine, storage, catalog, concurrent writers) where types silently disagree and the expensive bugs are invisible.

## Opening
> Reading is easy. Writing is where table formats earn their keep.

Verdict: strong hook. Epigraph plus the first body line ("A query engine that can only read is a glorified report generator.") sets stakes immediately with no preamble.

## Closing
> We paid that price. And then we moved on to the next problem: making sure users cannot write data they should not be able to see.

Verdict: lands it. Pays off the "price of admission" line and hands directly to the next chapter (security) without a trailing summary. (Note: the literal last block is the AI Logbook callout, which is structural, not the prose close.)

## Voice & editorial issues
1. L222: "The write path took one day to implement and three days to debug." Contradicts the opening narrative at L6 ("One day. But that one day compressed more debugging...") and L57 ("Four hours of debugging") and L230's "always four hours." The "three days to debug" figure appears nowhere else and reads as a new, unsupported number planted in the summary. Reconcile: commit to one debug figure everywhere or drop it.
2. L57: "One line of code. Four hours of debugging. That ratio would become familiar on the write path." Good rhythm, but the same beat repeats near-verbatim at L230 ("The fix is always one line. The debugging is always four hours."). Two near-identical payoffs of the same line dulls it. Keep the L230 version (closes a triad) and vary or trim L57.
3. L153: "This separation turned out to be one of the better architectural decisions on the write path." "one of the better" hedges. Tighten: "The separation paid off: the write handler knows nothing about SQL, the query handler knows nothing about Iceberg commits."
4. L129: "The commit protocol is elegant and important and takes almost no time at all." Three "and"s. Rewrite: "The commit protocol is elegant, important, and almost free."
5. L178: "The routing works. And now the handlers deliver." "And now the handlers deliver" is slightly performative as a transition. Cut to "The routing worked. The handlers came next."

## Mechanical violations (PROSE only)
none. Em-dashes all rendered as `--` (L5, L14, L36, L40, L53). No endashes, no unicode arrows, no emoji in prose.

## Exclamation marks in prose
none. The two `!` hits (L79, L83) are Rust `format!` macros inside a code fence.

## Continuity data
### Concepts INTRODUCED / defined here
- Iceberg three-stage commit protocol -> write files, commit metadata, atomicity
- fast_append / overwrite / row_delta / rewrite_files -> Iceberg transaction types
- `stamp_field_ids` -> stamps field-IDs, fixes nullability, casts types
- `write_data_files_streaming` -> streaming per-batch Parquet writer
- Copy-on-Write vs Merge-on-Read -> row-mutation strategies
- DoPut ingest -> Flight client streams Arrow directly
- Three write entry points -> CTAS, INSERT, DoPut converge
- CREATE OR REPLACE TABLE -> drop-then-create, non-atomic
- Cache invalidation on write -> table-scoped conservative purge

### Concepts ASSUMED (used as if already known)
- DataFusion, Arrow RecordBatch, Parquet, S3, Polaris (established earlier)
- Bearer token passthrough (explicit back-ref to ch4, L195)
- `SendableRecordBatchStream`, `SessionCatalog` (assumed Rust/DataFusion competence; OK)
- sqlparser-rs `Statement::CreateTable` (OK)
- RisingWave iceberg-rust fork (introduced here L173/L180; cross-check dependency chapter)
- Snapshot isolation / optimistic concurrency (defined in-chapter via callout)

### Key factual / numeric claims
- "March 14 ... first read query. March 15, CTAS and INSERT INTO worked end to end. One day." (L6)
- AWS conditional puts via `If-None-Match` added August 2024 (L22-23)
- Streaming batch size "typically 8,000 rows" (L36)
- "six-million-row lineorder table at scale factor 1" (L36)
- Sequential field IDs starting from 1 (L40)
- Type maps: `Int64`->`long`, `Utf8`->`string`, `Float64`->`double` (L40)
- DataFusion `Timestamp(Nanosecond, None)` vs Iceberg `Timestamp(Microsecond, None)` (L47, L55)
- "three hours" (L55) then "four hours" (L57) -- minor internal wobble (3 hrs staring + 1 = 4 total; defensible but loose)
- `RollingFileWriterBuilder` 128 MB default (L68)
- `file_prefix` values: "ctas", "insert", "ingest" (L70)
- Commit "four lines" (L72) -- refers to four logical statements, not physical lines
- iceberg-datafusion 0.9 had no upstream TableProvider write support (L116-117)
- TPC-H SF10 = "eight tables totaling about 10 GB of Parquet" (L123)
- CTAS load "approximately 4 minutes on a single coordinator"; commit "Milliseconds"; protocol fastest "by three orders of magnitude" (L125-127)
- RisingWave iceberg-rust fork rev `1978911ec4`, provides `rewrite_files()`, ships `PositionDeleteFileWriter` (L173, L180)
- Upstream "had not shipped `OverwriteAction`" (L180, L228) -- NOTE: at L116 the gap is "no upstream support in iceberg-datafusion 0.9"; at L180/L228 it's "OverwriteAction." Both called "upstream." Verify these are two distinct gaps, not a conflation.
- "CTAS and INSERT INTO cover 80% of what data pipelines need" (L226)
- Write path "one day to implement and three days to debug" (L222) -- CONFLICTS with "One day"/"four hours" elsewhere (issue 1)

### Cross-references
- L5: "Chapters 4 through 6 tell that story" (back-ref, read path)
- L195: "Everything in Chapter 4 about bearer token passthrough applies to writes" (back-ref)
- L234: forward hand-off to security chapter
- L173/L190: forward promise -- full Merge-on-Read / compaction "will follow" (cross-check against later write/roadmap chapter so it is not claimed done elsewhere)

## Pacing
Flows well. Short-long alternation is consistent; headers read as a clean outline. "Four Hours for One Enum Variant" is the strongest beat. No walls of text; densest block is the .iceberg callout (7 sentences), acceptable for a deep-dive. "What We Learned" lesson 4 re-treads the timestamp/nullable bugs already narrated, skirting (not crossing) the trailing-summary line.

## Grade
Voice adherence: A-. Clean mechanics, strong hook and close, authentic "invisible bugs / one line, four hours" voice. Held back from A by the unreconciled "three days to debug" number (issue 1) contradicting the chapter's own "one day / four hours" framing, and the duplicated one-line/four-hours payoff (issue 2).
