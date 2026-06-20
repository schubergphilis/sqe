# Findings: 16c-following-through.md

## Thesis
Working through six months of a 35-point "punch list" (matrix score 129 -> 164), the chapter argues that a capability matrix is only worth keeping when each partial cell names a concrete code change, and that stale docs and per-backend wrappers are silent debt you pay down deliberately.

## Opening
> "The previous chapter ended at 129 of 189 cells full. Sixty-eight per cent. We had named the gaps."

Verdict: strong hook. Direct callback that picks up the exact number from 16b and immediately raises the stakes ("That sentence is easy to write. Living up to it is the work.").

## Closing
> "The matrix keeps the engineers honest because the engineers keep the matrix honest. That is the only state in which a public scoreboard is worth having."

Verdict: lands it. Chiastic line plus a single-sentence verdict; earned, not a trailing summary.

## Voice & editorial issues
1. L65: "This is the spec feature that says..." Starts a sentence with "This" referring to the previous sentence (partition evolution). CLAUDE.md: "Never start a sentence with 'This' referring to the previous sentence. Name the subject." Rewrite: "Partition evolution is the spec feature that says..."
2. L153: "This is the kind of work the matrix does not measure but the engineers do." Same "This"-as-subject rule. Rewrite: "The loader refactor is the kind of work the matrix does not measure but the engineers do."
3. L285: "This is the unglamorous work that does not appear on any dashboard." Same rule. Rewrite: "Test migration is the unglamorous work that does not appear on any dashboard."
4. L6: "Sixty-eight per cent." British "per cent" spelling sits beside "68.3%" usage in 16b and the figures-with-% style used everywhere else in this chapter (74.1%, 81.5%, 86.8%). Minor consistency nit; prefer "Sixty-eight percent" or just "68%."
5. L101 / L149: the live-test reveal ("found `...iceberg_user_events`... on account 311141556126 in `eu-central-1`") is good transparency-trait writing, but a real AWS account id in prose is a leak risk for the published book. Not a voice fault; flag for the sanitizer pass.

## Mechanical violations (PROSE only)
none

## Exclamation marks in prose
none. (All `!` hits are inside code fences: L45 `assert!`, L223-224 `assert_eq!`, L260 `format!`.)

## Continuity data
### Concepts INTRODUCED / defined here
- Phase R -> bloom filter probe (unit test)
- Phase M -> PARTITIONED BY parser path (six transforms + void)
- Phase N -> partition evolution (ADD/DROP/REPLACE PARTITION FIELD)
- Phase O -> live catalog integration tests (HMS/Glue/JDBC/Nessie)
- Phase Q -> Unity Catalog OSS via Iceberg REST adapter
- loader refactor -> `iceberg-catalog-loader` factory replacing per-backend wrappers
- S3 Tables backend -> `iceberg-catalog-s3tables` / `S3TablesCatalog`
- "stale docs are silent debt" / "loader patterns beat per-backend wrappers" / "partial cells point at a PR not a phase" (three rules)

### Concepts ASSUMED (used as if already known)
- the matrix / 189-cell scoreboard, partial vs full vs none statuses (16b)
- "What comes next" punch-list contract from prior chapter (16b)
- bloom-filter property round-trip via `SHOW CREATE TABLE` (earlier)
- CoW vs MoR write modes, position/equality deletes (earlier)
- DataFusion coercion, sqlparser-rs, iceberg-rust, Polaris, Arrow types
- trino-compatibility doc, benchmark report ("222 of 222")

### Key factual / numeric claims
- 129 of 189 cells = 68% at chapter start; ends 164/189 (86.8%)
- "Thirty-five points across six phases over six months" (35 = 164-129)
- Score transitions: bloom 158->162 (L57); live catalogs 153->158 (L103); unity 158->162 same MR as bloom (L113); loader 162->163 (L153); jdbc v3 163->164 (L230)
- bloom filter test "60-line unit test" / "five-line change" / writer caveat was wrong (no separate worker parquet writer; uses `parquet_writer_config` / `build_writer_props`)
- six Iceberg transforms: identity, year, month, day, hour, bucket(N,col), truncate(N,col), plus void
- partition-evolution fix "fifteen-minute fix" / "one assertion, one branch"
- HMS image `apache/hive:standalone-metastore-4.1.0`, port 19083:9083; Thrift addr needs `host:port` no scheme; IPv4 `127.0.0.1` fix
- Glue live test: `iceberg_demo_analytics.iceberg_user_events`, ~1.5 million rows, account 311141556126, `eu-central-1`; S3 Tables in `eu-west-1` returned `testtablebucket/testnamespace/daily_sales`
- Nessie `ghcr.io/projectnessie/nessie:0.107.5`; PostgreSQL JDBC `postgres:15`
- Five cells flipped Phase O: hive-metastore:v2/v3, aws-glue-catalog:v2/v3, nessie:v3
- Unity OSS adapter `/api/2.1/unity-catalog/iceberg/`, image pinned `unitycatalog/unitycatalog:main-2f2e32d`, read-only (501 on mutate, per `unitycatalog/unitycatalog#3`), table `unity.default.marksheet_uniform`
- loader refactor deleted "six hundred lines" / wrapper modules glue.rs, hms.rs, sql.rs, hadoop.rs in `crates/sqe-catalog/src/backends/`; dispatch now in `crates/sqe-catalog/src/rest_catalog.rs`
- MoR dispatcher shipped "Phase O+ step 8d" in `handle_delete_dispatch`; `write.delete.mode = merge-on-read`
- JDBC v3 test "Twenty-five lines"; format-version=3 round-trip
- SQL surface: `SqlType::JSON` arrived sqlparser-rs 0.54 -> maps to Utf8; TIME -> Time64(Microsecond); TIME WITH TIME ZONE rejected; TIME(p>6) rejected; localtime() bug returned Timestamp not Time64; "hundred and fifty lines across two files"
- Type System coverage 74.1% -> 81.5%; Scalar JSON 91.7% -> 100%
- six cells remain none (multi-arg-transforms:v3, variant-type:v3, shredded-variant:v3, geometry-type:v3, vector-type:v3, lineage:v3); eight remain partial; equality-deletes:v2 caveat `RowDeltaOperation::delete_entries` returns `Ok(vec![])` against RisingWave fork's SnapshotProducer

### Cross-references
- Opening explicitly references "the previous chapter" (16b): 129/189, "What comes next" section, "quoting only the numbers earned by tests that ran today."
- Echo of 16b: "Three numbers. Three different questions" (matrix / trino-compat doc / benchmark report) mirrors 16b's "Both numbers belong in the README."
- No explicit forward refs to later chapters.

## Pacing
Flows well. Non-linear score ordering (sections jump 158->162, then 153->158, then 162 again) could confuse a linear reader, but L113 disarms it with "Same MR as the bloom filter probe." No walls of text; longest paras (L325, L101) are dense but justified inventory. Strong rhythm of short verdict lines closing each section ("Score X to Y.").

## Grade
Voice adherence: A-. On-voice throughout, clean mechanics, strong hook and close; only deductions are three "This"-as-subject sentence openers (L65/L153/L285) and the "per cent" spelling nit.
