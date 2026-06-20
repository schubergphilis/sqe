# Findings: 07-making-dbt-work.md

## Thesis
dbt does not care about an engine's architecture; it cares about the metadata surface (information_schema, system tables, JDBC types) and exact string-matched type names. Making dbt work is unglamorous compatibility plumbing, and that plumbing is where adoption actually lives.

## Opening
> dbt doesn't care about your architecture. It cares about list_relations.
> With `information_schema`, `SHOW` commands, and namespace resolution in place, the engine could describe itself.

Verdict: strong hook. The epigraph plus "Then we ran `dbt run` and nothing happened" (L7) sets up the conflict in three short beats.

## Closing
> The translation is tedious. It's also the difference between an engine that works and an engine people use.

Verdict: lands it. Short-long-short rhythm, restates the thesis as a prescription rather than a summary. No trailing recap.

## Voice & editorial issues
1. L34 - The paragraph beginning "There's a cost we accepted:" runs 7 sentences and is a wall of text (rule: paragraphs > 5 sentences). It crams the 500-table cost, the dbt-filter rationale, the future pushdown optimization, and "we haven't needed it yet" into one block. Split after "...unfiltered `SELECT * FROM information_schema.columns`." into a second paragraph starting "A future optimization would push the filter down...".
2. L32 - The inline call-graph arrow chain ("SQL query -> DataFusion resolves virtual table -> provider calls Polaris REST -> ...") shoves an inventory into prose, making a 5-sentence paragraph dense. Consider a short bullet chain or a single sentence naming the dominant cost. The densest prose in the chapter.
3. L60 - "The design is Path A from our evaluation" assumes the reader knows what Path A and Path B are. Path B is glossed later at L102/L104 ("the Trino compat shim"), but the Path A/B fork is introduced here without a back-reference. If the evaluation was in an earlier chapter, add "as we decided in Chapter X"; otherwise name it on first use. A dangling label.
4. L93 - Paragraph is 6 sentences (Materializations bullet), slightly over the 5-sentence guideline; the last two sentences ("Each template maps..." and "The materialization macros are where...") partly repeat each other. Cut one.
5. Repetition - The "boring/unglamorous/nobody builds X but without it nothing works" beat is hit five-plus times (L34, L99, L121, L132, L137, L237, L239). It is the chapter's real thesis, but the repetition dulls it. Trim two of the restatements.
6. L194 - "That observation planted a seed." closes a section as a vague forward-tease. Make the cross-reference explicit ("we come back to this in Chapter X") or cut it. Standalone it reads as throat-clearing for a payoff that does not clearly arrive.

## Mechanical violations (PROSE only)
none

## Exclamation marks in prose
none

## Continuity data
### Concepts INTRODUCED / defined here
- dbt discovery sequence -> metadata-query cascade before run
- dbt-sqe adapter -> native Python ADBC adapter
- SQEConnectionManager -> connection lifecycle component
- SQEAdapter -> dbt discovery methods component
- SQEColumn -> Arrow/Iceberg/dbt type mapper
- Path A vs Path B -> native adapter vs Trino shim
- system.metadata schema -> Iceberg-specific introspection tables
- system.jdbc schema -> Trino-JDBC-compat metadata tables
- JdbcSchemaProvider -> five JDBC metadata tables
- SqeErrorCode -> 27 structured error codes
- UUID file naming -> per-write Parquet prefix fix
- 17 Trino-compatible UDFs -> date/conditional/introspection aliases

### Concepts ASSUMED (used as if already known)
- information_schema, SHOW commands, namespace resolution (L5, "in place" - from earlier chapter)
- OIDC handshake / bearer passthrough (L16, explicitly "as described in Chapter 4")
- SessionCatalog (L32, named without definition)
- ADBC Flight SQL / Arrow Flight (assumed reader competence - fine)
- Iceberg snapshots, commit protocol, row-level deletes, Iceberg v2 (L95, L165)
- Merge-on-Read materialization strategies
- "Path A from our evaluation" (L60 - assumes a prior evaluation; no explicit ref)

### Key factual / numeric claims
- Polaris round-trip "typically under 10 milliseconds" (L32)
- "500 tables, that's 500 `load_table` calls" (L34)
- dbt project "50 models might query metadata hundreds of times" (L30, L207)
- adapter "roughly 2,000 lines of Python" (L99, L104)
- "dbt's adapter test suite has 47 tests. We passed 43 on the first run. The 4 failures... transaction semantics" (L111)
- "a dbt project with 12 models failed" (L117)
- products seed 500 rows, customers seed 2,000 rows; failure at "row 501" (L163, L169)
- static prefix `insert-00000.parquet` -> UUID prefix fix, "one line" (L163, L167)
- "17 Trino-compatible UDFs" enumerated (L183)
- "27 error codes (`SqeErrorCode`)": TABLE_NOT_FOUND(11), COLUMN_NOT_FOUND(12), TYPE_MISMATCH(21), PERMISSION_DENIED(31), CATALOG_ERROR(41), STORAGE_ERROR(51); JDBC type codes BOOLEAN 16, INTEGER 4, VARCHAR 12 (L209, L141)
- "five tables in `JdbcSchemaProvider`" (L141)
- three fixes "roughly 600 lines of Rust" (L225)
- "roughly a third of our total development time on metadata surfaces" (L237)
- profile port 50051, type sqe (L48-L52)
- Trino error after fix: error_code=11, name=TABLE_NOT_FOUND (L214)

### Cross-references
- L16 explicit back-ref: "authenticates via OIDC exactly as described in Chapter 4"
- L60 implicit back-ref: "Path A from our evaluation" (no chapter number - verify the Path A/B evaluation appears earlier; if so add the number)
- L194 implicit forward-tease: "That observation planted a seed" (re: useless error messages; the error-code taxonomy actually pays off later in THIS chapter at L209 - verify the tease is not meant for another chapter)
- L111 / L225 two `.fieldreport` callouts; L147 an empty `.ailog` placeholder ("To be completed by AI Logbook agent") - unfinished placeholder still in the chapter.

## Pacing
Flows well overall; strong narrative arc in "The First Real Run" (three connected bugs, each revealing the next). The two dense paragraphs at L32-L34 (information_schema performance) drag and are the only wall-of-text section. The middle (adapter components) is list-heavy but appropriate as inventory. The "boring/unglamorous" thesis is restated too many times.

## Grade
Voice adherence: A-. Clean mechanics (zero emdash/arrow/exclamation), strong hook and close, excellent narrative arc; docked for one wall-of-text passage (L34), an unfinished `.ailog` placeholder, an undefined "Path A" label, and over-repeating the "unglamorous work" beat.
