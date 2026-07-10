# Findings: 06-the-catalog-is-the-api.md

## Thesis
An engine's real API is its catalog (information_schema + system tables), not its query protocol; tools discover your warehouse through SQL metadata, so a query engine that can't describe itself is unusable.

## Opening
> The engine worked. You could connect from DBeaver, authenticate via OIDC, and run a query that hit Polaris, fetched credentials, scanned Parquet from S3, and returned Arrow batches over Flight SQL.
Verdict: strong hook. "The engine worked" then immediately the empty schema browser is a clean problem-reveal, not preamble.

## Closing
> If the catalog lies, every tool built on top of it produces wrong results.
> Build a catalog worth reading.
Verdict: lands it. Single-line imperative echoes the epigraph ("Make it worth reading") and closes the loop without summarizing.

## Voice & editorial issues
1. L182 `Together, these five layers reduced warm-query overhead from ~540 ms to under 1 ms.` Verify the "five layers" count: the section lists TableMetadataCache, ManifestCache, FooterCache, RestCatalog instance cache, SessionContext cache = 5. Count is correct; flagging only because it's a load-bearing number cross-checked elsewhere.
2. L165 `This falls out naturally from the bearer passthrough architecture described in Chapter 4.` Close to the forbidden "this approach ensures" family but reads as a plain causal claim, not filler. Acceptable.
3. L339 `your engine's API is not its query protocol. It's not the Flight SQL endpoint, not the Trino compat layer, not the JDBC driver.` Strong anaphora, on-voice. No issue.
4. No hedging, no throat-clearing, no rhetorical-question transitions, no trailing summary. The chapter is unusually clean against the voice rules.

## Mechanical violations (PROSE only)
none. All `--` are double-hyphens (voice convention), zero U+2014/U+2013/arrows/emoji in prose. Tree diagram at L285-313 uses `|-- \-- ->` ASCII, which is excluded.

## Exclamation marks in prose
none

## Continuity data
### Concepts INTRODUCED / defined here
- information_schema views -> tables/columns/schemata discovery
- InformationSchemaProvider -> SchemaProvider returning virtual tables
- virtual table provider -> dynamic catalog-backed table
- list_namespaces_safe -> error-swallowing namespace lister
- system.runtime.queries/tasks/nodes -> SQL engine introspection
- QueryRecordsFn / QueryTracker bridge -> closure crossing crate boundary
- TableMetadataCache -> moka cache, 30s TTL, token-fingerprint keyed
- ManifestCache -> immutable manifests, 512 MB, 1h TTL
- FooterCache -> Parquet footers, size-weighted eviction
- RestCatalog instance cache -> 5-min TTL, ~250 ms create cost
- SessionContext cache -> per token fingerprint (SHA-256), try_get_with
- parse_and_classify / StatementKind -> parser wrapping classifier (18 variants, 30 tests)
- parse_table_ref -> 1/2/3-part name resolution, "default" fallback
- SqeCatalogProvider / SystemCatalogProvider -> the two registered catalogs
- EXPLAIN FULL -> custom explain w/ pruning + cost

### Concepts ASSUMED (used as if already known)
- Flight SQL, Arrow RecordBatch, MemTable (Arrow/DataFusion baseline - fine)
- OIDC / bearer token passthrough (defined Ch4, explicitly back-referenced)
- Polaris REST catalog, iceberg-rust RestCatalog
- SessionCatalog (used heavily; check whether Ch3-5 defined this type)
- distributed mode / fragment scheduler / workers (forward-leaning; "distributed mode" used L200/L205 before the distributed-execution chapter)
- TVFs / UDFs (mentioned L180 in passing)
- token fingerprint (used before the term is defined - first use L172)

### Key factual / numeric claims
- "Chapter 3 through 5, working end to end" (L6)
- TableMetadataCache TTL 30 seconds; "99-query benchmark run ... 1,500 times" (L172)
- ManifestCache 512 MB size limit, 1-hour TTL (L174)
- RestCatalog "~250 ms" to create, 5-minute TTL (L178)
- SessionContext cache keyed SHA-256 (L180)
- "reduced warm-query overhead from ~540 ms to under 1 ms" (L182)
- "five layers" of caching (L182)
- "TPC-DS query touching 15 dimension tables meant 15 network round-trips" at "scale factor 1" (L170)
- "StatementKind enum grew to 18 variants" (L244)
- "classifier has 30 test cases" (L244)
- "load test with 50 concurrent clients" (L205)
- worker URLs http://worker-1:50052, worker-2, port 50052 (L205)
- env!("CARGO_PKG_VERSION") for nodes table version (L202)
- "The fix was one line. The debugging took two hours." (L332) - voice.md quotes a "four hours" variant of this anecdote (different bug, likely fine, but confirm)
- three SQL standard views; two catalogs (warehouse + system)

### Cross-references
- L6 back: "Chapter 3 through 5, working end to end"
- L165 back: "bearer passthrough architecture described in Chapter 4"
- L189/L200/L205 forward (implicit): distributed execution / fragment scheduler / Trino compatibility - not labeled "we'll cover in ch X"; consider an explicit forward ref
- L247 (.devto callout): references dev.to article "When Your SQL Engine Understands Meaning"

## Pacing
Flows well. Progressive disclosure: problem -> provider -> one table in detail -> registration -> system catalog -> SHOW -> namespace -> full hierarchy -> the bug -> the lesson. The caching subsection (L168-182) is the densest stretch but is a bulleted inventory, not a wall of text. No paragraph exceeds five sentences. Good short/long rhythm ("Live data, on demand." / "The fix was one line.").

## Grade
Voice adherence: A. Clean against every forbidden word/pattern, strong hook and close, consistent short-long rhythm, dead-end/field-report callouts used as the voice guide prescribes; only nits are unverified cross-chapter numbers and a couple of distributed-mode terms used slightly ahead of their defining chapter.
