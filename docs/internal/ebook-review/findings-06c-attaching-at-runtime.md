# Findings: 06c-attaching-at-runtime.md

## Thesis
SQE gains runtime catalog management from SQL (ATTACH/DETACH, CREATE SECRET) backed by a process-local registry; the registry-as-source-of-truth pattern fixes a lifecycle bug where attached catalogs vanished after one query and generalizes to the engine's other state stores.

## Opening
> The previous chapter ended with six backends behind one dispatch. Operators wrote TOML. The engine connected. That story closed cleanly. Then the same teammate who pasted a HuggingFace path into the CLI asked the next obvious question. "Can I attach a Glue catalog from SQL?"
Verdict: strong hook. Picks up a named recurring character and a concrete question, not a topic announcement.

## Closing
> The engine is starting to look like a database, not a query parser. That is the right direction.
Verdict: lands it. Short, opinionated, earns the architectural claim the chapter built toward.

## Voice & editorial issues
1. L216-217 `"Two lessons. One is small. One is large."` then L218 "The small one is about leftovers" and L220 "The large lesson is about state stores." This labelled-takeaways structure edges toward a trailing-summary section. It mostly survives because both lessons add new framing rather than restating, but "The lesson" heading + enumerated recap is the most summary-flavored part of an otherwise narrative chapter. Tighten: cut "Two lessons. One is small. One is large." and lead straight into the leftover lesson.
2. L165 `"Less code. Same behaviour. Better behaviour, in fact:"` The "in fact" is mild filler. Rewrite: "Less code. Same behaviour, and better: the embedded path now works the same as the cluster path."
3. L88 is a 9-sentence paragraph (wall of text by the >5 rule). Strong content (the 3 AM stale-catalog scenario) but dense. Add a break after "...most do not want once they think about it." so the Monday/Sunday scenario stands alone.
4. L220-222 re-explains the Arc<RwLock> / coordinator-owns-state point already made at L86 and L148. Acceptable as a deliberate motif but watch it doesn't tip into restatement across three sections.

## Mechanical violations (PROSE only)
none

## Exclamation marks in prose
none

## Continuity data
### Concepts INTRODUCED / defined here
- ATTACH / DETACH -> runtime catalog attach SQL
- CREATE SECRET / DROP SECRET -> named credential storage SQL
- `AttachStatement` -> custom parser AST node
- `CatalogKind` enum -> backend type tag (Sqlite, IcebergRest, Glue, S3tables, Hms, Jdbc)
- `SecretStore` -> in-memory secret map with zeroize-on-drop
- `Secret` enum -> bearer/aws/basic credential kinds
- `RuntimeCatalogRegistry` -> process-local attached-catalog store
- registry-as-source-of-truth -> session context is derived view
- DROP SECRET in-use guard -> blocks drop while referenced
- `known_catalog_names` -> union of TOML + system + runtime catalogs

### Concepts ASSUMED (used as if already known)
- `build_catalog` / the loader (attributed to "chapter 6b")
- `MASKED WITH`, `ROWS WHERE` parser extensions (attributed to "chapter 9")
- policy store / plan rewriter / row filters / column masks (attributed to "chapter 9")
- `WritableIcebergCatalog` (used, not defined here)
- `create_session_context`, `SessionContext`, session-context cache
- `enable_url_table()`, `DynamicFileCatalog`, embedded/CLI mode
- sqlparser post-parse rewrite mechanism
- HuggingFace-path-in-CLI anecdote (from prior chapter)
- `SHOW CATALOGS`, 3-part qualifier pre-flight check

### Key factual / numeric claims
- "six backends behind one dispatch" (prev chapter)
- CatalogKind variants: Sqlite, IcebergRest, Glue, S3tables, Hms, Jdbc (six)
- Three secret kinds: bearer, aws, basic
- "DataFusion 53.1 has `register_catalog` ... does not have `deregister_catalog`"
- `MemoryCatalogProviderList`, `CatalogProviderList`, `DashMap` (DataFusion internals)
- Test stack: `wiremock` / `MockServer`, endpoints `/v1/config` and `/v1/namespaces`
- "tests run in 7 seconds"; "Seven cases" / "seven test cases"
- Error ports: fake REST on 59999, primary catalog on 59998
- Cross-ref targets: "chapter 6b" (loader), "chapter 9" (MASKED WITH / policy store / row filters / column masks)
- Forward: "runtime grant cache for the GRANT/REVOKE chapters"

### Cross-references
- Back: "the loader from chapter 6b" (L36, L132); "from chapter 9" for MASKED WITH/ROWS WHERE (L25) and policy stores (L148, L220)
- Forward: "the next one in the queue is a runtime grant cache for the GRANT/REVOKE chapters" (L224)
- DRIFT RISK: this file is `06c` and cites "chapter 6b" and "chapter 9" by number. Verify policy/GRANT-REVOKE material actually lands in chapter 9 and the loader in 6b. The book mixes lettered sub-chapters (06b/06c) with plain-number cites ("chapter 9"); confirm numbering is consistent at build time. Cross-check `known_catalog_names` adding "system" and "datafusion" against the prior dispatch chapter's catalog list.

## Pacing
Flows well. Problem -> SQL surface -> secrets -> registry -> bug -> fix -> testing -> lesson is a clean arc. Only L88 reads as a wall of text; the rest alternates short and long sentences cleanly.

## Grade
Voice adherence: A. Strong hook, transparent dead-end-and-fix narrative, opinionated closings, zero forbidden words or mechanical violations; only soft spots are a faintly summary-flavored "The lesson" section and one over-long paragraph.
