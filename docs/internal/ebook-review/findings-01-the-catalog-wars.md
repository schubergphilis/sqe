# Findings: 01-the-catalog-wars.md

## Thesis
The catalog (not compute or storage) is the control plane of a data platform, and the right catalog has no opinions: SQE chose Apache Polaris because it implements the Iceberg REST spec, vends scoped credentials, and stays out of the engine's way, which makes the catalog choice reversible.

## Opening
> Every data platform has a centre of gravity. It's not the compute engine. It's not the storage layer. It's the thing that answers two questions: what tables exist, and where are their files?
Verdict: strong hook. Plain claim, three short sentences building to the central question; the epigraph above it primes it well.

## Closing
> The next chapter covers what happens after the catalog tells you where the data is. The data itself: Apache Iceberg's table format, from metadata trees to manifest files to the Parquet files at the bottom. The catalog is the index. Iceberg is the book.
Verdict: lands it. The "catalog is the index, Iceberg is the book" callback to the epigraph closes the loop and hands off to ch2 cleanly.

## Voice & editorial issues
1. L88 `These are features, not bugs, but they create a gravity well.` -- "features, not bugs" is a tired idiom. The same construction recurs at L277. Rewrite: `These are deliberate. They also create a gravity well.`
2. L322 `When testing the catalog integration is cheap, you test it more. When you test it more, you find problems earlier. When you find problems earlier, the catalog code is better.` -- anaphora chain reads as a slogan/copywriting cadence rather than engineering prose; verges on the trailing-summary feel. Compress: `Cheap tests get run more, so problems surface earlier, so the catalog code is better.`
3. L181 / L208-211 repeat the credential-vending mechanism nearly verbatim from L45-47 ("user authenticates ... catalog vends ... engine is a conduit"). By the third pass it's restatement. Trim the L208-211 recap since the dedicated section can assume the intro already landed it.
4. L394-398 the "Five Catalogs" section ends with three consecutive paragraphs each restating "zero engine changes / same engine binary / capability did not exist a week earlier." The point is made by L394; L396-398 partially re-make it. Tighten to one closing paragraph.
5. L130 `it's a non-starter.` -- fine, but L67 already used "doesn't fit that model" and L141 "different solutions" for the same dismissal beat across sections; the chapter leans on a single rhythm (concede strength -> "but it's not for us"). Acceptable given the comparative structure, but watch the repetition.

## Mechanical violations (PROSE only)
none

## Exclamation marks in prose
none (L197 is `format!` in a code fence; L281 is Markdown image alt-text)

## Continuity data
### Concepts INTRODUCED / defined here
- Catalog -> resolves table names to file locations
- Iceberg REST Catalog spec -> HTTP/JSON catalog protocol
- Credential vending -> catalog returns scoped temp creds
- Bearer token passthrough -> user identity to catalog
- `sqe-catalog` crate -> SQE's catalog layer
- `SessionCatalog` -> per-session bearer-token catalog wrapper
- `CredentialCache` / `VendedCredentials` -> extracts vended S3 creds
- `SqeCatalogProvider` -> DataFusion CatalogProvider bridge
- Sync-to-async catalog bridge -> cached namespace snapshot
- Dead end: Glue as primary catalog -> managed catalog lock-in

### Concepts ASSUMED (used as if already known)
- DataFusion `CatalogProvider` / `SchemaProvider` / `TableProvider` traits (used without intro; ch3 territory)
- `iceberg-rust` `RestCatalog` / `RestCatalogBuilder`
- OIDC / OAuth2 bearer tokens (named, not defined; ch4 auth)
- `moka` cache, `reqwest`, OpenDAL/`OpenDalStorageFactory`
- OPA / Cedar policy engines (referenced as ch8 governance)
- Parquet, S3, SigV4, Thrift RPC, Helm (assumed reader competence)

### Key factual / numeric claims
- "fifteen years" pretending the catalog didn't matter (L10)
- "two decades of tight coupling" (L36); "two-in-the-morning pages" (L25)
- Glue added Iceberg REST endpoint "in late 2024" (L55)
- Used Glue "for nearly two years of development" (L72)
- Databricks open-sourced Unity Catalog "mid-2024" (L78)
- Blog: "Glue Iceberg Rest Api and PyIceberg" (December 2024) (L60)
- Blog: "Unity Catalog Iceberg Rest Api and PyIceberg" (December 2024) (L83)
- Blog: "Bridging Clouds: Access Snowflake Iceberg Tables via Glue and Spark" (October 2024) (L106)
- Blog: "DuckDB S3 Tables with Iceberg using Iceberg Rest API" (January 2025) (L125)
- Snowflake-to-Glue bridge ran "about six months", "metadata drift three times", "about four hours to diagnose" (L116)
- Snowflake contributed Polaris to ASF "in 2024" (L152)
- Polaris config: `polaris_url`, `warehouse = "production"`, `metadata_cache_ttl_secs = 30` (L160-165)
- `apache/polaris:1.5.0`, `POLARIS_BOOTSTRAP_CREDENTIALS: "root:s3cr3t"`, port 8181 (L307-312)
- Catalog starts "in under two seconds" (L314); full test stack "under five seconds" (L320)
- `sqe-catalog` = "several thousand lines of Rust across eight core modules" (L352); 8 modules listed (L354-361)
- Crate deps: `iceberg`, `iceberg-catalog-rest`, `datafusion`, `moka`, `reqwest` (L365)
- Five-catalog test: `apache/hive:standalone-metastore-4.1.0` Thrift port 9083 (L376); `ghcr.io/projectnessie/nessie:0.107.5`, 0.76 line 404'd on `/iceberg/v1/config` (L378)
- Glue test in eu-central-1 (L382)
- S3 Tables endpoint `https://glue.<region>.amazonaws.com/iceberg`; props `rest.sigv4-enabled=true`, `rest.signing-name=glue`, `rest.signing-region=<region>` (L386-390)
- S3 Tables demo: namespace `table_demo_analytics`, table `table_user_events` (L392)
- Matrix score moves "from 153/189 to 158/189", "Five cells flip from partial to full" (L396)
- Polaris timestamp format diffs: "Polaris 0.9-vs-1.0", "three days of debugging" (L414)
- Credential prop names: `s3.access-key-id`, `s3.secret-access-key`, `s3.session-token` (L219-231)

### Cross-references
- L279 "a custom policy engine built into your query engine (which is what we did in Chapter 8)" -- ref to ch8
- L417 "The next chapter covers ... Apache Iceberg's table format" -- forward ref to ch2
- L270 "a pattern you'll see throughout SQE" -- soft forward ref (sync/async bridge)
- Numerous `.devto` callouts cite external blog posts (continuity with the published article record)

## Pacing
Flows well; alternates narrative (catalog evaluations) with code-grounded sections. The five-catalog section (L370-398) is the densest and slightly over-stays its welcome with three near-redundant closing paragraphs (see issue 4). No wall-of-text paragraphs; all stay within 3-5 sentences.

## Grade
Voice adherence: A-. Hits the voice consistently (short-long rhythm, opinionated, dead-end callout, transparency about failures, no forbidden words or mechanical violations); only loses points for a few restatement loops (credential vending repeated 3x, anaphora-slogan cadence at L322, redundant section closers).
