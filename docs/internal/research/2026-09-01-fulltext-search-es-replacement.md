# Fulltext search on the lakehouse: replacing OpenSearch for security logs

Date: 2026-09-01
Status: research complete, no implementation decision taken
Related: `docs/internal/lakehouse-engine-comparison.md`, issue #369 (bloom probe), MR !554 (bloom-on-write)

## The problem

Teams run OpenSearch + Kibana for security logging: OCSF-format JSON events, up to 1 TB/day, with hard bursts under DDoS. The stack is multi-tenant and highly secure. Requirements for any replacement:

- Fulltext search and fast regex over large datasets. Make-or-break.
- Grafana dashboards, a search interface, SQL endpoint and MCP with auth.
- IP datatypes and functions.
- Kubernetes, no public cloud dependency, open source preferred.
- vector.dev already transforms everything to OCSF at ingest.

ClickHouse is on the table but the team doubts its fulltext. The question behind the question: can the data live once, in Iceberg, and still serve search in under a second?

## Verdict

No open-source system serves fulltext + SQL + Spark from one Iceberg copy as of September 2026. Every engine that has an inverted index builds it into its own native storage format. The realistic pattern today is two planes: a hot search tier plus Iceberg as system of record, both fed from the same pipeline.

The gap is closable. The literature shows sub-second fulltext on object storage is achievable when the index is a snapshot-bound sidecar, resident on local NVMe, pruning 99.9% of the data before any fetch. Nobody has shipped that on Iceberg, open source. SQE already owns the hardest prerequisites: multi-tenant fine-grained security on Iceberg, an authenticated Flight SQL + MCP endpoint, a write path, and compaction.

## Part 1: market survey (Sept 2026)

### Search-native engines on object storage

| System | Fulltext | Regex | Object storage | SQL | Notes |
|---|---|---|---|---|---|
| Quickwit | Excellent (tantivy) | Yes, on the index | Native, stateless searchers | Limited | Alive post-Datadog: relicensed AGPL to Apache 2.0, commits through Aug 2026. Index = 19-25% of raw. Official Grafana plugin. Risk: single-vendor control |
| OpenObserve | SQL + fulltext, tantivy-in-Puffin sidecars | Partial | Native, Parquet files | Yes | AGPL-3.0 core. Unfiltered match_all degrades to grep |
| GreptimeDB | Bloom (~10% of raw) + tantivy (~100% of raw) indexes | Vendor claims | Native, Parquet + Puffin | Yes, first-class | Apache 2.0 core, official operator. Young, vendor-benchmarked |
| VictoriaLogs | Per-block token blooms on all body tokens | Good | No. Local disk only, S3 is backup (issue #48 open) | No | Disqualified on storage model |
| Grafana Loki | No. Blooms cover structured metadata only, body search is grep-at-query-time | No | Yes | No | Abandoned body n-gram blooms as too expensive to build |
| OpenSearch | Excellent (Lucene) | Yes | Remote store + searchable snapshots, Apache 2.0 | SQL/PPL | p99 on remote tier: 11.3s. Roadmap: "OpenSearch Table", Iceberg-compatible indexes. Track it |

### OLAP engines with fulltext

| Engine | Fulltext status | Regex accel | Iceberg | Index on external Iceberg tables |
|---|---|---|---|---|
| ClickHouse | Text index GA in 26.2, real inverted index, deterministic granule skipping | No. Regex tokenizer is an open TODO with no ETA | Read GA 25.4, write GA 26.2 | No. MergeTree-only |
| Apache Doris | Mature since ~2.0, V3 format in 3.1. Best log case studies (Tencent: 2-4x ES at 1/3 cost). Native IP types | Unconfirmed | Read solid, write on 2026 H2 roadmap | No |
| StarRocks | Preview-grade (3.3 to 4.1). N-gram bloom for LIKE | LIKE only | Strong read, mutations pending | No. Workaround: indexed materialized-view copies |
| Apache Pinot | Lucene index: the only OSS one with true regex support. Confluera (security) and Webex (100+ TB/day) run it | Yes (Lucene flavor) | StarTree Cloud only | No in OSS |
| Databend | Inverted index DDL exists (tantivy) | Unconfirmed | Unconfirmed | Unconfirmed |

The pattern is uniform. Fulltext indexing is a native-storage feature everywhere it exists. The team's ClickHouse skepticism is half right: the 26.2 text index is real and deterministic, but regex stays unaccelerated.

### Iceberg spec state

- Puffin standardizes two blob types: theta sketches and deletion vectors. Nothing else.
- Bloom-filter index PoC (apache/iceberg PR #15311, Feb 2026) showed 658 files pruned to 1, then died of inactivity in May 2026. Stalled, not rejected.
- Universal Index Framework (PR #15101): generic IndexType/createIndex/listIndexes plumbing. Open and active, no index type behind it yet.
- A V4 indexing focus group is forming on the dev list. Anton Okolnychyi's inverted-index thread (May 2025) targets MoR lookups, not fulltext.
- Apache Paimon already ships a per-file index slot (bloom + bitmap types, `rewrite_file_index` procedure). Prior art for the proposal, still no fulltext type.

### Streaming layer

RisingWave has zero search capability. It is a streaming SQL engine with a mature Iceberg sink (CoW/MoR modes, inline compaction in v2.6). Flink CDC ships an Iceberg sink v2 with compaction options. Kafka vendors converge on Iceberg topics (Confluent Tableflow, Redpanda, AutoMQ), and Apache Fluss (incubating) tiers into the lakehouse. Ingestion into Iceberg is being commoditized. The query side, especially search, is unowned. vector.dev gained a Parquet-to-S3 sink in July 2026 but has no Iceberg sink, so an Iceberg path needs a loader or an ingest endpoint regardless.

## Part 2: what the literature says about sub-second search on object storage

### The physics budget

Durner, Leis, Neumann (VLDB 2023, AnyBlob): S3-class GET costs ~30 ms base plus ~20 ms/MiB. The optimal request size is 8-16 MiB. Saturating a NIC takes ~200+ concurrent requests. One parallel wave of 16 MiB fetches costs ~350 ms of wall time no matter how many you issue. Two dependent waves and the budget is gone.

Consequences. The critical path may contain single-digit sequential round trips. Small index fragments must be coalesced into 8-16 MiB reads or pay the base-latency tax per fragment. mmap-era index formats (Lucene, tantivy) assume every byte is microseconds away and collapse in the tail on S3: OpenSearch searchable snapshots publish p50 1.1 s but p99 11.3 s; Elastic's frozen tier reaches p99.9 of 14 s. LanceDB and LangChain both abandoned tantivy-on-S3 independently for this reason.

On-prem changes the constants. NVMe-backed MinIO benches at ~24 GiB/s reads and LAN base latency is millisecond-class, but no rigorous latency-percentile study exists for on-prem object stores. A Durner-style microbenchmark against our own MinIO/RustFS is step one of any PoC.

### What the systems that hit sub-second actually do

Every published sub-second number is a warm-index number. Every cold number is 0.9-2.6 s. Four patterns repeat:

1. A tiny resident footer resolves the plan in one read. Quickwit records every byte range the term dictionary touches at build time into a "hotcache" (<0.1% of split size, RAM-resident); a cold query costs 3-4 range GETs per split instead of dozens.
2. Postings are block-bitpacked, never naive. LangChain SmithDB (DataFusion + Vortex, the closest blueprint to SQE): FST term dictionary (546 terms compress to 3.8 KiB), fixed 128-element bitpacked delta blocks, 32 MB byte-budgeted row groups, I/O coalescing within a 1 MB gap up to 16 MB windows. Fulltext P50 400 ms, P99 870 ms. They rejected tantivy because it assumes mmap and assigns its own doc IDs instead of pointing at row positions in the data file.
3. A local NVMe/RAM cache tier is mandatory, not optional. turbopuffer: cold 874 ms, warm 14 ms; S3 is the source of truth, NVMe is a read-through cache of immutable objects. Datadog Husky runs three cache tiers and a pruning funnel where only 0.4% of queries ever reach blob storage (p50 2 ms, p99 257 ms across 6.4 trillion points).
4. Wildcard and regex go through n-grams. Husky uses hashed 4-grams. The Google Code Search trigram design (Russ Cox) is canonical: ~20% index overhead, ~100x on selective phrases, only ~10x for case-insensitive search.

### Log-aware encoding is worth more than generic Parquet

CLP (OSDI 2021) and uSlope (OSDI 2024, Uber at 10+ PB/day): decomposing logs into templates, variable columns, and dictionaries gives 30-68x compression and faster search than decompress-then-grep, and 29% of queries terminate on schema metadata without touching data. OCSF is already structured, so the fulltext problem shrinks to a handful of text columns (message, raw_data, cmdline, url, user_agent). Identifier columns (IPs, hashes, session IDs) need token blooms only. Loki's cautionary number applies to the build side: their body n-gram bloom builders ran at ~4 MB/s/core and the design was scoped down as unaffordable. Index selectively.

### Key figures for the budget

| Fact | Number | Source |
|---|---|---|
| S3 GET cost model | ~30 ms base + ~20 ms/MiB, 8-16 MiB optimal | Durner et al., VLDB 2023 |
| Low-latency object tier GET | ~5 ms (S3 Express class) | Milliscale, arXiv 2603.02108 |
| Zone-map pruning ceiling on sorted data | ~100x (128 s to 1.9 s) | Moerkotte, VLDB 1998 |
| Parquet split-block bloom sizing | 10.5 bits/value for 1% FPR, 16.9 for 0.1% | parquet-format BloomFilter.md |
| Trigram index for substring/regex | ~20% of corpus, ~100x on selective phrases | Cox, swtch.com |
| Warm-cache hit rate on 0.1%-sized cache | ~80% read-only queries | Snowflake, NSDI 2020 |
| Log-token rarity | 94.5% of log tokens occur in <=6 rows | ClickHouse object-storage index redesign, 2026-03 |
| Compressed-log search economics | 30-68x compression, 29% of queries need zero data scan | CLP OSDI 2021, uSlope OSDI 2024 |

## Part 3: the realistic target architecture

Sizing for 1 TB/day raw OCSF: zstd Parquet lands at ~100 GB/day (8-12x; OCSF is repetitive). A 90-day hunt window is ~9 TB of Parquet on the object store.

```
vector.dev (OCSF) -> streaming commit to Iceberg (1-5 min cadence)
                     zstd Parquet, partitioned by (tenant, hour), time-sorted files

Tier 0  leading edge (< commit interval): in-memory ingest buffer, searched
        directly, or an accepted 1-5 minute blind spot
Tier 1  RESIDENT INDEX on query-node NVMe + RAM:
        token blooms + trigram blooms per column per file/row-group
        ~1-3% of Parquet -> ~1-3 GB/day -> 90 days = 100-300 GB. Fits one NVMe.
Tier 2  DATA on MinIO/RustFS: fetched only for surviving row groups,
        coalesced 8-16 MiB reads, hundreds of concurrent GETs
Tier 3  result cache + blob-range cache for iterative analyst workflows
```

Worked budget for the canonical hunt ("this IOC/IP/hash, last 30 days, tenant X"):

| Phase | Cost | Grounding |
|---|---|---|
| Plan + Iceberg metadata | 20-50 ms | manifests cached (SQE has this) |
| Partition prune (tenant, time) | ~0 ms | logs are time-sorted on ingest |
| Bloom probe, ~24K files x few columns | 50-150 ms | RAM/NVMe resident, no network |
| Survivors at 1% FPR | 10-100 row groups | needle tokens are rare (94.5% in <=6 rows) |
| One parallel coalesced fetch wave | 150-400 ms | LAN object store, 8-16 MiB requests |
| Decode + verify (true regex on survivors) | 100-300 ms | SIMD regex, multi-core |
| Total | 0.4-1 s | matches SmithDB's published P50/P99 |

Honest boundaries:

- Under 1 s, reliably: needle-in-haystack. IOCs, IPs, hashes, session IDs, rare tokens, regex with extractable literals. Most of security hunting.
- 1-5 s: first-touch cold data, case-insensitive substring, regex without required literals, very broad time ranges.
- Never under 1 s as a full answer: common-token queries and month-scale aggregations. Elasticsearch feels fast there only through top-k early termination. Copy the UX, not the engine: stream newest-partition-first and return the first page inside a second while the scan continues.
- DDoS bursts: index build is decoupled from commit. Unindexed segments fall back to scan (the SmithDB/OpenObserve pattern), so bursts degrade acceleration recency, never correctness.

## Part 4: consistency model. The index is separate in place, welded in logic

Three architectures hide inside "take the index out and serve it":

1. A separate search system (ES beside the lake). Dual-write, two sources of truth, deletes and compaction do not propagate, and the search tier needs its own tenant ACL model. Drift is structural. Avoid.
2. A snapshot-bound sidecar index, cached locally. The index blobs live on the same object store, referenced from Iceberg metadata, each blob naming the exact data file and snapshot it covers. A query plans against a snapshot; per data file, a valid blob means prune, a missing or stale blob means scan. The index is advisory: false positives allowed, false negatives forbidden. Staleness costs performance on the newest slice only, never correctness. The NVMe copy is a read-through cache of immutable objects, so coherence reduces to eviction. Compaction rewrites or drops affected blobs in the same commit as the data (the lesson from Puffin stats going stale under rewrite_data_files, and from !794: metadata rides the commit that changes the data). Old snapshots keep their blobs until snapshot expiry, so time travel keeps working.
3. Serving results from the index alone. Do not cross this line. Postings say which rows match; the row content is always late-materialized through the engine scan. Copying stored fields into the index rebuilds Elasticsearch: a second full copy with its own retention and delete problem, at 25-100% overhead instead of 1-3%. And the scan path is where row filters and column masks are enforced. Index-only serving would bypass policy, which is disqualifying for multi-tenant security data. Pruning-then-scan makes search results inherit tenant policy for free, because search results are scan results.

Contrast with deletion vectors: DVs are correctness-critical and needed spec-level manifest binding. Pruning indexes are optional accelerators, which is exactly why they can ship as an engine extension today without a spec change and without breaking any other reader.

## Part 5: what this means for SQE

SQE already has the pieces no candidate engine has: OIDC passthrough with row filters, column masks and tag-based policies on Iceberg; an authenticated Flight SQL + MCP endpoint; Trino wire compat (Grafana works via the Trino datasource today); a write path, compaction, bloom-on-write, and the #369 bloom-probe pruning machinery.

Phased plan:

1. Phase 1: token and trigram bloom sidecars per data file. Tokenize designated string columns at write time, store blooms as Puffin-style sidecar blobs referenced from table properties (the sqe.column-tags pattern). A has_token predicate prunes files; regex prunes via required-literal extraction against the trigram blooms, then verifies on survivors. Composes with existing scan parallelism and row-group pruning. Bounded work, and it makes SQE the only open engine with search-accelerated Iceberg.
2. Phase 2: a real inverted index per data file: FST term dictionary plus block-bitpacked postings keyed by row position, integrated into the DataFusion scan as SmithDB did. Row positions are Iceberg's native addressing (position deletes, row lineage) and map onto Parquet page selection. Prefer a purpose-built format over embedding tantivy; tantivy assumes mmap and owns its doc IDs, the two properties the object-storage literature identifies as fatal.
3. Supporting work: an ingest endpoint or Vector loader (vector.dev cannot write Iceberg), index refresh inside rewrite_data_files commits, IP types as FixedSizeBinary(16) plus CIDR UDFs, and newest-first streaming result pages for the search UX.

What SQE would still not be: Kibana. The discover UI, the ES API for XSOAR, and PPL come from elsewhere or get scoped out.

## Part 6: standardization path

The Iceberg process runs GitHub issue -> design doc -> dev-list DISCUSS -> focus group -> vote. Three attachment points already exist: the V4 indexing focus group, PR #15101 (Universal Index Framework, plumbing without a payload), and the dead-but-instructive bloom PoC #15311, which stalled on inactivity rather than merits. Running code with benchmarks on a named workload is what wins there.

Sequence: ship Phase 1 in SQE as a non-breaking engine extension; publish the blob format spec plus OCSF benchmark numbers; propose token-bloom-v1 and ngram-bloom-v1 as the first blob types under #15101, citing Paimon's shipped file-index framework as prior art and deletion vectors as the structural precedent; contribute a reader to DataFusion, where discussions #6545 and #16358 already ask for search pushdown. The spec text to fight for is small: indexes are advisory pruning structures, false negatives forbidden, tokenizer identity declared in blob properties, unindexed files scanned. Scoring stays out of scope. Expect 12-24 months to ratification; the sidecar design works without waiting. If the format standardizes, streaming writers (RisingWave, Flink sinks) can build the blooms at ingest, which turns one engine's feature into an ecosystem standard.

The only academic prior art for the exact pattern is the Puffin-backed vector index preprint (arXiv 2606.04196), which attaches ANN graphs to Iceberg snapshots as sidecars. Citable, unreviewed.

## Part 7: PoC checklist

1. Durner-style microbenchmark against our MinIO/RustFS: first-byte and total latency vs request size, cold and warm, under concurrency.
2. Bloom build cost per core on real OCSF data at our token cardinality (Loki's 4 MB/s/core is the warning number).
3. Pruning selectivity measurement: token and trigram blooms at 1% FPR against a captured week of security data, needle and broad queries.
4. End-to-end budget replay: one query through plan, probe, fetch, verify on the two-worker rig, against the table in Part 3.

## Sources

Systems: Quickwit (quickwit.io/blog: adversarial benchmark 2023-09, Lambda 2024-01, Binance 2024-07, hot_directory.rs), Datadog Husky (datadoghq.com/blog/engineering: introducing-husky, husky-query-architecture), turbopuffer (turbopuffer.com: architecture, fts-v2 2026-02, fts-v2-postings 2026-01), LangChain SmithDB (langchain.com/blog: introducing-smithdb 2026-05, inverted-index posts 2026-06), ClickHouse (clickhouse.com/blog: full-text-search-ga-release 2026-03, full-text-search-object-storage 2026-03), Grafana Loki (grafana.com/blog: loki-3.0, query-acceleration 2024-05), Elastic searchable-snapshots benchmark 2025-01, OpenSearch searchable snapshots GA blog.

Literature: Durner/Leis/Neumann VLDB 2023 (p2769); Milliscale arXiv 2603.02108; Moerkotte VLDB 1998; Vuppalapati et al. NSDI 2020; Dageville et al. SIGMOD 2016; parquet-format BloomFilter.md; Cox swtch.com/~rsc/regexp/regexp4.html; CLP OSDI 2021; uSlope OSDI 2024; LogGrep EuroSys 2023; arXiv 2606.04196.

Iceberg: puffin-spec.md; PRs #15311, #15101; Paimon fileindex spec; DataFusion discussions #6545, #16358.
