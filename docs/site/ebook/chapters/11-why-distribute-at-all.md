# Why Distribute at All {#sec:distributed-why}

> The fastest distributed query is the one that runs on a single node.

SQE worked. By the end of Part III, we had a query engine that authenticated users via OIDC, queried Iceberg tables through Polaris, enforced row-level security through plan rewriting, wrote data via CTAS and INSERT INTO, exported Prometheus metrics, and served it all over Arrow Flight SQL. One binary. One process. No cluster.

The question wasn't whether it worked. The question was how long it would keep working.


## The Comfortable Plateau

For our initial workloads, single-node SQE was more than enough. DataFusion is fast. Iceberg's metadata layer prunes partitions before a single byte of Parquet is read. Column projection means you only deserialize what the query needs. On a machine with 16 cores and 64GB of RAM, we could scan tens of gigabytes per query without breaking a sweat.

The numbers were good. A full scan of a 5GB Iceberg table with aggregation completed in under two seconds. An analyst's typical query -- filtered by date partition, projecting five columns, grouped by region -- came back in 200 milliseconds. The dbt models ran. The dashboards refreshed. Nobody complained.

This is the part of the story where many teams make their first mistake. The engine is fast. The users are happy. So you start planning distributed execution because you assume you'll need it eventually. You spin up a Kubernetes cluster, deploy coordinators and workers, add health checks and heartbeat protocols, build a scheduler, handle partial failures, and six months later you have a distributed system that's slower than the single-node version for every query your users actually run.

We almost did this. The architecture docs from week one included distributed execution in Phase 3. The temptation was real. But we had a rule: measure first.

::: {.antipattern}
**Antipattern: Distributed by Default.** "We might need to scale" is not a requirement. "We're scanning 500GB per query across 200 partitions" is. Start single-node. Measure. Then decide. The operational cost of distribution is not zero, and you pay it on every query, even the ones that would have been faster without it.
:::


## Where Single-Node Stops

We ran TPC-H at increasing scale factors on a single node. Scale factor 1 (roughly 1GB): queries completed in under a second. Scale factor 10: a few seconds. Scale factor 100: some queries took minutes. We profiled.

The bottleneck was not CPU. DataFusion's vectorized execution on Arrow columnar data is remarkably efficient. Aggregations, hash joins, sort-merge joins -- the compute kernels were not the problem. The bottleneck was scan I/O: reading Parquet files from S3.

This makes sense when you think about it. A query against a 100GB Iceberg table might touch hundreds of Parquet files. Each file requires an HTTP GET to S3. Even with connection pooling and parallel reads, a single node has a finite number of network sockets, a finite amount of bandwidth, and a finite amount of memory to buffer incoming data. The CPU sits idle while the network fills the pipeline.

We measured the breakdown:

| Query Phase | % of Wall Clock (SF-100) |
|---|---|
| SQL parsing + planning | < 1% |
| Iceberg metadata + partition pruning | 2-5% |
| Parquet scan (S3 reads) | 60-75% |
| Filter + projection | 5-10% |
| Aggregation / join | 10-20% |
| Result serialization | < 1% |

The scan phase dominated. And the scan phase is embarrassingly parallel -- each Parquet file can be read independently, by any machine that has the credentials and knows the file path.

This is the key insight that drives everything in Part IV. The parallelism we need is not compute parallelism. It's I/O parallelism. We don't need more CPUs. We need more network pipes reading from S3 simultaneously.


## Amdahl's Law for Query Engines

Gene Amdahl told us in 1967: the speedup from parallelism is limited by the sequential fraction of the workload. If 25% of your work is inherently serial, no amount of parallelism will give you more than a 4x speedup.

For a query engine, the serial fraction includes:

- **SQL parsing and planning.** One coordinator parses the SQL, builds the logical plan, applies policy rewrites, runs the optimizer, and produces the physical plan. This cannot be parallelized. It's also cheap -- typically under 10 milliseconds.

- **Final aggregation.** A `SELECT count(*) FROM lineitem` can count rows in parallel across workers, but someone has to sum the partial counts. A `SELECT ... ORDER BY ... LIMIT 10` can sort locally, but the final merge-sort and top-10 selection happens in one place.

- **Result assembly.** The coordinator collects results from workers and streams them to the client. One connection, one stream.

The parallel fraction includes:

- **Scanning.** Reading Parquet files from S3. This is the big one. Each file is independent. Each worker reads its assigned files in parallel with every other worker.

- **Filtering and projection.** Applied locally at each worker after scanning. No coordination needed.

- **Local aggregation.** Partial aggregates computed per worker before being sent to the coordinator for final aggregation.

For our scan-heavy workloads, the parallel fraction was 60-75% of wall clock time. Amdahl's Law says that with a 70% parallel fraction:

| Workers | Theoretical Speedup | Actual (measured) |
|---|---|---|
| 1 | 1.0x | 1.0x |
| 2 | 1.5x | 1.4x |
| 4 | 2.1x | 1.8x |
| 8 | 2.6x | 2.1x |
| 16 | 2.9x | 2.2x |

The gap between theoretical and actual comes from coordination overhead: serializing scan tasks, shipping them over gRPC, collecting results, network latency. Each of these costs is small individually, but they add up. And they're fixed costs -- you pay them whether the query scans 1GB or 100GB.

The table tells you something important. Going from 1 to 2 workers gives you a meaningful speedup. Going from 8 to 16 gives you almost nothing. The law of diminishing returns is not a guideline. It's a law.


## The Crossover Point

The crossover point is the data volume where two workers on smaller machines outperform one worker on a bigger machine. Below this point, distribution is pure overhead. Above it, distribution pays for itself.

For SQE, we found this point empirically. We ran the same TPC-H queries on three configurations:

1. **Single node:** 16 cores, 64GB RAM, 10Gbps network
2. **Two workers:** each 8 cores, 32GB RAM, 10Gbps network
3. **Four workers:** each 4 cores, 16GB RAM, 10Gbps network

At scale factor 10 (~10GB), the single node won every query. The overhead of distributing -- serializing scan tasks, sending them over gRPC, collecting results -- exceeded the time saved by parallel scanning. The data just wasn't big enough for I/O to be the bottleneck.

At scale factor 50 (~50GB), two workers matched the single node on scan-heavy queries and lost on aggregation-heavy ones. The crossover was happening.

At scale factor 100 (~100GB), two workers consistently beat the single node by 30-40% on scan-heavy queries. Four workers beat it by 50-60%. The aggregation-heavy queries still favored fewer, bigger machines.

The crossover point for our hardware and network configuration was roughly 30-50GB of scanned data per query. Below that, single-node was faster. Above it, distribution paid for itself.

Your number will be different. It depends on your network bandwidth, your S3 endpoint's throughput, your machine specs, and your query patterns. The point is not the specific number. The point is that this number exists, and you should find it before committing to a distributed architecture.


## Partition-Level Parallelism

Iceberg tables are partitioned. The partition scheme defines how data files are organized -- by date, by region, by customer ID, whatever makes sense for the query patterns. When a query includes a predicate on the partition column, Iceberg's metadata layer prunes partitions that can't contain matching rows. This happens before any data is read.

After pruning, you're left with a set of data files that need to be scanned. These files are the natural unit of distribution. Each file is self-contained: it has its own schema, its own row group statistics, its own column chunks. Any machine with S3 credentials and a Parquet reader can process it independently.

SQE's distribution model works at this level. The coordinator:

1. Plans the query and produces a physical plan with an `IcebergScanExec` node
2. Asks the `IcebergScanExec` for its data file paths (post-pruning)
3. Splits those file paths across available workers
4. Replaces the `IcebergScanExec` with a `DistributedScanExec` that fans out to workers

The splitting is straightforward. The `split_files` function in `sqe-planner` distributes files using the weighted scheduler:

```rust
/// Distributes data file paths across N workers using round-robin assignment.
pub fn split_files(files: Vec<String>, num_workers: usize) -> Vec<Vec<String>> {
    if num_workers == 0 || files.is_empty() {
        return vec![];
    }
    let mut groups: Vec<Vec<String>> = (0..num_workers).map(|_| Vec::new()).collect();
    for (i, file) in files.into_iter().enumerate() {
        groups[i % num_workers].push(file);
    }
    groups
}
```

Round-robin is the starting point. The `WeightedScheduler` improves on this by assigning the heaviest tasks first (largest-first bin packing) and tracking accumulated load per worker. If a worker already has in-flight fragments, it gets fewer new ones. If a worker is unhealthy, it gets none.

The key decision: distribution happens only at the scan level. Filters, projections, and local aggregations run on the same worker that scanned the data. Only the partial results travel back to the coordinator for final aggregation. This minimizes network transfer -- the most expensive operation in a distributed query.


## The Decision Framework

After measuring, we built a decision framework. Not a flowchart -- a set of questions with clear answers.

**Question 1: How much data does the query scan?**

If the answer is under 10GB after partition pruning, stay single-node. The coordination overhead will eat any parallelism gain. DataFusion on a single machine will saturate your CPU before it saturates your network on datasets this small.

**Question 2: Is the query scan-heavy or compute-heavy?**

Scan-heavy queries (full table scans, large range scans, broad aggregations) benefit most from distribution. The bottleneck is I/O, and adding workers adds I/O bandwidth.

Compute-heavy queries (complex multi-way joins, window functions, nested subqueries) benefit less. The bottleneck is CPU, and distribution doesn't help much because the serial fraction (final join, final sort) is large. For these queries, a bigger single machine is often better than several smaller ones.

**Question 3: What's the concurrency?**

This is the factor that catches people by surprise. A single query scanning 50GB might run fine on one node. But 20 users each scanning 50GB simultaneously will not. Concurrency is where distribution earns its keep -- not by making individual queries faster, but by giving each query its own I/O bandwidth.

A single node running DataFusion has a thread pool sized to its CPU core count. When one query occupies those threads, other queries wait. This isn't a design flaw -- it's resource contention. DataFusion handles it gracefully with task scheduling, but there's a hard ceiling. A 16-core machine can run 16 parallel scan tasks. If you have 20 concurrent queries each wanting 8 parallel scans, the math doesn't work.

With two workers and 20 concurrent queries, the scheduler distributes fragments across both workers. Each worker handles half the I/O load. Each worker has its own thread pool, its own network connections, its own memory budget. Individual queries might not be faster, but the system processes twice as many queries per second. For a dbt pipeline running 30 models with `--threads=8`, this is the difference between a 40-minute batch and a 20-minute one.

**Question 4: Is the data growing?**

If you're scanning 5GB today and the table grows 1GB per month, you'll cross the distribution threshold in two years. If it grows 10GB per month, you'll cross it in three months. Plan for the trend, not the snapshot.

Here's the framework as a table:

| Condition | Recommendation |
|---|---|
| Scanned data < 10GB, low concurrency | Single-node. Don't distribute. |
| Scanned data 10-50GB, scan-heavy queries | Test both. Measure the crossover for your hardware. |
| Scanned data > 50GB, scan-heavy queries | Distribute. The I/O parallelism will pay for itself. |
| Compute-heavy queries (complex joins) | Prefer vertical scaling (bigger machine) over horizontal. |
| High concurrency (>10 concurrent queries) | Distribute for throughput, even if individual queries don't speed up. |
| Data growing > 5GB/month | Plan for distribution now, implement when you cross the threshold. |


## The SQE Mode Toggle

We built SQE so that the same binary runs in both modes. The config file controls the behavior:

```toml
[coordinator]
mode = "coordinator"
worker_urls = ["http://worker-1:50052", "http://worker-2:50052"]
```

The legacy values `hybrid`, `local`, and `distributed` are accepted as aliases but resolve to `coordinator` mode internally. The coordinator automatically falls back to local execution when no workers are healthy.

The behavior is straightforward. Workers are optional. If workers are registered and healthy, the coordinator distributes scan work to them. If no workers are available -- because none are configured, or because they've all crashed -- the coordinator falls back to local execution. This is how SQE ran for the first two weeks of development, how it runs in integration tests, and how it should run when your data fits on one machine. When workers are present and healthy, the coordinator plans and schedules; workers execute.

The fallback logic lives in `try_distribute`:

```rust
async fn try_distribute(
    &self,
    plan: Arc<dyn ExecutionPlan>,
    session: &Session,
    query_id: &uuid::Uuid,
) -> Arc<dyn ExecutionPlan> {
    // No worker registry? Execute locally.
    let registry = match self.worker_registry {
        Some(ref r) => r,
        None => return plan,
    };

    // No healthy workers? Execute locally.
    let healthy = registry.healthy_workers().await;
    if healthy.is_empty() {
        return plan;
    }

    // No IcebergScanExec in the plan? Execute locally.
    let scan_node = match find_iceberg_scan(&plan) {
        Some(node) => node,
        None => return plan,
    };

    // Fewer files than workers? Not worth distributing.
    let file_paths = scan_node.data_file_paths().await;
    if file_paths.len() < healthy.len() {
        return plan;
    }

    // Worth distributing. Build the DistributedScanExec.
    // ...
}
```

Each guard clause returns the original plan unchanged. The coordinator doesn't know or care whether it's running in single-node or distributed mode. It just asks: "Can I distribute this? Should I?" If both answers are yes, it does. If either answer is no, it runs locally.

This design means you can start with a single `sqe-server` binary, no workers, no cluster, no Kubernetes. When the data grows past the crossover point, you add workers. The coordinator discovers them through heartbeats, starts distributing scan work, and everything else stays the same. The SQL is the same. The Flight SQL connection is the same. The auth flow is the same. The policy enforcement is the same.

::: {.sovereignty}
**Sovereignty principle:** Distribution should be an operational decision, not an architectural one. The same engine, the same binary, the same config format. You add capacity by adding workers, not by migrating to a different system. The sovereignty thesis applies to your own infrastructure too -- you shouldn't be locked into a deployment model.
:::


## The Guard Clauses

The `try_distribute` method has five guard clauses, and each one was added because we hit a real problem.

**No worker registry.** If `worker_urls` is empty in the config, no `WorkerRegistry` is created. The coordinator doesn't even have the data structure to track workers. Distribution is impossible and no cycles are wasted checking.

**No healthy workers.** Workers register via heartbeat. If all workers have missed three consecutive heartbeats (15 seconds), they're marked unhealthy. The coordinator falls back to local execution rather than failing the query. We added this after a test where we killed all workers and expected graceful degradation. We got query failures instead.

**No IcebergScanExec.** Not every query touches Iceberg tables. `SHOW TABLES`, `SELECT * FROM system.runtime.queries`, `EXPLAIN` -- these are metadata queries that run entirely on the coordinator. There's nothing to distribute.

**Fewer files than workers.** If a query touches three Parquet files and you have four workers, one worker would sit idle. Worse, the coordination overhead (serializing scan tasks, gRPC calls, result collection) exceeds the time saved. The threshold is simple: if you can't give every worker at least one file, don't distribute.

**Multiple scan nodes.** A query with a join between two Iceberg tables has two `IcebergScanExec` nodes. Distributing both requires coordinating which worker gets which files from which table, and the shuffle between the join sides is expensive. We haven't built this yet. When we detect multiple scan nodes, we fall back to local execution. The comment in the code says "joins -- not yet supported." Honest beats ambitious.

::: {.fieldreport}
**Field report:** The "fewer files than workers" guard was the last one we added. We discovered it during TPC-H scale factor 0.01, where some tables have a single Parquet file. The coordinator was dutifully serializing a scan task, sending it to a worker over gRPC, waiting for the result -- and the entire round trip took longer than just reading the file locally. The fix was one comparison: `if total_files < num_workers { return plan; }`.
:::


## When Not to Distribute

I want to be explicit about this, because the rest of Part IV is about distributed execution and it would be easy to lose the thread.

**Don't distribute development and test workloads.** Your integration tests run against scale factor 0.01. Adding workers to this makes it slower, not faster, and adds failure modes that have nothing to do with what you're testing.

**Don't distribute metadata-heavy workloads.** If your users spend most of their time running `SHOW TABLES`, `DESCRIBE`, and `EXPLAIN`, they're hitting information_schema virtual providers, not Iceberg tables. A single coordinator handles this efficiently.

**Don't distribute because it looks good on an architecture diagram.** I've seen teams deploy Kubernetes operators, Helm charts, worker autoscaling, and distributed tracing infrastructure for workloads that would run faster on a single m5.4xlarge. The cost isn't just money. It's operational surface area. Every worker is a process that can crash, a network connection that can stall, a container that can be OOM-killed. You trade one problem (slow queries) for a different problem (operational complexity).

**Don't distribute until you've exhausted vertical scaling.** A machine with 32 cores and 128GB of RAM running DataFusion can process a surprising amount of data. DataFusion's thread pool scales linearly with CPU cores for scan and filter operations. Before adding workers, try a bigger machine. If the bigger machine solves your problem, congratulations -- you don't have a distributed systems problem.

The right time to distribute is when you've measured, found the bottleneck is I/O bandwidth, confirmed that vertical scaling has plateaued, and the data volume justifies the operational cost. For most teams, that's later than they think.


## The Cost You Pay

Every distributed query pays a tax that local queries don't. This tax is small per query, but it's never zero.

**Serialization.** The coordinator builds a `ScanTask` for each worker, serializes it to JSON, and packs it into a Flight `Ticket`. The worker deserializes it, configures its S3 client, and begins scanning. This adds 1-5 milliseconds per fragment.

**Network round-trips.** The coordinator opens a gRPC channel to each worker, sends the ticket, and receives a stream of Arrow record batches. Each gRPC connection has a handshake cost. Each batch has framing overhead. On our test network (10Gbps, sub-millisecond latency), this adds 5-20 milliseconds per fragment.

**Coordination.** The coordinator tracks fragment state, handles progress callbacks, manages the `tokio::select!` that races execution against a timeout deadline, and performs final aggregation. This is CPU work that doesn't exist in single-node mode.

**Failure handling.** The `DistributedScanExec` checks worker health, retries failed fragments on different workers, and falls back to local execution when no workers are available. This code path is never exercised in single-node mode. In distributed mode, it's always present, always consuming a few CPU cycles for health checks, always adding a few milliseconds of latency for the health-check evaluation.

The total tax for a query that touches four workers: roughly 30-50 milliseconds of overhead. For a query that scans 100GB and takes 30 seconds, this is noise. For a query that scans 100MB and takes 200 milliseconds, it's a 15-25% penalty. This is why the guard clauses matter. Don't distribute small queries.


## What We Built (And What We Deferred)

The distribution model in SQE as of this writing handles the common case: scan-heavy queries against a single Iceberg table, distributed across a pool of stateless workers. This covers our primary workload -- batch analytics, dbt models, dashboard queries.

What we built:

- Scan-level distribution with round-robin and weighted scheduling
- Automatic fallback to local execution when distribution isn't beneficial
- Worker health tracking via heartbeats with automatic failover
- Fragment retry on worker failure (up to two attempts per fragment)
- Credential passthrough so workers read S3 as the authenticated user

The weighted scheduler deserves a mention. The naive approach is round-robin: give worker 1 files 0, 3, 6; give worker 2 files 1, 4, 7; and so on. This works when all files are the same size. They never are. Iceberg data files vary in size depending on when they were written, how the data was partitioned, and whether compaction has run. The `WeightedScheduler` estimates each scan task's cost by file count, sorts tasks heaviest-first, and assigns each one to the worker with the lowest accumulated load. Largest-first bin packing. It's a well-known heuristic, and it produces good-enough balance without the complexity of optimal scheduling algorithms that NP-hard problems would require.

What we deferred:

- **Distributed joins.** Queries that join two large Iceberg tables still run locally. The shuffle cost of redistributing both sides of a join across workers is significant, and the scheduling complexity is an order of magnitude higher. This is the next major feature.
- **Distributed aggregation.** Partial aggregations run on workers, but the final aggregation runs on the coordinator. For queries with high-cardinality GROUP BY, this creates a bottleneck. Two-phase aggregation with distributed merge is planned but not implemented.
- **Data locality.** Workers don't have local caches. Every scan reads from S3. A worker that's co-located with an S3 shard would be faster, but our S3 endpoints (RustFS in test, AWS S3 in production) don't expose locality information.
- **Dynamic scaling.** The worker pool is static -- defined in config. Autoscaling based on query backlog is an operational concern we pushed to Kubernetes HPA rather than building into the engine.

There are also limitations within what we did build:

- **Partition skew.** The weighted scheduler estimates cost by file count. File count is a poor proxy when predicate selectivity varies across partitions. A partition with 10 files where the predicate matches 1% of rows finishes in milliseconds. A partition with 10 files where the predicate matches everything takes seconds. The scheduler sees them as equal. Manifest-level statistics (row counts, column min/max) could improve this, but we do not read them during scheduling today.
- **No partial aggregation pushdown.** Workers return raw batches. A `COUNT(*)` query reads every row from every worker back to the coordinator, which then counts them. The correct optimization is to count locally on each worker and return a single integer per fragment. DataFusion supports partial aggregation in its physical plan, but wiring it through the `ScanTask` protocol is not trivial. It is on the roadmap.
- **Straggler mitigation.** If one worker is slow -- S3 throttling, noisy neighbor, GC pause on the Polaris JVM -- the entire query waits. The weighted scheduler reduces the probability of stragglers by balancing load, but it cannot eliminate them. Speculative execution (launching a duplicate fragment on a different worker when one is late) is a known technique we have not implemented. The complexity is significant: you need cancellation, deduplication, and a cost model that knows when speculation is worth the extra I/O.
- **Coordinator NIC saturation.** Every result byte flows through the coordinator on its way to the client. For low-selectivity queries that return large result sets, the coordinator's network interface becomes the bottleneck. Four workers each streaming 1GB of results saturate a 10Gbps NIC in seconds. Direct-to-client streaming from workers would solve this, but it breaks the trust model where the coordinator is the only client-facing component.

Each of these is a real limitation. Each has a workaround. And each is less important than getting the basic distribution model right and reliable, which is what Chapters 12 through 14 are about.


## The 1TB Problem on Small Servers

Everything above assumes you can give the coordinator enough memory to hold intermediate results. But what happens when the data doesn't fit?

### The Memory Math

Consider `SELECT * FROM lineitem ORDER BY l_shipdate` on a 1TB `lineitem` table. The sort operator must consume the entire input before producing any output. On a single coordinator, that means 1TB of intermediate data in memory -- or, with spill-to-disk, 1TB of sorted runs on local storage. Even with efficient external merge sort, the coordinator's NIC and disk bandwidth become the bottleneck.

Now add eight workers. Each worker sorts its 125GB partition locally (spilling 125GB to its own local disk). The coordinator performs a k-way merge of eight pre-sorted streams, consuming only a small buffer per stream. Total spill per worker: 125GB. Total memory on the coordinator for the merge: 8 * buffer_size. The work and the spill are distributed.

The same arithmetic applies to aggregation. A `GROUP BY` with millions of distinct groups requires a hash table proportional to the group count. On one coordinator, that hash table must fit in memory (or the engine OOMs -- DataFusion's `GroupedHashAggregate` does not spill today). With two-phase aggregation across eight workers, each worker handles 1/8 of the groups, and the hash table fits.

### How Others Handle This

**Trino** originally had no spill support at all -- the entire intermediate dataset had to fit in memory across the cluster. Presto/Trino added spill-to-disk in later versions, but it remains opt-in and the documentation warns about performance degradation. Trino's approach is "provision enough memory" first, spill as a safety net.

**Spark** takes the opposite approach: it spills aggressively and early. Every shuffle writes to disk. Every sort writes to disk. The `tungsten` off-heap memory manager and `ExternalSorter` make this efficient, but the baseline cost of always writing to disk is non-trivial. Spark's model assumes disk I/O is cheap (true for local NVMe, less true for network-attached storage).

**DuckDB** proves that a single-node engine can handle datasets far larger than memory. Its buffer manager pages data between memory and disk transparently, with out-of-core hash join and sort. DuckDB processes 1TB on 16GB machines by treating disk as an extension of memory. The limitation is single-node I/O bandwidth -- one machine, one NIC, one disk controller.

**ClickHouse** avoids the problem for most workloads by using pre-aggregated materialized views and merge trees. For ad-hoc queries, it distributes across shards but each shard processes its partition independently. ClickHouse's model works well for append-heavy workloads but requires schema-level planning that Iceberg's schema-on-read approach doesn't impose.

### SQE's Hybrid Approach

SQE combines both strategies:

**Phase A (spill first):** the coordinator uses `FairSpillPool` with watermarks to manage memory pressure. Sort operators spill sorted runs to disk. Join operators fall back to SortMergeJoin when the build side exceeds the memory threshold. Late materialization and file pruning reduce the amount of data that enters the pipeline in the first place. This handles the "survival" case: queries complete correctly, though large sorts and aggregations may be slow.

**Phase B (push computation down):** the coordinator decomposes the physical plan into stages and pushes computation to workers via Arrow Flight `DoExchange`. Sorts are range-partitioned across workers. Aggregations run in two phases (partial on workers, final on coordinator or designated workers). Joins use broadcast, shuffle hash, or pre-sorted merge depending on input size. This handles the "performance" case: queries complete quickly because work and memory pressure are distributed.

The key design choice is that Phase A is always active. Even in distributed mode, each worker uses `FairSpillPool` for its local execution. Phase B adds distribution on top of Phase A's memory safety. A worker that runs out of memory spills to disk rather than crashing -- the same safety net applies at every level.


## The Lesson

Distribution is not a feature. It's a trade-off. You trade simplicity for throughput. You trade one failure mode (slow queries) for many failure modes (worker crashes, network partitions, gRPC hangs, S3 throttling, credential expiry, partial results). You trade a single process you can attach a debugger to for a fleet of processes communicating over the network.

The trade-off is worth it when the numbers say so. Not when the architecture diagram says so. Not when the job description says so. Not when the conference talk says so.

We measured. We found the crossover point. We built the mode toggle so the same binary works either way. And we added guard clauses so the system itself decides, per query, whether distribution is worth the cost.

The fastest distributed query is the one that runs on a single node. Don't distribute until you must. And when you must, the next three chapters will show you how.

::: {.ailog}
**AI Logbook:** The AI implemented `try_distribute` with its five guard clauses and the `split_files` round-robin function in one pass. The human ran TPC-H at increasing scale factors to find the crossover point where distribution pays for itself: 30-50GB of scanned data on our hardware. The "fewer files than workers" guard clause was the last one added, after the human observed that distributing a single-file scan at scale factor 0.01 was slower than reading it locally. The AI never questioned whether distribution was needed; the human had to measure first.
:::
