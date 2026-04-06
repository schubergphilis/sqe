# Research Papers

The streaming execution engine draws on decades of database systems research. This chapter lists the papers that most directly influenced SQE's design, with notes on how each idea is implemented.

## Papers and Their Influence

| Paper | Venue | How It Influenced SQE |
|-------|-------|----------------------|
| Graefe, "Volcano -- An Extensible and Parallel Query Evaluation System" | IEEE TKDE 1994 | Exchange operator model -- foundation for DoExchange shuffle |
| Shapiro, "Join Processing in Database Systems with Large Main Memories" | VLDB 1986 | Partition-based spill -- why SortMergeJoin is the safe fallback until hash join spill lands |
| Sethi et al., "Presto: SQL on Everything" | ICDE 2019 | Coordinator/worker architecture SQE mirrors; no-spill-then-spill evolution |
| Leis et al., "Morsel-Driven Parallelism: A NUMA-Aware Query Evaluation Framework" | SIGMOD 2014 | NUMA-aware execution; informs batch sizing and DataFusion's pull model |
| Pedreira et al., "Velox: Meta's Unified Execution Engine" | VLDB 2022 | Cooperative memory arbitration -- future direction for SQE |
| Raasveldt & Muhleisen, "DuckDB: An Embeddable Analytical Database" | SIGMOD 2019 | Single-node out-of-core proof that 1TB works on 16GB with proper buffer management |
| Pang et al., "Memory-Adaptive External Sorting" | VLDB 1993 | Dynamic sort splitting -- how FairSpillPool divides memory |
| Abadi et al., "Materialization Strategies in a Column-Oriented DBMS" | ICDE 2007 | Late materialization -- predicate columns first, projection for survivors |
| Yang et al., "Predicate Transfer: Efficient Pre-Filtering for Joins" | SIGMOD 2025 | Push join keys as IN-list to probe side -- skip 90%+ of probe files |

## Detailed Notes

### Graefe, "Volcano" (1994)

Volcano introduced the exchange operator as the universal mechanism for parallelism in query evaluation. An exchange operator sits between two plan tree segments and handles data redistribution -- hash partitioning, round-robin, or broadcast -- without the operators above or below knowing about parallelism. SQE's `DoExchange` shuffle is a direct implementation of this model over Arrow Flight's bidirectional streaming RPC. Each shuffle boundary in the stage decomposition corresponds to an exchange operator. The key insight from Volcano that SQE preserves: the operators themselves are single-threaded and unaware of distribution; all parallelism is encapsulated in the exchange.

### Shapiro, "Hybrid Hash Join" (1986)

Shapiro's hybrid hash join partitions the build side into buckets, keeping one bucket in memory and spilling the rest to disk. During the probe phase, the in-memory bucket is probed immediately; the spilled buckets are read back and probed sequentially. This is the standard approach for hash join spill in systems like PostgreSQL and Trino. DataFusion does not yet implement hash join spill upstream, so SQE cannot use this technique directly. Instead, SQE falls back to SortMergeJoin for large joins -- both sides are sorted (spilling via external merge sort) and then merged with constant memory. The SortMergeJoin fallback is the safe path; when DataFusion adds hash join spill, SQE can adopt the hybrid approach from Shapiro for better performance on unsorted inputs.

### Sethi et al., "Presto: SQL on Everything" (2019)

Presto's architecture -- a stateless coordinator that plans and schedules, stateless workers that execute, no shared storage between them -- is the direct model for SQE's coordinator/worker split. The paper describes Presto's evolution from a no-spill engine (all intermediate data in memory) to one that supports spill-to-disk under memory pressure. SQE followed the same evolution: Phase A added coordinator spill (the "safe" path), and Phase B added distributed computation (pushing work to workers so the coordinator handles less data). The paper's observation that "most queries fit in memory; spill is for the tail" matches SQE's experience -- 20 of 22 TPC-H queries run without spill on 512MB; only the largest sorts and aggregations trigger it.

### Leis et al., "Morsel-Driven Parallelism" (2014)

The morsel-driven model assigns small, fixed-size chunks of work (morsels) to worker threads, enabling NUMA-aware scheduling without explicit thread pinning. DataFusion's pull-based execution model, where each operator produces batches on demand, is conceptually similar. SQE's batch sizing (default 8192 rows per `RecordBatch`) is informed by this paper's finding that small, uniform work units lead to better load balancing and cache utilization. The paper also highlights the importance of avoiding global synchronization in the hot path -- a principle SQE follows by using per-operator memory consumers and `tokio::sync::watch` channels for credential refresh rather than shared mutexes.

### Pedreira et al., "Velox" (2022)

Velox introduces cooperative memory arbitration: operators register with a central arbitrator and respond to memory pressure by spilling or shrinking their buffers. This is more sophisticated than DataFusion's `FairSpillPool`, which divides memory equally and triggers spill when any consumer exceeds its share. Velox's arbitrator can make global decisions -- asking a low-priority operator to spill so a high-priority one can proceed. SQE does not implement priority-based arbitration today, but the FairSpillPool's watermark system (green/yellow/orange/red) provides a simpler version of the same concept. The Velox paper's cooperative model is the planned future direction for SQE's memory management, particularly for mixed workloads where interactive queries should preempt batch jobs.

### Raasveldt & Muhleisen, "DuckDB" (2019)

DuckDB proves that a single-node engine with proper buffer management can process datasets far larger than available memory. Its out-of-core hash join and sort implementations use disk-backed buffers that transparently page data in and out. SQE's Phase A is built on the same principle: spill-to-disk is not an error path, it is the normal execution path for large queries on small machines. The difference is that SQE operates over remote storage (S3) rather than local files, so the I/O pipeline (coalescing, footer cache, prefetch) is more critical. DuckDB's benchmark results -- 1TB queries on 16GB machines -- provided the confidence that SQE's 512MB target was achievable with the right memory management.

### Pang et al., "Memory-Adaptive External Sorting" (1993)

This paper addresses the problem of external sorting when available memory fluctuates during execution (due to other concurrent operators). The key technique is dynamic run splitting: instead of committing to a fixed run size at the start of the sort, the algorithm adapts the run size based on currently available memory. SQE's `FairSpillPool` implements a version of this: as other operators allocate and release memory, the pool available to a `SortExec` changes. When memory shrinks (another query starts), the sort produces smaller runs and spills more frequently. When memory grows (another query finishes), the sort can produce larger runs. The k-way merge at the end adapts to whatever set of runs was produced.

### Abadi et al., "Materialization Strategies" (2007)

Abadi's paper compares early materialization (read all columns, then filter) with late materialization (read predicate columns, filter, then read remaining columns for survivors) in column-oriented databases. Late materialization wins when predicates are selective because it avoids reading non-predicate columns for filtered-out rows. SQE implements late materialization in the Iceberg scan layer: the scan planner splits the column set into predicate columns and projection-only columns, reads the predicate columns first, applies the `RowFilter`, and then reads the projection columns only for rows that survived the filter. For a query like `SELECT * FROM orders WHERE status = 'CLOSED'` on a table where 5% of rows match, this reduces column-chunk reads by up to 19x (for a 20-column table where `status` is one column).

### Yang et al., "Predicate Transfer" (2025)

Predicate transfer pushes join key values from the build side of a join to the probe side's scan, filtering probe-side data before it enters the join operator. In the simplest form, the distinct join keys from the build side are collected into an `IN`-list and injected as a predicate on the probe side's table scan. SQE implements this for distributed joins: after the build side is scanned and its distinct keys are known, the coordinator pushes the key set to probe-side workers as a scan predicate. Combined with Iceberg's file-level min/max statistics, this can skip entire data files on the probe side. For selective joins (e.g., a dimension table join where only 100 of 10,000 distinct key values appear), predicate transfer skips 90%+ of probe-side files, dramatically reducing I/O. This is particularly effective for star-schema queries common in analytical workloads.
