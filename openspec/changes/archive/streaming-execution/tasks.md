## 1. Coordinator Spill-to-Disk

- [x] 1.1 Configure FairSpillPool with memory_limit from sqe.toml
- [x] 1.2 Implement watermark system (green/yellow/orange/red thresholds)
- [x] 1.3 Implement admission control (query queue at red watermark)
- [x] 1.4 Configure spill_dir and spill_compression options
- [x] 1.5 Add spill metrics (bytes spilled, spill count, spill duration)

## 2. Late Materialization

- [x] 2.1 Implement two-phase RowFilter scan (predicate columns first, projection for survivors)
- [x] 2.2 Integrate late materialization into Iceberg scan planner
- [x] 2.3 Add late-materialization metrics (rows filtered, bytes saved)

## 3. Iceberg Scan Planning

- [x] 3.1 Implement file-level min/max pruning from Iceberg manifest statistics
- [x] 3.2 Implement sort-order detection from Iceberg metadata
- [x] 3.3 Implement PageIndex pruning for Parquet column index
- [x] 3.4 Implement TopK optimization for ORDER BY ... LIMIT N
- [x] 3.5 Add pruning metrics (files skipped, pages skipped)

## 4. S3 I/O Pipeline

- [x] 4.1 Implement request coalescing (merge adjacent byte ranges within coalesce_threshold)
- [x] 4.2 Implement footer cache (LRU cache for Parquet footers)
- [x] 4.3 Implement prefetch (background fetch of next row group)
- [x] 4.4 Add S3 I/O metrics (requests coalesced, cache hits, prefetch hits)

## 5. SortMergeJoin Fallback

- [x] 5.1 Implement optimizer rule to rewrite hash join to sort-merge join above threshold
- [x] 5.2 Configure hash_join_memory_threshold in sqe.toml
- [x] 5.3 Add join strategy metrics (hash vs sort-merge selection counts)

## 6. DoExchange Shuffle

- [x] 6.1 Implement DoExchange handler on worker Flight service
- [x] 6.2 Implement hash-partition shuffle (hash(key) % num_partitions)
- [x] 6.3 Implement range-partition shuffle (quantile-based split points)
- [x] 6.4 Add shuffle metrics (bytes shuffled, partitions, duration)

## 7. Distributed Sort

- [x] 7.1 Implement sample collection from workers
- [x] 7.2 Implement range boundary computation on coordinator
- [x] 7.3 Implement range-partition and local sort on workers
- [x] 7.4 Implement k-way merge on coordinator

## 8. Two-Phase Aggregation

- [x] 8.1 Implement partial aggregation on workers
- [x] 8.2 Implement shuffle by grouping key
- [x] 8.3 Implement final aggregation merge

## 9. Distributed Joins

- [x] 9.1 Implement broadcast join (small side replicated below broadcast_threshold)
- [x] 9.2 Implement shuffle hash join (both sides hash-partitioned)
- [x] 9.3 Implement pre-sorted merge join (sort-order detection from Iceberg metadata)
- [x] 9.4 Implement predicate transfer (build-side distinct keys as IN-list on probe side)

## 10. Multi-Endpoint Flight SQL

- [x] 10.1 Implement multi-endpoint get_flight_info (return worker endpoints)
- [x] 10.2 Implement direct-to-client streaming from workers

## 11. Stage Decomposition

- [x] 11.1 Implement stage decomposer (split plan at shuffle boundaries)
- [x] 11.2 Implement stage orchestration on coordinator

## 12. Trino Function Compatibility

- [x] 12.1 Implement date_format() function
- [x] 12.2 Implement date_parse() function
- [x] 12.3 Implement now() function
- [x] 12.4 Implement json_object() function
- [x] 12.5 Implement transaction stubs (BEGIN/COMMIT/ROLLBACK)

## 13. Observability

- [x] 13.1 Add time-to-first-row metric
- [x] 13.2 Add per-stage timing metrics
- [x] 13.3 Integrate all new metrics into Prometheus exporter

## 14. Testing

- [x] 14.1 TPC-H SF1 benchmark on 512MB coordinator (21/22 pass target)
- [x] 14.2 Unit tests for all new components (1,188 tests total across codebase)
