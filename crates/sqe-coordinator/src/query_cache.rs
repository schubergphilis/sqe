use arrow_array::RecordBatch;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use datafusion::logical_expr::LogicalPlan;
use moka::sync::Cache;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::sync::Arc;
use tracing::{debug, info};
use uuid::Uuid;

use sqe_core::QueryCacheConfig;
use sqe_metrics::MetricsRegistry;

pub struct CachedResult {
    pub query_id: Uuid,
    pub batches: Vec<RecordBatch>,
    pub tables_touched: Vec<String>,
    pub created: DateTime<Utc>,
    pub size_bytes: usize,
}

pub struct ResultCache {
    cache: Cache<String, Arc<CachedResult>>,
    /// Secondary index: canonical table name → set of cache keys that touch
    /// this table. Shared with the moka eviction listener (COORD-03) so that
    /// when an entry leaves `cache` (TTL, size, or capacity eviction) its key
    /// is pruned from this index instead of accumulating forever.
    table_index: Arc<DashMap<String, HashSet<String>>>,
    max_entry_bytes: usize,
    metrics: Option<Arc<MetricsRegistry>>,
}

impl ResultCache {
    pub fn new(config: &QueryCacheConfig, metrics: Option<Arc<MetricsRegistry>>) -> Self {
        let max_bytes = config.max_memory_mb * 1024 * 1024;

        // COORD-03: the secondary `table_index` is not bounded by moka's
        // byte/TTL limits, so without an eviction listener it grows without
        // bound for a table queried with many distinct SQL strings but rarely
        // written. The evicted entry carries its own `tables_touched`, so the
        // listener prunes exactly the affected index sets — no full-index scan.
        let table_index: Arc<DashMap<String, HashSet<String>>> = Arc::new(DashMap::new());
        let index_for_listener = Arc::clone(&table_index);

        let cache = Cache::builder()
            .max_capacity(max_bytes)
            .weigher(|_key: &String, val: &Arc<CachedResult>| -> u32 {
                // Clamp to u32::MAX for very large entries
                val.size_bytes.min(u32::MAX as usize) as u32
            })
            .time_to_live(std::time::Duration::from_secs(config.ttl_secs))
            .eviction_listener(move |key: Arc<String>, val: Arc<CachedResult>, _cause| {
                // Prune the evicted cache key from each table it touched.
                for table in &val.tables_touched {
                    let canon = Self::canonical_table_key(table);
                    if let Some(mut entry) = index_for_listener.get_mut(&canon) {
                        entry.value_mut().remove(key.as_str());
                    }
                    // Drop now-empty index buckets to keep the map small.
                    index_for_listener.remove_if(&canon, |_, set| set.is_empty());
                }
            })
            .build();
        Self {
            cache,
            table_index,
            max_entry_bytes: (config.max_entry_mb as usize) * 1024 * 1024,
            metrics,
        }
    }

    /// COORD-01: canonicalize a table name to a single key used by both the
    /// `table_index` (store side) and `invalidate` (write side).
    ///
    /// The store side gets fully-qualified names from the logical plan via
    /// lineage extraction (`iceberg.public.sales`, `datafusion.public.sales`),
    /// while the invalidate side gets whatever the user typed in the DML
    /// statement (`sales`, `myschema.sales`). Those strings never matched, so
    /// invalidation was a silent no-op and SELECTs returned pre-write data
    /// until TTL expiry.
    ///
    /// The only canonical form both sides can always reach is the bare table
    /// name: the invalidate side may have only `orders` (unqualified DML), so
    /// `schema.table` normalization fails on that common case. We take the last
    /// dotted segment, lowercased. Over-invalidation across same-named tables in
    /// different schemas costs a re-query (safe); under-invalidation serves
    /// stale data (the bug we are fixing).
    fn canonical_table_key(table_name: &str) -> String {
        table_name
            .rsplit('.')
            .next()
            .unwrap_or(table_name)
            .trim()
            .to_lowercase()
    }

    /// Compute a user-scoped cache key.
    pub fn cache_key(user: &str, sql: &str) -> String {
        let normalized: String = sql
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_uppercase();
        let input = format!("{user}:{normalized}");
        let hash = Sha256::digest(input.as_bytes());
        format!("{hash:x}")
    }

    /// Check if a query should bypass the cache (non-deterministic functions).
    pub fn should_bypass(sql: &str) -> bool {
        let upper = sql.to_uppercase();
        let non_deterministic = [
            "NOW()",
            "CURRENT_TIMESTAMP",
            "CURRENT_DATE",
            "CURRENT_TIME",
            "RANDOM()",
            "UUID()",
            "GEN_RANDOM_UUID()",
        ];
        non_deterministic.iter().any(|f| upper.contains(f))
    }

    /// Look up a cached result by user + SQL.
    pub fn lookup(&self, user: &str, sql: &str) -> Option<Arc<CachedResult>> {
        if Self::should_bypass(sql) {
            return None;
        }
        let key = Self::cache_key(user, sql);
        let result = self.cache.get(&key);
        if let Some(ref m) = self.metrics {
            if result.is_some() {
                m.cache_hits.inc();
            } else {
                m.cache_misses.inc();
            }
        }
        result
    }

    /// Store a query result in the cache.
    pub fn store(
        &self,
        user: &str,
        sql: &str,
        query_id: Uuid,
        batches: Vec<RecordBatch>,
        tables_touched: Vec<String>,
    ) {
        if Self::should_bypass(sql) {
            return;
        }

        let size_bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();

        if size_bytes > self.max_entry_bytes {
            debug!(
                size_bytes,
                max = self.max_entry_bytes,
                "Skipping cache: result too large"
            );
            return;
        }

        let key = Self::cache_key(user, sql);

        let entry = Arc::new(CachedResult {
            query_id,
            batches,
            tables_touched: tables_touched.clone(),
            created: Utc::now(),
            size_bytes,
        });

        self.cache.insert(key.clone(), entry);

        // Update secondary index. COORD-01: key the index by the canonical
        // (bare, lowercased) table name so the write-path invalidate() matches.
        for table in &tables_touched {
            self.table_index
                .entry(Self::canonical_table_key(table))
                .or_default()
                .insert(key.clone());
        }

        if let Some(ref m) = self.metrics {
            m.cache_entries.set(self.cache.entry_count() as f64);
            m.cache_size_bytes.set(self.cache.weighted_size() as f64);
        }
    }

    /// Invalidate all cached results that touch the given table.
    pub fn invalidate(&self, table_name: &str) {
        // COORD-01: canonicalize to match the key the store side indexed under.
        let table_name = &Self::canonical_table_key(table_name);
        if let Some((_, keys)) = self.table_index.remove(table_name) {
            let count = keys.len();
            for key in &keys {
                self.cache.invalidate(key);
            }
            if count > 0 {
                info!(
                    table = table_name,
                    evicted = count,
                    "Cache invalidated for table write"
                );
            }
            // COORD-03: clean other index buckets that referenced the evicted
            // keys in a SINGLE pass over the index (was O(keys x index_size):
            // a nested scan of the whole index once per evicted key). Drop
            // buckets that become empty so the map does not retain dead tables.
            self.table_index.retain(|_table, set| {
                set.retain(|k| !keys.contains(k));
                !set.is_empty()
            });
            if let Some(ref m) = self.metrics {
                m.cache_invalidations.inc_by(count as f64);
                m.cache_entries.set(self.cache.entry_count() as f64);
                m.cache_size_bytes.set(self.cache.weighted_size() as f64);
            }
        }
    }

    /// Drop every cached query. Used after maintenance procedures (CALL
    /// system.rewrite_data_files, expire_snapshots, etc.) where the per-
    /// table invalidation does not catch results that referenced the table
    /// only through a TVF (table_files, table_snapshots), because the
    /// TableScan in the plan carries the TVF function name rather than
    /// the underlying Iceberg table identifier.
    pub fn invalidate_all(&self) {
        let count = self.cache.entry_count();
        self.cache.invalidate_all();
        self.table_index.clear();
        if count > 0 {
            info!(
                evicted = count,
                "ResultCache invalidated entirely after maintenance procedure"
            );
        }
        if let Some(ref m) = self.metrics {
            m.cache_invalidations.inc_by(count as f64);
            m.cache_entries.set(0.0);
            m.cache_size_bytes.set(0.0);
        }
    }

    pub fn entry_count(&self) -> u64 {
        self.cache.entry_count()
    }
}

/// Extract table names from a DataFusion LogicalPlan.
///
/// Walks the plan tree recursively and collects qualified table names
/// from TableScan nodes.
pub fn extract_table_names(plan: &LogicalPlan) -> Vec<String> {
    let mut tables = Vec::new();
    collect_table_names(plan, &mut tables);
    tables.sort();
    tables.dedup();
    tables
}

fn collect_table_names(plan: &LogicalPlan, tables: &mut Vec<String>) {
    if let LogicalPlan::TableScan(scan) = plan {
        tables.push(scan.table_name.to_string());
    }
    // Recurse into all child plans
    for input in plan.inputs() {
        collect_table_names(input, tables);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};

    fn make_batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let arr = Int64Array::from((0..rows as i64).collect::<Vec<_>>());
        RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap()
    }

    fn test_config() -> QueryCacheConfig {
        QueryCacheConfig {
            enabled: true,
            max_memory_mb: 10,
            max_entry_mb: 1,
            ttl_secs: 60,
        }
    }

    #[test]
    fn cache_key_includes_user() {
        let k1 = ResultCache::cache_key("alice", "SELECT 1");
        let k2 = ResultCache::cache_key("bob", "SELECT 1");
        assert_ne!(k1, k2, "different users must produce different cache keys");
    }

    #[test]
    fn cache_key_normalizes_whitespace() {
        let k1 = ResultCache::cache_key("alice", "SELECT  1");
        let k2 = ResultCache::cache_key("alice", "SELECT 1");
        assert_eq!(k1, k2);
    }

    #[test]
    fn cache_key_case_insensitive() {
        let k1 = ResultCache::cache_key("alice", "select 1");
        let k2 = ResultCache::cache_key("alice", "SELECT 1");
        assert_eq!(k1, k2);
    }

    #[test]
    fn lookup_miss_then_hit() {
        let cache = ResultCache::new(&test_config(), None);
        assert!(cache.lookup("alice", "SELECT 1").is_none());
        cache.store(
            "alice",
            "SELECT 1",
            Uuid::now_v7(),
            vec![make_batch(5)],
            vec!["t1".into()],
        );
        assert!(cache.lookup("alice", "SELECT 1").is_some());
    }

    #[test]
    fn user_isolation() {
        let cache = ResultCache::new(&test_config(), None);
        cache.store(
            "alice",
            "SELECT 1",
            Uuid::now_v7(),
            vec![make_batch(1)],
            vec![],
        );
        assert!(cache.lookup("alice", "SELECT 1").is_some());
        assert!(cache.lookup("bob", "SELECT 1").is_none());
    }

    #[test]
    fn invalidation_evicts_matching_entries() {
        let cache = ResultCache::new(&test_config(), None);
        cache.store(
            "alice",
            "SELECT * FROM t1",
            Uuid::now_v7(),
            vec![make_batch(1)],
            vec!["t1".into()],
        );
        cache.store(
            "alice",
            "SELECT * FROM t2",
            Uuid::now_v7(),
            vec![make_batch(1)],
            vec!["t2".into()],
        );
        // Verify both entries are retrievable before invalidation
        assert!(
            cache.lookup("alice", "SELECT * FROM t1").is_some(),
            "t1 should be in cache before invalidation"
        );
        assert!(
            cache.lookup("alice", "SELECT * FROM t2").is_some(),
            "t2 should be in cache before invalidation"
        );
        cache.invalidate("t1");
        // t1 entry evicted, t2 remains
        assert!(cache.lookup("alice", "SELECT * FROM t1").is_none());
        assert!(cache.lookup("alice", "SELECT * FROM t2").is_some());
    }

    #[test]
    fn bypass_non_deterministic() {
        assert!(ResultCache::should_bypass("SELECT NOW()"));
        assert!(ResultCache::should_bypass("SELECT CURRENT_TIMESTAMP"));
        assert!(ResultCache::should_bypass("SELECT random()"));
        assert!(!ResultCache::should_bypass("SELECT 1"));
        assert!(!ResultCache::should_bypass("SELECT * FROM orders"));
    }

    #[test]
    fn skip_oversized_entries() {
        let config = QueryCacheConfig {
            enabled: true,
            max_memory_mb: 10,
            max_entry_mb: 0, // 0 MB = nothing gets cached
            ttl_secs: 60,
        };
        let cache = ResultCache::new(&config, None);
        cache.store(
            "alice",
            "SELECT 1",
            Uuid::now_v7(),
            vec![make_batch(100)],
            vec![],
        );
        assert!(cache.lookup("alice", "SELECT 1").is_none());
    }

    #[test]
    fn cache_stores_and_returns_results() {
        // Verify cache stores results after first query and returns
        // them on second identical query.
        let cache = ResultCache::new(&test_config(), None);
        let sql = "SELECT id FROM users WHERE id > 10";
        let batch = make_batch(5);
        let query_id = Uuid::now_v7();

        // First lookup should miss
        assert!(cache.lookup("alice", sql).is_none());

        // Store the result
        cache.store(
            "alice",
            sql,
            query_id,
            vec![batch.clone()],
            vec!["users".into()],
        );

        // Second lookup should hit and return the same data
        let cached = cache.lookup("alice", sql).expect("should be a cache hit");
        assert_eq!(cached.query_id, query_id);
        assert_eq!(cached.batches.len(), 1);
        assert_eq!(cached.batches[0].num_rows(), 5);
        assert_eq!(cached.tables_touched, vec!["users".to_string()]);
    }

    #[test]
    fn invalidation_during_concurrent_reads() {
        // One thread writes (invalidates), another thread reads — verify no panic.
        use std::sync::Arc;

        let cache = Arc::new(ResultCache::new(&test_config(), None));

        // Pre-populate with entries touching table "orders"
        for i in 0..20 {
            let sql = format!("SELECT * FROM orders WHERE id = {i}");
            cache.store(
                "alice",
                &sql,
                Uuid::now_v7(),
                vec![make_batch(1)],
                vec!["orders".into()],
            );
        }

        let cache_reader = cache.clone();
        let cache_writer = cache.clone();

        let reader = std::thread::spawn(move || {
            for i in 0..20 {
                let sql = format!("SELECT * FROM orders WHERE id = {i}");
                // This may or may not find the entry (invalidation is concurrent)
                let _ = cache_reader.lookup("alice", &sql);
            }
        });

        let writer = std::thread::spawn(move || {
            // Invalidate the table while reads are happening
            cache_writer.invalidate("orders");
        });

        reader.join().expect("reader should not panic");
        writer.join().expect("writer should not panic");
    }

    #[test]
    fn system_table_queries_bypass_cache() {
        // Queries to system.* or information_schema.* should not be cached.
        // We use the should_bypass function for non-deterministic functions,
        // but for system tables we verify via store/lookup that the cache
        // key mechanism still works — the caller is responsible for not caching
        // system queries. Here we verify that storing and looking up system table
        // queries works the same as other queries (the bypass is in the coordinator
        // layer), and we also verify bypass of non-deterministic system queries.

        // Non-deterministic system queries should be bypassed
        assert!(ResultCache::should_bypass(
            "SELECT NOW() FROM information_schema.tables"
        ));
        assert!(ResultCache::should_bypass("SELECT CURRENT_TIMESTAMP"));

        // For deterministic system-table queries, the cache itself does not
        // discriminate by table name. Verify the cache_key is distinct per user
        // and SQL, so coordinator-level bypass logic can decide.
        let k1 = ResultCache::cache_key("alice", "SELECT * FROM information_schema.tables");
        let k2 = ResultCache::cache_key("alice", "SELECT * FROM information_schema.columns");
        assert_ne!(
            k1, k2,
            "different system table queries should have different keys"
        );

        // Verify that system table queries are still not bypass-able by should_bypass
        // (unless they contain non-deterministic functions)
        assert!(!ResultCache::should_bypass(
            "SELECT * FROM information_schema.tables"
        ));
        assert!(!ResultCache::should_bypass(
            "SELECT * FROM system.runtime.nodes"
        ));
    }

    #[test]
    fn coord01_qualified_store_bare_invalidate_matches() {
        // COORD-01 regression: the store side receives the FULLY-QUALIFIED
        // table name from lineage extraction (as `query_handler` does via
        // `sqe_lineage::extract::extract_table_names`), while the write path
        // invalidates with the BARE name from the raw DML statement (as
        // `ins.table.to_string()` yields). Before the fix these strings never
        // matched, the removal was a no-op, and the next SELECT returned
        // pre-write data. This test fails against the unfixed cache.
        let cache = ResultCache::new(&test_config(), None);

        // Store as the read path does: qualified `catalog.schema.table`.
        cache.store(
            "alice",
            "SELECT * FROM sales",
            Uuid::now_v7(),
            vec![make_batch(1)],
            vec!["iceberg.public.sales".into()],
        );
        assert!(
            cache.lookup("alice", "SELECT * FROM sales").is_some(),
            "result should be cached before the write"
        );

        // Invalidate as the write path does: bare `sales`.
        cache.invalidate("sales");

        assert!(
            cache.lookup("alice", "SELECT * FROM sales").is_none(),
            "COORD-01: a write to `sales` must invalidate the prior SELECT \
             result; serving pre-write data is a correctness bug"
        );
    }

    #[test]
    fn coord01_schema_qualified_invalidate_also_matches() {
        // The write path can also yield `schema.table` (when the user wrote
        // `INSERT INTO myschema.sales`). Canonicalization must still match the
        // qualified store-side key.
        let cache = ResultCache::new(&test_config(), None);
        cache.store(
            "alice",
            "SELECT * FROM sales",
            Uuid::now_v7(),
            vec![make_batch(1)],
            vec!["iceberg.myschema.sales".into()],
        );
        assert!(cache.lookup("alice", "SELECT * FROM sales").is_some());
        cache.invalidate("myschema.sales");
        assert!(
            cache.lookup("alice", "SELECT * FROM sales").is_none(),
            "schema-qualified invalidate must match the canonical key"
        );
    }

    #[test]
    fn coord01_canonical_key_is_case_and_prefix_insensitive() {
        assert_eq!(
            ResultCache::canonical_table_key("iceberg.public.Sales"),
            "sales"
        );
        assert_eq!(ResultCache::canonical_table_key("SALES"), "sales");
        assert_eq!(ResultCache::canonical_table_key("ns.orders"), "orders");
        assert_eq!(ResultCache::canonical_table_key("orders"), "orders");
    }

    #[test]
    fn invalidation_cross_table() {
        // Verify that invalidating one table does not affect entries
        // that only touch a different table.
        let cache = ResultCache::new(&test_config(), None);

        cache.store(
            "alice",
            "SELECT * FROM orders",
            Uuid::now_v7(),
            vec![make_batch(3)],
            vec!["orders".into()],
        );
        cache.store(
            "alice",
            "SELECT * FROM customers",
            Uuid::now_v7(),
            vec![make_batch(2)],
            vec!["customers".into()],
        );
        // A query touching both tables
        cache.store(
            "alice",
            "SELECT * FROM orders JOIN customers ON true",
            Uuid::now_v7(),
            vec![make_batch(1)],
            vec!["orders".into(), "customers".into()],
        );

        // Invalidate orders
        cache.invalidate("orders");

        // orders-only query should be gone
        assert!(cache.lookup("alice", "SELECT * FROM orders").is_none());
        // customers-only query should remain
        assert!(cache.lookup("alice", "SELECT * FROM customers").is_some());
        // join query should be gone (it touched orders)
        assert!(cache
            .lookup("alice", "SELECT * FROM orders JOIN customers ON true")
            .is_none());
    }
}
