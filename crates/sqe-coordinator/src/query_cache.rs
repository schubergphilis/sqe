use std::collections::HashSet;
use std::sync::Arc;
use arrow_array::RecordBatch;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use datafusion::logical_expr::LogicalPlan;
use moka::sync::Cache;
use sha2::{Digest, Sha256};
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
    /// Secondary index: table_name → set of cache keys that touch this table
    table_index: DashMap<String, HashSet<String>>,
    max_entry_bytes: usize,
    metrics: Option<Arc<MetricsRegistry>>,
}

impl ResultCache {
    pub fn new(config: &QueryCacheConfig, metrics: Option<Arc<MetricsRegistry>>) -> Self {
        let max_bytes = config.max_memory_mb * 1024 * 1024;
        let cache = Cache::builder()
            .max_capacity(max_bytes)
            .weigher(|_key: &String, val: &Arc<CachedResult>| -> u32 {
                // Clamp to u32::MAX for very large entries
                val.size_bytes.min(u32::MAX as usize) as u32
            })
            .time_to_live(std::time::Duration::from_secs(config.ttl_secs))
            .build();
        Self {
            cache,
            table_index: DashMap::new(),
            max_entry_bytes: (config.max_entry_mb as usize) * 1024 * 1024,
            metrics,
        }
    }

    /// Compute a user-scoped cache key.
    pub fn cache_key(user: &str, sql: &str) -> String {
        let normalized: String = sql.split_whitespace().collect::<Vec<_>>().join(" ").to_uppercase();
        let input = format!("{user}:{normalized}");
        let hash = Sha256::digest(input.as_bytes());
        format!("{hash:x}")
    }

    /// Check if a query should bypass the cache (non-deterministic functions).
    pub fn should_bypass(sql: &str) -> bool {
        let upper = sql.to_uppercase();
        let non_deterministic = [
            "NOW()", "CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME",
            "RANDOM()", "UUID()", "GEN_RANDOM_UUID()",
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
            if result.is_some() { m.cache_hits.inc(); } else { m.cache_misses.inc(); }
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

        let size_bytes: usize = batches.iter()
            .map(|b| b.get_array_memory_size())
            .sum();

        if size_bytes > self.max_entry_bytes {
            debug!(size_bytes, max = self.max_entry_bytes, "Skipping cache: result too large");
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

        // Update secondary index
        for table in &tables_touched {
            self.table_index
                .entry(table.clone())
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
        if let Some((_, keys)) = self.table_index.remove(table_name) {
            let count = keys.len();
            for key in &keys {
                self.cache.invalidate(key);
            }
            if count > 0 {
                info!(table = table_name, evicted = count, "Cache invalidated for table write");
            }
            // Also clean up other table_index entries that reference evicted keys
            for key in &keys {
                for mut entry in self.table_index.iter_mut() {
                    entry.value_mut().remove(key);
                }
            }
            if let Some(ref m) = self.metrics {
                m.cache_invalidations.inc_by(count as f64);
                m.cache_entries.set(self.cache.entry_count() as f64);
                m.cache_size_bytes.set(self.cache.weighted_size() as f64);
            }
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
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));
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
        cache.store("alice", "SELECT 1", Uuid::now_v7(), vec![make_batch(5)], vec!["t1".into()]);
        assert!(cache.lookup("alice", "SELECT 1").is_some());
    }

    #[test]
    fn user_isolation() {
        let cache = ResultCache::new(&test_config(), None);
        cache.store("alice", "SELECT 1", Uuid::now_v7(), vec![make_batch(1)], vec![]);
        assert!(cache.lookup("alice", "SELECT 1").is_some());
        assert!(cache.lookup("bob", "SELECT 1").is_none());
    }

    #[test]
    fn invalidation_evicts_matching_entries() {
        let cache = ResultCache::new(&test_config(), None);
        cache.store("alice", "SELECT * FROM t1", Uuid::now_v7(), vec![make_batch(1)], vec!["t1".into()]);
        cache.store("alice", "SELECT * FROM t2", Uuid::now_v7(), vec![make_batch(1)], vec!["t2".into()]);
        // Verify both entries are retrievable before invalidation
        assert!(cache.lookup("alice", "SELECT * FROM t1").is_some(), "t1 should be in cache before invalidation");
        assert!(cache.lookup("alice", "SELECT * FROM t2").is_some(), "t2 should be in cache before invalidation");
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
        cache.store("alice", "SELECT 1", Uuid::now_v7(), vec![make_batch(100)], vec![]);
        assert!(cache.lookup("alice", "SELECT 1").is_none());
    }
}
