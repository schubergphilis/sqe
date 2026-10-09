//! Per-execution admission layered over the coordinator's shared memory pool.
//!
//! This bounds DataFusion's fallible operator reservations, including hash
//! builds whose input statistics are absent. Untracked allocations and the
//! MemoryPool contract's infallible `grow` are not a process RSS limit.

use std::fmt;
use std::sync::Arc;

use crate::memory_headroom::HeadroomBudget;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::SessionContext;

#[derive(Debug)]
struct QueryMemoryPool {
    shared: Arc<dyn MemoryPool>,
    limit: usize,
    /// `limit`, with the share only non-spillable consumers may use
    /// (`coordinator.unspillable_headroom_percent`).
    budget: HeadroomBudget,
}

impl fmt::Display for QueryMemoryPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "query memory ({}/{} bytes)", self.reserved(), self.limit)
    }
}

impl MemoryPool for QueryMemoryPool {
    fn name(&self) -> &str {
        "QueryMemoryPool"
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.shared.register(consumer);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.shared.unregister(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        // DataFusion requires this method to succeed; operators requiring
        // admission must use try_grow. Still account these bytes in both pools.
        self.budget.charge(reservation.consumer(), additional);
        self.shared.grow(reservation, additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.shared.shrink(reservation, shrink);
        self.budget.release(reservation.consumer(), shrink);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        self.budget
            .try_charge(reservation.consumer(), additional, "query.max_query_memory")?;
        if let Err(error) = self.shared.try_grow(reservation, additional) {
            self.budget.release(reservation.consumer(), additional);
            return Err(error);
        }
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.budget.charged()
    }

    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Finite(match self.shared.memory_limit() {
            MemoryLimit::Finite(shared) => shared.min(self.limit),
            _ => self.limit,
        })
    }
}

/// Preserve resource refusal through buffered, streaming and Flight paths.
pub(crate) fn execution_error(error: &DataFusionError) -> sqe_core::SqeError {
    let code = if matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)) {
        sqe_core::SqeErrorCode::ResourceExhausted
    } else {
        sqe_core::SqeErrorCode::ExecutionFailed
    };
    sqe_core::SqeError::with_code(code, format!("Query execution failed: {error}"))
}

/// Clone session state for one execution, preserving catalogs, object stores,
/// caches and the disk manager. Cached sessions must never share query quotas.
/// `headroom_percent` of `limit` is kept for non-spillable consumers
/// ([`crate::memory_headroom`]).
pub(crate) fn with_query_limit(
    ctx: SessionContext,
    limit: usize,
    headroom_percent: u8,
) -> SessionContext {
    let shared = Arc::clone(&ctx.runtime_env().memory_pool);
    with_query_limit_over(ctx, shared, limit, headroom_percent)
}

/// [`with_query_limit`] with the shared pool given explicitly, for a context
/// whose own pool is already a query pool. The join fallback (#784) uses it
/// to give the retry a fresh quota instead of nesting it inside the first
/// attempt's.
pub(crate) fn with_query_limit_over(
    ctx: SessionContext,
    shared: Arc<dyn MemoryPool>,
    limit: usize,
    headroom_percent: u8,
) -> SessionContext {
    if limit == 0 {
        return ctx;
    }
    let mut runtime = (*ctx.runtime_env()).clone();
    runtime.memory_pool = Arc::new(QueryMemoryPool {
        shared,
        limit,
        budget: HeadroomBudget::new(limit, headroom_percent, true),
    });
    let state = SessionStateBuilder::from(ctx.state())
        .with_runtime_env(Arc::new(runtime))
        .build();
    SessionContext::new_with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn pool(shared: Arc<dyn MemoryPool>, limit: usize) -> Arc<dyn MemoryPool> {
        Arc::new(QueryMemoryPool {
            shared,
            limit,
            budget: HeadroomBudget::new(limit, 0, true),
        })
    }

    #[test]
    fn independent_query_limits_still_share_global_admission() {
        let shared: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(100));
        let first = pool(shared.clone(), 60);
        let second = pool(shared.clone(), 60);
        let a = MemoryConsumer::new("a").register(&first);
        let b = MemoryConsumer::new("b").register(&second);
        a.try_grow(60).unwrap();
        assert!(a.try_grow(1).is_err());
        assert!(b.try_grow(60).is_err());
        assert_eq!(
            second.reserved(),
            0,
            "global rejection must roll back query charge"
        );
        b.try_grow(40).unwrap();
        assert_eq!(shared.reserved(), 100);
        drop(a);
        b.try_grow(20).unwrap();
        assert_eq!(second.reserved(), 60);
        drop(b);
        assert_eq!(shared.reserved(), 0);
        assert_eq!(first.reserved(), 0);
        assert_eq!(second.reserved(), 0);
    }

    #[test]
    fn parallel_consumers_cannot_oversubscribe_one_query() {
        let shared: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024));
        let query = pool(shared.clone(), 64);
        let barrier = Arc::new(std::sync::Barrier::new(8));
        let admitted = Arc::new(AtomicUsize::new(0));
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let (query, barrier, admitted) = (query.clone(), barrier.clone(), admitted.clone());
                scope.spawn(move || {
                    let reservation = MemoryConsumer::new("parallel").register(&query);
                    barrier.wait();
                    if reservation.try_grow(32).is_ok() {
                        admitted.fetch_add(1, Ordering::Relaxed);
                    }
                    barrier.wait();
                    assert!(query.reserved() <= 64);
                });
            }
        });
        assert_eq!(admitted.load(Ordering::Relaxed), 2);
        assert_eq!(query.reserved(), 0);
        assert_eq!(shared.reserved(), 0);
    }

    #[tokio::test]
    async fn hash_build_refusal_releases_global_memory_and_next_query_runs() {
        use arrow_array::{Int64Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use datafusion::datasource::MemTable;
        use datafusion::execution::runtime_env::RuntimeEnvBuilder;
        use datafusion::prelude::SessionConfig;
        let shared: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(8 * 1024 * 1024));
        let runtime = Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(shared.clone())
                .build()
                .unwrap(),
        );
        let ctx = SessionContext::new_with_config_rt(
            SessionConfig::new().with_target_partitions(2),
            runtime,
        );
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from_iter_values(0..32768))],
        )
        .unwrap();
        ctx.register_table(
            "build_side",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
        let query = with_query_limit(ctx.clone(), 64 * 1024, 0);
        let frame = query
            .sql("SELECT a.id FROM build_side a JOIN build_side b ON a.id = b.id")
            .await
            .unwrap();
        let plan = frame.create_physical_plan().await.unwrap();
        let display = datafusion::physical_plan::displayable(plan.as_ref())
            .indent(false)
            .to_string();
        assert!(
            display.contains("HashJoinExec"),
            "exercise hash-build admission: {display}"
        );
        let error = frame.collect().await.unwrap_err().to_string();
        assert!(
            error.contains("query.max_query_memory"),
            "unexpected error: {error}"
        );
        drop(plan);
        drop(query);
        assert_eq!(
            shared.reserved(),
            0,
            "failed build must release its global reservation"
        );
        let light = with_query_limit(ctx, 64 * 1024, 0)
            .sql("SELECT 1")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(light[0].num_rows(), 1);
        assert_eq!(shared.reserved(), 0);
    }

    #[tokio::test]
    async fn execution_contexts_preserve_runtime_resources_and_have_fresh_quotas() {
        let ctx = SessionContext::new();
        ctx.register_table(
            "fixture",
            Arc::new(
                datafusion::datasource::MemTable::try_new(
                    Arc::new(arrow_schema::Schema::empty()),
                    vec![vec![]],
                )
                .unwrap(),
            ),
        )
        .unwrap();
        let first = with_query_limit(ctx.clone(), 64, 0);
        let second = with_query_limit(ctx.clone(), 64, 0);
        assert!(Arc::ptr_eq(
            &ctx.runtime_env().disk_manager,
            &first.runtime_env().disk_manager
        ));
        assert!(Arc::ptr_eq(
            &ctx.runtime_env().object_store_registry,
            &first.runtime_env().object_store_registry
        ));
        assert!(!Arc::ptr_eq(
            &first.runtime_env().memory_pool,
            &second.runtime_env().memory_pool
        ));
        assert!(first.table_exist("fixture").unwrap());
        let a = MemoryConsumer::new("a").register(&first.runtime_env().memory_pool);
        let b = MemoryConsumer::new("b").register(&second.runtime_env().memory_pool);
        a.try_grow(64).unwrap();
        b.try_grow(64).unwrap();
        assert!(a.try_grow(1).is_err());
        drop(a);
        drop(b);
        assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
    }
}
