//! Sort-merge retry for read queries whose hash builds outgrow query memory
//! (#784, design: `docs/internal/specs/2026-10-09-adaptive-join-fallback-design.md`).
//!
//! DataFusion's hash-join build side cannot spill, and Iceberg statistics
//! are too inexact to predict its size, so a large build ends in
//! `ResourcesExhausted` under `query.max_query_memory`. When that happens
//! before the query has produced a batch, [`FallbackStream`] drops the
//! failed plan and plans the same policy-enforced logical plan again with
//! `prefer_hash_join = false`. DataFusion's planner then picks sort-merge
//! joins, which spill, and `EnforceDistribution` / `EnforceSorting` place
//! their exchanges and sorts. Nothing is replayed from the first attempt,
//! and the new plan builds its own filters.
//!
//! The retry runs once, on the coordinator, under a fresh query quota.
//! Distribution is not attempted for it. It is "complete, not fast": small
//! broadcasts and hash-join dynamic filters are gone from the retry plan.

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Instant;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::execution::memory_pool::{MemoryLimit, MemoryPool};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{
    execute_stream, ExecutionPlan, RecordBatchStream, SendableRecordBatchStream,
};
use datafusion::prelude::SessionContext;
use futures::future::BoxFuture;
use futures::{FutureExt, Stream};
use sqe_core::config::QueryConfig;
use tracing::warn;

/// The plan the retry executed, handed to whoever reports on the query
/// (spill metrics, the streaming finalizer's profile and plan metrics).
/// Empty unless the fallback ran.
#[derive(Clone, Default)]
pub(crate) struct ReplannedPlan(Arc<Mutex<Option<Arc<dyn ExecutionPlan>>>>);

impl ReplannedPlan {
    fn set(&self, plan: Arc<dyn ExecutionPlan>) {
        if let Ok(mut slot) = self.0.lock() {
            *slot = Some(plan);
        }
    }

    /// The retry's plan, once. Later calls return `None`.
    pub(crate) fn take(&self) -> Option<Arc<dyn ExecutionPlan>> {
        self.0.lock().ok().and_then(|mut slot| slot.take())
    }
}

/// Everything the retry needs, owned, so the stream can replan after the
/// request that opened it has returned.
pub(crate) struct JoinFallback {
    /// The first attempt's context. The retry copies its state, with a new
    /// config and a fresh query pool.
    ctx: SessionContext,
    shared_pool: Arc<dyn MemoryPool>,
    limit: usize,
    headroom_percent: u8,
    logical: LogicalPlan,
    query: QueryConfig,
    metrics: Option<Arc<sqe_metrics::MetricsRegistry>>,
    query_id: uuid::Uuid,
    started: Instant,
}

impl JoinFallback {
    /// `None` when the fallback is off (`query.join_fallback`) or cannot
    /// help: without spill files a sort-merge plan holds as much as the hash
    /// plan did.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        ctx: &SessionContext,
        shared_pool: Arc<dyn MemoryPool>,
        limit: usize,
        headroom_percent: u8,
        logical: LogicalPlan,
        query: &QueryConfig,
        metrics: Option<Arc<sqe_metrics::MetricsRegistry>>,
        query_id: uuid::Uuid,
    ) -> Option<Self> {
        if !query.join_fallback || !ctx.runtime_env().disk_manager.tmp_files_enabled() {
            return None;
        }
        Some(Self {
            ctx: ctx.clone(),
            shared_pool,
            limit,
            headroom_percent,
            logical,
            query: query.clone(),
            metrics,
            query_id,
            started: Instant::now(),
        })
    }

    /// Whether `error`, raised by `plan` before its first batch, is one the
    /// retry can fix. Any memory refusal counts, whichever consumer asked
    /// last: at a 1 GB cap, TPC-H q09 was refused at scan decode while its
    /// hash builds held most of the pool.
    fn applies(plan: &Arc<dyn ExecutionPlan>, error: &DataFusionError) -> bool {
        matches!(error.find_root(), DataFusionError::ResourcesExhausted(_))
            && has_switchable_hash_join(plan)
    }

    /// Plan the logical plan again with sort-merge joins, under a fresh
    /// quota, and open its stream.
    async fn replan(
        self,
        replanned: ReplannedPlan,
    ) -> Result<(Arc<dyn ExecutionPlan>, SendableRecordBatchStream), DataFusionError> {
        let mut ctx = sort_merge_context(
            &self.ctx,
            Arc::clone(&self.shared_pool),
            self.limit,
            self.headroom_percent,
            None,
        );
        let mut plan = self.plan_on(&ctx).await?;
        let current = ctx.copied_config().options().execution.target_partitions;
        let partitions = retry_partitions(self.limit, count_sorters(&plan), current);
        if partitions < current {
            ctx = sort_merge_context(
                &self.ctx,
                Arc::clone(&self.shared_pool),
                self.limit,
                self.headroom_percent,
                Some(partitions),
            );
            plan = self.plan_on(&ctx).await?;
        }
        replanned.set(Arc::clone(&plan));
        let stream = execute_stream(Arc::clone(&plan), sort_task_context(&ctx, &plan))?;
        Ok((plan, stream))
    }

    /// The physical plan of the logical plan on `ctx`, after the
    /// coordinator's post-planning rules.
    async fn plan_on(
        &self,
        ctx: &SessionContext,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // `carried`: the planning thread does not see the task's
        // transaction scope (#821).
        sqe_core::runtime::on_planning_stack(sqe_catalog::txn::carried(async {
            let physical = ctx.state().create_physical_plan(&self.logical).await?;
            crate::post_planning::apply_post_planning_rules(
                ctx,
                &self.query,
                self.metrics.as_ref(),
                physical,
            )
            .map_err(|e| DataFusionError::External(Box::new(e)))
        }))
        .await
    }
}

/// Smallest share of the quota each sorter in the retry should get: four
/// times the merge floor, so a sorter can buffer batches next to its merge
/// reservation.
const MIN_SORTER_SHARE: usize = 4 * MIN_MERGE_RESERVATION;

/// Partitions the retry runs at. A sort-merge plan runs one sorter per join
/// input per partition, and each one has to buffer at least a batch before
/// it can spill. Under a small quota and many partitions the shares get too
/// small for that, and the retry fails with "Not enough memory to continue
/// external sort". The retry then drops to as many partitions as give every
/// sorter [`MIN_SORTER_SHARE`], but never below 2 (DataFusion plans a
/// sort-merge join only above 1) and never above `current`. A 1 GB quota
/// with ten sorts per partition keeps 25 partitions, so realistic caps run
/// at full width.
fn retry_partitions(limit: usize, sorters: usize, current: usize) -> usize {
    if limit == 0 || sorters == 0 || current <= 2 {
        return current;
    }
    let per_partition = sorters.div_ceil(current);
    (limit / (per_partition * MIN_SORTER_SHARE)).clamp(2, current)
}

/// Smallest merge reservation a sort gets, as on the worker.
const MIN_MERGE_RESERVATION: usize = 1024 * 1024;

/// Bytes each sort reserves for its merge. Every sort partition reserves
/// `sort_spill_reservation_bytes` (10 MB by default) before it buffers
/// anything, and that reservation cannot spill. A sort-merge plan sorts both
/// inputs of every join in every partition, so at the default a 1 GB quota
/// is gone at about 100 sorters, before a row is joined; a plain `ORDER BY`
/// at 16 partitions takes 160 MB the same way (#788). The sorters share a
/// quarter of the quota instead, with [`MIN_MERGE_RESERVATION`] as the floor
/// and the configured value as the ceiling. A smaller reservation means more
/// merge passes.
fn merge_reservation(limit: usize, sorters: usize, configured: usize) -> usize {
    if limit == 0 || sorters == 0 {
        return configured;
    }
    (limit / 4 / sorters)
        .max(MIN_MERGE_RESERVATION)
        .min(configured)
}

/// The task context `plan` executes under on `ctx`: `ctx`'s, with each sort's
/// merge reservation sized by [`merge_reservation`] from the quota of `ctx`'s
/// memory pool. Both coordinator execution paths, `EXPLAIN ANALYZE` and the
/// sort-merge retry execute through it.
pub(crate) fn sort_task_context(
    ctx: &SessionContext,
    plan: &Arc<dyn ExecutionPlan>,
) -> Arc<TaskContext> {
    let sorters = count_sorters(plan);
    if sorters == 0 {
        return ctx.task_ctx();
    }
    let limit = match ctx.runtime_env().memory_pool.memory_limit() {
        MemoryLimit::Finite(limit) => limit,
        _ => 0,
    };
    let mut config = ctx.copied_config();
    let execution = &mut config.options_mut().execution;
    let reservation = merge_reservation(limit, sorters, execution.sort_spill_reservation_bytes);
    if reservation == execution.sort_spill_reservation_bytes {
        return ctx.task_ctx();
    }
    execution.sort_spill_reservation_bytes = reservation;
    let state = SessionStateBuilder::from(ctx.state())
        .with_config(config)
        .build();
    Arc::new(TaskContext::from(&state))
}

/// External sorters `plan` will run: one per output partition of each
/// `SortExec`.
fn count_sorters(plan: &Arc<dyn ExecutionPlan>) -> usize {
    let own = if plan.downcast_ref::<SortExec>().is_some() {
        plan.properties().output_partitioning().partition_count()
    } else {
        0
    };
    own + plan
        .children()
        .into_iter()
        .map(count_sorters)
        .sum::<usize>()
}

/// A copy of `ctx` that plans equi-joins as sort-merge joins, with its own
/// query quota over `shared_pool`, at `target_partitions` when given. The
/// quota is a fair one: under a greedy quota one sorter, or one side's spill
/// merge, could take every byte while another sorter still held nothing, and
/// a sorter refused with nothing to spill fails the query.
fn sort_merge_context(
    ctx: &SessionContext,
    shared_pool: Arc<dyn MemoryPool>,
    limit: usize,
    headroom_percent: u8,
    target_partitions: Option<usize>,
) -> SessionContext {
    let state = ctx.state();
    let mut config = state.config().clone();
    config.options_mut().optimizer.prefer_hash_join = false;
    if let Some(partitions) = target_partitions {
        config.options_mut().execution.target_partitions = partitions;
    }
    let state = SessionStateBuilder::from(state).with_config(config).build();
    crate::query_memory::with_query_limit_over(
        SessionContext::new_with_state(state),
        shared_pool,
        limit,
        headroom_percent,
        true,
    )
}

/// True when replanning without hash joins would change `plan`: it has a
/// hash join on equi-keys that is not null-aware. DataFusion keeps
/// null-aware anti joins (`NOT IN` over a nullable subquery) as hash joins,
/// because the sort-merge join does not implement their semantics.
pub(crate) fn has_switchable_hash_join(plan: &Arc<dyn ExecutionPlan>) -> bool {
    if let Some(join) = plan.downcast_ref::<HashJoinExec>() {
        if !join.on().is_empty() && !join.null_aware {
            return true;
        }
    }
    plan.children().into_iter().any(has_switchable_hash_join)
}

/// The retry being planned: its physical plan and output stream.
type Replanning = BoxFuture<
    'static,
    Result<(Arc<dyn ExecutionPlan>, SendableRecordBatchStream), DataFusionError>,
>;

enum State {
    /// The first attempt, before it has produced a batch.
    First {
        stream: SendableRecordBatchStream,
        plan: Arc<dyn ExecutionPlan>,
        fallback: Box<JoinFallback>,
    },
    Replanning(Replanning),
    /// Either attempt after it started producing batches. `retry` is set
    /// while an error from the retry has not been counted yet.
    Running {
        stream: SendableRecordBatchStream,
        retry: bool,
    },
    Done,
}

/// The query's output stream, with the sort-merge retry armed until the
/// first batch.
pub(crate) struct FallbackStream {
    schema: SchemaRef,
    state: State,
    replanned: ReplannedPlan,
    metrics: Option<Arc<sqe_metrics::MetricsRegistry>>,
}

impl FallbackStream {
    /// Wrap `stream` (the execution of `plan`). Without a `fallback` the
    /// stream passes through unchanged.
    pub(crate) fn wrap(
        stream: SendableRecordBatchStream,
        plan: Arc<dyn ExecutionPlan>,
        fallback: Option<JoinFallback>,
        replanned: ReplannedPlan,
    ) -> SendableRecordBatchStream {
        let Some(fallback) = fallback.filter(|_| has_switchable_hash_join(&plan)) else {
            return stream;
        };
        Box::pin(Self {
            schema: stream.schema(),
            metrics: fallback.metrics.clone(),
            state: State::First {
                stream,
                plan,
                fallback: Box::new(fallback),
            },
            replanned,
        })
    }

    fn count(&self, outcome: &str) {
        if let Some(metrics) = &self.metrics {
            metrics
                .join_fallback_total
                .with_label_values(&[outcome])
                .inc();
        }
    }
}

impl Stream for FallbackStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                State::First { stream, plan, .. } => {
                    match stream.as_mut().poll_next(cx) {
                        Poll::Ready(Some(Err(error))) if JoinFallback::applies(plan, &error) => {
                            // Drop the failed attempt first so its
                            // reservations are released before the retry
                            // asks for memory.
                            let State::First { fallback, .. } =
                                std::mem::replace(&mut self.state, State::Done)
                            else {
                                unreachable!("matched First above");
                            };
                            let wasted = fallback.started.elapsed();
                            warn!(
                                query_id = %fallback.query_id,
                                limit_bytes = fallback.limit,
                                wasted_ms = wasted.as_millis() as u64,
                                error = %error,
                                "query ran out of memory before its first batch; \
                                 retrying with sort-merge joins (#784)"
                            );
                            if let Some(metrics) = &self.metrics {
                                metrics
                                    .join_fallback_wasted_seconds
                                    .inc_by(wasted.as_secs_f64());
                            }
                            self.count("replanned");
                            let replanned = self.replanned.clone();
                            self.state = State::Replanning(fallback.replan(replanned).boxed());
                        }
                        Poll::Ready(Some(item)) => {
                            let State::First { stream, .. } =
                                std::mem::replace(&mut self.state, State::Done)
                            else {
                                unreachable!("matched First above");
                            };
                            self.state = State::Running {
                                stream,
                                retry: false,
                            };
                            return Poll::Ready(Some(item));
                        }
                        other => return other,
                    }
                }
                State::Replanning(future) => match future.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok((plan, stream))) => {
                        if plan.schema() != self.schema {
                            self.state = State::Done;
                            self.count("failed");
                            return Poll::Ready(Some(Err(DataFusionError::Internal(
                                "sort-merge fallback produced a different output schema".into(),
                            ))));
                        }
                        self.state = State::Running {
                            stream,
                            retry: true,
                        };
                    }
                    Poll::Ready(Err(error)) => {
                        self.state = State::Done;
                        self.count("failed");
                        return Poll::Ready(Some(Err(error)));
                    }
                },
                State::Running { stream, retry } => {
                    let polled = stream.as_mut().poll_next(cx);
                    let retry_failed = *retry && matches!(polled, Poll::Ready(Some(Err(_))));
                    if retry_failed {
                        *retry = false;
                        self.count("failed");
                    }
                    return polled;
                }
                State::Done => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for FallbackStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::SessionConfig;
    use futures::TryStreamExt;

    const ROWS: i64 = 1_500_000;
    const CAP: usize = 16 * 1024 * 1024;

    /// A session over `shared` with two `ROWS`-row tables, `l` and `r`,
    /// joining one to one on `id`.
    fn session(shared: Arc<dyn MemoryPool>) -> SessionContext {
        let runtime = Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(shared)
                .build()
                .unwrap(),
        );
        let ctx = SessionContext::new_with_config_rt(
            // Eight partitions: sixteen sorters would get 1 MB of the 16 MB
            // quota each, so the retry has to narrow to two.
            SessionConfig::new().with_target_partitions(8),
            runtime,
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        for name in ["l", "r"] {
            let batches = (0..ROWS)
                .step_by(8192)
                .map(|start| {
                    let ids = start..(start + 8192).min(ROWS);
                    RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![
                            Arc::new(Int64Array::from_iter_values(ids.clone())),
                            Arc::new(Int64Array::from_iter_values(ids.map(|i| i * 2))),
                        ],
                    )
                    .unwrap()
                })
                .collect();
            ctx.register_table(
                name,
                Arc::new(MemTable::try_new(Arc::clone(&schema), vec![batches]).unwrap()),
            )
            .unwrap();
        }
        ctx
    }

    const JOIN: &str = "SELECT l.id, r.v FROM l JOIN r ON l.id = r.id";

    /// Run `sql` the way the coordinator does: under a `CAP` query quota,
    /// wrapped in the fallback when `fallback` is set.
    async fn run(
        sql: &str,
        fallback: bool,
    ) -> (
        Result<Vec<RecordBatch>, DataFusionError>,
        ReplannedPlan,
        Arc<dyn MemoryPool>,
    ) {
        let shared: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64 * CAP));
        let base = session(Arc::clone(&shared));
        let ctx = crate::query_memory::with_query_limit(base, CAP, 20);
        let logical = ctx.state().create_logical_plan(sql).await.unwrap();
        let plan = ctx.state().create_physical_plan(&logical).await.unwrap();
        let fallback = fallback
            .then(|| {
                JoinFallback::new(
                    &ctx,
                    Arc::clone(&shared),
                    CAP,
                    20,
                    logical,
                    &QueryConfig::default(),
                    None,
                    uuid::Uuid::nil(),
                )
            })
            .flatten();
        let replanned = ReplannedPlan::default();
        let stream = execute_stream(Arc::clone(&plan), sort_task_context(&ctx, &plan)).unwrap();
        let stream = FallbackStream::wrap(stream, plan, fallback, replanned.clone());
        (stream.try_collect().await, replanned, shared)
    }

    fn sum_v(batches: &[RecordBatch]) -> (usize, i64) {
        let rows = batches.iter().map(RecordBatch::num_rows).sum();
        let sum = batches
            .iter()
            .flat_map(|b| {
                b.column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .sum();
        (rows, sum)
    }

    #[tokio::test]
    async fn a_hash_build_over_the_quota_fails_without_the_fallback() {
        let (result, replanned, _) = run(JOIN, false).await;
        let error = result.unwrap_err();
        assert!(
            matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)),
            "the fixture must overflow the hash build: {error}"
        );
        assert!(replanned.take().is_none());
    }

    #[tokio::test]
    async fn the_fallback_completes_the_join_with_sort_merge() {
        let (result, replanned, shared) = run(JOIN, true).await;
        let batches = result.expect("the sort-merge retry completes under the same quota");
        assert_eq!(
            sum_v(&batches),
            (ROWS as usize, (0..ROWS).map(|i| i * 2).sum())
        );
        let plan = replanned.take().expect("the retry ran");
        let shape = displayable(plan.as_ref()).indent(false).to_string();
        assert!(shape.contains("SortMergeJoin"), "{shape}");
        assert!(!shape.contains("HashJoinExec"), "{shape}");
        // DataFusion's own distribution rules placed the join: both inputs
        // hash-partitioned on the key, nothing funnelled to one partition.
        assert!(
            shape.contains("RepartitionExec: partitioning=Hash"),
            "{shape}"
        );
        assert!(!shape.contains("CoalescePartitionsExec"), "{shape}");
        assert!(!shape.contains("SortPreservingMergeExec"), "{shape}");
        // 16 MB over two sorters per partition: 4 MB each at two partitions.
        assert_eq!(
            plan.properties().output_partitioning().partition_count(),
            2,
            "{shape}"
        );
        drop(plan);
        assert_eq!(shared.reserved(), 0, "both attempts released their memory");
    }

    #[test]
    fn merges_share_a_quarter_of_the_quota() {
        const MB: usize = 1024 * 1024;
        let configured = 10 * MB;
        // 1 GB over 128 sorters: 2 MB each instead of 10.
        assert_eq!(merge_reservation(1024 * MB, 128, configured), 2 * MB);
        // Few sorters never get more than configured.
        assert_eq!(merge_reservation(1024 * MB, 4, configured), configured);
        // Many sorters stop at the floor.
        assert_eq!(
            merge_reservation(64 * MB, 128, configured),
            MIN_MERGE_RESERVATION
        );
        // No quota or no sorts: unchanged.
        assert_eq!(merge_reservation(0, 128, configured), configured);
        assert_eq!(merge_reservation(1024 * MB, 0, configured), configured);
    }

    /// A sort wider than its quota completes by spilling (#788). At 8
    /// partitions the default merge reservations alone (8 x 10 MB) are more
    /// than the 64 MB quota, and under a greedy quota the first sorters to
    /// fill starve the rest, which fail holding nothing to spill.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_partitioned_sort_spills_within_a_tight_quota() {
        const MB: usize = 1024 * 1024;
        const SORT_ROWS: usize = 1_000_000;
        const PARTITIONS: usize = 8;
        let shared: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * MB));
        let runtime = Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&shared))
                .build()
                .unwrap(),
        );
        let base = SessionContext::new_with_config_rt(
            SessionConfig::new()
                .with_target_partitions(PARTITIONS)
                .with_batch_size(512),
            runtime,
        );
        // About 100 MB of rows: an id and a ~90-byte string to sort on.
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("s", DataType::Utf8, false),
        ]));
        let per_partition = SORT_ROWS / PARTITIONS;
        let partitions = (0..PARTITIONS)
            .map(|p| {
                (0..per_partition)
                    .step_by(512)
                    .map(|start| {
                        let ids: Vec<i64> = (start..(start + 512).min(per_partition))
                            .map(|i| (p * per_partition + i) as i64)
                            .collect();
                        let s = ids.iter().map(|id| {
                            let h = (*id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
                            format!(
                                "{h:016x} quickly regular deposits sleep furiously among the {h:x}"
                            )
                        });
                        RecordBatch::try_new(
                            Arc::clone(&schema),
                            vec![
                                Arc::new(Int64Array::from(ids.clone())),
                                Arc::new(datafusion::arrow::array::StringArray::from_iter_values(
                                    s,
                                )),
                            ],
                        )
                        .unwrap()
                    })
                    .collect()
            })
            .collect();
        base.register_table(
            "t",
            Arc::new(MemTable::try_new(Arc::clone(&schema), partitions).unwrap()),
        )
        .unwrap();
        let ctx = crate::query_memory::with_query_limit(base, 64 * MB, 20);
        for sql in [
            "SELECT id, s FROM t ORDER BY s",
            "SELECT max(rn), count(*) FROM (SELECT id, row_number() OVER (ORDER BY s) rn FROM t)",
        ] {
            let plan = ctx
                .sql(sql)
                .await
                .unwrap()
                .create_physical_plan()
                .await
                .unwrap();
            let batches = datafusion::physical_plan::collect(
                Arc::clone(&plan),
                sort_task_context(&ctx, &plan),
            )
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
            let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
            assert!(rows == SORT_ROWS || rows == 1, "{sql}: {rows} rows");
            drop(plan);
        }
        assert_eq!(shared.reserved(), 0);
    }

    #[test]
    fn the_retry_narrows_until_each_sorter_gets_its_share() {
        const MB: usize = 1024 * 1024;
        // 1 GB, ten sorts per partition at 16 partitions: full width.
        assert_eq!(retry_partitions(1024 * MB, 160, 16), 16);
        // 16 MB, two sorts per partition at 8 partitions: 4 MB each at 2.
        assert_eq!(retry_partitions(16 * MB, 16, 8), 2);
        // 64 MB, two sorts per partition at 16 partitions: 8.
        assert_eq!(retry_partitions(64 * MB, 32, 16), 8);
        // Never below 2, where DataFusion stops planning sort-merge joins.
        assert_eq!(retry_partitions(MB, 16, 8), 2);
        // No quota, no sorts or already narrow: unchanged.
        assert_eq!(retry_partitions(0, 16, 8), 8);
        assert_eq!(retry_partitions(16 * MB, 0, 8), 8);
        assert_eq!(retry_partitions(MB, 4, 2), 2);
    }

    #[tokio::test]
    async fn a_query_that_fits_never_replans() {
        let (result, replanned, _) = run(
            "SELECT l.id, r.v FROM l JOIN r ON l.id = r.id WHERE r.id < 100",
            true,
        )
        .await;
        assert_eq!(
            result
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            100
        );
        assert!(replanned.take().is_none());
    }

    #[tokio::test]
    async fn the_fallback_is_off_when_disabled_or_without_spill_files() {
        let ctx = session(Arc::new(GreedyMemoryPool::new(CAP)));
        let logical = ctx.state().create_logical_plan(JOIN).await.unwrap();
        let off = QueryConfig {
            join_fallback: false,
            ..QueryConfig::default()
        };
        let pool = || ctx.runtime_env().memory_pool.clone();
        let fallback = |query: &QueryConfig, ctx: &SessionContext| {
            JoinFallback::new(
                ctx,
                pool(),
                CAP,
                0,
                logical.clone(),
                query,
                None,
                uuid::Uuid::nil(),
            )
        };
        assert!(fallback(&QueryConfig::default(), &ctx).is_some());
        assert!(fallback(&off, &ctx).is_none());
        let no_spill = SessionContext::new_with_config_rt(
            SessionConfig::new(),
            Arc::new(
                RuntimeEnvBuilder::new()
                    .with_disk_manager_builder(
                        datafusion::execution::disk_manager::DiskManagerBuilder::default()
                            .with_mode(
                                datafusion::execution::disk_manager::DiskManagerMode::Disabled,
                            ),
                    )
                    .build()
                    .unwrap(),
            ),
        );
        assert!(fallback(&QueryConfig::default(), &no_spill).is_none());
    }

    #[tokio::test]
    async fn only_equi_joins_that_are_not_null_aware_can_switch() {
        let ctx = session(Arc::new(GreedyMemoryPool::new(64 * CAP)));
        let switchable = |sql: &'static str| {
            let ctx = ctx.clone();
            async move {
                let plan = ctx
                    .sql(sql)
                    .await
                    .unwrap()
                    .create_physical_plan()
                    .await
                    .unwrap();
                has_switchable_hash_join(&plan)
            }
        };
        assert!(switchable(JOIN).await);
        assert!(!switchable("SELECT id FROM l").await);
        assert!(!switchable("SELECT l.id FROM l JOIN r ON l.id < r.id").await);
        // `NOT IN` over a nullable subquery plans a null-aware anti join,
        // which DataFusion keeps hashed.
        assert!(
            !switchable(
                "SELECT id FROM l WHERE id NOT IN \
                 (SELECT CASE WHEN v > 0 THEN id END FROM r)"
            )
            .await
        );
    }
}
