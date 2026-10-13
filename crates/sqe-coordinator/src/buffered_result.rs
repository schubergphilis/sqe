//! Byte admission for retained SELECT results. Reservations follow Arrow
//! buffers through clones, slices, cache entries and ArrayData conversions.

use std::panic::AssertUnwindSafe;
use std::ptr::NonNull;
use std::sync::Arc;

use arrow::array::{make_array, ArrayData};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow_array::RecordBatch;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use sqe_core::{SqeError, SqeErrorCode};

pub(crate) struct BufferedResultBudget {
    pools: Vec<Arc<dyn MemoryPool>>,
    max_bytes: usize,
    limit_name: &'static str,
    retained_bytes: usize,
}

impl BufferedResultBudget {
    pub(crate) fn new(
        shared: Arc<dyn MemoryPool>,
        query: Arc<dyn MemoryPool>,
        max_bytes: usize,
    ) -> Self {
        let pools = if Arc::ptr_eq(&shared, &query) {
            vec![shared]
        } else {
            vec![shared, query]
        };
        Self {
            pools,
            max_bytes,
            limit_name: "query.max_query_memory",
            retained_bytes: 0,
        }
    }

    /// Name the setting `max_bytes` came from in the refusal, when it is not
    /// `query.max_query_memory`.
    pub(crate) fn with_limit_name(mut self, limit_name: &'static str) -> Self {
        self.limit_name = limit_name;
        self
    }

    pub(crate) fn retain(&mut self, batch: RecordBatch) -> Result<RecordBatch, SqeError> {
        let bytes = batch.get_array_memory_size();
        let total = self.retained_bytes.checked_add(bytes).ok_or_else(|| {
            SqeError::with_code(
                SqeErrorCode::ResourceExhausted,
                "Buffered result byte count overflow",
            )
        })?;
        if self.max_bytes > 0 && total > self.max_bytes {
            return Err(SqeError::with_code(
                SqeErrorCode::ResourceExhausted,
                format!("Buffered result needs {total} bytes, exceeding {}={} bytes; use LIMIT or a streaming client", self.limit_name, self.max_bytes),
            ));
        }
        let mut reservations = Vec::with_capacity(self.pools.len());
        for pool in &self.pools {
            let reservation = MemoryConsumer::new("buffered-query-result").register(pool);
            reservation.try_grow(bytes).map_err(|e| {
                SqeError::with_code(
                    SqeErrorCode::ResourceExhausted,
                    format!("Cannot retain {bytes} result bytes in the memory pool: {e}"),
                )
            })?;
            reservations.push(reservation);
        }
        let reservations = Arc::new(reservations);
        let columns = batch
            .columns()
            .iter()
            .map(|array| retain_data(array.to_data(), &reservations).map(make_array))
            .collect::<Result<Vec<_>, _>>()?;
        let retained = RecordBatch::try_new_with_options(
            batch.schema(),
            columns,
            &arrow_array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )
        .map_err(|e| SqeError::Execution(format!("Cannot retain result batch: {e}")))?;
        self.retained_bytes = total;
        Ok(retained)
    }
}

/// The original Buffer keeps its memory alive; every view of that memory also
/// keeps the budget alive. AssertUnwindSafe supplies Allocation's unwind marker:
/// the owner exposes no mutation, and both fields use RAII for cleanup.
struct ReservedAllocation {
    _buffer: Buffer,
    _reservations: AssertUnwindSafe<Arc<Vec<MemoryReservation>>>,
}

fn retain_buffer(buffer: Buffer, reservations: &Arc<Vec<MemoryReservation>>) -> Buffer {
    let ptr = NonNull::new(buffer.as_ptr() as *mut u8).expect("Arrow buffer pointer is non-null");
    let len = buffer.len();
    let owner = Arc::new(ReservedAllocation {
        _buffer: buffer,
        _reservations: AssertUnwindSafe(Arc::clone(reservations)),
    });
    // SAFETY: ptr and len come from the immutable original Buffer, which the
    // allocation owner retains until all references to this view are dropped.
    unsafe { Buffer::from_custom_allocation(ptr, len, owner) }
}

fn retain_data(
    data: ArrayData,
    reservations: &Arc<Vec<MemoryReservation>>,
) -> Result<ArrayData, SqeError> {
    let buffers = data
        .buffers()
        .iter()
        .cloned()
        .map(|buffer| retain_buffer(buffer, reservations))
        .collect();
    let nulls = data.nulls().map(|nulls| {
        let buffer = BooleanBuffer::new(
            retain_buffer(nulls.buffer().clone(), reservations),
            nulls.offset(),
            nulls.len(),
        );
        // SAFETY: only the buffer owner changed; the bits, length, offset
        // and known null count are exactly those of the original NullBuffer.
        unsafe { NullBuffer::new_unchecked(buffer, nulls.null_count()) }
    });
    let children = data
        .child_data()
        .iter()
        .cloned()
        .map(|child| retain_data(child, reservations))
        .collect::<Result<Vec<_>, _>>()?;
    let builder = data
        .into_builder()
        .buffers(buffers)
        .nulls(nulls)
        .child_data(children);
    // SAFETY: all layout metadata and buffer contents came from valid
    // ArrayData. Replacing owners preserves every pointer, length and child
    // layout. Revalidating contents here would rescan every UTF-8 result.
    Ok(unsafe { builder.build_unchecked() })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, StringArray};
    use datafusion::execution::memory_pool::GreedyMemoryPool;

    fn wide_batch() -> RecordBatch {
        RecordBatch::try_from_iter([(
            "value",
            Arc::new(StringArray::from(vec![
                Some("x".repeat(4096)),
                None,
                Some("y".repeat(4096)),
            ])) as arrow_array::ArrayRef,
        )])
        .unwrap()
    }

    #[test]
    fn rejects_wide_rows_below_the_row_limit() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
        let mut budget = BufferedResultBudget::new(pool.clone(), pool.clone(), 4096);
        let error = budget.retain(wide_batch()).unwrap_err();
        assert_eq!(error.error_code(), SqeErrorCode::ResourceExhausted);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn reservations_follow_slices_and_array_data() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
        let mut budget = BufferedResultBudget::new(pool.clone(), pool.clone(), 1 << 20);
        let original = wide_batch();
        let bytes = original.get_array_memory_size();
        let retained = budget.retain(original.clone()).unwrap();
        assert_eq!(original, retained);
        assert_eq!(pool.reserved(), bytes);
        let slice = retained.slice(1, 2);
        let data = slice.column(0).to_data();
        drop(retained);
        drop(slice);
        drop(budget);
        assert_eq!(pool.reserved(), bytes);
        let array = make_array(data);
        assert!(array.is_null(0));
        assert_eq!(array.len(), 2);
        drop(array);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn query_pool_failure_rolls_back_shared_reservation() {
        let shared: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
        let query: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(4096));
        let mut budget = BufferedResultBudget::new(shared.clone(), query.clone(), 0);
        assert!(budget.retain(wide_batch()).is_err());
        assert_eq!(shared.reserved(), 0);
        assert_eq!(query.reserved(), 0);
    }

    #[test]
    fn concurrent_results_share_the_budget_and_release_after_failure() {
        let batch = wide_batch();
        let bytes = batch.get_array_memory_size();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes * 2 - 1));
        let mut first = BufferedResultBudget::new(pool.clone(), pool.clone(), 0);
        let retained = first.retain(batch.clone()).unwrap();
        let mut second = BufferedResultBudget::new(pool.clone(), pool.clone(), 0);
        assert!(second.retain(batch).is_err());
        assert_eq!(pool.reserved(), bytes);
        drop(retained);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn nested_and_view_arrays_keep_values_and_owners_without_copying() {
        use arrow_array::{
            builder::{ListBuilder, StringBuilder},
            StringViewArray,
        };
        let mut list = ListBuilder::new(StringBuilder::new());
        list.values().append_value("nested");
        list.append(true);
        list.append(false);
        let original = RecordBatch::try_from_iter([
            ("list", Arc::new(list.finish()) as arrow_array::ArrayRef),
            (
                "view",
                Arc::new(StringViewArray::from(vec![Some("x".repeat(4096)), None]))
                    as arrow_array::ArrayRef,
            ),
        ])
        .unwrap();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
        let mut budget = BufferedResultBudget::new(pool.clone(), pool.clone(), 0);
        let retained = budget.retain(original.clone()).unwrap();
        assert_eq!(retained, original);
        let before = original.column(1).to_data();
        let after = retained.column(1).to_data();
        for (a, b) in before.buffers().iter().zip(after.buffers()) {
            assert_eq!(a.as_ptr(), b.as_ptr());
        }
        drop(after);
        let slice = retained.slice(1, 1);
        let data = slice.column(0).to_data();
        drop(retained);
        drop(slice);
        assert!(pool.reserved() > 0);
        drop(data);
        assert_eq!(pool.reserved(), 0);
    }
}
