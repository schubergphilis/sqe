//! Partially-drained query results parked between `FetchRequest`s.
//!
//! `PrepareResponse` used to inline every batch of a result, so a
//! `SELECT * FROM lineitem` held the whole result on the coordinator heap, a
//! second encoded copy, and then the HTTP body (finding 606). The server now
//! sends one byte-bounded page per response and parks the rest of the stream
//! here, keyed by the `result_uuid` it hands back to the client.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use futures::stream::BoxStream;
use moka::sync::Cache;
use tokio::sync::Mutex;

use crate::query_executor::QueryError;

/// How long a half-drained result survives without a `FetchRequest`. A client
/// that walks away mid-result must not pin its DataFusion stream (and the
/// memory reservation and concurrency permit behind it) indefinitely.
pub const PENDING_RESULT_IDLE: Duration = Duration::from_secs(300);

/// One parked result stream.
pub struct PendingResult {
    /// The session that started this query. A `FetchRequest` from any other
    /// connection is refused even if it guessed the uuid.
    pub connection_id: String,
    /// Result column names, in schema order. Carried so log lines and any
    /// future re-send of the header do not need the original plan.
    pub result_names: Vec<String>,
    /// The remainder of the result. `None` once the stream reported EOF: the
    /// entry survives one more `FetchRequest` so the client sees the empty
    /// response that terminates its loop.
    pub stream: Mutex<Option<BoxStream<'static, Result<RecordBatch, QueryError>>>>,
    /// Running total of Arrow bytes handed to this client across the initial
    /// `PrepareResponse` and every `FetchResponse`, checked against
    /// `max_result_bytes`.
    pub bytes_sent: AtomicU64,
    /// Pages already sent for this result; becomes `FetchResponse.batch_index`.
    pub pages_sent: AtomicU64,
}

impl PendingResult {
    pub fn new(
        connection_id: String,
        result_names: Vec<String>,
        stream: BoxStream<'static, Result<RecordBatch, QueryError>>,
        bytes_sent: u64,
    ) -> Self {
        Self {
            connection_id,
            result_names,
            stream: Mutex::new(Some(stream)),
            bytes_sent: AtomicU64::new(bytes_sent),
            pages_sent: AtomicU64::new(0),
        }
    }

    /// Add `bytes` to the running total and return the new total.
    pub fn charge(&self, bytes: u64) -> u64 {
        self.bytes_sent.fetch_add(bytes, Ordering::Relaxed) + bytes
    }

    /// Claim the next page number (1-based; page 0 is the `PrepareResponse`).
    pub fn next_page(&self) -> u64 {
        self.pages_sent.fetch_add(1, Ordering::Relaxed) + 1
    }
}

/// Uuid-keyed store of parked results, shared by every request handler.
#[derive(Clone)]
pub struct PendingResults {
    inner: Arc<Cache<i128, Arc<PendingResult>>>,
}

impl PendingResults {
    pub fn new(idle_timeout: Duration) -> Self {
        Self {
            inner: Arc::new(
                Cache::builder()
                    .time_to_idle(idle_timeout)
                    .max_capacity(10_000)
                    .build(),
            ),
        }
    }

    pub fn insert(&self, uuid: i128, pending: Arc<PendingResult>) {
        self.inner.insert(uuid, pending);
    }

    pub fn get(&self, uuid: i128) -> Option<Arc<PendingResult>> {
        self.inner.get(&uuid)
    }

    pub fn remove(&self, uuid: i128) {
        self.inner.invalidate(&uuid);
    }

    /// Drop every parked result belonging to `connection_id`. Called on
    /// `DisconnectMessage` and whenever a session is rejected/invalidated, so a
    /// dead session cannot leave live DataFusion streams behind.
    pub fn remove_for_connection(&self, connection_id: &str) {
        let doomed: Vec<i128> = self
            .inner
            .iter()
            .filter(|(_, v)| v.connection_id == connection_id)
            .map(|(k, _)| *k)
            .collect();
        for uuid in doomed {
            self.inner.invalidate(&uuid);
        }
    }

    pub fn len(&self) -> u64 {
        self.inner.entry_count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Force moka to flush deferred operations so `len`/`get` reflect recent
    /// `insert`/`invalidate` calls. Useful in tests; harmless in production.
    pub fn run_pending_tasks(&self) {
        self.inner.run_pending_tasks();
    }
}

impl Default for PendingResults {
    fn default() -> Self {
        Self::new(PENDING_RESULT_IDLE)
    }
}
