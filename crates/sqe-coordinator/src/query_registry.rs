//! Active query registry with per-query cancellation tokens.
//!
//! Each in-flight query is assigned a [`CancellationToken`] when it is
//! registered.  The coordinator keeps a reference so that:
//!
//! * A Flight SQL `CancelQuery` action can fire the token, causing the
//!   executing future to observe cancellation and abort gracefully.
//! * Normal query completion removes the entry from the registry.
//!
//! ## Follow-up work
//!
//! * **TaskContext integration (8.3):** Pass the `CancellationToken` into
//!   DataFusion's `TaskContext` so that individual operators can check for
//!   cancellation between record batches.
//! * **Worker propagation (8.3):** For distributed execution, propagate the
//!   cancellation signal to workers via a custom Flight metadata header
//!   (`x-sqe-cancel: <query_id>`).
//! * **Integration test (8.4):** End-to-end test that cancels an in-flight
//!   query and verifies workers stop and resources are freed.

use dashmap::DashMap;
use tokio_util::sync::CancellationToken;

/// Thread-safe registry of in-flight queries and their cancellation tokens.
///
/// Used by the coordinator to track active queries and support cancellation
/// from Flight SQL clients.
pub struct QueryRegistry {
    active: DashMap<String, CancellationToken>,
}

impl QueryRegistry {
    /// Create a new, empty query registry.
    pub fn new() -> Self {
        Self {
            active: DashMap::new(),
        }
    }

    /// Register a new query and return its cancellation token.
    ///
    /// The caller should pass this token into the query execution future
    /// so that cancellation can be observed via `token.cancelled()`.
    pub fn register(&self, query_id: &str) -> CancellationToken {
        let token = CancellationToken::new();
        self.active.insert(query_id.to_string(), token.clone());
        token
    }

    /// Cancel an active query by firing its cancellation token.
    ///
    /// Returns `true` if the query was found and cancelled, `false` if the
    /// query ID was not in the registry (already completed or never existed).
    pub fn cancel(&self, query_id: &str) -> bool {
        if let Some((_, token)) = self.active.remove(query_id) {
            token.cancel();
            true
        } else {
            false
        }
    }

    /// Mark a query as completed and remove it from the registry.
    ///
    /// This does **not** fire the cancellation token — it simply cleans up
    /// the registry entry after a query finishes normally.
    pub fn complete(&self, query_id: &str) {
        self.active.remove(query_id);
    }

    /// Return the number of currently active (in-flight) queries.
    pub fn active_count(&self) -> usize {
        self.active.len()
    }
}

impl Default for QueryRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn register_and_complete_lifecycle() {
        let registry = QueryRegistry::new();

        let token = registry.register("q-001");
        assert_eq!(registry.active_count(), 1);
        assert!(!token.is_cancelled());

        registry.complete("q-001");
        assert_eq!(registry.active_count(), 0);
        // Token is NOT cancelled by complete — only removed from registry.
        assert!(!token.is_cancelled());
    }

    #[tokio::test]
    async fn cancel_fires_the_token() {
        let registry = QueryRegistry::new();

        let token = registry.register("q-002");
        assert!(!token.is_cancelled());

        let cancelled = registry.cancel("q-002");
        assert!(cancelled);
        assert!(token.is_cancelled());
        assert_eq!(registry.active_count(), 0);
    }

    #[tokio::test]
    async fn cancel_returns_false_for_unknown_query() {
        let registry = QueryRegistry::new();
        assert!(!registry.cancel("nonexistent"));
    }

    #[tokio::test]
    async fn cancel_returns_false_for_already_completed_query() {
        let registry = QueryRegistry::new();

        registry.register("q-003");
        registry.complete("q-003");

        assert!(!registry.cancel("q-003"));
    }

    #[tokio::test]
    async fn multiple_concurrent_queries() {
        let registry = QueryRegistry::new();

        let t1 = registry.register("q-a");
        let t2 = registry.register("q-b");
        let t3 = registry.register("q-c");
        assert_eq!(registry.active_count(), 3);

        registry.cancel("q-b");
        assert_eq!(registry.active_count(), 2);
        assert!(!t1.is_cancelled());
        assert!(t2.is_cancelled());
        assert!(!t3.is_cancelled());

        registry.complete("q-a");
        registry.complete("q-c");
        assert_eq!(registry.active_count(), 0);
    }

    #[tokio::test]
    async fn child_token_observes_parent_cancellation() {
        let registry = QueryRegistry::new();

        let token = registry.register("q-004");
        let child = token.child_token();
        assert!(!child.is_cancelled());

        registry.cancel("q-004");
        assert!(child.is_cancelled());
    }
}
