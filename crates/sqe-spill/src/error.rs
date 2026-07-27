//! Typed errors for byte budgets and (later) spill I/O.

use thiserror::Error;

/// Result alias for `sqe-spill` operations.
pub type Result<T> = std::result::Result<T, BudgetError>;

/// Failures from [`crate::ByteBudget`] admission.
#[derive(Debug, Error)]
pub enum BudgetError {
    /// A single acquire requested more bytes than the budget's total capacity.
    /// Waiting would never succeed.
    #[error(
        "item too large for budget '{budget}': requested {requested} bytes, capacity {capacity} bytes"
    )]
    ItemTooLarge {
        budget: String,
        requested: usize,
        capacity: usize,
    },

    /// Non-blocking acquire could not obtain the requested units immediately.
    #[error("budget '{budget}' has insufficient free capacity for {requested} bytes (capacity {capacity}, used {used})")]
    InsufficientCapacity {
        budget: String,
        requested: usize,
        capacity: usize,
        used: usize,
    },

    /// The DataFusion memory pool refused the reservation (worker-wide limit).
    #[error("memory pool refused reservation for budget '{budget}': {source}")]
    PoolExhausted {
        budget: String,
        #[source]
        source: datafusion::error::DataFusionError,
    },

    /// The acquire was cancelled (e.g. drop of a future while waiting).
    #[error("budget '{budget}' acquire cancelled")]
    Cancelled { budget: String },
}
