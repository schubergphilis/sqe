//! Typed errors for byte budgets and spill I/O.

use thiserror::Error;

/// Result alias for `sqe-spill` operations.
pub type Result<T> = std::result::Result<T, BudgetError>;

/// Failures from [`crate::ByteBudget`] admission and spill storage.
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

    /// Spill root path is missing, not a directory, or fails validation.
    #[error("invalid spill root '{path}': {reason}")]
    InvalidSpillRoot { path: String, reason: String },

    /// Spill quota would be exceeded by this write.
    #[error("spill quota exceeded for scope '{scope}': need {need} bytes, free {free} bytes (max {max})")]
    SpillQuotaExceeded {
        scope: String,
        need: u64,
        free: u64,
        max: u64,
    },

    /// Local disk free space would fall below `min_free_bytes`.
    #[error(
        "spill free-space guard: need {need} free bytes after write, only {available} available"
    )]
    SpillDiskFull { need: u64, available: u64 },

    /// Segment is truncated, corrupted, or has a checksum mismatch.
    #[error("spill segment corruption at '{path}': {reason}")]
    SegmentCorrupt { path: String, reason: String },

    /// Segment version is not supported by this reader.
    #[error("unsupported spill segment version {version} at '{path}'")]
    UnsupportedSegmentVersion { path: String, version: u32 },

    /// I/O failure while reading or writing spill.
    #[error("spill I/O error at '{path}': {source}")]
    SpillIo {
        path: String,
        #[source]
        source: std::io::Error,
    },

    /// Generic spill configuration error.
    #[error("spill config error: {0}")]
    Config(String),
}
