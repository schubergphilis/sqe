//! Shared execution primitives for bounded-memory and spill.
//!
//! Phase 1 delivers ownership-based byte accounting:
//! - [`ByteBudget`] / [`BytePermit`]: fixed-unit admission against a capacity
//!   and (optionally) the DataFusion [`MemoryPool`].
//! - [`Accounted`]: a value that carries its permit so moves do not double-charge.
//!
//! Phase 3 adds the spill substrate:
//! - [`SpillManager`] / [`SpillScope`] / [`SpillSegment`]
//! - [`SegmentStore`] with a local filesystem implementation
//!
//! See `docs/superpowers/plans/2026-07-25-bounded-memory-spill-execution.md`.

pub mod accounted;
pub mod budget;
pub mod error;
pub mod manager;
pub mod scope;
pub mod segment;
pub mod store;
pub mod store_local;

pub use accounted::Accounted;
pub use budget::{ByteBudget, BytePermit, DEFAULT_BUDGET_GRANULARITY};
pub use error::{BudgetError, Result};
pub use manager::{SpillManager, SpillScopeGuard};
pub use scope::SpillScope;
pub use segment::{SpillSegment, SEGMENT_FORMAT_VERSION, SEGMENT_MAGIC};
pub use store::{SegmentReader, SegmentStore, SegmentWriter};
pub use store_local::LocalSegmentStore;
