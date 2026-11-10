//! Byte accounting and memory pools for the SQE worker.
//!
//! - [`ByteBudget`] / [`BytePermit`]: fixed-unit admission against a capacity
//!   and (optionally) the DataFusion [`MemoryPool`].
//! - [`Accounted`]: a value that carries its permit so moves do not double-charge.
//! - [`MemoryGovernor`] and [`ResizableFairSpillPool`]: the worker-wide pool,
//!   resizable on hot config reload.
//! - [`MemoryGrant`]: the operator memory grant the planner sizes sorts from.
//!
//! The crate once also held a spill segment store (local, S3 and tiered
//! backends behind a `SpillManager`). No query path ever wrote to it, since
//! workers only scan; it was removed and DataFusion's own disk manager does
//! all spilling.
//!
//! [`MemoryPool`]: datafusion::execution::memory_pool::MemoryPool

pub mod accounted;
pub mod budget;
pub mod error;
pub mod governor;
pub mod reclaim;
pub mod resizable_pool;

pub use accounted::Accounted;
pub use budget::{
    split_default_read_headroom, split_read_headroom, ByteBudget, BytePermit,
    DEFAULT_BUDGET_GRANULARITY, DEFAULT_READ_HEADROOM_DEN, DEFAULT_READ_HEADROOM_NUM,
};
pub use error::{BudgetError, Result};
pub use governor::MemoryGovernor;
pub use reclaim::{MemoryGrant, ReclaimableConsumer};
pub use resizable_pool::ResizableFairSpillPool;
