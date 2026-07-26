//! Shared execution primitives for bounded-memory and spill.
//!
//! Phase 1 delivers ownership-based byte accounting:
//! - [`ByteBudget`] / [`BytePermit`]: fixed-unit admission against a capacity
//!   and (optionally) the DataFusion [`MemoryPool`].
//! - [`Accounted`]: a value that carries its permit so moves do not double-charge.
//!
//! Later phases add `SpillManager`, segment stores, and the temporary-memory
//! governor on top of these primitives.
//!
//! See `docs/superpowers/plans/2026-07-25-bounded-memory-spill-execution.md`.

pub mod accounted;
pub mod budget;
pub mod error;

pub use accounted::Accounted;
pub use budget::{ByteBudget, BytePermit, DEFAULT_BUDGET_GRANULARITY};
pub use error::{BudgetError, Result};
