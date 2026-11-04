//! Moved to [`sqe_core::memory_headroom`] so the embedded engine (`sqe-cli`)
//! builds the same pool; re-exported under the old path.

pub(crate) use sqe_core::memory_headroom::{HeadroomBudget, HeadroomPool};
