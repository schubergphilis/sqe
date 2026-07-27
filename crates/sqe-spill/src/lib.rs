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
pub mod exchange;
pub mod fault;
pub mod governor;
pub mod manager;
pub mod operator_admit;
pub mod reclaim;
pub mod scope;
pub mod segment;
pub mod store;
pub mod store_local;

pub use accounted::Accounted;
pub use budget::{
    split_default_read_headroom, split_read_headroom, ByteBudget, BytePermit,
    DEFAULT_BUDGET_GRANULARITY, DEFAULT_READ_HEADROOM_DEN, DEFAULT_READ_HEADROOM_NUM,
};
pub use error::{BudgetError, Result};
pub use fault::{
    clear_faults, faults_injected, install_faults, serial_test_guard, take_fault, FaultSession,
    SpillFault,
};
pub use manager::{SpillManager, SpillScopeGuard};
pub use exchange::{
    exchange_scope, read_manifest, write_manifest_atomic, AttemptManifest, AttemptState,
    ExchangeAttemptStore, SharedExchangeAttemptStore, TaskKey, ATTEMPT_MANIFEST_VERSION,
};
pub use governor::{
    AdmissionDecision, AdmissionRequest, GrantGuard, MemoryGovernor, SharedMemoryGovernor,
    WorkloadClass,
};
pub use operator_admit::{
    admit_operator, LiveConsumerRegistry, OperatorConsumer, OperatorGrantGuard,
};
pub use reclaim::{
    GrantRegistry, MemoryGrant, ReclaimableConsumer, SharedGrantRegistry,
};
pub use scope::SpillScope;
pub use segment::{SpillSegment, SEGMENT_FORMAT_VERSION, SEGMENT_MAGIC};
pub use store::{SegmentReader, SegmentStore, SegmentWriter};
pub use store_local::LocalSegmentStore;
