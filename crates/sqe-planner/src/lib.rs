//! Planning for SQE's distributed scan.
//!
//! The live surface is small and this crate's public API describes it, not
//! the roadmap (review finding 108):
//!
//! - the scan-task model workers execute: [`ScanTask`], [`HiveScanDescriptor`],
//!   [`ScanCredentials`], [`ScanStorage`] and the ticket versions;
//! - morsel planning over files and row groups ([`plan_scan_morsels`],
//!   [`bin_pack_files`]);
//! - the coordinator's physical optimizer rules: [`JoinStrategyRule`],
//!   [`StarSchemaReorderRule`], [`SortMemoryRule`],
//!   [`SingleDistinctCountCompanionRule`] and [`PuffinStatisticsRequestRule`];
//! - the coordinator/worker protocol types ([`WorkerHello`], [`WorkerFault`]).
//!
//! Every join, aggregate and sort runs on the coordinator; workers scan and
//! stream Arrow back. The stage planner, shuffle nodes, range-partitioned
//! distributed sort, distributed aggregate and broadcast/shuffle-hash join
//! rules that used to live here had no caller in any binary and were
//! deleted on 2026-09-11 (issue #551); git history before that date keeps
//! them.

pub mod join_strategy;
pub mod scan_morsel;
pub mod scan_task;
pub mod single_distinct_count;
pub mod sort_memory;
pub mod sort_memory_rule;
pub mod splitter;
pub mod star_schema_reorder;
pub mod stats;
pub mod worker_fault;
pub mod worker_protocol;

pub use join_strategy::{BuildSizeEstimate, JoinStrategyRule, DEFAULT_HASH_JOIN_THRESHOLD};
pub use scan_morsel::{SCAN_TASK_VERSION_V3, SCAN_TASK_VERSION_V4};
pub use scan_morsel::{
    group_row_groups_into_morsels, plan_file_byte_morsels, plan_scan_morsels, RowGroupSlice,
    ScanMorsel, DEFAULT_MORSEL_MAX_BYTES, DEFAULT_MORSEL_TARGET_BYTES, SCAN_TASK_VERSION_CURRENT,
    SCAN_TASK_VERSION_V1, SCAN_TASK_VERSION_V2,
};
pub use scan_task::{
    HiveScanDescriptor, HiveScanFormat, ScanCredentials, ScanStorage, ScanTask, ScanTaskBuilder,
    TicketSecret,
};
pub use single_distinct_count::SingleDistinctCountCompanionRule;
pub use sort_memory::{SortAdmissionError, SortMemoryPolicy, DEFAULT_SORT_MERGE_FANIN};
pub use sort_memory_rule::{sort_grant_bytes, SortMemoryRule};
pub use splitter::bin_pack_files;
pub use star_schema_reorder::{StarSchemaReorderRule, DEFAULT_MIN_RATIO};
pub use stats::PuffinStatisticsRequestRule;
pub use worker_fault::{
    classify_anyhow_error, classify_datafusion_error, classify_error_chain, classify_message,
    classify_object_store_error, WorkerFault, FAULT_METADATA_KEY,
};
pub use worker_protocol::{
    CoordinatorHello, CoordinatorVersions, HeartbeatPayload, HelloCompat, WorkerCapacity,
    WorkerHello, HELLO_VERSION, HELLO_VERSION_LEGACY, PROTO_VERSION, PROTO_VERSION_UNKNOWN,
};
