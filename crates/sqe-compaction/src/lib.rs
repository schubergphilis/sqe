//! Shared Iceberg compaction primitives, usable by both `sqe-coordinator`
//! (the `CALL system.rewrite_data_files` procedure and the active-mode
//! maintenance scheduler) and, in a later phase, `sqe-worker` (the
//! distributed `compact_file_group` action). Workers cannot depend on
//! `sqe-coordinator`, so the catalog-independent rewrite logic lives here
//! instead.
//!
//! What stays OUT of this crate: anything that needs a session, a catalog
//! bridge, or audit logging. Those orchestration pieces
//! (`rewrite_data_files`/`rewrite_data_files_once`, `create_catalog_bridge`,
//! the commit/`RewriteFilesAction` path) remain in `sqe-coordinator::maintenance`
//! and call into this crate's primitives with already-resolved inputs.

pub mod rewrite;
pub mod wire;
pub mod write_memory;
pub mod writer;
pub mod zorder;

pub use rewrite::{
    covered_position_deletes, delete_heavy_files, expected_rows_after_deletes,
    group_files_by_partition, pack_file_groups, pack_file_groups_partition_aware,
    plan_delete_aware_read, rewrite_group, sort_group_stream, DeleteAwareReadPlan, SortCtx,
    SortSpec,
};
pub use wire::{
    sign, verify, CompactGroupFrame, CompactGroupRequest, CompactGroupResponse, S3Conn,
    SortSpecWire,
};
