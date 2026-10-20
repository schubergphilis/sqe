//! Write-mode resolution for MoR UPDATE / MERGE (Phase H): property
//! resolution, default-is-CoW and invalid-mode rejection, without a live
//! catalog. The live round trip (snapshot summary plus a Spark 4.1 read
//! of what SQE wrote) is in `mor_spark_interop`.

use sqe_core::table_properties::{
    resolve_merge_mode, resolve_update_mode, WriteMode, WRITE_DELETE_MODE, WRITE_MERGE_MODE,
    WRITE_UPDATE_MODE,
};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Dispatch-mode contract
// ---------------------------------------------------------------------------

/// Per-operation mode: a table can run DELETE as MoR but UPDATE as CoW
/// if only `write.delete.mode` is set. The three properties are
/// independent.
#[test]
fn write_update_mode_property_resolves_independently() {
    let mut props = HashMap::new();
    props.insert(WRITE_DELETE_MODE.to_string(), "merge-on-read".to_string());
    // write.update.mode unset -> default CoW
    assert_eq!(resolve_update_mode(&props).unwrap(), WriteMode::CopyOnWrite);
    assert_eq!(resolve_merge_mode(&props).unwrap(), WriteMode::CopyOnWrite);
}

#[test]
fn write_update_mode_merge_on_read_is_recognised() {
    let mut props = HashMap::new();
    props.insert(WRITE_UPDATE_MODE.to_string(), "merge-on-read".to_string());
    assert_eq!(resolve_update_mode(&props).unwrap(), WriteMode::MergeOnRead);
}

#[test]
fn write_merge_mode_merge_on_read_is_recognised() {
    let mut props = HashMap::new();
    props.insert(WRITE_MERGE_MODE.to_string(), "merge-on-read".to_string());
    assert_eq!(resolve_merge_mode(&props).unwrap(), WriteMode::MergeOnRead);
}

/// The dispatcher must reject typos at UPDATE and MERGE time, same as
/// DELETE. A user writing `"mor"` in their TBLPROPERTIES should see a
/// clear error, not a silent fall-through to CoW.
#[test]
fn write_update_mode_rejects_typos() {
    let mut props = HashMap::new();
    props.insert(WRITE_UPDATE_MODE.to_string(), "mor".to_string());
    assert!(resolve_update_mode(&props).is_err());

    props.insert(WRITE_UPDATE_MODE.to_string(), "MoR".to_string());
    assert!(resolve_update_mode(&props).is_err());
}

#[test]
fn write_merge_mode_rejects_typos() {
    let mut props = HashMap::new();
    props.insert(WRITE_MERGE_MODE.to_string(), "mor".to_string());
    assert!(resolve_merge_mode(&props).is_err());
}
