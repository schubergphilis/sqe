//! The write probe's update list: what the catalog authorizes it as.

use std::collections::HashMap;

use iceberg::spec::{
    FormatVersion, NestedField, PrimitiveType, Schema, SortOrder, TableMetadata,
    TableMetadataBuilder, Type, UnboundPartitionSpec, MAIN_BRANCH,
};
use iceberg::TableUpdate;
use sqe_catalog::write_probe::write_probe_updates;

fn fresh(format_version: FormatVersion) -> TableMetadata {
    let schema = Schema::builder()
        .with_fields(vec![NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        )
        .into()])
        .build()
        .unwrap();
    TableMetadataBuilder::new(
        schema,
        UnboundPartitionSpec::builder().build(),
        SortOrder::unsorted_order(),
        "s3://bucket/ns/t".to_string(),
        format_version,
        HashMap::new(),
    )
    .unwrap()
    .build()
    .unwrap()
    .metadata
}

fn apply(metadata: &TableMetadata, updates: Vec<TableUpdate>) -> TableMetadata {
    let mut builder = TableMetadataBuilder::new_from_metadata(metadata.clone(), None);
    for update in updates {
        builder = update.apply(builder).unwrap();
    }
    builder.build().unwrap().metadata
}

/// A fresh table has no `main`: the probe sends what a first append sends,
/// never a ref removal (which would need the right to drop branches and tags).
#[test]
fn a_fresh_table_is_probed_with_a_first_append() {
    for version in [FormatVersion::V2, FormatVersion::V3] {
        let metadata = fresh(version);
        let updates = write_probe_updates(&metadata);
        assert_eq!(updates.len(), 2, "{updates:?}");
        let TableUpdate::AddSnapshot { snapshot } = &updates[0] else {
            panic!("first update must add a snapshot: {updates:?}");
        };
        let TableUpdate::SetSnapshotRef {
            ref_name,
            reference,
        } = &updates[1]
        else {
            panic!("second update must set main: {updates:?}");
        };
        assert_eq!(ref_name, MAIN_BRANCH);
        assert_eq!(reference.snapshot_id, snapshot.snapshot_id());
        // The updates are a valid first commit: they apply to the metadata.
        let applied = apply(&metadata, updates.clone());
        assert_eq!(applied.current_snapshot_id(), Some(snapshot.snapshot_id()));
    }
}

/// With a `main`, the probe sets it to the snapshot it already points at.
#[test]
fn a_table_with_main_is_probed_with_an_unchanged_set_ref() {
    let metadata = fresh(FormatVersion::V2);
    let written = apply(&metadata, write_probe_updates(&metadata));
    let main = written
        .refs_iter()
        .find(|(n, _)| *n == MAIN_BRANCH)
        .unwrap()
        .1
        .clone();
    let updates = write_probe_updates(&written);
    assert!(
        matches!(
            updates.as_slice(),
            [TableUpdate::SetSnapshotRef { ref_name, reference }]
                if ref_name == MAIN_BRANCH && *reference == main
        ),
        "{updates:?}"
    );
}
