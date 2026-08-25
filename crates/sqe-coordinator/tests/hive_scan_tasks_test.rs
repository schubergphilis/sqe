//! Cycle 5, task 3.1: per-partition `ScanTask` construction.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use sqe_coordinator::hive_scan_tasks::{
    build_hive_scan_tasks, HivePartitionFiles, HiveReadFormat, HiveS3Access, HiveScanQuery,
    HiveScanTable,
};
use sqe_planner::HiveScanFormat;

fn full_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, true),
        Field::new("dt", DataType::Utf8, true),
    ]))
}

fn parquet() -> HiveReadFormat {
    HiveReadFormat {
        format: HiveScanFormat::Parquet,
        delimiter: b',',
        quote: None,
        escape: None,
        has_header: false,
    }
}

fn partition(region: &str, dt: &str, files: &[(&str, u64)]) -> HivePartitionFiles {
    HivePartitionFiles {
        values: BTreeMap::from([
            ("region".to_string(), region.to_string()),
            ("dt".to_string(), dt.to_string()),
        ]),
        files: files
            .iter()
            .map(|(p, s)| ((*p).to_string(), *s))
            .collect(),
    }
}

fn build(parts: &[HivePartitionFiles]) -> Vec<sqe_planner::ScanTask> {
    let read = parquet();
    let schema = full_schema();
    let s3 = HiveS3Access {
        endpoint: "http://minio:9000".to_string(),
        region: "us-east-1".to_string(),
        access_key: "ak".to_string(),
        secret_key: "sk".to_string(),
        ..Default::default()
    };
    let cols = ["region".to_string(), "dt".to_string()];
    let projected = ["order_id".to_string()];
    build_hive_scan_tasks(
        parts,
        &HiveScanTable {
            read: &read,
            partition_columns: &cols,
            full_schema: &schema,
            s3: &s3,
        },
        &HiveScanQuery {
            projected_columns: &projected,
            predicate_proto: None,
            limit: None,
            fragment_prefix: "frag",
        },
    )
    .expect("build")
}

#[test]
fn one_task_per_partition_each_carrying_its_own_values() {
    let tasks = build(&[
        partition("eu", "2024-01-01", &[("s3://b/eu/1.parquet", 100)]),
        partition("us", "2024-01-02", &[("s3://b/us/1.parquet", 200)]),
    ]);
    assert_eq!(tasks.len(), 2);

    let eu = tasks[0].hive.as_ref().expect("descriptor");
    let us = tasks[1].hive.as_ref().expect("descriptor");
    assert_eq!(eu.partition_values.get("region").unwrap(), "eu");
    assert_eq!(us.partition_values.get("region").unwrap(), "us");
    assert_ne!(
        tasks[0].fragment_id, tasks[1].fragment_id,
        "fragment ids must be distinct or retry/dedup cannot tell them apart"
    );
}

#[test]
fn a_task_never_spans_partitions() {
    // The descriptor holds ONE partition_values map. A task covering two
    // partitions would have to pick one and would mislabel the other's rows.
    let tasks = build(&[
        partition("eu", "2024-01-01", &[("s3://b/eu/1.parquet", 1)]),
        partition("us", "2024-01-02", &[("s3://b/us/1.parquet", 1)]),
    ]);
    for t in &tasks {
        let paths = &t.data_file_paths;
        let region = t.hive.as_ref().unwrap().partition_values.get("region").unwrap();
        assert!(
            paths.iter().all(|p| p.contains(region.as_str())),
            "task for region={region} carries files from another partition: {paths:?}"
        );
    }
}

#[test]
fn file_sizes_ride_along_so_work_can_be_balanced() {
    // Sizes are the reason the coordinator lists at all rather than shipping a
    // prefix. If they are dropped, placement is blind and cycle 5 buys much less.
    let tasks = build(&[partition(
        "eu",
        "2024-01-01",
        &[("s3://b/eu/1.parquet", 100), ("s3://b/eu/2.parquet", 250)],
    )]);
    assert_eq!(tasks[0].file_sizes_bytes, vec![100, 250]);
    assert_eq!(tasks[0].data_file_paths.len(), 2);
}

#[test]
fn field_ids_are_empty_and_projection_travels_by_name() {
    let tasks = build(&[partition("eu", "2024-01-01", &[("s3://b/eu/1.parquet", 1)])]);
    assert!(
        tasks[0].projected_field_ids.is_empty(),
        "a Hive table has no Iceberg field ids; a bogus one would make the \
         worker project by it"
    );
    assert_eq!(tasks[0].projected_columns, vec!["order_id".to_string()]);
}

#[test]
fn the_descriptor_carries_the_declared_partition_order_not_the_map_order() {
    let tasks = build(&[partition("eu", "2024-01-01", &[("s3://b/eu/1.parquet", 1)])]);
    let d = tasks[0].hive.as_ref().unwrap();
    assert_eq!(
        d.partition_columns,
        vec!["region".to_string(), "dt".to_string()],
        "values is a BTreeMap and sorts to dt,region; the declared order must win"
    );
}

#[test]
fn an_empty_partition_produces_no_task() {
    // Dispatching a worker a task with no files is pure overhead.
    let tasks = build(&[
        partition("eu", "2024-01-01", &[]),
        partition("us", "2024-01-02", &[("s3://b/us/1.parquet", 1)]),
    ]);
    assert_eq!(tasks.len(), 1);
    assert_eq!(
        tasks[0].hive.as_ref().unwrap().partition_values.get("region").unwrap(),
        "us"
    );
}

#[test]
fn every_task_declares_v3_and_validates() {
    let tasks = build(&[partition("eu", "2024-01-01", &[("s3://b/eu/1.parquet", 1)])]);
    assert_eq!(tasks[0].version, 3);
    assert!(tasks[0].validate_version().is_ok());
}

#[test]
fn s3_access_is_propagated_to_every_task() {
    let tasks = build(&[
        partition("eu", "2024-01-01", &[("s3://b/eu/1.parquet", 1)]),
        partition("us", "2024-01-02", &[("s3://b/us/1.parquet", 1)]),
    ]);
    for t in &tasks {
        assert_eq!(t.s3_endpoint, "http://minio:9000");
        assert_eq!(t.s3_region, "us-east-1");
        assert_eq!(t.s3_access_key, "ak");
    }
}
