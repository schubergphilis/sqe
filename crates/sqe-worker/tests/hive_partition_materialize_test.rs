//! Cycle 5, task 2.3: worker-side partition materialization must produce what
//! coordinator-local execution produces.
//!
//! These assert the two things that go wrong silently rather than loudly: a
//! partition column's TYPE, and the column ORDER. Both return wrong data
//! instead of an error, which is why they are asserted directly rather than
//! left to a row-count check.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use sqe_planner::{HiveScanDescriptor, HiveScanFormat};
use sqe_worker::hive_partition::{append_partition_columns, full_schema_of};

/// file columns (order_id, amount) followed by partition columns, in the
/// descriptor's declared order.
fn full_schema(partition_fields: Vec<Field>) -> Arc<Schema> {
    let mut fields = vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, true),
    ];
    fields.extend(partition_fields);
    Arc::new(Schema::new(fields))
}

fn file_batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("amount", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2])),
            Arc::new(Int64Array::from(vec![10_i64, 20])),
        ],
    )
    .unwrap()
}

fn descriptor(
    partition_columns: Vec<&str>,
    values: &[(&str, &str)],
    schema: &Schema,
) -> HiveScanDescriptor {
    HiveScanDescriptor {
        format: HiveScanFormat::Parquet,
        delimiter: b',',
        quote: None,
        escape: None,
        has_header: false,
        partition_values: values
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<BTreeMap<_, _>>(),
        partition_columns: partition_columns.iter().map(|s| s.to_string()).collect(),
        full_schema_json: serde_json::to_string(schema).expect("schema serializes"),
    }
}

#[test]
fn a_typed_partition_key_keeps_its_declared_type() {
    // `year` is an Int64 partition key. If the worker defaulted it to Utf8 the
    // rows would still come back and the query would still "work" -- with a
    // column of the wrong type. That is the failure this test exists for.
    let schema = full_schema(vec![Field::new("year", DataType::Int64, true)]);
    let desc = descriptor(vec!["year"], &[("year", "2024")], &schema);
    let parsed = full_schema_of(&desc).expect("schema round-trips through the wire form");

    let out = append_partition_columns(&file_batch(), &desc, &parsed).expect("materialize");

    assert_eq!(out.num_rows(), 2, "materializing must not change row count");
    let year = out.column_by_name("year").expect("year appended");
    assert_eq!(
        year.data_type(),
        &DataType::Int64,
        "an integer partition key must not become Utf8 on the worker path"
    );
    let vals = year.as_any().downcast_ref::<Int64Array>().expect("Int64");
    assert_eq!(vals.value(0), 2024);
    assert_eq!(vals.value(1), 2024);
}

#[test]
fn the_default_partition_sentinel_becomes_null_not_the_literal_string() {
    // The first version of this test asserted the literal string, which is
    // what `attach_partition_values` produces -- but that is an INTERMEDIATE.
    // The coordinator then runs `nullify_default_partition_columns`, which
    // rewrites a Utf8 partition column equal to the sentinel to NULL. Asserting
    // the intermediate would have locked in a worker that returns
    // "__HIVE_DEFAULT_PARTITION__" where the coordinator returns NULL: same
    // type, same row count, different values.
    let schema = full_schema(vec![Field::new("dt", DataType::Utf8, true)]);
    let desc = descriptor(vec!["dt"], &[], &schema); // no value for dt
    let parsed = full_schema_of(&desc).unwrap();

    let out = append_partition_columns(&file_batch(), &desc, &parsed).expect("materialize");
    let dt = out.column_by_name("dt").unwrap();
    assert_eq!(
        dt.null_count(),
        dt.len(),
        "the sentinel must resolve to NULL, matching what coordinator-local \
         execution returns after nullify_default_partition_columns"
    );
}

#[test]
fn an_explicit_value_is_not_nullified() {
    // The control for the test above: only the sentinel nullifies. If the
    // worker nullified every partition value the test above would still pass.
    let schema = full_schema(vec![Field::new("dt", DataType::Utf8, true)]);
    let desc = descriptor(vec!["dt"], &[("dt", "2024-01-01")], &schema);
    let parsed = full_schema_of(&desc).unwrap();

    let out = append_partition_columns(&file_batch(), &desc, &parsed).unwrap();
    let dt = out.column_by_name("dt").unwrap();
    assert_eq!(dt.null_count(), 0, "an explicit value must survive");
    assert_eq!(
        dt.as_any().downcast_ref::<StringArray>().unwrap().value(0),
        "2024-01-01"
    );
}

#[test]
fn declared_order_is_honoured_not_map_order() {
    // `partition_values` is a BTreeMap, which sorts to dt,region. The declared
    // order is region,dt. If the worker followed the map the two columns swap
    // -- same types, same row count, wrong values in each.
    let schema = full_schema(vec![
        Field::new("region", DataType::Utf8, true),
        Field::new("dt", DataType::Utf8, true),
    ]);
    let desc = descriptor(
        vec!["region", "dt"],
        &[("dt", "2024-01-01"), ("region", "eu")],
        &schema,
    );
    let parsed = full_schema_of(&desc).unwrap();

    let out = append_partition_columns(&file_batch(), &desc, &parsed).expect("materialize");

    let region = out.column_by_name("region").unwrap();
    let dt = out.column_by_name("dt").unwrap();
    assert_eq!(
        region
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "eu",
        "region must carry the region value, not the map's first key"
    );
    assert_eq!(
        dt.as_any().downcast_ref::<StringArray>().unwrap().value(0),
        "2024-01-01"
    );
}

#[test]
fn an_unpartitioned_table_is_passed_through_unchanged() {
    let schema = full_schema(vec![]);
    let desc = descriptor(vec![], &[], &schema);
    let parsed = full_schema_of(&desc).unwrap();
    let out = append_partition_columns(&file_batch(), &desc, &parsed).unwrap();
    assert_eq!(out.num_columns(), 2);
}

#[test]
fn a_descriptor_claiming_more_partitions_than_the_schema_is_an_error() {
    // A malformed descriptor must fail loudly. Silently materializing fewer
    // columns would produce a batch that does not match the declared schema.
    let schema = full_schema(vec![]);
    let desc = descriptor(vec!["a", "b"], &[], &schema);
    let parsed = full_schema_of(&desc).unwrap();
    let err = append_partition_columns(&file_batch(), &desc, &parsed).unwrap_err();
    assert!(err.contains("partition columns"), "got: {err}");
}
