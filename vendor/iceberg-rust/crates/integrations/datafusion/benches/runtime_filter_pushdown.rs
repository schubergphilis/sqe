// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Microbenchmarks for the runtime / dynamic filter pushdown path.
//!
//! Why this exists: SF10 TPC-H runs have a 5-7% wall-clock noise
//! floor (page cache, Trino JIT, Polaris startup state), which is
//! larger than the per-task-overhead effects we keep trying to
//! optimize away. This bench strips all of that out: no Parquet,
//! no S3, no network, no coordinator, no Trino. Just synthetic
//! `PhysicalExpr` -> `Predicate` conversion + `Predicate::bind` at
//! varying IN-list sizes.
//!
//! The questions we want to answer with hard numbers:
//!   1. Is `convert_physical_filters_to_predicate` actually O(N) in
//!      the IN-list size, and if so, what's the per-element constant?
//!   2. Is `Predicate::bind` O(1) regardless of N (as suspected)?
//!   3. Where is the cliff between "the conversion cost is negligible"
//!      and "the conversion dominates"?
//!
//! Run with: `cargo bench -p iceberg-datafusion --bench runtime_filter_pushdown`

use std::sync::Arc;

use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, in_list, lit};
use datafusion::scalar::ScalarValue;
use iceberg::expr::Bind;
use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
use iceberg_datafusion::physical_plan::physical_to_predicate::convert_physical_filters_to_predicate;

/// IN-list cardinalities to bench. Chosen to bracket iceberg-rust's
/// `IN_PREDICATE_LIMIT = 200` (where the row-group evaluator
/// short-circuits) and the pathological probe-set sizes seen at TPC-H
/// SF10 (l_orderkey IN list from a build of filtered orders runs
/// ~580K).
const IN_LIST_SIZES: &[usize] = &[10, 100, 199, 200, 201, 1_000, 10_000, 100_000, 580_000];

/// Build a one-column iceberg schema. The bench column is `l_orderkey`
/// because that's the column the q04 runtime filter targets.
fn iceberg_schema() -> IcebergSchema {
    IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(vec![Arc::new(NestedField::required(
            1,
            "l_orderkey",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()
        .expect("valid schema")
}

fn arrow_schema() -> ArrowSchema {
    ArrowSchema::new(vec![Field::new("l_orderkey", DataType::Int64, false)])
}

/// Build an InListExpr `l_orderkey IN (0, 1, 2, ..., n-1)` against the
/// physical schema column index 0. Uses the public `in_list` helper
/// (`InListExpr::new` is crate-private as of DF 53).
fn make_in_list(n: usize) -> Arc<dyn PhysicalExpr> {
    let schema = arrow_schema();
    let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("l_orderkey", 0));
    let list: Vec<Arc<dyn PhysicalExpr>> = (0..n)
        .map(|i| lit(ScalarValue::Int64(Some(i as i64))))
        .collect();
    in_list(col, list, &false, &schema).expect("synthetic in_list builds")
}

/// Build the realistic `bounds AND membership` shape that
/// `HashJoinExec`'s `shared_bounds.rs` actually emits: a min/max
/// bracket on the join key, ANDed with an IN-list of the build keys.
fn make_bounds_and_membership(n: usize) -> Arc<dyn PhysicalExpr> {
    use datafusion::logical_expr::Operator;

    let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("l_orderkey", 0));
    let lo: Arc<dyn PhysicalExpr> = lit(ScalarValue::Int64(Some(0)));
    let hi: Arc<dyn PhysicalExpr> = lit(ScalarValue::Int64(Some((n.max(1) - 1) as i64)));

    let ge: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(col.clone(), Operator::GtEq, lo));
    let le: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(col.clone(), Operator::LtEq, hi));
    let bounds: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(ge, Operator::And, le));

    let membership = make_in_list(n);

    Arc::new(BinaryExpr::new(bounds, Operator::And, membership))
}

fn bench_convert_in_list_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("convert_in_list_only");
    for &n in IN_LIST_SIZES {
        let filter = make_in_list(n);
        let filters = vec![filter];
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &filters, |b, f| {
            b.iter(|| {
                let p = convert_physical_filters_to_predicate(black_box(f.as_slice()));
                black_box(p);
            });
        });
    }
    group.finish();
}

fn bench_convert_bounds_and_membership(c: &mut Criterion) {
    let mut group = c.benchmark_group("convert_bounds_and_membership");
    for &n in IN_LIST_SIZES {
        let filter = make_bounds_and_membership(n);
        let filters = vec![filter];
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &filters, |b, f| {
            b.iter(|| {
                let p = convert_physical_filters_to_predicate(black_box(f.as_slice()));
                black_box(p);
            });
        });
    }
    group.finish();
}

fn bench_bind_predicate(c: &mut Criterion) {
    let mut group = c.benchmark_group("bind_predicate");
    let schema = Arc::new(iceberg_schema());
    for &n in IN_LIST_SIZES {
        let filters = vec![make_in_list(n)];
        let predicate =
            convert_physical_filters_to_predicate(&filters).expect("convertible synthetic filter");
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &predicate, |b, p| {
            b.iter_batched(
                || (p.clone(), schema.clone()),
                |(pred, sch)| {
                    let bound = pred.bind(sch, true);
                    black_box(bound)
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_convert_in_list_only,
    bench_convert_bounds_and_membership,
    bench_bind_predicate,
);
criterion_main!(benches);
