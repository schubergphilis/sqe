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

//! SQE PATCH (sqe#369): parquet bloom-filter (SBBF) row-group pruning
//! for membership predicates.
//!
//! The min/max stats path ([`super::row_group_metrics_evaluator`]) gives
//! up on `IN` predicates above `IN_PREDICATE_LIMIT` (200) literals, so a
//! hash-join runtime filter sealed as a large key set gets no row-group
//! pruning from statistics. When the file carries parquet bloom filters
//! on the probed column (Iceberg `write.parquet.bloom-filter-columns`),
//! testing every key against the row group's SBBF can prove the row
//! group contains none of the keys and prune it before any decode.
//!
//! This module extracts the *membership conjuncts* of a bound predicate:
//! `IN` sets and equality literals that appear in positive conjunctive
//! position only. The walk recurses into `AND` nodes and ignores `OR`,
//! `NOT`, and every non-membership leaf. That discipline is a safety
//! invariant: the reader's `final_predicate` also carries equality-delete
//! predicates, which arrive as negations (`NotEq` / `NotIn` after
//! `rewrite_not`); treating a negated set as a membership set would prune
//! row groups that must be scanned and return wrong results. A conjunct
//! extracted here is individually *required* by the predicate, so if
//! every one of its keys tests bloom-negative for a row group, that row
//! group provably matches no rows and can be skipped.
//!
//! The bloom filters themselves are loaded lazily by the caller (the
//! Arrow reader) via parquet's async
//! `get_row_group_column_bloom_filter`; this module only decides *what*
//! to probe and evaluates a loaded [`Sbbf`] against a conjunct's keys.

use std::collections::HashMap;

use parquet::bloom_filter::Sbbf;

use crate::expr::{BoundPredicate, PredicateOperator};
use crate::spec::{Datum, PrimitiveLiteral};

/// Default cap on the number of literals a single membership conjunct
/// may carry and still be bloom-probed. Probing is O(keys) per row
/// group in the worst (all-negative) case; the cap matches SQE's
/// runtime-filter InList emission cap so every sealed filter that
/// reaches the reader is probeable by default.
pub const DEFAULT_BLOOM_PROBE_MAX_VALUES: usize = 65536;

/// A single positive membership conjunct extracted from a bound
/// predicate: "column (by parquet leaf index) must be one of
/// `literals`".
#[derive(Debug, Clone)]
pub struct BloomProbeConjunct {
    /// Parquet leaf column index (from the reader's field-id map).
    pub parquet_column_index: usize,
    /// The membership keys. Never empty.
    pub literals: Vec<Datum>,
}

impl BloomProbeConjunct {
    /// Returns `true` when EVERY key of this conjunct tests
    /// bloom-negative in `sbbf`, i.e. the row group provably contains
    /// none of the keys and can be pruned. Short-circuits to `false`
    /// (keep the row group) on the first bloom-positive hit or on any
    /// key whose type cannot be probed.
    pub fn all_absent(&self, sbbf: &Sbbf) -> bool {
        !self.literals.is_empty()
            && self
                .literals
                .iter()
                .all(|datum| datum_absent(sbbf, datum) == Some(true))
    }
}

/// Test one datum against a loaded SBBF.
///
/// Returns `Some(true)` when the value is definitely absent,
/// `Some(false)` when the bloom reports (maybe-)present, and `None`
/// when the literal's physical representation is not supported (the
/// caller must treat `None` as present).
///
/// The datum comes from a *bound* predicate, so its literal type
/// already matches the Iceberg field type, which maps 1:1 onto the
/// parquet physical type the writer hashed into the bloom: `int`/
/// `date` -> INT32 (`i32`), `long`/`time`/`timestamp` -> INT64
/// (`i64`), `string` -> BYTE_ARRAY over the UTF-8 bytes, `binary` ->
/// BYTE_ARRAY, `float`/`double` -> 4/8-byte IEEE bit patterns.
fn datum_absent(sbbf: &Sbbf, datum: &Datum) -> Option<bool> {
    match datum.literal() {
        PrimitiveLiteral::Int(v) => Some(!sbbf.check(v)),
        PrimitiveLiteral::Long(v) => Some(!sbbf.check(v)),
        PrimitiveLiteral::String(v) => Some(!sbbf.check(v.as_str())),
        PrimitiveLiteral::Binary(v) => Some(!sbbf.check(v.as_slice())),
        PrimitiveLiteral::Float(v) => Some(!sbbf.check(&v.into_inner())),
        PrimitiveLiteral::Double(v) => Some(!sbbf.check(&v.into_inner())),
        // Boolean, Int128/UInt128 (decimal/uuid fixed-len), AboveMax,
        // BelowMin: not representable as a probeable physical value.
        _ => None,
    }
}

/// Extractor for bloom-probeable membership conjuncts.
pub struct SbbfRowGroupEvaluator;

impl SbbfRowGroupEvaluator {
    /// Collect the positive membership conjuncts of `predicate`.
    ///
    /// Walks `AND` nodes recursively; collects `Set(In)` and
    /// `Binary(Eq)` leaves whose column maps to a parquet leaf in
    /// `field_id_map`; ignores `OR` / `NOT` subtrees and every other
    /// leaf (they can never make pruning unsafe, only less complete).
    /// Conjuncts with more than `max_values` literals are dropped to
    /// bound probe cost.
    pub fn collect(
        predicate: &BoundPredicate,
        field_id_map: &HashMap<i32, usize>,
        max_values: usize,
    ) -> Vec<BloomProbeConjunct> {
        let mut out = Vec::new();
        Self::walk(predicate, field_id_map, max_values, &mut out);
        out
    }

    fn walk(
        predicate: &BoundPredicate,
        field_id_map: &HashMap<i32, usize>,
        max_values: usize,
        out: &mut Vec<BloomProbeConjunct>,
    ) {
        match predicate {
            BoundPredicate::And(expr) => {
                for input in expr.inputs() {
                    Self::walk(input, field_id_map, max_values, out);
                }
            }
            BoundPredicate::Set(expr) if expr.op() == PredicateOperator::In => {
                let literal_count = expr.literals().len();
                if literal_count == 0 || literal_count > max_values {
                    return;
                }
                let Some(&parquet_column_index) = field_id_map.get(&expr.term().field().id) else {
                    return;
                };
                out.push(BloomProbeConjunct {
                    parquet_column_index,
                    literals: expr.literals().iter().cloned().collect(),
                });
            }
            BoundPredicate::Binary(expr) if expr.op() == PredicateOperator::Eq => {
                let Some(&parquet_column_index) = field_id_map.get(&expr.term().field().id) else {
                    return;
                };
                out.push(BloomProbeConjunct {
                    parquet_column_index,
                    literals: vec![expr.literal().clone()],
                });
            }
            // Or / Not subtrees: a membership set inside them is not a
            // required conjunct of the whole predicate; using it to
            // prune would be unsound (e.g. negated equality-delete
            // predicates). NotIn / NotEq and every other leaf carry no
            // positive membership information. Skip them all.
            _ => {}
        }
    }
}
