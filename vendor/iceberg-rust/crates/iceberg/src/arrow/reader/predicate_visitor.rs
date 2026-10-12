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

//! Visitors that translate Iceberg bound predicates into the pieces needed for
//! Arrow-level evaluation: collecting referenced field IDs and producing
//! per-record-batch predicate closures.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_arith::boolean::{and, and_kleene, is_not_null, is_null, not, or, or_kleene};
use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, Float64Type};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Datum as ArrowDatum, RecordBatch, Scalar, UInt32Array,
    make_array,
};
use arrow_buffer::BooleanBuffer;
use arrow_cast::cast::cast;
use arrow_ord::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow_schema::{ArrowError, DataType};
use arrow_select::take::take;
use arrow_string::like::starts_with;
use fnv::FnvHashSet;
use parquet::schema::types::SchemaDescriptor;

use crate::arrow::get_arrow_datum;
use crate::error::{Result, invalid_data};
use crate::expr::visitors::bound_predicate_visitor::BoundPredicateVisitor;
use crate::expr::{BoundPredicate, BoundReference};
use crate::spec::{Datum, Literal};

/// A visitor to collect field ids from bound predicates.
pub(super) struct CollectFieldIdVisitor {
    pub(super) field_ids: HashSet<i32>,
}

impl CollectFieldIdVisitor {
    pub(super) fn field_ids(self) -> HashSet<i32> {
        self.field_ids
    }
}

impl BoundPredicateVisitor for CollectFieldIdVisitor {
    type T = ();

    fn always_true(&mut self) -> Result<()> {
        Ok(())
    }

    fn always_false(&mut self) -> Result<()> {
        Ok(())
    }

    fn and(&mut self, _lhs: (), _rhs: ()) -> Result<()> {
        Ok(())
    }

    fn or(&mut self, _lhs: (), _rhs: ()) -> Result<()> {
        Ok(())
    }

    fn not(&mut self, _inner: ()) -> Result<()> {
        Ok(())
    }

    fn is_null(&mut self, reference: &BoundReference, _predicate: &BoundPredicate) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn not_null(&mut self, reference: &BoundReference, _predicate: &BoundPredicate) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn is_nan(&mut self, reference: &BoundReference, _predicate: &BoundPredicate) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn not_nan(&mut self, reference: &BoundReference, _predicate: &BoundPredicate) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn less_than(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn less_than_or_eq(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn greater_than(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn greater_than_or_eq(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn eq(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn not_eq(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn starts_with(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn not_starts_with(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn r#in(
        &mut self,
        reference: &BoundReference,
        _literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn not_in(
        &mut self,
        reference: &BoundReference,
        _literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }
}

/// Where a predicate reads its column's values from.
pub(super) enum PredicateColumn {
    /// Index of the column in the record batch the row filter projects.
    Batch(usize),
    /// SQE PATCH (sqe#830): the file predates the column, so every row reads
    /// the column's initial-default (Iceberg spec, "Default values"). A
    /// one-row array, repeated to the batch's length.
    Default(ArrayRef),
}

/// A visitor to convert Iceberg bound predicates to Arrow predicates.
pub(super) struct PredicateConverter<'a> {
    /// The Parquet schema descriptor.
    pub(super) parquet_schema: &'a SchemaDescriptor,
    /// The map between field id and leaf column index in Parquet schema.
    pub(super) column_map: &'a HashMap<i32, usize>,
    /// The required column indices in Parquet schema for the predicates.
    pub(super) column_indices: &'a Vec<usize>,
}

impl PredicateConverter<'_> {
    /// When visiting a bound reference, we return index of the leaf column in the
    /// required column indices which is used to project the column in the record batch.
    /// Return None if the field id is not found in the column map, which is possible
    /// due to schema evolution.
    ///
    /// SQE PATCH (sqe#830): a column missing from the file that has an
    /// initial-default resolves to that default, not to None. Treating it as
    /// NULL dropped every pre-`ADD COLUMN ... DEFAULT` row from `col = default`.
    fn bound_reference(&mut self, reference: &BoundReference) -> Result<Option<PredicateColumn>> {
        let Some(index) = self.bound_column_index(reference)? else {
            let field = reference.field();
            return match (&field.initial_default, field.field_type.as_primitive_type()) {
                (Some(Literal::Primitive(value)), Some(ty)) => {
                    let datum = get_arrow_datum(&Datum::new(ty.clone(), value.clone()))?;
                    let (array, _) = datum.get();
                    Ok(Some(PredicateColumn::Default(make_array(array.to_data()))))
                }
                _ => Ok(None),
            };
        };
        Ok(Some(PredicateColumn::Batch(index)))
    }

    fn bound_column_index(&mut self, reference: &BoundReference) -> Result<Option<usize>> {
        // The leaf column's index in Parquet schema.
        if let Some(column_idx) = self.column_map.get(&reference.field().id) {
            if self.parquet_schema.get_column_root(*column_idx).is_group() {
                return Err(invalid_data!(
                    "Leaf column `{}` in predicates isn't a root column in Parquet schema.",
                    reference.field().name
                ));
            }

            // The leaf column's index in the required column indices.
            let index = self
                .column_indices
                .iter()
                .position(|&idx| idx == *column_idx)
                .ok_or(invalid_data!(
                "Leaf column `{}` in predicates cannot be found in the required column indices.",
                reference.field().name
            ))?;

            Ok(Some(index))
        } else {
            Ok(None)
        }
    }

    /// Build an Arrow predicate that always returns true.
    fn build_always_true(&self) -> Result<Box<PredicateResult>> {
        Ok(Box::new(|batch| {
            Ok(constant_bool_array(true, batch.num_rows()))
        }))
    }

    /// Build an Arrow predicate that always returns false.
    fn build_always_false(&self) -> Result<Box<PredicateResult>> {
        Ok(Box::new(|batch| {
            Ok(constant_bool_array(false, batch.num_rows()))
        }))
    }
}

/// Builds a non-null `BooleanArray` of `len` elements all set to `value`.
fn constant_bool_array(value: bool, len: usize) -> BooleanArray {
    let buffer = if value {
        BooleanBuffer::new_set(len)
    } else {
        BooleanBuffer::new_unset(len)
    };

    BooleanArray::new(buffer, None)
}

/// Gets the leaf column from the record batch for the required column index. Only
/// supports top-level columns for now.
fn project_column(
    batch: &RecordBatch,
    source: &PredicateColumn,
) -> std::result::Result<ArrayRef, ArrowError> {
    let column_idx = match source {
        PredicateColumn::Batch(index) => *index,
        PredicateColumn::Default(value) => {
            let zeros = UInt32Array::from(vec![0u32; batch.num_rows()]);
            return take(value.as_ref(), &zeros, None);
        }
    };
    let column = batch.column(column_idx);

    match column.data_type() {
        DataType::Struct(_) => Err(ArrowError::SchemaError(
            "Does not support struct column yet.".to_string(),
        )),
        _ => Ok(column.clone()),
    }
}

fn compute_is_nan(array: &ArrayRef) -> std::result::Result<BooleanArray, ArrowError> {
    // Compute NaN over the contiguous values slice, then fold the null bitmap
    // in with a single bitwise AND so that null slots become false.
    let (is_nan, nulls) = match array.data_type() {
        DataType::Float32 => {
            let arr = array.as_primitive::<Float32Type>();
            (
                BooleanBuffer::from_iter(arr.values().iter().map(|v| v.is_nan())),
                arr.nulls(),
            )
        }
        DataType::Float64 => {
            let arr = array.as_primitive::<Float64Type>();
            (
                BooleanBuffer::from_iter(arr.values().iter().map(|v| v.is_nan())),
                arr.nulls(),
            )
        }
        _ => unreachable!("is_nan is only valid for float types"),
    };

    let values = match nulls {
        Some(nulls) => &is_nan & nulls.inner(),
        None => is_nan,
    };

    Ok(BooleanArray::new(values, None))
}

pub(super) type PredicateResult =
    dyn FnMut(RecordBatch) -> std::result::Result<BooleanArray, ArrowError> + Send + 'static;

impl BoundPredicateVisitor for PredicateConverter<'_> {
    type T = Box<PredicateResult>;

    fn always_true(&mut self) -> Result<Box<PredicateResult>> {
        self.build_always_true()
    }

    fn always_false(&mut self) -> Result<Box<PredicateResult>> {
        self.build_always_false()
    }

    fn and(
        &mut self,
        mut lhs: Box<PredicateResult>,
        mut rhs: Box<PredicateResult>,
    ) -> Result<Box<PredicateResult>> {
        Ok(Box::new(move |batch| {
            let left = lhs(batch.clone())?;
            let right = rhs(batch)?;
            and_kleene(&left, &right)
        }))
    }

    fn or(
        &mut self,
        mut lhs: Box<PredicateResult>,
        mut rhs: Box<PredicateResult>,
    ) -> Result<Box<PredicateResult>> {
        Ok(Box::new(move |batch| {
            let left = lhs(batch.clone())?;
            let right = rhs(batch)?;
            or_kleene(&left, &right)
        }))
    }

    fn not(&mut self, mut inner: Box<PredicateResult>) -> Result<Box<PredicateResult>> {
        Ok(Box::new(move |batch| {
            let pred_ret = inner(batch)?;
            not(&pred_ret)
        }))
    }

    fn is_null(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            Ok(Box::new(move |batch| {
                let column = project_column(&batch, &idx)?;
                is_null(&column)
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_true()
        }
    }

    fn not_null(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            Ok(Box::new(move |batch| {
                let column = project_column(&batch, &idx)?;
                is_not_null(&column)
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn is_nan(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            Ok(Box::new(move |batch| {
                let column = project_column(&batch, &idx)?;
                compute_is_nan(&column)
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn not_nan(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            Ok(Box::new(move |batch| {
                let column = project_column(&batch, &idx)?;
                let is_nan = compute_is_nan(&column)?;
                not(&is_nan)
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_true()
        }
    }

    fn less_than(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, &idx)?;
                let literal = try_cast_literal(&literal, left.data_type())?;
                lt(&left, literal.as_ref())
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_true()
        }
    }

    fn less_than_or_eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, &idx)?;
                let literal = try_cast_literal(&literal, left.data_type())?;
                lt_eq(&left, literal.as_ref())
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_true()
        }
    }

    fn greater_than(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, &idx)?;
                let literal = try_cast_literal(&literal, left.data_type())?;
                gt(&left, literal.as_ref())
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn greater_than_or_eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, &idx)?;
                let literal = try_cast_literal(&literal, left.data_type())?;
                gt_eq(&left, literal.as_ref())
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, &idx)?;
                let literal = try_cast_literal(&literal, left.data_type())?;
                eq(&left, literal.as_ref())
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn not_eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, &idx)?;
                let literal = try_cast_literal(&literal, left.data_type())?;
                neq(&left, literal.as_ref())
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn starts_with(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, &idx)?;
                let literal = try_cast_literal(&literal, left.data_type())?;
                starts_with(&left, literal.as_ref())
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn not_starts_with(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, &idx)?;
                let literal = try_cast_literal(&literal, left.data_type())?;
                // update here if arrow ever adds a native not_starts_with
                not(&starts_with(&left, literal.as_ref())?)
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_true()
        }
    }

    fn r#in(
        &mut self,
        reference: &BoundReference,
        literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literals: Vec<_> = literals
                .iter()
                .map(|lit| get_arrow_datum(lit).unwrap())
                .collect();

            // SQE PATCH: hash-set membership for the common key types, built
            // lazily on the first batch (after `try_cast_literal` alignment so
            // cast semantics match the fallback). The or(eq) fallback below
            // runs one FULL-COLUMN kernel per literal per batch; runtime IN-list
            // join filters carry thousands of keys, which made that path
            // effectively quadratic (6.5K keys x 60M rows stalled scans for
            // minutes).
            let mut membership: Option<Option<MembershipSet>> = None;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, &idx)?;

                let set = membership
                    .get_or_insert_with(|| MembershipSet::try_build(left.data_type(), &literals));
                if let Some(set) = set {
                    return set.eval(left.as_ref());
                }

                // update this if arrow ever adds a native is_in kernel
                let mut acc = constant_bool_array(false, batch.num_rows());
                for literal in &literals {
                    let literal = try_cast_literal(literal, left.data_type())?;
                    acc = or(&acc, &eq(&left, literal.as_ref())?)?
                }

                Ok(acc)
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn not_in(
        &mut self,
        reference: &BoundReference,
        literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literals: Vec<_> = literals
                .iter()
                .map(|lit| get_arrow_datum(lit).unwrap())
                .collect();

            Ok(Box::new(move |batch| {
                // update this if arrow ever adds a native not_in kernel
                let left = project_column(&batch, &idx)?;
                let mut acc = constant_bool_array(true, batch.num_rows());
                for literal in &literals {
                    let literal = try_cast_literal(literal, left.data_type())?;
                    acc = and(&acc, &neq(&left, literal.as_ref())?)?
                }

                Ok(acc)
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_true()
        }
    }
}

/// SQE PATCH: hash-set membership evaluator for `IN` predicates over the
/// common join-key types. One set build, then O(1) per row, versus the or(eq)
/// fallback's one full-column comparison kernel per list element per batch.
///
/// Null semantics match the fallback: a null input row yields null (the
/// or-chain over `eq(null, lit)` is null), so compositions like NOT keep their
/// three-valued behaviour.
pub(super) enum MembershipSet {
    Int32(FnvHashSet<i32>),
    Int64(FnvHashSet<i64>),
    Date32(FnvHashSet<i32>),
    Utf8(FnvHashSet<String>),
}

impl MembershipSet {
    /// Build from literals aligned to `column_type` via [`try_cast_literal`].
    /// Returns `None` (caller falls back to the or(eq) loop) for types without
    /// a fast path or when any literal fails to cast or extract.
    pub(super) fn try_build(
        column_type: &DataType,
        literals: &[Arc<dyn ArrowDatum + Send + Sync>],
    ) -> Option<MembershipSet> {
        fn typed_values<A: Array + 'static, T>(
            literals: &[Arc<dyn ArrowDatum + Send + Sync>],
            column_type: &DataType,
            extract: impl Fn(&A, usize) -> T,
        ) -> Option<FnvHashSet<T>>
        where
            T: std::hash::Hash + Eq,
        {
            let mut set = FnvHashSet::default();
            for literal in literals {
                let cast = try_cast_literal(literal, column_type).ok()?;
                let (arr, _) = cast.get();
                let arr = arr.as_any().downcast_ref::<A>()?;
                if arr.len() != 1 || arr.is_null(0) {
                    return None;
                }
                set.insert(extract(arr, 0));
            }
            Some(set)
        }

        use arrow_array::{Date32Array, Int32Array, Int64Array, StringArray};
        match column_type {
            DataType::Int32 => typed_values(literals, column_type, |a: &Int32Array, i| a.value(i))
                .map(MembershipSet::Int32),
            DataType::Int64 => typed_values(literals, column_type, |a: &Int64Array, i| a.value(i))
                .map(MembershipSet::Int64),
            DataType::Date32 => {
                typed_values(literals, column_type, |a: &Date32Array, i| a.value(i))
                    .map(MembershipSet::Date32)
            }
            DataType::Utf8 => typed_values(literals, column_type, |a: &StringArray, i| {
                a.value(i).to_string()
            })
            .map(MembershipSet::Utf8),
            _ => None,
        }
    }

    pub(super) fn eval(&self, column: &dyn Array) -> std::result::Result<BooleanArray, ArrowError> {
        use arrow_array::{Date32Array, Int32Array, Int64Array, StringArray};

        fn check<A: Array + 'static>(
            column: &dyn Array,
            contains: impl Fn(&A, usize) -> bool,
        ) -> std::result::Result<BooleanArray, ArrowError> {
            let arr = column.as_any().downcast_ref::<A>().ok_or_else(|| {
                ArrowError::CastError(format!(
                    "IN membership set built for a different type than column {:?}",
                    column.data_type()
                ))
            })?;
            Ok((0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(contains(arr, i))
                    }
                })
                .collect())
        }

        match self {
            MembershipSet::Int32(set) => check::<Int32Array>(column, |a, i| set.contains(&a.value(i))),
            MembershipSet::Int64(set) => check::<Int64Array>(column, |a, i| set.contains(&a.value(i))),
            MembershipSet::Date32(set) => {
                check::<Date32Array>(column, |a, i| set.contains(&a.value(i)))
            }
            MembershipSet::Utf8(set) => check::<StringArray>(column, |a, i| set.contains(a.value(i))),
        }
    }
}

/// The Arrow type of an array that the Parquet reader reads may not match the exact Arrow type
/// that Iceberg uses for literals - but they are effectively the same logical type,
/// i.e. LargeUtf8 and Utf8 or Utf8View and Utf8 or Utf8View and LargeUtf8.
///
/// The Arrow compute kernels that we use must match the type exactly, so first cast the literal
/// into the type of the batch we read from Parquet before sending it to the compute kernel.
fn try_cast_literal(
    literal: &Arc<dyn ArrowDatum + Send + Sync>,
    column_type: &DataType,
) -> std::result::Result<Arc<dyn ArrowDatum + Send + Sync>, ArrowError> {
    let literal_array = literal.get().0;

    // No cast required
    if literal_array.data_type() == column_type {
        return Ok(Arc::clone(literal));
    }

    let literal_array = cast(literal_array, column_type)?;
    Ok(Arc::new(Scalar::new(literal_array)))
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use arrow_array::{Array, BooleanArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use parquet::schema::parser::parse_message_type;
    use parquet::schema::types::SchemaDescriptor;

    use super::{CollectFieldIdVisitor, PredicateConverter, constant_bool_array};
    use crate::expr::visitors::bound_predicate_visitor::visit;
    use crate::expr::{Bind, Predicate, Reference};
    use crate::spec::{Datum, NestedField, PrimitiveType, Schema, SchemaRef, Type};

    fn table_schema_simple() -> SchemaRef {
        Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_identifier_field_ids(vec![2])
                .with_fields(vec![
                    NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
                    NestedField::optional(4, "qux", Type::Primitive(PrimitiveType::Float)).into(),
                ])
                .build()
                .unwrap(),
        )
    }

    #[test]
    fn test_collect_field_id() {
        let schema = table_schema_simple();
        let expr = Reference::new("qux").is_null();
        let bound_expr = expr.bind(schema, true).unwrap();

        let mut visitor = CollectFieldIdVisitor {
            field_ids: HashSet::default(),
        };
        visit(&mut visitor, &bound_expr).unwrap();

        let mut expected = HashSet::default();
        expected.insert(4_i32);

        assert_eq!(visitor.field_ids, expected);
    }

    #[test]
    fn test_collect_field_id_with_and() {
        let schema = table_schema_simple();
        let expr = Reference::new("qux")
            .is_null()
            .and(Reference::new("baz").is_null());
        let bound_expr = expr.bind(schema, true).unwrap();

        let mut visitor = CollectFieldIdVisitor {
            field_ids: HashSet::default(),
        };
        visit(&mut visitor, &bound_expr).unwrap();

        let mut expected = HashSet::default();
        expected.insert(4_i32);
        expected.insert(3);

        assert_eq!(visitor.field_ids, expected);
    }

    #[test]
    fn test_collect_field_id_with_or() {
        let schema = table_schema_simple();
        let expr = Reference::new("qux")
            .is_null()
            .or(Reference::new("baz").is_null());
        let bound_expr = expr.bind(schema, true).unwrap();

        let mut visitor = CollectFieldIdVisitor {
            field_ids: HashSet::default(),
        };
        visit(&mut visitor, &bound_expr).unwrap();

        let mut expected = HashSet::default();
        expected.insert(4_i32);
        expected.insert(3);

        assert_eq!(visitor.field_ids, expected);
    }

    #[test]
    fn test_constant_bool_array() {
        for len in [0, 8192] {
            let all_true = constant_bool_array(true, len);
            assert_eq!(all_true.len(), len);
            assert_eq!(all_true.null_count(), 0);
            assert!(all_true.iter().all(|v| v == Some(true)));

            let all_false = constant_bool_array(false, len);
            assert_eq!(all_false.len(), len);
            assert_eq!(all_false.null_count(), 0);
            assert!(all_false.iter().all(|v| v == Some(false)));
        }
    }

    fn apply_predicate_to_batch(
        predicate: Predicate,
        schema: SchemaRef,
        batch: RecordBatch,
    ) -> BooleanArray {
        let bound = predicate.bind(schema, true).unwrap();

        // Build a trivial Parquet schema with one float column at field id 4
        let message_type = "
            message schema {
              optional float qux = 4;
            }
        ";
        let parquet_type = parse_message_type(message_type).expect("parse schema");
        let parquet_schema = SchemaDescriptor::new(Arc::new(parquet_type));

        let column_map = HashMap::from([(4i32, 0usize)]);
        let column_indices = vec![0usize];

        let mut converter = PredicateConverter {
            parquet_schema: &parquet_schema,
            column_map: &column_map,
            column_indices: &column_indices,
        };

        let mut predicate_fn = visit(&mut converter, &bound).unwrap();
        predicate_fn(batch).unwrap()
    }

    /// SQE PATCH (sqe#830): a column the file predates reads its
    /// initial-default inside a predicate, so `region = 'eu'` keeps those rows
    /// and `region IS NULL` drops them. A column with no default stays NULL.
    #[test]
    fn test_predicate_converter_missing_column_reads_initial_default() {
        use arrow_array::Float32Array;

        use crate::spec::Literal;

        let schema: SchemaRef = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(4, "qux", Type::Primitive(PrimitiveType::Float)).into(),
                    NestedField::optional(5, "region", Type::Primitive(PrimitiveType::String))
                        .with_initial_default(Literal::string("eu"))
                        .into(),
                    NestedField::optional(6, "note", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])
                .build()
                .unwrap(),
        );
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "qux",
            DataType::Float32,
            true,
        )]));
        let batch = || {
            RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(Float32Array::from(
                vec![Some(1.0f32), None],
            ))])
            .unwrap()
        };
        let all = |predicate: Predicate| {
            let result = apply_predicate_to_batch(predicate, schema.clone(), batch());
            assert_eq!(result.len(), 2);
            result.value(0) && result.value(1)
        };
        let none = |predicate: Predicate| {
            let result = apply_predicate_to_batch(predicate, schema.clone(), batch());
            !result.value(0) && !result.value(1)
        };

        assert!(all(Reference::new("region").equal_to(Datum::string("eu"))));
        assert!(none(Reference::new("region").equal_to(Datum::string("us"))));
        assert!(all(Reference::new("region").starts_with(Datum::string("e"))));
        assert!(none(Reference::new("region").is_null()));
        assert!(all(Reference::new("region").is_not_null()));
        assert!(all(Reference::new("note").is_null()), "no default: NULL");
    }

    #[test]
    fn test_predicate_converter_nan() {
        use arrow_array::Float32Array;

        let schema = table_schema_simple();
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "qux",
            DataType::Float32,
            true,
        )]));
        let values = vec![Some(1.0f32), Some(f32::NAN), None, Some(0.0f32)];

        // is_nan: non-null-propagating per Java's implementation - NULL → false
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(Float32Array::from(
            values.clone(),
        ))])
        .unwrap();
        let result =
            apply_predicate_to_batch(Reference::new("qux").is_nan(), schema.clone(), batch);
        assert_eq!(
            [
                result.value(0),
                result.value(1),
                result.value(2),
                result.value(3)
            ],
            [false, true, false, false]
        );
        assert!(!result.is_null(2));

        // not_nan: non-null-propagating per Java's implementation - NULL → true
        let batch =
            RecordBatch::try_new(arrow_schema, vec![Arc::new(Float32Array::from(values))]).unwrap();
        let result = apply_predicate_to_batch(Reference::new("qux").is_not_nan(), schema, batch);
        assert_eq!(
            [
                result.value(0),
                result.value(1),
                result.value(2),
                result.value(3)
            ],
            [true, false, true, true]
        );
        assert!(!result.is_null(2));
    }

    /// SQE PATCH: the IN-predicate hash-set fast path must match the or(eq)
    /// fallback's semantics exactly: member rows true, non-member rows false,
    /// null rows null, and unsupported types must decline.
    #[test]
    fn membership_set_matches_or_eq_semantics() {
        use arrow_array::{Datum as ArrowDatum, Int32Array, Int64Array, Scalar};

        use super::MembershipSet;

        let literals: Vec<Arc<dyn ArrowDatum + Send + Sync>> = vec![
            Arc::new(Scalar::new(Int32Array::from(vec![2]))),
            Arc::new(Scalar::new(Int32Array::from(vec![4]))),
        ];

        let set =
            MembershipSet::try_build(&DataType::Int32, &literals).expect("Int32 has a fast path");
        let column = Int32Array::from(vec![Some(1), Some(2), None, Some(4)]);
        let result = set.eval(&column).unwrap();
        assert_eq!(result.len(), 4);
        assert!(!result.value(0) && !result.is_null(0), "1 not in set");
        assert!(result.value(1), "2 in set");
        assert!(result.is_null(2), "null input stays null, not false");
        assert!(result.value(3), "4 in set");

        // Int64 column with Int32 literals: try_cast_literal widens.
        let set = MembershipSet::try_build(&DataType::Int64, &literals)
            .expect("literals cast to the column type");
        let result = set.eval(&Int64Array::from(vec![Some(2), Some(3)])).unwrap();
        assert!(result.value(0) && !result.value(1));

        assert!(
            MembershipSet::try_build(&DataType::Float64, &literals).is_none(),
            "unsupported types fall back to the or(eq) loop"
        );
    }
}
