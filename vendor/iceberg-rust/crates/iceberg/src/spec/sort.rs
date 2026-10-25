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

/*!
 * Sorting
 */
use core::fmt;
use std::collections::BTreeMap;
use std::fmt::Formatter;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use super::partition::{raw_sources, resolve_sources, UnknownTransform};
use super::transform::Transform;
use crate::error::Result;
use crate::spec::{PrimitiveType, Schema, Type};
use crate::{Error, ErrorKind};

/// Reference to [`SortOrder`].
pub type SortOrderRef = Arc<SortOrder>;
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Copy, Clone)]
/// Sort direction in a partition, either ascending or descending
pub enum SortDirection {
    /// Ascending
    #[serde(rename = "asc")]
    Ascending,
    /// Descending
    #[serde(rename = "desc")]
    Descending,
}

impl fmt::Display for SortDirection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match *self {
            SortDirection::Ascending => write!(f, "ascending"),
            SortDirection::Descending => write!(f, "descending"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Copy, Clone)]
/// Describes the order of null values when sorted.
pub enum NullOrder {
    #[serde(rename = "nulls-first")]
    /// Nulls are stored first
    First,
    #[serde(rename = "nulls-last")]
    /// Nulls are stored last
    Last,
}

impl fmt::Display for NullOrder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match *self {
            NullOrder::First => write!(f, "first"),
            NullOrder::Last => write!(f, "last"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, TypedBuilder)]
#[serde(rename_all = "kebab-case")]
/// Entry for every column that is to be sorted
pub struct SortField {
    /// A source column id from the table’s schema
    pub source_id: i32,
    /// A transform that is used to produce values to be sorted on from the source column.
    pub transform: Transform,
    /// A sort direction, that can only be either asc or desc
    pub direction: SortDirection,
    /// A null order that describes the order of null values when sorted.
    pub null_order: NullOrder,
}

impl fmt::Display for SortField {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SortField {{ source_id: {}, transform: {}, direction: {}, null_order: {} }}",
            self.source_id, self.transform, self.direction, self.null_order
        )
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Builder, Default)]
#[serde(try_from = "SortOrderSerde", into = "SortOrderSerde")]
#[builder(setter(prefix = "with"))]
#[builder(build_fn(skip))]
/// A sort order is defined by a sort order id and a list of sort fields.
/// The order of the sort fields within the list defines the order in which the sort is applied to the data.
pub struct SortOrder {
    /// Identifier for SortOrder, order_id `0` is no sort order.
    #[builder(default)]
    pub order_id: i64,
    /// Details of the sort
    #[builder(setter(each(name = "with_sort_field")), default)]
    pub fields: Vec<SortField>,
    /// SQE PATCH (sqe#864): the transform name and every source id of each
    /// field with a [`Transform::Unknown`] transform, keyed by its position in
    /// `fields`, so the order is written back exactly as it was read.
    #[builder(setter(skip))]
    unknown_transforms: BTreeMap<usize, UnknownTransform>,
}

/// One sort field as JSON, before its transform is resolved.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct RawSortField {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source_id: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source_ids: Option<Vec<i32>>,
    transform: String,
    direction: SortDirection,
    null_order: NullOrder,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct SortOrderSerde {
    order_id: i64,
    fields: Vec<RawSortField>,
}

impl TryFrom<SortOrderSerde> for SortOrder {
    type Error = String;

    fn try_from(value: SortOrderSerde) -> std::result::Result<Self, Self::Error> {
        let mut fields = Vec::with_capacity(value.fields.len());
        let mut unknown_transforms = BTreeMap::new();
        for (pos, f) in value.fields.into_iter().enumerate() {
            let what = format!("sort field {pos} of sort order {}", value.order_id);
            let (source_id, transform, unknown) =
                resolve_sources(&what, f.source_id, f.source_ids, &f.transform)?;
            if let Some(unknown) = unknown {
                unknown_transforms.insert(pos, unknown);
            }
            fields.push(SortField {
                source_id,
                transform,
                direction: f.direction,
                null_order: f.null_order,
            });
        }
        Ok(SortOrder {
            order_id: value.order_id,
            fields,
            unknown_transforms,
        })
    }
}

impl From<SortOrder> for SortOrderSerde {
    fn from(order: SortOrder) -> Self {
        let fields = order
            .fields
            .iter()
            .enumerate()
            .map(|(pos, f)| {
                let (source_id, source_ids, transform) =
                    raw_sources(f.source_id, f.transform, order.unknown_transforms.get(&pos));
                RawSortField {
                    source_id,
                    source_ids,
                    transform,
                    direction: f.direction,
                    null_order: f.null_order,
                }
            })
            .collect();
        SortOrderSerde {
            order_id: order.order_id,
            fields,
        }
    }
}

impl SortOrder {
    pub(crate) const UNSORTED_ORDER_ID: i64 = 0;

    /// Create sort order builder
    pub fn builder() -> SortOrderBuilder {
        SortOrderBuilder::default()
    }

    /// Create an unbound unsorted order
    pub fn unsorted_order() -> SortOrder {
        SortOrder {
            order_id: SortOrder::UNSORTED_ORDER_ID,
            fields: Vec::new(),
            unknown_transforms: BTreeMap::new(),
        }
    }

    /// SQE PATCH (sqe#864): the transform name and source ids of the sort
    /// field at `position` if its transform is unknown to this reader.
    pub fn unknown_transform(&self, position: usize) -> Option<&UnknownTransform> {
        self.unknown_transforms.get(&position)
    }

    /// SQE PATCH (sqe#864): build a sort order from its parts, as a struct
    /// literal did before the order carried unknown-transform records.
    pub fn new(order_id: i64, fields: Vec<SortField>) -> SortOrder {
        SortOrder {
            order_id,
            fields,
            unknown_transforms: BTreeMap::new(),
        }
    }

    /// Returns true if the sort order is unsorted.
    ///
    /// A [`SortOrder`] is unsorted if it has no sort fields.
    pub fn is_unsorted(&self) -> bool {
        self.fields.is_empty()
    }

    /// Set the order id for the sort order
    pub fn with_order_id(self, order_id: i64) -> SortOrder {
        SortOrder { order_id, ..self }
    }
}

impl SortOrderBuilder {
    /// Creates a new unbound sort order.
    pub fn build_unbound(&self) -> Result<SortOrder> {
        let fields = self.fields.clone().unwrap_or_default();
        match (self.order_id, fields.as_slice()) {
            (Some(SortOrder::UNSORTED_ORDER_ID) | None, []) => Ok(SortOrder::unsorted_order()),
            (_, []) => Err(Error::new(
                ErrorKind::Unexpected,
                format!("Unsorted order ID must be {}", SortOrder::UNSORTED_ORDER_ID),
            )),
            (Some(SortOrder::UNSORTED_ORDER_ID), [..]) => Err(Error::new(
                ErrorKind::Unexpected,
                format!(
                    "Sort order ID {} is reserved for unsorted order",
                    SortOrder::UNSORTED_ORDER_ID
                ),
            )),
            (maybe_order_id, [..]) => {
                Ok(SortOrder::new(maybe_order_id.unwrap_or(1), fields.to_vec()))
            }
        }
    }

    /// Creates a new bound sort order.
    pub fn build(&self, schema: &Schema) -> Result<SortOrder> {
        let unbound_sort_order = self.build_unbound()?;
        SortOrderBuilder::check_compatibility(unbound_sort_order, schema)
    }

    /// Returns the given sort order if it is compatible with the given schema
    fn check_compatibility(sort_order: SortOrder, schema: &Schema) -> Result<SortOrder> {
        let sort_fields = &sort_order.fields;
        for sort_field in sort_fields {
            match schema.field_by_id(sort_field.source_id) {
                None => {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Cannot find source column for sort field: {sort_field}"),
                    ));
                }
                // SQE PATCH (sqe#864): a reader cannot type-check a transform
                // it does not know, and a multi-argument field holds only its
                // first source here. Readers ignore such a field.
                Some(_) if sort_field.transform == Transform::Unknown => {}
                Some(source_field) => {
                    let source_type = source_field.field_type.as_ref();

                    if !source_type.is_primitive() {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!("Cannot sort by non-primitive source field: {source_type}"),
                        ));
                    }

                    if matches!(
                        source_type,
                        Type::Primitive(PrimitiveType::Geometry(_) | PrimitiveType::Geography(_))
                    ) {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!(
                                "Cannot sort by geospatial source field: {}",
                                source_field.name
                            ),
                        ));
                    }

                    let field_transform = sort_field.transform;
                    field_transform.result_type(source_type)?;
                }
            }
        }

        Ok(sort_order)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{GeometryType, ListType, NestedField, PrimitiveType, Type};

    #[test]
    fn test_sort_field() {
        let spec = r#"
        {
            "transform": "bucket[4]",
            "source-id": 3,
            "direction": "desc",
            "null-order": "nulls-last"
         }
        "#;

        let field: SortField = serde_json::from_str(spec).unwrap();
        assert_eq!(Transform::Bucket(4), field.transform);
        assert_eq!(3, field.source_id);
        assert_eq!(SortDirection::Descending, field.direction);
        assert_eq!(NullOrder::Last, field.null_order);
    }

    #[test]
    fn test_sort_order() {
        let spec = r#"
        {
        "order-id": 1,
        "fields": [ {
            "transform": "identity",
            "source-id": 2,
            "direction": "asc",
            "null-order": "nulls-first"
         }, {
            "transform": "bucket[4]",
            "source-id": 3,
            "direction": "desc",
            "null-order": "nulls-last"
         } ]
        }
        "#;

        let order: SortOrder = serde_json::from_str(spec).unwrap();
        assert_eq!(Transform::Identity, order.fields[0].transform);
        assert_eq!(2, order.fields[0].source_id);
        assert_eq!(SortDirection::Ascending, order.fields[0].direction);
        assert_eq!(NullOrder::First, order.fields[0].null_order);

        assert_eq!(Transform::Bucket(4), order.fields[1].transform);
        assert_eq!(3, order.fields[1].source_id);
        assert_eq!(SortDirection::Descending, order.fields[1].direction);
        assert_eq!(NullOrder::Last, order.fields[1].null_order);
    }

    #[test]
    fn test_build_unbound_should_return_err_if_unsorted_order_does_not_have_an_order_id_of_zero() {
        assert_eq!(
            SortOrder::builder()
                .with_order_id(1)
                .build_unbound()
                .expect_err("Expected an Err value")
                .message(),
            "Unsorted order ID must be 0"
        )
    }

    #[test]
    fn test_build_unbound_should_return_err_if_order_id_equals_zero_is_used_for_anything_other_than_unsorted_order()
     {
        assert_eq!(
            SortOrder::builder()
                .with_order_id(SortOrder::UNSORTED_ORDER_ID)
                .with_sort_field(
                    SortField::builder()
                        .source_id(2)
                        .direction(SortDirection::Ascending)
                        .null_order(NullOrder::First)
                        .transform(Transform::Identity)
                        .build()
                )
                .build_unbound()
                .expect_err("Expected an Err value")
                .message(),
            "Sort order ID 0 is reserved for unsorted order"
        )
    }

    #[test]
    fn test_build_unbound_returns_correct_default_order_id_for_no_fields() {
        assert_eq!(
            SortOrder::builder()
                .build_unbound()
                .expect("Expected an Ok value")
                .order_id,
            SortOrder::UNSORTED_ORDER_ID
        )
    }

    #[test]
    fn test_build_unbound_returns_correct_default_order_id_for_fields() {
        let sort_field = SortField::builder()
            .source_id(2)
            .direction(SortDirection::Ascending)
            .null_order(NullOrder::First)
            .transform(Transform::Identity)
            .build();
        assert_ne!(
            SortOrder::builder()
                .with_sort_field(sort_field.clone())
                .build_unbound()
                .expect("Expected an Ok value")
                .order_id,
            SortOrder::UNSORTED_ORDER_ID
        )
    }

    #[test]
    fn test_build_unbound_should_return_unsorted_sort_order() {
        assert_eq!(
            SortOrder::builder()
                .with_order_id(SortOrder::UNSORTED_ORDER_ID)
                .build_unbound()
                .expect("Expected an Ok value"),
            SortOrder::unsorted_order()
        )
    }

    #[test]
    fn test_build_unbound_should_return_sort_order_with_given_order_id_and_sort_fields() {
        let sort_field = SortField::builder()
            .source_id(2)
            .direction(SortDirection::Ascending)
            .null_order(NullOrder::First)
            .transform(Transform::Identity)
            .build();

        assert_eq!(
            SortOrder::builder()
                .with_order_id(2)
                .with_sort_field(sort_field.clone())
                .build_unbound()
                .expect("Expected an Ok value"),
            SortOrder::new(2, vec![sort_field])
        )
    }

    #[test]
    fn test_build_unbound_should_return_sort_order_with_given_sort_fields_and_defaults_to_1_if_missing_an_order_id()
     {
        let sort_field = SortField::builder()
            .source_id(2)
            .direction(SortDirection::Ascending)
            .null_order(NullOrder::First)
            .transform(Transform::Identity)
            .build();

        assert_eq!(
            SortOrder::builder()
                .with_sort_field(sort_field.clone())
                .build_unbound()
                .expect("Expected an Ok value"),
            SortOrder::new(1, vec![sort_field])
        )
    }

    #[test]
    fn test_build_should_return_err_if_sort_order_field_is_not_present_in_schema() {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "foo", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();

        let sort_order_builder_result = SortOrder::builder()
            .with_sort_field(
                SortField::builder()
                    .source_id(2)
                    .direction(SortDirection::Ascending)
                    .null_order(NullOrder::First)
                    .transform(Transform::Identity)
                    .build(),
            )
            .build(&schema);

        let err = sort_order_builder_result.expect_err("Expected an Err value");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            err.message(),
            "Cannot find source column for sort field: SortField { source_id: 2, transform: identity, direction: ascending, null_order: first }"
        );
    }

    #[test]
    fn test_build_should_return_err_if_source_field_is_not_a_primitive_type() {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(
                    1,
                    "foo",
                    Type::List(ListType {
                        element_field: NestedField::list_element(
                            2,
                            Type::Primitive(PrimitiveType::String),
                            true,
                        )
                        .into(),
                    }),
                )
                .into(),
            ])
            .build()
            .unwrap();

        let sort_order_builder_result = SortOrder::builder()
            .with_sort_field(
                SortField::builder()
                    .source_id(1)
                    .direction(SortDirection::Ascending)
                    .null_order(NullOrder::First)
                    .transform(Transform::Identity)
                    .build(),
            )
            .build(&schema);

        let err = sort_order_builder_result.expect_err("Expected an Err value");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            err.message(),
            "Cannot sort by non-primitive source field: list"
        );
    }

    #[test]
    fn test_build_should_return_err_if_source_field_is_variant() {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::optional(1, "v", Type::Variant(crate::spec::VariantType)).into(),
            ])
            .build()
            .unwrap();

        let sort_order_builder_result = SortOrder::builder()
            .with_sort_field(
                SortField::builder()
                    .source_id(1)
                    .direction(SortDirection::Ascending)
                    .null_order(NullOrder::First)
                    .transform(Transform::Identity)
                    .build(),
            )
            .build(&schema);

        let err = sort_order_builder_result.expect_err("Expected an Err value");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            err.message(),
            "Cannot sort by non-primitive source field: variant"
        );
    }

    #[test]
    fn test_build_should_reject_geospatial_source_fields() {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(
                    1,
                    "geom",
                    Type::Primitive(PrimitiveType::Geometry(GeometryType::default())),
                )
                .into(),
            ])
            .build()
            .unwrap();

        let err = SortOrder::builder()
            .with_sort_field(
                SortField::builder()
                    .source_id(1)
                    .direction(SortDirection::Ascending)
                    .null_order(NullOrder::First)
                    .transform(Transform::Void)
                    .build(),
            )
            .build(&schema)
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            err.message(),
            "Cannot sort by geospatial source field: geom"
        );
    }

    #[test]
    fn test_build_should_return_err_if_source_field_type_is_not_supported_by_transform() {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "foo", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();

        let sort_order_builder_result = SortOrder::builder()
            .with_sort_field(
                SortField::builder()
                    .source_id(1)
                    .direction(SortDirection::Ascending)
                    .null_order(NullOrder::First)
                    .transform(Transform::Year)
                    .build(),
            )
            .build(&schema);

        let err = sort_order_builder_result.expect_err("Expected an Err value");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            err.message(),
            "int is not a valid input type of year transform"
        );
    }

    #[test]
    fn test_build_should_return_valid_sort_order() {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();

        let sort_field = SortField::builder()
            .source_id(2)
            .direction(SortDirection::Ascending)
            .null_order(NullOrder::First)
            .transform(Transform::Identity)
            .build();

        let sort_order_builder_result = SortOrder::builder()
            .with_sort_field(sort_field.clone())
            .build(&schema);

        assert_eq!(
            sort_order_builder_result.expect("Expected an Ok value"),
            SortOrder::new(1, vec![sort_field])
        )
    }
}
