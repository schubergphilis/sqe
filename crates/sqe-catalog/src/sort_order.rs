//! Sort order detection from Iceberg table metadata.
//!
//! Reads the table's default sort order and converts Iceberg `SortField`s
//! to DataFusion `PhysicalSortExpr`s. When the sort order uses only
//! identity transforms on projected columns, the scan output is declared
//! as pre-sorted via `EquivalenceProperties`, letting DataFusion use
//! `SortPreservingMergeExec` instead of a full sort.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalSortExpr};
use iceberg::spec::{NullOrder, SortDirection, SortOrder, Transform};
use tracing::debug;

/// Convert an Iceberg `SortOrder` to DataFusion `PhysicalSortExpr`s.
///
/// Only identity transforms are supported -- sort fields with bucket,
/// truncate, or other transforms are not convertible to simple column
/// sort expressions and are skipped.
///
/// Returns `None` if the sort order is unsorted or no fields could be converted.
pub fn iceberg_sort_to_physical(
    sort_order: &SortOrder,
    iceberg_schema: &iceberg::spec::Schema,
    arrow_schema: &SchemaRef,
) -> Option<Vec<PhysicalSortExpr>> {
    if sort_order.is_unsorted() {
        return None;
    }

    let mut exprs = Vec::new();
    for field in &sort_order.fields {
        // Only identity transforms map directly to column sorts
        if field.transform != Transform::Identity {
            debug!(
                source_id = field.source_id,
                transform = %field.transform,
                "Skipping non-identity sort field"
            );
            break; // Prefix must be contiguous
        }

        // Look up the column name from the Iceberg schema via field ID
        let iceberg_field = match iceberg_schema.field_by_id(field.source_id) {
            Some(f) => f,
            None => {
                debug!(
                    source_id = field.source_id,
                    "Sort field ID not found in schema"
                );
                break;
            }
        };

        // Find the corresponding column index in the Arrow schema
        let col_idx = match arrow_schema
            .fields()
            .iter()
            .position(|f| f.name() == &iceberg_field.name)
        {
            Some(idx) => idx,
            None => {
                debug!(
                    column = %iceberg_field.name,
                    "Sort column not in projected schema, stopping"
                );
                break;
            }
        };

        let ascending = matches!(field.direction, SortDirection::Ascending);
        let nulls_first = matches!(field.null_order, NullOrder::First);

        exprs.push(PhysicalSortExpr {
            expr: Arc::new(Column::new(&iceberg_field.name, col_idx)),
            options: arrow::compute::SortOptions {
                descending: !ascending,
                nulls_first,
            },
        });
    }

    if exprs.is_empty() {
        None
    } else {
        debug!(
            sort_exprs = exprs.len(),
            "Detected pre-sorted output from Iceberg sort order"
        );
        Some(exprs)
    }
}

/// Build `EquivalenceProperties` that declare the output as sorted
/// according to the given physical sort expressions.
pub fn equivalence_with_sort(
    schema: SchemaRef,
    sort_exprs: Vec<PhysicalSortExpr>,
) -> EquivalenceProperties {
    let mut eq = EquivalenceProperties::new(schema);
    if !sort_exprs.is_empty() {
        eq.add_orderings(vec![sort_exprs]);
    }
    eq
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use iceberg::spec::{NestedField, NullOrder, PrimitiveType, SortDirection, SortField, Type};

    fn make_iceberg_schema() -> iceberg::spec::Schema {
        iceberg::spec::Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "name",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::optional(
                    3,
                    "ts",
                    Type::Primitive(PrimitiveType::Timestamptz),
                )),
            ])
            .build()
            .unwrap()
    }

    fn make_arrow_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("ts", DataType::Int64, true),
        ]))
    }

    #[test]
    fn test_unsorted_returns_none() {
        let sort_order = SortOrder::unsorted_order();
        let result =
            iceberg_sort_to_physical(&sort_order, &make_iceberg_schema(), &make_arrow_schema());
        assert!(result.is_none());
    }

    #[test]
    fn test_identity_sort_ascending() {
        let sort_order = SortOrder {
            order_id: 1,
            fields: vec![SortField {
                source_id: 1,
                transform: Transform::Identity,
                direction: SortDirection::Ascending,
                null_order: NullOrder::Last,
            }],
        };
        let exprs =
            iceberg_sort_to_physical(&sort_order, &make_iceberg_schema(), &make_arrow_schema())
                .unwrap();
        assert_eq!(exprs.len(), 1);
        assert!(!exprs[0].options.descending);
        assert!(!exprs[0].options.nulls_first);
    }

    #[test]
    fn test_identity_sort_descending_nulls_first() {
        let sort_order = SortOrder {
            order_id: 1,
            fields: vec![SortField {
                source_id: 2,
                transform: Transform::Identity,
                direction: SortDirection::Descending,
                null_order: NullOrder::First,
            }],
        };
        let exprs =
            iceberg_sort_to_physical(&sort_order, &make_iceberg_schema(), &make_arrow_schema())
                .unwrap();
        assert_eq!(exprs.len(), 1);
        assert!(exprs[0].options.descending);
        assert!(exprs[0].options.nulls_first);
    }

    #[test]
    fn test_multi_column_sort() {
        let sort_order = SortOrder {
            order_id: 1,
            fields: vec![
                SortField {
                    source_id: 1,
                    transform: Transform::Identity,
                    direction: SortDirection::Ascending,
                    null_order: NullOrder::Last,
                },
                SortField {
                    source_id: 2,
                    transform: Transform::Identity,
                    direction: SortDirection::Descending,
                    null_order: NullOrder::First,
                },
            ],
        };
        let exprs =
            iceberg_sort_to_physical(&sort_order, &make_iceberg_schema(), &make_arrow_schema())
                .unwrap();
        assert_eq!(exprs.len(), 2);
    }

    #[test]
    fn test_non_identity_transform_stops_prefix() {
        let sort_order = SortOrder {
            order_id: 1,
            fields: vec![
                SortField {
                    source_id: 1,
                    transform: Transform::Identity,
                    direction: SortDirection::Ascending,
                    null_order: NullOrder::Last,
                },
                SortField {
                    source_id: 3,
                    transform: Transform::Day,
                    direction: SortDirection::Ascending,
                    null_order: NullOrder::Last,
                },
            ],
        };
        let exprs =
            iceberg_sort_to_physical(&sort_order, &make_iceberg_schema(), &make_arrow_schema())
                .unwrap();
        // Only the first field (identity) should be included
        assert_eq!(exprs.len(), 1);
    }

    #[test]
    fn test_equivalence_with_sort() {
        let schema = make_arrow_schema();
        let sort_exprs = vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("id", 0)),
            options: arrow::compute::SortOptions {
                descending: false,
                nulls_first: false,
            },
        }];
        let eq = equivalence_with_sort(schema, sort_exprs);
        // The equivalence properties should have orderings
        assert!(!eq.oeq_class().is_empty());
    }
}
