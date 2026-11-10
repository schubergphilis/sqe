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

//! SQE PATCH (sqe#870): read only the shredded fields of a variant that a
//! query reads.
//!
//! A shredded variant column is a group of `metadata`, the residual `value`
//! and a `typed_value` group with one child per shredded object field. A
//! query that reads the variant only through paths such as `$.a` and `$.b.c`
//! needs `metadata`, the residual `value` and the `a` and `b` subtrees of
//! `typed_value`; every other `typed_value` field is skipped.
//!
//! The residual `value` is always read. It holds the object fields that were
//! not shredded, a variant that is not an object, and, when a row's
//! `typed_value` is NULL, the whole variant even if it is an object: the
//! shredding spec allows that, and SQE's writer does it for a row whose
//! field has another type than the shredded one. Reading `value` keeps
//! every such row right; for well-shredded data it is mostly NULL.

use std::collections::{BTreeSet, HashMap};

use parquet::arrow::ProjectionMask;
use parquet::basic::{ConvertedType, LogicalType};
use parquet::schema::types::{SchemaDescriptor, Type};

/// The object fields a query reads from each variant column, keyed by the
/// column's Iceberg field id: the first step of every path it reads.
pub type VariantFieldHints = HashMap<i32, BTreeSet<String>>;

/// `mask` without the shredded variant fields no hinted path reads, or
/// `None` when no column narrows. A column narrows only when it is a
/// top-level variant group with a field id in `hints`, its `typed_value` is
/// an object group, and at least one hinted name is one of that group's
/// fields; otherwise it is read whole. A hinted name that is not shredded in
/// this file lives in `value`, which is always read.
pub fn narrow_variant_projection(
    mask: &ProjectionMask,
    schema: &SchemaDescriptor,
    hints: &VariantFieldHints,
) -> Option<ProjectionMask> {
    if hints.is_empty() {
        return None;
    }
    let roots = schema.root_schema().get_fields();
    let mut dropped = vec![false; schema.num_columns()];
    let mut narrowed = false;
    for (root_idx, root) in roots.iter().enumerate() {
        let info = root.get_basic_info();
        if !info.has_id() {
            continue;
        }
        let Some(names) = hints.get(&info.id()) else {
            continue;
        };
        if !shreds_any_field(root, names) {
            continue;
        }
        for leaf in 0..schema.num_columns() {
            if schema.get_column_root_idx(leaf) != root_idx || !mask.leaf_included(leaf) {
                continue;
            }
            let column = schema.column(leaf);
            let parts = column.path().parts();
            let keep = match parts.get(1).map(String::as_str) {
                Some("typed_value") => parts.get(2).is_some_and(|name| names.contains(name)),
                _ => true,
            };
            if !keep {
                dropped[leaf] = true;
                narrowed = true;
            }
        }
    }
    narrowed.then(|| {
        ProjectionMask::leaves(
            schema,
            (0..schema.num_columns()).filter(|&leaf| mask.leaf_included(leaf) && !dropped[leaf]),
        )
    })
}

/// True when `root` is variant storage whose `typed_value` is an object
/// group holding at least one name in `names`, so the narrowed group keeps
/// a `typed_value` and still reads as shredded storage.
fn shreds_any_field(root: &Type, names: &BTreeSet<String>) -> bool {
    if names.is_empty() || !root.is_group() {
        return false;
    }
    let child = |name: &str| {
        root.get_fields()
            .iter()
            .find(|f| f.name() == name)
            .map(|f| f.as_ref())
    };
    if child("metadata").is_none_or(|m| m.is_group()) {
        return false;
    }
    let Some(typed_value) = child("typed_value") else {
        return false;
    };
    if !typed_value.is_group() {
        return false;
    }
    let info = typed_value.get_basic_info();
    let is_list_or_map = matches!(
        info.logical_type_ref(),
        Some(LogicalType::List | LogicalType::Map)
    ) || matches!(
        info.converted_type(),
        ConvertedType::LIST | ConvertedType::MAP | ConvertedType::MAP_KEY_VALUE
    );
    if is_list_or_map {
        return false;
    }
    names.iter().any(|name| {
        typed_value
            .get_fields()
            .iter()
            .any(|f| f.name() == name.as_str())
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parquet::schema::parser::parse_message_type;

    use super::*;

    fn schema(message: &str) -> SchemaDescriptor {
        SchemaDescriptor::new(Arc::new(parse_message_type(message).unwrap()))
    }

    const SHREDDED: &str = "
        message m {
          required int32 id = 1;
          optional group v = 2 {
            required binary metadata;
            optional binary value;
            optional group typed_value {
              required group a {
                optional binary value;
                optional int32 typed_value;
              }
              required group b {
                optional binary value;
                optional group typed_value {
                  required group c {
                    optional binary value;
                    optional binary typed_value (STRING);
                  }
                }
              }
            }
          }
        }";

    fn hints(names: &[&str]) -> VariantFieldHints {
        HashMap::from([(2, names.iter().map(|n| n.to_string()).collect())])
    }

    fn included(mask: &ProjectionMask, schema: &SchemaDescriptor) -> Vec<String> {
        (0..schema.num_columns())
            .filter(|&i| mask.leaf_included(i))
            .map(|i| schema.column(i).path().string())
            .collect()
    }

    #[test]
    fn a_hinted_field_reads_metadata_value_and_its_subtree_only() {
        let schema = schema(SHREDDED);
        let mask = narrow_variant_projection(&ProjectionMask::all(), &schema, &hints(&["b"]))
            .expect("narrows");
        assert_eq!(included(&mask, &schema), vec![
            "id",
            "v.metadata",
            "v.value",
            "v.typed_value.b.value",
            "v.typed_value.b.typed_value.c.value",
            "v.typed_value.b.typed_value.c.typed_value",
        ]);
    }

    #[test]
    fn leaves_outside_the_mask_stay_out() {
        let schema = schema(SHREDDED);
        let base = ProjectionMask::leaves(&schema, 1..schema.num_columns());
        let mask = narrow_variant_projection(&base, &schema, &hints(&["a"])).expect("narrows");
        assert_eq!(included(&mask, &schema), vec![
            "v.metadata",
            "v.value",
            "v.typed_value.a.value",
            "v.typed_value.a.typed_value",
        ]);
    }

    #[test]
    fn an_unshredded_name_is_read_from_the_residual() {
        let schema = schema(SHREDDED);
        // `z` is not shredded here, so it is in `value`, which is read.
        let mask = narrow_variant_projection(&ProjectionMask::all(), &schema, &hints(&["a", "z"]))
            .expect("narrows");
        assert_eq!(included(&mask, &schema), vec![
            "id",
            "v.metadata",
            "v.value",
            "v.typed_value.a.value",
            "v.typed_value.a.typed_value",
        ]);
        // No hinted name shredded: no `typed_value` field would remain.
        assert!(narrow_variant_projection(&ProjectionMask::all(), &schema, &hints(&["z"])).is_none());
    }

    #[test]
    fn an_unshredded_or_array_variant_is_read_whole() {
        let unshredded = schema(
            "message m { optional group v = 2 { required binary metadata; required binary value; } }",
        );
        assert!(narrow_variant_projection(&ProjectionMask::all(), &unshredded, &hints(&["a"])).is_none());
        let array = schema(
            "message m { optional group v = 2 { required binary metadata; optional binary value;
               optional group typed_value (LIST) { repeated group list { required group element {
                 optional binary value; optional int32 typed_value; } } } } }",
        );
        assert!(narrow_variant_projection(&ProjectionMask::all(), &array, &hints(&["list"])).is_none());
        let primitive = schema(
            "message m { optional group v = 2 { required binary metadata; optional binary value;
               optional int32 typed_value; } }",
        );
        assert!(narrow_variant_projection(&ProjectionMask::all(), &primitive, &hints(&["a"])).is_none());
    }

    #[test]
    fn a_column_without_hints_is_untouched() {
        let schema = schema(SHREDDED);
        let other = HashMap::from([(7, BTreeSet::from(["a".to_string()]))]);
        assert!(narrow_variant_projection(&ProjectionMask::all(), &schema, &other).is_none());
    }
}
