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

//! SQE PATCH (sqe#892): the schema, partition spec and sort order half of an
//! atomic `CREATE OR REPLACE TABLE ... AS SELECT`.
//!
//! Iceberg Java does a replace with a replace transaction on the existing
//! table. iceberg-rust has none, so a replace whose columns or partitioning
//! changed used to create a staged table and swap it in with DROP + RENAME,
//! leaving a window in which the table did not exist. This action makes the
//! new shape current on the existing table instead. Placed before an
//! `overwrite_files` action in the same [`Transaction`], it turns the replace
//! into one commit: the overwrite runs against the evolved table, so its
//! manifests use the new schema and partition spec.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::spec::{
    FormatVersion, ListType, MAIN_BRANCH, MapType, NestedField, NestedFieldRef,
    PartitionSpecBuilder, PrimitiveType, Schema, SchemaId, SortOrder, StructType, TableMetadata,
    Transform, Type, UnboundPartitionSpec,
};
use crate::table::Table;
use crate::transaction::Transaction;
use crate::transaction::action::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, Result, TableRequirement, TableUpdate};

/// The ids of a table's shape when a replace was planned.
///
/// The replace writes its data files against the schema and spec ids it
/// predicts from these. When the table moves to other ids before the commit,
/// those files no longer fit and the commit is refused rather than rebased.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShapePins {
    /// The table's UUID.
    pub uuid: Uuid,
    /// The current schema id.
    pub current_schema_id: SchemaId,
    /// The last assigned column id.
    pub last_column_id: i32,
    /// The default partition spec id.
    pub default_spec_id: i32,
    /// The last assigned partition field id.
    pub last_partition_id: i32,
}

impl ShapePins {
    /// The pins of `metadata` as it is now.
    pub fn of(metadata: &TableMetadata) -> Self {
        Self {
            uuid: metadata.uuid(),
            current_schema_id: metadata.current_schema_id(),
            last_column_id: metadata.last_column_id(),
            default_spec_id: metadata.default_partition_spec_id(),
            last_partition_id: metadata.last_partition_id(),
        }
    }

    /// `Ok` while `metadata` still has these ids. Otherwise an error naming
    /// the concurrent schema change. It is not retryable: the data written
    /// for the planned shape does not fit the table's new one.
    pub fn check(&self, metadata: &TableMetadata) -> Result<()> {
        let now = Self::of(metadata);
        if now == *self {
            return Ok(());
        }
        let what = if now.uuid != self.uuid {
            format!(
                "the table was re-created (uuid {} -> {})",
                self.uuid, now.uuid
            )
        } else {
            format!(
                "a concurrent schema or partition change moved the table from schema {} / spec {} \
                 (last column id {}, last partition id {}) to schema {} / spec {} (last column id \
                 {}, last partition id {})",
                self.current_schema_id,
                self.default_spec_id,
                self.last_column_id,
                self.last_partition_id,
                now.current_schema_id,
                now.default_spec_id,
                now.last_column_id,
                now.last_partition_id
            )
        };
        Err(Error::new(
            ErrorKind::PreconditionFailed,
            format!("replace refused: {what} while the replace was writing"),
        ))
    }
}

/// Makes `schema`, `spec` and (optionally) a newer format version current on
/// an existing table, in the form an atomic replace needs.
///
/// SQE PATCH (sqe#892). The updates come in the order the metadata builder
/// accepts them: format upgrade, schema, partition spec, sort order. All of
/// them are in this one action, because a `Transaction` builds the metadata
/// after every action and a default spec or sort order that names a dropped
/// column would fail that build in between.
///
/// - A schema equal to an existing one is selected by its id
///   (`SetCurrentSchema(id)`); a new one is `AddSchema` + `SetCurrentSchema(-1)`.
///   The schema's field ids are used as given: the caller assigns them.
/// - Partition field ids are reused by (source id, transform) from every spec
///   of the table, and new ones are assigned above the last partition id, here
///   rather than by the catalog, so the spec the data is written with is the
///   one the catalog stores. A spec compatible with an existing one selects
///   that spec's id.
/// - The default sort order stays when it still binds to the new schema;
///   otherwise the table becomes unsorted. [`Self::with_sort_order`]
///   replaces it instead (sqe#897): an equal existing order is selected by
///   its id, a new one is added with the next free id, and the commit then
///   also asserts the default sort order id it was planned on.
///
/// The requirements pin the [`ShapePins`] the replace was planned on and,
/// with [`Self::with_expected_main`], the `main` head the overwrite removes
/// files from. `commit` checks both against the table it runs on before
/// building anything, so a rebase in `Transaction::do_commit` onto a moved
/// table fails instead of committing data planned for another state:
/// - moved shape ids: `PreconditionFailed`, never retryable;
/// - moved `main` only: `CatalogCommitConflicts`, not retryable here, so the
///   caller reruns its whole attempt (a new remove set) on the new head.
#[derive(Debug, Clone)]
pub struct ReplaceTableShapeAction {
    schema: Schema,
    spec: UnboundPartitionSpec,
    format_version: Option<FormatVersion>,
    pins: ShapePins,
    expected_main: Option<Option<i64>>,
    /// Field id of the caller's schema -> assigned id, for `with_sort_order`.
    ids: HashMap<i32, i32>,
    /// The sort order to make default, its source ids already assigned.
    sort_order: Option<SortOrder>,
    /// The default sort order id when the replace was planned.
    default_sort_order_id: i64,
}

impl ReplaceTableShapeAction {
    /// Plans `schema` and `spec` for `base`.
    ///
    /// The field ids of `schema` only tell its columns apart; they are
    /// reassigned here by name from `base`'s current schema (see
    /// [`assign_ids_by_name`]), and the source ids of `spec` follow them.
    /// `spec` field ids may be unset; see the type docs for how they are
    /// assigned.
    pub fn new(base: &Table, schema: Schema, spec: UnboundPartitionSpec) -> Result<Self> {
        let metadata = base.metadata();
        let last_column_id = metadata.last_column_id();
        let (mut assigned, mut ids) =
            assign_ids_by_name(metadata.current_schema(), &schema, last_column_id)?;
        // A shape the table had before, but not the current one: names it
        // lost since then would get fresh ids above. Take that older schema's
        // ids when they reproduce it exactly, so it is selected by id rather
        // than added again.
        if !metadata
            .schemas_iter()
            .any(|existing| assigned.is_same_schema(existing))
        {
            let mut older: Vec<_> = metadata.schemas_iter().collect();
            older.sort_by_key(|s| std::cmp::Reverse(s.schema_id()));
            for old in older {
                let (candidate, candidate_ids) = assign_ids_by_name(old, &schema, last_column_id)?;
                if candidate.is_same_schema(old) {
                    (assigned, ids) = (candidate, candidate_ids);
                    break;
                }
            }
        }
        let schema = assigned;
        let mut spec = spec;
        for field in &mut spec.fields {
            field.source_id = *ids.get(&field.source_id).ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "partition field {} names source id {}, which is not in the schema",
                        field.name, field.source_id
                    ),
                )
            })?;
        }
        Ok(Self {
            schema,
            spec,
            format_version: None,
            pins: ShapePins::of(metadata),
            expected_main: None,
            ids,
            sort_order: None,
            default_sort_order_id: metadata.default_sort_order_id(),
        })
    }

    /// Make `order` the default sort order in the same commit (sqe#897).
    /// Its source ids name fields of the schema given to [`Self::new`]; they
    /// follow that schema's reassigned ids. An unsorted order makes the table
    /// unsorted.
    pub fn with_sort_order(mut self, order: SortOrder) -> Result<Self> {
        let mut order = order;
        for field in &mut order.fields {
            field.source_id = *self.ids.get(&field.source_id).ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "sort field names source id {}, which is not in the schema",
                        field.source_id
                    ),
                )
            })?;
        }
        self.sort_order = Some(order);
        Ok(self)
    }

    /// The schema this replace makes current, with its assigned field ids.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Upgrade the table to `format_version` in the same commit. A version at
    /// or below the table's is ignored; refusing a downgrade is the caller's
    /// call, made before any data is written.
    pub fn with_format_version(mut self, format_version: FormatVersion) -> Self {
        self.format_version = Some(format_version);
        self
    }

    /// Require `main` to be at `snapshot_id` (`None`: no snapshot yet).
    pub fn with_expected_main(mut self, snapshot_id: Option<i64>) -> Self {
        self.expected_main = Some(snapshot_id);
        self
    }

    /// The shape ids this replace was planned on.
    pub fn pins(&self) -> ShapePins {
        self.pins
    }

    /// The table as it reads after this action: the view the replace writes
    /// its data files through. Fails like the commit would, so a replace the
    /// metadata cannot take is refused before any data is written.
    pub fn evolved_table(&self, table: &Table) -> Result<Table> {
        let (updates, _) = self.plan(table.metadata())?;
        Transaction::update_table_metadata(table.clone(), &updates)
    }

    fn plan(&self, metadata: &TableMetadata) -> Result<(Vec<TableUpdate>, Vec<TableRequirement>)> {
        self.pins.check(metadata)?;
        let main_now = metadata
            .snapshot_for_ref(MAIN_BRANCH)
            .map(|s| s.snapshot_id());
        if let Some(expected) = self.expected_main
            && main_now != expected
        {
            return Err(Error::new(
                ErrorKind::CatalogCommitConflicts,
                format!(
                    "replace planned on main {expected:?}, but a concurrent commit moved it to \
                     {main_now:?}"
                ),
            ));
        }

        let mut updates = Vec::new();
        if let Some(format_version) = self.format_version
            && format_version > metadata.format_version()
        {
            updates.push(TableUpdate::UpgradeFormatVersion { format_version });
        }

        let mut schema = self.schema.clone();
        match metadata
            .schemas_iter()
            .find(|existing| self.schema.is_same_schema(existing))
        {
            Some(existing) => {
                schema = existing.as_ref().clone();
                if existing.schema_id() != metadata.current_schema_id() {
                    updates.push(TableUpdate::SetCurrentSchema {
                        schema_id: existing.schema_id(),
                    });
                }
            }
            None => {
                updates.push(TableUpdate::AddSchema {
                    schema: self.schema.clone(),
                });
                updates.push(TableUpdate::SetCurrentSchema { schema_id: -1 });
            }
        }
        let schema = Arc::new(schema);

        let spec = PartitionSpecBuilder::new_from_unbound(
            self.reuse_partition_field_ids(metadata),
            schema.clone(),
        )?
        .with_last_assigned_field_id(metadata.last_partition_id())
        .build()?;
        match metadata
            .partition_specs_iter()
            .find(|existing| spec.is_compatible_with(existing))
        {
            Some(existing) => {
                if existing.spec_id() != metadata.default_partition_spec_id() {
                    updates.push(TableUpdate::SetDefaultSpec {
                        spec_id: existing.spec_id(),
                    });
                }
            }
            None => {
                // The catalog assigns the id (`SetDefaultSpec(-1)` names the
                // spec this commit adds), but Iceberg Java's parser requires
                // `spec-id` in `add-spec`: Polaris answers a bare 400 without
                // it. The next free id is what Java's own `UpdatePartitionSpec`
                // sends.
                let next_id = metadata
                    .partition_specs_iter()
                    .map(|existing| existing.spec_id())
                    .max()
                    .map_or(0, |max| max + 1);
                let mut spec = spec.into_unbound();
                spec.spec_id = Some(next_id);
                updates.push(TableUpdate::AddSpec { spec });
                updates.push(TableUpdate::SetDefaultSpec { spec_id: -1 });
            }
        }

        match &self.sort_order {
            Some(wanted) if !wanted.is_unsorted() => {
                Self::select_sort_order(metadata, &schema, wanted, &mut updates)?;
            }
            Some(_) => {
                if !metadata.default_sort_order().is_unsorted() {
                    Self::make_unsorted(metadata, &mut updates);
                }
            }
            None => {
                let sort_order = metadata.default_sort_order();
                let sort_order_binds = SortOrder::builder()
                    .with_order_id(sort_order.order_id)
                    .with_fields(sort_order.fields.clone())
                    .build(&schema)
                    .is_ok();
                if !sort_order_binds {
                    Self::make_unsorted(metadata, &mut updates);
                }
            }
        }

        let mut requirements = vec![
            TableRequirement::UuidMatch {
                uuid: self.pins.uuid,
            },
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: self.pins.current_schema_id,
            },
            TableRequirement::LastAssignedFieldIdMatch {
                last_assigned_field_id: self.pins.last_column_id,
            },
            TableRequirement::DefaultSpecIdMatch {
                default_spec_id: self.pins.default_spec_id,
            },
            TableRequirement::LastAssignedPartitionIdMatch {
                last_assigned_partition_id: self.pins.last_partition_id,
            },
        ];
        if self.sort_order.is_some() {
            requirements.push(TableRequirement::DefaultSortOrderIdMatch {
                default_sort_order_id: self.default_sort_order_id,
            });
        }
        if let Some(expected) = self.expected_main {
            requirements.push(TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: expected,
            });
        }
        Ok((updates, requirements))
    }

    /// Make the unsorted order the default: order 0 when the table has it,
    /// else added.
    fn make_unsorted(metadata: &TableMetadata, updates: &mut Vec<TableUpdate>) {
        if metadata
            .sort_order_by_id(SortOrder::UNSORTED_ORDER_ID)
            .is_some_and(|order| order.is_unsorted())
        {
            updates.push(TableUpdate::SetDefaultSortOrder {
                sort_order_id: SortOrder::UNSORTED_ORDER_ID,
            });
        } else {
            updates.push(TableUpdate::AddSortOrder {
                sort_order: SortOrder::unsorted_order(),
            });
            updates.push(TableUpdate::SetDefaultSortOrder { sort_order_id: -1 });
        }
    }

    /// Make the sorted order `wanted` the default. An order with the same
    /// fields is selected by its id: a catalog that reuses an equal order
    /// (Polaris does) would otherwise leave `SetDefaultSortOrder(-1)` with no
    /// order added in this commit. A new one gets the next free id, which
    /// Iceberg Java's parser requires in `add-sort-order`.
    fn select_sort_order(
        metadata: &TableMetadata,
        schema: &Schema,
        wanted: &SortOrder,
        updates: &mut Vec<TableUpdate>,
    ) -> Result<()> {
        SortOrder::builder()
            .with_order_id(1)
            .with_fields(wanted.fields.clone())
            .build(schema)?;
        if let Some(existing) = metadata
            .sort_orders_iter()
            .find(|order| order.fields == wanted.fields)
        {
            if existing.order_id != metadata.default_sort_order_id() {
                updates.push(TableUpdate::SetDefaultSortOrder {
                    sort_order_id: existing.order_id,
                });
            }
            return Ok(());
        }
        let next_id = metadata
            .sort_orders_iter()
            .map(|order| order.order_id)
            .max()
            .map_or(1, |max| max.max(0) + 1);
        updates.push(TableUpdate::AddSortOrder {
            sort_order: wanted.clone().with_order_id(next_id),
        });
        updates.push(TableUpdate::SetDefaultSortOrder { sort_order_id: -1 });
        Ok(())
    }

    /// `self.spec` with every unset field id taken from an equivalent field
    /// (same source id and transform) of any spec of the table; the highest
    /// such id wins, as in `TableMetadataBuilder::add_partition_spec`.
    fn reuse_partition_field_ids(&self, metadata: &TableMetadata) -> UnboundPartitionSpec {
        let mut equivalent: HashMap<(i32, Transform), i32> = HashMap::new();
        for field in metadata
            .partition_specs_iter()
            .flat_map(|spec| spec.fields())
        {
            equivalent
                .entry((field.source_id, field.transform))
                .and_modify(|id| *id = (*id).max(field.field_id))
                .or_insert(field.field_id);
        }
        let mut spec = self.spec.clone();
        for field in &mut spec.fields {
            if field.field_id.is_none() {
                field.field_id = equivalent.get(&(field.source_id, field.transform)).copied();
            }
        }
        spec
    }
}

/// `schema` with the field ids of `current` where a column keeps its place.
///
/// A field of `schema` takes the id of the field with the same name in the
/// same parent of `current` (list elements and map keys and values by their
/// position) when its type is the same or an allowed promotion: int to long,
/// float to double, a decimal to a wider precision with the same scale,
/// unknown to any primitive, or the same kind of nested type. Every other
/// field gets a fresh id above `last_column_id`. Unlike Iceberg Java, a name
/// whose type changed in another way gets a fresh id, so old data files are
/// never read as the new type (sqe#892 U6).
///
/// A field stays required only when it keeps the id of a required field:
/// the replace keeps the table's history, and files written before it lack a
/// new field or may hold nulls in a formerly optional one, so time travel and
/// rollback across the replace read them as NULL. Map keys and identifier
/// fields, which Iceberg requires, stay required.
///
/// Returns the schema and a map from each field id of `schema` to its
/// assigned id.
pub fn assign_ids_by_name(
    current: &Schema,
    schema: &Schema,
    last_column_id: i32,
) -> Result<(Schema, HashMap<i32, i32>)> {
    let mut next_id = last_column_id;
    let mut ids = HashMap::new();
    let identifiers = schema.identifier_field_ids().collect::<HashSet<_>>();
    let fields = assign_struct(
        Some(current.as_struct()),
        schema.as_struct(),
        &identifiers,
        &mut next_id,
        &mut ids,
    );
    let identifier_ids = schema
        .identifier_field_ids()
        .map(|id| ids.get(&id).copied().unwrap_or(id))
        .collect::<Vec<_>>();
    let schema = Schema::builder()
        .with_fields(fields)
        .with_identifier_field_ids(identifier_ids)
        .build()?;
    Ok((schema, ids))
}

fn assign_struct(
    current: Option<&StructType>,
    new: &StructType,
    identifiers: &HashSet<i32>,
    next_id: &mut i32,
    ids: &mut HashMap<i32, i32>,
) -> Vec<NestedFieldRef> {
    new.fields()
        .iter()
        .map(|field| {
            let old = current.and_then(|c| c.field_by_name(&field.name));
            let stays_required = identifiers.contains(&field.id);
            let old = old.map(|f| f.as_ref());
            assign_field(old, field, stays_required, identifiers, next_id, ids)
        })
        .collect()
}

/// `stays_required`: a map key or identifier field, which Iceberg requires.
fn assign_field(
    old: Option<&NestedField>,
    new: &NestedField,
    stays_required: bool,
    identifiers: &HashSet<i32>,
    next_id: &mut i32,
    ids: &mut HashMap<i32, i32>,
) -> NestedFieldRef {
    let old = old.filter(|old| keeps_id(&old.field_type, &new.field_type));
    let required = new.required && (stays_required || old.is_some_and(|old| old.required));
    let id = match old {
        Some(old) => old.id,
        None => {
            *next_id += 1;
            *next_id
        }
    };
    ids.insert(new.id, id);
    let old_type = old.map(|old| old.field_type.as_ref());
    let field_type = match (old_type, new.field_type.as_ref()) {
        (old, Type::Struct(new)) => {
            let old = match old {
                Some(Type::Struct(old)) => Some(old),
                _ => None,
            };
            Type::Struct(StructType::new(assign_struct(
                old,
                new,
                identifiers,
                next_id,
                ids,
            )))
        }
        (old, Type::List(new)) => {
            let old = match old {
                Some(Type::List(old)) => Some(old.element_field.as_ref()),
                _ => None,
            };
            Type::List(ListType {
                element_field: assign_field(
                    old,
                    &new.element_field,
                    false,
                    identifiers,
                    next_id,
                    ids,
                ),
            })
        }
        (old, Type::Map(new)) => {
            let (old_key, old_value) = match old {
                Some(Type::Map(old)) => {
                    (Some(old.key_field.as_ref()), Some(old.value_field.as_ref()))
                }
                _ => (None, None),
            };
            Type::Map(MapType {
                key_field: assign_field(
                    old_key,
                    &new.key_field,
                    true,
                    identifiers,
                    next_id,
                    ids,
                ),
                value_field: assign_field(
                    old_value,
                    &new.value_field,
                    false,
                    identifiers,
                    next_id,
                    ids,
                ),
            })
        }
        (_, other) => other.clone(),
    };
    Arc::new(NestedField {
        id,
        required,
        field_type: Box::new(field_type),
        ..new.clone()
    })
}

/// Whether a field whose type goes from `old` to `new` keeps its id.
fn keeps_id(old: &Type, new: &Type) -> bool {
    use PrimitiveType as P;
    match (old, new) {
        _ if old == new => true,
        (Type::Primitive(old), Type::Primitive(new)) => {
            matches!(
                (old, new),
                (P::Unknown, _) | (P::Int, P::Long) | (P::Float, P::Double)
            ) || matches!(
                (old, new),
                (
                    P::Decimal { precision: p, scale: s },
                    P::Decimal { precision: p2, scale: s2 },
                ) if p2 > p && s2 == s
            )
        }
        (Type::Struct(_), Type::Struct(_))
        | (Type::List(_), Type::List(_))
        | (Type::Map(_), Type::Map(_)) => true,
        _ => false,
    }
}

#[async_trait]
impl TransactionAction for ReplaceTableShapeAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let (updates, requirements) = self.plan(table.metadata())?;
        Ok(ActionCommit::new(updates, requirements))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::assign_ids_by_name;
    use crate::spec::{
        FormatVersion, ListType, NestedField, NullOrder, PrimitiveType, Schema, SortDirection,
        SortField, SortOrder, StructType, Transform, Type, UnboundPartitionSpec,
    };
    use crate::table::Table;
    use crate::transaction::action::ApplyTransactionAction;
    use crate::transaction::tests::make_v2_table;
    use crate::transaction::{ReplaceTableShapeAction, Transaction};
    use crate::{ErrorKind, TableRequirement, TableUpdate};

    fn long(id: i32, name: &str) -> Arc<NestedField> {
        Arc::new(NestedField::optional(
            id,
            name,
            Type::Primitive(PrimitiveType::Long),
        ))
    }

    fn schema(fields: Vec<Arc<NestedField>>) -> Schema {
        Schema::builder().with_fields(fields).build().unwrap()
    }

    fn unpartitioned() -> UnboundPartitionSpec {
        UnboundPartitionSpec::builder().build()
    }

    fn identity(source_id: i32, name: &str) -> UnboundPartitionSpec {
        UnboundPartitionSpec::builder()
            .add_partition_field(source_id, name, Transform::Identity)
            .unwrap()
            .build()
    }

    fn shape(table: &Table, schema: Schema, spec: UnboundPartitionSpec) -> ReplaceTableShapeAction {
        ReplaceTableShapeAction::new(table, schema, spec).unwrap()
    }

    fn field(id: i32, name: &str, field_type: Type) -> Arc<NestedField> {
        Arc::new(NestedField::optional(id, name, field_type))
    }

    fn ids_of(schema: &Schema) -> Vec<(String, i32)> {
        schema
            .as_struct()
            .fields()
            .iter()
            .map(|f| (f.name.clone(), f.id))
            .collect()
    }

    /// `(a INT, b INT, c STRING)` replaced by `(a BIGINT, b STRING, d INT)`:
    /// a keeps its id (a promotion), b and d get fresh ones.
    #[test]
    fn ids_follow_names_when_the_type_is_kept_or_promoted() {
        let p = |t| Type::Primitive(t);
        let current = schema(vec![
            field(1, "a", p(PrimitiveType::Int)),
            field(2, "b", p(PrimitiveType::Int)),
            field(3, "c", p(PrimitiveType::String)),
        ]);
        let new = schema(vec![
            field(1, "a", p(PrimitiveType::Long)),
            field(2, "b", p(PrimitiveType::String)),
            field(3, "d", p(PrimitiveType::Int)),
        ]);
        let (assigned, ids) = assign_ids_by_name(&current, &new, 3).unwrap();
        assert_eq!(
            ids_of(&assigned),
            vec![
                ("a".to_string(), 1),
                ("b".to_string(), 4),
                ("d".to_string(), 5)
            ]
        );
        assert_eq!(ids[&3], 5);
        assert_eq!(assigned.highest_field_id(), 5);
    }

    /// Files written before the replace lack `b` (retyped) and `d` (new) and
    /// may hold nulls in `o` (optional before), so only `a` stays required.
    /// A map key stays required, as Iceberg demands.
    #[test]
    fn only_a_field_that_was_required_stays_required() {
        let req = |id, name: &str, t| Arc::new(NestedField::required(id, name, Type::Primitive(t)));
        let current = schema(vec![
            req(1, "a", PrimitiveType::Int),
            req(2, "b", PrimitiveType::Int),
            field(3, "o", Type::Primitive(PrimitiveType::Int)),
        ]);
        let map = Type::Map(crate::spec::MapType {
            key_field: req(14, "key", PrimitiveType::String),
            value_field: req(15, "value", PrimitiveType::Int),
        });
        let new = schema(vec![
            req(10, "a", PrimitiveType::Long),
            req(11, "b", PrimitiveType::String),
            req(12, "o", PrimitiveType::Int),
            req(13, "d", PrimitiveType::Int),
            Arc::new(NestedField::required(16, "m", map)),
        ]);
        let (assigned, _) = assign_ids_by_name(&current, &new, 3).unwrap();
        let required = |name: &str| assigned.field_by_name(name).unwrap().required;
        assert!(required("a"));
        for name in ["b", "o", "d", "m"] {
            assert!(!required(name), "{name} is optional after the replace");
        }
        let Type::Map(m) = assigned.field_by_name("m").unwrap().field_type.as_ref() else {
            panic!("m is a map");
        };
        assert!(m.key_field.required, "a map key stays required");
        assert!(!m.value_field.required);
    }

    #[test]
    fn nested_ids_follow_names_and_positions() {
        let p = |t| Type::Primitive(t);
        let current = schema(vec![
            field(
                1,
                "s",
                Type::Struct(StructType::new(vec![
                    field(2, "x", p(PrimitiveType::Int)),
                    field(3, "y", p(PrimitiveType::Int)),
                ])),
            ),
            field(
                4,
                "l",
                Type::List(ListType::new(field(5, "element", p(PrimitiveType::Float)))),
            ),
        ]);
        // s.y dropped, s.z added, the list element promoted to double.
        let new = schema(vec![
            field(
                10,
                "s",
                Type::Struct(StructType::new(vec![
                    field(11, "x", p(PrimitiveType::Int)),
                    field(12, "z", p(PrimitiveType::Int)),
                ])),
            ),
            field(
                13,
                "l",
                Type::List(ListType::new(field(
                    14,
                    "element",
                    p(PrimitiveType::Double),
                ))),
            ),
        ]);
        let (assigned, ids) = assign_ids_by_name(&current, &new, 5).unwrap();
        assert_eq!(
            [ids[&10], ids[&11], ids[&12], ids[&13], ids[&14]],
            [1, 2, 6, 4, 5]
        );
        assert!(assigned.field_by_id(6).is_some());
    }

    /// Shape A, then B, then A again: the third replace selects A's schema.
    #[tokio::test]
    async fn a_shape_seen_before_reuses_its_ids_and_schema() {
        let table = make_v2_table();
        let a = || schema(vec![long(1, "x"), long(2, "a")]);
        let b = || schema(vec![long(1, "x"), long(2, "b")]);
        let after_a = shape(&table, a(), unpartitioned())
            .evolved_table(&table)
            .unwrap();
        let a_id = after_a.metadata().current_schema_id();
        let after_b = shape(&after_a, b(), unpartitioned())
            .evolved_table(&after_a)
            .unwrap();
        assert_ne!(after_b.metadata().current_schema_id(), a_id);
        let action = shape(&after_b, a(), unpartitioned());
        let (updates, _) = commit_of(&after_b, action).await;
        assert_eq!(
            updates[0],
            TableUpdate::SetCurrentSchema { schema_id: a_id }
        );
        assert!(
            !updates
                .iter()
                .any(|u| matches!(u, TableUpdate::AddSchema { .. }))
        );
    }

    #[test]
    fn a_partition_source_follows_its_column() {
        let table = make_v2_table();
        // y is column 2 of the table; here the caller numbered it 7.
        let action = shape(
            &table,
            schema(vec![long(6, "w"), long(7, "y")]),
            identity(7, "y"),
        );
        assert_eq!(action.spec.fields[0].source_id, 2);
        assert_eq!(action.schema().field_by_name("w").unwrap().id, 4);
    }

    /// A replace that drops the partition column leaves spec 0 `identity(x)` naming a column
    /// the current schema lacks. The files removed by the replace are still listed in a
    /// spec-0 manifest, so spec 0 binds to the newest schema that holds `x`, as a long, not
    /// to the current schema, where `x` would type as `unknown`.
    #[test]
    fn an_old_spec_binds_to_a_schema_that_still_holds_its_source() {
        let table = make_v2_table();
        let evolved = shape(&table, schema(vec![long(4, "w")]), unpartitioned())
            .evolved_table(&table)
            .unwrap();
        let metadata = evolved.metadata();
        assert!(metadata.current_schema().field_by_name("x").is_none());
        let spec0 = metadata.partition_spec_by_id(0).unwrap();
        let bound = metadata.schema_for_partition_spec(spec0);
        assert_eq!(bound.schema_id(), 1);
        let partition_type = spec0.partition_type(bound).unwrap();
        assert_eq!(
            *partition_type.fields()[0].field_type,
            Type::Primitive(PrimitiveType::Long)
        );
        // The default spec binds to the current schema.
        let default = metadata.default_partition_spec();
        assert_eq!(
            metadata.schema_for_partition_spec(default).schema_id(),
            metadata.current_schema_id()
        );
    }

    async fn commit_of(
        table: &Table,
        action: ReplaceTableShapeAction,
    ) -> (Vec<TableUpdate>, Vec<TableRequirement>) {
        let tx = action.apply(Transaction::new(table)).unwrap();
        let mut commit = tx.prepare_commit().await.unwrap();
        (commit.take_updates(), commit.take_requirements())
    }

    /// The fixture has schemas 0 (x) and 1 (x, y, z; current), spec 0
    /// `identity(x)` with field id 1000 and sort order 3 on y and z.
    #[tokio::test]
    async fn new_schema_adds_and_selects_it_and_drops_a_sort_on_gone_columns() {
        let table = make_v2_table();
        let action = shape(
            &table,
            schema(vec![long(1, "x"), long(4, "w")]),
            identity(1, "x"),
        );
        let evolved = action.evolved_table(&table).unwrap();
        let (updates, requirements) = commit_of(&table, action).await;

        assert!(matches!(&updates[0], TableUpdate::AddSchema { .. }));
        assert_eq!(updates[1], TableUpdate::SetCurrentSchema { schema_id: -1 });
        // identity(x) is spec 0 already: nothing for the spec.
        assert!(!updates.iter().any(|u| matches!(
            u,
            TableUpdate::AddSpec { .. } | TableUpdate::SetDefaultSpec { .. }
        )));
        // Sort order 3 names y and z, both gone: the table becomes unsorted.
        assert!(
            updates
                .iter()
                .any(|u| matches!(u, TableUpdate::AddSortOrder { .. }))
        );
        assert_eq!(evolved.metadata().current_schema_id(), 2);
        assert_eq!(evolved.metadata().last_column_id(), 4);
        assert!(evolved.metadata().default_sort_order().is_unsorted());
        assert_eq!(evolved.metadata().uuid(), table.metadata().uuid());
        assert!(
            requirements.contains(&TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: 1
            })
        );
        assert!(
            requirements.contains(&TableRequirement::LastAssignedFieldIdMatch {
                last_assigned_field_id: 3
            })
        );
    }

    fn desc_on(source_id: i32) -> SortOrder {
        SortOrder::new(1, vec![SortField {
            source_id,
            transform: Transform::Identity,
            direction: SortDirection::Descending,
            null_order: NullOrder::Last,
        }])
    }

    /// sqe#897: a requested sort order names the caller's field ids. `w` is
    /// id 9 to the caller and gets id 4, so the order follows it; it is new,
    /// so it is added as order 4 (after the fixture's order 3), made default
    /// in the same commit, and the commit asserts the planned default order.
    #[tokio::test]
    async fn a_requested_sort_order_follows_the_assigned_ids_and_is_added() {
        let table = make_v2_table();
        let action = shape(
            &table,
            schema(vec![long(1, "x"), long(9, "w")]),
            unpartitioned(),
        )
        .with_sort_order(desc_on(9))
        .unwrap();
        let evolved = action.evolved_table(&table).unwrap();
        let (updates, requirements) = commit_of(&table, action).await;

        let default = evolved.metadata().default_sort_order();
        assert_eq!(default.order_id, 4);
        assert_eq!(default.fields, desc_on(4).fields);
        assert!(updates.contains(&TableUpdate::SetDefaultSortOrder { sort_order_id: -1 }));
        assert!(
            requirements.contains(&TableRequirement::DefaultSortOrderIdMatch {
                default_sort_order_id: 3
            })
        );
        assert!(
            shape(&table, schema(vec![long(1, "x")]), unpartitioned())
                .with_sort_order(desc_on(9))
                .is_err(),
            "a sort on a column the schema lacks is refused"
        );
    }

    /// An order equal to an existing one is selected by id, never added again
    /// (Polaris reuses it and `-1` would then name nothing); an unsorted
    /// request makes a sorted table unsorted even though its order binds.
    #[tokio::test]
    async fn a_requested_sort_order_reuses_an_equal_one_and_unsorted_unsorts() {
        let table = make_v2_table();
        let current = table.metadata().current_schema().as_ref().clone();
        let live = table.metadata().default_sort_order().as_ref().clone();
        let (updates, _) = commit_of(
            &table,
            shape(&table, current.clone(), unpartitioned())
                .with_sort_order(live)
                .unwrap(),
        )
        .await;
        assert!(!updates.iter().any(|u| matches!(
            u,
            TableUpdate::AddSortOrder { .. } | TableUpdate::SetDefaultSortOrder { .. }
        )));

        let action = shape(&table, current, unpartitioned())
            .with_sort_order(SortOrder::unsorted_order())
            .unwrap();
        assert!(
            action
                .evolved_table(&table)
                .unwrap()
                .metadata()
                .default_sort_order()
                .is_unsorted()
        );
    }

    #[tokio::test]
    async fn a_schema_seen_before_is_selected_by_id_not_added() {
        let table = make_v2_table();
        // Equal to schema 0.
        let x = Arc::new(NestedField::required(
            1,
            "x",
            Type::Primitive(PrimitiveType::Long),
        ));
        let action = shape(&table, schema(vec![x]), identity(1, "x"));
        let evolved = action.evolved_table(&table).unwrap();
        let (updates, _) = commit_of(&table, action).await;
        assert_eq!(updates[0], TableUpdate::SetCurrentSchema { schema_id: 0 });
        assert!(
            !updates
                .iter()
                .any(|u| matches!(u, TableUpdate::AddSchema { .. }))
        );
        assert_eq!(evolved.metadata().current_schema_id(), 0);
    }

    #[tokio::test]
    async fn new_spec_gets_explicit_field_ids_and_a_known_one_is_selected_by_id() {
        let table = make_v2_table();
        let fields = vec![long(1, "x"), long(2, "y"), long(3, "z")];
        let action = shape(&table, schema(fields.clone()), unpartitioned());
        let evolved = action.evolved_table(&table).unwrap();
        let (updates, _) = commit_of(&table, action).await;
        let added = updates
            .iter()
            .find_map(|u| match u {
                TableUpdate::AddSpec { spec } => Some(spec.clone()),
                _ => None,
            })
            .expect("an unpartitioned spec is new for this table");
        assert!(added.fields().is_empty());
        // Iceberg Java (Polaris) refuses an `add-spec` without `spec-id`.
        assert_eq!(added.spec_id(), Some(1));
        let json = serde_json::to_value(TableUpdate::AddSpec { spec: added }).unwrap();
        assert_eq!(json["spec"]["spec-id"], 1, "{json}");
        assert!(updates.contains(&TableUpdate::SetDefaultSpec { spec_id: -1 }));
        assert_eq!(evolved.metadata().default_partition_spec_id(), 1);

        // identity(y) is new: its field id is assigned here, above 1000.
        let action = shape(&table, schema(fields.clone()), identity(2, "y"));
        let (updates, _) = commit_of(&table, action).await;
        let added = updates
            .iter()
            .find_map(|u| match u {
                TableUpdate::AddSpec { spec } => Some(spec.clone()),
                _ => None,
            })
            .unwrap();
        assert_eq!(added.fields()[0].field_id, Some(1001));

        // Back to identity(x) from the evolved table: spec 0 by id.
        let action = shape(&evolved, schema(fields), identity(1, "x"));
        let (updates, _) = commit_of(&evolved, action).await;
        assert!(updates.contains(&TableUpdate::SetDefaultSpec { spec_id: 0 }));
        assert!(
            !updates
                .iter()
                .any(|u| matches!(u, TableUpdate::AddSpec { .. }))
        );
    }

    #[tokio::test]
    async fn moved_schema_is_refused_and_not_retryable() {
        let table = make_v2_table();
        let action = shape(&table, schema(vec![long(1, "x")]), identity(1, "x"));
        // Someone else's schema change lands first.
        let moved = Transaction::update_table_metadata(
            table.clone(),
            &[
                TableUpdate::AddSchema {
                    schema: schema(vec![long(1, "x"), long(2, "y"), long(3, "z"), long(4, "q")]),
                },
                TableUpdate::SetCurrentSchema { schema_id: -1 },
            ],
        )
        .unwrap();
        let err = action
            .apply(Transaction::new(&moved))
            .unwrap()
            .prepare_commit()
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert!(!err.retryable());
        assert!(
            err.message()
                .contains("concurrent schema or partition change"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn moved_main_is_a_conflict_the_caller_reruns() {
        let table = make_v2_table();
        let action =
            shape(&table, schema(vec![long(1, "x")]), identity(1, "x")).with_expected_main(Some(1));
        let err = action.evolved_table(&table).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::CatalogCommitConflicts);
        assert!(!err.retryable());
    }

    #[tokio::test]
    async fn format_upgrade_comes_first_and_a_lower_version_is_ignored() {
        let table = make_v2_table();
        let fields = vec![long(1, "x"), long(2, "y"), long(3, "z")];
        let action = shape(&table, schema(fields.clone()), identity(1, "x"))
            .with_format_version(FormatVersion::V3);
        let (updates, _) = commit_of(&table, action).await;
        assert_eq!(
            updates[0],
            TableUpdate::UpgradeFormatVersion {
                format_version: FormatVersion::V3
            }
        );

        let action =
            shape(&table, schema(fields), identity(1, "x")).with_format_version(FormatVersion::V1);
        let (updates, _) = commit_of(&table, action).await;
        assert!(
            !updates
                .iter()
                .any(|u| matches!(u, TableUpdate::UpgradeFormatVersion { .. }))
        );
    }
}
