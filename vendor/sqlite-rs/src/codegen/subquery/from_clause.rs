// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! `FROM`-subquery schema resolution and materialization — see
//! `super`'s module doc.

use crate::codegen::index_maintenance::valid_table_root_page;
use crate::codegen::select::{
    compile_select_joined_scan, compile_select_scan, CodegenError, ScanCursors,
};
use crate::codegen::{Emitter, RegAlloc};
use crate::parser::ast::{ExprKind, ResultColumn, Select, TableRef, TableRefKind};
use crate::schema::TableSchema;
use crate::vdbe::{Instruction, Opcode, P4};

/// Resolves a subquery's own single-table `FROM` against `catalog`,
/// rejecting anything this MVP pass doesn't materialize: no `FROM` at
/// all is only valid when the subquery has no column references (e.g.
/// `SELECT (SELECT 1)`), and a `JOIN`ed `FROM` isn't supported.
pub(crate) fn resolve_subquery_schema(
    subselect: &Select,
    catalog: &[TableSchema],
) -> Result<Option<TableSchema>, CodegenError> {
    let Some(from) = &subselect.from else {
        return Ok(None);
    };
    if !from.joins.is_empty() {
        return Err(CodegenError::Unsupported {
            reason: "a subquery whose own FROM clause has a JOIN is not yet supported".to_string(),
        });
    }
    let Some(name) = from.first.name() else {
        return Err(CodegenError::Unsupported {
            reason: "a subquery-expression's own FROM being itself a subquery is not yet \
                     supported"
                .to_string(),
        });
    };
    let schema = catalog
        .iter()
        .find(|s| s.name.eq_ignore_ascii_case(name))
        .cloned()
        .ok_or_else(|| CodegenError::Unsupported {
            reason: format!(
                "subquery references table {name:?}, which isn't visible to this compiler's \
                 catalog"
            ),
        })?;
    Ok(Some(schema))
}

/// The column names a `FROM`-subquery's own `SELECT` list exposes to the
/// enclosing query (#257) — used to build the synthetic [`TableSchema`]
/// a materialized subquery-in-FROM is bound into `Scope` as.
/// `table_refs`/`schemas` are the subquery's own resolved `FROM` tables,
/// same order, for `*`/`table.*` expansion; an unaliased computed
/// expression falls back to a positional `columnN` name (`N` 1-based),
/// same convention SQLite itself uses for an anonymous result column.
fn subquery_output_columns(
    subquery: &Select,
    table_refs: &[&TableRef],
    schemas: &[TableSchema],
) -> Vec<String> {
    let mut out = Vec::new();
    for (i, col) in subquery.columns.iter().enumerate() {
        match col {
            ResultColumn::Star => {
                for schema in schemas {
                    out.extend(schema.columns.iter().cloned());
                }
            }
            ResultColumn::TableStar { table } => {
                if let Some(schema) = table_refs
                    .iter()
                    .position(|t| t.alias.as_deref().or(t.name()).unwrap_or("") == table)
                    .and_then(|idx| schemas.get(idx))
                {
                    out.extend(schema.columns.iter().cloned());
                }
            }
            ResultColumn::Expr { expr, alias } => {
                let name = alias.clone().unwrap_or_else(|| match &expr.kind {
                    ExprKind::Column { name, .. } => name.clone(),
                    _ => format!("column{}", i.saturating_add(1)),
                });
                out.push(name);
            }
        }
    }
    out
}

/// A `FROM`-subquery's own `FROM` table(s) (#257) — the first table plus
/// every join's table, same order. Split from [`resolve_subquery_schemas`]
/// (rather than returning both together) because a function borrowing
/// from two different reference parameters (`subquery` here, `catalog`
/// there) can't have its output lifetime elided, and this codebase's
/// `make check-mvl-limit` gate forbids writing an explicit lifetime to spell
/// it out.
pub(super) fn subquery_own_table_refs(subquery: &Select) -> Result<Vec<&TableRef>, CodegenError> {
    let Some(from) = &subquery.from else {
        return Err(CodegenError::Unsupported {
            reason: "a subquery in FROM must itself have a FROM clause".to_string(),
        });
    };
    Ok(std::iter::once(&from.first)
        .chain(from.joins.iter().map(|j| &j.table))
        .collect())
}

/// Resolves each of `table_refs` against `catalog` — one schema per
/// table, same order. Delegates to [`resolve_from_table_schema`], which
/// itself recurses for a nested `TableRefKind::Subquery` (e.g. a CTE
/// whose own body's `FROM` names another CTE, #376), so nesting to
/// arbitrary depth just falls out rather than needing its own handling
/// here.
fn resolve_subquery_schemas(
    table_refs: &[&TableRef],
    catalog: &[TableSchema],
) -> Result<Vec<TableSchema>, CodegenError> {
    table_refs
        .iter()
        .map(|table_ref| resolve_from_table_schema(table_ref, catalog))
        .collect()
}

/// Builds the synthetic [`TableSchema`] (#257) a materialized
/// subquery-in-FROM is bound into `Scope` as — `name` left empty, since
/// only the caller (which has the `TableRef`) knows the subquery's
/// mandatory alias.
fn subquery_result_schema(
    subquery: &Select,
    table_refs: &[&TableRef],
    schemas: &[TableSchema],
) -> TableSchema {
    let columns = subquery_output_columns(subquery, table_refs, schemas);
    TableSchema {
        unresolved_autoindex: false,
        name: String::new(),
        root_page: 0,
        columns: columns.clone(),
        without_rowid: false,
        strict: false,
        column_types: vec![String::new(); columns.len()],
        column_collations: vec![],
        is_virtual: false,
        sql: String::new(),
        indexes: Vec::new(),
        rowid_alias: None,
    }
}

/// Resolves `table_ref` to the [`TableSchema`] the rest of codegen
/// should treat it as: a real catalog lookup by name, or (#257) the
/// synthetic schema describing a `FROM`-subquery's own projected
/// columns (its `name` is `table_ref`'s alias — mandatory for a
/// subquery, enforced by the parser). Used by callers (the `sqlite-rs`
/// CLI, `INSERT ... SELECT`) that need a `TableSchema` up front, before
/// the codegen pass that actually emits the materialization
/// (`compile_select_with_catalog`/`compile_select_joined` call
/// [`materialize_from_subquery`] themselves once compiling).
pub fn resolve_from_table_schema(
    table_ref: &TableRef,
    catalog: &[TableSchema],
) -> Result<TableSchema, CodegenError> {
    match &table_ref.kind {
        crate::parser::ast::TableRefKind::Name(name) => catalog
            .iter()
            .find(|s| s.name.eq_ignore_ascii_case(name))
            .cloned()
            .ok_or_else(|| CodegenError::Unsupported {
                reason: format!("no such table: {name}"),
            }),
        crate::parser::ast::TableRefKind::Subquery(subquery) => {
            let table_refs = subquery_own_table_refs(subquery)?;
            let schemas = resolve_subquery_schemas(&table_refs, catalog)?;
            let mut schema = subquery_result_schema(subquery, &table_refs, &schemas);
            schema.name = table_ref.alias.clone().unwrap_or_default();
            Ok(schema)
        }
    }
}

/// Materializes a `FROM`-subquery (#257) into an in-memory ephemeral
/// table opened on `dest_cursor`, so the enclosing query can then scan
/// it exactly like a real table cursor (`Rewind`/`Next`/`Column`/
/// `Rowid`). Drives the subquery's own scan through
/// [`compile_select_scan`] (single-table) or [`compile_select_joined_scan`]
/// (its own `FROM` has a JOIN — criterion 3), substituting a row sink
/// that `MakeRecord`s each projected row and `Insert`s it into
/// `dest_cursor` with a freshly `Sequence`d rowid, in place of
/// `ResultRow` — the same substitution #208's `INSERT ... SELECT`
/// codegen uses. Returns the synthetic [`TableSchema`] (`name` left
/// empty — the caller fills in the subquery's alias) describing the
/// materialized table's columns, for the caller to bind into `Scope`.
/// A subquery nested inside another subquery's `FROM` is not yet
/// supported (this pass materializes one level).
///
/// #425: if an earlier call in this same statement's compile already
/// materialized a *structurally identical* `subquery` (checked via
/// `Select`'s derived `PartialEq` — this is how `expand_with_clause`
/// rewrites a CTE referenced N times: N independent `TableRefKind::
/// Subquery` AST clones, one per `FROM`/`JOIN` site, preserving
/// self-join correctness, but every clone is byte-for-byte the same
/// query), this call reuses that materialization (`OpenDup`) instead
/// of paying to re-run and re-populate the identical query again. Safe
/// for any subquery-in-FROM, not just CTEs, given what this crate can
/// express today: no correlated variables reach this materialization
/// path (see the module doc), and no volatile/non-deterministic
/// expression exists yet either (`random()` and
/// `CURRENT_TIME`/`CURRENT_DATE`/`CURRENT_TIMESTAMP` are all still
/// `unsupported(..)` in the parser — checked as of #425/#421's V7.2
/// review), so two textually-identical subqueries are currently
/// guaranteed to produce the same rows.
///
/// **This stops being true the day a volatile function is added.**
/// `SELECT * FROM (SELECT random() r FROM t) a JOIN (SELECT random() r
/// FROM t) b` — two independent, unrelated derived tables that happen
/// to be textually identical — would then incorrectly share one
/// evaluation instead of drawing independently, unlike real `sqlite3`.
/// Whichever ticket adds the first such function must revisit this
/// cache: either exclude a `Select` containing one from
/// `RegAlloc::cached_cte`/`cache_cte`, or narrow the cache key from raw
/// structural equality to genuine CTE identity (tracked back to
/// `expand_with_clause`'s substitution) so it never activates for two
/// merely-coincidental derived tables in the first place.
pub(crate) fn materialize_from_subquery(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    subquery: &Select,
    catalog: &[TableSchema],
    dest_cursor: i32,
) -> Result<TableSchema, CodegenError> {
    if let Some((source_cursor, schema)) = reg.cached_cte(subquery) {
        em.emit(Instruction::new(
            Opcode::OpenDup,
            dest_cursor,
            source_cursor,
            0,
        ));
        return Ok(schema);
    }

    // #382: a compound (`UNION`/`UNION ALL`) body isn't handled by this
    // materialization path yet — only `subquery`'s own `first` arm would
    // be scanned into `dest_cursor`, silently dropping every other
    // arm's rows (discovered via a CTE-body-is-UNION corpus regression).
    // Reject cleanly here rather than let that data loss reach a
    // caller, matching the "not yet supported" pattern used just below
    // for a joined subquery-in-FROM. Fast-follow: teach this function
    // (and its view-expansion counterpart, `expand_views`, which shares
    // the same underlying materialization) to loop over every compound
    // arm like `compile_select_compound` does at the top level.
    if !subquery.compound.is_empty() {
        return Err(CodegenError::Unsupported {
            reason: "a compound (UNION) SELECT as a CTE/view/derived-table body is not yet \
                     supported"
                .to_string(),
        });
    }

    let table_refs = subquery_own_table_refs(subquery)?;
    let schemas = resolve_subquery_schemas(&table_refs, catalog)?;
    let Some(from) = &subquery.from else {
        return Err(CodegenError::Unsupported {
            reason: "a subquery in FROM must itself have a FROM clause".to_string(),
        });
    };
    let synthetic_schema = subquery_result_schema(subquery, &table_refs, &schemas);

    em.emit(Instruction {
        opcode: Opcode::OpenEphemeral,
        p1: dest_cursor,
        p2: 0,
        p3: 0,
        p4: P4::None,
        p5: 1,
    });

    let end_label = em.new_label();
    let mut sink = |em: &mut Emitter, reg: &mut RegAlloc, first: i32, count: i32| {
        let rowid_reg = reg.alloc();
        em.emit(Instruction::new(
            Opcode::Sequence,
            dest_cursor,
            rowid_reg,
            0,
        ));
        let record_reg = reg.alloc();
        em.emit(Instruction::new(
            Opcode::MakeRecord,
            first,
            count,
            record_reg,
        ));
        em.emit(Instruction::new(
            Opcode::Insert,
            dest_cursor,
            rowid_reg,
            record_reg,
        ));
        Ok(())
    };

    if from.joins.is_empty() {
        let schema = schemas.first().ok_or_else(|| CodegenError::Unsupported {
            reason: "materialized subquery FROM has no schema".to_string(),
        })?;
        let cursors = ScanCursors {
            table: reg.alloc_cursor(),
            sort: reg.alloc_cursor(),
            pseudo: reg.alloc_cursor(),
            distinct: reg.alloc_cursor(),
        };
        match &from.first.kind {
            TableRefKind::Name(_) => {
                let root_page = valid_table_root_page(schema)?;
                em.emit(Instruction::new(
                    Opcode::OpenRead,
                    cursors.table,
                    root_page,
                    0,
                ));
            }
            TableRefKind::Subquery(inner) => {
                // A subquery-in-FROM nested inside this one (#376: a CTE
                // whose own body's FROM names another CTE) — materialize
                // it into the same cursor this level would otherwise
                // `OpenRead` a real table into.
                materialize_from_subquery(em, reg, inner, catalog, cursors.table)?;
            }
        }
        compile_select_scan(
            em,
            reg,
            subquery,
            schema,
            cursors,
            end_label,
            catalog,
            &crate::planner::Stats::default(),
            &mut sink,
        )?;
    } else {
        let cursor_base = reg.alloc_cursor();
        // Reserve `table_count + 2` contiguous cursor numbers (one per
        // joined table, plus the sort/pseudo or distinct cursor
        // `compile_select_joined_scan` may itself derive by offsetting
        // from `cursor_base`) so a later `reg.alloc_cursor()` call (e.g.
        // for a correlated subquery expression inside this subquery)
        // can't collide with a number that function computes by
        // arithmetic rather than by calling `alloc_cursor` itself.
        for _ in 0..schemas.len().saturating_add(1) {
            reg.alloc_cursor();
        }
        compile_select_joined_scan(
            em,
            reg,
            subquery,
            &schemas,
            catalog,
            cursor_base,
            end_label,
            &std::collections::HashMap::new(),
            &mut sink,
        )?;
    }
    em.place(end_label);

    reg.cache_cte(subquery, dest_cursor, synthetic_schema.clone());

    Ok(synthetic_schema)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing, clippy::panic)]
mod tests {
    use super::*;
    use crate::codegen::{Emitter, RegAlloc};
    use crate::parser::error::{parse_select, ParseOutcome};

    fn parse(sql: &str) -> Select {
        match parse_select(sql) {
            ParseOutcome::Accepted(select) => *select,
            other => panic!("expected Accepted, got {other:?}"),
        }
    }

    fn table(name: &str, root_page: u32) -> TableSchema {
        TableSchema {
            unresolved_autoindex: false,
            name: name.to_string(),
            root_page,
            columns: vec!["a".to_string(), "b".to_string()],
            without_rowid: false,
            strict: false,
            column_types: vec![String::new(), String::new()],
            column_collations: vec![],
            is_virtual: false,
            sql: format!("CREATE TABLE {name}(a, b)"),
            indexes: Vec::new(),
            rowid_alias: None,
        }
    }

    fn from_of(select: &Select) -> &TableRef {
        &select.from.as_ref().unwrap().first
    }

    #[test]
    fn resolve_subquery_schema_none_when_no_from() {
        let select = parse("SELECT (SELECT 1)");
        let inner = match &select.columns[0] {
            ResultColumn::Expr { expr, .. } => match &expr.kind {
                ExprKind::Subquery(sub) => (**sub).clone(),
                _ => panic!("expected subquery expr"),
            },
            _ => panic!("expected Expr result column"),
        };
        let result = resolve_subquery_schema(&inner, &[]).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn resolve_subquery_schema_rejects_join_in_own_from() {
        let sub = parse("SELECT a FROM t JOIN u ON t.a = u.a");
        let err = resolve_subquery_schema(&sub, &[]).unwrap_err();
        assert!(matches!(err, CodegenError::Unsupported { reason } if reason.contains("JOIN")));
    }

    #[test]
    fn resolve_subquery_schema_rejects_nested_subquery_from() {
        let sub = parse("SELECT a FROM (SELECT a FROM t) AS x");
        let err = resolve_subquery_schema(&sub, &[]).unwrap_err();
        assert!(
            matches!(err, CodegenError::Unsupported { reason } if reason.contains("subquery-expression"))
        );
    }

    #[test]
    fn resolve_subquery_schema_rejects_unknown_table() {
        let sub = parse("SELECT a FROM missing");
        let err = resolve_subquery_schema(&sub, &[]).unwrap_err();
        assert!(matches!(err, CodegenError::Unsupported { reason } if reason.contains("missing")));
    }

    #[test]
    fn resolve_subquery_schema_finds_catalog_table_case_insensitively() {
        let sub = parse("SELECT a FROM T");
        let catalog = vec![table("t", 2)];
        let schema = resolve_subquery_schema(&sub, &catalog).unwrap().unwrap();
        assert_eq!(schema.name, "t");
    }

    #[test]
    fn resolve_from_table_schema_name_not_found() {
        let select = parse("SELECT a FROM missing");
        let err = resolve_from_table_schema(from_of(&select), &[]).unwrap_err();
        assert!(
            matches!(err, CodegenError::Unsupported { reason } if reason.contains("no such table"))
        );
    }

    #[test]
    fn resolve_from_table_schema_name_found() {
        let select = parse("SELECT a FROM t");
        let catalog = vec![table("t", 2)];
        let schema = resolve_from_table_schema(from_of(&select), &catalog).unwrap();
        assert_eq!(schema.name, "t");
    }

    #[test]
    fn resolve_from_table_schema_star_and_alias_expr() {
        let select = parse("SELECT * FROM (SELECT a, b AS c FROM t) AS s");
        let catalog = vec![table("t", 2)];
        let schema = resolve_from_table_schema(from_of(&select), &catalog).unwrap();
        assert_eq!(schema.name, "s");
        assert_eq!(schema.columns, vec!["a".to_string(), "c".to_string()]);
    }

    #[test]
    fn resolve_from_table_schema_table_star() {
        let select = parse("SELECT s.* FROM (SELECT a, b FROM t) AS s");
        let catalog = vec![table("t", 2)];
        let schema = resolve_from_table_schema(from_of(&select), &catalog).unwrap();
        assert_eq!(schema.columns, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn resolve_from_table_schema_computed_expr_gets_positional_name() {
        let select = parse("SELECT * FROM (SELECT a + 1 FROM t) AS s");
        let catalog = vec![table("t", 2)];
        let schema = resolve_from_table_schema(from_of(&select), &catalog).unwrap();
        assert_eq!(schema.columns, vec!["column1".to_string()]);
    }

    #[test]
    fn resolve_from_table_schema_unaliased_column_expr_uses_column_name() {
        let select = parse("SELECT a FROM (SELECT a FROM t) AS s");
        let catalog = vec![table("t", 2)];
        let schema = resolve_from_table_schema(from_of(&select), &catalog).unwrap();
        assert_eq!(schema.columns, vec!["a".to_string()]);
    }

    #[test]
    fn materialize_from_subquery_rejects_compound() {
        let subquery = parse("SELECT a FROM t UNION SELECT a FROM t2");
        let mut em = Emitter::new();
        let mut reg = RegAlloc::default();
        let err = materialize_from_subquery(&mut em, &mut reg, &subquery, &[], 1).unwrap_err();
        assert!(matches!(err, CodegenError::Unsupported { reason } if reason.contains("UNION")));
    }

    #[test]
    fn materialize_from_subquery_rejects_unknown_table() {
        let subquery = parse("SELECT a FROM missing");
        let mut em = Emitter::new();
        let mut reg = RegAlloc::default();
        let err = materialize_from_subquery(&mut em, &mut reg, &subquery, &[], 1).unwrap_err();
        assert!(matches!(err, CodegenError::Unsupported { .. }));
    }

    #[test]
    fn materialize_from_subquery_single_table_and_cache_reuse() {
        let subquery = parse("SELECT a, b FROM t");
        let catalog = vec![table("t", 2)];
        let mut em = Emitter::new();
        let mut reg = RegAlloc::default();

        let schema = materialize_from_subquery(&mut em, &mut reg, &subquery, &catalog, 10).unwrap();
        assert_eq!(schema.columns, vec!["a".to_string(), "b".to_string()]);

        // #425: an identical subquery reuses the cached materialization via
        // OpenDup instead of re-running the scan.
        let before = em.here();
        let schema2 =
            materialize_from_subquery(&mut em, &mut reg, &subquery, &catalog, 11).unwrap();
        assert_eq!(schema2.columns, schema.columns);
        let program = em.finish();
        assert_eq!(program.len(), before + 1);
        assert_eq!(program.get(before).unwrap().opcode, Opcode::OpenDup);
    }

    #[test]
    fn materialize_from_subquery_joined_own_from() {
        let subquery = parse("SELECT t.a FROM t JOIN t2 ON t.a = t2.a");
        let catalog = vec![table("t", 2), table("t2", 3)];
        let mut em = Emitter::new();
        let mut reg = RegAlloc::default();
        let schema = materialize_from_subquery(&mut em, &mut reg, &subquery, &catalog, 10).unwrap();
        assert_eq!(schema.columns, vec!["a".to_string()]);
    }

    #[test]
    fn materialize_from_subquery_nested_subquery_in_own_from() {
        let subquery = parse("SELECT a FROM (SELECT a FROM t) AS inner_s");
        let catalog = vec![table("t", 2)];
        let mut em = Emitter::new();
        let mut reg = RegAlloc::default();
        let schema = materialize_from_subquery(&mut em, &mut reg, &subquery, &catalog, 10).unwrap();
        assert_eq!(schema.columns, vec!["a".to_string()]);
    }
}
