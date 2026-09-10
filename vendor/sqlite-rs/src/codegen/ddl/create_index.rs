// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! `CreateIndex` AST -> `Program` compilation (#215). A single
//! `Opcode::CreateIndex` instruction allocates the index's root page,
//! populates it with one entry per pre-existing row of the target table,
//! registers the row in `sqlite_master`, and bumps the schema cookie —
//! all at exec time, mirroring `create_table.rs`'s single-opcode shape.
//!
//! Column resolution and the `DESC`-column rejection mirror
//! `codegen::index_maintenance::emit_index_key_ops`'s reasoning exactly:
//! no index b-tree comparator in this codebase is aware of per-column
//! sort direction, so a `DESC` column would build a key that reads back
//! in the wrong order under a comparator that always treats it as
//! ascending.

use crate::codegen::expr::column_index;
use crate::codegen::select::CodegenError;
use crate::codegen::Emitter;
use crate::parser::ast::CreateIndex;
use crate::schema::TableSchema;
use crate::vdbe::{Instruction, Opcode, Program, P4};

/// Compiles `CREATE INDEX` into a single `Opcode::CreateIndex` instruction
/// that allocates the index's root page, populates it from the target
/// table's existing rows, registers it in `sqlite_master`, and bumps the
/// schema cookie at exec time.
pub fn compile_create_index(
    ci: &CreateIndex,
    schema: &TableSchema,
    source: &str,
) -> Result<Program, CodegenError> {
    let mut column_indices = Vec::with_capacity(ci.columns.len());
    for col in &ci.columns {
        if col.desc == Some(true) {
            return Err(CodegenError::Unsupported {
                reason: format!(
                    "index {} has a DESC column; descending index keys aren't supported yet",
                    ci.name
                ),
            });
        }
        let crate::parser::ast::ExprKind::Column { name, .. } = &col.expr.kind else {
            return Err(CodegenError::Unsupported {
                reason: format!(
                    "index {} indexes an expression, not a plain column; not supported yet",
                    ci.name
                ),
            });
        };
        let idx = column_index(schema, name).ok_or_else(|| CodegenError::Unsupported {
            reason: format!(
                "index {} references a column this codegen can't resolve: {}",
                ci.name, name
            ),
        })?;
        column_indices.push(idx);
    }

    let start = ci.span.offset as usize;
    let end = start.saturating_add(ci.span.len as usize);
    let sql = source
        .get(start..end)
        .ok_or_else(|| CodegenError::Unsupported {
            reason: "CREATE INDEX statement span out of bounds of the source text".to_string(),
        })?
        .to_string();

    let mut em = Emitter::new();
    let init_addr = em.emit(Instruction::new(Opcode::Init, 0, 0, 0));
    let body_start = em.new_label();
    em.place(body_start);
    em.patch_p2(init_addr, body_start);

    em.emit(Instruction::with_p4(
        Opcode::CreateIndex,
        0,
        0,
        0,
        P4::CreateIndex {
            name: ci.name.clone(),
            table_name: schema.name.clone(),
            table_root_page: schema.root_page,
            sql,
            column_indices,
            unique: ci.unique,
        },
    ));
    em.emit(Instruction::new(Opcode::Halt, 0, 0, 0));
    Ok(em.finish())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing, clippy::panic)]
mod tests {
    use super::*;
    use crate::parser::error::{parse_create_index, ParseOutcome};

    fn schema() -> TableSchema {
        TableSchema {
            unresolved_autoindex: false,
            name: "t".to_string(),
            root_page: 2,
            columns: vec!["a".to_string(), "b".to_string()],
            without_rowid: false,
            strict: false,
            column_types: vec!["INTEGER".to_string(), "TEXT".to_string()],
            column_collations: vec![],
            is_virtual: false,
            sql: "CREATE TABLE t(a INTEGER, b TEXT)".to_string(),
            indexes: vec![],
            rowid_alias: None,
        }
        .with_computed_rowid_alias()
    }

    #[test]
    fn resolves_column_and_carries_verbatim_sql() {
        let sql = "CREATE INDEX idx_t_b ON t(b)";
        let ci = match parse_create_index(sql) {
            ParseOutcome::Accepted(c) => c,
            other => panic!("expected Accepted, got {other:?}"),
        };
        let program = compile_create_index(&ci, &schema(), sql).unwrap();

        match &program.instructions[1].p4 {
            P4::CreateIndex {
                name,
                column_indices,
                sql: got_sql,
                ..
            } => {
                assert_eq!(name, "idx_t_b");
                assert_eq!(column_indices, &vec![1]);
                assert_eq!(got_sql, sql);
            }
            other => panic!("expected P4::CreateIndex, got {other:?}"),
        }
    }

    #[test]
    fn rejects_desc_column() {
        let sql = "CREATE INDEX idx_t_b ON t(b DESC)";
        let ci = match parse_create_index(sql) {
            ParseOutcome::Accepted(c) => c,
            other => panic!("expected Accepted, got {other:?}"),
        };
        let err = compile_create_index(&ci, &schema(), sql).unwrap_err();
        assert!(matches!(err, CodegenError::Unsupported { .. }));
    }

    #[test]
    fn rejects_expression_column() {
        let sql = "CREATE INDEX idx_t_expr ON t(a + 1)";
        let ci = match parse_create_index(sql) {
            ParseOutcome::Accepted(c) => c,
            other => panic!("expected Accepted, got {other:?}"),
        };
        let err = compile_create_index(&ci, &schema(), sql).unwrap_err();
        match err {
            CodegenError::Unsupported { reason } => {
                assert!(reason.contains("indexes an expression"), "{reason}");
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[test]
    fn rejects_unresolvable_column() {
        let sql = "CREATE INDEX idx_t_c ON t(c)";
        let ci = match parse_create_index(sql) {
            ParseOutcome::Accepted(c) => c,
            other => panic!("expected Accepted, got {other:?}"),
        };
        let err = compile_create_index(&ci, &schema(), sql).unwrap_err();
        match err {
            CodegenError::Unsupported { reason } => {
                assert!(reason.contains("can't resolve"), "{reason}");
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[test]
    fn rejects_out_of_bounds_span() {
        let sql = "CREATE INDEX idx_t_b ON t(b)";
        let mut ci = match parse_create_index(sql) {
            ParseOutcome::Accepted(c) => c,
            other => panic!("expected Accepted, got {other:?}"),
        };
        ci.span.offset = u32::MAX - 1;
        ci.span.len = 10;
        let err = compile_create_index(&ci, &schema(), sql).unwrap_err();
        match err {
            CodegenError::Unsupported { reason } => {
                assert!(reason.contains("out of bounds"), "{reason}");
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }
}
