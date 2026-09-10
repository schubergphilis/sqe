// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Value-mode expression compilation — see `super`'s module doc.

use super::cond::compile_cond;
use crate::codegen::{CodegenError, CondTargets, Emitter, RegAlloc, Scope, Target};
use crate::parser::ast::{BinaryOp, Expr, ExprKind, Literal, ParamKind, UnaryOp};
use crate::schema::TableSchema;
use crate::vdbe::{affinity_of, Affinity, Collation, Instruction, Opcode, P4};

/// Reads column `idx` of the row at `cursor` into `dest`, emitting
/// `Rowid` rather than `Column` for a rowid-alias column. A table's
/// `INTEGER PRIMARY KEY` column is stored as a NULL placeholder in
/// every record (spike 003 finding 1) — reading it with `Column` yields
/// NULL, so `SELECT x FROM t WHERE x=2` silently matched nothing until
/// this substitution existed. `src/dump.rs` has always done the same
/// thing; this is the compiled read path catching up.
///
/// Every column read in the compiled path must come through here.
/// `select.rs`'s result-column expansion emitted a bare `Column`
/// instead, which is why `SELECT *` still answered NULL for an
/// `INTEGER PRIMARY KEY` long after `SELECT id` was fixed.
pub(crate) fn emit_column_read(
    em: &mut Emitter,
    schema: &TableSchema,
    cursor: i32,
    idx: usize,
    dest: i32,
) -> Result<(), CodegenError> {
    if schema.rowid_alias == Some(idx) {
        em.emit(Instruction::new(Opcode::Rowid, cursor, dest, 0));
        return Ok(());
    }
    em.emit(Instruction::new(
        Opcode::Column,
        cursor,
        i32::try_from(idx).map_err(|_| CodegenError::Unsupported {
            reason: format!("column index {idx} does not fit in a P2 operand"),
        })?,
        dest,
    ));
    // SQLite's on-disk format may store a REAL value using the integer-0/1
    // serial type optimization (file format doc, serial types 8/9) when
    // the value is losslessly an integer — independent of the column's
    // declared affinity. Real SQLite's OP_Column for a REAL-affinity
    // column is always followed by OP_RealAffinity to undo that
    // optimization on read; without it, `SELECT r FROM t` for a REAL
    // column holding `0.0` answered `0` instead of `0.0` (#143).
    if schema
        .column_types
        .get(idx)
        .is_some_and(|t| affinity_of(t) == Affinity::Real)
    {
        em.emit(Instruction::new(Opcode::RealAffinity, dest, 0, 0));
    }
    Ok(())
}

/// Whether this call is one of SQLite's built-in aggregates
/// (`func.c`'s aggregate registry). `max`/`min` are overloaded: the
/// one-argument form is the aggregate, but `max(a, b)` is an ordinary
/// scalar function, so arity — not the name alone — decides.
pub(crate) fn is_aggregate_call(name: &str, args: &crate::parser::ast::FunctionArgs) -> bool {
    let arity = match args {
        crate::parser::ast::FunctionArgs::Star => 0,
        crate::parser::ast::FunctionArgs::List(list) => list.len(),
    };
    match name.to_ascii_lowercase().as_str() {
        "avg" | "count" | "group_concat" | "string_agg" | "sum" | "total" => true,
        "max" | "min" => arity <= 1,
        _ => false,
    }
}

/// An expression's own affinity, per SQLite's `sqlite3ExprAffinity`
/// (spec 008 Requirement 1's comparison-affinity half, #138): a bare
/// column carries its declared-type affinity, a `CAST` carries its
/// target type's affinity, and a parenthesized expression defers to
/// its inner expression. Every other expression (literals, function
/// calls, arithmetic) has no affinity of its own — matching SQLite,
/// where only columns and casts do.
pub(crate) fn expr_affinity(scope: &Scope, expr: &Expr) -> Option<Affinity> {
    match &expr.kind {
        ExprKind::Column { table, name, .. } => {
            let (_, idx, schema, _) = scope.resolve(table.as_deref(), name).ok()?;
            let declared = schema.column_types.get(idx)?;
            Some(affinity_of(declared))
        }
        ExprKind::Cast { type_name, .. } => Some(affinity_of(type_name)),
        ExprKind::Paren(inner) => expr_affinity(scope, inner),
        _ => None,
    }
}

/// If `expr` is `x COLLATE name`, resolves `name` to a [`Collation`];
/// unrecognized collation names fall back to `None` (BINARY default).
pub(crate) fn collation_of(expr: &Expr) -> Option<Collation> {
    match &expr.kind {
        ExprKind::Collate { collation, .. } => {
            // `eq_ignore_ascii_case` rather than an uppercased copy —
            // collation names are ASCII, and this runs per COLLATE
            // expression during codegen (#590 item 8).
            if collation.eq_ignore_ascii_case("BINARY") {
                Some(Collation::Binary)
            } else if collation.eq_ignore_ascii_case("NOCASE") {
                Some(Collation::NoCase)
            } else if collation.eq_ignore_ascii_case("RTRIM") {
                Some(Collation::RTrim)
            } else {
                None
            }
        }
        ExprKind::Paren(inner) => collation_of(inner),
        _ => None,
    }
}

/// An expression's collation for comparison purposes (#500): an explicit
/// `x COLLATE name` always wins (matching SQLite's own precedence), and
/// otherwise a bare column falls back to its schema-declared `COLLATE`
/// (default [`Collation::Binary`] when the column has none). Mirrors
/// [`expr_affinity`]'s column-resolution shape.
pub(crate) fn expr_collation(scope: &Scope, expr: &Expr) -> Option<Collation> {
    if let Some(collation) = collation_of(expr) {
        return Some(collation);
    }
    match &expr.kind {
        ExprKind::Column { table, name, .. } => {
            let (_, idx, schema, _) = scope.resolve(table.as_deref(), name).ok()?;
            schema.column_collations.get(idx).copied()
        }
        ExprKind::Paren(inner) => expr_collation(scope, inner),
        _ => None,
    }
}

/// Compiles `expr` into a fresh register holding its value (value
/// mode) — used for result columns, function arguments, CASE branch
/// results, and as the operand feed for jump-mode comparisons.
pub(crate) fn compile_value(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    scope: &Scope,
    expr: &Expr,
) -> Result<i32, CodegenError> {
    match &expr.kind {
        ExprKind::Paren(inner) => compile_value(em, reg, scope, inner),
        ExprKind::Collate { expr: inner, .. } => compile_value(em, reg, scope, inner),

        ExprKind::Literal(lit) => {
            let r = reg.alloc();
            match lit {
                Literal::Integer(i) => {
                    // #142: `Opcode::Integer`'s P1 is i32-only, so a
                    // literal outside that range (but within i64) loads
                    // via `Int64`'s P4-carried i64 immediate instead —
                    // the harvested 64-bit counterpart, not a codegen
                    // error.
                    match i32::try_from(*i) {
                        Ok(p1) => {
                            em.emit(Instruction::new(Opcode::Integer, p1, r, 0));
                        }
                        Err(_) => {
                            em.emit(Instruction::with_p4(Opcode::Int64, 0, r, 0, P4::Int(*i)));
                        }
                    }
                }
                Literal::True => {
                    em.emit(Instruction::new(Opcode::Integer, 1, r, 0));
                }
                Literal::False => {
                    em.emit(Instruction::new(Opcode::Integer, 0, r, 0));
                }
                Literal::Str(s) => {
                    em.emit(Instruction::with_p4(
                        Opcode::String8,
                        0,
                        r,
                        0,
                        P4::Str(s.clone()),
                    ));
                }
                // #142: a real literal loads as an actual `Value::Real`
                // via the harvested `Real` opcode, not `String8` text
                // relying on coercion at comparison/arithmetic time
                // (the #138 bug this used to cause).
                Literal::Float(f) => {
                    em.emit(Instruction::with_p4(Opcode::Real, 0, r, 0, P4::Real(*f)));
                }
                // #142: a blob literal loads as an actual `Value::Blob`
                // via the harvested `Blob` opcode — hex-text never
                // actually coerced back to a blob (BLOB affinity never
                // converts text to blob, matching SQLite), so
                // `WHERE b = x'41'` always failed under the old scheme.
                Literal::Blob(bytes) => {
                    let len =
                        i32::try_from(bytes.len()).map_err(|_| CodegenError::Unsupported {
                            reason: format!(
                                "blob literal of {} bytes does not fit in a P1 operand",
                                bytes.len()
                            ),
                        })?;
                    em.emit(Instruction::with_p4(
                        Opcode::Blob,
                        len,
                        r,
                        0,
                        P4::Blob(bytes.clone()),
                    ));
                }
                Literal::Null => {} // Fresh registers already read as NULL.
            }
            Ok(r)
        }

        // `?` and `?NNN` compile to `Variable`, reading whatever the
        // caller bound via `Vm::bind_params`/`execute_with_params`
        // (#137).
        //
        // Named forms (`:name`/`@name`/`$name`) are rejected rather than
        // compiled (013/Req 3). They are still not wired to an index — that
        // was out of #137's bounded scope and remains out of scope here —
        // but the previous behaviour was to emit nothing and hand back a
        // fresh (NULL-reading) register. That turns `WHERE x = :name` into
        // `WHERE x = NULL`, which matches no row and raises no error: a
        // silent wrong answer, and the worst possible failure mode for a
        // consumer binding parameters by name. Refusing to compile is the
        // honest answer until the indices exist.
        ExprKind::Param(kind) => {
            let index = match kind {
                ParamKind::Anonymous => reg.anonymous_param(),
                ParamKind::Numbered(n) => reg.numbered_param(*n),
                ParamKind::Colon(name) | ParamKind::At(name) | ParamKind::Dollar(name) => {
                    let sigil = match kind {
                        ParamKind::At(_) => '@',
                        ParamKind::Dollar(_) => '$',
                        _ => ':',
                    };
                    return Err(CodegenError::Unsupported {
                        reason: format!(
                            "named parameter {sigil}{name} is not supported — bind by position                              with ? or ?NNN"
                        ),
                    });
                }
            };
            let r = reg.alloc();
            let p1 = i32::try_from(index).map_err(|_| CodegenError::Unsupported {
                reason: format!("parameter index {index} is out of range"),
            })?;
            em.emit(Instruction::new(Opcode::Variable, p1, r, 0));
            Ok(r)
        }

        ExprKind::Column { table, name, .. } => {
            let (cursor, idx, schema, forced_null) = scope.resolve(table.as_deref(), name)?;
            let r = reg.alloc();
            if forced_null {
                // #237's LEFT JOIN null-extension: this binding has no
                // matching row (or `cursor` may not even be positioned
                // on live data at all), so every column reads as NULL
                // rather than going through a real `Column`/`Rowid`
                // read.
                em.emit(Instruction::new(Opcode::Null, 0, r, 0));
            } else {
                emit_column_read(em, schema, cursor, idx, r)?;
            }
            Ok(r)
        }

        ExprKind::FunctionCall { name, args, .. } => {
            // Aggregates need a grouping/accumulator pass this V2
            // compiler doesn't have. Rejecting them is not just a
            // missing-feature guard: compiling one as an ordinary
            // scalar `Function` call emits it *per row*, so
            // `SELECT count(*) FROM t` silently returns one row per
            // input row instead of a single count — wrong output is
            // worse than a refusal.
            if is_aggregate_call(name, args) {
                return Err(CodegenError::Unsupported {
                    reason: format!("aggregate function {}", name.to_ascii_lowercase()),
                });
            }
            let arg_exprs = match args {
                crate::parser::ast::FunctionArgs::Star => &[][..],
                crate::parser::ast::FunctionArgs::List(list) => list.as_slice(),
            };
            // `Function` reads its arguments from a contiguous register
            // window starting at P2, so the args must land next to each
            // other. Reserving the window up front and *then* compiling
            // into it does not work: `compile_value` allocates its own
            // destination, so every argument landed past the reservation.
            // Instead, compile the args first and take the window from
            // where they actually landed — consecutive simple args are
            // naturally adjacent this way. When that does not hold (an
            // argument whose own lowering allocates its destination
            // before its operands, e.g. `coalesce(i, -1)` alongside
            // another such call), fall back to copying each arg into a
            // freshly reserved contiguous run (#141).
            let mut arg_regs = Vec::with_capacity(arg_exprs.len());
            for arg in arg_exprs.iter() {
                arg_regs.push(compile_value(em, reg, scope, arg)?);
            }
            let mut first = match arg_regs.first().copied() {
                Some(r) => r,
                // Zero-arg call (or `f(*)`): P2 still has to point at a
                // register, and nothing reads it.
                None => reg.alloc(),
            };
            let already_contiguous = arg_regs
                .iter()
                .enumerate()
                .all(|(i, &r)| r == first.saturating_add(i32::try_from(i).unwrap_or(i32::MAX)));
            if !already_contiguous {
                let dests: Vec<i32> = (0..arg_regs.len()).map(|_| reg.alloc()).collect();
                if let Some(&dest_first) = dests.first() {
                    first = dest_first;
                }
                for (&r, &dest) in arg_regs.iter().zip(&dests) {
                    em.emit(Instruction::new(Opcode::Copy, r, dest, 0));
                }
            }
            let dest = reg.alloc();
            em.emit(Instruction::with_p4(
                Opcode::Function,
                0,
                first,
                dest,
                P4::Str(format!(
                    "{}({})",
                    name.to_ascii_lowercase(),
                    arg_exprs.len()
                )),
            ));
            Ok(dest)
        }

        ExprKind::Like {
            expr: inner,
            pattern,
            glob,
            negated,
            escape,
        } => {
            let (name, arity) = match escape {
                Some(_) if !*glob => ("like", 3),
                _ if *glob => ("glob", 2),
                _ => ("like", 2),
            };
            // Registry argument order is (pattern, text[, escape]) —
            // the reverse of SQL's `text LIKE pattern` syntax. Compile
            // operands in that order so the bump allocator hands out a
            // contiguous run matching `Function`'s expected layout.
            let pat_r = compile_value(em, reg, scope, pattern)?;
            let txt_r = compile_value(em, reg, scope, inner)?;
            if txt_r != pat_r.saturating_add(1) {
                return Err(CodegenError::Unsupported {
                    reason: "LIKE/GLOB text operand did not land in the register contiguous \
                             with its pattern operand"
                        .to_string(),
                });
            }
            if let Some(e) = escape {
                let esc_r = compile_value(em, reg, scope, e)?;
                if esc_r != pat_r.saturating_add(2) {
                    return Err(CodegenError::Unsupported {
                        reason: "LIKE ESCAPE operand did not land in the register contiguous \
                                 with its pattern/text operands"
                            .to_string(),
                    });
                }
            }
            let dest = reg.alloc();
            let p4 = P4::Str(format!("{name}({arity})"));
            em.emit(Instruction::with_p4(Opcode::Function, 0, pat_r, dest, p4));
            if *negated {
                let out = compile_negate_value(em, reg, dest);
                return Ok(out);
            }
            Ok(dest)
        }

        ExprKind::Unary { op, expr: inner } => match op {
            UnaryOp::Plus => compile_value(em, reg, scope, inner),
            UnaryOp::Minus => {
                let r = compile_value(em, reg, scope, inner)?;
                let zero = reg.alloc();
                em.emit(Instruction::new(Opcode::Integer, 0, zero, 0));
                let dest = reg.alloc();
                // Subtract: r[P3] = r[P2] - r[P1] -> 0 - r = -r via
                // P1=r, P2=zero.
                em.emit(Instruction::new(Opcode::Subtract, r, zero, dest));
                Ok(dest)
            }
            // `Not` is the whole reason this is not routed through
            // `compile_bool_to_value`: it is the oracle's own lowering
            // for `SELECT NOT x` (one instruction, verified against the
            // pinned 3.53.4 `EXPLAIN`), and it propagates NULL in a
            // register, which jump-mode code cannot do at all.
            UnaryOp::Not => {
                let r = compile_value(em, reg, scope, inner)?;
                let dest = reg.alloc();
                em.emit(Instruction::new(Opcode::Not, r, dest, 0));
                Ok(dest)
            }
            UnaryOp::BitNot => {
                let r = compile_value(em, reg, scope, inner)?;
                let dest = reg.alloc();
                em.emit(Instruction::new(Opcode::BitNot, r, dest, 0));
                Ok(dest)
            }
        },

        ExprKind::Binary { op, lhs, rhs }
            if matches!(
                op,
                BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod
            ) =>
        {
            let l = compile_value(em, reg, scope, lhs)?;
            let r = compile_value(em, reg, scope, rhs)?;
            let dest = reg.alloc();
            // The caller's own `matches!` filter guarantees `op` is one
            // of these five; any other value is a codegen-internal
            // error, not a reachable SQL-input case.
            let opcode = match op {
                BinaryOp::Add => Opcode::Add,
                BinaryOp::Sub => Opcode::Subtract,
                BinaryOp::Mul => Opcode::Multiply,
                BinaryOp::Div => Opcode::Divide,
                BinaryOp::Mod => Opcode::Remainder,
                _ => {
                    return Err(CodegenError::Unsupported {
                        reason: "arithmetic lowering reached with a non-arithmetic operator"
                            .to_string(),
                    })
                }
            };
            // Subtract/Divide/Remainder read as `r[P2] <op> r[P1]`
            // (SQLite's own operand order, per arithmetic.rs) — pass
            // (rhs=P1, lhs=P2) so `lhs <op> rhs` is what's computed.
            match opcode {
                Opcode::Subtract | Opcode::Divide | Opcode::Remainder => {
                    em.emit(Instruction::new(opcode, r, l, dest));
                }
                _ => {
                    em.emit(Instruction::new(opcode, l, r, dest));
                }
            }
            Ok(dest)
        }

        ExprKind::Binary { op, lhs, rhs }
            if matches!(
                op,
                BinaryOp::BitAnd
                    | BinaryOp::BitOr
                    | BinaryOp::Shl
                    | BinaryOp::Shr
                    | BinaryOp::Concat
            ) =>
        {
            let l = compile_value(em, reg, scope, lhs)?;
            let r = compile_value(em, reg, scope, rhs)?;
            let dest = reg.alloc();
            let opcode = match op {
                BinaryOp::BitAnd => Opcode::BitAnd,
                BinaryOp::BitOr => Opcode::BitOr,
                BinaryOp::Shl => Opcode::ShiftLeft,
                BinaryOp::Shr => Opcode::ShiftRight,
                BinaryOp::Concat => Opcode::Concat,
                _ => {
                    return Err(CodegenError::Unsupported {
                        reason: "bitwise/concat lowering reached with a non-bitwise operator"
                            .to_string(),
                    })
                }
            };
            // ShiftLeft/ShiftRight/Concat read as `r[P2] <op> r[P1]`
            // (SQLite's own operand order, verified against harvested
            // EXPLAIN) — pass (rhs=P1, lhs=P2) so `lhs <op> rhs` is what's
            // computed. BitAnd/BitOr are commutative, so operand order
            // doesn't change the result.
            match opcode {
                Opcode::ShiftLeft | Opcode::ShiftRight | Opcode::Concat => {
                    em.emit(Instruction::new(opcode, r, l, dest));
                }
                _ => {
                    em.emit(Instruction::new(opcode, l, r, dest));
                }
            }
            Ok(dest)
        }

        // Comparisons and the logical connectives are conditions used
        // in a value context: they answer true/false/unknown, which
        // `compile_bool_to_value` materializes three-valued. Before
        // #134 they fell into the catch-all below and compiled to a
        // bare NULL register, so `SELECT price = 10` answered NULL for
        // every row, NULL operand or not.
        ExprKind::Binary {
            op:
                BinaryOp::Eq
                | BinaryOp::Ne
                | BinaryOp::Lt
                | BinaryOp::Le
                | BinaryOp::Gt
                | BinaryOp::Ge
                | BinaryOp::And
                | BinaryOp::Or,
            ..
        } => compile_bool_to_value(em, reg, scope, expr),

        // Unreachable in practice: `BinaryOp` has no variant left
        // uncovered by the two arms above (#139). Kept as a defensive
        // fallback rather than a `_ => unreachable!()` so a future
        // `BinaryOp` addition fails soft (wrong answer) instead of
        // panicking mid-query.
        ExprKind::Binary { .. } => {
            let r = reg.alloc();
            em.emit(Instruction::new(Opcode::Null, 0, r, 0));
            Ok(r)
        }

        // #142: `CAST` forces its target affinity via the harvested
        // `Cast` opcode (P2 = the affinity's ASCII byte, matching the
        // oracle's own `EXPLAIN` shape: `Cast r[N], affinity(r[N])`),
        // never `MustBeInt`/`RealAffinity` — those are a guard opcode
        // (aborts instead of truncating, wrong for `CAST('apple' AS
        // INTEGER)` = `0`) and a column-load coercion opcode
        // respectively, neither of which implements `CAST`'s own lossy
        // conversion rule (`src/vdbe/cast.rs`).
        ExprKind::Cast {
            expr: inner,
            type_name,
        } => {
            let r = compile_value(em, reg, scope, inner)?;
            let affinity = affinity_of(type_name);
            let p2 = i32::from(affinity.to_p4_byte());
            em.emit(Instruction::new(Opcode::Cast, r, p2, 0));
            Ok(r)
        }

        ExprKind::Case {
            operand,
            whens,
            else_,
        } => {
            let dest = reg.alloc();
            let end_label = em.new_label();
            for (when_expr, then_expr) in whens {
                let next_label = em.new_label();
                let cond = match operand {
                    Some(op_expr) => Expr {
                        kind: ExprKind::Binary {
                            op: BinaryOp::Eq,
                            lhs: op_expr.clone(),
                            rhs: Box::new(when_expr.clone()),
                        },
                        span: when_expr.span,
                    },
                    None => when_expr.clone(),
                };
                // A `WHEN` whose condition is unknown is not a match,
                // exactly like a false one — `NullTarget::False`, the
                // same setting `WHERE` uses.
                compile_cond(
                    em,
                    reg,
                    scope,
                    &cond,
                    CondTargets::null_is_false(Target::Fallthrough, Target::Jump(next_label)),
                )?;
                emit_branch_into(em, reg, scope, then_expr, dest)?;
                em.goto(end_label);
                em.place(next_label);
            }
            // `dest` is a register slot the scan loop reuses every
            // iteration — a prior row's CASE result would otherwise
            // leak into this row's output when no WHEN matches and
            // there's no ELSE (registers don't reset between loop
            // iterations), so the no-match path always explicitly
            // (re)writes NULL rather than relying on "never written".
            match else_ {
                Some(else_expr) => emit_branch_into(em, reg, scope, else_expr, dest)?,
                None => {
                    // This used to fake a NULL with an out-of-range
                    // `Column` read; `Null` (#134) says what it means,
                    // and is what the oracle emits here.
                    em.emit(Instruction::new(Opcode::Null, 0, dest, 0));
                }
            }
            em.place(end_label);
            Ok(dest)
        }

        // Boolean-valued expressions used in a value context (e.g. `a
        // = b` as a result column) materialize 0/1 via the jump-mode
        // compiler, matching Requirement 11's shape even when the
        // condition's answer must land in a register.
        ExprKind::Is { .. }
        | ExprKind::IsNull { .. }
        | ExprKind::Between { .. }
        | ExprKind::In { .. }
        | ExprKind::Exists { .. }
        | ExprKind::InSubquery { .. }
        | ExprKind::InSubqueryMulti { .. } => compile_bool_to_value(em, reg, scope, expr),

        // #238: a scalar subquery in value position — `SELECT (SELECT
        // max(x) FROM t)`, `x = (SELECT ...)`, etc. #306: if this
        // subquery was hoisted (materialized once, before the enclosing
        // scan's `Rewind`, because it's uncorrelated), its result is
        // already sitting in a register — reuse it instead of
        // re-running the subquery's whole scan on every outer row.
        ExprKind::Subquery(subquery) => {
            let key = crate::codegen::subquery::select_id(subquery);
            match scope.hoisted.get(&key) {
                Some(crate::codegen::subquery::HoistedSubquery::Scalar { reg: r }) => Ok(*r),
                _ => match scope.memoized.get(&key) {
                    // #314: correlated, but memoized per distinct value
                    // of the one outer column it's correlated against.
                    Some(memo) => crate::codegen::subquery::compile_memoized_scalar_subquery(
                        em, reg, scope, subquery, memo,
                    ),
                    None => {
                        crate::codegen::subquery::compile_scalar_subquery(em, reg, scope, subquery)
                    }
                },
            }
        }
    }
}

/// Boolean negation of an already-computed value register (used by
/// `NOT LIKE`/`NOT GLOB`) into a fresh register. The old `IfNot`-based
/// 0/1 materialization resolved a NULL `src` to 1; `Not` propagates it
/// (#134), which is what `x NOT LIKE NULL` has to yield.
fn compile_negate_value(em: &mut Emitter, reg: &mut RegAlloc, src: i32) -> i32 {
    let out = reg.alloc();
    em.emit(Instruction::new(Opcode::Not, src, out, 0));
    out
}

/// CASE's branch results (each computed into its own register) must
/// land in one shared destination. `Literal` and `Column` branches are
/// re-emitted directly into `dest`; any other branch expression is
/// compiled via `compile_value` into its own register and `Copy`'d
/// into `dest` (#141) — evaluating straight into `dest` and leaving it
/// untouched on a re-entrant compile would otherwise risk a stale
/// register from a prior branch or a prior loop iteration leaking out
/// as this branch's result.
fn emit_branch_into(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    scope: &Scope,
    expr: &Expr,
    dest: i32,
) -> Result<(), CodegenError> {
    match &expr.kind {
        ExprKind::Literal(Literal::Integer(i)) => match i32::try_from(*i) {
            Ok(p1) => {
                em.emit(Instruction::new(Opcode::Integer, p1, dest, 0));
            }
            Err(_) => {
                em.emit(Instruction::with_p4(Opcode::Int64, 0, dest, 0, P4::Int(*i)));
            }
        },
        ExprKind::Literal(Literal::True) => {
            em.emit(Instruction::new(Opcode::Integer, 1, dest, 0));
        }
        ExprKind::Literal(Literal::False) => {
            em.emit(Instruction::new(Opcode::Integer, 0, dest, 0));
        }
        ExprKind::Literal(Literal::Str(s)) => {
            em.emit(Instruction::with_p4(
                Opcode::String8,
                0,
                dest,
                0,
                P4::Str(s.clone()),
            ));
        }
        ExprKind::Literal(Literal::Float(f)) => {
            em.emit(Instruction::with_p4(Opcode::Real, 0, dest, 0, P4::Real(*f)));
        }
        ExprKind::Literal(Literal::Blob(bytes)) => {
            let len = i32::try_from(bytes.len()).map_err(|_| CodegenError::Unsupported {
                reason: format!(
                    "blob literal of {} bytes does not fit in a P1 operand",
                    bytes.len()
                ),
            })?;
            em.emit(Instruction::with_p4(
                Opcode::Blob,
                len,
                dest,
                0,
                P4::Blob(bytes.clone()),
            ));
        }
        // `dest` is shared across branches and reused every scan
        // iteration, so an explicit NULL branch has to overwrite it.
        // Emitting nothing (the pre-#134 behavior, correct only for a
        // never-written fresh register) leaked the previous row's
        // result out of `SELECT CASE WHEN c THEN x ELSE NULL END`.
        ExprKind::Literal(Literal::Null) => {
            em.emit(Instruction::new(Opcode::Null, 0, dest, 0));
        }
        ExprKind::Column { table, name, .. } => {
            let (cursor, idx, schema, forced_null) = scope.resolve(table.as_deref(), name)?;
            if forced_null {
                em.emit(Instruction::new(Opcode::Null, 0, dest, 0));
            } else {
                emit_column_read(em, schema, cursor, idx, dest)?;
            }
        }
        _ => {
            let r = compile_value(em, reg, scope, expr)?;
            em.emit(Instruction::new(Opcode::Copy, r, dest, 0));
        }
    }
    Ok(())
}

/// Whether a condition's outcome is always definitely true or
/// definitely false — never SQL's unknown. `IS`/`IS NOT` and
/// `IS NULL`/`IS NOT NULL` are the only such conditions in the V2
/// grammar; they exist precisely to answer questions about NULL
/// without inheriting it.
fn is_definite(expr: &Expr) -> bool {
    match &expr.kind {
        ExprKind::Paren(inner) => is_definite(inner),
        // #238: EXISTS is always definitely true or false (see
        // `subquery::compile_exists`'s doc comment) — unlike
        // `InSubquery`, whose NULL-LHS case really is unknown.
        ExprKind::Is { .. } | ExprKind::IsNull { .. } | ExprKind::Exists { .. } => true,
        _ => false,
    }
}

/// Materializes a condition's answer into a register. A condition has
/// three possible answers and jump-mode code only has two
/// destinations, so a genuinely three-valued expression is compiled
/// twice: once asking "is it definitely true?" and once asking "is it
/// definitely false?" (the same condition with `NullTarget::True`, so
/// unknown separates from false instead of joining it). Anything that
/// answers neither is unknown, and lands on the `Null` opcode.
///
/// The alternative — a third continuation threaded through
/// `compile_cond` — does not work: `AND`/`OR` cannot route an unknown
/// left operand anywhere until the right one has been evaluated (see
/// `NullTarget`'s doc comment), so they would have to duplicate their
/// right operand's code anyway, once per path.
fn compile_bool_to_value(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    scope: &Scope,
    expr: &Expr,
) -> Result<i32, CodegenError> {
    let dest = reg.alloc();
    let true_label = em.new_label();
    let end_label = em.new_label();

    if is_definite(expr) {
        compile_cond(
            em,
            reg,
            scope,
            expr,
            CondTargets::null_is_false(Target::Jump(true_label), Target::Fallthrough),
        )?;
        em.emit(Instruction::new(Opcode::Integer, 0, dest, 0));
        em.goto(end_label);
        em.place(true_label);
        em.emit(Instruction::new(Opcode::Integer, 1, dest, 0));
        em.place(end_label);
        return Ok(dest);
    }

    let null_label = em.new_label();
    let false_label = em.new_label();
    // Pass 1: definitely true? Unknown joins false here, so reaching
    // the fallthrough means "false or unknown".
    compile_cond(
        em,
        reg,
        scope,
        expr,
        CondTargets::null_is_false(Target::Jump(true_label), Target::Fallthrough),
    )?;
    // Pass 2: which of the two was it? `NullTarget::True` sends
    // unknown to the true side, which pass 1 already ruled out, so
    // that side can only be reached by an unknown answer.
    compile_cond(
        em,
        reg,
        scope,
        expr,
        CondTargets::null_is_true(Target::Jump(null_label), Target::Jump(false_label)),
    )?;

    em.place(false_label);
    em.emit(Instruction::new(Opcode::Integer, 0, dest, 0));
    em.goto(end_label);
    em.place(null_label);
    em.emit(Instruction::new(Opcode::Null, 0, dest, 0));
    em.goto(end_label);
    em.place(true_label);
    em.emit(Instruction::new(Opcode::Integer, 1, dest, 0));
    em.place(end_label);
    Ok(dest)
}
