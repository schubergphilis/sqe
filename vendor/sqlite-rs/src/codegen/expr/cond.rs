// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Jump-mode condition compilation — see `super`'s module doc.

use super::value::{collation_of, compile_value, expr_affinity, expr_collation};
use crate::codegen::{
    p4_coll_seq, CodegenError, CondTargets, Emitter, Label, NullTarget, RegAlloc, Scope, Target,
};
use crate::parser::ast::{BinaryOp, Expr, ExprKind, UnaryOp};
use crate::schema::TableSchema;
use crate::vdbe::{comparison_affinity, Affinity, Collation, Instruction, Opcode};

/// Resolves a bare `Expr::Column` name against a single schema; any
/// other expression is a codegen error only when a caller specifically
/// requires a plain column (there is no such requirement in this
/// module — kept for callers like `select.rs`'s ORDER BY/DISTINCT
/// column-index lookups, and [`crate::codegen::Scope::resolve`]'s own
/// per-binding lookup).
pub(crate) fn column_index(schema: &TableSchema, name: &str) -> Option<usize> {
    schema
        .columns
        .iter()
        .position(|c| c.eq_ignore_ascii_case(name))
}

/// #581: a rough, static (no `ANALYZE` data needed) cost class for an
/// expression, used only to order `AND`/`OR` operands cheapest-first so
/// short-circuit evaluation skips the pricier side more often — never
/// to change what a query returns. Lower is cheaper. `Paren` is
/// transparent (its inner cost is the whole thing's cost); everything
/// not explicitly classified (`Case`, `Cast`, `Like`, ...) gets the
/// arithmetic tier rather than the cheapest one, since none of them are
/// as cheap as a bare column/literal read.
fn cost_class(expr: &Expr) -> u8 {
    match &expr.kind {
        ExprKind::Paren(inner) => cost_class(inner),
        ExprKind::Literal(_) | ExprKind::Param(_) | ExprKind::Column { .. } => 0,
        ExprKind::Unary { expr: inner, .. } => cost_class(inner).max(1),
        ExprKind::Binary { op, lhs, rhs } => {
            let base = match op {
                BinaryOp::And | BinaryOp::Or => 0,
                _ => 1,
            };
            cost_class(lhs).max(cost_class(rhs)).max(base)
        }
        ExprKind::FunctionCall { .. } => 2,
        ExprKind::Subquery(_)
        | ExprKind::Exists { .. }
        | ExprKind::InSubquery { .. }
        | ExprKind::InSubqueryMulti { .. } => 3,
        _ => 1,
    }
}

/// Whether `rhs` is strictly cheaper (by [`cost_class`]) than `lhs`,
/// i.e. whether a caller should swap them so the cheaper operand
/// compiles first — safe for `AND`/`OR` because both are commutative
/// under SQL's three-valued logic and SQLite's expression language has
/// no operand with an evaluation side effect that order could
/// observably change (#581). Ties keep the original left-to-right
/// order.
fn rhs_is_cheaper(lhs: &Expr, rhs: &Expr) -> bool {
    cost_class(rhs) < cost_class(lhs)
}

/// Compiles `expr` as a boolean condition. [`CondTargets`] says where
/// control continues on each of the three outcomes: `on_true`/
/// `on_false` are real jump labels (or "fall through to the next
/// emitted instruction"), and `on_null` names which of those two the
/// *unknown* outcome joins — SQL is three-valued, and a jump has two
/// destinations.
///
/// Callers that want SQL's `WHERE` semantics use
/// [`CondTargets::null_is_false`]: a predicate whose truth is unknown
/// excludes the row exactly like a false one. What they must NOT do is
/// assume that stays true under negation — `NOT unknown` is still
/// unknown, so [`CondTargets::negate`] swaps the two targets *and*
/// flips `on_null`, leaving the unknown outcome on the address it
/// already had (#134).
pub(crate) fn compile_cond(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    scope: &Scope,
    expr: &Expr,
    targets: CondTargets,
) -> Result<(), CodegenError> {
    match &expr.kind {
        ExprKind::Paren(inner) => compile_cond(em, reg, scope, inner, targets),

        // Swapping the targets is right — it is what SQLite's own
        // `sqlite3ExprIfTrue`/`sqlite3ExprIfFalse` pair does for
        // `TK_NOT` — but only once `on_null` comes along for the
        // ride. Flipping it keeps the unknown outcome on the same
        // address across the swap; without that (the #134 bug) NULL
        // silently inherited whichever target had just become "false",
        // i.e. the keep-the-row one, and `WHERE NOT (x = 5)` returned
        // rows where `x IS NULL`.
        ExprKind::Unary {
            op: UnaryOp::Not,
            expr: inner,
        } => compile_cond(em, reg, scope, inner, targets.negate()),

        ExprKind::Binary {
            op: BinaryOp::And,
            lhs,
            rhs,
        } => {
            // `on_false` must be a real label before `lhs` compiles
            // — if it were left as `Fallthrough`, "fall through" would
            // wrongly mean "continue into rhs's test code" (the next
            // thing physically emitted) rather than the AND's actual
            // false continuation, which only exists after `rhs` compiles.
            //
            // `on_null` passes to both operands unchanged, and that
            // is exactly three-valued AND, both ways round. With
            // `NullTarget::False`: an unknown `lhs` goes straight to
            // the false continuation, which is right because every
            // completion of `unknown AND rhs` (false, or unknown) lands
            // there too. With `NullTarget::True`: an unknown `lhs`
            // falls through into `rhs`'s test, which is also right —
            // `unknown AND false` is false and reaches `false_label`
            // via `rhs`, while `unknown AND true`/`unknown AND unknown`
            // are unknown and reach `on_true`, where NULL belongs
            // under this setting.
            //
            // #581: AND is commutative under three-valued logic, so
            // whichever operand is cheaper (by [`cost_class`]) compiles
            // first — the eagerly-evaluated slot — letting the pricier
            // operand's evaluation be skipped whenever the cheap one
            // already resolves the jump.
            let (first, second) = if rhs_is_cheaper(lhs, rhs) {
                (rhs, lhs)
            } else {
                (lhs, rhs)
            };
            let (false_label, is_new) = ensure_label(em, targets.on_false);
            let operand = targets.with_false(Target::Jump(false_label));
            compile_cond(
                em,
                reg,
                scope,
                first,
                operand.with_true(Target::Fallthrough),
            )?;
            compile_cond(em, reg, scope, second, operand)?;
            if is_new {
                em.place(false_label);
            }
            Ok(())
        }

        ExprKind::Binary {
            op: BinaryOp::Or,
            lhs,
            rhs,
        } => {
            // Symmetric to `And` above: `on_true` must be a real
            // label before `lhs` compiles, or a `Fallthrough` true
            // would wrongly land in `rhs`'s test code instead of OR's
            // actual true continuation. `on_null` threads through
            // unchanged for the mirror-image reason: under
            // `NullTarget::True` an unknown `lhs` jumps straight to
            // `true_label` (every completion of `unknown OR rhs` is
            // true or unknown, and both belong there), and under
            // `NullTarget::False` it falls into `rhs`, which decides
            // between true and the false/unknown continuation.
            //
            // #581: same cheapest-first reordering as AND above — OR
            // is equally commutative under three-valued logic.
            let (first, second) = if rhs_is_cheaper(lhs, rhs) {
                (rhs, lhs)
            } else {
                (lhs, rhs)
            };
            let (true_label, is_new) = ensure_label(em, targets.on_true);
            let operand = targets.with_true(Target::Jump(true_label));
            compile_cond(
                em,
                reg,
                scope,
                first,
                operand.with_false(Target::Fallthrough),
            )?;
            compile_cond(em, reg, scope, second, operand)?;
            if is_new {
                em.place(true_label);
            }
            Ok(())
        }

        ExprKind::Binary { op, lhs, rhs }
            if matches!(
                op,
                BinaryOp::Eq
                    | BinaryOp::Ne
                    | BinaryOp::Lt
                    | BinaryOp::Le
                    | BinaryOp::Gt
                    | BinaryOp::Ge
            ) =>
        {
            let collation = collation_of(lhs)
                .or_else(|| collation_of(rhs))
                .or_else(|| expr_collation(scope, lhs))
                .or_else(|| expr_collation(scope, rhs));
            let affinity =
                comparison_affinity(expr_affinity(scope, lhs), expr_affinity(scope, rhs));
            let l = compile_value(em, reg, scope, lhs)?;
            let r = compile_value(em, reg, scope, rhs)?;
            emit_compare_false_jump(em, *op, l, r, collation, affinity, targets)
        }

        ExprKind::Is { lhs, rhs, negated } => {
            // `a IS b`: true when both NULL, or both non-NULL and
            // equal — unlike `=`, never propagates NULL to "unknown".
            // No single opcode expresses this; compute it into a
            // 0/1 register first, then test truthiness like any other
            // value-mode boolean (LIKE/GLOB take the same shape).
            // `on_null` is deliberately ignored here and in the
            // `IsNull` arm below: these two are the only conditions in
            // SQL that are always definitely true or definitely false,
            // so they have no unknown outcome to route, and swapping
            // the targets for `negated` is sound without flipping it.
            let (t, f) = if *negated {
                (targets.on_false, targets.on_true)
            } else {
                (targets.on_true, targets.on_false)
            };
            let l = compile_value(em, reg, scope, lhs)?;
            let r = compile_value(em, reg, scope, rhs)?;
            let result = reg.alloc();
            let both_null = em.new_label();
            let done = em.new_label();
            let addr = em.emit(Instruction::new(Opcode::IsNull, l, 0, 0));
            em.patch_p2(addr, both_null);

            let eq_true = em.new_label();
            let addr = em.emit(Instruction::new(Opcode::Eq, l, 0, r));
            em.patch_p2(addr, eq_true);
            em.emit(Instruction::new(Opcode::Integer, 0, result, 0));
            em.goto(done);
            em.place(eq_true);
            em.emit(Instruction::new(Opcode::Integer, 1, result, 0));
            em.goto(done);

            em.place(both_null);
            let r_null = em.new_label();
            let addr = em.emit(Instruction::new(Opcode::IsNull, r, 0, 0));
            em.patch_p2(addr, r_null);
            em.emit(Instruction::new(Opcode::Integer, 0, result, 0));
            em.goto(done);
            em.place(r_null);
            em.emit(Instruction::new(Opcode::Integer, 1, result, 0));

            em.place(done);
            finish_bool(em, t, f, |em, false_label| {
                let addr = em.emit(Instruction::new(Opcode::IfNot, result, 0, 1));
                em.patch_p2(addr, false_label);
            });
            Ok(())
        }

        ExprKind::IsNull {
            expr: inner,
            negated,
        } => {
            let r = compile_value(em, reg, scope, inner)?;
            // negated=false is `IS NULL` (condition true when NULL);
            // negated=true is `IS NOT NULL` (condition true when not
            // NULL). Emit the opcode matching "jump when condition is
            // false" so `finish_bool` sees a uniform false-jump
            // primitive.
            // negated=false (IS NULL): condition true iff NULL, so its
            // false-jump primitive fires when NOT null -> `NotNull`.
            // negated=true (IS NOT NULL): condition true iff not NULL,
            // so its false-jump primitive fires when NULL -> `IsNull`.
            let false_jump_op = if *negated {
                Opcode::IsNull
            } else {
                Opcode::NotNull
            };
            finish_bool(em, targets.on_true, targets.on_false, |em, false_label| {
                let addr = em.emit(Instruction::new(false_jump_op, r, 0, 0));
                em.patch_p2(addr, false_label);
            });
            Ok(())
        }

        ExprKind::Between {
            expr: inner,
            lo,
            hi,
            negated,
        } => {
            // Lowered per SQLite's own rule: `x BETWEEN lo AND hi` is
            // `x >= lo AND x <= hi`, and `x NOT BETWEEN lo AND hi` is
            // `x < lo OR x > hi` — NOT the same shape with true/false
            // swapped. Swapping targets would make a NULL `x` (where
            // neither comparison jumps at all, per
            // `emit_compare_false_jump`'s three-valued contract) come
            // out *true* for `NOT BETWEEN`, when the honest answer is
            // "unknown", which WHERE excludes just like false.
            let cmp = |op, rhs: &Expr| Expr {
                kind: ExprKind::Binary {
                    op,
                    lhs: inner.clone(),
                    rhs: Box::new(rhs.clone()),
                },
                span: expr.span,
            };

            if *negated {
                let lt_lo = cmp(BinaryOp::Lt, lo);
                let gt_hi = cmp(BinaryOp::Gt, hi);
                let (t_label, t_is_new) = ensure_label(em, targets.on_true);
                let arm = targets.with_true(Target::Jump(t_label));
                compile_cond(em, reg, scope, &lt_lo, arm.with_false(Target::Fallthrough))?;
                compile_cond(em, reg, scope, &gt_hi, arm)?;
                if t_is_new {
                    em.place(t_label);
                }
            } else {
                let ge_lo = cmp(BinaryOp::Ge, lo);
                let le_hi = cmp(BinaryOp::Le, hi);
                let (f_label, f_is_new) = ensure_label(em, targets.on_false);
                let arm = targets.with_false(Target::Jump(f_label));
                compile_cond(em, reg, scope, &ge_lo, arm.with_true(Target::Fallthrough))?;
                compile_cond(em, reg, scope, &le_hi, arm)?;
                if f_is_new {
                    em.place(f_label);
                }
            }
            Ok(())
        }

        ExprKind::In {
            expr: inner,
            list,
            negated,
        } => {
            if list.is_empty() {
                // `x IN ()` is always false, even for a NULL `x` — an
                // empty list leaves nothing to be uncertain against.
                let (t, f) = if *negated {
                    (targets.on_false, targets.on_true)
                } else {
                    (targets.on_true, targets.on_false)
                };
                return compile_always_false(em, t, f);
            }

            // `IN`'s three outcomes — a definite match, a definite
            // non-match, or "unknown" (no match found, but `inner` or
            // some list item was NULL along the way) — don't collapse
            // to a single true/false jump the way other comparisons
            // do: `NOT IN`'s definite-non-match and unknown outcomes
            // diverge (`NOT FALSE` = true, `NOT NULL` = still NULL),
            // so a per-item comparison can't just swap targets like
            // `emit_compare_false_jump` does. `saw_null` is a small
            // exception to this module's "never an intermediate
            // boolean register" rule (shared with the `Is`/`IsNot`
            // handling above), needed to remember that exception past
            // the loop that discovers it.
            let l = compile_value(em, reg, scope, inner)?;
            let saw_null = reg.alloc();
            em.emit(Instruction::new(Opcode::Integer, 0, saw_null, 0));

            let (true_label, true_is_new) = ensure_label(em, targets.on_true);
            let (false_label, false_is_new) = ensure_label(em, targets.on_false);
            // A match found means IN is true; exhausting the list
            // without one means IN is false — `negated` (`NOT IN`)
            // swaps which final label each of those routes to. An
            // unknown outcome, below, always routes to `false_label`
            // regardless of `negated`: NULL is never true.
            let (found_label, unmatched_label) = if *negated {
                (false_label, true_label)
            } else {
                (true_label, false_label)
            };
            // `negated` does not move the unknown outcome — `NOT
            // unknown` is still unknown — so it routes by `on_null`
            // alone, exactly like every other arm (#134). Before that
            // ticket this was hardcoded to `false_label`, which was
            // right only because `WHERE` was the sole caller.
            let null_label = match targets.on_null {
                NullTarget::True => true_label,
                NullTarget::False => false_label,
            };

            let inner_null_addr = em.emit(Instruction::new(Opcode::IsNull, l, 0, 0));
            em.patch_p2(inner_null_addr, null_label);

            for item in list.iter() {
                let collation = collation_of(inner)
                    .or_else(|| collation_of(item))
                    .or_else(|| expr_collation(scope, inner))
                    .or_else(|| expr_collation(scope, item));
                let affinity =
                    comparison_affinity(expr_affinity(scope, inner), expr_affinity(scope, item));
                let p4 = p4_coll_seq(collation.unwrap_or(Collation::Binary), affinity);
                let r = compile_value(em, reg, scope, item)?;

                let item_null_label = em.new_label();
                let skip_label = em.new_label();
                let addr = em.emit(Instruction::new(Opcode::IsNull, r, 0, 0));
                em.patch_p2(addr, item_null_label);
                let addr = em.emit(Instruction::with_p4(Opcode::Eq, l, 0, r, p4));
                em.patch_p2(addr, found_label);
                em.goto(skip_label);

                em.place(item_null_label);
                em.emit(Instruction::new(Opcode::Integer, 1, saw_null, 0));
                em.place(skip_label);
            }

            // Exhausted the list without a match: route to
            // `unmatched_label` only if every comparison was a clean
            // non-match (`saw_null` still 0); otherwise at least one
            // comparison was against NULL, so the honest answer is
            // "unknown", which goes wherever `on_null` says.
            let addr = em.emit(Instruction::new(Opcode::IfNot, saw_null, 0, 0));
            em.patch_p2(addr, unmatched_label);
            em.goto(null_label);

            if false_is_new {
                em.place(false_label);
            }
            if true_is_new {
                em.place(true_label);
            }
            Ok(())
        }

        ExprKind::Like { .. } => {
            let r = compile_value(em, reg, scope, expr)?;
            finish_truthy(em, r, targets);
            Ok(())
        }

        // #238: EXISTS/NOT EXISTS never has an unknown outcome (see
        // `subquery::compile_exists`'s doc comment), so `targets` is
        // handed through as-is rather than materialized via
        // `compile_bool_to_value`.
        ExprKind::Exists { subquery, negated } => {
            crate::codegen::subquery::compile_exists(em, reg, scope, subquery, *negated, targets)
        }

        ExprKind::InSubquery {
            expr: inner,
            subquery,
            negated,
        } => crate::codegen::subquery::compile_in_subquery(
            em, reg, scope, inner, subquery, *negated, targets,
        ),

        ExprKind::InSubqueryMulti {
            exprs,
            subquery,
            negated,
        } => crate::codegen::subquery::compile_in_subquery_multi(
            em, reg, scope, exprs, subquery, *negated, targets,
        ),

        // A scalar subquery used directly as a boolean condition (e.g.
        // `WHERE (SELECT x FROM t)`): compute its value, then test
        // truthiness like any other value-mode boolean.
        ExprKind::Subquery(_) => {
            let r = compile_value(em, reg, scope, expr)?;
            finish_truthy(em, r, targets);
            Ok(())
        }

        // Any other expression used in boolean context (a bare column,
        // a function call, CASE, etc.): evaluate to a value and test
        // truthiness the same way as LIKE above.
        _ => {
            let r = compile_value(em, reg, scope, expr)?;
            finish_truthy(em, r, targets);
            Ok(())
        }
    }
}

/// Tests an already-computed value register for truthiness as a
/// three-valued condition. `IfNot`'s `P3` flag folds NULL into the
/// false jump, which covers `NullTarget::False` in one instruction;
/// the other setting needs an explicit `IsNull` probe first, because
/// no single opcode routes NULL and falsy to *different* addresses.
pub(super) fn finish_truthy(em: &mut Emitter, r: i32, targets: CondTargets) {
    match targets.on_null {
        NullTarget::False => {
            finish_bool(em, targets.on_true, targets.on_false, |em, false_label| {
                let addr = em.emit(Instruction::new(Opcode::IfNot, r, 0, 1));
                em.patch_p2(addr, false_label);
            });
        }
        NullTarget::True => {
            let (t_label, t_is_new) = ensure_label(em, targets.on_true);
            let addr = em.emit(Instruction::new(Opcode::IsNull, r, 0, 0));
            em.patch_p2(addr, t_label);
            // NULL is already gone, so `IfNot`'s P3 stays 0 and the
            // remaining test is the plain two-valued one.
            finish_bool(
                em,
                Target::Jump(t_label),
                targets.on_false,
                |em, false_label| {
                    let addr = em.emit(Instruction::new(Opcode::IfNot, r, 0, 0));
                    em.patch_p2(addr, false_label);
                },
            );
            if t_is_new {
                em.place(t_label);
            }
        }
    }
}

/// `x IN ()` / other statically-false conditions: jump to the false
/// target (or fall through if it's already the fallthrough), never
/// touching the true one.
fn compile_always_false(
    em: &mut Emitter,
    _true_target: Target,
    false_target: Target,
) -> Result<(), CodegenError> {
    if let Target::Jump(label) = false_target {
        em.goto(label);
    }
    Ok(())
}

/// Given a primitive that emits a "jump to `false_label` when the
/// condition is false, fall through when true" instruction, resolves
/// the full `(on_true, on_false)` combination — inserting an
/// extra `Goto` when both are already real jump targets.
pub(super) fn finish_bool(
    em: &mut Emitter,
    true_target: Target,
    false_target: Target,
    emit_false_jump: impl FnOnce(&mut Emitter, Label),
) {
    match (true_target, false_target) {
        (Target::Fallthrough, Target::Jump(f)) => emit_false_jump(em, f),
        (Target::Jump(t), Target::Fallthrough) => {
            let synth = em.new_label();
            emit_false_jump(em, synth);
            em.goto(t);
            em.place(synth);
        }
        (Target::Jump(t), Target::Jump(f)) => {
            emit_false_jump(em, f);
            em.goto(t);
        }
        (Target::Fallthrough, Target::Fallthrough) => {
            // Both branches continue at the same point — the condition
            // was evaluated only for a side effect (never emitted by
            // this module), so give false-jump a label that resolves to
            // "here" too.
            let synth = em.new_label();
            emit_false_jump(em, synth);
            em.place(synth);
        }
    }
}

/// Emits the appropriate compare opcode as a "jump to `false_label` on
/// false" primitive, then resolves `true_target`/`false_target` via
/// [`finish_bool`]. `Ne` has no dedicated opcode — it's `Eq`'s
/// complement, so its false-jump primitive is a plain `Eq` jump.
/// Resolves `target` to a real label usable as an immediate jump
/// destination, returning whether that label still needs `em.place`-ing
/// (i.e. it was freshly synthesized for a `Fallthrough` target rather
/// than an already-real `Jump`).
pub(crate) fn ensure_label(em: &mut Emitter, target: Target) -> (Label, bool) {
    match target {
        Target::Jump(l) => (l, false),
        Target::Fallthrough => (em.new_label(), true),
    }
}

/// Compiles a comparison as a jump. Always uses a true-jump-then-Goto
/// shape (never the tempting "jump on the complementary opcode"
/// shortcut): the compare opcodes (spec 009, Requirement 5) never jump
/// at all when either operand is NULL (`compare_jump`'s own three-
/// valued-logic contract), so a complement-based false-jump would
/// silently treat a NULL comparison as "condition holds" instead of
/// "condition's truth is unknown, which WHERE excludes just like
/// false" — this shape treats NULL and FALSE identically (both fail to
/// reach the true label, so both fall into the `Goto false_target`),
/// matching WHERE's own three-valued semantics for every comparator
/// uniformly, at the cost of one extra `Goto` versus the minimal
/// oracle-matching instruction count.
fn emit_compare_false_jump(
    em: &mut Emitter,
    op: BinaryOp,
    lhs: i32,
    rhs: i32,
    collation: Option<Collation>,
    affinity: Affinity,
    targets: CondTargets,
) -> Result<(), CodegenError> {
    let p4 = p4_coll_seq(collation.unwrap_or(Collation::Binary), affinity);
    // `Ne` has no opcode of its own; it's `Eq` with true/false swapped
    // — and that swap needs `null_target` flipped with it for the same
    // reason `NOT` does (#134). Without the flip, `WHERE x <> 5`
    // returned rows where `x IS NULL`: the NULL comparison reached the
    // trailing `Goto`, which the swap had just repointed at the
    // keep-the-row target. The caller only ever passes a comparison
    // operator (guarded by its own `matches!` filter), so `Some` always
    // holds; a non-comparison op is a codegen-internal error, not a
    // reachable SQL-input case.
    let resolved = match op {
        BinaryOp::Ne => Some((Opcode::Eq, targets.negate())),
        BinaryOp::Eq => Some((Opcode::Eq, targets)),
        BinaryOp::Lt => Some((Opcode::Lt, targets)),
        BinaryOp::Le => Some((Opcode::Le, targets)),
        BinaryOp::Gt => Some((Opcode::Gt, targets)),
        BinaryOp::Ge => Some((Opcode::Ge, targets)),
        _ => None,
    };
    let Some((opcode, targets)) = resolved else {
        return Err(CodegenError::Unsupported {
            reason: "emit_compare_false_jump called with a non-comparison operator".to_string(),
        });
    };
    let (t_label, t_is_new) = ensure_label(em, targets.on_true);
    // A NULL operand makes the compare opcode not jump at all, so it
    // otherwise always lands on `f`. When the unknown outcome belongs
    // with `t` instead, probe for it explicitly first — SQLite spells
    // the same thing as the `SQLITE_JUMPIFNULL` bit in the compare
    // instruction's P5, which this instruction format does not carry.
    if targets.on_null == NullTarget::True {
        let addr = em.emit(Instruction::new(Opcode::IsNull, lhs, 0, 0));
        em.patch_p2(addr, t_label);
        let addr = em.emit(Instruction::new(Opcode::IsNull, rhs, 0, 0));
        em.patch_p2(addr, t_label);
    }
    let addr = em.emit(Instruction::with_p4(opcode, lhs, 0, rhs, p4));
    em.patch_p2(addr, t_label);
    if let Target::Jump(fl) = targets.on_false {
        em.goto(fl);
    }
    if t_is_new {
        em.place(t_label);
    }
    Ok(())
}
