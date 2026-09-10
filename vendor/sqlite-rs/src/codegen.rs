// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! SQL codegen: compiles a parsed [`crate::parser::ast::Select`] into a
//! [`crate::vdbe::Program`] (spec 009, Requirements 7, 10, 11 — the
//! convergence ticket #91, needing #89's VDBE core and #90's cursor/
//! sorter/ephemeral opcodes). Expressions compile to jump-based control
//! flow, never an intermediate boolean register (Requirement 11).

pub mod analyze;
pub mod ddl;
pub mod dispatch;
pub mod expr;
pub(crate) mod index_maintenance;
pub mod pragma;
pub mod prepare;
pub mod select;
pub mod stmt;
pub(crate) mod subquery;
pub mod transaction;

pub use analyze::compile_analyze;
pub use ddl::{
    compile_create_index, compile_create_table, compile_create_view, compile_drop_index,
    compile_drop_table,
};
pub use dispatch::{compile_statement, leading_keywords, DispatchError};
pub use pragma::compile_pragma;
pub use prepare::{compile_select_program, result_column_names, PrepareError, SelectOutcome};
pub use select::{
    compile_select, compile_select_compound, compile_select_joined, compile_select_with_catalog,
    compile_select_with_catalog_and_stats, explain_query_plan, output_column_names, CodegenError,
    EqpRow,
};
pub use stmt::delete::{compile_delete, compile_delete_with_catalog};
pub use stmt::insert::compile_insert;
pub use stmt::update::{compile_update, compile_update_with_catalog};
pub use stmt::{delete, insert, update};
pub use subquery::{
    expand_with_clause, flatten_from_subqueries, push_down_where_predicates,
    resolve_from_table_schema, resolve_views, ExpandViews, ResolvedView,
};
pub use transaction::{compile_begin, compile_commit, compile_rollback};

use std::collections::HashMap;

use crate::vdbe::{Instruction, Opcode, Program, P4};

/// A not-yet-resolved jump target, placed later via [`Emitter::place`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct Label(usize);

/// Where a boolean condition's true/false outcome continues: either an
/// explicit jump target, or "fall through to the next emitted
/// instruction" — the classic jumping-code-generation technique (Aho
/// et al.), used throughout `expr.rs` so AND/OR/CASE compose without
/// materializing an intermediate boolean register (Requirement 11).
#[derive(Debug, Clone, Copy)]
pub(crate) enum Target {
    Jump(Label),
    Fallthrough,
}

/// Where a condition's *unknown* (SQL NULL) outcome continues — SQLite's
/// own `jumpIfNull` flag (`sqlite3ExprIfTrue`/`sqlite3ExprIfFalse`),
/// carried as the third field of [`CondTargets`].
///
/// It names one of the other two targets rather than being a third
/// [`Target`] of its own, on purpose. NULL is never an independent
/// continuation in practice: `WHERE` folds it into false (a NULL
/// predicate excludes the row), and `NOT` must leave it pinned to the
/// same address while swapping which of the two targets that address
/// is — which [`CondTargets::negate`] does in one line. An absolute
/// third label would have to be rewritten every time `AND`/`OR`
/// synthesize a fresh false/true label, and, worse, would be
/// unrepresentable for `AND`/`OR` at all: `NULL AND false` is *false*,
/// so a genuinely independent unknown continuation could not be taken
/// until the second operand had been evaluated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NullTarget {
    /// NULL continues where [`CondTargets::on_true`] does.
    True,
    /// NULL continues where [`CondTargets::on_false`] does — what
    /// `WHERE`, `CASE WHEN`, and every other boolean consumer in V2
    /// wants.
    False,
}

/// The full jump-mode contract: where a condition's true, false, and
/// unknown outcomes each continue. Bundled rather than passed as three
/// parameters because [`negate`](CondTargets::negate) has to move all
/// three together — swapping true and false without flipping
/// `on_null` is precisely the #134 bug.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CondTargets {
    pub(crate) on_true: Target,
    pub(crate) on_false: Target,
    pub(crate) on_null: NullTarget,
}

impl CondTargets {
    /// The setting every boolean consumer in V2 wants: unknown joins
    /// false.
    pub(crate) fn null_is_false(on_true: Target, on_false: Target) -> Self {
        CondTargets {
            on_true,
            on_false,
            on_null: NullTarget::False,
        }
    }

    /// Unknown joins true — used only to separate "definitely false"
    /// from "unknown" when materializing a condition into a register.
    pub(crate) fn null_is_true(on_true: Target, on_false: Target) -> Self {
        CondTargets {
            on_true,
            on_false,
            on_null: NullTarget::True,
        }
    }

    /// The contract for the operand of a `NOT`: true and false trade
    /// places, and `on_null` flips so the unknown outcome still names
    /// the address it named before the swap.
    pub(crate) fn negate(self) -> Self {
        CondTargets {
            on_true: self.on_false,
            on_false: self.on_true,
            on_null: match self.on_null {
                NullTarget::True => NullTarget::False,
                NullTarget::False => NullTarget::True,
            },
        }
    }

    pub(crate) fn with_true(self, on_true: Target) -> Self {
        CondTargets { on_true, ..self }
    }

    pub(crate) fn with_false(self, on_false: Target) -> Self {
        CondTargets { on_false, ..self }
    }
}

/// Builds a [`Program`] with forward-referenceable jump targets:
/// `new_label`/`place` mark an address, `patch_p2` records a pending
/// fixup (every jump-carrying opcode this ticket emits targets `P2`),
/// and `finish` resolves every pending fixup in one pass.
#[derive(Debug, Default)]
pub(crate) struct Emitter {
    instructions: Vec<Instruction>,
    labels: HashMap<Label, usize>,
    patches: Vec<(usize, Label)>,
    next_label: usize,
}

impl Emitter {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn emit(&mut self, instr: Instruction) -> usize {
        self.instructions.push(instr);
        self.instructions.len().saturating_sub(1)
    }

    pub(crate) fn here(&self) -> usize {
        self.instructions.len()
    }

    pub(crate) fn new_label(&mut self) -> Label {
        let label = Label(self.next_label);
        self.next_label = self.next_label.saturating_add(1);
        label
    }

    /// Binds `label` to the current (next-to-be-emitted) address.
    pub(crate) fn place(&mut self, label: Label) {
        self.labels.insert(label, self.here());
    }

    pub(crate) fn patch_p2(&mut self, addr: usize, label: Label) {
        self.patches.push((addr, label));
    }

    /// Overwrites an already-emitted instruction's `P4`, for cases
    /// where the value (e.g. a sort-key descriptor) isn't known until
    /// after later instructions — computing it requires registers that
    /// only get allocated once the code between the placeholder and
    /// the fixup has been emitted — have already been generated.
    pub(crate) fn patch_p4(&mut self, addr: usize, p4: P4) {
        if let Some(instr) = self.instructions.get_mut(addr) {
            instr.p4 = p4;
        }
    }

    /// Resolves every pending patch against its placed label's address,
    /// consuming the emitter into a finished [`Program`].
    pub(crate) fn finish(mut self) -> Program {
        for (addr, label) in &self.patches {
            let Some(&resolved) = self.labels.get(label) else {
                continue; // Every patched label is always placed by construction; skip defensively rather than panic.
            };
            #[allow(clippy::cast_possible_wrap)]
            let target = resolved as i32;
            if let Some(instr) = self.instructions.get_mut(*addr) {
                instr.p2 = target;
            }
        }
        Program::new(self.instructions)
    }

    /// Emits an unconditional jump to `label`, patched once placed.
    pub(crate) fn goto(&mut self, label: Label) {
        let addr = self.emit(Instruction::new(Opcode::Goto, 0, 0, 0));
        self.patch_p2(addr, label);
    }
}

/// A monotonically-increasing register bump allocator — the simplest
/// correct scheme for V2's scope; SQLite's real register allocator
/// reuses freed slots, which this deliberately does not (known
/// simplification, not a TODO to chase further).
#[derive(Debug)]
pub(crate) struct RegAlloc {
    next: i32,
    /// Next bind-parameter index to hand out for a bare `?`
    /// (`ParamKind::Anonymous`) — 1-based, matching SQLite's
    /// `sqlite3_bind_*` convention and `Opcode::Variable`'s `P1`.
    next_param: u32,
    /// Next cursor number to hand out for a subquery's own scan (#238) —
    /// started well above every fixed cursor constant this compiler's
    /// other features use (`TABLE_CURSOR`/`SORT_CURSOR`/`PSEUDO_CURSOR`/
    /// `DISTINCT_CURSOR`, plus one per joined table), so a subquery's
    /// cursor never collides with the enclosing query's — subqueries in
    /// the same statement are never open concurrently (each fully scans
    /// and closes over before the next expression compiles), so a
    /// single monotonically-increasing counter suffices without needing
    /// to reason about lifetimes across subqueries.
    next_cursor: i32,
    /// Every `FROM`-subquery already materialized earlier in this same
    /// statement's compile, keyed by structural equality of its
    /// `Select` — the cursor it was materialized into, plus its
    /// synthetic output schema (#425). A `WITH`-clause CTE referenced N
    /// times rewrites into N independent but byte-for-byte identical
    /// `TableRefKind::Subquery` clones (preserving self-join
    /// correctness — `subquery/cte.rs`'s module doc); the first one to
    /// materialize (`OpenEphemeral`+populate) gets cached here, and
    /// every later structurally-identical reference just `OpenDup`s
    /// onto that cursor instead of re-running the query. A `Vec` with a
    /// linear scan rather than a `HashMap` (`Select` has no cheap
    /// hash) — bounded by the number of distinct FROM-subqueries in
    /// one statement, always small. Scoped to one `RegAlloc` (one
    /// top-level statement compile). **Safe only as long as no volatile/
    /// non-deterministic expression exists in this crate** — see
    /// `subquery::from_clause::materialize_from_subquery`'s doc for the
    /// full caveat and what must change the day one is added.
    materialized_ctes: Vec<(crate::parser::ast::Select, i32, crate::schema::TableSchema)>,
}

impl Default for RegAlloc {
    fn default() -> Self {
        RegAlloc {
            next: 0,
            next_param: 0,
            next_cursor: 1000,
            materialized_ctes: Vec::new(),
        }
    }
}

impl RegAlloc {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Hands out a fresh cursor number for a subquery's own table scan
    /// or ephemeral materialization (#238).
    pub(crate) fn alloc_cursor(&mut self) -> i32 {
        let c = self.next_cursor;
        self.next_cursor = self.next_cursor.saturating_add(1);
        c
    }

    /// The cursor/schema a structurally-identical `FROM`-subquery was
    /// already materialized into earlier in this same statement's
    /// compile, if any (#425).
    pub(crate) fn cached_cte(
        &self,
        subquery: &crate::parser::ast::Select,
    ) -> Option<(i32, crate::schema::TableSchema)> {
        self.materialized_ctes
            .iter()
            .find(|(cached, _, _)| cached == subquery)
            .map(|(_, cursor, schema)| (*cursor, schema.clone()))
    }

    /// Records that `subquery` has now been materialized into `cursor`,
    /// with the given synthetic output schema, so a later structurally-
    /// identical reference reuses it instead of materializing again
    /// (#425).
    pub(crate) fn cache_cte(
        &mut self,
        subquery: &crate::parser::ast::Select,
        cursor: i32,
        schema: crate::schema::TableSchema,
    ) {
        self.materialized_ctes
            .push((subquery.clone(), cursor, schema));
    }

    pub(crate) fn alloc(&mut self) -> i32 {
        let r = self.next;
        self.next = self.next.saturating_add(1);
        r
    }

    /// The register the next `alloc()` call would hand out, without
    /// allocating it — used to find the highest register a just-compiled
    /// expression touched (its last-allocated register isn't always its
    /// own return value, e.g. `CASE` allocates its destination first).
    pub(crate) fn peek(&self) -> i32 {
        self.next
    }

    /// Assigns register-independent parameter index for a bare `?`,
    /// incrementing past any `?NNN` index already claimed via
    /// [`RegAlloc::numbered_param`].
    pub(crate) fn anonymous_param(&mut self) -> u32 {
        self.next_param = self.next_param.saturating_add(1);
        self.next_param
    }

    /// Claims an explicit `?NNN` parameter index, advancing
    /// `next_param` past it so a later bare `?` doesn't collide.
    pub(crate) fn numbered_param(&mut self, n: u32) -> u32 {
        self.next_param = self.next_param.max(n);
        n
    }
}

pub(crate) fn p4_coll_seq(
    collation: crate::vdbe::Collation,
    affinity: crate::vdbe::Affinity,
) -> P4 {
    P4::CollSeq {
        collation,
        affinity: affinity.to_p4_byte(),
    }
}

/// One table bound into a query's [`Scope`]: its cursor number, and the
/// schema/alias used to resolve `table.column`/bare `column` references
/// against it. Owns its `TableSchema` (a small, `Clone` metadata struct)
/// rather than borrowing it — this crate's qualified subset (`make
/// mvl-limit`) forbids explicit lifetimes in `src/`, so a borrowed
/// `Scope<'a>` is not an option here.
#[derive(Debug, Clone)]
pub(crate) struct TableBinding {
    /// The table's `AS alias`, if any. Once a table is aliased, SQLite
    /// no longer accepts the real table name as a qualifier — `resolve`
    /// preserves that: it matches `alias` when present, `name`
    /// otherwise, never both.
    pub(crate) alias: Option<String>,
    pub(crate) name: String,
    pub(crate) schema: crate::schema::TableSchema,
    pub(crate) cursor: i32,
    /// #237's LEFT JOIN null-extension: when true, every column read
    /// against this binding compiles to `Opcode::Null` instead of a
    /// real `Column`/`Rowid` read against `cursor` — `cursor` may not
    /// even hold a matching row (or any row at all) in that case. Set
    /// by the join codegen's "no match found" branch; always `false`
    /// for [`Scope::single`] and for an INNER/CROSS-joined table.
    pub(crate) forced_null: bool,
    /// This table's `ANALYZE` statistics (#461, spec 011/Req 4), empty
    /// (the `Default`) when no caller has threaded real
    /// `planner::load_stats` output through — `join_access::
    /// choose_join_access` only ever *vetoes* a structurally-available
    /// index seek using this, never invents one, so an empty `Stats`
    /// (no `ANALYZE` has run, or the caller hasn't wired stats through
    /// this particular codegen path yet) reproduces this ticket's
    /// pre-#461 behavior exactly.
    pub(crate) stats: crate::planner::Stats,
}

impl TableBinding {
    /// Whether `table` (a `Column` expression's optional qualifier)
    /// names this binding — the alias when present, the bare table name
    /// otherwise.
    pub(crate) fn matches_qualifier(&self, table: &str) -> bool {
        match &self.alias {
            Some(alias) => alias.eq_ignore_ascii_case(table),
            None => self.name.eq_ignore_ascii_case(table),
        }
    }
}

/// The set of tables a `FROM` clause's column references resolve
/// against — one binding for a plain single-table `SELECT`, one per
/// table for a join chain (#237). [`Scope::resolve`] is the single entry
/// point `expr.rs` uses instead of the old `schema: &TableSchema, cursor:
/// i32` parameter pair.
#[derive(Debug, Clone, Default)]
pub(crate) struct Scope {
    pub(crate) tables: Vec<TableBinding>,
    /// The full table catalog (#238), used only to resolve a subquery
    /// expression's (`Subquery`/`Exists`/`InSubquery`) own `FROM` table
    /// — a subquery may name a table that isn't part of the enclosing
    /// query's own `FROM` clause at all, so `tables` above (the
    /// enclosing scope's own bindings) isn't enough. Empty for every
    /// caller that never compiles a subquery-bearing expression (most
    /// of `delete.rs`/`insert.rs`/`update.rs`, and any `Scope::single`/
    /// literal-construction call site that hasn't opted in via
    /// [`Scope::with_catalog`]) — a subquery reached through one of
    /// those compiles to `CodegenError::Unsupported` (no table found)
    /// rather than silently resolving against the wrong catalog.
    pub(crate) catalog: Vec<crate::schema::TableSchema>,
    /// A correlated subquery's enclosing scope (#238 follow-up): set on
    /// the `Scope` built for a subquery's own `FROM` table(s) so
    /// [`Scope::resolve`] can fall back to it once this scope's own
    /// `tables` fails to resolve a reference. `None` for every ordinary
    /// (non-subquery) scope. SQL's scoping rule — the subquery's own
    /// tables shadow the enclosing query's, never the other way round —
    /// falls out for free from trying `self.tables` first and `outer`
    /// only on failure, rather than merging the two table lists.
    pub(crate) outer: Option<Box<Scope>>,
    /// #250's NATURAL/USING codegen: `dedup_star[i]` names the columns
    /// (lowercased) that a plain `SELECT *` expansion must skip for
    /// `tables[i]` — the right-hand side of a NATURAL/USING join
    /// suppresses its copy of each shared column so only the left-most
    /// table's value survives in `*` output, matching real SQLite.
    /// Always a same-length-as-`tables` vec of empty sets outside a
    /// joined query. `table.*` and explicit column references are
    /// unaffected — this only gates the [`select::ResultColumn::Star`]
    /// expansion path.
    pub(crate) dedup_star: Vec<std::collections::HashSet<String>>,
    /// Uncorrelated WHERE-clause subqueries hoisted out of the enclosing
    /// single-table scan loop and materialized exactly once, before the
    /// scan's `Rewind` (#306) — keyed by [`subquery::select_id`] of the
    /// subquery's own `Select` AST node. `compile_cond`/`compile_value`'s
    /// `InSubquery`/`Subquery` dispatch consult this map first, reading
    /// the precomputed cursor/register instead of re-materializing the
    /// subquery on every outer row. Empty (the default) for every scope
    /// that isn't a single-table scan's own — a correlated subquery, or
    /// one outside this pass's scope (see
    /// [`subquery::hoist_uncorrelated_where_subqueries`]'s doc comment),
    /// is simply never inserted, so its lookup misses and it falls
    /// through to the unmodified per-row materialization path.
    pub(crate) hoisted: std::rc::Rc<HashMap<usize, subquery::HoistedSubquery>>,
    /// Correlated WHERE-clause scalar subqueries memoized against a
    /// single-table scan's per-row correlated value (#314) — keyed by
    /// [`subquery::select_id`], same shape as [`Scope::hoisted`] but for
    /// the correlated case #306 explicitly left untouched (a correlated
    /// subquery's result can differ per row, so it can't be
    /// materialized once — but it *can* be cached per distinct value of
    /// the one outer column it's correlated against, which is what
    /// [`subquery::memoize_correlated_where_subqueries`] sets up).
    /// Empty (the default) outside a single-table scan's own scope, or
    /// for any correlated subquery whose shape falls outside that
    /// pass's narrow recognition (see its own doc comment) — such a
    /// subquery's lookup simply misses and it falls through to the
    /// unmodified per-row [`subquery::compile_scalar_subquery`] path.
    pub(crate) memoized: std::rc::Rc<HashMap<usize, subquery::MemoizedSubquery>>,
}

impl Scope {
    /// The single-table case — every pre-#237 call site's `schema`/
    /// `cursor` pair, wrapped so `expr.rs`'s signatures can be uniform
    /// over 1..N tables without duplicating codegen for the N=1 case.
    pub(crate) fn single(schema: &crate::schema::TableSchema, cursor: i32) -> Self {
        Scope {
            tables: vec![TableBinding {
                alias: None,
                name: schema.name.clone(),
                schema: schema.clone(),
                cursor,
                forced_null: false,
                stats: crate::planner::Stats::default(),
            }],
            catalog: Vec::new(),
            outer: None,
            dedup_star: vec![std::collections::HashSet::new()],
            hoisted: std::rc::Rc::default(),
            memoized: std::rc::Rc::default(),
        }
    }

    /// Attaches the full table catalog (#238) so subquery expressions
    /// compiled against this scope can resolve their own `FROM` table
    /// even when it isn't one of `tables` above.
    pub(crate) fn with_catalog(mut self, catalog: Vec<crate::schema::TableSchema>) -> Self {
        self.catalog = catalog;
        self
    }

    /// Marks this scope as a (possibly) correlated subquery's own scope,
    /// with `outer` as the enclosing query's scope to fall back to —
    /// see [`Scope::outer`]'s doc comment for the shadowing rule this
    /// implements.
    pub(crate) fn with_outer(mut self, outer: Scope) -> Self {
        self.outer = Some(Box::new(outer));
        self
    }

    /// Attaches a single-table scan's hoisted-uncorrelated-subquery map
    /// (#306) — see [`Scope::hoisted`]'s doc comment.
    pub(crate) fn with_hoisted(
        mut self,
        hoisted: std::rc::Rc<HashMap<usize, subquery::HoistedSubquery>>,
    ) -> Self {
        self.hoisted = hoisted;
        self
    }

    /// Attaches a single-table scan's memoized-correlated-subquery map
    /// (#314) — see [`Scope::memoized`]'s doc comment.
    pub(crate) fn with_memoized(
        mut self,
        memoized: std::rc::Rc<HashMap<usize, subquery::MemoizedSubquery>>,
    ) -> Self {
        self.memoized = memoized;
        self
    }

    /// Resolves a `table.name`/bare `name` column reference to
    /// `(cursor, column_index, schema, forced_null)`. `table: Some(_)`
    /// matches the alias-or-name qualifier exactly (see
    /// [`TableBinding::matches_qualifier`]); `table: None` searches
    /// every binding and rejects more than one match as ambiguous —
    /// SQLite's own rule for an unqualified column shared by two joined
    /// tables. `forced_null` is [`TableBinding::forced_null`]'s value
    /// for whichever binding resolved — see its doc comment.
    ///
    /// Tries this scope's own `tables` first; only on failure does it
    /// fall back to `self.outer` (a correlated reference) if set — so a
    /// name that resolves in both this scope and an enclosing one binds
    /// to this scope, matching SQL's shadowing rule, and two same-named
    /// columns split across this scope and `outer` are never reported
    /// ambiguous (only same-scope ambiguity is, per the existing rule
    /// below).
    pub(crate) fn resolve(
        &self,
        table: Option<&str>,
        name: &str,
    ) -> Result<(i32, usize, &crate::schema::TableSchema, bool), select::CodegenError> {
        match self.resolve_own(table, name) {
            Ok(v) => Ok(v),
            Err(own_err) => match &self.outer {
                Some(outer) => outer.resolve(table, name),
                None => Err(own_err),
            },
        }
    }

    fn resolve_own(
        &self,
        table: Option<&str>,
        name: &str,
    ) -> Result<(i32, usize, &crate::schema::TableSchema, bool), select::CodegenError> {
        let binding_idx = self.resolve_own_binding_index(table, name)?;
        let binding =
            self.tables
                .get(binding_idx)
                .ok_or_else(|| select::CodegenError::UnknownColumn {
                    name: name.to_string(),
                })?;
        let idx = expr::column_index(&binding.schema, name).unwrap_or(0);
        Ok((binding.cursor, idx, &binding.schema, binding.forced_null))
    }

    /// The qualifier-match-or-ambiguity rule behind [`Scope::resolve_own`]:
    /// `table: Some(_)` matches the alias-or-name qualifier exactly
    /// against `self.tables` (see [`TableBinding::matches_qualifier`]);
    /// `table: None` searches every binding and rejects more than one
    /// match as ambiguous. Returns the matching binding's *position*
    /// within `self.tables` rather than its `cursor` — needed by callers
    /// (e.g. `select/join_access.rs::resolve_scope_column`) that must
    /// compute an offset into a flat, all-bindings-concatenated row
    /// rather than read through a per-binding cursor.
    pub(crate) fn resolve_own_binding_index(
        &self,
        table: Option<&str>,
        name: &str,
    ) -> Result<usize, select::CodegenError> {
        if let Some(table) = table {
            let idx = self
                .tables
                .iter()
                .position(|b| b.matches_qualifier(table))
                .ok_or_else(|| select::CodegenError::UnknownColumn {
                    name: format!("{table}.{name}"),
                })?;
            let binding =
                self.tables
                    .get(idx)
                    .ok_or_else(|| select::CodegenError::UnknownColumn {
                        name: format!("{table}.{name}"),
                    })?;
            expr::column_index(&binding.schema, name).ok_or_else(|| {
                select::CodegenError::UnknownColumn {
                    name: format!("{table}.{name}"),
                }
            })?;
            return Ok(idx);
        }
        let mut found: Option<usize> = None;
        for (i, binding) in self.tables.iter().enumerate() {
            if expr::column_index(&binding.schema, name).is_some() {
                if found.is_some() {
                    return Err(select::CodegenError::AmbiguousColumn {
                        name: name.to_string(),
                    });
                }
                found = Some(i);
            }
        }
        found.ok_or_else(|| select::CodegenError::UnknownColumn {
            name: name.to_string(),
        })
    }
}
