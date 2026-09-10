// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Statement dispatch: keyword-sniffs a raw SQL string to pick the right
//! parser/compiler pair for one INSERT/UPDATE/DELETE/CREATE TABLE/CREATE
//! INDEX/DROP TABLE/DROP INDEX statement (#292 — moved out of the CLI
//! binary so it's usable without depending on the binary crate, e.g. by
//! a future REPL).

use crate::parser::ast::{InsertSource, TableRefKind};
use crate::parser::error::ParseOutcome;
use crate::parser::error::{
    parse_analyze, parse_begin, parse_commit, parse_create_index, parse_create_table,
    parse_create_view, parse_delete, parse_drop_index, parse_drop_table, parse_insert,
    parse_pragma, parse_rollback, parse_update,
};
use crate::schema::{TableSchema, ViewSchema};
use crate::vdbe::Program;

use super::{
    compile_analyze, compile_begin, compile_commit, compile_create_index, compile_create_table,
    compile_create_view, compile_delete_with_catalog, compile_drop_index, compile_drop_table,
    compile_insert, compile_pragma, compile_rollback, compile_update_with_catalog,
    expand_with_clause, resolve_from_table_schema, resolve_views, CodegenError, ExpandViews,
};

/// Failure compiling one dispatched statement — everything
/// [`compile_statement`] can fail with, folded into one error type so
/// callers (the CLI, a future REPL) don't need to know about the
/// per-statement parser/codegen error types individually.
#[derive(Debug)]
pub enum DispatchError {
    /// The statement referenced a table not present in the schema catalog.
    NoSuchTable(String),

    /// The statement referenced an index not present in the schema catalog.
    NoSuchIndex(String),

    /// The leading keyword(s) didn't match any statement kind this
    /// dispatcher knows how to parse/compile.
    Unrecognized(String),

    /// A `SELECT` (or an embedding statement) had no `FROM` clause.
    NoFromClause,

    /// Compilation of the parsed statement failed.
    Codegen(CodegenError),

    /// Parsing the statement failed.
    ParseFailed(String),
}

impl std::fmt::Display for DispatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DispatchError::NoSuchTable(name) => write!(f, "no such table: {name}"),
            DispatchError::NoSuchIndex(name) => write!(f, "no such index: {name}"),
            DispatchError::Unrecognized(kw) => {
                write!(f, "unsupported or unrecognized statement: {kw:?} ...")
            }
            DispatchError::NoFromClause => write!(f, "SELECT has no FROM clause"),
            DispatchError::Codegen(source) => write!(f, "{source}"),
            DispatchError::ParseFailed(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for DispatchError {}

impl From<CodegenError> for DispatchError {
    fn from(source: CodegenError) -> Self {
        DispatchError::Codegen(source)
    }
}

/// The first one or two whitespace-separated words of `sql`, uppercased
/// — enough to pick which statement-specific parser to hand `sql` to
/// (`CREATE TABLE` vs `CREATE INDEX`/`CREATE UNIQUE INDEX`, `DROP TABLE`
/// vs `DROP INDEX`), without re-tokenizing the whole statement twice.
///
/// Kept as `Vec<String>` (rather than `[&str; 3]`) since callers outside
/// this module (the REPL's `leading_keywords` consumer) hold onto the
/// result past `sql`'s lifetime; the per-word allocation is unavoidable
/// there. `compile_statement` below instead does its own borrowed,
/// non-allocating scan for the hot dispatch path.
pub fn leading_keywords(sql: &str) -> Vec<String> {
    sql.split_whitespace()
        .take(3)
        .map(|w| w.to_ascii_uppercase())
        .collect()
}

/// Every leading word [`compile_statement`]'s dispatch branches on, in
/// canonical uppercase spelling — the entire vocabulary [`canonical`]
/// can return.
const DISPATCH_WORDS: &[&str] = &[
    "ANALYZE", "BEGIN", "COMMIT", "CREATE", "DELETE", "DROP", "END", "INDEX", "INSERT", "PRAGMA",
    "ROLLBACK", "TABLE", "UNIQUE", "UPDATE", "VIEW",
];

/// `word`'s canonical uppercase spelling if it's one of the statement
/// keywords dispatch branches on, else `""` — a `&'static str`, so
/// [`compile_statement`] can match on borrowed string literals without
/// allocating an uppercased copy of every leading word the way
/// [`leading_keywords`] does (#590 item 8). Any word outside this fixed
/// vocabulary maps to `""`, which matches no dispatch arm and so falls
/// through to `Unrecognized` exactly as an unknown keyword did before.
fn canonical(word: &str) -> &'static str {
    DISPATCH_WORDS
        .iter()
        .copied()
        .find(|candidate| candidate.eq_ignore_ascii_case(word))
        .unwrap_or("")
}

fn parse_error<T: std::fmt::Debug>(other: ParseOutcome<T>) -> DispatchError {
    DispatchError::ParseFailed(format!("{other:?}"))
}

/// Parses `sql`, picks the compiler for its leading keyword(s), and
/// compiles it against `schemas` — the `exec <file> "<SQL>"` CLI
/// subcommand's core (#215's write-path CLI surface), shared by any
/// future caller that needs to run a single INSERT/UPDATE/DELETE/CREATE
/// TABLE/CREATE INDEX/DROP TABLE/DROP INDEX statement against a known
/// catalog.
pub fn compile_statement(
    sql: &str,
    schemas: &[TableSchema],
    views: &[ViewSchema],
) -> Result<Program, DispatchError> {
    let find_schema = |name: &str| -> Result<&TableSchema, DispatchError> {
        schemas
            .iter()
            .find(|s| s.name.eq_ignore_ascii_case(name))
            .ok_or_else(|| DispatchError::NoSuchTable(name.to_string()))
    };
    let find_index_root = |name: &str| -> Result<u32, DispatchError> {
        schemas
            .iter()
            .flat_map(|s| &s.indexes)
            .find(|idx| idx.name.eq_ignore_ascii_case(name))
            .map(|idx| idx.root_page)
            .ok_or_else(|| DispatchError::NoSuchIndex(name.to_string()))
    };

    let mut words = sql.split_whitespace();
    let first_word = words.next().unwrap_or("");
    let head = canonical(first_word);
    let second = canonical(words.next().unwrap_or(""));

    match head {
        "BEGIN" => match parse_begin(sql) {
            ParseOutcome::Accepted(begin) => Ok(compile_begin(&begin)),
            other => Err(parse_error(other)),
        },
        "COMMIT" | "END" => match parse_commit(sql) {
            ParseOutcome::Accepted(commit) => Ok(compile_commit(&commit)),
            other => Err(parse_error(other)),
        },
        "ROLLBACK" => match parse_rollback(sql) {
            ParseOutcome::Accepted(rollback) => Ok(compile_rollback(&rollback)),
            other => Err(parse_error(other)),
        },
        "PRAGMA" => match parse_pragma(sql) {
            ParseOutcome::Accepted(pragma) => Ok(compile_pragma(&pragma)),
            other => Err(parse_error(other)),
        },
        "ANALYZE" => match parse_analyze(sql) {
            ParseOutcome::Accepted(analyze) => {
                let targets: Vec<&TableSchema> = match &analyze.target {
                    None => schemas.iter().collect(),
                    Some(name) => {
                        if let Some(schema) =
                            schemas.iter().find(|s| s.name.eq_ignore_ascii_case(name))
                        {
                            vec![schema]
                        } else if schemas
                            .iter()
                            .flat_map(|s| &s.indexes)
                            .any(|idx| idx.name.eq_ignore_ascii_case(name))
                        {
                            // Real SQLite also accepts `ANALYZE index-name`
                            // (analyzing just that index's owning table) —
                            // syntactically valid, but out of this MVP's
                            // scope (spec 011/Req 1), so `Unsupported`
                            // rather than the `NoSuchTable` a genuinely
                            // unknown name gets below.
                            return Err(CodegenError::Unsupported {
                                reason: format!(
                                    "ANALYZE of a single index ({name:?}) is not yet supported"
                                ),
                            }
                            .into());
                        } else {
                            return Err(DispatchError::NoSuchTable(name.clone()));
                        }
                    }
                };
                Ok(compile_analyze(&targets)?)
            }
            other => Err(parse_error(other)),
        },
        "INSERT" => match parse_insert(sql) {
            ParseOutcome::Accepted(mut insert) => {
                let schema = find_schema(&insert.table)?;
                let select_schemas: Option<Vec<TableSchema>> = match &insert.source {
                    InsertSource::Select(select) => {
                        // Same `WITH`/view expansion `compile_select_program`
                        // runs for a plain SELECT (#375/#380), so a CTE/view
                        // name in the source at least *resolves* against the
                        // catalog instead of failing with an unexplained "no
                        // such table". The INSERT codegen path below
                        // (`compile_insert`'s single-/joined-table scan)
                        // only knows how to scan a *real* table's root page,
                        // though — it doesn't yet drive `#257`'s FROM-
                        // subquery materialization the way a plain SELECT's
                        // codegen does — so a CTE/view expanding into a
                        // `TableRefKind::Subquery` here is rejected
                        // explicitly rather than falling through to
                        // `compile_insert` and failing with a confusing
                        // "invalid root page (0)" (the subquery's synthetic,
                        // rootpage-less schema).
                        let resolved_views = resolve_views(views);
                        let cte_expanded = expand_with_clause(select);
                        let expanded = cte_expanded.expand_views(&resolved_views)?;

                        let Some(from) = &expanded.from else {
                            return Err(DispatchError::NoFromClause);
                        };
                        let is_subquery = |r: &crate::parser::ast::TableRef| {
                            matches!(r.kind, TableRefKind::Subquery(_))
                        };
                        if is_subquery(&from.first)
                            || from.joins.iter().any(|j| is_subquery(&j.table))
                        {
                            return Err(CodegenError::Unsupported {
                                reason: "INSERT ... SELECT with a CTE or view source is not yet \
                                         supported"
                                    .to_string(),
                            }
                            .into());
                        }
                        let mut joined_schemas =
                            vec![resolve_from_table_schema(&from.first, schemas)?];
                        for join in &from.joins {
                            joined_schemas.push(resolve_from_table_schema(&join.table, schemas)?);
                        }
                        insert.source = InsertSource::Select(Box::new(expanded.into_owned()));
                        Some(joined_schemas)
                    }
                    InsertSource::Values(_) | InsertSource::DefaultValues => None,
                };
                Ok(compile_insert(&insert, schema, select_schemas.as_deref())?)
            }
            other => Err(parse_error(other)),
        },
        "UPDATE" => match parse_update(sql) {
            ParseOutcome::Accepted(update) => {
                let schema = find_schema(&update.table)?;
                Ok(compile_update_with_catalog(&update, schema, schemas)?)
            }
            other => Err(parse_error(other)),
        },
        "DELETE" => match parse_delete(sql) {
            ParseOutcome::Accepted(delete) => {
                let schema = find_schema(&delete.table)?;
                Ok(compile_delete_with_catalog(&delete, schema, schemas)?)
            }
            other => Err(parse_error(other)),
        },
        "CREATE" if second == "TABLE" => match parse_create_table(sql) {
            ParseOutcome::Accepted(create) => Ok(compile_create_table(&create, sql)?),
            other => Err(parse_error(other)),
        },
        "CREATE" if second == "VIEW" => match parse_create_view(sql) {
            ParseOutcome::Accepted(create) => Ok(compile_create_view(&create, sql)?),
            other => Err(parse_error(other)),
        },
        "CREATE" if second == "INDEX" || second == "UNIQUE" => match parse_create_index(sql) {
            ParseOutcome::Accepted(ci) => {
                let schema = find_schema(&ci.table)?;
                Ok(compile_create_index(&ci, schema, sql)?)
            }
            other => Err(parse_error(other)),
        },
        "DROP" if second == "TABLE" => match parse_drop_table(sql) {
            ParseOutcome::Accepted(drop) => {
                let schema = find_schema(&drop.name)?;
                Ok(compile_drop_table(&drop, schema)?)
            }
            other => Err(parse_error(other)),
        },
        "DROP" if second == "INDEX" => match parse_drop_index(sql) {
            ParseOutcome::Accepted(di) => {
                let root_page = find_index_root(&di.name)?;
                Ok(compile_drop_index(&di, root_page)?)
            }
            other => Err(parse_error(other)),
        },
        // Reports the statement's actual leading word (uppercased, as
        // before), not `canonical`'s `""` sentinel — this is a cold
        // path, so the one allocation is free.
        _ => Err(DispatchError::Unrecognized(first_word.to_ascii_uppercase())),
    }
}
