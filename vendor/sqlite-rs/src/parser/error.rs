// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! The three-way SELECT-core parse outcome (spec 002-parser
//! Requirement 4), per spike 006 (#57)'s verdict: `sqlite3` only ever
//! accepts or rejects, but sqlite-rs's V2 slice additionally needs to
//! distinguish "syntactically valid SQL we haven't implemented yet"
//! (e.g. `JOIN`) from "actually malformed" — otherwise a not-yet-built
//! feature reads identically to a typo.

use super::ast::{
    Analyze, Begin, Commit, CreateIndex, CreateTable, CreateView, Delete, DropIndex, DropTable,
    DropView, Explain, Insert, Pragma, Rollback, Select, Update,
};
use super::grammar::Parser;
use super::tokenizer::{Span, Tokenizer};

/// The three-way result of attempting to parse a statement: cleanly
/// accepted, syntactically-valid-but-unimplemented, or genuinely malformed.
#[derive(Debug, Clone, PartialEq)]
pub enum ParseOutcome<T> {
    /// Parsed successfully into a `T` (e.g. [`Select`], [`Update`]).
    Accepted(Box<T>),
    /// Syntactically-recognized SQL this parser doesn't implement yet
    /// (joins, subqueries, compound selects, ...). `span` points at the
    /// token that triggered the unsupported construct.
    Unsupported {
        /// Human-readable description of the unsupported construct.
        message: String,
        /// Location of the token that triggered the unsupported construct.
        span: Span,
    },
    /// Malformed SQL: a genuine syntax error. `span` points at the
    /// offending token.
    Invalid {
        /// Human-readable description of the syntax error.
        message: String,
        /// Location of the offending token.
        span: Span,
    },
}

/// Failure carried internally by the recursive-descent parser; folded
/// into [`ParseOutcome::Unsupported`]/[`ParseOutcome::Invalid`] by
/// [`parse_select`].
#[derive(Debug, Clone, PartialEq)]
pub(super) enum ParseFail {
    Unsupported { message: String, span: Span },
    Invalid { message: String, span: Span },
}

pub(super) type PResult<T> = Result<T, ParseFail>;

/// Parses a single SELECT statement (spec 002-parser Requirements 2-4;
/// grammar `.openspec/grammar/sqlite.ebnf` V2 block). Never panics —
/// any input produces one of the three [`ParseOutcome`] variants.
pub fn parse_select(src: &str) -> ParseOutcome<Select> {
    let tokens = Tokenizer::tokenize(src);
    let mut parser = Parser::new(tokens);
    match parser.parse_select_stmt() {
        Ok(select) => match parser.expect_end() {
            Ok(()) => ParseOutcome::Accepted(Box::new(select)),
            Err(ParseFail::Unsupported { message, span }) => {
                ParseOutcome::Unsupported { message, span }
            }
            Err(ParseFail::Invalid { message, span }) => ParseOutcome::Invalid { message, span },
        },
        Err(ParseFail::Unsupported { message, span }) => {
            ParseOutcome::Unsupported { message, span }
        }
        Err(ParseFail::Invalid { message, span }) => ParseOutcome::Invalid { message, span },
    }
}

/// Parses `EXPLAIN [QUERY PLAN] select-stmt` (#243, grammar V4). Never
/// panics — any input produces one of the three [`ParseOutcome`]
/// variants. Bare `EXPLAIN` (no `QUERY PLAN`) and non-`SELECT` bodies
/// are `Unsupported`, not `Invalid` — syntactically recognized SQL this
/// entry point doesn't implement, per [`ParseOutcome`]'s three-way
/// contract.
pub fn parse_explain(src: &str) -> ParseOutcome<Explain> {
    let tokens = Tokenizer::tokenize(src);
    let mut parser = Parser::new(tokens);
    match parser.parse_explain_stmt() {
        Ok(explain) => match parser.expect_end() {
            Ok(()) => ParseOutcome::Accepted(Box::new(explain)),
            Err(ParseFail::Unsupported { message, span }) => {
                ParseOutcome::Unsupported { message, span }
            }
            Err(ParseFail::Invalid { message, span }) => ParseOutcome::Invalid { message, span },
        },
        Err(ParseFail::Unsupported { message, span }) => {
            ParseOutcome::Unsupported { message, span }
        }
        Err(ParseFail::Invalid { message, span }) => ParseOutcome::Invalid { message, span },
    }
}

/// Parses a single INSERT statement (grammar `.openspec/grammar/sqlite.ebnf`
/// V3 block). Never panics — any input produces one of the three
/// [`ParseOutcome`] variants.
pub fn parse_insert(src: &str) -> ParseOutcome<Insert> {
    let tokens = Tokenizer::tokenize(src);
    let mut parser = Parser::new(tokens);
    match parser.parse_insert_stmt() {
        Ok(insert) => match parser.expect_end() {
            Ok(()) => ParseOutcome::Accepted(Box::new(insert)),
            Err(ParseFail::Unsupported { message, span }) => {
                ParseOutcome::Unsupported { message, span }
            }
            Err(ParseFail::Invalid { message, span }) => ParseOutcome::Invalid { message, span },
        },
        Err(ParseFail::Unsupported { message, span }) => {
            ParseOutcome::Unsupported { message, span }
        }
        Err(ParseFail::Invalid { message, span }) => ParseOutcome::Invalid { message, span },
    }
}

/// Parses a single DELETE statement (grammar `.openspec/grammar/sqlite.ebnf`
/// V3 block). Never panics — any input produces one of the three
/// [`ParseOutcome`] variants.
pub fn parse_delete(src: &str) -> ParseOutcome<Delete> {
    let tokens = Tokenizer::tokenize(src);
    let mut parser = Parser::new(tokens);
    match parser.parse_delete_stmt() {
        Ok(delete) => match parser.expect_end() {
            Ok(()) => ParseOutcome::Accepted(Box::new(delete)),
            Err(ParseFail::Unsupported { message, span }) => {
                ParseOutcome::Unsupported { message, span }
            }
            Err(ParseFail::Invalid { message, span }) => ParseOutcome::Invalid { message, span },
        },
        Err(ParseFail::Unsupported { message, span }) => {
            ParseOutcome::Unsupported { message, span }
        }
        Err(ParseFail::Invalid { message, span }) => ParseOutcome::Invalid { message, span },
    }
}

/// Parses a single UPDATE statement (spec 002-parser V3 slice; grammar
/// `.openspec/grammar/sqlite.ebnf` `update-stmt`). Never panics — any
/// input produces one of the three [`ParseOutcome`] variants.
pub fn parse_update(src: &str) -> ParseOutcome<Update> {
    let tokens = Tokenizer::tokenize(src);
    let mut parser = Parser::new(tokens);
    match parser.parse_update_stmt() {
        Ok(update) => match parser.expect_end() {
            Ok(()) => ParseOutcome::Accepted(Box::new(update)),
            Err(ParseFail::Unsupported { message, span }) => {
                ParseOutcome::Unsupported { message, span }
            }
            Err(ParseFail::Invalid { message, span }) => ParseOutcome::Invalid { message, span },
        },
        Err(ParseFail::Unsupported { message, span }) => {
            ParseOutcome::Unsupported { message, span }
        }
        Err(ParseFail::Invalid { message, span }) => ParseOutcome::Invalid { message, span },
    }
}

/// Parses a single CREATE TABLE statement (grammar
/// `.openspec/grammar/sqlite.ebnf` V3 block). Never panics — any input
/// produces one of the three [`ParseOutcome`] variants.
pub fn parse_create_table(src: &str) -> ParseOutcome<CreateTable> {
    let tokens = Tokenizer::tokenize(src);
    let mut parser = Parser::new(tokens);
    match parser.parse_create_table_stmt() {
        Ok(stmt) => match parser.expect_end() {
            Ok(()) => ParseOutcome::Accepted(Box::new(stmt)),
            Err(ParseFail::Unsupported { message, span }) => {
                ParseOutcome::Unsupported { message, span }
            }
            Err(ParseFail::Invalid { message, span }) => ParseOutcome::Invalid { message, span },
        },
        Err(ParseFail::Unsupported { message, span }) => {
            ParseOutcome::Unsupported { message, span }
        }
        Err(ParseFail::Invalid { message, span }) => ParseOutcome::Invalid { message, span },
    }
}

/// Parses a single CREATE INDEX statement (grammar
/// `.openspec/grammar/sqlite.ebnf` V3 block). Never panics — any input
/// produces one of the three [`ParseOutcome`] variants.
pub fn parse_create_index(src: &str) -> ParseOutcome<CreateIndex> {
    let tokens = Tokenizer::tokenize(src);
    let mut parser = Parser::new(tokens);
    match parser.parse_create_index_stmt() {
        Ok(stmt) => match parser.expect_end() {
            Ok(()) => ParseOutcome::Accepted(Box::new(stmt)),
            Err(ParseFail::Unsupported { message, span }) => {
                ParseOutcome::Unsupported { message, span }
            }
            Err(ParseFail::Invalid { message, span }) => ParseOutcome::Invalid { message, span },
        },
        Err(ParseFail::Unsupported { message, span }) => {
            ParseOutcome::Unsupported { message, span }
        }
        Err(ParseFail::Invalid { message, span }) => ParseOutcome::Invalid { message, span },
    }
}

/// Parses a single CREATE VIEW statement (grammar
/// `.openspec/grammar/sqlite.ebnf` V6 block, #379). Never panics — any
/// input produces one of the three [`ParseOutcome`] variants.
pub fn parse_create_view(src: &str) -> ParseOutcome<CreateView> {
    let tokens = Tokenizer::tokenize(src);
    let mut parser = Parser::new(tokens);
    match parser.parse_create_view_stmt() {
        Ok(stmt) => match parser.expect_end() {
            Ok(()) => ParseOutcome::Accepted(Box::new(stmt)),
            Err(ParseFail::Unsupported { message, span }) => {
                ParseOutcome::Unsupported { message, span }
            }
            Err(ParseFail::Invalid { message, span }) => ParseOutcome::Invalid { message, span },
        },
        Err(ParseFail::Unsupported { message, span }) => {
            ParseOutcome::Unsupported { message, span }
        }
        Err(ParseFail::Invalid { message, span }) => ParseOutcome::Invalid { message, span },
    }
}

/// Parses a single DROP VIEW statement (grammar
/// `.openspec/grammar/sqlite.ebnf` V6 block, #379). Never panics — any
/// input produces one of the three [`ParseOutcome`] variants.
pub fn parse_drop_view(src: &str) -> ParseOutcome<DropView> {
    let tokens = Tokenizer::tokenize(src);
    let mut parser = Parser::new(tokens);
    match parser.parse_drop_view_stmt() {
        Ok(stmt) => match parser.expect_end() {
            Ok(()) => ParseOutcome::Accepted(Box::new(stmt)),
            Err(ParseFail::Unsupported { message, span }) => {
                ParseOutcome::Unsupported { message, span }
            }
            Err(ParseFail::Invalid { message, span }) => ParseOutcome::Invalid { message, span },
        },
        Err(ParseFail::Unsupported { message, span }) => {
            ParseOutcome::Unsupported { message, span }
        }
        Err(ParseFail::Invalid { message, span }) => ParseOutcome::Invalid { message, span },
    }
}

/// Parses a single DROP TABLE statement (grammar
/// `.openspec/grammar/sqlite.ebnf` V3 block). Never panics — any input
/// produces one of the three [`ParseOutcome`] variants.
pub fn parse_drop_table(src: &str) -> ParseOutcome<DropTable> {
    let tokens = Tokenizer::tokenize(src);
    let mut parser = Parser::new(tokens);
    match parser.parse_drop_table_stmt() {
        Ok(stmt) => match parser.expect_end() {
            Ok(()) => ParseOutcome::Accepted(Box::new(stmt)),
            Err(ParseFail::Unsupported { message, span }) => {
                ParseOutcome::Unsupported { message, span }
            }
            Err(ParseFail::Invalid { message, span }) => ParseOutcome::Invalid { message, span },
        },
        Err(ParseFail::Unsupported { message, span }) => {
            ParseOutcome::Unsupported { message, span }
        }
        Err(ParseFail::Invalid { message, span }) => ParseOutcome::Invalid { message, span },
    }
}

/// Parses a single DROP INDEX statement (grammar
/// `.openspec/grammar/sqlite.ebnf` V3 block). Never panics — any input
/// produces one of the three [`ParseOutcome`] variants.
pub fn parse_drop_index(src: &str) -> ParseOutcome<DropIndex> {
    let tokens = Tokenizer::tokenize(src);
    let mut parser = Parser::new(tokens);
    match parser.parse_drop_index_stmt() {
        Ok(stmt) => match parser.expect_end() {
            Ok(()) => ParseOutcome::Accepted(Box::new(stmt)),
            Err(ParseFail::Unsupported { message, span }) => {
                ParseOutcome::Unsupported { message, span }
            }
            Err(ParseFail::Invalid { message, span }) => ParseOutcome::Invalid { message, span },
        },
        Err(ParseFail::Unsupported { message, span }) => {
            ParseOutcome::Unsupported { message, span }
        }
        Err(ParseFail::Invalid { message, span }) => ParseOutcome::Invalid { message, span },
    }
}

/// Parses a single BEGIN statement (grammar `.openspec/grammar/sqlite.ebnf`
/// V5 block, #356). Never panics — any input produces one of the three
/// [`ParseOutcome`] variants.
pub fn parse_begin(src: &str) -> ParseOutcome<Begin> {
    let tokens = Tokenizer::tokenize(src);
    let mut parser = Parser::new(tokens);
    match parser.parse_begin_stmt() {
        Ok(stmt) => match parser.expect_end() {
            Ok(()) => ParseOutcome::Accepted(Box::new(stmt)),
            Err(ParseFail::Unsupported { message, span }) => {
                ParseOutcome::Unsupported { message, span }
            }
            Err(ParseFail::Invalid { message, span }) => ParseOutcome::Invalid { message, span },
        },
        Err(ParseFail::Unsupported { message, span }) => {
            ParseOutcome::Unsupported { message, span }
        }
        Err(ParseFail::Invalid { message, span }) => ParseOutcome::Invalid { message, span },
    }
}

/// Parses a single COMMIT/END statement (grammar
/// `.openspec/grammar/sqlite.ebnf` V5 block, #356). Never panics — any input
/// produces one of the three [`ParseOutcome`] variants.
pub fn parse_commit(src: &str) -> ParseOutcome<Commit> {
    let tokens = Tokenizer::tokenize(src);
    let mut parser = Parser::new(tokens);
    match parser.parse_commit_stmt() {
        Ok(stmt) => match parser.expect_end() {
            Ok(()) => ParseOutcome::Accepted(Box::new(stmt)),
            Err(ParseFail::Unsupported { message, span }) => {
                ParseOutcome::Unsupported { message, span }
            }
            Err(ParseFail::Invalid { message, span }) => ParseOutcome::Invalid { message, span },
        },
        Err(ParseFail::Unsupported { message, span }) => {
            ParseOutcome::Unsupported { message, span }
        }
        Err(ParseFail::Invalid { message, span }) => ParseOutcome::Invalid { message, span },
    }
}

/// Parses a single ROLLBACK statement (grammar
/// `.openspec/grammar/sqlite.ebnf` V5 block, #356). Never panics — any input
/// produces one of the three [`ParseOutcome`] variants.
pub fn parse_rollback(src: &str) -> ParseOutcome<Rollback> {
    let tokens = Tokenizer::tokenize(src);
    let mut parser = Parser::new(tokens);
    match parser.parse_rollback_stmt() {
        Ok(stmt) => match parser.expect_end() {
            Ok(()) => ParseOutcome::Accepted(Box::new(stmt)),
            Err(ParseFail::Unsupported { message, span }) => {
                ParseOutcome::Unsupported { message, span }
            }
            Err(ParseFail::Invalid { message, span }) => ParseOutcome::Invalid { message, span },
        },
        Err(ParseFail::Unsupported { message, span }) => {
            ParseOutcome::Unsupported { message, span }
        }
        Err(ParseFail::Invalid { message, span }) => ParseOutcome::Invalid { message, span },
    }
}

/// Parses a single PRAGMA statement (grammar
/// `.openspec/grammar/sqlite.ebnf` V6 carve-out, #388) — only `PRAGMA
/// journal_mode = WAL|DELETE` is `Accepted`; any other pragma name or
/// value is `Unsupported`. Never panics — any input produces one of the
/// three [`ParseOutcome`] variants.
pub fn parse_pragma(src: &str) -> ParseOutcome<Pragma> {
    let tokens = Tokenizer::tokenize(src);
    let mut parser = Parser::new(tokens);
    match parser.parse_pragma_stmt() {
        Ok(stmt) => match parser.expect_end() {
            Ok(()) => ParseOutcome::Accepted(Box::new(stmt)),
            Err(ParseFail::Unsupported { message, span }) => {
                ParseOutcome::Unsupported { message, span }
            }
            Err(ParseFail::Invalid { message, span }) => ParseOutcome::Invalid { message, span },
        },
        Err(ParseFail::Unsupported { message, span }) => {
            ParseOutcome::Unsupported { message, span }
        }
        Err(ParseFail::Invalid { message, span }) => ParseOutcome::Invalid { message, span },
    }
}

/// Parses a single ANALYZE statement (grammar
/// `.openspec/grammar/sqlite.ebnf` V7 carve-out, #461): `ANALYZE` or
/// `ANALYZE table-name`. Never panics — any input produces one of the
/// three [`ParseOutcome`] variants.
pub fn parse_analyze(src: &str) -> ParseOutcome<Analyze> {
    let tokens = Tokenizer::tokenize(src);
    let mut parser = Parser::new(tokens);
    match parser.parse_analyze_stmt() {
        Ok(stmt) => match parser.expect_end() {
            Ok(()) => ParseOutcome::Accepted(Box::new(stmt)),
            Err(ParseFail::Unsupported { message, span }) => {
                ParseOutcome::Unsupported { message, span }
            }
            Err(ParseFail::Invalid { message, span }) => ParseOutcome::Invalid { message, span },
        },
        Err(ParseFail::Unsupported { message, span }) => {
            ParseOutcome::Unsupported { message, span }
        }
        Err(ParseFail::Invalid { message, span }) => ParseOutcome::Invalid { message, span },
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing, clippy::panic)]
mod tests {
    use super::*;

    fn assert_accepted<T: std::fmt::Debug>(outcome: ParseOutcome<T>, src: &str) {
        match outcome {
            ParseOutcome::Accepted(_) => {}
            other => panic!("expected accepted for {src:?}, got {other:?}"),
        }
    }

    fn assert_unsupported<T: std::fmt::Debug>(outcome: ParseOutcome<T>, src: &str) {
        match outcome {
            ParseOutcome::Unsupported { .. } => {}
            other => panic!("expected unsupported for {src:?}, got {other:?}"),
        }
    }

    fn assert_invalid<T: std::fmt::Debug>(outcome: ParseOutcome<T>, src: &str) {
        match outcome {
            ParseOutcome::Invalid { .. } => {}
            other => panic!("expected invalid for {src:?}, got {other:?}"),
        }
    }

    #[test]
    fn select_three_way_outcome() {
        assert_accepted(parse_select("SELECT 1"), "SELECT 1");
        assert_unsupported(
            parse_select("SELECT 1 UNION SELECT 2 INTERSECT SELECT 3"),
            "SELECT 1 UNION SELECT 2 INTERSECT SELECT 3",
        );
        assert_invalid(parse_select("SELECT FROM t"), "SELECT FROM t");
    }

    #[test]
    fn explain_three_way_outcome() {
        assert_accepted(
            parse_explain("EXPLAIN QUERY PLAN SELECT 1"),
            "EXPLAIN QUERY PLAN SELECT 1",
        );
        assert_unsupported(
            parse_explain("EXPLAIN QUERY PLAN SELECT 1 UNION SELECT 2 INTERSECT SELECT 3"),
            "EXPLAIN QUERY PLAN SELECT 1 UNION SELECT 2 INTERSECT SELECT 3",
        );
        assert_invalid(parse_explain("EXPLAIN QUERY PLAN"), "EXPLAIN QUERY PLAN");
    }

    #[test]
    fn insert_three_way_outcome() {
        assert_accepted(
            parse_insert("INSERT INTO t VALUES (1, 2)"),
            "INSERT INTO t VALUES (1, 2)",
        );
        assert_unsupported(
            parse_insert("INSERT INTO t VALUES (CURRENT_TIMESTAMP)"),
            "INSERT INTO t VALUES (CURRENT_TIMESTAMP)",
        );
        assert_invalid(parse_insert("INSERT INTO t"), "INSERT INTO t");
    }

    #[test]
    fn delete_three_way_outcome() {
        assert_accepted(parse_delete("DELETE FROM t"), "DELETE FROM t");
        assert_unsupported(
            parse_delete("DELETE FROM t UNION SELECT 1"),
            "DELETE FROM t UNION SELECT 1",
        );
        assert_invalid(parse_delete("DELETE FROM"), "DELETE FROM");
    }

    #[test]
    fn update_three_way_outcome() {
        assert_accepted(
            parse_update("UPDATE t1 SET x=1 WHERE x>0"),
            "UPDATE t1 SET x=1 WHERE x>0",
        );
        assert_unsupported(
            parse_update("UPDATE t1 SET x=1 UNION SELECT 1"),
            "UPDATE t1 SET x=1 UNION SELECT 1",
        );
        assert_invalid(parse_update("UPDATE t1 SET"), "UPDATE t1 SET");
    }

    #[test]
    fn create_table_three_way_outcome() {
        assert_accepted(
            parse_create_table("CREATE TABLE t (a INTEGER, b TEXT)"),
            "CREATE TABLE t (a INTEGER, b TEXT)",
        );
        assert_unsupported(
            parse_create_table("CREATE TEMP TABLE t (a)"),
            "CREATE TEMP TABLE t (a)",
        );
        assert_invalid(
            parse_create_table("CREATE TABLE t a INTEGER)"),
            "CREATE TABLE t a INTEGER)",
        );
    }

    #[test]
    fn create_index_three_way_outcome() {
        assert_accepted(
            parse_create_index("CREATE INDEX i ON t (a)"),
            "CREATE INDEX i ON t (a)",
        );
        assert_unsupported(
            parse_create_index("CREATE INDEX i ON t (a) UNION SELECT 1"),
            "CREATE INDEX i ON t (a) UNION SELECT 1",
        );
        assert_invalid(parse_create_index("CREATE INDEX"), "CREATE INDEX");
    }

    #[test]
    fn create_view_three_way_outcome() {
        assert_accepted(
            parse_create_view("CREATE VIEW v AS SELECT 1"),
            "CREATE VIEW v AS SELECT 1",
        );
        assert_unsupported(
            parse_create_view("CREATE TEMP VIEW v AS SELECT 1"),
            "CREATE TEMP VIEW v AS SELECT 1",
        );
        assert_invalid(parse_create_view("CREATE VIEW"), "CREATE VIEW");
    }

    #[test]
    fn drop_view_three_way_outcome() {
        assert_accepted(parse_drop_view("DROP VIEW v"), "DROP VIEW v");
        assert_unsupported(
            parse_drop_view("DROP VIEW v UNION SELECT 1"),
            "DROP VIEW v UNION SELECT 1",
        );
        assert_invalid(parse_drop_view("DROP VIEW"), "DROP VIEW");
    }

    #[test]
    fn drop_table_three_way_outcome() {
        assert_accepted(parse_drop_table("DROP TABLE t"), "DROP TABLE t");
        assert_unsupported(
            parse_drop_table("DROP TABLE t UNION SELECT 1"),
            "DROP TABLE t UNION SELECT 1",
        );
        assert_invalid(parse_drop_table("DROP TABLE"), "DROP TABLE");
    }

    #[test]
    fn drop_index_three_way_outcome() {
        assert_accepted(parse_drop_index("DROP INDEX i"), "DROP INDEX i");
        assert_unsupported(
            parse_drop_index("DROP INDEX i UNION SELECT 1"),
            "DROP INDEX i UNION SELECT 1",
        );
        assert_invalid(parse_drop_index("DROP INDEX"), "DROP INDEX");
    }

    #[test]
    fn begin_three_way_outcome() {
        assert_accepted(parse_begin("BEGIN"), "BEGIN");
        assert_invalid(
            parse_begin("BEGIN TRANSACTION EXTRA"),
            "BEGIN TRANSACTION EXTRA",
        );
    }

    #[test]
    fn commit_three_way_outcome() {
        assert_accepted(parse_commit("COMMIT"), "COMMIT");
        assert_invalid(parse_commit("COMMIT EXTRA"), "COMMIT EXTRA");
    }

    #[test]
    fn rollback_three_way_outcome() {
        assert_accepted(parse_rollback("ROLLBACK"), "ROLLBACK");
        assert_invalid(parse_rollback("ROLLBACK EXTRA"), "ROLLBACK EXTRA");
    }

    #[test]
    fn pragma_three_way_outcome() {
        assert_accepted(
            parse_pragma("PRAGMA journal_mode = WAL"),
            "PRAGMA journal_mode = WAL",
        );
        assert_unsupported(
            parse_pragma("PRAGMA cache_size = 10"),
            "PRAGMA cache_size = 10",
        );
        assert_invalid(parse_pragma("PRAGMA"), "PRAGMA");
    }

    #[test]
    fn analyze_three_way_outcome() {
        assert_accepted(parse_analyze("ANALYZE"), "ANALYZE");
        assert_accepted(parse_analyze("ANALYZE t"), "ANALYZE t");
        assert_unsupported(parse_analyze("ANALYZE main.t"), "ANALYZE main.t");
        assert_invalid(parse_analyze("ANALYZE 123"), "ANALYZE 123");
    }
}
