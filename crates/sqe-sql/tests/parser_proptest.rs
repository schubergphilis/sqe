//! Property-based tests for the SQE classifier and ATTACH/SECRET parsers.
//!
//! The handwritten tests in `src/classifier.rs` cover happy-path strings and
//! known corner cases. They do not exercise random or malicious inputs.
//! These properties pin the invariant that the parser surfaces never panic
//! on arbitrary text, and that shaped SELECT statements always classify as
//! `Query`.
//!
//! Pattern: each property accepts an input from a generator, runs the
//! parser, and asserts the result-shape envelope. Errors are fine; panics
//! are not. To keep CI time bounded we cap the proptest cases at the
//! library default (256) and rely on shrinking to surface the smallest
//! failing input.
//!
//! This is the starter harness for issue #119. The JWT and OAuth parsers
//! are tracked as follow-up work in separate crates.

use proptest::prelude::*;
use sqe_sql::{
    attach::{try_parse_attach, try_parse_create_secret, try_parse_detach, try_parse_drop_secret},
    parse_and_classify, StatementKind,
};

/// Identifier generator: ASCII letters, digits, and underscore. First char
/// is a letter so SQL parsers do not reject it as a numeric literal.
fn ident() -> impl Strategy<Value = String> {
    "[a-zA-Z][a-zA-Z0-9_]{0,15}".prop_map(|s| s.to_string())
}

/// Generate strings that are likely to surface parser bugs: bytes that
/// commonly trip tokenisers (quotes, escape sequences, control characters,
/// non-ASCII), but bounded in length so each case runs fast.
fn nasty_string() -> impl Strategy<Value = String> {
    proptest::collection::vec(
        prop_oneof![
            // Boring printable ASCII keeps the corpus realistic.
            any::<char>().prop_filter("printable", |c| c.is_ascii_graphic() || *c == ' '),
            // Tokeniser-hostile characters that frequently appear in real bug reports.
            Just('\''),
            Just('"'),
            Just('`'),
            Just('\\'),
            Just('\n'),
            Just('\t'),
            Just('\0'),
            Just(';'),
            Just(','),
            Just('('),
            Just(')'),
            // A handful of multibyte scalars to exercise byte-index handling.
            Just('é'),
            Just('日'),
        ],
        0..256,
    )
    .prop_map(|chars| chars.into_iter().collect::<String>())
}

proptest! {
    /// The classifier must not panic on any input. Errors are fine; panics
    /// are unacceptable because a coordinator that crashes on bad SQL is a
    /// denial-of-service surface.
    #[test]
    fn classifier_does_not_panic_on_arbitrary_input(s in nasty_string()) {
        // We do not care whether the parse succeeds or fails. We only care
        // that the call returns a Result without unwinding.
        let _ = parse_and_classify(&s);
    }

    /// The classifier must not panic on inputs that almost look like SQL.
    /// Concatenating random fragments around SQL-shaped keywords is a known
    /// trigger for tokeniser edge cases.
    #[test]
    fn classifier_does_not_panic_on_sql_lookalikes(
        keyword in prop::sample::select(vec![
            "SELECT", "INSERT", "UPDATE", "DELETE", "MERGE",
            "CREATE", "DROP", "ALTER", "GRANT", "REVOKE",
            "SHOW", "EXPLAIN", "USE", "BEGIN", "COMMIT", "ROLLBACK",
            "ATTACH", "DETACH",
        ]),
        prefix in nasty_string(),
        suffix in nasty_string(),
    ) {
        let sql = format!("{prefix} {keyword} {suffix}");
        let _ = parse_and_classify(&sql);
    }

    /// A well-formed `SELECT <col> FROM <table>` made of random valid
    /// identifiers must classify as `StatementKind::Query`. This guards
    /// against accidental misclassification when refactoring the SHOW /
    /// EXPLAIN pre-scans at the top of `parse_and_classify`.
    #[test]
    fn shaped_select_classifies_as_query(col in ident(), table in ident()) {
        let sql = format!("SELECT {col} FROM {table}");
        let kind = parse_and_classify(&sql).expect("shaped SELECT must parse");
        prop_assert!(
            matches!(kind, StatementKind::Query(_)),
            "expected Query, got: {kind:?}"
        );
    }

    /// `SHOW CATALOGS` is intercepted before sqlparser sees it. The pre-scan
    /// is case-insensitive and tolerates trailing whitespace. Pin that.
    #[test]
    fn show_catalogs_pre_scan_is_case_insensitive(
        trailing in "[ \\t]*",
        case in prop::sample::select(vec![
            "SHOW CATALOGS",
            "show catalogs",
            "Show Catalogs",
            "SHOW catalogs",
        ]),
    ) {
        let sql = format!("{case}{trailing}");
        let kind = parse_and_classify(&sql).expect("SHOW CATALOGS must parse");
        prop_assert!(
            matches!(kind, StatementKind::ShowCatalogs),
            "expected ShowCatalogs, got: {kind:?}"
        );
    }

    /// The ATTACH / DETACH / CREATE SECRET / DROP SECRET try-parsers each
    /// return Result<Option<_>, _>; arbitrary input must never panic.
    /// `Ok(None)` (statement is not theirs) is the common case for random
    /// strings, and we accept any of Ok/Err.
    #[test]
    fn attach_family_parsers_do_not_panic(s in nasty_string()) {
        let _ = try_parse_attach(&s);
        let _ = try_parse_detach(&s);
        let _ = try_parse_create_secret(&s);
        let _ = try_parse_drop_secret(&s);
    }
}
