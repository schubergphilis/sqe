//! SQL script splitting for `-f file.sql` mode.
//!
//! Splits a script into statements at top-level `;`, respecting:
//! - Single-quoted string literals (`'foo;bar'` stays one token)
//! - Double-quoted identifiers (`"foo;bar"` stays one token)
//! - Single-line comments (`-- foo;bar\n` ends at the newline)
//! - Multi-line comments (`/* foo;bar */` consumed entirely)
//!
//! Backslash escapes inside single-quoted strings are NOT interpreted —
//! ANSI SQL uses `''` to represent a literal single quote, which the
//! state machine handles automatically by exiting and re-entering the
//! string state. The same applies to `""` inside double-quoted
//! identifiers.
//!
//! This is deliberately a small lexical splitter rather than a full
//! parser. A full parser would catch corner cases like dollar-quoted
//! strings (Postgres `$tag$...$tag$`), but DataFusion / Trino dialects
//! don't use those, so the cost of a sqlparser dependency for the CLI
//! isn't justified.

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Code,
    SingleQuote,
    DoubleQuote,
    LineComment,
    BlockComment,
}

/// Split a SQL script into trimmed top-level statements.
pub fn split_statements(input: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut buf = String::with_capacity(input.len());
    let mut state = State::Code;
    let bytes = input.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        let c = bytes[i] as char;
        let next = bytes.get(i + 1).map(|b| *b as char);

        match state {
            State::Code => match c {
                ';' => {
                    let trimmed = buf.trim();
                    if !trimmed.is_empty() {
                        out.push(trimmed.to_string());
                    }
                    buf.clear();
                }
                '\'' => {
                    buf.push(c);
                    state = State::SingleQuote;
                }
                '"' => {
                    buf.push(c);
                    state = State::DoubleQuote;
                }
                '-' if next == Some('-') => {
                    buf.push(c);
                    buf.push('-');
                    i += 1;
                    state = State::LineComment;
                }
                '/' if next == Some('*') => {
                    buf.push(c);
                    buf.push('*');
                    i += 1;
                    state = State::BlockComment;
                }
                _ => buf.push(c),
            },
            State::SingleQuote => {
                buf.push(c);
                if c == '\'' {
                    // SQL escapes a literal `'` by doubling it (`''`).
                    // If the next char is also `'`, stay in the string.
                    if next == Some('\'') {
                        buf.push('\'');
                        i += 1;
                    } else {
                        state = State::Code;
                    }
                }
            }
            State::DoubleQuote => {
                buf.push(c);
                if c == '"' {
                    if next == Some('"') {
                        buf.push('"');
                        i += 1;
                    } else {
                        state = State::Code;
                    }
                }
            }
            State::LineComment => {
                buf.push(c);
                if c == '\n' {
                    state = State::Code;
                }
            }
            State::BlockComment => {
                buf.push(c);
                if c == '*' && next == Some('/') {
                    buf.push('/');
                    i += 1;
                    state = State::Code;
                }
            }
        }
        i += 1;
    }

    let trimmed = buf.trim();
    if !trimmed.is_empty() {
        out.push(trimmed.to_string());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_input_returns_empty() {
        assert!(split_statements("").is_empty());
        assert!(split_statements("   \n  \t").is_empty());
    }

    #[test]
    fn single_statement_no_terminator() {
        assert_eq!(split_statements("SELECT 1"), vec!["SELECT 1"]);
    }

    #[test]
    fn single_statement_with_terminator() {
        assert_eq!(split_statements("SELECT 1;"), vec!["SELECT 1"]);
    }

    #[test]
    fn two_statements_split_cleanly() {
        let stmts = split_statements("SELECT 1; SELECT 2;");
        assert_eq!(stmts, vec!["SELECT 1", "SELECT 2"]);
    }

    #[test]
    fn semicolon_inside_single_quote_does_not_split() {
        let stmts = split_statements("SELECT 'a;b'; SELECT 2;");
        assert_eq!(stmts, vec!["SELECT 'a;b'", "SELECT 2"]);
    }

    #[test]
    fn doubled_quote_inside_single_quote() {
        let stmts = split_statements("SELECT 'it''s; tricky'; SELECT 2;");
        assert_eq!(stmts, vec!["SELECT 'it''s; tricky'", "SELECT 2"]);
    }

    #[test]
    fn semicolon_inside_double_quote_identifier_does_not_split() {
        let stmts = split_statements("SELECT \"col;name\" FROM t; SELECT 2;");
        assert_eq!(stmts, vec!["SELECT \"col;name\" FROM t", "SELECT 2"]);
    }

    #[test]
    fn semicolon_inside_line_comment_does_not_split() {
        let stmts = split_statements("SELECT 1 -- ;not a split\n; SELECT 2;");
        assert_eq!(stmts, vec!["SELECT 1 -- ;not a split", "SELECT 2"]);
    }

    #[test]
    fn semicolon_inside_block_comment_does_not_split() {
        let stmts = split_statements("SELECT 1 /* ;not; a; split */; SELECT 2;");
        assert_eq!(stmts, vec!["SELECT 1 /* ;not; a; split */", "SELECT 2"]);
    }

    #[test]
    fn whitespace_only_between_statements_is_dropped() {
        let stmts = split_statements("SELECT 1;\n\n  \n;SELECT 2;");
        assert_eq!(stmts, vec!["SELECT 1", "SELECT 2"]);
    }

    #[test]
    fn multi_line_statement_preserves_internal_whitespace() {
        let stmts = split_statements("SELECT\n  a,\n  b\nFROM t;");
        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("a,\n"));
    }
}
