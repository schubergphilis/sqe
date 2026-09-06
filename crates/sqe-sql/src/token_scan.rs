//! Token-level helpers shared by the classifier and the SQL front door.
//!
//! Everything here operates on sqlparser's token stream rather than on the
//! raw text, so quoting, comments, whitespace and case are handled the way
//! the parser handles them. The classifier used to test uppercased string
//! prefixes before the parser ran (review finding 107); these helpers are
//! what replaced those tests.

use sqe_core::{SqeError, SqeErrorCode};
use sqlparser::dialect::Dialect;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Location, Token, TokenWithSpan, Tokenizer};

/// Typed syntax error without an upstream source.
pub(crate) fn syntax_error(message: impl Into<String>) -> SqeError {
    SqeError::with_code(SqeErrorCode::SyntaxError, message.into())
}

/// Typed syntax error that keeps the sqlparser error as its source.
pub(crate) fn parse_error(e: ParserError) -> SqeError {
    SqeError::sourced(SqeErrorCode::SyntaxError, format!("Parse error: {e}"), e)
}

/// Tokenize `sql` once, keeping locations. Tokenizer failures (unterminated
/// string, bad escape) surface as typed syntax errors.
pub(crate) fn tokenize(dialect: &dyn Dialect, sql: &str) -> sqe_core::Result<Vec<TokenWithSpan>> {
    Tokenizer::new(dialect, sql)
        .tokenize_with_location()
        .map_err(|e| parse_error(ParserError::TokenizerError(e.to_string())))
}

fn is_significant(tok: &Token) -> bool {
    !matches!(tok, Token::Whitespace(_) | Token::EOF)
}

/// Reject input that carries more than one statement. Trailing semicolons
/// (`SELECT 1;`, `SELECT 1;;`) are fine; a semicolon followed by another
/// significant token is not. The error names the position of the second
/// statement so a client that concatenated a script can see where the cut
/// is. Only the first statement used to be executed and the rest silently
/// dropped (finding 107).
pub(crate) fn reject_multi_statement(tokens: &[TokenWithSpan]) -> sqe_core::Result<()> {
    let mut after_semicolon = false;
    for tok in tokens {
        match &tok.token {
            Token::SemiColon => after_semicolon = true,
            t if after_semicolon && is_significant(t) => {
                let loc = tok.span.start;
                return Err(syntax_error(format!(
                    "Parse error: multiple statements are not supported; a second statement \
                     starts at line {}, column {}. Send one statement per request.",
                    loc.line, loc.column
                )));
            }
            _ => {}
        }
    }
    Ok(())
}

/// Convert a tokenizer `Location` (1-based line and column, columns counted
/// in characters) into a byte offset into `sql`. Used to hand the text of a
/// statement body -- starting at its first significant token, so leading
/// comments and whitespace are dropped -- to the sub-parsers that still
/// operate on strings.
pub(crate) fn byte_offset(sql: &str, loc: Location) -> usize {
    if loc.line == 0 {
        return 0;
    }
    let mut line = 1u64;
    let mut column = 1u64;
    for (idx, ch) in sql.char_indices() {
        if line == loc.line && column == loc.column {
            return idx;
        }
        if ch == '\n' {
            line += 1;
            column = 1;
        } else {
            column += 1;
        }
    }
    sql.len()
}

/// The uppercased value of the token at `parser`'s current position plus `n`
/// when it is an unquoted word (keyword or plain identifier); `None` for
/// quoted identifiers, literals, punctuation and EOF.
pub(crate) fn word_at(parser: &Parser, n: usize) -> Option<String> {
    match parser.peek_nth_token(n).token {
        Token::Word(w) if w.quote_style.is_none() => Some(w.value.to_ascii_uppercase()),
        _ => None,
    }
}

/// Uppercased leading unquoted words, at most `max`, stopping at the first
/// token that is not an unquoted word. This is what the classifier
/// dispatches on.
pub(crate) fn leading_words(parser: &Parser, max: usize) -> Vec<String> {
    (0..max).map_while(|n| word_at(parser, n)).collect()
}

/// Consume the next token when it is the unquoted word `word` (case
/// insensitive). Works for both reserved keywords and plain identifiers such
/// as `TAGS` or `WRITE_BRANCH`, which sqlparser does not model as keywords.
pub(crate) fn consume_word(parser: &mut Parser, word: &str) -> bool {
    if word_at(parser, 0).as_deref() == Some(&word.to_ascii_uppercase()) {
        parser.next_token();
        true
    } else {
        false
    }
}

/// Consume the exact sequence `words`, or consume nothing and return false.
pub(crate) fn consume_words(parser: &mut Parser, words: &[&str]) -> bool {
    let all_match = words
        .iter()
        .enumerate()
        .all(|(i, w)| word_at(parser, i).as_deref() == Some(&w.to_ascii_uppercase()));
    if all_match {
        for _ in words {
            parser.next_token();
        }
    }
    all_match
}

/// Require the unquoted word `word` next; error otherwise.
pub(crate) fn expect_word(parser: &mut Parser, word: &str) -> sqe_core::Result<()> {
    if consume_word(parser, word) {
        Ok(())
    } else {
        let found = parser.peek_token();
        parser.expected::<()>(word, found).map_err(parse_error)
    }
}

/// Require that only trailing semicolons remain. Anything else is a syntax
/// error naming the offending token.
pub(crate) fn expect_end(parser: &mut Parser) -> sqe_core::Result<()> {
    while parser.consume_token(&Token::SemiColon) {}
    let tok = parser.peek_token();
    if tok.token == Token::EOF {
        Ok(())
    } else {
        parser
            .expected::<()>("end of statement", tok)
            .map_err(parse_error)
    }
}

/// True when only trailing semicolons remain.
pub(crate) fn at_end(parser: &mut Parser) -> bool {
    while parser.consume_token(&Token::SemiColon) {}
    parser.peek_token().token == Token::EOF
}

/// Parse a possibly dotted, possibly quoted object reference and return it
/// as written (quotes preserved), e.g. `"my ns"."my tbl"`. Handlers that
/// receive these strings already split and unquote them themselves.
pub(crate) fn parse_name_ref(parser: &mut Parser) -> sqe_core::Result<String> {
    parser
        .parse_object_name(false)
        .map(|n| n.to_string())
        .map_err(parse_error)
}

/// Parse a principal or policy name: a bare or double-quoted identifier, or a
/// single-quoted string. Returns the unquoted value.
pub(crate) fn parse_principal_name(parser: &mut Parser) -> sqe_core::Result<String> {
    match parser.peek_token().token {
        Token::SingleQuotedString(s) => {
            parser.next_token();
            Ok(s)
        }
        _ => parser
            .parse_identifier()
            .map(|i| i.value)
            .map_err(parse_error),
    }
}

/// Remaining source text from the parser's current token to the end, with
/// trailing semicolons and whitespace removed. Empty when at EOF.
pub(crate) fn remaining_text<'a>(parser: &Parser, sql: &'a str) -> &'a str {
    let tok = parser.peek_token();
    if tok.token == Token::EOF {
        return "";
    }
    sql[byte_offset(sql, tok.span.start)..]
        .trim()
        .trim_end_matches(';')
        .trim()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::GenericDialect;

    fn toks(sql: &str) -> Vec<TokenWithSpan> {
        tokenize(&GenericDialect {}, sql).unwrap()
    }

    #[test]
    fn trailing_semicolons_are_not_a_second_statement() {
        assert!(reject_multi_statement(&toks("SELECT 1;")).is_ok());
        assert!(reject_multi_statement(&toks("SELECT 1;;  ")).is_ok());
        assert!(reject_multi_statement(&toks("SELECT ';' ; ")).is_ok());
    }

    #[test]
    fn a_second_statement_is_named_by_position() {
        let err = reject_multi_statement(&toks("SELECT 1;\n  SELECT 2")).unwrap_err();
        assert_eq!(err.error_code(), SqeErrorCode::SyntaxError);
        assert!(err.to_string().contains("line 2, column 3"), "{err}");
    }

    #[test]
    fn a_semicolon_inside_a_literal_or_comment_is_ignored() {
        assert!(reject_multi_statement(&toks("SELECT 'a; b' -- ; SELECT 2")).is_ok());
        assert!(reject_multi_statement(&toks("SELECT 1 /* ; SELECT 2 */")).is_ok());
    }

    #[test]
    fn byte_offset_handles_multibyte_and_newlines() {
        let sql = "-- caf\u{e9}\nSELECT 1";
        let tokens = toks(sql);
        let first = tokens.iter().find(|t| is_significant(&t.token)).unwrap();
        assert_eq!(&sql[byte_offset(sql, first.span.start)..], "SELECT 1");
    }

    #[test]
    fn leading_words_stop_at_quoted_identifier() {
        let dialect = GenericDialect {};
        let parser = Parser::new(&dialect)
            .try_with_sql("show tags on \"My Table\"")
            .unwrap();
        assert_eq!(leading_words(&parser, 6), vec!["SHOW", "TAGS", "ON"]);
    }
}
