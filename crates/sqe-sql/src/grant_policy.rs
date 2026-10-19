//! `GRANT SELECT ... MASKED WITH (col = expr, ...)` and `... ROWS WHERE expr`.
//!
//! The clauses trail a standard GRANT, which sqlparser reads on its own once
//! they are cut off. They are found on the token stream, so a `masked` or
//! `rows` inside a quoted name, a string literal or a parenthesised
//! expression is never mistaken for a clause keyword.
//!
//! The grant they ride on is narrowed to what a Ranger mask or row-filter
//! item can carry: `SELECT` on one table, to one user, role or group.

use sqe_core::Result;
use sqlparser::ast::{Action, GrantObjects, GranteesType, Privileges, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, TokenWithSpan, Word};

use crate::token_scan::{syntax_error, tokenize};

/// One `column = expression` entry of a `MASKED WITH` list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrantMask {
    /// The column, unquoted.
    pub column: String,
    /// The mask expression as written. It may name the column itself or use
    /// the `{col}` placeholder; both are substituted the same way.
    pub expression: String,
}

/// A GRANT carrying at least one of the two clauses.
#[derive(Debug, Clone, PartialEq)]
pub struct GrantWithPolicy {
    /// The standard `Statement::Grant`, clauses removed.
    pub grant: Statement,
    pub masks: Vec<GrantMask>,
    pub row_filter: Option<String>,
}

/// What a policy store writes for one `GRANT ... MASKED WITH / ROWS WHERE`:
/// the grantee's masks and row filter on one table. Each mask replaces the
/// grantee's mask on that column, the filter replaces its row filter on the
/// table, and anything else the grantee holds there is left alone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrantPolicySpec {
    /// Dotted table name as written (`schema.table` or `catalog.schema.table`).
    pub table: String,
    pub principal: crate::policy_ddl::PolicyPrincipal,
    pub masks: Vec<GrantMask>,
    pub row_filter: Option<String>,
}

/// Parse a GRANT with `MASKED WITH` and/or `ROWS WHERE`. Returns `Ok(None)`
/// when neither clause is present, so a plain GRANT takes the standard path.
pub fn try_parse_grant_policy(sql: &str) -> Result<Option<GrantWithPolicy>> {
    let dialect = GenericDialect {};
    let tokens = tokenize(&dialect, sql)?;
    let Some(first_clause) = find_clause(&tokens, 0) else {
        return Ok(None);
    };

    let base: String = tokens[..first_clause]
        .iter()
        .map(|t| t.token.to_string())
        .collect();
    let grant = Parser::new(&dialect)
        .try_with_sql(&base)
        .and_then(|mut p| p.parse_statement())
        .map_err(|e| syntax_error(format!("GRANT: {e}")))?;
    validate_grant(&grant)?;

    let mut masks: Option<Vec<GrantMask>> = None;
    let mut row_filter: Option<String> = None;
    let mut pos = first_clause;
    loop {
        let (kind, body_start) = clause_at(&tokens, pos).expect("pos is at a clause");
        match kind {
            Clause::MaskedWith => {
                if masks.is_some() {
                    return Err(syntax("MASKED WITH may appear only once"));
                }
                let close = matching_paren(&tokens, body_start)?;
                masks = Some(parse_mask_list(&tokens[body_start + 1..close])?);
                pos = next_significant(&tokens, close + 1);
            }
            Clause::RowsWhere => {
                if row_filter.is_some() {
                    return Err(syntax("ROWS WHERE may appear only once"));
                }
                let end =
                    find_clause(&tokens, body_start).unwrap_or_else(|| statement_end(&tokens));
                let expr = render(&tokens[body_start..end]);
                if expr.is_empty() {
                    return Err(syntax("ROWS WHERE requires a filter expression"));
                }
                validate_expr(&expr, "ROWS WHERE")?;
                row_filter = Some(expr);
                pos = end;
            }
        }
        if pos >= statement_end(&tokens) {
            break;
        }
        if clause_at(&tokens, pos).is_none() {
            return Err(syntax(&format!(
                "unexpected `{}` after the MASKED WITH / ROWS WHERE clause",
                render(&tokens[pos..statement_end(&tokens)])
            )));
        }
    }

    Ok(Some(GrantWithPolicy {
        grant,
        masks: masks.unwrap_or_default(),
        row_filter,
    }))
}

#[derive(Clone, Copy)]
enum Clause {
    MaskedWith,
    RowsWhere,
}

fn is_keyword(tok: &Token, kw: &str) -> bool {
    matches!(tok, Token::Word(Word { value, quote_style: None, .. }) if value.eq_ignore_ascii_case(kw))
}

fn is_significant(tok: &Token) -> bool {
    !matches!(tok, Token::Whitespace(_) | Token::EOF)
}

fn next_significant(tokens: &[TokenWithSpan], from: usize) -> usize {
    (from..tokens.len())
        .find(|&i| is_significant(&tokens[i].token))
        .unwrap_or(tokens.len())
}

/// The index of the trailing `;` / EOF run: everything at or after it is
/// insignificant.
fn statement_end(tokens: &[TokenWithSpan]) -> usize {
    let mut end = tokens.len();
    while end > 0
        && matches!(
            tokens[end - 1].token,
            Token::Whitespace(_) | Token::EOF | Token::SemiColon
        )
    {
        end -= 1;
    }
    end
}

/// Whether a clause starts at `pos`; returns its kind and the index of its
/// body (the `(` of `MASKED WITH`, the first token after `ROWS WHERE`).
/// `MASKED WITH` must be followed by `(`, which keeps a grantee named
/// `masked` before `WITH GRANT OPTION` a plain grant.
fn clause_at(tokens: &[TokenWithSpan], pos: usize) -> Option<(Clause, usize)> {
    let first = tokens.get(pos)?;
    let second = next_significant(tokens, pos + 1);
    let second_tok = &tokens.get(second)?.token;
    if is_keyword(&first.token, "MASKED") && is_keyword(second_tok, "WITH") {
        let paren = next_significant(tokens, second + 1);
        if matches!(tokens.get(paren).map(|t| &t.token), Some(Token::LParen)) {
            return Some((Clause::MaskedWith, paren));
        }
    }
    if is_keyword(&first.token, "ROWS") && is_keyword(second_tok, "WHERE") {
        return Some((Clause::RowsWhere, second + 1));
    }
    None
}

/// The first clause at parenthesis depth zero at or after `from`.
fn find_clause(tokens: &[TokenWithSpan], from: usize) -> Option<usize> {
    let mut depth = 0usize;
    for i in from..tokens.len() {
        match tokens[i].token {
            Token::LParen => depth += 1,
            Token::RParen => depth = depth.saturating_sub(1),
            _ if depth == 0 && clause_at(tokens, i).is_some() => return Some(i),
            _ => {}
        }
    }
    None
}

fn matching_paren(tokens: &[TokenWithSpan], open: usize) -> Result<usize> {
    let mut depth = 0usize;
    for (i, tok) in tokens.iter().enumerate().skip(open) {
        match tok.token {
            Token::LParen => depth += 1,
            Token::RParen => {
                depth -= 1;
                if depth == 0 {
                    return Ok(i);
                }
            }
            _ => {}
        }
    }
    Err(syntax("unterminated MASKED WITH ( ... ) list"))
}

fn render(tokens: &[TokenWithSpan]) -> String {
    tokens
        .iter()
        .map(|t| t.token.to_string())
        .collect::<String>()
        .trim()
        .to_string()
}

fn parse_mask_list(tokens: &[TokenWithSpan]) -> Result<Vec<GrantMask>> {
    let mut entries: Vec<&[TokenWithSpan]> = Vec::new();
    let mut depth = 0usize;
    let mut start = 0;
    for (i, tok) in tokens.iter().enumerate() {
        match tok.token {
            Token::LParen => depth += 1,
            Token::RParen => depth = depth.saturating_sub(1),
            Token::Comma if depth == 0 => {
                entries.push(&tokens[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    entries.push(&tokens[start..]);

    let mut masks: Vec<GrantMask> = Vec::new();
    for entry in entries {
        let col_at = next_significant(entry, 0);
        let Some(Token::Word(word)) = entry.get(col_at).map(|t| &t.token) else {
            return Err(syntax(
                "MASKED WITH expects `<column> = <expression>` entries",
            ));
        };
        let eq_at = next_significant(entry, col_at + 1);
        if !matches!(entry.get(eq_at).map(|t| &t.token), Some(Token::Eq)) {
            return Err(syntax(&format!(
                "MASKED WITH entry for `{}` needs `= <expression>`",
                word.value
            )));
        }
        let expression = render(&entry[eq_at + 1..]);
        if expression.is_empty() {
            return Err(syntax(&format!(
                "MASKED WITH entry for `{}` has no expression",
                word.value
            )));
        }
        // `{col}` is the CUSTOM-mask placeholder; validate with it filled in.
        validate_expr(&expression.replace("{col}", &word.value), "MASKED WITH")?;
        if masks
            .iter()
            .any(|m| m.column.eq_ignore_ascii_case(&word.value))
        {
            return Err(syntax(&format!(
                "column `{}` is masked more than once",
                word.value
            )));
        }
        masks.push(GrantMask {
            column: word.value.clone(),
            expression,
        });
    }
    Ok(masks)
}

fn validate_expr(expr: &str, clause: &str) -> Result<()> {
    let dialect = GenericDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql(expr)
        .map_err(|e| syntax_error(format!("GRANT: {clause} expression: {e}")))?;
    parser
        .parse_expr()
        .map_err(|e| syntax_error(format!("GRANT: {clause} expression: {e}")))?;
    if parser.peek_token().token != Token::EOF {
        return Err(syntax(&format!(
            "{clause} expression `{expr}` has trailing input"
        )));
    }
    Ok(())
}

/// The grant must name what one Ranger mask / row-filter item can hold.
fn validate_grant(stmt: &Statement) -> Result<()> {
    let Statement::Grant(g) = stmt else {
        return Err(syntax("MASKED WITH and ROWS WHERE are valid only on GRANT"));
    };
    match &g.privileges {
        Privileges::Actions(actions)
            if matches!(actions.as_slice(), [Action::Select { columns: None }]) => {}
        Privileges::Actions(actions)
            if matches!(actions.as_slice(), [Action::Select { columns: Some(_) }]) =>
        {
            return Err(syntax(
                "column-level SELECT is not supported; grant SELECT on the table \
                 and mask the columns",
            ));
        }
        _ => {
            return Err(syntax(
                "MASKED WITH and ROWS WHERE need the privilege to be exactly SELECT",
            ));
        }
    }
    match &g.objects {
        Some(GrantObjects::Tables(tables)) if tables.len() == 1 => {
            if tables[0].0.len() < 2 {
                return Err(syntax(
                    "MASKED WITH and ROWS WHERE need a qualified table name \
                     (<schema>.<table> or <catalog>.<schema>.<table>)",
                ));
            }
        }
        _ => {
            return Err(syntax(
                "MASKED WITH and ROWS WHERE apply to exactly one table",
            ));
        }
    }
    match g.grantees.as_slice() {
        [grantee] => match grantee.grantee_type {
            GranteesType::Public => Err(syntax(
                "MASKED WITH and ROWS WHERE cannot be granted TO PUBLIC; name a role, user or group",
            )),
            _ => Ok(()),
        },
        _ => Err(syntax(
            "MASKED WITH and ROWS WHERE need exactly one grantee",
        )),
    }
}

fn syntax(message: &str) -> sqe_core::SqeError {
    syntax_error(format!("GRANT: {message}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(sql: &str) -> GrantWithPolicy {
        try_parse_grant_policy(sql)
            .unwrap()
            .expect("clause present")
    }

    fn err(sql: &str) -> String {
        try_parse_grant_policy(sql).unwrap_err().to_string()
    }

    #[test]
    fn plain_grant_is_not_claimed() {
        assert!(try_parse_grant_policy("GRANT SELECT ON s.t TO ROLE r")
            .unwrap()
            .is_none());
        // A grantee named `masked` before WITH GRANT OPTION stays a plain grant.
        assert!(
            try_parse_grant_policy("GRANT SELECT ON s.t TO ROLE masked WITH GRANT OPTION")
                .unwrap()
                .is_none()
        );
        assert!(try_parse_grant_policy("GRANT SELECT ON rows.t TO ROLE r")
            .unwrap()
            .is_none());
    }

    #[test]
    fn parses_masks_with_nested_parens_and_quotes() {
        let g = parse(
            "GRANT SELECT ON sales.users TO ROLE \"support\" MASKED WITH (\
             email = sha256(email), \"Note\" = concat(substr(note, 1, 2), ', x'))",
        );
        assert_eq!(g.masks.len(), 2);
        assert_eq!(g.masks[0].column, "email");
        assert_eq!(g.masks[0].expression, "sha256(email)");
        assert_eq!(g.masks[1].column, "Note");
        assert_eq!(g.masks[1].expression, "concat(substr(note, 1, 2), ', x')");
        assert!(g.row_filter.is_none());
        assert!(matches!(g.grant, Statement::Grant(_)));
    }

    #[test]
    fn parses_row_filter_and_both_orders() {
        let g =
            parse("grant select on cat.s.orders to role eu rows where region = 'EU' and amt > 0;");
        assert_eq!(g.row_filter.as_deref(), Some("region = 'EU' and amt > 0"));
        assert!(g.masks.is_empty());

        let g = parse(
            "GRANT SELECT ON s.t TO USER alice ROWS WHERE region IN ('EU', 'UK') \
             MASKED WITH (ssn = '***')",
        );
        assert_eq!(g.row_filter.as_deref(), Some("region IN ('EU', 'UK')"));
        assert_eq!(g.masks[0].expression, "'***'");

        let g = parse("GRANT SELECT ON s.t TO GROUP g MASKED WITH (a = NULL) ROWS WHERE b = 1");
        assert_eq!(g.row_filter.as_deref(), Some("b = 1"));
        assert_eq!(g.masks[0].column, "a");
    }

    #[test]
    fn keywords_inside_literals_are_not_clauses() {
        let g = parse("GRANT SELECT ON s.t TO ROLE r ROWS WHERE note <> 'masked with (x)'");
        assert_eq!(g.row_filter.as_deref(), Some("note <> 'masked with (x)'"));
    }

    #[test]
    fn placeholder_expression_validates() {
        let g = parse("GRANT SELECT ON s.t TO ROLE r MASKED WITH (email = sha256({col}))");
        assert_eq!(g.masks[0].expression, "sha256({col})");
    }

    #[test]
    fn rejects_what_a_mask_item_cannot_hold() {
        assert!(
            err("GRANT SELECT, INSERT ON s.t TO ROLE r ROWS WHERE a = 1")
                .contains("exactly SELECT")
        );
        assert!(err("GRANT SELECT (a) ON s.t TO ROLE r ROWS WHERE a = 1").contains("column-level"));
        assert!(err("GRANT SELECT ON t TO ROLE r ROWS WHERE a = 1").contains("qualified"));
        assert!(err("GRANT SELECT ON s.t, s.u TO ROLE r ROWS WHERE a = 1")
            .contains("exactly one table"));
        assert!(err("GRANT SELECT ON SCHEMA s TO ROLE r ROWS WHERE a = 1")
            .contains("exactly one table"));
        assert!(err("GRANT SELECT ON s.t TO PUBLIC ROWS WHERE a = 1").contains("PUBLIC"));
        assert!(
            err("GRANT SELECT ON s.t TO ROLE a, ROLE b ROWS WHERE a = 1").contains("one grantee")
        );
    }

    #[test]
    fn rejects_malformed_clauses() {
        assert!(err("GRANT SELECT ON s.t TO ROLE r MASKED WITH ()").contains("MASKED WITH"));
        assert!(err("GRANT SELECT ON s.t TO ROLE r MASKED WITH (a)").contains("needs `="));
        assert!(err("GRANT SELECT ON s.t TO ROLE r MASKED WITH (a = )").contains("no expression"));
        assert!(
            err("GRANT SELECT ON s.t TO ROLE r MASKED WITH (a = 1, A = 2)")
                .contains("more than once")
        );
        assert!(
            err("GRANT SELECT ON s.t TO ROLE r MASKED WITH (a = 1) MASKED WITH (b = 2)")
                .contains("only once")
        );
        assert!(err("GRANT SELECT ON s.t TO ROLE r ROWS WHERE").contains("requires a filter"));
        assert!(err("GRANT SELECT ON s.t TO ROLE r ROWS WHERE a = = 1")
            .contains("ROWS WHERE expression"));
        assert!(
            err("GRANT SELECT ON s.t TO ROLE r MASKED WITH (a = 1) junk").contains("unexpected")
        );
        assert!(err("GRANT SELECT ON s.t TO ROLE r MASKED WITH (a = 1").contains("unterminated"));
    }
}
