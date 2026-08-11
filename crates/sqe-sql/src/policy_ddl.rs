//! Databricks-inspired SQL DDL for authoring Ranger fine-grained policies.
//!
//! Ranger remains the shared policy store used by SQE and Spark/Kyuubi. These
//! statements replace hand-written Ranger JSON with a SQL management surface.

use sqe_core::{Result, SqeError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyTarget {
    Table(String),
    Tag(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyPrincipalKind {
    User,
    Role,
    Group,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyPrincipal {
    pub kind: PolicyPrincipalKind,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FineGrainedPolicyKind {
    ColumnMask {
        mask_type: String,
        column: Option<String>,
        expression: Option<String>,
    },
    RowFilter {
        expression: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePolicyStatement {
    pub name: String,
    pub or_replace: bool,
    pub target: PolicyTarget,
    pub principal: PolicyPrincipal,
    pub kind: FineGrainedPolicyKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropPolicyStatement {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyDdlStatement {
    Create(CreatePolicyStatement),
    Drop(DropPolicyStatement),
}

/// Parse `CREATE [OR REPLACE] POLICY` and `DROP POLICY [IF EXISTS]`.
/// Returns `Ok(None)` for unrelated SQL.
pub fn try_parse_policy_ddl(sql: &str) -> Result<Option<PolicyDdlStatement>> {
    let sql = sql.trim().trim_end_matches(';').trim();
    if let Some(rest) = strip_prefix_ci(sql, "DROP POLICY ") {
        let (if_exists, rest) = if let Some(rest) = strip_prefix_ci(rest, "IF EXISTS ") {
            (true, rest)
        } else {
            (false, rest)
        };
        let (name, tail) = take_name(rest, "policy name")?;
        require_empty(tail)?;
        return Ok(Some(PolicyDdlStatement::Drop(DropPolicyStatement {
            name,
            if_exists,
        })));
    }

    let Some(mut rest) = strip_prefix_ci(sql, "CREATE ") else {
        return Ok(None);
    };
    let or_replace = if let Some(after) = strip_prefix_ci(rest, "OR REPLACE ") {
        rest = after;
        true
    } else {
        false
    };
    let Some(after_policy) = strip_prefix_ci(rest, "POLICY ") else {
        return Ok(None);
    };
    let (name, rest) = take_name(after_policy, "policy name")?;
    let rest = expect_kw(rest, "ON")?;
    let (target, rest) = if let Some(after) = strip_prefix_ci(rest, "TABLE ") {
        let (table, rest) = take_name(after, "table name")?;
        (PolicyTarget::Table(table), rest)
    } else if let Some(after) = strip_prefix_ci(rest, "TAG ") {
        let (tag, rest) = take_name(after, "tag name")?;
        (PolicyTarget::Tag(tag), rest)
    } else {
        return syntax("expected ON TABLE <name> or ON TAG <name>");
    };

    let rest = rest.trim_start();
    let (kind, principal, tail) = if let Some(after) = strip_prefix_ci(rest, "COLUMN MASK ") {
        parse_column_mask(after, &target)?
    } else if let Some(after) = strip_prefix_ci(rest, "ROW FILTER ") {
        parse_row_filter(after)?
    } else {
        return syntax("expected COLUMN MASK or ROW FILTER");
    };
    require_empty(tail)?;
    Ok(Some(PolicyDdlStatement::Create(CreatePolicyStatement {
        name,
        or_replace,
        target,
        principal,
        kind,
    })))
}

fn parse_column_mask<'a>(
    rest: &'a str,
    target: &PolicyTarget,
) -> Result<(FineGrainedPolicyKind, PolicyPrincipal, &'a str)> {
    let (mask_type, rest) = take_name(rest, "mask type")?;
    let (principal, mut rest) = parse_principal(rest)?;
    let mut column = None;
    if let Some(after) = strip_prefix_ci(rest.trim_start(), "ON COLUMN ") {
        let (value, after) = take_name(after, "column name")?;
        column = Some(value);
        rest = after;
    }
    match target {
        PolicyTarget::Table(_) if column.is_none() => {
            return syntax("a table COLUMN MASK requires ON COLUMN <name>");
        }
        PolicyTarget::Tag(_) if column.is_some() => {
            return syntax("a tag COLUMN MASK must not specify ON COLUMN");
        }
        _ => {}
    }
    let (expression, rest) = if let Some(after) = strip_prefix_ci(rest.trim_start(), "USING ") {
        let (expr, rest) = take_parenthesized(after)?;
        (Some(expr), rest)
    } else {
        (None, rest)
    };
    if mask_type.eq_ignore_ascii_case("CUSTOM") && expression.is_none() {
        return syntax("CUSTOM COLUMN MASK requires USING (<expression>)");
    }
    if !mask_type.eq_ignore_ascii_case("CUSTOM") && expression.is_some() {
        return syntax("USING is valid only with the CUSTOM mask type");
    }
    Ok((FineGrainedPolicyKind::ColumnMask { mask_type, column, expression }, principal, rest))
}

fn parse_row_filter(rest: &str) -> Result<(FineGrainedPolicyKind, PolicyPrincipal, &str)> {
    let (principal, rest) = parse_principal(rest)?;
    let rest = expect_kw(rest, "USING")?;
    let (expression, rest) = take_parenthesized(rest)?;
    Ok((FineGrainedPolicyKind::RowFilter { expression }, principal, rest))
}

fn parse_principal(rest: &str) -> Result<(PolicyPrincipal, &str)> {
    let rest = expect_kw(rest, "TO")?;
    let rest = rest.trim_start();
    let (kind, rest) = if let Some(after) = strip_prefix_ci(rest, "USER ") {
        (PolicyPrincipalKind::User, after)
    } else if let Some(after) = strip_prefix_ci(rest, "ROLE ") {
        (PolicyPrincipalKind::Role, after)
    } else if let Some(after) = strip_prefix_ci(rest, "GROUP ") {
        (PolicyPrincipalKind::Group, after)
    } else {
        return syntax("expected TO USER, TO ROLE, or TO GROUP");
    };
    let (name, rest) = take_name(rest, "principal name")?;
    Ok((PolicyPrincipal { kind, name }, rest))
}

fn take_parenthesized(s: &str) -> Result<(String, &str)> {
    let s = s.trim_start();
    if !s.starts_with('(') {
        return syntax("expected a parenthesized SQL expression");
    }
    let mut depth = 0usize;
    let mut quote = None;
    let mut chars = s.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        if let Some(q) = quote {
            if c == q {
                if chars.peek().is_some_and(|(_, next)| *next == q) {
                    chars.next();
                } else {
                    quote = None;
                }
            }
            continue;
        }
        match c {
            '\'' | '"' => quote = Some(c),
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    let expr = s[1..i].trim().to_string();
                    if expr.is_empty() {
                        return syntax("policy expression must not be empty");
                    }
                    return Ok((expr, &s[i + 1..]));
                }
            }
            _ => {}
        }
    }
    syntax("unterminated parenthesized policy expression")
}

fn take_name<'a>(s: &'a str, what: &str) -> Result<(String, &'a str)> {
    let s = s.trim_start();
    if s.starts_with('"') {
        if let Some(end) = s[1..].find('"') {
            let end = end + 1;
            return Ok((s[1..end].to_string(), &s[end + 1..]));
        }
        return syntax(&format!("unterminated quoted {what}"));
    }
    let end = s.find(char::is_whitespace).unwrap_or(s.len());
    if end == 0 {
        return syntax(&format!("expected {what}"));
    }
    Ok((s[..end].to_string(), &s[end..]))
}

fn expect_kw<'a>(s: &'a str, keyword: &str) -> Result<&'a str> {
    let s = s.trim_start();
    strip_prefix_ci(s, keyword)
        .filter(|rest| rest.is_empty() || rest.starts_with(char::is_whitespace) || rest.starts_with('('))
        .map(str::trim_start)
        .ok_or_else(|| SqeError::Execution(format!("CREATE POLICY: expected {keyword}")))
}

fn strip_prefix_ci<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    s.get(..prefix.len())
        .filter(|head| head.eq_ignore_ascii_case(prefix))
        .map(|_| &s[prefix.len()..])
}

fn require_empty(rest: &str) -> Result<()> {
    if rest.trim().is_empty() {
        Ok(())
    } else {
        syntax(&format!("unexpected trailing input `{}`", rest.trim()))
    }
}

fn syntax<T>(message: &str) -> Result<T> {
    Err(SqeError::Execution(format!("CREATE POLICY: {message}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_resource_mask() {
        let parsed = try_parse_policy_ddl(
            "CREATE OR REPLACE POLICY p ON TABLE sales_wh.ac.orders \
             COLUMN MASK MASK_NULL TO ROLE engineer ON COLUMN amount",
        ).unwrap().unwrap();
        let PolicyDdlStatement::Create(stmt) = parsed else { panic!("create") };
        assert!(stmt.or_replace);
        assert_eq!(stmt.target, PolicyTarget::Table("sales_wh.ac.orders".into()));
        assert_eq!(stmt.principal.kind, PolicyPrincipalKind::Role);
        assert!(matches!(stmt.kind, FineGrainedPolicyKind::ColumnMask { column: Some(ref c), expression: None, .. } if c == "amount"));
    }

    #[test]
    fn parses_row_filter_with_quoted_literal() {
        let parsed = try_parse_policy_ddl(
            "CREATE POLICY p ON TABLE c.s.t ROW FILTER TO ROLE engineer \
             USING (region = 'EU' AND note = 'it''s (safe)')",
        ).unwrap().unwrap();
        let PolicyDdlStatement::Create(stmt) = parsed else { panic!("create") };
        assert!(matches!(stmt.kind, FineGrainedPolicyKind::RowFilter { ref expression } if expression.contains("it's (safe)") || expression.contains("it''s (safe)")));
    }

    #[test]
    fn parses_tag_custom_mask_and_drop() {
        assert!(matches!(
            try_parse_policy_ddl("CREATE POLICY p ON TAG pii COLUMN MASK CUSTOM TO ROLE r USING (substr({col}, 2, 3))").unwrap(),
            Some(PolicyDdlStatement::Create(_))
        ));
        assert_eq!(
            try_parse_policy_ddl("DROP POLICY IF EXISTS p;").unwrap(),
            Some(PolicyDdlStatement::Drop(DropPolicyStatement { name: "p".into(), if_exists: true }))
        );
    }

    #[test]
    fn does_not_claim_unrelated_create() {
        assert_eq!(try_parse_policy_ddl("CREATE TABLE t (id INT)").unwrap(), None);
    }
}
