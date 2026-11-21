//! Column-tag authoring DDL: `ALTER TABLE ... SET TAGS / UNSET TAGS` (SQE-native)
//! and the Snowflake-compatible `MODIFY|ALTER COLUMN <col> SET TAG / UNSET TAG`
//! forms. These author column->tag-label associations stored in the
//! `sqe.column-tags` table property. They are DISTINCT from Iceberg snapshot
//! tags (`CREATE TAG` / `DROP TAG`, see `ddl.rs`).

use std::collections::BTreeMap;

use sqe_core::{Result, SqeError, SqeErrorCode};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TagAction {
    Set,
    /// Remove the listed tags; an empty tag list removes ALL tags on the column.
    Unset,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnTagOp {
    pub column: String,
    pub tags: Vec<String>,
    pub action: TagAction,
    /// Values of the tags in `tags` that were given one (`'k' = 'v'`,
    /// `SET TAG k = 'v'`), by tag key. A `Set` of a key with no entry here
    /// stores the key without a value. Always empty for `Unset`.
    pub values: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetTagsStatement {
    pub table: String,
    pub ops: Vec<ColumnTagOp>,
}

/// Try to parse a column-tag DDL. Returns `Ok(None)` if `sql` is not one of the
/// SET TAGS / UNSET TAGS / MODIFY|ALTER COLUMN SET TAG forms, so the caller falls
/// through to sqlparser. Returns `Err` when the input is recognizably a tag
/// statement but malformed.
pub fn try_parse_set_tags(sql: &str) -> Result<Option<SetTagsStatement>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("ALTER TABLE ") {
        return Ok(None);
    }
    let after_at = trimmed["ALTER TABLE ".len()..].trim_start();
    let (table, rest) = split_identifier(after_at)?;
    let rest = rest.trim_start();
    let rest_upper = rest.to_ascii_uppercase();

    if rest_upper.starts_with("SET TAGS") {
        let body = rest["SET TAGS".len()..].trim_start();
        let ops = parse_native_set_list(body)?;
        return Ok(Some(SetTagsStatement { table, ops }));
    }
    if rest_upper.starts_with("UNSET TAGS") {
        let body = rest["UNSET TAGS".len()..].trim_start();
        let ops = parse_native_unset_list(body)?;
        return Ok(Some(SetTagsStatement { table, ops }));
    }

    let after_col = if rest_upper.starts_with("MODIFY COLUMN ") {
        Some(&rest["MODIFY COLUMN ".len()..])
    } else if rest_upper.starts_with("ALTER COLUMN ") {
        Some(&rest["ALTER COLUMN ".len()..])
    } else {
        None
    };
    if let Some(after) = after_col {
        let (column, rest2) = split_identifier(after)?;
        let rest2 = rest2.trim_start();
        let rest2_upper = rest2.to_ascii_uppercase();
        // Databricks: `ALTER COLUMN c SET TAGS ('k' = 'v', 'k2')` and
        // `ALTER COLUMN c UNSET TAGS ('k')`. Checked before the singular
        // Snowflake forms, which are prefixes of these.
        if let Some(body) = strip_kw(rest2, &rest2_upper, "SET TAGS") {
            let (tags, values) = keys_and_values(parse_databricks_tag_list(body, "SET TAGS")?);
            return Ok(Some(SetTagsStatement {
                table,
                ops: vec![ColumnTagOp {
                    column,
                    tags,
                    action: TagAction::Set,
                    values,
                }],
            }));
        }
        if let Some(body) = strip_kw(rest2, &rest2_upper, "UNSET TAGS") {
            let mut tags = Vec::new();
            for (key, value) in parse_databricks_tag_list(body, "UNSET TAGS")? {
                if value.is_some() {
                    return Err(SqeError::with_code(
                        SqeErrorCode::ExecutionFailed,
                        format!(
                            "UNSET TAGS: name the tag to remove without a value, got `{key} = ...`"
                        ),
                    ));
                }
                tags.push(key);
            }
            return Ok(Some(SetTagsStatement {
                table,
                ops: vec![ColumnTagOp {
                    column,
                    tags,
                    action: TagAction::Unset,
                    values: BTreeMap::new(),
                }],
            }));
        }
        // SET TAG (singular) but not SET TAGS (plural, native form).
        if rest2_upper.starts_with("SET TAG") && !rest2_upper.starts_with("SET TAGS") {
            let body = rest2["SET TAG".len()..].trim_start();
            let (tags, values) = keys_and_values(parse_snowflake_assignments(body)?);
            return Ok(Some(SetTagsStatement {
                table,
                ops: vec![ColumnTagOp {
                    column,
                    tags,
                    action: TagAction::Set,
                    values,
                }],
            }));
        }
        if rest2_upper.starts_with("UNSET TAG") && !rest2_upper.starts_with("UNSET TAGS") {
            let body = rest2["UNSET TAG".len()..].trim_start();
            let tags = parse_snowflake_names(body)?;
            return Ok(Some(SetTagsStatement {
                table,
                ops: vec![ColumnTagOp {
                    column,
                    tags,
                    action: TagAction::Unset,
                    values: BTreeMap::new(),
                }],
            }));
        }
        // MODIFY/ALTER COLUMN but not a tag op: not ours.
        return Ok(None);
    }
    Ok(None)
}

/// `ALTER TABLE t MODIFY COLUMN c SET MASKING POLICY p` and its `UNSET` form.
///
/// Snowflake's spelling for binding a named masking policy to a column. `policy`
/// is `None` for `UNSET MASKING POLICY`, which Snowflake also writes without a
/// name because a column carries at most one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaskingPolicyOp {
    pub table: String,
    pub column: String,
    /// `None` means detach whatever is attached.
    pub policy: Option<String>,
}

/// Try to parse the masking-policy attach/detach DDL.
///
/// Separate from `try_parse_set_tags` because the two cannot be resolved at the
/// same layer: a tag name is final at parse time, while a POLICY name has to be
/// looked up in the policy store to find the tag it is keyed on. The parser has no
/// store, so it hands the name onward.
pub fn try_parse_masking_policy(sql: &str) -> Result<Option<MaskingPolicyOp>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("ALTER TABLE ") {
        return Ok(None);
    }
    let after_at = trimmed["ALTER TABLE ".len()..].trim_start();
    let (table, rest) = split_identifier(after_at)?;
    let rest = rest.trim_start();
    let rest_upper = rest.to_ascii_uppercase();

    let after_col = if rest_upper.starts_with("MODIFY COLUMN ") {
        &rest["MODIFY COLUMN ".len()..]
    } else if rest_upper.starts_with("ALTER COLUMN ") {
        &rest["ALTER COLUMN ".len()..]
    } else {
        return Ok(None);
    };
    let (column, rest2) = split_identifier(after_col)?;
    let rest2 = rest2.trim_start();
    let rest2_upper = rest2.to_ascii_uppercase();

    // UNSET is checked FIRST. "SET MASKING POLICY" is a SUFFIX of
    // "UNSET MASKING POLICY", so testing SET first matches the UNSET form and turns
    // a detach into an attach -- silently, and in the unsafe direction. Unlike the
    // POLICY/POLICIES pair in the classifier, where I once claimed an ordering
    // hazard that did not exist, this one is real: swapping these two branches
    // fails `unset_is_never_read_as_set` and `the_detach_form_parses_...`.
    if let Some(body) = strip_kw(rest2, &rest2_upper, "UNSET MASKING POLICY") {
        let name = body.trim();
        if !name.is_empty() {
            // Snowflake allows naming the policy on a detach. The name is not
            // needed (a column carries one policy) but it is still parsed, so a
            // malformed one is an error rather than being ignored.
            let (_named, _) = split_identifier(name)?;
        }
        return Ok(Some(MaskingPolicyOp {
            table,
            column,
            policy: None,
        }));
    }
    if let Some(body) = strip_kw(rest2, &rest2_upper, "SET MASKING POLICY") {
        // Checked BEFORE split_identifier, whose own error names "SET TAGS" --
        // accurate for the forms it was written for, misleading here. A caller who
        // forgot the name should be told how to find one.
        if body.trim().is_empty() {
            return Err(SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                "SET MASKING POLICY needs a policy name. Run SHOW MASKING POLICIES \
                 to see what is available."
                    .to_string(),
            ));
        }
        let (name, _) = split_identifier(body)?;
        return Ok(Some(MaskingPolicyOp {
            table,
            column,
            policy: Some(name),
        }));
    }
    Ok(None)
}

/// Strip a case-insensitive keyword prefix, returning the remainder.
fn strip_kw<'a>(s: &'a str, upper: &str, keyword: &str) -> Option<&'a str> {
    upper.starts_with(keyword).then(|| &s[keyword.len()..])
}

/// Read a leading (possibly dotted/quoted) identifier; return (cleaned, rest).
/// `"a"."b"` and `a.b` both yield `a.b`. Quotes are stripped, dots preserved.
fn split_identifier(s: &str) -> Result<(String, &str)> {
    let s = s.trim_start();
    let mut out = String::new();
    let mut in_quote = false;
    let mut end = 0usize;
    for (i, c) in s.char_indices() {
        match c {
            '"' => {
                in_quote = !in_quote;
                end = i + c.len_utf8();
            }
            _ if in_quote => {
                out.push(c);
                end = i + c.len_utf8();
            }
            _ if c.is_alphanumeric() || c == '_' || c == '.' => {
                out.push(c);
                end = i + c.len_utf8();
            }
            _ => break,
        }
    }
    if in_quote {
        return Err(SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            format!("SET TAGS: unterminated double-quote in identifier near `{s}`"),
        ));
    }
    if out.is_empty() {
        return Err(SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            format!("SET TAGS: expected an identifier near `{s}`"),
        ));
    }
    Ok((out, &s[end..]))
}

/// Strip a balanced outer `( ... )` and return the inner slice. `context`
/// names what is being parsed (e.g. the column) so a missing paren points the
/// operator at the offending token.
fn strip_parens<'a>(s: &'a str, context: &str) -> Result<&'a str> {
    let s = s.trim();
    if !s.starts_with('(') {
        return Err(SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            format!("SET TAGS: {context}: expected a `(` to open the tag list, got `{s}`"),
        ));
    }
    s.strip_prefix('(')
        .and_then(|x| x.strip_suffix(')'))
        .ok_or_else(|| {
            SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                format!("SET TAGS: {context}: missing closing `)` for the tag list near `{s}`"),
            )
        })
}

/// Split on top-level `,` (ignoring commas inside parentheses or single quotes).
fn split_top_level(s: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut in_str = false;
    let mut cur = String::new();
    for c in s.chars() {
        match c {
            '\'' => {
                in_str = !in_str;
                cur.push(c);
            }
            '(' if !in_str => {
                depth += 1;
                cur.push(c);
            }
            ')' if !in_str => {
                depth -= 1;
                cur.push(c);
            }
            ',' if !in_str && depth == 0 => {
                parts.push(cur.trim().to_string());
                cur.clear();
            }
            _ => cur.push(c),
        }
    }
    if !cur.trim().is_empty() {
        parts.push(cur.trim().to_string());
    }
    parts
}

/// Strip surrounding single quotes or double quotes from a tag/name token.
fn unquote(s: &str) -> String {
    let s = s.trim();
    if (s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2)
        || (s.starts_with('"') && s.ends_with('"') && s.len() >= 2)
    {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

/// Byte offset of the first `=` outside single or double quotes.
fn find_top_level_eq(s: &str) -> Option<usize> {
    let mut quote: Option<char> = None;
    for (i, c) in s.char_indices() {
        match (quote, c) {
            (None, '\'' | '"') => quote = Some(c),
            (Some(q), _) if c == q => quote = None,
            (None, '=') => return Some(i),
            _ => {}
        }
    }
    None
}

/// Databricks' tag list `( 'key' = 'value', 'key2', ... )` as `(key, value)`
/// pairs; a key-only tag has no value. Keys are string literals in Databricks;
/// bare and double-quoted names are accepted as well. Keys are kept as
/// written (no case folding, dots allowed), like the native form's tags.
fn parse_databricks_tag_list(body: &str, context: &str) -> Result<Vec<(String, Option<String>)>> {
    let inner = strip_parens(body, context)?;
    let mut tags = Vec::new();
    for item in split_top_level(inner) {
        // `'k' =` with nothing after it is an error; `'k' = ''` is an empty
        // value, which `keys_and_values` stores as a key-only tag.
        let (key, value, missing_value) = match find_top_level_eq(&item) {
            Some(eq) => {
                let raw = &item[eq + 1..];
                (
                    unquote(&item[..eq]),
                    Some(unquote(raw)),
                    raw.trim().is_empty(),
                )
            }
            None => (unquote(&item), None, false),
        };
        if key.is_empty() {
            return Err(SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                format!("{context}: expected a tag name, got `{item}`"),
            ));
        }
        if missing_value {
            return Err(SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                format!("{context}: tag `{key}` has `=` but no value"),
            ));
        }
        tags.push((key, value));
    }
    if tags.is_empty() {
        return Err(SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            format!("{context}: empty tag list"),
        ));
    }
    Ok(tags)
}

/// Databricks' table-level form (`SET TAGS ('k' = 'v')`, `UNSET TAGS ('k')`)
/// starts its list with a string literal, where the native form names a
/// column. Tags on tables, views, schemas and catalogs need storage SQE does
/// not have yet, so this is refused with a pointer to the column form rather
/// than a parse error about identifiers.
fn refuse_table_level_tags(item: &str, context: &str) -> Result<()> {
    if item.trim_start().starts_with('\'') {
        return Err(SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            format!(
                "{context}: tags on a table are not supported yet, only tags on its \
                 columns. Use ALTER TABLE <table> ALTER COLUMN <column> {context} ('key' = 'value')"
            ),
        ));
    }
    Ok(())
}

/// `( col = ( 'tag', ... ), col2 = (...) )`
fn parse_native_set_list(body: &str) -> Result<Vec<ColumnTagOp>> {
    let inner = strip_parens(body, "tag list")?;
    let mut ops = Vec::new();
    for item in split_top_level(inner) {
        refuse_table_level_tags(&item, "SET TAGS")?;
        let eq = item.find('=').ok_or_else(|| {
            SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                format!("SET TAGS: expected `col = (...)`, got `{item}`"),
            )
        })?;
        let (col, _) = split_identifier(item[..eq].trim())?;
        let tags_part = item[eq + 1..].trim();
        let tags_inner = strip_parens(tags_part, &format!("column `{col}`"))?;
        let tags: Vec<String> = split_top_level(tags_inner)
            .into_iter()
            .map(|t| unquote(&t))
            .filter(|t| !t.is_empty())
            .collect();
        if tags.is_empty() {
            return Err(SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                format!("SET TAGS: column `{col}` has no tags"),
            ));
        }
        ops.push(ColumnTagOp {
            column: col,
            tags,
            action: TagAction::Set,
            values: BTreeMap::new(),
        });
    }
    if ops.is_empty() {
        return Err(SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            "SET TAGS: empty tag list",
        ));
    }
    Ok(ops)
}

/// `( col, col2, ... )` -> Unset with empty tags (remove all on each column).
fn parse_native_unset_list(body: &str) -> Result<Vec<ColumnTagOp>> {
    let inner = strip_parens(body, "column list")?;
    let mut ops = Vec::new();
    for item in split_top_level(inner) {
        refuse_table_level_tags(&item, "UNSET TAGS")?;
        let (col, _) = split_identifier(item.trim())?;
        ops.push(ColumnTagOp {
            column: col,
            tags: vec![],
            action: TagAction::Unset,
            values: BTreeMap::new(),
        });
    }
    if ops.is_empty() {
        return Err(SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            "UNSET TAGS: empty column list",
        ));
    }
    Ok(ops)
}

/// Snowflake `name [= 'val'] [, name [= 'val']]*` -> tag names (values discarded).
fn parse_snowflake_assignments(body: &str) -> Result<Vec<(String, Option<String>)>> {
    let mut names = Vec::new();
    for item in split_top_level(body) {
        let (name_part, value) = match find_top_level_eq(&item) {
            Some(eq) => {
                let value = unquote(item[eq + 1..].trim());
                (item[..eq].trim(), (!value.is_empty()).then_some(value))
            }
            None => (item.trim(), None),
        };
        let (name, _) = split_identifier(&unquote(name_part))?;
        names.push((name, value));
    }
    if names.is_empty() {
        return Err(SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            "SET TAG: expected a tag name",
        ));
    }
    Ok(names)
}

/// Split parsed `(key, value)` pairs into the op's key list and value map.
/// Keys keep their first position; a key given twice keeps its last value.
/// An empty value (`'k' = ''`) is stored as a key-only tag, which is how
/// Databricks reports a key-only tag.
fn keys_and_values(
    pairs: Vec<(String, Option<String>)>,
) -> (Vec<String>, BTreeMap<String, String>) {
    let mut tags: Vec<String> = Vec::new();
    let mut values = BTreeMap::new();
    for (key, value) in pairs {
        match value.filter(|v| !v.is_empty()) {
            Some(v) => {
                values.insert(key.clone(), v);
            }
            None => {
                values.remove(&key);
            }
        }
        if !tags.contains(&key) {
            tags.push(key);
        }
    }
    (tags, values)
}

/// Snowflake `name [, name]*` -> tag names.
fn parse_snowflake_names(body: &str) -> Result<Vec<String>> {
    let mut names = Vec::new();
    for item in split_top_level(body) {
        let (name, _) = split_identifier(&unquote(item.trim()))?;
        names.push(name);
    }
    if names.is_empty() {
        return Err(SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            "UNSET TAG: expected a tag name",
        ));
    }
    Ok(names)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(sql: &str) -> SetTagsStatement {
        try_parse_set_tags(sql)
            .expect("parse must not error")
            .expect("must recognize as SET TAGS")
    }

    #[test]
    fn native_set_single_column_multi_tag() {
        let s = parse("ALTER TABLE sales.orders SET TAGS (email = ('PII','GDPR'))");
        assert_eq!(s.table, "sales.orders");
        assert_eq!(s.ops.len(), 1);
        assert_eq!(s.ops[0].column, "email");
        assert_eq!(s.ops[0].tags, vec!["PII", "GDPR"]);
        assert_eq!(s.ops[0].action, TagAction::Set);
    }

    #[test]
    fn native_set_multi_column() {
        let s = parse("ALTER TABLE t SET TAGS (email = ('PII'), salary = ('PII','CONF'))");
        assert_eq!(s.ops.len(), 2);
        assert_eq!(s.ops[0].column, "email");
        assert_eq!(s.ops[0].tags, vec!["PII"]);
        assert_eq!(s.ops[1].column, "salary");
        assert_eq!(s.ops[1].tags, vec!["PII", "CONF"]);
    }

    #[test]
    fn native_set_bare_identifier_tags() {
        // Tags may be bare identifiers, not just quoted strings.
        let s = parse("ALTER TABLE t SET TAGS (email = (PII, GDPR))");
        assert_eq!(s.ops[0].tags, vec!["PII", "GDPR"]);
    }

    #[test]
    fn native_unset_tags_removes_all() {
        let s = parse("ALTER TABLE t UNSET TAGS (email, salary)");
        assert_eq!(s.ops.len(), 2);
        assert_eq!(s.ops[0].column, "email");
        assert!(s.ops[0].tags.is_empty());
        assert_eq!(s.ops[0].action, TagAction::Unset);
        assert_eq!(s.ops[1].column, "salary");
        assert_eq!(s.ops[1].action, TagAction::Unset);
    }

    #[test]
    fn snowflake_modify_column_set_tag_keeps_the_value() {
        let s = parse("ALTER TABLE t MODIFY COLUMN email SET TAG PII = 'true'");
        assert_eq!(s.ops.len(), 1);
        assert_eq!(s.ops[0].column, "email");
        assert_eq!(s.ops[0].tags, vec!["PII"]);
        assert_eq!(s.ops[0].action, TagAction::Set);
        assert_eq!(
            s.ops[0].values,
            BTreeMap::from([("PII".to_string(), "true".to_string())])
        );
    }

    #[test]
    fn snowflake_modify_column_multi_tag() {
        let s = parse("ALTER TABLE t MODIFY COLUMN email SET TAG PII = 'true', GDPR = 'x'");
        assert_eq!(s.ops[0].tags, vec!["PII", "GDPR"]);
        assert_eq!(s.ops[0].values.get("GDPR").map(String::as_str), Some("x"));
    }

    #[test]
    fn an_empty_or_missing_value_is_key_only() {
        let s = parse("ALTER TABLE t MODIFY COLUMN email SET TAG PII = '', GDPR");
        assert_eq!(s.ops[0].tags, vec!["PII", "GDPR"]);
        assert!(s.ops[0].values.is_empty(), "{:?}", s.ops[0].values);
        let s = parse("ALTER TABLE t ALTER COLUMN email SET TAGS ('pii' = '')");
        assert!(s.ops[0].values.is_empty());
    }

    #[test]
    fn a_key_given_twice_keeps_the_last_value() {
        let s = parse("ALTER TABLE t ALTER COLUMN email SET TAGS ('k' = 'a', 'k' = 'b')");
        assert_eq!(s.ops[0].values.get("k").map(String::as_str), Some("b"));
    }

    #[test]
    fn snowflake_alter_column_synonym() {
        // ALTER COLUMN is a synonym for MODIFY COLUMN.
        let s = parse("ALTER TABLE t ALTER COLUMN email SET TAG PII");
        assert_eq!(s.ops[0].column, "email");
        assert_eq!(s.ops[0].tags, vec!["PII"]);
    }

    #[test]
    fn snowflake_unset_tag_named() {
        let s = parse("ALTER TABLE t MODIFY COLUMN email UNSET TAG GDPR");
        assert_eq!(s.ops[0].column, "email");
        assert_eq!(s.ops[0].tags, vec!["GDPR"]);
        assert_eq!(s.ops[0].action, TagAction::Unset);
    }

    #[test]
    fn databricks_column_set_tags_key_value_and_key_only() {
        let s = parse("ALTER TABLE c.s.t ALTER COLUMN email SET TAGS ('lt_uc' = 'v', 'pii')");
        assert_eq!(s.table, "c.s.t");
        assert_eq!(s.ops.len(), 1);
        assert_eq!(s.ops[0].column, "email");
        assert_eq!(s.ops[0].tags, vec!["lt_uc", "pii"]);
        assert_eq!(s.ops[0].action, TagAction::Set);
        assert_eq!(
            s.ops[0].values,
            BTreeMap::from([("lt_uc".to_string(), "v".to_string())])
        );

        let s = parse("alter table c.s.t alter column email set tags ('lt_uc');");
        assert_eq!(s.ops[0].tags, vec!["lt_uc"]);
        assert_eq!(s.ops[0].action, TagAction::Set);
    }

    #[test]
    fn databricks_column_unset_tags_names_the_tags() {
        let s = parse("ALTER TABLE c.s.t ALTER COLUMN email UNSET TAGS ('lt_uc', 'pii')");
        assert_eq!(s.ops[0].column, "email");
        assert_eq!(s.ops[0].tags, vec!["lt_uc", "pii"]);
        assert_eq!(s.ops[0].action, TagAction::Unset);
    }

    #[test]
    fn databricks_tag_keys_keep_dots_case_and_quoted_commas() {
        // A tag key is a string literal: dots, case and commas inside the
        // quotes are part of it, and an `=` inside a value does not split.
        let s = parse(
            "ALTER TABLE t ALTER COLUMN \"Email\" SET TAGS ('Live.PII' = 'a=b', 'x,y', \"dq\")",
        );
        assert_eq!(s.ops[0].column, "Email");
        assert_eq!(s.ops[0].tags, vec!["Live.PII", "x,y", "dq"]);
        assert_eq!(
            s.ops[0].values.get("Live.PII").map(String::as_str),
            Some("a=b")
        );
    }

    #[test]
    fn databricks_unset_tags_with_a_value_errors() {
        let err = try_parse_set_tags("ALTER TABLE t ALTER COLUMN email UNSET TAGS ('k' = 'v')")
            .expect_err("UNSET TAGS takes names only");
        assert!(err.to_string().contains("without a value"), "{err}");
    }

    #[test]
    fn databricks_malformed_column_tag_lists_error() {
        for sql in [
            "ALTER TABLE t ALTER COLUMN email SET TAGS ()",
            "ALTER TABLE t ALTER COLUMN email SET TAGS ('k' = )",
            "ALTER TABLE t ALTER COLUMN email SET TAGS 'k' = 'v'",
            "ALTER TABLE t ALTER COLUMN email UNSET TAGS ()",
            "ALTER TABLE t ALTER COLUMN email SET TAGS ('k' = 'v'",
        ] {
            assert!(try_parse_set_tags(sql).is_err(), "must error: {sql}");
        }
    }

    #[test]
    fn databricks_table_level_tags_are_refused_with_a_pointer() {
        for sql in [
            "ALTER TABLE c.s.t SET TAGS ('lt_uc' = 'v')",
            "ALTER TABLE c.s.t SET TAGS ('lt_uc')",
            "ALTER TABLE c.s.t UNSET TAGS ('lt_uc')",
        ] {
            let err = try_parse_set_tags(sql).expect_err(sql);
            let msg = err.to_string();
            assert!(msg.contains("not supported yet"), "{sql}: {msg}");
            assert!(msg.contains("ALTER COLUMN"), "{sql}: {msg}");
        }
    }

    #[test]
    fn snowflake_singular_forms_still_parse_after_databricks_check() {
        // `SET TAG` is a prefix of `SET TAGS`; the singular form must still
        // reach the Snowflake parser.
        let s = parse("ALTER TABLE t ALTER COLUMN email SET TAG tags_owner = 'x'");
        assert_eq!(s.ops[0].tags, vec!["tags_owner"]);
        let s = parse("ALTER TABLE t ALTER COLUMN email UNSET TAG tags_owner");
        assert_eq!(s.ops[0].tags, vec!["tags_owner"]);
        assert_eq!(s.ops[0].action, TagAction::Unset);
    }

    #[test]
    fn quoted_table_and_column() {
        let s = parse(r#"ALTER TABLE "sales"."orders" SET TAGS ("email" = ('PII'))"#);
        assert_eq!(s.table, "sales.orders");
        assert_eq!(s.ops[0].column, "email");
    }

    #[test]
    fn trailing_semicolon_ok() {
        let s = parse("ALTER TABLE t SET TAGS (c = ('X'));");
        assert_eq!(s.ops[0].tags, vec!["X"]);
    }

    #[test]
    fn not_set_tags_returns_none() {
        // SET TBLPROPERTIES, ADD COLUMN, ALTER COLUMN TYPE, CREATE/DROP TAG must
        // all fall through (Ok(None)).
        for sql in [
            "ALTER TABLE t SET TBLPROPERTIES ('write.format.default' = 'parquet')",
            "ALTER TABLE t ADD COLUMN x INT",
            "ALTER TABLE t ALTER COLUMN x TYPE BIGINT",
            "ALTER TABLE t CREATE TAG v1",
            "ALTER TABLE t DROP TAG v1",
            "SELECT 1",
        ] {
            assert!(
                try_parse_set_tags(sql).unwrap().is_none(),
                "must not claim: {sql}"
            );
        }
    }

    #[test]
    fn malformed_set_tags_errors() {
        // Recognizably SET TAGS but broken -> Err, not None (clear diagnostic).
        assert!(try_parse_set_tags("ALTER TABLE t SET TAGS (email = )").is_err());
        assert!(try_parse_set_tags("ALTER TABLE t SET TAGS email = ('PII')").is_err());
    }

    #[test]
    fn unbalanced_inner_paren_errors_and_names_column() {
        // `SET TAGS (email = ('PII'` — missing the closing `)` on both the tag
        // list and the column's value list. Must Err (not None, not a wrong
        // parse) and name the offending column.
        let err = try_parse_set_tags("ALTER TABLE t SET TAGS (email = ('PII'")
            .expect_err("unbalanced parens must error");
        let msg = err.to_string();
        assert!(
            msg.contains("email"),
            "error must name the column, got: {msg}"
        );
    }

    #[test]
    fn trailing_garbage_after_tag_list_errors() {
        // Junk after the closing `)` of the tag list must not be silently
        // ignored -- it indicates a malformed statement.
        assert!(
            try_parse_set_tags("ALTER TABLE t SET TAGS (email = ('PII')) junk").is_err(),
            "trailing garbage after the tag list must error"
        );
    }

    #[test]
    fn unterminated_quoted_identifier_errors() {
        // An unterminated double-quote in an identifier must error instead of
        // silently truncating the rest of the statement.
        let err = try_parse_set_tags(r#"ALTER TABLE "orders SET TAGS (email = ('PII'))"#)
            .expect_err("unterminated quote must error");
        assert!(
            err.to_string().contains("unterminated"),
            "error must mention the unterminated quote, got: {err}"
        );
    }
}

#[cfg(test)]
mod masking_policy_parse_tests {
    use super::*;

    #[test]
    fn the_snowflake_attach_form_parses() {
        for sql in [
            "ALTER TABLE c.ns.t MODIFY COLUMN ssn SET MASKING POLICY mask_ssn",
            "alter table c.ns.t alter column ssn set masking policy mask_ssn;",
            "ALTER TABLE \"c\".\"ns\".\"t\" MODIFY COLUMN \"ssn\" SET MASKING POLICY \"mask_ssn\"",
        ] {
            let op = try_parse_masking_policy(sql)
                .expect(sql)
                .unwrap_or_else(|| panic!("not parsed: {sql}"));
            assert_eq!(op.table, "c.ns.t", "{sql}");
            assert_eq!(op.column, "ssn", "{sql}");
            assert_eq!(op.policy.as_deref(), Some("mask_ssn"), "{sql}");
        }
    }

    #[test]
    fn the_detach_form_parses_with_and_without_a_name() {
        for sql in [
            "ALTER TABLE c.ns.t MODIFY COLUMN ssn UNSET MASKING POLICY",
            "ALTER TABLE c.ns.t MODIFY COLUMN ssn UNSET MASKING POLICY mask_ssn",
        ] {
            let op = try_parse_masking_policy(sql).expect(sql).expect(sql);
            assert_eq!(op.policy, None, "{sql}: UNSET always detaches");
            assert_eq!(op.column, "ssn");
        }
    }

    /// THE ordering test. "SET MASKING POLICY" is a SUFFIX of
    /// "UNSET MASKING POLICY", so checking SET first matches the UNSET input and
    /// turns a detach into an attach: it would leave the column masked when the
    /// caller asked to unmask, or attach a garbage policy name.
    ///
    /// Mutation-verified: forcing the SET branch to be evaluated first fails this
    /// test and `the_detach_form_parses_with_and_without_a_name`.
    #[test]
    fn unset_is_never_read_as_set() {
        let op =
            try_parse_masking_policy("ALTER TABLE c.ns.t MODIFY COLUMN ssn UNSET MASKING POLICY")
                .expect("parse")
                .expect("recognised");
        assert_eq!(op.policy, None, "UNSET must detach, never attach");
    }

    #[test]
    fn an_attach_with_no_policy_name_is_an_error() {
        let err =
            try_parse_masking_policy("ALTER TABLE c.ns.t MODIFY COLUMN ssn SET MASKING POLICY")
                .expect_err("a name is required");
        assert!(
            err.to_string().contains("SHOW MASKING POLICIES"),
            "the error should point at how to find a name, got: {err}"
        );
    }

    /// Not ours: fall through to sqlparser rather than erroring.
    #[test]
    fn unrelated_alter_statements_are_not_claimed() {
        for sql in [
            "ALTER TABLE c.ns.t ADD COLUMN x VARCHAR",
            "ALTER TABLE c.ns.t MODIFY COLUMN ssn SET TAG pii = 'true'",
            "ALTER TABLE c.ns.t RENAME COLUMN a TO b",
            "SELECT 1",
        ] {
            assert_eq!(
                try_parse_masking_policy(sql).expect(sql),
                None,
                "{sql} must not be claimed as a masking-policy statement"
            );
        }
    }

    /// The tag parser must not claim the masking-policy forms either, or the
    /// classifier would route them to the tag handler with "MASKING" as a tag name.
    #[test]
    fn the_tag_parser_does_not_claim_the_masking_policy_forms() {
        for sql in [
            "ALTER TABLE c.ns.t MODIFY COLUMN ssn SET MASKING POLICY mask_ssn",
            "ALTER TABLE c.ns.t MODIFY COLUMN ssn UNSET MASKING POLICY",
        ] {
            assert!(
                matches!(try_parse_set_tags(sql), Ok(None)),
                "{sql}: the tag parser claimed a masking-policy statement: {:?}",
                try_parse_set_tags(sql)
            );
        }
    }
}
