//! Placeholder scanning and binding for prepared statements.
//!
//! Both prepared-statement paths, Flight SQL and the Trino HTTP
//! `EXECUTE <name> USING ...`, rewrite `?` placeholders to numbered
//! `$1..$N` ones ([`number_placeholders`]), plan the statement, and bind
//! typed values into the logical plan ([`bind_params`]). A value never
//! becomes SQL text, and the text that is logged and audited holds the
//! placeholders (finding 406).
//!
//! The numbering is driven by one lexer, [`placeholder_offsets`], which finds every `?`
//! that is real SQL: outside single-quoted strings (including `E'...'` escape
//! strings, where a backslash escapes the next character), double-quoted
//! identifiers, dollar-quoted strings (`$$...$$`, `$tag$...$tag$`), `--`
//! line comments and `/* ... */` block comments. A `?` in any of those used to
//! consume a bind value and shift every later parameter.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::DFSchema;
use datafusion::logical_expr::expr::Placeholder;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan};
use datafusion::scalar::ScalarValue;

/// Byte offsets of every `?` in `sql` that is a bind placeholder.
///
/// See the module docs for what is skipped. Nested block comments are not
/// tracked (the first `*/` closes the comment), matching sqlparser's
/// tokenizer for the Generic dialect.
pub fn placeholder_offsets(sql: &str) -> Vec<usize> {
    let bytes = sql.as_bytes();
    let mut offsets = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i];
        match c {
            b'?' => {
                offsets.push(i);
                i += 1;
            }
            // Line comment: skip to end of line.
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                i = memchr_from(bytes, i, b'\n').unwrap_or(bytes.len());
            }
            // Block comment: skip to the closing `*/`.
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i = find_from(bytes, i + 2, b"*/")
                    .map(|p| p + 2)
                    .unwrap_or(bytes.len());
            }
            // Escape string `E'...'`: backslash escapes the next byte.
            b'E' | b'e'
                if bytes.get(i + 1) == Some(&b'\'')
                    && !is_ident_byte(bytes.get(i.wrapping_sub(1)).copied()) =>
            {
                i += 2;
                while i < bytes.len() {
                    match bytes[i] {
                        b'\\' => i += 2,
                        b'\'' if bytes.get(i + 1) == Some(&b'\'') => i += 2,
                        b'\'' => {
                            i += 1;
                            break;
                        }
                        _ => i += 1,
                    }
                }
            }
            // Plain single-quoted string; `''` is an escaped quote and simply
            // closes and reopens, which this loop handles by returning here.
            b'\'' => {
                i += 1;
                i = memchr_from(bytes, i, b'\'')
                    .map(|p| p + 1)
                    .unwrap_or(bytes.len());
            }
            b'"' => {
                i += 1;
                i = memchr_from(bytes, i, b'"')
                    .map(|p| p + 1)
                    .unwrap_or(bytes.len());
            }
            // Dollar-quoted string: `$$...$$` or `$tag$...$tag$`. A `$` followed
            // by a digit is a numbered placeholder, never a tag.
            b'$' => match dollar_tag_end(bytes, i) {
                Some(tag_end) => {
                    let tag = &bytes[i..tag_end];
                    i = find_from(bytes, tag_end, tag)
                        .map(|p| p + tag.len())
                        .unwrap_or(bytes.len());
                }
                None => i += 1,
            },
            _ => i += 1,
        }
    }
    offsets
}

fn is_ident_byte(b: Option<u8>) -> bool {
    matches!(b, Some(b) if b.is_ascii_alphanumeric() || b == b'_')
}

/// If a dollar-quote opening tag starts at `start`, return the byte offset just
/// past its closing `$`.
fn dollar_tag_end(bytes: &[u8], start: usize) -> Option<usize> {
    debug_assert_eq!(bytes[start], b'$');
    let mut j = start + 1;
    match bytes.get(j) {
        Some(b'$') => return Some(j + 1),
        Some(b) if b.is_ascii_alphabetic() || *b == b'_' => {}
        _ => return None,
    }
    while let Some(b) = bytes.get(j) {
        if *b == b'$' {
            return Some(j + 1);
        }
        if !(b.is_ascii_alphanumeric() || *b == b'_') {
            return None;
        }
        j += 1;
    }
    None
}

fn memchr_from(bytes: &[u8], from: usize, needle: u8) -> Option<usize> {
    bytes[from.min(bytes.len())..]
        .iter()
        .position(|b| *b == needle)
        .map(|p| p + from)
}

fn find_from(bytes: &[u8], from: usize, needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || from >= bytes.len() {
        return None;
    }
    bytes[from..]
        .windows(needle.len())
        .position(|w| w == needle)
        .map(|p| p + from)
}

/// Replace each `?` placeholder in `sql` with a numbered `$1`, `$2`, ...
/// placeholder, returning the rewritten SQL and the placeholder count.
///
/// Placeholders inside strings, quoted identifiers, dollar-quoted strings
/// and comments are ignored (see [`placeholder_offsets`]). Numbered
/// placeholders let DataFusion plan a prepared statement (inferring
/// placeholder types and the output schema) without bound values, which
/// `DESCRIBE OUTPUT` / `DESCRIBE INPUT`, the Flight SQL prepared-statement
/// store and [`bind_params`] need.
pub fn number_placeholders(sql: &str) -> (String, usize) {
    let offsets = placeholder_offsets(sql);
    let mut out = String::with_capacity(sql.len() + offsets.len() * 3);
    let mut last = 0usize;
    for (n, offset) in offsets.iter().enumerate() {
        out.push_str(&sql[last..*offset]);
        out.push('$');
        out.push_str(&(n + 1).to_string());
        last = offset + 1;
    }
    out.push_str(&sql[last..]);
    (out, offsets.len())
}

/// Substitute `params` for the `$1..$N` placeholders of `plan`, each value
/// first checked against the type DataFusion infers for its placeholder
/// ([`coerce_param`]). A placeholder whose type cannot be inferred takes the
/// value as it is.
///
/// Laxer than [`coerce_param`], because the caller could not know the
/// types (a Flight SQL prepared DML statement advertises every parameter as
/// `Utf8`, so a JDBC `setInt` arrives as `"5"`). A value of its
/// placeholder's type family stays as bound, for the planner to coerce (the
/// inferred type of `1 - $1` is Int64, and `1.5` cast to it would
/// truncate); a string bound to a numeric or boolean placeholder is parsed;
/// any other mismatch is refused.
///
/// `params` covers the whole statement; `plan` may be one part of it (a
/// MERGE's source, a DELETE's filter) and mention only some placeholders,
/// so values without a placeholder here are not an error. A plan with no
/// placeholders comes back unchanged. Errors name the position and the
/// types, never the value: the value is data, and errors reach the audit
/// log and query history.
pub fn bind_params(plan: LogicalPlan, params: &[ScalarValue]) -> Result<LogicalPlan, String> {
    let plan = type_case_placeholders(plan)
        .map_err(|e| format!("parameter type inference failed: {e}"))?;
    let types = plan
        .get_parameter_types()
        .map_err(|e| format!("parameter type inference failed: {e}"))?;
    if types.is_empty() {
        return Ok(plan);
    }
    let values = params
        .iter()
        .enumerate()
        .map(|(i, value)| match types.get(&format!("${}", i + 1)) {
            Some(Some(expected)) => coerce_untyped_param(value.clone(), expected, i + 1),
            _ => Ok(value.clone()),
        })
        .collect::<Result<Vec<_>, _>>()?;
    plan.with_param_values(values)
        .map_err(|e| format!("binding statement parameters failed: {e}"))
}

/// Give a bare placeholder in a CASE branch the type of the CASE's ELSE.
///
/// DataFusion infers placeholder types from comparisons, BETWEEN, IN and
/// LIKE, not from CASE branches. UPDATE and MERGE assign through
/// `CASE WHEN <cond> THEN <value> ELSE <column> END`, so `SET value = $1`
/// left `$1` untyped: a string bound to a BIGINT column then failed in the
/// execution-time cast, whose error quotes the value. CASE coerces every
/// branch to one type, and the ELSE column is the target, so its type is
/// the one the value must have.
fn type_case_placeholders(plan: LogicalPlan) -> datafusion::common::Result<LogicalPlan> {
    plan.transform_up_with_subqueries(|node| {
        let inputs = node.inputs();
        let [input] = inputs.as_slice() else {
            return Ok(Transformed::no(node));
        };
        let schema = Arc::clone(input.schema());
        node.map_expressions(|e| e.transform_up(|e| type_case_branches(e, &schema)))
    })
    .map(|t| t.data)
}

fn type_case_branches(e: Expr, schema: &DFSchema) -> datafusion::common::Result<Transformed<Expr>> {
    let Expr::Case(mut case) = e else {
        return Ok(Transformed::no(e));
    };
    let target = match case.else_expr.as_deref().map(|e| e.get_type(schema)) {
        Some(Ok(t)) if t != DataType::Null => t,
        _ => return Ok(Transformed::no(Expr::Case(case))),
    };
    let mut typed = false;
    for (_, then) in case.when_then_expr.iter_mut() {
        if let Expr::Placeholder(Placeholder {
            field: field @ None,
            ..
        }) = then.as_mut()
        {
            *field = Some(Arc::new(Field::new("", target.clone(), true)));
            typed = true;
        }
    }
    Ok(if typed {
        Transformed::yes(Expr::Case(case))
    } else {
        Transformed::no(Expr::Case(case))
    })
}

/// Check one bound value against the placeholder type DataFusion inferred.
///
/// Exact matches pass. Nulls become a typed null. Values in the same type
/// family (any integer or float for a numeric placeholder, any string
/// encoding for a string placeholder, ...) are cast with DataFusion's strict
/// cast, so an out-of-range integer is an error rather than a silent wrap.
/// A string bound to a date, time or timestamp placeholder is parsed; a
/// string bound to a numeric or boolean placeholder is rejected outright, as
/// is any other cross-family bind. The rejection happens here, before the
/// plan is bound or executed, and names the types only: the cast error would
/// quote the value.
pub fn coerce_param(
    value: ScalarValue,
    expected: &DataType,
    position: usize,
) -> Result<ScalarValue, String> {
    let actual = value.data_type();
    if &actual == expected {
        return Ok(value);
    }
    if value.is_null() {
        return ScalarValue::try_from(expected)
            .map_err(|_| format!("parameter ${position}: cannot build a null of {expected}"));
    }
    let compatible = match (family(&actual), family(expected)) {
        (Some(a), Some(e)) if a == e => true,
        (Some(Family::String), Some(Family::Date | Family::Time | Family::Timestamp)) => true,
        _ => false,
    };
    if !compatible {
        return Err(format!(
            "parameter ${position}: expected {expected}, got {actual}"
        ));
    }
    value
        .cast_to(expected)
        .map_err(|_| format!("parameter ${position}: cannot convert {actual} to {expected}"))
}

/// The check [`bind_params`] applies to a value whose caller did not know
/// the placeholder's type. A value of the placeholder's family stays as
/// bound: the type DataFusion infers comes from the other operand
/// (`1 - $1` infers Int64), and casting `1.5` to it would truncate, where
/// the planner's own coercion widens. A string bound to a numeric or boolean
/// placeholder is parsed. Anything else goes through [`coerce_param`].
fn coerce_untyped_param(
    value: ScalarValue,
    expected: &DataType,
    position: usize,
) -> Result<ScalarValue, String> {
    let actual = value.data_type();
    if value.is_null() {
        return coerce_param(value, expected, position);
    }
    if family(&actual).is_some() && family(&actual) == family(expected) {
        return Ok(value);
    }
    let parse = family(&actual) == Some(Family::String)
        && matches!(family(expected), Some(Family::Numeric | Family::Boolean));
    if !parse {
        return coerce_param(value, expected, position);
    }
    value
        .cast_to(expected)
        .map_err(|_| format!("parameter ${position}: cannot convert {actual} to {expected}"))
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Family {
    Boolean,
    Numeric,
    String,
    Binary,
    Date,
    Time,
    Timestamp,
    Duration,
    Interval,
}

fn family(dt: &DataType) -> Option<Family> {
    use DataType::*;
    Some(match dt {
        Boolean => Family::Boolean,
        Int8
        | Int16
        | Int32
        | Int64
        | UInt8
        | UInt16
        | UInt32
        | UInt64
        | Float16
        | Float32
        | Float64
        | Decimal128(_, _)
        | Decimal256(_, _) => Family::Numeric,
        Utf8 | LargeUtf8 | Utf8View => Family::String,
        Binary | LargeBinary | BinaryView | FixedSizeBinary(_) => Family::Binary,
        Date32 | Date64 => Family::Date,
        Time32(_) | Time64(_) => Family::Time,
        Timestamp(_, _) => Family::Timestamp,
        Duration(_) => Family::Duration,
        Interval(_) => Family::Interval,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn number_placeholders_basic() {
        let (sql, n) = number_placeholders("SELECT * FROM t WHERE a = ? AND b = ?");
        assert_eq!(sql, "SELECT * FROM t WHERE a = $1 AND b = $2");
        assert_eq!(n, 2);
    }

    #[test]
    fn number_placeholders_skips_inside_quotes() {
        let (sql, n) = number_placeholders("SELECT '? lit', c FROM t WHERE c = ?");
        assert_eq!(sql, "SELECT '? lit', c FROM t WHERE c = $1");
        assert_eq!(n, 1);
    }

    #[test]
    fn number_placeholders_none() {
        let (sql, n) = number_placeholders("SELECT 1");
        assert_eq!(sql, "SELECT 1");
        assert_eq!(n, 0);
    }

    #[test]
    fn number_placeholders_skips_comments() {
        let (sql, n) = number_placeholders("SELECT ? -- really?\n/* or? */ FROM t WHERE a = ?");
        assert_eq!(sql, "SELECT $1 -- really?\n/* or? */ FROM t WHERE a = $2");
        assert_eq!(n, 2);
    }

    /// Finding 406 acceptance: a `?` in a line or block comment is not a
    /// parameter.
    #[test]
    fn number_placeholders_ignores_line_and_block_comments() {
        assert_eq!(
            number_placeholders("SELECT 1 -- what?"),
            ("SELECT 1 -- what?".to_string(), 0)
        );
        assert_eq!(
            number_placeholders("SELECT ? /* why? */"),
            ("SELECT $1 /* why? */".to_string(), 1)
        );
    }

    #[test]
    fn number_placeholders_ignores_escape_string() {
        // The escaped quote inside E'...' must not end the string early.
        let (numbered, n) = number_placeholders(r"SELECT E'it\'s ?', ? FROM t");
        assert_eq!(numbered, r"SELECT E'it\'s ?', $1 FROM t");
        assert_eq!(n, 1);
        // `e` at the end of an identifier is not an escape-string prefix.
        let (numbered, n) = number_placeholders("SELECT one'x' , ?");
        assert_eq!(n, 1);
        assert!(numbered.ends_with("$1"));
    }

    #[test]
    fn number_placeholders_ignores_dollar_quoted() {
        let (numbered, n) = number_placeholders("SELECT $$who?$$, $tag$ what? $tag$, ? FROM t");
        assert_eq!(numbered, "SELECT $$who?$$, $tag$ what? $tag$, $1 FROM t");
        assert_eq!(n, 1);
        // A numbered placeholder is not a dollar-quote tag.
        assert_eq!(placeholder_offsets("SELECT $1, ?"), vec![11]);
    }

    #[test]
    fn number_placeholders_doubled_quote_inside_string() {
        let (numbered, n) = number_placeholders("SELECT 'it''s ?', ?");
        assert_eq!(numbered, "SELECT 'it''s ?', $1");
        assert_eq!(n, 1);
    }

    async fn plan(sql: &str) -> LogicalPlan {
        let ctx = datafusion::prelude::SessionContext::new();
        ctx.sql("CREATE TABLE t (id BIGINT, name VARCHAR, ok BOOLEAN)")
            .await
            .expect("create t");
        ctx.state().create_logical_plan(sql).await.expect("plan")
    }

    fn placeholders(plan: &LogicalPlan) -> usize {
        plan.get_parameter_types().expect("types").len()
    }

    #[tokio::test]
    async fn bind_params_leaves_a_plan_without_placeholders() {
        let p = plan("SELECT id FROM t").await;
        let bound = bind_params(p.clone(), &[ScalarValue::Int64(Some(1))]).unwrap();
        assert_eq!(bound, p);
    }

    /// A value of the placeholder's family is bound as is, a string bound
    /// to a numeric or boolean placeholder is parsed, and values beyond
    /// the plan's placeholders (another part of the statement) are no
    /// error.
    #[tokio::test]
    async fn bind_params_binds_and_parses_strings() {
        let p = plan("SELECT id FROM t WHERE id = $1 AND ok = $2").await;
        let bound = bind_params(
            p,
            &[
                ScalarValue::Utf8(Some("5".into())),
                ScalarValue::Utf8(Some("true".into())),
                ScalarValue::Int32(Some(9)),
            ],
        )
        .unwrap();
        assert_eq!(placeholders(&bound), 0);
        let text = bound.display_indent().to_string();
        assert!(text.contains("t.id = Int64(5)"), "{text}");
        assert!(text.contains("t.ok = Boolean(true)"), "{text}");

        let p = plan("SELECT id FROM t WHERE id = $1").await;
        let bound = bind_params(p, &[ScalarValue::Int32(Some(7))]).unwrap();
        assert!(bound.display_indent().to_string().contains("Int32(7)"));
    }

    /// A value that does not fit its placeholder is refused by position and
    /// types; the error never quotes the value.
    #[tokio::test]
    async fn bind_params_refuses_a_mismatch_without_echoing_the_value() {
        let p = plan("SELECT id FROM t WHERE id = $1").await;
        let err = bind_params(p, &[ScalarValue::Utf8(Some("secret-value".into()))]).unwrap_err();
        assert!(err.contains("parameter $1"), "{err}");
        assert!(!err.contains("secret"), "{err}");

        let p = plan("SELECT id FROM t WHERE name = $1").await;
        let err = bind_params(p, &[ScalarValue::Boolean(Some(true))]).unwrap_err();
        assert_eq!(err, "parameter $1: expected Utf8View, got Boolean");
    }

    /// The UPDATE / MERGE assignment shape: a placeholder in a CASE branch
    /// takes the ELSE column's type, so a string is parsed or refused before
    /// execution instead of failing in a cast that quotes it.
    #[tokio::test]
    async fn bind_params_types_a_case_branch_from_its_else() {
        let p = plan("SELECT CASE WHEN ok THEN $1 ELSE id END AS id FROM t").await;
        let bound = bind_params(p, &[ScalarValue::Utf8(Some("5".into()))]).unwrap();
        assert!(bound.display_indent().to_string().contains("Int64(5)"));

        let p = plan("SELECT CASE WHEN ok THEN $1 ELSE id END AS id FROM t").await;
        let err = bind_params(p, &[ScalarValue::Utf8(Some("secret-value".into()))]).unwrap_err();
        assert!(err.contains("parameter $1"), "{err}");
        assert!(!err.contains("secret"), "{err}");
    }

    #[tokio::test]
    async fn bind_params_types_a_null() {
        let p = plan("SELECT id FROM t WHERE id = $1").await;
        let bound = bind_params(p, &[ScalarValue::Null]).unwrap();
        assert!(bound.display_indent().to_string().contains("Int64(NULL)"));
    }

    #[test]
    fn unterminated_constructs_do_not_panic() {
        assert!(placeholder_offsets("SELECT '?").is_empty());
        assert!(placeholder_offsets("SELECT /* ?").is_empty());
        assert!(placeholder_offsets("SELECT $$ ?").is_empty());
        assert!(placeholder_offsets("SELECT E'?").is_empty());
        assert_eq!(placeholder_offsets("SELECT $"), Vec::<usize>::new());
    }
}
