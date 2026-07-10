//! Parameter substitution for prepared statements.
//!
//! Both the Flight SQL path (`DoGetPreparedStatement`) and the Trino HTTP path
//! (`EXECUTE <name> USING ...`) bind parameters by replacing `?` placeholders
//! with SQL literals. The substitution is text-level and quote-aware so a `?`
//! inside a string or quoted identifier is left untouched.

/// Replace `?` placeholders in `sql` with the bound literals, in order.
///
/// Placeholders inside single-quoted strings or double-quoted identifiers are
/// ignored. Returns an error if the placeholder count differs from the number
/// of bound values, mirroring a JDBC `SQLException` rather than executing with
/// a partial bind. Each value in `params` is inserted verbatim, so callers must
/// pass already-quoted SQL literals (e.g. `'foo'`, `42`, `DATE '2020-01-01'`).
pub fn substitute_placeholders(sql: &str, params: &[String]) -> Result<String, String> {
    let mut out =
        String::with_capacity(sql.len() + params.iter().map(|s| s.len()).sum::<usize>());
    let mut next: usize = 0;
    let mut in_single = false;
    let mut in_double = false;
    let bytes = sql.as_bytes();
    let mut i: usize = 0;
    while i < bytes.len() {
        let c = bytes[i] as char;
        if !in_single && !in_double && c == '?' {
            if next >= params.len() {
                return Err(format!(
                    "prepared statement expected {} parameters but bind supplied {}",
                    next + 1,
                    params.len()
                ));
            }
            out.push_str(&params[next]);
            next += 1;
            i += 1;
            continue;
        }
        if !in_double && c == '\'' {
            in_single = !in_single;
        } else if !in_single && c == '"' {
            in_double = !in_double;
        }
        out.push(c);
        i += 1;
    }
    if next != params.len() {
        return Err(format!(
            "prepared statement consumed {next} parameters but bind supplied {}",
            params.len()
        ));
    }
    Ok(out)
}

/// Replace each `?` placeholder in `sql` with a numbered `$1`, `$2`, ...
/// placeholder, returning the rewritten SQL and the placeholder count.
///
/// Quote-aware like [`substitute_placeholders`]: a `?` inside a single-quoted
/// string or double-quoted identifier is left untouched. Numbered placeholders
/// let DataFusion plan a prepared statement (inferring placeholder types and
/// the output schema) without bound values, which `DESCRIBE OUTPUT` /
/// `DESCRIBE INPUT` need.
pub fn number_placeholders(sql: &str) -> (String, usize) {
    let mut out = String::with_capacity(sql.len() + 8);
    let mut count: usize = 0;
    let mut in_single = false;
    let mut in_double = false;
    for c in sql.chars() {
        if !in_single && !in_double && c == '?' {
            count += 1;
            out.push('$');
            out.push_str(&count.to_string());
            continue;
        }
        if !in_double && c == '\'' {
            in_single = !in_single;
        } else if !in_single && c == '"' {
            in_double = !in_double;
        }
        out.push(c);
    }
    (out, count)
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
    fn substitute_placeholders_basic() {
        let sql = "SELECT * FROM t WHERE a = ? AND b = ?";
        let out = substitute_placeholders(sql, &["1".into(), "'foo'".into()]).unwrap();
        assert_eq!(out, "SELECT * FROM t WHERE a = 1 AND b = 'foo'");
    }

    #[test]
    fn substitute_placeholders_skips_inside_quotes() {
        let sql = "SELECT '? literal', c FROM t WHERE c = ?";
        let out = substitute_placeholders(sql, &["42".into()]).unwrap();
        assert_eq!(out, "SELECT '? literal', c FROM t WHERE c = 42");
    }

    #[test]
    fn substitute_placeholders_mismatch_errors() {
        assert!(substitute_placeholders("?", &[]).is_err());
        assert!(substitute_placeholders("?", &["a".into(), "b".into()]).is_err());
    }
}
