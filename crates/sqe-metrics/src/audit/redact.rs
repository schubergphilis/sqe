//! PII redaction for audit payloads: SQL literal scrubbing and GDPR identifier
//! handling ([`GdprIdentifierMode`]).

/// Redact common PII patterns and secret literals from SQL text for audit
/// log safety.
///
/// Replaces:
/// - Email addresses -> [EMAIL]
/// - Phone numbers (US/intl) -> [PHONE]
/// - SSN patterns (XXX-XX-XXXX) -> [SSN]
/// - Credit card-like numbers (13-19 digits) -> [CARD]
/// - Quoted secret literals (`TOKEN '...'`, `PASSWORD '...'`,
///   `ACCESS_KEY_ID '...'`, `SECRET_ACCESS_KEY '...'`, `SESSION_TOKEN '...'`,
///   `SECRET '...'`) -> [REDACTED]
///
/// The secret-literal pass is the belt-and-suspenders guard for issue #4:
/// without it, `CREATE SECRET ... TOKEN '<jwt>'` lands verbatim in the audit
/// JSONL, OTel/Loki sinks, and any debug-level trace, exfiltrating every
/// long-lived bearer ever created in the cluster.
///
/// IMPORTANT (SQL-07): `redact_pii` is **best-effort pattern matching**, not a
/// guarantee. It catches email / SSN / phone / card / secret-keyword *shapes*;
/// it does NOT catch free-form sensitive literals such as
/// `WHERE patient_id = 'P-998877'` or `WHERE diagnosis = 'HIV positive'`. For
/// sinks at a different trust boundary (lineage), prefer [`strip_sql_literals`]
/// (which removes ALL literals) plus the SQL hash.
pub fn redact_pii(sql: &str) -> String {
    use std::sync::OnceLock;

    static EMAIL_RE: OnceLock<regex::Regex> = OnceLock::new();
    static SSN_RE: OnceLock<regex::Regex> = OnceLock::new();
    static PHONE_RE: OnceLock<regex::Regex> = OnceLock::new();
    static CARD_RE: OnceLock<regex::Regex> = OnceLock::new();
    static SECRET_RE: OnceLock<regex::Regex> = OnceLock::new();

    let email_re = EMAIL_RE.get_or_init(|| {
        regex::Regex::new(r"'[^']*[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}[^']*'").unwrap()
    });
    let ssn_re = SSN_RE.get_or_init(|| regex::Regex::new(r"'\d{3}-\d{2}-\d{4}'").unwrap());
    let phone_re = PHONE_RE.get_or_init(|| {
        regex::Regex::new(r"'(?:\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'").unwrap()
    });
    let card_re = CARD_RE
        .get_or_init(|| regex::Regex::new(r"'\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{1,7}'").unwrap());
    let secret_re = SECRET_RE.get_or_init(|| {
        regex::Regex::new(
            r"(?i)\b(TOKEN|PASSWORD|PASSWD|SECRET|ACCESS_KEY_ID|SECRET_ACCESS_KEY|SESSION_TOKEN|API_KEY|CLIENT_SECRET|BEARER)\b(\s*=\s*|\s+|\s*\(\s*)'[^']*'",
        )
        .unwrap()
    });

    let mut result = sql.to_string();
    result = email_re.replace_all(&result, "'[EMAIL]'").to_string();
    result = ssn_re.replace_all(&result, "'[SSN]'").to_string();
    result = phone_re.replace_all(&result, "'[PHONE]'").to_string();
    result = card_re.replace_all(&result, "'[CARD]'").to_string();
    result = secret_re
        .replace_all(&result, "$1$2'[REDACTED]'")
        .to_string();
    result
}

use sha2::{Digest, Sha256};

/// Modes for handling GDPR-tagged column identifiers in logged SQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GdprIdentifierMode {
    /// Replace the identifier with a stable per-(salt, column) token of the
    /// form `col_<first 8 hex digits of sha256(salt + lowercased name)>`.
    /// Queries referencing the same column share the same token across log
    /// lines, so they remain correlatable without leaking the column name.
    Tokenize,
    /// Replace the identifier with the literal string `[GDPR]`.
    Drop,
    /// Leave the identifier in place (name is not sensitive by itself).
    Keep,
}

/// Mask GDPR-tagged column identifiers (and adjacent literal values) in `sql`.
///
/// For each column name in `masked_columns`:
/// - The identifier is replaced case-insensitively (word boundaries) per `mode`.
/// - If any masked column matched, ALL string and numeric literals in the
///   resulting SQL are stripped via [`strip_sql_literals`] so adjacent VALUES
///   cannot survive regardless of quoting or position.
///
/// Empty `masked_columns` returns `sql` unchanged (byte-identical).
pub fn mask_gdpr_columns(
    sql: &str,
    masked_columns: &[String],
    mode: GdprIdentifierMode,
    salt: &str,
) -> String {
    if masked_columns.is_empty() {
        return sql.to_string();
    }
    let mut out = sql.to_string();
    let mut any = false;
    for col in masked_columns {
        let pattern = format!(r"(?i)\b{}\b", regex::escape(col));
        let re = match regex::Regex::new(&pattern) {
            Ok(r) => r,
            Err(_) => continue,
        };
        if !re.is_match(&out) {
            continue;
        }
        any = true;
        let replacement = match mode {
            GdprIdentifierMode::Keep => col.clone(),
            GdprIdentifierMode::Drop => "[GDPR]".to_string(),
            GdprIdentifierMode::Tokenize => {
                let mut h = Sha256::new();
                h.update(salt.as_bytes());
                h.update(col.to_lowercase().as_bytes());
                let hex = format!("{:x}", h.finalize());
                format!("col_{}", &hex[..8])
            }
        };
        out = re.replace_all(&out, replacement.as_str()).to_string();
    }
    if any {
        out = strip_sql_literals(&out);
    }
    out
}

/// SQL-07: replace every string and numeric literal in `sql` with a `?`
/// placeholder, leaving structure (keywords, identifiers, operators) intact.
///
/// Unlike [`redact_pii`] (pattern-only, best-effort), this removes ALL literal
/// values, so free-form sensitive data in predicates
/// (`WHERE patient_id = 'P-998877'`, `WHERE diagnosis = 'HIV positive'`,
/// `WHERE balance > 50000`) cannot reach a sink. Use it for sinks that sit at
/// a different trust boundary than the SQL client (lineage). The query shape is
/// preserved for debugging; correlate exact text via the SQL hash if needed.
///
/// Single-quoted strings (with `''` escapes) become `'?'`; standalone numeric
/// literals become `?`. A best-effort lexer, not a full SQL parser, but it is
/// total (never panics) and fail-closed (an unterminated quote consumes to EOL).
pub fn strip_sql_literals(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if c == b'\'' {
            // String literal: consume until the closing quote, handling the
            // doubled-quote ('') escape. Emit a single placeholder.
            out.push_str("'?'");
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    // Doubled quote -> escaped quote, stay in the string.
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    i += 1; // closing quote
                    break;
                }
                i += 1;
            }
        } else if c.is_ascii_digit() && (i == 0 || !is_ident_byte(bytes[i - 1])) {
            // Numeric literal not part of an identifier (e.g. not `col1`).
            // Consume digits, decimal point, and exponent.
            out.push('?');
            i += 1;
            while i < bytes.len()
                && (bytes[i].is_ascii_digit()
                    || bytes[i] == b'.'
                    || bytes[i] == b'e'
                    || bytes[i] == b'E'
                    || bytes[i] == b'+'
                    || bytes[i] == b'-')
            {
                // Stop a trailing +/- that is an operator, not an exponent sign.
                if (bytes[i] == b'+' || bytes[i] == b'-')
                    && !(i > 0 && (bytes[i - 1] == b'e' || bytes[i - 1] == b'E'))
                {
                    break;
                }
                i += 1;
            }
        } else {
            // Push this UTF-8 character whole (i is at a char boundary here
            // because string/number branches only advance over ASCII bytes).
            let ch_len = utf8_char_len(c);
            let end = (i + ch_len).min(bytes.len());
            out.push_str(&sql[i..end]);
            i = end;
        }
    }
    out
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn utf8_char_len(first: u8) -> usize {
    if first < 0x80 {
        1
    } else if first >> 5 == 0b110 {
        2
    } else if first >> 4 == 0b1110 {
        3
    } else if first >> 3 == 0b11110 {
        4
    } else {
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Task 7: GDPR column masking ---

    #[test]
    fn tokenize_hides_value_and_replaces_identifier_stably() {
        let sql = "SELECT id FROM users WHERE email = 'alice@x.io' AND email <> 'bob@x.io'";
        let out = mask_gdpr_columns(sql, &["email".into()], GdprIdentifierMode::Tokenize, "s1");
        assert!(!out.contains("alice@x.io"));
        assert!(!out.contains("bob@x.io"));
        assert!(!out.contains("email"));
        // Same column tokenizes to the same token within one salt (correlatable).
        let token_count = out.matches("col_").count();
        assert_eq!(token_count, 2);
    }

    #[test]
    fn drop_mode_removes_identifier_entirely() {
        let sql = "SELECT email FROM users";
        let out = mask_gdpr_columns(sql, &["email".into()], GdprIdentifierMode::Drop, "s1");
        assert!(!out.contains("email"));
    }

    #[test]
    fn keep_mode_keeps_identifier_but_strips_value() {
        let sql = "SELECT id FROM users WHERE email = 'alice@x.io'";
        let out = mask_gdpr_columns(sql, &["email".into()], GdprIdentifierMode::Keep, "s1");
        assert!(out.contains("email"));
        assert!(!out.contains("alice@x.io"));
    }

    #[test]
    fn non_gdpr_columns_untouched() {
        let sql = "SELECT id FROM users WHERE country = 'NL'";
        let out = mask_gdpr_columns(sql, &["email".into()], GdprIdentifierMode::Tokenize, "s1");
        assert_eq!(out, sql);
    }

    #[test]
    fn redact_email_in_where_clause() {
        let sql = "SELECT * FROM users WHERE email = 'alice@example.com'";
        let redacted = redact_pii(sql);
        assert!(!redacted.contains("alice@example.com"));
        assert!(redacted.contains("[EMAIL]"));
    }

    #[test]
    fn redact_ssn() {
        let sql = "SELECT * FROM records WHERE ssn = '123-45-6789'";
        let redacted = redact_pii(sql);
        assert!(!redacted.contains("123-45-6789"));
        assert!(redacted.contains("[SSN]"));
    }

    #[test]
    fn redact_phone() {
        let sql = "SELECT * FROM contacts WHERE phone = '(555) 123-4567'";
        let redacted = redact_pii(sql);
        assert!(redacted.contains("[PHONE]"));
    }

    #[test]
    fn no_redaction_for_normal_sql() {
        let sql = "SELECT id, name FROM products WHERE category = 'electronics'";
        let redacted = redact_pii(sql);
        assert_eq!(redacted, sql);
    }

    #[test]
    fn redact_multiple_patterns() {
        let sql = "INSERT INTO users (email, ssn) VALUES ('bob@test.com', '987-65-4321')";
        let redacted = redact_pii(sql);
        assert!(redacted.contains("[EMAIL]"));
        assert!(redacted.contains("[SSN]"));
        assert!(!redacted.contains("bob@test.com"));
    }

    // --- Secret-literal redaction (issue #4 regression tests) ---

    #[test]
    fn redact_create_secret_bearer_token() {
        let sql = "CREATE SECRET my_token (TYPE bearer, TOKEN 'eyJhbGciOiJSUzI1NiJ9.payload.sig')";
        let redacted = redact_pii(sql);
        assert!(
            !redacted.contains("eyJhbGciOiJSUzI1NiJ9.payload.sig"),
            "bearer token literal must not survive: {redacted}"
        );
        assert!(redacted.contains("[REDACTED]"));
        assert!(redacted.to_uppercase().contains("TOKEN"));
    }

    #[test]
    fn redact_create_secret_password() {
        let sql = "CREATE SECRET my_pw (TYPE password, PASSWORD 'hunter2!correct horse')";
        let redacted = redact_pii(sql);
        assert!(!redacted.contains("hunter2!correct horse"));
        assert!(redacted.contains("[REDACTED]"));
    }

    #[test]
    fn redact_create_secret_aws_keys() {
        let sql = "CREATE SECRET aws (\
            TYPE aws, \
            ACCESS_KEY_ID 'AKIAIOSFODNN7EXAMPLE', \
            SECRET_ACCESS_KEY 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY', \
            SESSION_TOKEN 'FQoDYXdzEPv...EXAMPLE')";
        let redacted = redact_pii(sql);
        assert!(!redacted.contains("AKIAIOSFODNN7EXAMPLE"), "{redacted}");
        assert!(
            !redacted.contains("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"),
            "{redacted}"
        );
        assert!(!redacted.contains("FQoDYXdzEPv...EXAMPLE"), "{redacted}");
        assert!(redacted.matches("[REDACTED]").count() >= 3);
    }

    #[test]
    fn redact_secret_kv_equals_style() {
        let sql = "CREATE SECRET s WITH (token = 'abc.def.ghi', password = 'hunter2')";
        let redacted = redact_pii(sql);
        assert!(!redacted.contains("abc.def.ghi"));
        assert!(!redacted.contains("hunter2"));
    }

    #[test]
    fn redact_secret_case_insensitive() {
        let sql = "CREATE SECRET x (token 'abc', Password 'def', api_key 'ghi')";
        let redacted = redact_pii(sql);
        assert!(!redacted.contains("'abc'"));
        assert!(!redacted.contains("'def'"));
        assert!(!redacted.contains("'ghi'"));
    }

    #[test]
    fn redact_does_not_touch_column_named_token() {
        let sql = "SELECT token FROM creds WHERE id = 1";
        let redacted = redact_pii(sql);
        assert_eq!(redacted, sql);
    }

    // --- SQL-07: literal stripping for lineage sinks ---

    #[test]
    fn strip_literals_removes_freeform_pii() {
        // The exact case redact_pii misses: a non-pattern sensitive literal.
        let sql = "SELECT * FROM patients WHERE patient_id = 'P-998877'";
        let stripped = strip_sql_literals(sql);
        assert!(
            !stripped.contains("P-998877"),
            "freeform literal leaked: {stripped}"
        );
        assert!(
            stripped.contains("'?'"),
            "string literal must become a placeholder"
        );
        // Structure (table + column) is preserved for debugging.
        assert!(stripped.contains("patients"));
        assert!(stripped.contains("patient_id"));
    }

    #[test]
    fn strip_literals_removes_numbers_but_keeps_identifiers() {
        let sql = "SELECT col1, col2 FROM t WHERE balance > 50000 AND year = 2026";
        let stripped = strip_sql_literals(sql);
        assert!(
            !stripped.contains("50000"),
            "numeric literal leaked: {stripped}"
        );
        assert!(
            !stripped.contains("2026"),
            "numeric literal leaked: {stripped}"
        );
        // `col1`/`col2` are identifiers with trailing digits, not literals.
        assert!(
            stripped.contains("col1"),
            "identifier must survive: {stripped}"
        );
        assert!(
            stripped.contains("col2"),
            "identifier must survive: {stripped}"
        );
    }

    #[test]
    fn strip_literals_handles_escaped_quotes() {
        let sql = "SELECT * FROM t WHERE name = 'O''Brien'";
        let stripped = strip_sql_literals(sql);
        assert!(
            !stripped.contains("Brien"),
            "escaped-quote literal leaked: {stripped}"
        );
        assert!(stripped.contains("'?'"));
    }

    #[test]
    fn strip_literals_total_on_unterminated_quote() {
        // Must not panic; consumes to end of input.
        let sql = "SELECT * FROM t WHERE x = 'oops";
        let stripped = strip_sql_literals(sql);
        assert!(!stripped.contains("oops"));
    }

    #[test]
    fn strip_literals_preserves_non_ascii_structure() {
        // Non-ASCII identifier/comment bytes must pass through without panic.
        let sql = "SELECT * FROM t -- café WHERE x = 'sécret'";
        let stripped = strip_sql_literals(sql);
        assert!(!stripped.contains("sécret"));
        assert!(stripped.contains("café"));
    }
}
