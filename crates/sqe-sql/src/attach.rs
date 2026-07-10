//! AST types and post-parse hooks for the ATTACH/DETACH/SECRET SQL extensions.
//!
//! These statements let operators register Iceberg catalogs and credentials
//! at runtime, mirroring DuckDB's `ATTACH` and `CREATE SECRET` ergonomics.
//! See `docs/superpowers/specs/2026-05-09-attach-catalog-and-secrets-design.md`.
//!
//! sqlparser-rs has no native AST for the SQE option-list shape, so the
//! classifier dispatches to the small hand-rolled parser in this module
//! before falling through to sqlparser. The pattern mirrors how GRANT/REVOKE
//! and SHOW GRANTS are handled in `classifier.rs`.

use std::collections::BTreeMap;

use sqe_core::{Secret, SqeError};

/// `ATTACH '<location>' AS <name> (TYPE <kind>, <option> = <value>, ...)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachStatement {
    pub name: String,
    pub location: String,
    pub kind: CatalogKind,
    pub options: BTreeMap<String, OptionValue>,
}

/// `DETACH <name>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetachStatement {
    pub name: String,
}

/// `CREATE SECRET <name> (TYPE <kind>, <option> = <value>, ...)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateSecretStatement {
    pub name: String,
    pub kind: SecretKind,
    pub options: BTreeMap<String, OptionValue>,
}

/// `DROP SECRET <name>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropSecretStatement {
    pub name: String,
}

/// Kinds of Iceberg catalog backends that can be attached at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogKind {
    IcebergRest,
    Glue,
    S3Tables,
    Hms,
    Jdbc,
    Sqlite,
    Hadoop,
}

impl CatalogKind {
    /// Parse a case-insensitive backend keyword. Returns `None` for unknown values.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "iceberg_rest" | "rest" => Some(Self::IcebergRest),
            "glue" => Some(Self::Glue),
            "s3tables" => Some(Self::S3Tables),
            "hms" | "hive" => Some(Self::Hms),
            "jdbc" => Some(Self::Jdbc),
            "sqlite" => Some(Self::Sqlite),
            "hadoop" => Some(Self::Hadoop),
            _ => None,
        }
    }

    /// Stable lowercase label for metrics and audit logs.
    pub fn name(self) -> &'static str {
        match self {
            Self::IcebergRest => "iceberg_rest",
            Self::Glue => "glue",
            Self::S3Tables => "s3tables",
            Self::Hms => "hms",
            Self::Jdbc => "jdbc",
            Self::Sqlite => "sqlite",
            Self::Hadoop => "hadoop",
        }
    }
}

/// Kinds of credential bundles supported by `CREATE SECRET`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretKind {
    Aws,
    Bearer,
    Basic,
}

impl SecretKind {
    /// Parse a case-insensitive secret kind. Returns `None` for unknown values.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "aws" => Some(Self::Aws),
            "bearer" => Some(Self::Bearer),
            "basic" => Some(Self::Basic),
            _ => None,
        }
    }

    /// Stable lowercase label for metrics and audit logs.
    pub fn name(self) -> &'static str {
        match self {
            Self::Aws => "aws",
            Self::Bearer => "bearer",
            Self::Basic => "basic",
        }
    }
}

/// Right-hand value of an `ATTACH`/`CREATE SECRET` option.
///
/// String literals carry concrete values. Bare identifiers reference a named
/// secret in the in-memory secret store and are resolved at attach time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OptionValue {
    /// Single-quoted string literal: `WAREHOUSE 'my_wh'`.
    String(String),
    /// Unquoted identifier referencing a secret name: `SECRET aws_prod`.
    SecretRef(String),
}

impl OptionValue {
    /// Returns the inner string value if this is `Self::String`, else `None`.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(s) => Some(s),
            _ => None,
        }
    }

    /// Returns the secret name if this is `Self::SecretRef`, else `None`.
    pub fn as_secret_ref(&self) -> Option<&str> {
        match self {
            Self::SecretRef(s) => Some(s),
            _ => None,
        }
    }
}

/// Build a `Secret` payload from a parsed `CREATE SECRET` statement.
///
/// Both the coordinator and the embedded CLI need the same validation rules.
/// Previously each duplicated the same kind-dispatch and option-extraction;
/// the embedded path missed the admin gate added later for the coordinator.
/// Keeping the body here means a new SecretKind variant gets a single fix.
pub fn build_secret_from_stmt(stmt: &CreateSecretStatement) -> Result<Secret, SqeError> {
    let opts = &stmt.options;
    let get_str = |key: &str| -> Result<String, SqeError> {
        opts.get(key)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| {
                SqeError::Execution(format!(
                    "CREATE SECRET: missing required option {key} for {:?} secret",
                    stmt.kind.name()
                ))
            })
    };
    let get_opt =
        |key: &str| -> Option<String> { opts.get(key).and_then(|v| v.as_str()).map(|s| s.to_string()) };

    let secret = match stmt.kind {
        SecretKind::Aws => Secret::Aws {
            access_key: get_opt("ACCESS_KEY_ID"),
            secret_key: get_opt("SECRET_ACCESS_KEY"),
            session_token: get_opt("SESSION_TOKEN"),
            region: get_opt("REGION"),
            profile: get_opt("PROFILE"),
        },
        SecretKind::Bearer => Secret::Bearer { token: get_str("TOKEN")? },
        SecretKind::Basic => Secret::Basic {
            username: get_str("USERNAME")?,
            password: get_str("PASSWORD")?,
        },
    };
    Ok(secret)
}

// ---------------------------------------------------------------------------
// Hand-rolled parser for the SQE-specific shapes.
//
// sqlparser-rs (0.54) only knows the SQLite `ATTACH '<file>' AS <name>` shape
// and has no concept of `(TYPE <kind>, ...)` trailing options. It also has no
// AST at all for our `CREATE SECRET` / `DROP SECRET` / `SHOW SECRETS` shapes.
// We therefore detect these shapes before reaching sqlparser, parse them with
// a small purpose-built lexer, and emit a SQE-specific `StatementKind`.
// ---------------------------------------------------------------------------

/// Try to parse `ATTACH '<location>' AS <name> (TYPE <kind>, ...)` from the
/// untrimmed SQL string. Returns:
/// - `Ok(Some(stmt))` on a successful match.
/// - `Ok(None)` if the SQL does not look like a SQE-style ATTACH (caller
///   should fall through to sqlparser, which will handle the legacy SQLite
///   shape, or surface a parse error).
/// - `Err(_)` if it does match the SQE shape but is malformed (e.g. unknown
///   TYPE, missing TYPE).
pub fn try_parse_attach(sql: &str) -> Result<Option<AttachStatement>, SqeError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("ATTACH ") {
        return Ok(None);
    }

    // Quick check: SQE shape requires a trailing `(...)`. If absent, this is
    // likely the SQLite `ATTACH 'foo.db' AS foo` form. Fall through.
    if !trimmed.ends_with(')') {
        return Ok(None);
    }

    // Strip the leading `ATTACH ` keyword.
    let body = trimmed["ATTACH ".len()..].trim();

    // Extract the location: a single-quoted string at the front.
    let (location, after_location) = take_quoted_string(body)
        .ok_or_else(|| err("ATTACH: expected '<location>' as a single-quoted string"))?;
    let after_location = after_location.trim();

    // Match `AS <name>`.
    let after_as = after_location
        .strip_prefix_ci("AS")
        .ok_or_else(|| err("ATTACH: expected AS <name> after the location string"))?;
    let after_as = after_as.trim();

    // Extract the name: bare identifier up to the first whitespace or `(`.
    let (name, after_name) = take_identifier(after_as)
        .ok_or_else(|| err("ATTACH: expected catalog name after AS"))?;
    let after_name = after_name.trim();

    // The remainder must be a `(...)` option list. If not, this is not the
    // SQE shape (could be legacy SQLite form with extra whitespace before EOF).
    if !after_name.starts_with('(') || !after_name.ends_with(')') {
        return Ok(None);
    }
    let inner = &after_name[1..after_name.len() - 1];
    let mut options = parse_option_list(inner)?;

    // The TYPE option is mandatory and consumed off the map. TYPE values may
    // arrive as either a bare identifier (`TYPE iceberg_rest`) or a quoted
    // string (`TYPE 'iceberg_rest'`); both work because `as_text` is
    // form-agnostic.
    let type_value = options
        .remove("TYPE")
        .ok_or_else(|| err("ATTACH: missing required option TYPE"))?;
    let type_str = type_value.as_text();
    let kind = CatalogKind::parse(type_str)
        .ok_or_else(|| err(&format!("ATTACH: unknown catalog TYPE '{type_str}'")))?;

    Ok(Some(AttachStatement {
        name: name.to_string(),
        location: location.to_string(),
        kind,
        options,
    }))
}

/// Try to parse `DETACH <name>` from the untrimmed SQL string.
/// Returns `Ok(None)` if the SQL does not start with `DETACH `.
pub fn try_parse_detach(sql: &str) -> Result<Option<DetachStatement>, SqeError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("DETACH ") && upper != "DETACH" {
        return Ok(None);
    }
    let body = trimmed[6..].trim_start(); // 6 = len("DETACH")
    if body.is_empty() {
        return Err(err("DETACH: expected catalog name"));
    }
    let (name, rest) = take_identifier(body)
        .ok_or_else(|| err("DETACH: expected catalog name as a bare identifier"))?;
    if !rest.trim().is_empty() {
        return Err(err(&format!(
            "DETACH: unexpected trailing tokens after name '{name}'"
        )));
    }
    Ok(Some(DetachStatement { name: name.to_string() }))
}

/// Try to parse `CREATE SECRET <name> (TYPE <kind>, <option> = <value>, ...)`.
pub fn try_parse_create_secret(sql: &str) -> Result<Option<CreateSecretStatement>, SqeError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("CREATE SECRET ") {
        return Ok(None);
    }

    let body = trimmed["CREATE SECRET ".len()..].trim();
    let (name, after_name) = take_identifier(body)
        .ok_or_else(|| err("CREATE SECRET: expected secret name"))?;
    let after_name = after_name.trim();
    if !after_name.starts_with('(') || !after_name.ends_with(')') {
        return Err(err("CREATE SECRET: expected (TYPE <kind>, ...) option list"));
    }
    let inner = &after_name[1..after_name.len() - 1];
    let mut options = parse_option_list(inner)?;

    let type_value = options
        .remove("TYPE")
        .ok_or_else(|| err("CREATE SECRET: missing required option TYPE"))?;
    let type_str = type_value.as_text();
    let kind = SecretKind::parse(type_str)
        .ok_or_else(|| err(&format!("CREATE SECRET: unknown TYPE '{type_str}'")))?;

    Ok(Some(CreateSecretStatement {
        name: name.to_string(),
        kind,
        options,
    }))
}

/// Try to parse `DROP SECRET <name>`.
pub fn try_parse_drop_secret(sql: &str) -> Result<Option<DropSecretStatement>, SqeError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("DROP SECRET ") {
        return Ok(None);
    }
    let body = trimmed["DROP SECRET ".len()..].trim();
    let (name, rest) = take_identifier(body)
        .ok_or_else(|| err("DROP SECRET: expected secret name"))?;
    if !rest.trim().is_empty() {
        return Err(err(&format!(
            "DROP SECRET: unexpected trailing tokens after name '{name}'"
        )));
    }
    Ok(Some(DropSecretStatement { name: name.to_string() }))
}

/// Returns `true` if the SQL is `SHOW SECRETS` (case-insensitive, with or
/// without trailing semicolon and surrounding whitespace).
pub fn is_show_secrets(sql: &str) -> bool {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    trimmed.eq_ignore_ascii_case("SHOW SECRETS")
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn err(msg: &str) -> SqeError {
    SqeError::Execution(msg.to_string())
}

/// Trait-like helper: case-insensitive prefix strip that requires a trailing
/// non-identifier character (whitespace) to avoid swallowing `ASBC`-style
/// identifiers when looking for the keyword `AS`.
trait StripPrefixCi {
    fn strip_prefix_ci(&self, prefix: &str) -> Option<&str>;
}

impl StripPrefixCi for str {
    fn strip_prefix_ci(&self, prefix: &str) -> Option<&str> {
        let plen = prefix.len();
        if self.len() < plen {
            return None;
        }
        if !self[..plen].eq_ignore_ascii_case(prefix) {
            return None;
        }
        // Reject if the character right after the prefix could continue an
        // identifier (e.g. `AS` must not match the start of `ASCII`).
        let rest = &self[plen..];
        match rest.chars().next() {
            None => Some(rest),
            Some(c) if !is_ident_continue(c) => Some(rest),
            _ => None,
        }
    }
}

fn is_ident_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_'
}

fn is_ident_continue(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// Consume a single-quoted string literal at the start of `s`. Doubled
/// single quotes inside the literal are accepted as the SQL-standard escape
/// for a single quote. Returns the unescaped value plus the remainder.
fn take_quoted_string(s: &str) -> Option<(String, &str)> {
    let s = s.trim_start();
    let mut chars = s.char_indices();
    if chars.next().map(|(_, c)| c) != Some('\'') {
        return None;
    }
    let mut value = String::new();
    while let Some((idx, c)) = chars.next() {
        if c == '\'' {
            // Check for an escaped quote: '' inside a literal.
            let after = &s[idx + 1..];
            if let Some(next) = after.chars().next() {
                if next == '\'' {
                    value.push('\'');
                    chars.next();
                    continue;
                }
            }
            return Some((value, &s[idx + 1..]));
        }
        value.push(c);
    }
    None
}

/// Consume a bare identifier at the start of `s`. Returns the identifier and
/// the remainder. Identifiers are ASCII letters/digits/underscores starting
/// with a letter or underscore.
fn take_identifier(s: &str) -> Option<(&str, &str)> {
    let s = s.trim_start();
    let mut iter = s.char_indices();
    match iter.next() {
        Some((_, c)) if is_ident_start(c) => {}
        _ => return None,
    }
    let mut end = s.len();
    for (idx, c) in iter {
        if !is_ident_continue(c) {
            end = idx;
            break;
        }
    }
    Some((&s[..end], &s[end..]))
}

/// Parse a comma-separated option list. Each item is one of:
///   - `KEY = 'value'` -> `OptionValue::String`
///   - `KEY = ident`   -> `OptionValue::SecretRef` (only valid when KEY is `SECRET`)
///   - `KEY 'value'`   -> `OptionValue::String` (the `=` is optional)
///   - `KEY ident`     -> `OptionValue::SecretRef` (same constraint as above)
///
/// The optional `=` lets us accept both DuckDB's `(KEY value, ...)` shape
/// and the more SQL-flavoured `(KEY = value, ...)`.
/// Keys are upper-cased on insert so look-ups are case-insensitive.
/// Duplicate keys produce an error.
fn parse_option_list(inner: &str) -> Result<BTreeMap<String, OptionValue>, SqeError> {
    let mut out: BTreeMap<String, OptionValue> = BTreeMap::new();
    for raw in split_top_level_commas(inner) {
        let item = raw.trim();
        if item.is_empty() {
            continue;
        }
        let (key, rest) = take_identifier(item)
            .ok_or_else(|| err(&format!("option list: expected KEY in '{item}'")))?;
        let key_upper = key.to_ascii_uppercase();
        let mut rest = rest.trim_start();
        // Allow `=` between key and value, but don't require it.
        if let Some(stripped) = rest.strip_prefix('=') {
            rest = stripped.trim_start();
        }
        if rest.is_empty() {
            return Err(err(&format!("option list: missing value for {key_upper}")));
        }
        let value = parse_option_value(rest, &key_upper)?;
        if out.insert(key_upper.clone(), value).is_some() {
            return Err(err(&format!("option list: duplicate key {key_upper}")));
        }
    }
    Ok(out)
}

/// Decide whether the value text starts with a quoted string or a bare
/// identifier and return the corresponding `OptionValue`. Bare identifiers
/// are only allowed when the key is `SECRET`; for any other key, an
/// unquoted value is rejected so we don't silently treat user-typed text as
/// a secret reference.
fn parse_option_value(text: &str, key: &str) -> Result<OptionValue, SqeError> {
    let text = text.trim();
    if text.starts_with('\'') {
        let (s, rest) = take_quoted_string(text)
            .ok_or_else(|| err(&format!("option list: unterminated string for {key}")))?;
        if !rest.trim().is_empty() {
            return Err(err(&format!(
                "option list: unexpected trailing tokens after value for {key}"
            )));
        }
        Ok(OptionValue::String(s))
    } else if let Some((ident, rest)) = take_identifier(text) {
        if !rest.trim().is_empty() {
            return Err(err(&format!(
                "option list: unexpected trailing tokens after value for {key}"
            )));
        }
        // Only the `SECRET` key may take a bare identifier; everything else
        // demands a quoted string so we fail loudly when the user forgets the
        // quotes around `WAREHOUSE 'my_wh'`. The TYPE keyword is the one
        // documented exception: TYPE iceberg_rest stays a bare identifier.
        if key == "SECRET" || key == "TYPE" {
            Ok(OptionValue::SecretRef(ident.to_string()))
        } else {
            Err(err(&format!(
                "option list: value for {key} must be a single-quoted string (got bare identifier '{ident}')"
            )))
        }
    } else {
        Err(err(&format!("option list: invalid value for {key}")))
    }
}

/// Split `s` on top-level commas. Respects single-quoted string literals so
/// `'a,b'` stays one chunk. There are no nested parentheses in SQE option
/// lists, so we don't need to track them.
fn split_top_level_commas(s: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut in_string = false;
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if c == b'\'' {
            // Toggle on `'`, but stay inside on the SQL-doubled escape `''`.
            if in_string && i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                i += 2;
                continue;
            }
            in_string = !in_string;
        } else if c == b',' && !in_string {
            out.push(&s[start..i]);
            start = i + 1;
        }
        i += 1;
    }
    out.push(&s[start..]);
    out
}

/// Special-case for TYPE: TYPE values are bare identifiers, so the
/// `parse_option_value` path returns `OptionValue::SecretRef` for them.
/// Both `try_parse_attach` and `try_parse_create_secret` call `as_str` on the
/// returned value, which would yield `None` for a `SecretRef`. To keep the
/// AST shape consistent (TYPE always parses as `OptionValue::String`), the
/// `as_str` path on `SecretRef` extracts the identifier.
//
// Implementation note: rather than complicate `OptionValue`, we treat the
// TYPE key specially in `parse_option_value` (already wired above) and also
// route SecretRef through `as_str` when callers need the raw text. The
// helpers below give the parsers a uniform "give me the text" view.
impl OptionValue {
    /// Returns the raw text of either a string literal or a bare identifier.
    /// Used by parsers that don't care which form was used (e.g. for TYPE).
    pub(crate) fn as_text(&self) -> &str {
        match self {
            Self::String(s) => s,
            Self::SecretRef(s) => s,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_kind_parses_all_variants() {
        assert_eq!(CatalogKind::parse("iceberg_rest"), Some(CatalogKind::IcebergRest));
        assert_eq!(CatalogKind::parse("rest"), Some(CatalogKind::IcebergRest));
        assert_eq!(CatalogKind::parse("glue"), Some(CatalogKind::Glue));
        assert_eq!(CatalogKind::parse("s3tables"), Some(CatalogKind::S3Tables));
        assert_eq!(CatalogKind::parse("hms"), Some(CatalogKind::Hms));
        assert_eq!(CatalogKind::parse("hive"), Some(CatalogKind::Hms));
        assert_eq!(CatalogKind::parse("jdbc"), Some(CatalogKind::Jdbc));
        assert_eq!(CatalogKind::parse("sqlite"), Some(CatalogKind::Sqlite));
        assert_eq!(CatalogKind::parse("hadoop"), Some(CatalogKind::Hadoop));
    }

    #[test]
    fn catalog_kind_parse_is_case_insensitive() {
        assert_eq!(CatalogKind::parse("ICEBERG_REST"), Some(CatalogKind::IcebergRest));
        assert_eq!(CatalogKind::parse("Glue"), Some(CatalogKind::Glue));
        assert_eq!(CatalogKind::parse("HaDoOp"), Some(CatalogKind::Hadoop));
    }

    #[test]
    fn catalog_kind_parse_rejects_unknown() {
        assert_eq!(CatalogKind::parse("postgres"), None);
        assert_eq!(CatalogKind::parse(""), None);
        assert_eq!(CatalogKind::parse("not_a_backend"), None);
    }

    #[test]
    fn catalog_kind_name_round_trip() {
        for k in [
            CatalogKind::IcebergRest,
            CatalogKind::Glue,
            CatalogKind::S3Tables,
            CatalogKind::Hms,
            CatalogKind::Jdbc,
            CatalogKind::Sqlite,
            CatalogKind::Hadoop,
        ] {
            // Every canonical name must round-trip through parse().
            assert_eq!(CatalogKind::parse(k.name()), Some(k), "round-trip failed for {k:?}");
        }
    }

    #[test]
    fn secret_kind_parses_all_variants() {
        assert_eq!(SecretKind::parse("aws"), Some(SecretKind::Aws));
        assert_eq!(SecretKind::parse("bearer"), Some(SecretKind::Bearer));
        assert_eq!(SecretKind::parse("basic"), Some(SecretKind::Basic));
    }

    #[test]
    fn secret_kind_parse_is_case_insensitive() {
        assert_eq!(SecretKind::parse("AWS"), Some(SecretKind::Aws));
        assert_eq!(SecretKind::parse("Bearer"), Some(SecretKind::Bearer));
        assert_eq!(SecretKind::parse("BASIC"), Some(SecretKind::Basic));
    }

    #[test]
    fn secret_kind_parse_rejects_unknown() {
        assert_eq!(SecretKind::parse("oauth"), None);
        assert_eq!(SecretKind::parse(""), None);
    }

    #[test]
    fn secret_kind_name_round_trip() {
        for k in [SecretKind::Aws, SecretKind::Bearer, SecretKind::Basic] {
            assert_eq!(SecretKind::parse(k.name()), Some(k));
        }
    }

    #[test]
    fn option_value_as_str_discriminates() {
        let v = OptionValue::String("hello".to_string());
        assert_eq!(v.as_str(), Some("hello"));
        assert_eq!(v.as_secret_ref(), None);
    }

    #[test]
    fn option_value_as_secret_ref_discriminates() {
        let v = OptionValue::SecretRef("aws_prod".to_string());
        assert_eq!(v.as_secret_ref(), Some("aws_prod"));
        assert_eq!(v.as_str(), None);
    }

    #[test]
    fn ast_structs_are_constructable() {
        // Smoke test to confirm the public API shape compiles.
        let attach = AttachStatement {
            name: "polaris".to_string(),
            location: "https://polaris.example.com/api/catalog".to_string(),
            kind: CatalogKind::IcebergRest,
            options: BTreeMap::new(),
        };
        assert_eq!(attach.kind, CatalogKind::IcebergRest);

        let detach = DetachStatement { name: "polaris".to_string() };
        assert_eq!(detach.name, "polaris");

        let create = CreateSecretStatement {
            name: "aws_prod".to_string(),
            kind: SecretKind::Aws,
            options: BTreeMap::new(),
        };
        assert_eq!(create.kind, SecretKind::Aws);

        let drop_s = DropSecretStatement { name: "aws_prod".to_string() };
        assert_eq!(drop_s.name, "aws_prod");
    }
}
