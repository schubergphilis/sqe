//! Benchmark environment profiles: one TOML file per environment
//! (`local`, `storagegrid`, `r2`, `aws`, or a custom path) describing the
//! S3 store, the Polaris catalog, and the concurrency knobs. Secrets are
//! never stored here; they resolve at runtime from env vars or a named AWS
//! credentials profile (see `resolve_s3_credentials`).

#![allow(dead_code)]

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Profile {
    pub name: String,
    #[serde(default)]
    pub manage_stack: bool,
    pub s3: S3Profile,
    pub polaris: PolarisProfile,
    #[serde(default)]
    pub concurrency: ConcurrencyProfile,
}

#[derive(Debug, Clone, Deserialize)]
pub struct S3Profile {
    pub endpoint: String,
    #[serde(default = "default_region")]
    pub region: String,
    #[serde(default)]
    pub path_style: bool,
    /// Named AWS credentials profile to resolve access/secret keys from at
    /// runtime. `None` => resolve from AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY.
    #[serde(default)]
    pub aws_profile: Option<String>,
    pub warehouse_bucket: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PolarisProfile {
    pub url: String,
    pub warehouse: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConcurrencyProfile {
    #[serde(default = "default_manifest")]
    pub manifest: u32,
    #[serde(default = "default_direct_read")]
    pub direct_read: u32,
    #[serde(default = "default_write_streams")]
    pub write_streams: u32,
}

impl Default for ConcurrencyProfile {
    fn default() -> Self {
        Self {
            manifest: default_manifest(),
            direct_read: default_direct_read(),
            write_streams: default_write_streams(),
        }
    }
}

fn default_region() -> String {
    "us-east-1".to_string()
}
fn default_manifest() -> u32 {
    64
}
fn default_direct_read() -> u32 {
    8
}
fn default_write_streams() -> u32 {
    8
}

/// Key names (case-insensitive, exact match) that are never allowed to
/// appear anywhere in a committed profile document. These identify a value
/// as a credential rather than configuration; matching is by key name, not
/// by scanning raw text, so unrelated fields like `client_secret_path` or a
/// `# rotate secret_key` comment don't false-trigger.
const FORBIDDEN_SECRET_KEYS: &[&str] = &[
    "secret_key",
    "secret_access_key",
    "aws_secret_access_key",
    "client_secret",
    "password",
    "token",
    "access_token",
    "session_token",
    "bearer_token",
];

/// Recursively walk a parsed TOML value, checking every table key against
/// the forbidden secret-key-name set (case-insensitive, exact match).
fn check_no_secret_keys(value: &toml::Value) -> anyhow::Result<()> {
    if let toml::Value::Table(table) = value {
        for (key, val) in table {
            let lower = key.to_lowercase();
            if FORBIDDEN_SECRET_KEYS.contains(&lower.as_str()) {
                anyhow::bail!(
                    "profile contains a forbidden inline secret key ('{key}'); \
                     resolve credentials from env or an AWS profile instead"
                );
            }
            check_no_secret_keys(val)?;
        }
    }
    Ok(())
}

/// Parse a profile from a TOML string. Rejects any inline S3/OAuth secret so
/// a committed profile can never carry a credential (constraint: no committed
/// secrets). Callers use `load_profile` for the name/path resolution wrapper.
pub fn parse_profile_str(s: &str) -> anyhow::Result<Profile> {
    // Guard: forbid secret-bearing keys anywhere in the document. These belong
    // in env vars or a named AWS profile, never in a repo-tracked file. The
    // check is key-name-aware (case-insensitive, exact match) rather than a
    // raw substring scan, so it catches conventional names like
    // `aws_secret_access_key` and `SECRET_KEY` while allowing benign fields
    // such as `client_secret_path`.
    let value: toml::Value = toml::from_str(s)?;
    check_no_secret_keys(&value)?;
    let profile: Profile = value.try_into()?;
    Ok(profile)
}

/// Load a profile by preset name (`local`, `storagegrid`, `r2`, `aws`) from
/// `benchmarks/profiles/<name>.toml`, or from an explicit file path if the
/// argument contains a path separator or ends in `.toml`.
pub fn load_profile(name_or_path: &str) -> anyhow::Result<Profile> {
    let path = if name_or_path.contains('/') || name_or_path.ends_with(".toml") {
        std::path::PathBuf::from(name_or_path)
    } else {
        std::path::PathBuf::from(format!("benchmarks/profiles/{name_or_path}.toml"))
    };
    let text = std::fs::read_to_string(&path)
        .map_err(|e| anyhow::anyhow!("reading profile {}: {e}", path.display()))?;
    parse_profile_str(&text)
}

/// Resolved S3 credentials. Held only in memory; never serialized or logged.
pub struct S3Credentials {
    pub access_key: String,
    pub secret_key: String,
}

/// Resolve S3 credentials at runtime. If the profile names an AWS credentials
/// profile, read it via `aws configure get` semantics through env; otherwise
/// fall back to the standard AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY vars.
/// Never reads secrets from the profile TOML (Task 1 forbids them).
pub fn resolve_s3_credentials(s3: &S3Profile) -> anyhow::Result<S3Credentials> {
    if let Some(prof) = &s3.aws_profile {
        let access_key = aws_config_get(prof, "aws_access_key_id")?;
        let secret_key = aws_config_get(prof, "aws_secret_access_key")?;
        return Ok(S3Credentials { access_key, secret_key });
    }
    let access_key = std::env::var("AWS_ACCESS_KEY_ID")
        .map_err(|_| anyhow::anyhow!("AWS_ACCESS_KEY_ID not set (and no aws_profile in profile)"))?;
    let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY")
        .map_err(|_| anyhow::anyhow!("AWS_SECRET_ACCESS_KEY not set (and no aws_profile in profile)"))?;
    Ok(S3Credentials { access_key, secret_key })
}

fn aws_config_get(profile: &str, key: &str) -> anyhow::Result<String> {
    let out = std::process::Command::new("aws")
        .args(["configure", "get", key, "--profile", profile])
        .output()
        .map_err(|e| anyhow::anyhow!("running `aws configure get {key}`: {e}"))?;
    if !out.status.success() {
        anyhow::bail!("aws profile '{profile}' has no {key}");
    }
    let value = String::from_utf8(out.stdout)?.trim().to_string();
    if value.is_empty() {
        anyhow::bail!("aws profile '{profile}' has an empty {key}");
    }
    Ok(value)
}

/// Escape a value for interpolation into a single-quoted SQL string literal,
/// doubling any embedded `'` per standard SQL escaping. Without this, a
/// secret containing a quote (e.g. `sup'ersecret`) would prematurely
/// terminate its literal and corrupt the generated statement -- and would
/// also make later redaction unable to find the true end of the value.
fn sql_escape(v: &str) -> String {
    v.replace('\'', "''")
}

/// Build the coordinator-wide `ATTACH` statement for the golden catalog.
/// Secrets travel inline (dev/bench posture — the coordinator config is
/// localhost). Callers MUST log only `redact_attach_sql(&sql)`, never `sql`.
pub fn build_attach_sql(
    catalog: &str,
    profile: &Profile,
    creds: &S3Credentials,
    token: &str,
) -> String {
    format!(
        "ATTACH '{url}' AS {catalog} (\
         TYPE iceberg_rest, \
         WAREHOUSE '{warehouse}', \
         TOKEN '{token}', \
         S3_ENDPOINT '{endpoint}', \
         S3_REGION '{region}', \
         S3_ACCESS_KEY '{ak}', \
         S3_SECRET_KEY '{sk}', \
         S3_PATH_STYLE '{path_style}')",
        url = sql_escape(&profile.polaris.url),
        warehouse = sql_escape(&profile.polaris.warehouse),
        token = sql_escape(token),
        endpoint = sql_escape(&profile.s3.endpoint),
        region = sql_escape(&profile.s3.region),
        ak = sql_escape(&creds.access_key),
        sk = sql_escape(&creds.secret_key),
        path_style = profile.s3.path_style,
    )
}

/// Redact secret-valued options in an ATTACH statement for safe logging.
pub fn redact_attach_sql(sql: &str) -> String {
    let mut out = sql.to_string();
    for opt in ["S3_SECRET_KEY", "S3_ACCESS_KEY", "TOKEN"] {
        out = redact_option(&out, opt);
    }
    out
}

/// Replace every `<OPT> '<value>'` occurrence with `<OPT> '***'`, where
/// `<value>` may contain SQL-escaped quotes (`''`). Scanning must be
/// escaping-aware: a doubled `''` inside the value is NOT the terminator,
/// only a lone `'` is. Handles every occurrence of `opt` in the string, not
/// just the first, since a naive single `find` would leave later
/// occurrences of the same option un-redacted.
fn redact_option(sql: &str, opt: &str) -> String {
    let needle = format!("{opt} '");
    let mut out = String::with_capacity(sql.len());
    let mut rest = sql;
    loop {
        let Some(start) = rest.find(&needle) else {
            out.push_str(rest);
            return out;
        };
        // Copy everything up to and including the opening quote.
        let value_start = start + needle.len();
        out.push_str(&rest[..value_start]);

        // Scan the value, treating `''` as an escaped quote and a lone `'`
        // as the terminator.
        let value_bytes = &rest.as_bytes()[value_start..];
        let mut i = 0;
        let end_rel = loop {
            match value_bytes.get(i) {
                None => break value_bytes.len(),
                Some(b'\'') => {
                    if value_bytes.get(i + 1) == Some(&b'\'') {
                        i += 2; // escaped quote, keep scanning
                    } else {
                        break i; // true terminator
                    }
                }
                Some(_) => i += 1,
            }
        };

        out.push_str("***");
        let value_end = value_start + end_rel;
        // Re-append the terminating quote (if present) and continue
        // scanning the remainder for further occurrences of `opt`.
        if value_end < rest.len() {
            out.push('\'');
            rest = &rest[value_end + 1..];
        } else {
            rest = &rest[value_end..];
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LOCAL_TOML: &str = r#"
name = "local"
manage_stack = true

[s3]
endpoint = "http://localhost:19000"
region = "us-east-1"
path_style = true
warehouse_bucket = "test-warehouse"

[polaris]
url = "http://localhost:18181/api/catalog"
warehouse = "test_warehouse"

[concurrency]
manifest = 64
direct_read = 8
write_streams = 8
"#;

    fn sample_profile() -> Profile {
        parse_profile_str(LOCAL_TOML).unwrap()
    }

    #[test]
    fn parses_a_full_profile() {
        let p = parse_profile_str(LOCAL_TOML).expect("parse");
        assert_eq!(p.name, "local");
        assert!(p.manage_stack);
        assert_eq!(p.s3.endpoint, "http://localhost:19000");
        assert!(p.s3.path_style);
        assert_eq!(p.polaris.warehouse, "test_warehouse");
        assert_eq!(p.concurrency.manifest, 64);
    }

    #[test]
    fn concurrency_defaults_apply_when_section_absent() {
        let toml = r#"
name = "min"
manage_stack = false
[s3]
endpoint = "http://x"
region = "us-east-1"
path_style = true
warehouse_bucket = "b"
[polaris]
url = "http://p"
warehouse = "w"
"#;
        let p = parse_profile_str(toml).expect("parse");
        assert_eq!(p.concurrency.manifest, 64);
        assert_eq!(p.concurrency.direct_read, 8);
    }

    #[test]
    fn rejects_inline_secret_keys() {
        let toml = r#"
name = "bad"
manage_stack = false
[s3]
endpoint = "http://x"
region = "us-east-1"
path_style = true
warehouse_bucket = "b"
secret_key = "AKIAsupersecret"
[polaris]
url = "http://p"
warehouse = "w"
"#;
        let err = parse_profile_str(toml).unwrap_err().to_string();
        assert!(err.contains("secret"), "got: {err}");
    }

    #[test]
    fn rejects_aws_secret_access_key() {
        let toml = r#"
name = "bad"
manage_stack = false
[s3]
endpoint = "http://x"
region = "us-east-1"
path_style = true
warehouse_bucket = "b"
aws_secret_access_key = "wJalrXUtnFEMI"
[polaris]
url = "http://p"
warehouse = "w"
"#;
        let err = parse_profile_str(toml).unwrap_err().to_string();
        assert!(err.contains("aws_secret_access_key"), "got: {err}");
    }

    #[test]
    fn rejects_uppercase_secret_key() {
        let toml = r#"
name = "bad"
manage_stack = false
[s3]
endpoint = "http://x"
region = "us-east-1"
path_style = true
warehouse_bucket = "b"
SECRET_KEY = "x"
[polaris]
url = "http://p"
warehouse = "w"
"#;
        let err = parse_profile_str(toml).unwrap_err().to_string();
        assert!(err.contains("SECRET_KEY"), "got: {err}");
    }

    #[test]
    fn allows_secret_path_field_and_comment() {
        // A `client_secret_path` field (an unknown, non-`deny_unknown_fields`
        // field, so it round-trips harmlessly) and a comment mentioning
        // "secret_key" must not false-trigger the guard: the guard is
        // key-name-aware, not a raw substring scan over the document text.
        let toml = r#"
# rotate secret_key regularly per the ops runbook
name = "local"
manage_stack = true

[s3]
endpoint = "http://localhost:19000"
region = "us-east-1"
path_style = true
warehouse_bucket = "test-warehouse"

[polaris]
url = "http://localhost:18181/api/catalog"
warehouse = "test_warehouse"
client_secret_path = "/run/secrets/oauth"
"#;
        let p = parse_profile_str(toml).expect("parse");
        assert_eq!(p.name, "local");
    }

    #[test]
    fn build_attach_sql_includes_s3_options() {
        let p = sample_profile();
        let creds = S3Credentials {
            access_key: "ak".to_string(),
            secret_key: "sk".to_string(),
        };
        let sql = build_attach_sql("golden", &p, &creds, "tok");
        assert!(sql.starts_with("ATTACH 'http://localhost:18181/api/catalog' AS golden"));
        assert!(sql.contains("TYPE iceberg_rest"));
        assert!(sql.contains("WAREHOUSE 'test_warehouse'"));
        assert!(sql.contains("S3_ENDPOINT 'http://localhost:19000'"));
        assert!(sql.contains("S3_ACCESS_KEY 'ak'"));
        assert!(sql.contains("S3_SECRET_KEY 'sk'"));
        assert!(sql.contains("S3_PATH_STYLE 'true'"));
        assert!(sql.contains("TOKEN 'tok'"));
    }

    #[test]
    fn redact_hides_secret_and_token_values() {
        let p = sample_profile();
        let creds = S3Credentials {
            access_key: "ak".to_string(),
            secret_key: "supersecret".to_string(),
        };
        let sql = build_attach_sql("golden", &p, &creds, "bearer-xyz");
        let red = redact_attach_sql(&sql);
        assert!(!red.contains("supersecret"), "secret leaked: {red}");
        assert!(!red.contains("bearer-xyz"), "token leaked: {red}");
        assert!(red.contains("S3_SECRET_KEY '***'"));
        assert!(red.contains("TOKEN '***'"));
        // Non-secret options remain visible for debugging.
        assert!(red.contains("S3_ENDPOINT 'http://localhost:19000'"));
    }

    #[test]
    fn build_attach_sql_escapes_quotes() {
        let p = sample_profile();
        let creds = S3Credentials {
            access_key: "ak".to_string(),
            secret_key: "sup'er'secret".to_string(),
        };
        let sql = build_attach_sql("golden", &p, &creds, "a'b");
        // Embedded quotes must be doubled so the literal stays well-formed.
        assert!(sql.contains("S3_SECRET_KEY 'sup''er''secret'"), "got: {sql}");
        assert!(sql.contains("TOKEN 'a''b'"), "got: {sql}");
    }

    #[test]
    fn redact_hides_secret_with_embedded_quote() {
        let p = sample_profile();
        let creds = S3Credentials {
            access_key: "ak".to_string(),
            secret_key: "sup'er'secret".to_string(),
        };
        let sql = build_attach_sql("golden", &p, &creds, "a'b");
        let red = redact_attach_sql(&sql);

        // The raw secret and token must not survive at all, in any form.
        assert!(!red.contains("sup'er'secret"), "secret leaked: {red}");
        assert!(!red.contains("sup''er''secret"), "escaped secret leaked: {red}");
        assert!(!red.contains("a'b"), "token leaked: {red}");
        // The specific fragment the old (non-escaping-aware) bug used to
        // leave behind when a quote appeared inside the value.
        assert!(!red.contains("er'secret"), "fragment leaked: {red}");
        assert!(!red.contains("er''secret"), "fragment leaked: {red}");

        assert!(red.contains("S3_SECRET_KEY '***'"), "got: {red}");
        assert!(red.contains("TOKEN '***'"), "got: {red}");
    }

    #[test]
    fn redact_all_occurrences() {
        // Hand-built string with the same option appearing twice; the old
        // implementation only redacted the first `find` match.
        let sql = "ATTACH 'x' AS c (TOKEN 'x', OTHER 'y', TOKEN 'x')";
        let red = redact_attach_sql(sql);
        assert_eq!(red.matches("TOKEN '***'").count(), 2, "got: {red}");
        assert!(!red.contains("TOKEN 'x'"), "raw token leaked: {red}");
    }
}
