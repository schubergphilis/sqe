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
}
