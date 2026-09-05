//! Layered `SQE_*` environment overlay for [`super::SqeConfig`].
//!
//! TOML is parsed to a [`toml::Value`] tree, then every `SQE_<SECTION>__<FIELD>`
//! process variable is walked into the same tree (typed by the existing value
//! when present, otherwise inferred). Deserialize runs once. Malformed values
//! and unknown keys fail closed.
//!
//! A small alias table keeps the short operator names that predate the
//! two-level convention (`SQE_PRODUCTION_MODE`, `SQE_MEMORY_LIMIT`, …).

use super::SqeConfig;
use crate::error::SqeError;
use crate::SecretString;

/// Documented short names that do not follow `SQE_<SECTION>__<FIELD>`.
/// Applied first; a later namespaced variable for the same path wins.
const ALIASES: &[(&str, &str)] = &[
    ("SQE_PRODUCTION_MODE", "coordinator.production_mode"),
    ("SQE_MEMORY_LIMIT", "coordinator.memory_limit"),
    ("SQE_MEMORY_POOL", "coordinator.memory_pool"),
    ("SQE_MODE", "coordinator.mode"),
    ("SQE_TLS__CERT_FILE", "coordinator.tls.cert_file"),
    ("SQE_TLS__KEY_FILE", "coordinator.tls.key_file"),
    ("SQE_TLS__CA_FILE", "coordinator.tls.ca_file"),
    ("SQE_CATALOG__POLARIS_URL", "catalog.catalog_url"),
    ("SQE_WORKER_ROW_GROUP_PRUNING", "worker.row_group_pruning"),
];

/// Overlay `SQE_*` environment variables onto a parsed TOML document.
///
/// Returns the list of applied variable names (for the startup audit line).
pub fn overlay_env(root: &mut toml::Value) -> Result<Vec<String>, SqeError> {
    let mut applied = Vec::new();

    // Aliases first so a namespaced twin overwrites them when both are set.
    for (env_key, path) in ALIASES {
        if let Some(raw) = read_env(env_key)? {
            insert_path(root, path, env_key, &raw)?;
            log_override(env_key, path, &raw);
            applied.push((*env_key).to_string());
        }
    }

    for (key, raw) in collect_sqe_vars()? {
        if ALIASES.iter().any(|(alias, _)| *alias == key) {
            continue;
        }
        let path = env_key_to_path(&key).ok_or_else(|| {
            SqeError::Config(format!(
                "unknown override {key}: environment variables must be \
                 SQE_<SECTION>__<FIELD> (or a documented alias)"
            ))
        })?;
        insert_path(root, &path, &key, &raw)?;
        log_override(&key, &path, &raw);
        applied.push(key);
    }

    Ok(applied)
}

/// Load config from a TOML string, applying env overlays, secret inheritance
/// and AWS fallbacks. Does not run [`SqeConfig::validate`] so CLI flags can
/// still patch the struct before startup checks.
pub fn load_from_str(content: &str) -> crate::error::Result<SqeConfig> {
    let mut value: toml::Value = toml::from_str(content)
        .map_err(|e| SqeError::Config(format!("Failed to parse config: {e}")))?;
    let applied = overlay_env(&mut value)?;
    let mut config: SqeConfig = value.try_into().map_err(|e| {
        let hint = if applied.is_empty() {
            String::new()
        } else {
            format!(" (after env overrides {})", applied.join(", "))
        };
        SqeError::Config(format!("Failed to parse config{hint}: {e}"))
    })?;
    inherit_oidc_password_secret(&mut config);
    apply_aws_env_fallbacks(&mut config);
    Ok(config)
}

/// Dump the merged, redacted TOML (secrets replaced with `<redacted>`).
pub fn load_redacted_toml(content: &str) -> crate::error::Result<String> {
    let mut value: toml::Value = toml::from_str(content)
        .map_err(|e| SqeError::Config(format!("Failed to parse config: {e}")))?;
    overlay_env(&mut value)?;
    redact_secrets(&mut value);
    toml::to_string_pretty(&value)
        .map_err(|e| SqeError::Config(format!("Failed to render config: {e}")))
}

pub(super) fn inherit_oidc_password_secret(config: &mut SqeConfig) {
    let shared_secret = config.auth.client_secret.expose().to_string();
    if shared_secret.is_empty() {
        return;
    }
    for provider in config.auth.providers.iter_mut() {
        if let super::AuthProviderConfig::OidcPassword { client_secret, .. } = provider {
            if client_secret.is_empty() {
                *client_secret = shared_secret.clone();
            }
        }
    }
}

pub(super) fn apply_aws_env_fallbacks(config: &mut SqeConfig) {
    let non_empty = |name: &str| {
        std::env::var(name)
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
    };

    if config.storage.s3_access_key.is_empty() && config.storage.s3_secret_key.expose().is_empty() {
        if let (Some(access), Some(secret)) = (
            non_empty("AWS_ACCESS_KEY_ID"),
            non_empty("AWS_SECRET_ACCESS_KEY"),
        ) {
            config.storage.s3_access_key = access;
            config.storage.s3_secret_key = SecretString::new(secret);
            if let Some(token) = non_empty("AWS_SESSION_TOKEN") {
                config.storage.s3_session_token = SecretString::new(token);
            }
        }
    }

    if config.storage.s3_region.is_empty() {
        if let Some(region) = non_empty("AWS_REGION").or_else(|| non_empty("AWS_DEFAULT_REGION")) {
            config.storage.s3_region = region;
        }
    }

    if config.storage.s3_endpoint.is_empty() {
        if let Some(endpoint) =
            non_empty("AWS_ENDPOINT_URL_S3").or_else(|| non_empty("AWS_ENDPOINT_URL"))
        {
            config.storage.s3_endpoint = endpoint;
        }
    }
}

fn read_env(key: &str) -> Result<Option<String>, SqeError> {
    match std::env::var(key) {
        Ok(v) => Ok(Some(v)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(e) => Err(SqeError::Config(format!("{key} is not valid unicode: {e}"))),
    }
}

/// `SQE_FOO__BAR_BAZ` → `foo.bar_baz`. Requires at least one `__` so flat
/// names (`SQE_USER`, `SQE_CONFIG`, `SQE_IT_*`) are ignored unless aliased.
fn env_key_to_path(key: &str) -> Option<String> {
    let rest = key.strip_prefix("SQE_")?;
    if !rest.contains("__") {
        return None;
    }
    if !rest
        .chars()
        .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
    {
        return None;
    }
    Some(
        rest.split("__")
            .map(|seg| seg.to_ascii_lowercase())
            .collect::<Vec<_>>()
            .join("."),
    )
}

fn collect_sqe_vars() -> Result<Vec<(String, String)>, SqeError> {
    let mut out = Vec::new();
    for (k, v) in std::env::vars() {
        if k.starts_with("SQE_") && env_key_to_path(&k).is_some() {
            out.push((k, v));
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

fn insert_path(
    root: &mut toml::Value,
    path: &str,
    env_key: &str,
    raw: &str,
) -> Result<(), SqeError> {
    let trimmed = raw.trim();
    let segs: Vec<&str> = path.split('.').collect();
    if segs.is_empty() {
        return Err(SqeError::Config(format!("{env_key} has an empty path")));
    }
    let mut cur = root;
    for (i, seg) in segs.iter().enumerate() {
        let last = i + 1 == segs.len();
        if last {
            let existing = match cur {
                toml::Value::Table(t) => t.get(*seg),
                toml::Value::Array(arr) => {
                    let idx: usize = seg.parse().map_err(|_| {
                        SqeError::Config(format!(
                            "{env_key}: {seg:?} is not an array index under {path}"
                        ))
                    })?;
                    arr.get(idx)
                }
                other => {
                    return Err(SqeError::Config(format!(
                        "{env_key}: cannot set {path} on a {}",
                        type_name(other)
                    )));
                }
            };
            let parsed = parse_env_value(env_key, trimmed, existing)?;
            match cur {
                toml::Value::Table(t) => {
                    t.insert((*seg).to_string(), parsed);
                }
                toml::Value::Array(arr) => {
                    let idx: usize = seg.parse().map_err(|_| {
                        SqeError::Config(format!("{env_key}: {seg:?} is not an array index"))
                    })?;
                    if idx >= arr.len() {
                        return Err(SqeError::Config(format!(
                            "{env_key}: index {idx} is out of range for {path} \
                             (array length {})",
                            arr.len()
                        )));
                    }
                    arr[idx] = parsed;
                }
                _ => unreachable!(),
            }
            return Ok(());
        }

        cur = descend(cur, seg, env_key, path)?;
    }
    Ok(())
}

fn descend<'a>(
    cur: &'a mut toml::Value,
    seg: &str,
    env_key: &str,
    path: &str,
) -> Result<&'a mut toml::Value, SqeError> {
    if let Ok(idx) = seg.parse::<usize>() {
        let arr = cur.as_array_mut().ok_or_else(|| {
            SqeError::Config(format!(
                "{env_key}: {seg} is an array index but the parent of {path} is not an array"
            ))
        })?;
        if idx >= arr.len() {
            return Err(SqeError::Config(format!(
                "{env_key}: index {idx} is out of range (array length {})",
                arr.len()
            )));
        }
        return Ok(&mut arr[idx]);
    }

    if !cur.is_table() {
        *cur = toml::Value::Table(toml::map::Map::new());
    }
    let table = cur.as_table_mut().expect("just made a table");
    Ok(table
        .entry(seg.to_string())
        .or_insert_with(|| toml::Value::Table(toml::map::Map::new())))
}

fn parse_env_value(
    env_key: &str,
    trimmed: &str,
    existing: Option<&toml::Value>,
) -> Result<toml::Value, SqeError> {
    if let Some(existing) = existing {
        return coerce_to(env_key, trimmed, existing);
    }
    Ok(infer_value(trimmed))
}

fn coerce_to(env_key: &str, trimmed: &str, existing: &toml::Value) -> Result<toml::Value, SqeError> {
    match existing {
        toml::Value::Boolean(_) => parse_bool(env_key, trimmed).map(toml::Value::Boolean),
        toml::Value::Integer(_) => trimmed.parse::<i64>().map(toml::Value::Integer).map_err(|_| {
            SqeError::Config(format!("{env_key}={trimmed:?} is not a valid integer"))
        }),
        toml::Value::Float(_) => trimmed.parse::<f64>().map(toml::Value::Float).map_err(|_| {
            SqeError::Config(format!("{env_key}={trimmed:?} is not a valid number"))
        }),
        toml::Value::String(_) => Ok(toml::Value::String(trimmed.to_string())),
        toml::Value::Datetime(_) => Ok(toml::Value::String(trimmed.to_string())),
        other => Err(SqeError::Config(format!(
            "{env_key} cannot override a {} field",
            type_name(other)
        ))),
    }
}

fn infer_value(trimmed: &str) -> toml::Value {
    match trimmed.to_ascii_lowercase().as_str() {
        "true" | "yes" => return toml::Value::Boolean(true),
        "false" | "no" => return toml::Value::Boolean(false),
        _ => {}
    }
    if let Ok(i) = trimmed.parse::<i64>() {
        return toml::Value::Integer(i);
    }
    if let Ok(f) = trimmed.parse::<f64>() {
        return toml::Value::Float(f);
    }
    toml::Value::String(trimmed.to_string())
}

fn parse_bool(env_key: &str, trimmed: &str) -> Result<bool, SqeError> {
    match trimmed.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        _ => Err(SqeError::Config(format!(
            "{env_key}={trimmed:?} is not a valid bool"
        ))),
    }
}

fn type_name(v: &toml::Value) -> &'static str {
    match v {
        toml::Value::Boolean(_) => "bool",
        toml::Value::Integer(_) => "integer",
        toml::Value::Float(_) => "float",
        toml::Value::String(_) => "string",
        toml::Value::Datetime(_) => "datetime",
        toml::Value::Array(_) => "array",
        toml::Value::Table(_) => "table",
    }
}

fn is_secret_path(path: &str) -> bool {
    let p = path.to_ascii_lowercase();
    p.contains("secret")
        || p.contains("password")
        || p.contains("token")
        || p.ends_with("_key")
        || p.contains(".key")
        || p.contains("mask_key")
}

fn log_override(env_key: &str, path: &str, raw: &str) {
    if is_secret_path(path) {
        tracing::info!(env = env_key, path, "config override applied from environment");
    } else {
        tracing::info!(
            env = env_key,
            path,
            value = raw.trim(),
            "config override applied from environment"
        );
    }
}

fn redact_secrets(value: &mut toml::Value) {
    match value {
        toml::Value::Table(t) => {
            for (k, v) in t.iter_mut() {
                if is_secret_path(k) && v.as_str().is_some() {
                    *v = toml::Value::String("<redacted>".into());
                } else {
                    redact_secrets(v);
                }
            }
        }
        toml::Value::Array(arr) => {
            for v in arr {
                redact_secrets(v);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_key_to_path_two_level() {
        assert_eq!(
            env_key_to_path("SQE_QUERY__MAX_CONCURRENT_PER_USER").as_deref(),
            Some("query.max_concurrent_per_user")
        );
        assert_eq!(
            env_key_to_path("SQE_METRICS__OPENLINEAGE__ENABLED").as_deref(),
            Some("metrics.openlineage.enabled")
        );
        assert_eq!(env_key_to_path("SQE_USER"), None);
        assert_eq!(env_key_to_path("SQE_CONFIG"), None);
        assert_eq!(env_key_to_path("SQE_IT_METADATA_LOCATION"), None);
    }

    #[test]
    fn infer_bool_and_int() {
        assert_eq!(infer_value("true"), toml::Value::Boolean(true));
        assert_eq!(infer_value("YES"), toml::Value::Boolean(true));
        assert_eq!(infer_value("8GB"), toml::Value::String("8GB".into()));
        assert_eq!(infer_value("300"), toml::Value::Integer(300));
        assert_eq!(infer_value("1.0"), toml::Value::Float(1.0));
    }

    #[test]
    fn parse_bool_rejects_garbage() {
        let err = parse_bool("SQE_PRODUCTION_MODE", "maybe").unwrap_err().to_string();
        assert!(err.contains("SQE_PRODUCTION_MODE"));
        assert!(err.contains("maybe"));
    }

}
