//! Runtime catalog construction for `ATTACH ... (TYPE <kind>, ...)`.
//!
//! `build_catalog` is the single dispatch the coordinator and embedded
//! query handler call when the parser produces an [`AttachStatement`].
//! Each per-kind helper translates the SQL option dictionary plus the
//! optional secret reference into the `(name, props)` shape the
//! upstream `iceberg-catalog-*` builders consume, then returns an
//! `Arc<dyn iceberg::Catalog>` ready to register with DataFusion.
//!
//! Backends mirror what the cluster's TOML-driven `for_session_with`
//! path in `rest_catalog.rs` already wires; the SQL surface is
//! additive, not a reimplementation.
//!
//! Spec: `docs/superpowers/specs/2026-05-09-attach-catalog-and-secrets-design.md` §5.3
//!
//! All public errors are returned as `String` rather than `SqeError`
//! to keep the boundary narrow. The coordinator wraps them into the
//! ambient error type at the call site.
//!
//! Option keys are case-insensitive but are normalised to ASCII
//! uppercase by the parser, so all lookups in this module use the
//! upper-case spelling: `WAREHOUSE`, `SECRET`, `TOKEN`, `PREFIX`,
//! `REGION`, `ENDPOINT_URL`, `AUTH_MODE`.

use std::collections::BTreeMap;
use std::sync::Arc;

// `Secret` is only matched in backend builders that are feature-gated
// (rest / glue / s3tables / hms). Keep the import behind the same gates so
// slim `--no-default-features` builds do not warn under `-D warnings`.
#[cfg(any(
    feature = "rest",
    feature = "glue",
    feature = "s3tables",
    feature = "hms",
))]
use sqe_core::Secret;
use sqe_core::SecretStore;
use sqe_sql::{CatalogKind, OptionValue};

/// Build an `iceberg::Catalog` from an ATTACH option dictionary.
///
/// `location` is the per-backend primary identifier (REST URL, JDBC
/// URL, ARN, filesystem path, ...). `kind` selects the dispatch.
/// `options` is the post-parser uppercase-keyed dictionary.
/// `secrets` is the in-memory secret store; the `SECRET <name>`
/// option is resolved against it.
///
/// # Errors
///
/// Returns `Err(String)` on:
/// - Unknown or wrong-typed secret reference
/// - Missing required option
/// - Backend not compiled in (cargo feature off)
/// - Builder construction failure (network reach, malformed
///   location, sqlx driver missing, etc.)
pub async fn build_catalog(
    location: &str,
    kind: CatalogKind,
    options: &BTreeMap<String, OptionValue>,
    secrets: &SecretStore,
) -> Result<Arc<dyn iceberg::Catalog>, String> {
    let result = match kind {
        CatalogKind::Sqlite => build_sqlite(location, options).await,
        CatalogKind::IcebergRest => build_iceberg_rest(location, options, secrets).await,
        CatalogKind::Glue => build_glue(location, options, secrets).await,
        CatalogKind::S3Tables => build_s3tables(location, options, secrets).await,
        CatalogKind::Hms => build_hms(location, options, secrets).await,
        CatalogKind::Jdbc => build_jdbc(location, options, secrets).await,
        CatalogKind::Hadoop => Err(error_not_yet("hadoop")),
    };
    result.map_err(|e| format!("ATTACH (TYPE {}) failed: {e}", kind.name()))
}

fn error_not_yet(kind: &str) -> String {
    format!("TYPE {kind} not yet supported in this build")
}

// ---------------------------------------------------------------------------
// Per-backend builders.
//
// Each helper is small on purpose: pull the options it needs, build
// the props HashMap, hand off to the upstream `CatalogBuilder`. Cargo
// feature gates mirror the gates on `sqe_catalog`'s top-level features
// so a slim build (e.g. `--no-default-features --features rest,hadoop`)
// rejects the unsupported kinds with a clear "feature not enabled"
// message rather than a link error.
// ---------------------------------------------------------------------------

/// Build a SQLite-backed Iceberg catalog (`iceberg_catalog_sql`).
///
/// `location` is the SQLite database path. Two shapes accepted:
/// - bare filesystem path: `/var/lib/sqe/wh` -> db at `<path>/sqe.db`,
///   warehouse data at `<path>/iceberg/` (mirrors the embedded
///   `--catalog NAME=PATH` flag default in `crates/sqe-cli/src/embedded.rs`)
/// - explicit URL: `sqlite:///abs/path/db.sqlite?mode=rwc`
///
/// `WAREHOUSE` option overrides the derived `<path>/iceberg/` data
/// directory. Required for the URL-shape; optional for bare paths.
///
/// Mirrors `attach_sqlite_catalog` in `crates/sqe-cli/src/embedded.rs`.
#[cfg(feature = "sql-sqlite")]
async fn build_sqlite(
    location: &str,
    options: &BTreeMap<String, OptionValue>,
) -> Result<Arc<dyn iceberg::Catalog>, String> {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use iceberg::CatalogBuilder;
    use iceberg_catalog_sql::{
        SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalogBuilder,
    };

    let trimmed = location.trim();
    if trimmed.is_empty() {
        return Err("location must not be empty for TYPE sqlite".to_string());
    }

    // Resolve the SQLite URL plus the warehouse data directory. The
    // bare-path shape mirrors what `EmbeddedCatalog` does today: the
    // location IS the warehouse root, the db file lives next to a
    // sibling `iceberg/` data dir.
    let (db_url, default_warehouse) = if trimmed.starts_with("sqlite:") {
        // URL shape: caller picked the db path explicitly. The
        // warehouse must come from the WAREHOUSE option because we
        // can't infer a sibling directory from the URL safely.
        (trimmed.to_string(), None)
    } else {
        let path = PathBuf::from(trimmed);
        std::fs::create_dir_all(&path).map_err(|e| {
            format!(
                "could not create warehouse directory {}: {e}",
                path.display()
            )
        })?;
        let abs = path.canonicalize().map_err(|e| {
            format!("could not canonicalise warehouse path {}: {e}", path.display())
        })?;
        let data_root = abs.join("iceberg");
        std::fs::create_dir_all(&data_root)
            .map_err(|e| format!("could not create data dir {}: {e}", data_root.display()))?;
        let db_path = abs.join("sqe.db");
        // `mode=rwc` tells SQLite to create the db file if missing;
        // sqlx defaults to read-write without create and fails on
        // first run otherwise.
        let url = format!("sqlite://{}?mode=rwc", db_path.display());
        let warehouse = format!("file://{}", data_root.display());
        (url, Some(warehouse))
    };

    let warehouse = match options.get("WAREHOUSE").and_then(OptionValue::as_str) {
        Some(w) => w.to_string(),
        None => default_warehouse.ok_or_else(|| {
            "option `WAREHOUSE` is required for TYPE sqlite when location is a sqlite:// URL"
                .to_string()
        })?,
    };

    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), db_url);
    props.insert(SQL_CATALOG_PROP_WAREHOUSE.to_string(), warehouse);

    // The builder's `name` is recorded as the row scope inside the
    // SQLite metadata tables. Embedded mode uses a fixed name so
    // every db file holds a single coherent scope; mirror that here.
    // The user-facing catalog identifier comes from
    // `register_catalog(name, ...)` at the coordinator level.
    let catalog = SqlCatalogBuilder::default()
        .load("sqe-attached".to_string(), props)
        .await
        .map_err(|e| format!("SQLite catalog open failed: {e}"))?;

    Ok(Arc::new(catalog))
}

#[cfg(not(feature = "sql-sqlite"))]
async fn build_sqlite(
    _location: &str,
    _options: &BTreeMap<String, OptionValue>,
) -> Result<Arc<dyn iceberg::Catalog>, String> {
    Err(
        "TYPE sqlite requires the `sql-sqlite` cargo feature on sqe-catalog \
         (or `sql-postgres` for `TYPE jdbc`)"
            .to_string(),
    )
}

/// Build an Iceberg REST catalog (`iceberg_catalog_rest`).
///
/// `location` is the REST URL (e.g. `https://polaris/api/catalog`).
/// Required option: `WAREHOUSE`. Optional: `SECRET <name>` referencing
/// a `Secret::Bearer { token }`, or `TOKEN '<value>'` for an inline
/// token (less safe, but useful for prototyping). `PREFIX '<value>'`
/// passes through a non-standard API prefix.
///
/// Polaris, Nessie, Unity OSS, Glue's REST endpoint, and the
/// federated AWS S3 Tables REST endpoint all flow through this builder.
#[cfg(feature = "rest")]
async fn build_iceberg_rest(
    location: &str,
    options: &BTreeMap<String, OptionValue>,
    secrets: &SecretStore,
) -> Result<Arc<dyn iceberg::Catalog>, String> {
    use std::collections::HashMap;

    use iceberg::CatalogBuilder;
    use iceberg_catalog_rest::{
        REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
    };

    let trimmed = location.trim();
    if trimmed.is_empty() {
        return Err("location must not be empty for TYPE iceberg_rest".to_string());
    }

    let warehouse = options
        .get("WAREHOUSE")
        .and_then(OptionValue::as_str)
        .ok_or_else(|| "option `WAREHOUSE` is required for TYPE iceberg_rest".to_string())?;

    let mut props: HashMap<String, String> = HashMap::new();
    props.insert(REST_CATALOG_PROP_URI.to_string(), trimmed.to_string());
    props.insert(REST_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.to_string());

    // SECRET (preferred) and TOKEN (inline) both end up in the
    // `token` prop the REST catalog reads. SECRET wins when both are
    // present so users can override an inline value by name.
    if let Some(token) = options.get("TOKEN").and_then(OptionValue::as_str) {
        props.insert("token".to_string(), token.to_string());
    }
    if let Some(secret_ref) = options.get("SECRET").and_then(OptionValue::as_secret_ref) {
        let secret = secrets.get(secret_ref).map_err(|e| e.to_string())?;
        match &secret {
            Secret::Bearer { token } => {
                props.insert("token".to_string(), token.clone());
            }
            other => {
                return Err(format!(
                    "secret '{secret_ref}' is type {} but TYPE iceberg_rest expects bearer",
                    other.type_name()
                ));
            }
        }
    }

    if let Some(prefix) = options.get("PREFIX").and_then(OptionValue::as_str) {
        props.insert("prefix".to_string(), prefix.to_string());
    }

    // S3 FileIO config for non-AWS endpoints (RustFS, StorageGRID, ...).
    // Without these, the manifest-list/data-file reads fall back to the
    // ambient AWS credential chain and fail with "region is missing" against
    // a custom endpoint. Absent options leave that fallback in place.
    for (k, v) in s3_props_from_options(options) {
        props.insert(k, v);
    }

    let catalog = RestCatalogBuilder::default()
        .load("sqe-attached-rest".to_string(), props)
        .await
        .map_err(|e| format!("Iceberg REST catalog open failed: {e}"))?;

    Ok(Arc::new(catalog))
}

#[cfg(not(feature = "rest"))]
async fn build_iceberg_rest(
    _location: &str,
    _options: &BTreeMap<String, OptionValue>,
    _secrets: &SecretStore,
) -> Result<Arc<dyn iceberg::Catalog>, String> {
    Err("TYPE iceberg_rest requires the `rest` cargo feature on sqe-catalog".to_string())
}

/// Map ATTACH `S3_*` options to the `s3.*` catalog FileIO props the REST
/// catalog builder consumes (mirrors `rest_catalog.rs:868-894`). Only
/// present options produce props; absent ones are skipped so ambient config
/// still applies as a fallback.
///
/// Only compiled when `rest` is enabled: the sole production call site is
/// `build_iceberg_rest`, and unit tests for this helper are also rest-gated.
#[cfg(feature = "rest")]
fn s3_props_from_options(options: &BTreeMap<String, OptionValue>) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for (opt_key, prop_key) in [
        ("S3_ENDPOINT", "s3.endpoint"),
        ("S3_REGION", "s3.region"),
        ("S3_ACCESS_KEY", "s3.access-key-id"),
        ("S3_SECRET_KEY", "s3.secret-access-key"),
        ("S3_PATH_STYLE", "s3.path-style-access"),
    ] {
        if let Some(v) = options.get(opt_key).and_then(OptionValue::as_str) {
            out.push((prop_key.to_string(), v.to_string()));
        }
    }
    out
}

/// Resolve a `SECRET <name>` option against the secret store and
/// translate the resulting `Secret::Aws` into the `(access_key,
/// secret_key, session_token, region, profile)` props the upstream
/// AWS catalog builders consume. Returns `Ok(None)` when no SECRET
/// is set; the AWS default credential chain handles that case at the
/// builder layer.
///
/// `kind_label` is interpolated into the wrong-kind error so the
/// caller does not need to format twice ("glue", "s3tables").
#[cfg(any(feature = "glue", feature = "s3tables"))]
fn aws_secret_to_props(
    options: &BTreeMap<String, OptionValue>,
    secrets: &SecretStore,
    kind_label: &str,
) -> Result<Option<AwsProps>, String> {
    let Some(secret_ref) = options.get("SECRET").and_then(OptionValue::as_secret_ref) else {
        return Ok(None);
    };
    let secret = secrets.get(secret_ref).map_err(|e| e.to_string())?;
    match &secret {
        Secret::Aws {
            access_key,
            secret_key,
            session_token,
            region,
            profile,
        } => Ok(Some(AwsProps {
            access_key: access_key.clone(),
            secret_key: secret_key.clone(),
            session_token: session_token.clone(),
            region: region.clone(),
            profile: profile.clone(),
        })),
        other => Err(format!(
            "secret '{secret_ref}' is type {} but TYPE {kind_label} expects aws",
            other.type_name()
        )),
    }
}

#[cfg(any(feature = "glue", feature = "s3tables"))]
struct AwsProps {
    access_key: Option<String>,
    secret_key: Option<String>,
    session_token: Option<String>,
    region: Option<String>,
    profile: Option<String>,
}

/// Build an AWS Glue Data Catalog (`iceberg_catalog_glue`).
///
/// `location` is the Glue catalog ARN
/// (`arn:aws:glue:<region>:<account>:catalog/<name>`) — currently
/// surfaced through the Glue builder's `URI` prop, which doubles as
/// the LocalStack endpoint for tests. Required: `WAREHOUSE`. Optional:
/// `SECRET <name>` referencing `Secret::Aws`, `REGION`, `ENDPOINT_URL`
/// (overrides location-derived endpoint).
///
/// AWS credential resolution order matches `build_aws_config` from
/// `aws_config.rs`: explicit SECRET props win, then env, then shared
/// credentials, then IMDS / ECS / EKS Pod Identity. The `REGION`
/// option overrides the secret-supplied region.
#[cfg(feature = "glue")]
async fn build_glue(
    location: &str,
    options: &BTreeMap<String, OptionValue>,
    secrets: &SecretStore,
) -> Result<Arc<dyn iceberg::Catalog>, String> {
    use std::collections::HashMap;

    use iceberg::CatalogBuilder;
    use iceberg_catalog_glue::{
        AWS_ACCESS_KEY_ID, AWS_PROFILE_NAME, AWS_REGION_NAME, AWS_SECRET_ACCESS_KEY,
        AWS_SESSION_TOKEN, GLUE_CATALOG_PROP_WAREHOUSE, GlueCatalogBuilder,
    };

    let trimmed = location.trim();
    let warehouse = options
        .get("WAREHOUSE")
        .and_then(OptionValue::as_str)
        .ok_or_else(|| "option `WAREHOUSE` is required for TYPE glue".to_string())?;

    let mut props: HashMap<String, String> = HashMap::new();
    props.insert(GLUE_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.to_string());

    // The Glue builder's `URI` prop carries the AWS endpoint. The
    // ATTACH `<location>` is the catalog ARN; we forward it as the
    // SDK's endpoint hint when ENDPOINT_URL is not set explicitly,
    // matching what LocalStack callers expect.
    if let Some(endpoint) = options.get("ENDPOINT_URL").and_then(OptionValue::as_str) {
        props.insert(
            iceberg_catalog_glue::GLUE_CATALOG_PROP_URI.to_string(),
            endpoint.to_string(),
        );
    } else if !trimmed.is_empty() && !trimmed.starts_with("arn:") {
        // Bare URL (LocalStack, AWS endpoint test). ARNs don't go
        // into `uri`; the AWS SDK derives the endpoint from region
        // when uri is unset.
        props.insert(
            iceberg_catalog_glue::GLUE_CATALOG_PROP_URI.to_string(),
            trimmed.to_string(),
        );
    }

    if let Some(aws) = aws_secret_to_props(options, secrets, "glue")? {
        if let (Some(ak), Some(sk)) = (aws.access_key, aws.secret_key) {
            props.insert(AWS_ACCESS_KEY_ID.to_string(), ak);
            props.insert(AWS_SECRET_ACCESS_KEY.to_string(), sk);
        }
        if let Some(st) = aws.session_token {
            props.insert(AWS_SESSION_TOKEN.to_string(), st);
        }
        if let Some(r) = aws.region {
            props.insert(AWS_REGION_NAME.to_string(), r);
        }
        if let Some(p) = aws.profile {
            props.insert(AWS_PROFILE_NAME.to_string(), p);
        }
    }

    // REGION option always wins over secret-supplied region.
    if let Some(r) = options.get("REGION").and_then(OptionValue::as_str) {
        props.insert(AWS_REGION_NAME.to_string(), r.to_string());
    }

    let catalog = GlueCatalogBuilder::default()
        .load("sqe-attached-glue".to_string(), props)
        .await
        .map_err(|e| format!("AWS Glue catalog open failed: {e}"))?;

    Ok(Arc::new(catalog))
}

#[cfg(not(feature = "glue"))]
async fn build_glue(
    _location: &str,
    _options: &BTreeMap<String, OptionValue>,
    _secrets: &SecretStore,
) -> Result<Arc<dyn iceberg::Catalog>, String> {
    Err("TYPE glue requires the `glue` cargo feature on sqe-catalog".to_string())
}

/// Build an AWS S3 Tables catalog (`iceberg_catalog_s3tables`).
///
/// `location` is the table-bucket ARN
/// (`arn:aws:s3tables:<region>:<account>:bucket/<name>`) and goes
/// into the `table_bucket_arn` prop. Optional: `SECRET <name>`,
/// `REGION`, `ENDPOINT_URL` (LocalStack support).
///
/// The s3tables crate keeps its AWS prop name constants private to
/// the module, so we use the matching string literals here. The
/// upstream `create_sdk_config` reads these by name.
#[cfg(feature = "s3tables")]
async fn build_s3tables(
    location: &str,
    options: &BTreeMap<String, OptionValue>,
    secrets: &SecretStore,
) -> Result<Arc<dyn iceberg::Catalog>, String> {
    use std::collections::HashMap;

    use iceberg::CatalogBuilder;
    use iceberg_catalog_s3tables::{
        S3TABLES_CATALOG_PROP_ENDPOINT_URL, S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN,
        S3TablesCatalogBuilder,
    };

    // String literals matching the s3tables crate's private utils
    // constants. Kept here because the s3tables module does not
    // re-export them (utils is `mod utils`, not `pub mod utils`).
    const AWS_REGION_NAME: &str = "region_name";
    const AWS_ACCESS_KEY_ID: &str = "aws_access_key_id";
    const AWS_SECRET_ACCESS_KEY: &str = "aws_secret_access_key";
    const AWS_SESSION_TOKEN: &str = "aws_session_token";
    const AWS_PROFILE_NAME: &str = "profile_name";

    let trimmed = location.trim();
    if trimmed.is_empty() {
        return Err("location must not be empty for TYPE s3tables".to_string());
    }

    let mut props: HashMap<String, String> = HashMap::new();
    props.insert(
        S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN.to_string(),
        trimmed.to_string(),
    );

    if let Some(endpoint) = options.get("ENDPOINT_URL").and_then(OptionValue::as_str) {
        props.insert(
            S3TABLES_CATALOG_PROP_ENDPOINT_URL.to_string(),
            endpoint.to_string(),
        );
    }

    if let Some(aws) = aws_secret_to_props(options, secrets, "s3tables")? {
        if let (Some(ak), Some(sk)) = (aws.access_key, aws.secret_key) {
            props.insert(AWS_ACCESS_KEY_ID.to_string(), ak);
            props.insert(AWS_SECRET_ACCESS_KEY.to_string(), sk);
        }
        if let Some(st) = aws.session_token {
            props.insert(AWS_SESSION_TOKEN.to_string(), st);
        }
        if let Some(r) = aws.region {
            props.insert(AWS_REGION_NAME.to_string(), r);
        }
        if let Some(p) = aws.profile {
            props.insert(AWS_PROFILE_NAME.to_string(), p);
        }
    }

    if let Some(r) = options.get("REGION").and_then(OptionValue::as_str) {
        props.insert(AWS_REGION_NAME.to_string(), r.to_string());
    }

    let catalog = S3TablesCatalogBuilder::default()
        .load("sqe-attached-s3tables".to_string(), props)
        .await
        .map_err(|e| format!("AWS S3 Tables catalog open failed: {e}"))?;

    Ok(Arc::new(catalog))
}

#[cfg(not(feature = "s3tables"))]
async fn build_s3tables(
    _location: &str,
    _options: &BTreeMap<String, OptionValue>,
    _secrets: &SecretStore,
) -> Result<Arc<dyn iceberg::Catalog>, String> {
    Err("TYPE s3tables requires the `s3tables` cargo feature on sqe-catalog".to_string())
}

/// Build a Hive Metastore catalog (`iceberg_catalog_hms`).
///
/// `location` is the Thrift URL (`thrift://hms.example.com:9083`).
/// Required: `WAREHOUSE`. Optional: `AUTH_MODE` (`none` | `kerberos`
/// | `plain`); `SECRET <name>` referencing `Secret::Basic` is
/// required when `AUTH_MODE = plain`.
///
/// The upstream `HmsCatalogBuilder` accepts the props but does not
/// natively consume SASL/PLAIN credentials yet (tracked upstream).
/// This builder validates the SQL contract: `AUTH_MODE = plain`
/// without a Basic secret is a parse-time error so misconfiguration
/// surfaces here rather than at first thrift call. Threading the
/// resolved username/password into the upstream builder is a
/// follow-up once iceberg-catalog-hms grows the corresponding
/// props.
#[cfg(feature = "hms")]
async fn build_hms(
    location: &str,
    options: &BTreeMap<String, OptionValue>,
    secrets: &SecretStore,
) -> Result<Arc<dyn iceberg::Catalog>, String> {
    use std::collections::HashMap;

    use iceberg::CatalogBuilder;
    use iceberg_catalog_hms::{
        HMS_CATALOG_PROP_URI, HMS_CATALOG_PROP_WAREHOUSE, HmsCatalogBuilder,
    };

    let trimmed = location.trim();
    if trimmed.is_empty() {
        return Err("location must not be empty for TYPE hms".to_string());
    }
    // Spec form is `thrift://host:port`; the upstream builder
    // expects a bare `host:port` socket address (it calls
    // `to_socket_addrs()` directly). Strip the scheme so SQL users
    // can write the natural URL form.
    let address = trimmed.strip_prefix("thrift://").unwrap_or(trimmed);

    let warehouse = options
        .get("WAREHOUSE")
        .and_then(OptionValue::as_str)
        .ok_or_else(|| "option `WAREHOUSE` is required for TYPE hms".to_string())?;

    // AUTH_MODE / SECRET validation. The upstream builder doesn't
    // actually consume basic-auth creds today; we still enforce the
    // SQL contract so users aren't surprised at first thrift call.
    let auth_mode = options
        .get("AUTH_MODE")
        .and_then(OptionValue::as_str)
        .unwrap_or("none")
        .to_ascii_lowercase();
    match auth_mode.as_str() {
        "none" | "kerberos" => {
            // Ignore SECRET in these modes; SECRET only makes sense
            // for `plain`. We don't error to keep the option list
            // forward-compatible.
        }
        "plain" => {
            let secret_ref = options
                .get("SECRET")
                .and_then(OptionValue::as_secret_ref)
                .ok_or_else(|| {
                    "option `SECRET <name>` is required when `AUTH_MODE = plain` for TYPE hms"
                        .to_string()
                })?;
            let secret = secrets.get(secret_ref).map_err(|e| e.to_string())?;
            match &secret {
                Secret::Basic { .. } => {
                    // Fields will be threaded through once the
                    // upstream HMS builder grows SASL/PLAIN props.
                    // TODO(spec §4.4): once iceberg-catalog-hms
                    // accepts username/password props, plumb the
                    // basic creds into the props HashMap here.
                }
                other => {
                    return Err(format!(
                        "secret '{secret_ref}' is type {} but TYPE hms with AUTH_MODE plain expects basic",
                        other.type_name()
                    ));
                }
            }
        }
        other => {
            return Err(format!(
                "AUTH_MODE '{other}' is not supported (expected one of: none, kerberos, plain)"
            ));
        }
    }

    let mut props: HashMap<String, String> = HashMap::new();
    props.insert(HMS_CATALOG_PROP_URI.to_string(), address.to_string());
    props.insert(
        HMS_CATALOG_PROP_WAREHOUSE.to_string(),
        warehouse.to_string(),
    );

    let catalog = HmsCatalogBuilder::default()
        .load("sqe-attached-hms".to_string(), props)
        .await
        .map_err(|e| format!("Hive Metastore catalog open failed: {e}"))?;

    Ok(Arc::new(catalog))
}

#[cfg(not(feature = "hms"))]
async fn build_hms(
    _location: &str,
    _options: &BTreeMap<String, OptionValue>,
    _secrets: &SecretStore,
) -> Result<Arc<dyn iceberg::Catalog>, String> {
    Err("TYPE hms requires the `hms` cargo feature on sqe-catalog".to_string())
}

/// Build a JDBC-style SQL catalog (`iceberg_catalog_sql`).
///
/// Uses the same upstream builder as `TYPE sqlite` but accepts an arbitrary
/// database URL: PostgreSQL, MySQL, or SQLite. Required: `WAREHOUSE`. Optional:
/// `SECRET <name>` referencing `Secret::Basic { username, password }`.
///
/// WHY THE SECRET IS SPLICED INTO THE URL rather than passed as props: sqlx
/// parses credentials out of the connection URL itself and the catalog builder
/// exposes no separate username/password property, so a Basic secret has
/// nowhere else to go. A URL that already carries inline credentials is
/// overridden by the secret, because the secret is the managed source of truth
/// and silently preferring the inline copy would make rotation a no-op.
///
/// Spec form is `jdbc:postgresql://...` (the JDBC syntax familiar from Java);
/// sqlx expects `postgresql://...` without the prefix. The `jdbc:` prefix is
/// stripped when present so either form works in SQL.
#[cfg(any(feature = "sql", feature = "sql-postgres", feature = "sql-sqlite"))]
async fn build_jdbc(
    location: &str,
    options: &BTreeMap<String, OptionValue>,
    secrets: &SecretStore,
) -> Result<Arc<dyn iceberg::Catalog>, String> {
    use std::collections::HashMap;

    use iceberg::CatalogBuilder;
    use iceberg_catalog_sql::{
        SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalogBuilder,
    };

    let trimmed = location.trim();
    if trimmed.is_empty() {
        return Err("location must not be empty for TYPE jdbc".to_string());
    }
    let url_form = trimmed.strip_prefix("jdbc:").unwrap_or(trimmed);

    let warehouse = options
        .get("WAREHOUSE")
        .and_then(OptionValue::as_str)
        .ok_or_else(|| "option `WAREHOUSE` is required for TYPE jdbc".to_string())?;

    let final_url = if let Some(secret_ref) = options.get("SECRET").and_then(OptionValue::as_secret_ref)
    {
        let secret = secrets.get(secret_ref).map_err(|e| e.to_string())?;
        match &secret {
            Secret::Basic { username, password } => {
                splice_basic_into_url(url_form, username, password)?
            }
            other => {
                return Err(format!(
                    "secret '{secret_ref}' is type {} but TYPE jdbc expects basic",
                    other.type_name()
                ));
            }
        }
    } else {
        url_form.to_string()
    };

    let mut props: HashMap<String, String> = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), final_url);
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        warehouse.to_string(),
    );

    let catalog = SqlCatalogBuilder::default()
        .load("sqe-attached-jdbc".to_string(), props)
        .await
        .map_err(|e| format!("JDBC SQL catalog open failed: {e}"))?;

    Ok(Arc::new(catalog))
}

#[cfg(not(any(feature = "sql", feature = "sql-postgres", feature = "sql-sqlite")))]
async fn build_jdbc(
    _location: &str,
    _options: &BTreeMap<String, OptionValue>,
    _secrets: &SecretStore,
) -> Result<Arc<dyn iceberg::Catalog>, String> {
    Err(
        "TYPE jdbc requires the `sql`, `sql-postgres`, or `sql-sqlite` cargo feature \
         on sqe-catalog"
            .to_string(),
    )
}

/// Insert (or replace) a `user:password@` segment in a URL.
///
/// sqlx-compatible URLs look like `postgresql://user:pass@host/db`. Existing
/// credentials are replaced; otherwise the pair is inserted before the host.
///
/// The `@` search is deliberately conservative: a `/` before the `@` means the
/// `@` belongs to the path (or a query value), not to userinfo, so the URL is
/// left alone rather than mangled. Getting that wrong would corrupt a
/// connection string instead of failing loudly.
#[cfg(any(feature = "sql", feature = "sql-postgres", feature = "sql-sqlite"))]
fn splice_basic_into_url(url: &str, username: &str, password: &str) -> Result<String, String> {
    let scheme_end = url
        .find("://")
        .ok_or_else(|| format!("URL `{url}` is missing the `<scheme>://` prefix"))?;
    let scheme_with_sep = &url[..scheme_end + 3];
    let after_scheme = &url[scheme_end + 3..];

    let host_part = match after_scheme.find('@') {
        Some(at) if !after_scheme[..at].contains('/') => &after_scheme[at + 1..],
        _ => after_scheme,
    };

    let user_enc = url_encode_userinfo(username);
    let pass_enc = url_encode_userinfo(password);
    Ok(format!("{scheme_with_sep}{user_enc}:{pass_enc}@{host_part}"))
}

/// Percent-encode everything outside the unreserved set for the userinfo part
/// of a URL.
///
/// Encoding only the "obviously unsafe" characters would be the bug: a password
/// containing `@`, `:`, `/` or `?` silently truncates or redirects the
/// connection string. Unreserved characters pass through unchanged so
/// credentials that already work keep working.
#[cfg(any(feature = "sql", feature = "sql-postgres", feature = "sql-sqlite"))]
fn url_encode_userinfo(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(all(test, feature = "rest"))]
mod s3_option_tests {
    use std::collections::BTreeMap;

    use super::*;

    fn opt(s: &str) -> OptionValue {
        OptionValue::String(s.to_string())
    }

    #[test]
    fn s3_options_map_to_props() {
        let mut o = BTreeMap::new();
        o.insert("S3_ENDPOINT".to_string(), opt("http://localhost:19000"));
        o.insert("S3_REGION".to_string(), opt("us-east-1"));
        o.insert("S3_ACCESS_KEY".to_string(), opt("ak"));
        o.insert("S3_SECRET_KEY".to_string(), opt("sk"));
        o.insert("S3_PATH_STYLE".to_string(), opt("true"));
        let props: std::collections::HashMap<_, _> =
            s3_props_from_options(&o).into_iter().collect();
        assert_eq!(
            props.get("s3.endpoint").map(String::as_str),
            Some("http://localhost:19000")
        );
        assert_eq!(
            props.get("s3.region").map(String::as_str),
            Some("us-east-1")
        );
        assert_eq!(
            props.get("s3.access-key-id").map(String::as_str),
            Some("ak")
        );
        assert_eq!(
            props.get("s3.secret-access-key").map(String::as_str),
            Some("sk")
        );
        assert_eq!(
            props.get("s3.path-style-access").map(String::as_str),
            Some("true")
        );
    }

    #[test]
    fn absent_s3_options_yield_no_props() {
        let o: BTreeMap<String, OptionValue> = BTreeMap::new();
        assert!(s3_props_from_options(&o).is_empty());
    }
}

/// `splice_basic_into_url` writes credentials into a connection string, so a
/// mistake here either leaks them into the wrong field or silently corrupts the
/// URL. These cover the shapes a real `ATTACH ... TYPE jdbc` can hand it.
#[cfg(all(test, any(feature = "sql", feature = "sql-postgres", feature = "sql-sqlite")))]
mod jdbc_url_tests {
    use super::*;

    #[test]
    fn inserts_credentials_when_the_url_has_none() {
        assert_eq!(
            splice_basic_into_url("postgresql://db.example.com:5432/iceberg", "u", "p").unwrap(),
            "postgresql://u:p@db.example.com:5432/iceberg"
        );
    }

    /// The secret is the managed source of truth, so an inline pair is replaced
    /// rather than preferred. Preferring the inline copy would make rotating the
    /// secret a silent no-op.
    #[test]
    fn replaces_credentials_already_present() {
        assert_eq!(
            splice_basic_into_url("postgresql://old:stale@db:5432/ice", "new", "fresh").unwrap(),
            "postgresql://new:fresh@db:5432/ice"
        );
    }

    /// An `@` after a `/` belongs to the path, not to userinfo. Treating it as
    /// credentials would truncate the host and corrupt the connection string.
    #[test]
    fn an_at_sign_inside_the_path_is_not_mistaken_for_credentials() {
        assert_eq!(
            splice_basic_into_url("postgresql://db:5432/schema@weird", "u", "p").unwrap(),
            "postgresql://u:p@db:5432/schema@weird"
        );
    }

    /// Characters that delimit a URL must not survive raw in userinfo, or the
    /// password silently redirects or truncates the connection.
    #[test]
    fn url_delimiters_in_credentials_are_percent_encoded() {
        let out = splice_basic_into_url("mysql://h/db", "user@corp", "p@ss:w/rd?x").unwrap();
        assert_eq!(out, "mysql://user%40corp:p%40ss%3Aw%2Frd%3Fx@h/db");
        // The raw forms must be gone, or a parser would split on them.
        assert!(!out.contains("user@corp"));
        assert!(!out.contains("p@ss"));
    }

    #[test]
    fn unreserved_characters_pass_through_unchanged() {
        assert_eq!(url_encode_userinfo("aZ09-_.~"), "aZ09-_.~");
    }

    #[test]
    fn non_ascii_is_encoded_per_utf8_byte() {
        // 'é' is two UTF-8 bytes; both must be encoded, not the char.
        assert_eq!(url_encode_userinfo("é"), "%C3%A9");
    }

    #[test]
    fn a_url_without_a_scheme_is_an_error_not_a_guess() {
        let err = splice_basic_into_url("db.example.com/iceberg", "u", "p").unwrap_err();
        assert!(err.contains("scheme"), "unexpected message: {err}");
    }
}
