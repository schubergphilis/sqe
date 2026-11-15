use serde::Deserialize;

use super::{default_true, host_is_loopback, ProtectedLocations};

/// Security policy for table-valued-function path arguments.
///
/// All defaults are fail-closed: cloud object stores (`s3://`, `abfss://`,
/// `gs://`, `hf://`) keep working out of the box; local paths and arbitrary
/// `http(s)://` hosts are rejected unless explicitly enabled.
#[derive(Debug, Deserialize, Clone)]
pub struct TvfPolicy {
    /// When `true`, file TVFs may read absolute paths on the coordinator /
    /// worker filesystem (e.g. `/var/data/foo.parquet`). Default: `false`
    /// because an authenticated user could read `/etc/shadow`, a mounted
    /// secret, or `/var/run/secrets/kubernetes.io/serviceaccount/token`.
    /// Enable on single-tenant deployments where every user is trusted to
    /// read the local filesystem (rare; production deployments should
    /// stage files in an object store instead).
    #[serde(default)]
    pub allow_local_paths: bool,
    /// When `true`, file TVFs may fetch from arbitrary `http(s)://` hosts.
    /// Default: `false` because IMDS lives at `http://169.254.169.254`
    /// and worker pods often run with cloud-side privileges higher than
    /// the user's. When `false`, only `allowed_http_hosts` are reachable
    /// (empty list = no HTTP at all).
    #[serde(default)]
    pub allow_http: bool,
    /// When `allow_http` is `false`, this allowlist names the hosts that
    /// file TVFs may reach. Compared case-insensitively against the URL's
    /// host (no port, no path). Examples: `["data.example.com",
    /// "huggingface.co"]`. Wildcards are not supported; add each fully-
    /// qualified host explicitly.
    #[serde(default)]
    pub allowed_http_hosts: Vec<String>,
    /// Object-store URL prefixes that file TVFs may read **with the
    /// engine's own storage credentials**. Default: empty = DENY all
    /// engine-credentialed object-store TVF reads. Without this gate any
    /// authenticated user could `read_csv('s3://<any-bucket>/<any-key>')`
    /// and the engine would fetch it with its static S3 key — a complete
    /// bypass of the catalog authorization plane (Polaris/OPA), which only
    /// covers catalog tables.
    ///
    /// Entries are compared on URL-path-segment boundaries (so
    /// `"s3://staging"` does NOT match `s3://staging-evil/...`) after
    /// scheme canonicalisation (`s3a://`→`s3://`, `abfs://`→`abfss://`,
    /// `az://`→`azure://`, `gcs://`→`gs://`). The literal placeholder
    /// `{user}` expands to the authenticated username, enabling per-user
    /// staging areas:
    ///
    /// ```toml
    /// [storage.tvf]
    /// allowed_object_store_prefixes = [
    ///   "s3://data-platform-staging/_table-load-staging/",
    ///   "s3://notebook-scratch/{user}/",
    ///   "s3://staging/ingest/ws/{catalog}/",
    /// ]
    /// ```
    ///
    /// `{catalog}` expands to the session's catalog (the one it names, or the
    /// config default), and only when that catalog answered the caller's own
    /// bearer while the session was built: a catalog reached with
    /// engine-held auth, or merely named, does not expand it. A discovered
    /// (`polaris-auto`) catalog counts. Without such a catalog the entry is
    /// dropped, never matched with an empty segment. Names are matched
    /// exactly (case-sensitive) and must be `[A-Za-z0-9._@+-]` without `..`.
    /// Both placeholders may appear in one entry.
    ///
    /// Paths whose TVF call carries complete *inline* credentials
    /// (`access_key` + `secret_key`, an Azure key/SAS token, or an inline
    /// GCS service-account key) bypass this list: the engine's storage key
    /// is not used, so the object store itself enforces access.
    #[serde(default)]
    pub allowed_object_store_prefixes: Vec<String>,
    /// Roles (matched case-insensitively against the caller's JWT roles)
    /// that may read **any** object-store path with the engine's storage
    /// credentials, bypassing `allowed_object_store_prefixes`. Default:
    /// empty = no role-based override. Intended for platform
    /// administrators / service identities that own the storage anyway.
    #[serde(default)]
    pub object_store_admin_roles: Vec<String>,
    /// When `true` (default), a TVF call carrying complete inline credentials
    /// (`access_key`+`secret_key`, an Azure key/SAS token, or an inline GCS
    /// service-account key) bypasses `allowed_object_store_prefixes` entirely:
    /// the engine's storage key is not used, so the object store enforces
    /// access with the caller-supplied credential.
    ///
    /// Set to `false` (SEC-04) to disable that bypass. Inline-credentialed
    /// reads then still have to satisfy the normal gate
    /// (`allowed_object_store_prefixes`, `object_store_admin_roles`, or a
    /// trusted-local caller), so a multi-tenant coordinator cannot be turned
    /// into an arbitrary fetcher by any user who passes their own keys.
    /// Production deployments that do not intend users to bring their own
    /// object-store credentials should set this `false`.
    #[serde(default = "default_true")]
    pub allow_inline_credentials: bool,
    /// When `true`, every object-store path is readable with the engine's
    /// storage credentials, bypassing `allowed_object_store_prefixes`
    /// entirely. Default `false` (fail-closed). This is the explicit
    /// "allow all buckets" switch: convenient for a single-tenant laptop or
    /// a trusted-network read gateway where the engine's credentials already
    /// scope what is reachable, and dangerous on a multi-tenant coordinator,
    /// where it lets any caller read anything those credentials can. The
    /// startup log warns when it is on.
    #[serde(default)]
    pub allow_all_object_stores: bool,
    /// Locations no file TVF reads with the engine's credentials, whatever
    /// the settings above say: Hive table data (#886). Not configurable
    /// here; it is the process-wide registry unless a test injects one.
    #[serde(skip, default = "ProtectedLocations::global")]
    pub protected: std::sync::Arc<ProtectedLocations>,
}

impl Default for TvfPolicy {
    fn default() -> Self {
        // Matches the serde defaults (fail-closed for local/http, deny-all for
        // engine-credentialed object stores, inline-credential bypass enabled
        // for back-compat). A manual impl is required because `allow_inline_
        // credentials` defaults to `true`, which `#[derive(Default)]` cannot
        // express for a `bool`.
        Self {
            allow_local_paths: false,
            allow_http: false,
            allowed_http_hosts: Vec::new(),
            allowed_object_store_prefixes: Vec::new(),
            object_store_admin_roles: Vec::new(),
            allow_inline_credentials: true,
            allow_all_object_stores: false,
            protected: ProtectedLocations::global(),
        }
    }
}

/// Identity of the caller invoking a file TVF, resolved at session-context
/// build time from the authenticated session. Drives
/// [`TvfPolicy::check_path`]'s object-store gating.
#[derive(Debug, Clone, Default)]
pub struct TvfCaller {
    /// Authenticated username (JWT `user_claim`). `None` for callers with
    /// no resolvable identity — `{user}` prefixes never match for them.
    pub username: Option<String>,
    /// Roles from the caller's JWT (`roles_claim`), matched against
    /// `[storage.tvf] object_store_admin_roles`.
    pub roles: Vec<String>,
    /// `true` for a caller that already owns whatever it is reading, so
    /// object-store prefix gating is skipped entirely: the embedded CLI
    /// (`trusted_local`) and coordinator startup validating its own
    /// operator-supplied TOML (`trusted_startup_config`). The coordinator
    /// must NEVER set this for a remote session's query or DDL -- both
    /// constructors below exist so a grep for `trusted: true` finds only
    /// those two non-session call sites, never one that could be confused
    /// with `for_user`.
    ///
    /// This field is `pub`, like the rest of the struct, so nothing at the
    /// type level stops a struct literal from setting it directly and
    /// bypassing both named constructors. The two constructors are not an
    /// access-control boundary; they are the reviewable surface. A caller
    /// site that writes `trusted: true` outside `trusted_local()` or
    /// `trusted_startup_config()` is the thing to look for in review, and
    /// grepping the token `trusted: true` (or either constructor's name)
    /// finds every place that ever sets it -- there is no other way to.
    pub trusted: bool,
    /// The catalog this caller's session is scoped to, for denial messages.
    /// `None` outside a catalog-scoped session.
    pub scope_catalog: Option<String>,
    /// That catalog's `[catalogs.<name>] tvf_prefixes`: allowed on top of
    /// the global `allowed_object_store_prefixes`, for this caller only.
    pub catalog_prefixes: Vec<String>,
    /// The session catalog, set only when it answered this caller's own
    /// bearer while the session was built. The `{catalog}` placeholder
    /// expands to it; with `None`, entries using `{catalog}` are dropped.
    /// Separate from `scope_catalog`, which any session can set by naming a
    /// catalog.
    pub catalog_with_access: Option<String>,
}

impl TvfCaller {
    /// Caller identity for an authenticated remote session.
    pub fn for_user(username: String, roles: Vec<String>) -> Self {
        Self {
            username: Some(username),
            roles,
            trusted: false,
            scope_catalog: None,
            catalog_prefixes: Vec::new(),
            catalog_with_access: None,
        }
    }

    /// Scope this caller to `catalog`, whose `tvf_prefixes` it may then read
    /// in addition to the global list. A session scoped to another catalog
    /// never sees them.
    pub fn with_catalog_scope(mut self, catalog: &str, prefixes: &[String]) -> Self {
        self.scope_catalog = Some(catalog.to_string());
        self.catalog_prefixes = prefixes.to_vec();
        self
    }

    /// Record that the scoped catalog answered this caller's own bearer, so
    /// `{catalog}` expands to its name. Call only with that proof in hand:
    /// naming a catalog is not access to it.
    pub fn with_proven_catalog_access(mut self) -> Self {
        self.catalog_with_access = self.scope_catalog.clone();
        self
    }

    /// Trusted local caller (embedded CLI). Object-store prefix gating is
    /// skipped — the local user owns the config and the credentials in it.
    pub fn trusted_local() -> Self {
        Self {
            username: None,
            roles: Vec::new(),
            trusted: true,
            scope_catalog: None,
            catalog_prefixes: Vec::new(),
            catalog_with_access: None,
        }
    }

    /// Trusted caller representing OPERATOR-AUTHORED CONFIGURATION,
    /// nothing else. Used exactly once in this codebase: coordinator
    /// startup, validating a `[[hive.roots]]` entry from the TOML file the
    /// operator deployed, before any session or request exists to build a
    /// `for_user` caller from.
    ///
    /// MUST NEVER be constructed for anything that carries a user
    /// identity, or on any path reachable from a request a caller sent --
    /// an authenticated session's ATTACH, a TVF call, a query. Those go
    /// through `for_user`, always. This constructor bypassing
    /// object-store prefix gating is only safe because its one call site
    /// runs before the coordinator accepts any connection; the same
    /// bypass on a session path would let an authenticated-but-otherwise-
    /// unprivileged caller read any `s3://`/`gs://`/`abfss://` location
    /// the engine's own credentials can reach.
    ///
    /// Same effect as `trusted_local` -- object-store prefix gating is
    /// skipped, local/http paths still need `[storage.tvf]
    /// allow_local_paths` / `allow_http` regardless of `trusted` (see
    /// `check_path`) -- but kept as its own name rather than reused from
    /// the embedded-CLI constructor: an operator's TOML and a
    /// single-tenant local process are trusted for different reasons, and
    /// conflating them would make a future audit of "where is `trusted`
    /// set" have to reason about both at once.
    pub fn trusted_startup_config() -> Self {
        Self {
            username: None,
            roles: Vec::new(),
            trusted: true,
            scope_catalog: None,
            catalog_prefixes: Vec::new(),
            catalog_with_access: None,
        }
    }
}

/// Canonicalise an object-store URL for prefix comparison: scheme aliases
/// collapse to one spelling, and the scheme is lowercased. Any path segment
/// that could be re-interpreted as a `.`/`..` traversal or a hidden segment
/// boundary by a downstream URL/HTTP layer AFTER this prefix check is
/// rejected outright: a literal `.`/`..`, the percent-encodings of `.`
/// (`%2e`) and `/` (`%2f`), and double-encoding (`%25…`). We reject rather
/// than decode-and-renormalise because object_store / reqwest decoding
/// behaviour varies per backend, so the only sound stance is to refuse
/// anything ambiguous.
pub(super) fn canonicalize_object_url(path: &str) -> Result<String, String> {
    const ALIASES: &[(&str, &str)] = &[
        ("s3a://", "s3://"),
        ("abfs://", "abfss://"),
        ("az://", "azure://"),
        ("gcs://", "gs://"),
    ];
    let lower = path.to_lowercase();
    let mut out = path.to_string();
    for (from, to) in ALIASES {
        if lower.starts_with(from) {
            // Don't rewrite `abfss://` via the `abfs://` alias.
            if *from == "abfs://" && lower.starts_with("abfss://") {
                continue;
            }
            out = format!("{to}{}", &path[from.len()..]);
            break;
        }
    }
    if let Some(idx) = out.find("://") {
        let (scheme, rest) = out.split_at(idx);
        out = format!("{}{}", scheme.to_ascii_lowercase(), rest);
    }
    let after_scheme = out.split_once("://").map(|x| x.1).unwrap_or("");
    for seg in after_scheme.split('/') {
        let lower = seg.to_ascii_lowercase();
        // A percent-encoded slash (`%2f`) or double-encoding (`%25…`) could
        // smuggle an extra segment boundary or a dot segment past this
        // check; reject any segment carrying them.
        if lower.contains("%2f") || lower.contains("%25") {
            return Err(format!(
                "TVF: object-store path '{path}' contains a percent-encoded \
                 slash or double-encoding in a path segment, which is not allowed"
            ));
        }
        let decoded = lower.replace("%2e", ".");
        if decoded == "." || decoded == ".." {
            return Err(format!(
                "TVF: object-store path '{path}' contains a dot path segment \
                 ('.' / '..'), which is not allowed"
            ));
        }
    }
    Ok(out)
}

/// `true` for the object-store schemes the TVF gate reads with the
/// engine's credentials (aliases included).
pub(super) fn is_object_store_url(path: &str) -> bool {
    const SCHEMES: &[&str] = &[
        "s3://", "s3a://", "abfss://", "abfs://", "azure://", "az://", "gs://", "gcs://",
    ];
    let lower = path.to_ascii_lowercase();
    SCHEMES.iter().any(|s| lower.starts_with(s))
}

/// A `tvf_prefixes` entry must be an object-store URL with a bucket that
/// survives [`canonicalize_object_url`]; anything else would never match.
pub(super) fn validate_tvf_prefix(prefix: &str) -> Result<(), String> {
    const SCHEMES: &[&str] = &[
        "s3://", "s3a://", "abfss://", "abfs://", "azure://", "az://", "gs://", "gcs://",
    ];
    let lower = prefix.to_ascii_lowercase();
    let Some(scheme) = SCHEMES.iter().find(|s| lower.starts_with(**s)) else {
        return Err(format!(
            "'{prefix}' is not an object-store URL (s3://, abfss://, gs://, ...)"
        ));
    };
    if prefix[scheme.len()..].trim_matches('/').is_empty() {
        return Err(format!("'{prefix}' names no bucket"));
    }
    canonicalize_object_url(prefix).map(|_| ())
}

/// `true` when `path` falls under `prefix` on a path-segment boundary.
/// `s3://staging` matches `s3://staging` and `s3://staging/x` but NOT
/// `s3://staging-evil/x`.
pub(super) fn object_prefix_matches(path: &str, prefix: &str) -> bool {
    if prefix.is_empty() {
        return false;
    }
    match path.strip_prefix(prefix) {
        Some(rest) => prefix.ends_with('/') || rest.is_empty() || rest.starts_with('/'),
        None => false,
    }
}

/// Expand the `{user}` and `{catalog}` placeholders in a configured prefix.
/// Returns `None` when the prefix needs a value that is absent or unsafe in a
/// URL (anything outside `[A-Za-z0-9._@+-]`, or only dots; a catalog name
/// may not contain `..` either), so the entry is dropped, never widened to
/// an empty segment.
pub(super) fn substitute_placeholders(
    prefix: &str,
    username: Option<&str>,
    catalog: Option<&str>,
) -> Option<String> {
    fn url_safe(value: &str) -> bool {
        !value.is_empty()
            && !value.chars().all(|c| c == '.')
            && value
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '@' | '+'))
    }
    let mut out = prefix.to_string();
    if out.contains("{user}") {
        let user = username.filter(|u| url_safe(u))?;
        out = out.replace("{user}", user);
    }
    if out.contains("{catalog}") {
        let catalog = catalog.filter(|c| url_safe(c) && !c.contains(".."))?;
        out = out.replace("{catalog}", catalog);
    }
    Some(out)
}

impl TvfPolicy {
    /// Return `Ok(())` if a TVF invoked by `caller` may reference `path`,
    /// or an error describing why it cannot.
    ///
    /// - Object-store schemes (`s3://`, `s3a://`, `abfss://`, `abfs://`,
    ///   `azure://`, `az://`, `gs://`, `gcs://`) use the engine's own
    ///   storage credentials, so they are gated per caller identity:
    ///   allowed only under `allowed_object_store_prefixes` (with `{user}`
    ///   and `{catalog}` substitution), for a role in `object_store_admin_roles`, for a
    ///   trusted local caller, or when the call carries complete inline
    ///   credentials (`inline_credentials = true`). Default: DENY.
    /// - `hf://` resolves to anonymous HTTPS downloads from
    ///   huggingface.co; no engine storage credentials are involved, so it
    ///   stays allowed.
    /// - Local paths (`/...`, `file://`, no scheme) need
    ///   `allow_local_paths = true`.
    /// - `http(s)://` needs either `allow_http = true` or an exact match
    ///   in `allowed_http_hosts`.
    pub fn check_path(
        &self,
        path: &str,
        caller: &TvfCaller,
        inline_credentials: bool,
    ) -> Result<(), String> {
        let lower = path.to_lowercase();
        if is_object_store_url(path) {
            return self.check_object_store(path, caller, inline_credentials);
        }
        if lower.starts_with("hf://") {
            // Resolved upstream to https://huggingface.co downloads; the
            // engine's storage credentials are never attached.
            return Ok(());
        }

        if lower.starts_with("http://") || lower.starts_with("https://") {
            return self.check_http_destination(path);
        }

        // Everything else — bare path, `file://`, `/...` — is a local path.
        if self.allow_local_paths {
            return Ok(());
        }
        Err(format!(
            "TVF: local filesystem paths are disabled. \
             Path '{path}' is not an object-store URL (s3://, abfss://, gs://, hf://, ...). \
             Set `[storage.tvf] allow_local_paths = true` to permit local reads, \
             or stage the file in an object store."
        ))
    }

    /// Identity-aware gate for engine-credentialed object-store TVF reads.
    /// Fail-closed: with no configuration, every `s3://` / `abfss://` /
    /// `gs://` (and alias-scheme) path is denied unless the call carries
    /// complete inline credentials or the caller is trusted-local.
    fn check_object_store(
        &self,
        path: &str,
        caller: &TvfCaller,
        inline_credentials: bool,
    ) -> Result<(), String> {
        if caller.trusted || (inline_credentials && self.allow_inline_credentials) {
            return Ok(());
        }
        // Hive table data is refused before any setting below can open it.
        self.refuse_protected(path, caller)?;
        self.check_object_store_allowlist(path, caller)
    }

    /// `true` when `caller` holds a role in `object_store_admin_roles`.
    pub(super) fn is_object_store_admin(&self, caller: &TvfCaller) -> bool {
        !self.object_store_admin_roles.is_empty()
            && caller.roles.iter().any(|r| {
                self.object_store_admin_roles
                    .iter()
                    .any(|a| a.eq_ignore_ascii_case(r))
            })
    }

    /// The allow-list half of [`Self::check_object_store`], without the
    /// Hive refusal: the Hive gate's fallback uses it, so a table SQE has
    /// loaded is not refused by its own registry entry.
    pub(super) fn check_object_store_allowlist(
        &self,
        path: &str,
        caller: &TvfCaller,
    ) -> Result<(), String> {
        if caller.trusted {
            return Ok(());
        }
        // Explicit allow-all: the operator has taken responsibility for the
        // boundary (single tenant, or the engine's credentials already scope
        // what is reachable). Startup logs a warning when this is set.
        if self.allow_all_object_stores || self.is_object_store_admin(caller) {
            return Ok(());
        }

        let principal = caller.username.as_deref().unwrap_or("<anonymous>");
        let canon = match canonicalize_object_url(path) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(
                    principal = %principal,
                    path = %path,
                    "TVF object-store access denied: {e}"
                );
                return Err(e);
            }
        };
        // The session catalog's own prefixes widen the global list for this
        // caller only; a session scoped to another catalog never carries them.
        for raw_prefix in self
            .allowed_object_store_prefixes
            .iter()
            .chain(&caller.catalog_prefixes)
        {
            let Some(substituted) = substitute_placeholders(
                raw_prefix,
                caller.username.as_deref(),
                caller.catalog_with_access.as_deref(),
            ) else {
                continue;
            };
            // A misconfigured prefix (e.g. containing dot segments) is
            // skipped rather than widened.
            let Ok(canon_prefix) = canonicalize_object_url(&substituted) else {
                continue;
            };
            if object_prefix_matches(&canon, &canon_prefix) {
                return Ok(());
            }
        }

        tracing::warn!(
            principal = %principal,
            path = %path,
            scope_catalog = caller.scope_catalog.as_deref().unwrap_or(""),
            "TVF object-store access denied: no matching \
             `[storage.tvf] allowed_object_store_prefixes` or catalog \
             `tvf_prefixes` entry"
        );
        let catalog_hint = match caller.scope_catalog.as_deref() {
            Some(catalog) => format!(
                " This session is scoped to catalog '{catalog}', whose \
                 `[catalogs.{catalog}] tvf_prefixes` add {} more.",
                caller.catalog_prefixes.len()
            ),
            None => String::new(),
        };
        let inline_hint = if self.allow_inline_credentials {
            ", or pass complete inline credentials (e.g. access_key/secret_key) \
             so the engine's storage key is not used"
        } else {
            " (inline credentials do not bypass this gate: \
             `[storage.tvf] allow_inline_credentials = false`)"
        };
        Err(format!(
            "TVF: object-store path '{path}' denied for user '{principal}'. \
             Reads with the engine's storage credentials are limited to \
             `[storage.tvf] allowed_object_store_prefixes` ({} configured).{catalog_hint} \
             Add a matching prefix (the `{{user}}` placeholder expands to the \
             authenticated username, `{{catalog}}` to the session catalog when \
             it answered your own credentials), grant a role listed in \
             `[storage.tvf] object_store_admin_roles`{inline_hint}.",
            self.allowed_object_store_prefixes.len()
        ))
    }

    /// Validate an S3-style endpoint override (the `endpoint =>` argument
    /// on `read_parquet` / `read_csv` / `read_json`). Issue #46 closed
    /// the SSRF gap left by issue #10: the path argument was checked but
    /// the endpoint override flowed straight into `AmazonS3Builder`, so
    /// `read_parquet('s3://x/y', endpoint => 'http://169.254.169.254/...')`
    /// still pivoted to IMDS.
    ///
    /// Empty endpoints are allowed (the operator's storage config kicks
    /// in). HTTP/HTTPS endpoints are validated against the same allowlist
    /// as path arguments. Bare-host endpoints (e.g. `minio.local:9000`)
    /// are allowed since they cannot reach IMDS over its expected scheme.
    pub fn check_endpoint(&self, endpoint: &str) -> Result<(), String> {
        if endpoint.is_empty() {
            return Ok(());
        }
        let lower = endpoint.to_lowercase();
        if !lower.starts_with("http://") && !lower.starts_with("https://") {
            return Ok(());
        }
        self.check_http_destination(endpoint)
    }

    /// Parse destinations exactly as the HTTP client does. Userinfo is never
    /// meaningful for file readers and must not masquerade as an allowed host.
    fn check_http_destination(&self, destination: &str) -> Result<(), String> {
        let authority = destination
            .split_once("://")
            .map(|(_, rest)| rest)
            .unwrap_or("");
        if authority.is_empty() || authority.starts_with('/') || destination.contains('\\') {
            return Err(format!("TVF: malformed URL '{destination}'"));
        }
        let parsed = url::Url::parse(destination)
            .map_err(|_| format!("TVF: malformed URL '{destination}'"))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err("TVF: HTTP destination requires http or https".to_string());
        }
        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err("TVF: HTTP destination must not contain userinfo".to_string());
        }
        let host = parsed
            .host_str()
            .ok_or_else(|| "TVF: malformed URL (missing host)".to_string())?;
        if self.allow_http {
            return Ok(());
        }
        if !self
            .allowed_http_hosts
            .iter()
            .any(|h| h.eq_ignore_ascii_case(host))
        {
            return Err(format!(
                "TVF: HTTP host '{host}' is not in `[storage.tvf] allowed_http_hosts`"
            ));
        }
        // Finding 753: an allow-listed *name* can be rebound by its DNS owner
        // to IMDS or loopback after the operator vetted it. A literal IP or a
        // loopback name is the operator naming the address itself, so only
        // hostnames are resolved. TOCTOU remains: the HTTP client resolves
        // again, and a fast-flux record can differ between the two lookups.
        if host_is_loopback(host)
            || host
                .trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .is_ok()
        {
            return Ok(());
        }
        // ponytail: blocking resolver on the planning path; async lookup if it shows up in traces.
        let port = parsed.port_or_known_default().unwrap_or(80);
        // A lookup failure is not rebinding evidence (no address to reach);
        // the HTTP client reports it on its own.
        let addrs: Vec<std::net::IpAddr> = std::net::ToSocketAddrs::to_socket_addrs(&(host, port))
            .map(|it| it.map(|a| a.ip()).collect())
            .unwrap_or_default();
        match self.rebinding_target(&addrs) {
            None => Ok(()),
            Some(ip) => Err(format!(
                "TVF: HTTP host '{host}' resolves to {ip}, a link-local, loopback or \
                 unspecified address; allow-list the literal IP instead if intended"
            )),
        }
    }

    /// The first resolved address that must not be reached through an
    /// allow-listed hostname (finding 753) and that the operator did not also
    /// allow-list as a literal. Pure so the resolver can be bypassed in tests.
    fn rebinding_target(&self, addrs: &[std::net::IpAddr]) -> Option<std::net::IpAddr> {
        const IMDS_V6: std::net::Ipv6Addr =
            std::net::Ipv6Addr::new(0xfd00, 0x0ec2, 0, 0, 0, 0, 0, 0x254);
        let blocked = |ip: &std::net::IpAddr| match ip {
            std::net::IpAddr::V4(v4) => {
                v4.is_loopback() || v4.is_link_local() || v4.is_unspecified()
            }
            std::net::IpAddr::V6(v6) => {
                v6.is_loopback()
                    || v6.is_unspecified()
                    || (v6.segments()[0] & 0xffc0) == 0xfe80
                    || *v6 == IMDS_V6
            }
        };
        let listed = |ip: &std::net::IpAddr| {
            self.allowed_http_hosts
                .iter()
                .any(|h| h.trim_matches(['[', ']']).parse::<std::net::IpAddr>() == Ok(*ip))
        };
        addrs.iter().copied().find(|ip| blocked(ip) && !listed(ip))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StorageConfig;

    // --- TvfPolicy (issue #10 regression tests) ---

    #[test]
    fn tvf_object_store_config_deserializes_from_toml() {
        #[derive(Deserialize)]
        struct Wrap {
            storage: StorageConfig,
        }
        let toml = r#"
            [storage]
            s3_endpoint = "http://s3:9000"

            [storage.tvf]
            allowed_object_store_prefixes = [
                "s3://data-platform-staging/_table-load-staging/",
                "s3://scratch/{user}/",
            ]
            object_store_admin_roles = ["sqe-storage-admin"]
        "#;
        let w: Wrap = toml::from_str(toml).expect("[storage.tvf] TOML deserializes");
        assert_eq!(w.storage.tvf.allowed_object_store_prefixes.len(), 2);
        assert_eq!(
            w.storage.tvf.object_store_admin_roles,
            vec!["sqe-storage-admin".to_string()]
        );
        // Untouched defaults stay fail-closed.
        assert!(!w.storage.tvf.allow_local_paths);
        assert!(!w.storage.tvf.allow_http);
    }

    /// Plain authenticated remote caller (no roles).
    fn caller(name: &str) -> TvfCaller {
        TvfCaller::for_user(name.to_string(), Vec::new())
    }

    /// Shorthand: check `path` as a default remote caller without inline
    /// credentials.
    fn check_remote(policy: &TvfPolicy, path: &str) -> Result<(), String> {
        policy.check_path(path, &caller("alice"), false)
    }

    #[test]
    fn tvf_default_denies_object_store_schemes_for_remote_callers() {
        // Identity-aware gate: with no `allowed_object_store_prefixes`,
        // engine-credentialed object-store reads are denied out of the box.
        let policy = TvfPolicy::default();
        for path in [
            "s3://my-bucket/data.parquet",
            "s3a://my-bucket/data.parquet",
            "abfss://c@a.dfs.core.windows.net/x",
            "abfs://c@a.dfs.core.windows.net/x",
            "azure://container/x",
            "az://container/x",
            "gs://bucket/x",
            "gcs://bucket/x",
        ] {
            let err = check_remote(&policy, path).unwrap_err();
            assert!(
                err.contains("allowed_object_store_prefixes"),
                "expected deny naming the config for {path}, got: {err}"
            );
        }
        // hf:// stays open — no engine storage credentials involved.
        assert!(check_remote(&policy, "hf://datasets/foo/bar/x").is_ok());
    }

    /// A prefix allowed for catalog A is rejected for a caller scoped to
    /// catalog B, and the global list still applies to both.
    #[test]
    fn tvf_catalog_prefixes_apply_only_to_their_catalog() {
        let policy = TvfPolicy {
            allowed_object_store_prefixes: vec!["s3://shared-landing/".to_string()],
            ..TvfPolicy::default()
        };
        let a_prefixes = vec!["s3://ws-a-landing/".to_string()];
        let in_a = caller("alice").with_catalog_scope("ws_a", &a_prefixes);
        let in_b = caller("alice").with_catalog_scope("ws_b", &["s3://ws-b-landing/".to_string()]);

        policy
            .check_path("s3://ws-a-landing/2026/x.parquet", &in_a, false)
            .expect("catalog A's prefix in a session scoped to A");
        let err = policy
            .check_path("s3://ws-a-landing/2026/x.parquet", &in_b, false)
            .unwrap_err();
        assert!(
            err.contains("scoped to catalog 'ws_b'")
                && err.contains("[catalogs.ws_b] tvf_prefixes"),
            "denial names the session catalog: {err}"
        );
        // Unscoped callers see the global list only.
        assert!(policy
            .check_path("s3://ws-a-landing/2026/x.parquet", &caller("alice"), false)
            .is_err());
        // The global list holds in every scope.
        for c in [&in_a, &in_b] {
            policy
                .check_path("s3://shared-landing/x.csv", c, false)
                .expect("global prefix");
        }
        // Same segment-boundary matching as the global list.
        assert!(policy
            .check_path("s3://ws-a-landing-evil/x.parquet", &in_a, false)
            .is_err());
    }

    #[test]
    fn tvf_catalog_prefixes_expand_the_user_placeholder() {
        let policy = TvfPolicy::default();
        let scoped = |user: &str| {
            caller(user).with_catalog_scope("ws_a", &["s3://ws-a-scratch/{user}/".to_string()])
        };
        policy
            .check_path("s3://ws-a-scratch/alice/x.csv", &scoped("alice"), false)
            .expect("own scratch area");
        assert!(policy
            .check_path("s3://ws-a-scratch/alice/x.csv", &scoped("bob"), false)
            .is_err());
    }

    fn catalog_staging_policy() -> TvfPolicy {
        TvfPolicy {
            allowed_object_store_prefixes: vec!["s3://staging/ingest/ws/{catalog}/".to_string()],
            ..TvfPolicy::default()
        }
    }

    /// `{catalog}` expands to the session catalog only once its access is
    /// proven, and is matched exactly.
    #[test]
    fn tvf_catalog_placeholder_expands_to_a_proven_session_catalog() {
        let policy = catalog_staging_policy();
        let in_ws1 = caller("alice")
            .with_catalog_scope("ws1", &[])
            .with_proven_catalog_access();
        policy
            .check_path("s3://staging/ingest/ws/ws1/x.parquet", &in_ws1, false)
            .expect("own workspace staging");
        for other in [
            "s3://staging/ingest/ws/ws2/x.parquet",
            "s3://staging/ingest/ws/WS1/x.parquet",
            "s3://staging/ingest/ws/ws1-evil/x.parquet",
            "s3://staging/ingest/ws//x.parquet",
        ] {
            let err = policy.check_path(other, &in_ws1, false).unwrap_err();
            assert!(
                err.contains("allowed_object_store_prefixes"),
                "{other}: {err}"
            );
        }
    }

    /// A caller with no session catalog, or one that only named it, gets
    /// nothing from a `{catalog}` entry: it is dropped, never matched as an
    /// empty segment.
    #[test]
    fn tvf_catalog_placeholder_needs_proven_access() {
        let policy = catalog_staging_policy();
        let named_only = caller("alice").with_catalog_scope("ws1", &[]);
        for c in [&caller("alice"), &named_only] {
            for path in [
                "s3://staging/ingest/ws/ws1/x.parquet",
                "s3://staging/ingest/ws//x.parquet",
                "s3://staging/ingest/ws/x.parquet",
            ] {
                let err = policy.check_path(path, c, false).unwrap_err();
                assert!(err.contains("allowed_object_store_prefixes"), "{err}");
            }
        }
    }

    /// A catalog name that could change the URL shape never expands.
    #[test]
    fn tvf_catalog_placeholder_rejects_unsafe_names() {
        let policy = catalog_staging_policy();
        for name in ["ws1/../ws2", "a/b", "..", ".", "a..b", "", "ws 1", "ws%2f1"] {
            let c = caller("alice")
                .with_catalog_scope(name, &[])
                .with_proven_catalog_access();
            for path in [
                format!("s3://staging/ingest/ws/{name}/x.parquet"),
                "s3://staging/ingest/ws/b/x.parquet".to_string(),
                "s3://staging/ingest/ws2/x.parquet".to_string(),
            ] {
                assert!(
                    policy.check_path(&path, &c, false).is_err(),
                    "catalog '{name}' opened {path}"
                );
            }
        }
        assert_eq!(
            substitute_placeholders("s3://s/{catalog}/", None, Some("a..b")),
            None
        );
        assert_eq!(
            substitute_placeholders("s3://s/{catalog}/", None, Some("a/b")),
            None
        );
        assert_eq!(
            substitute_placeholders("s3://s/{catalog}/", None, None),
            None
        );
        assert_eq!(
            substitute_placeholders("s3://s/{catalog}/", None, Some("ws.prod-1")),
            Some("s3://s/ws.prod-1/".to_string())
        );
    }

    /// `{user}` and `{catalog}` combine; both must resolve.
    #[test]
    fn tvf_catalog_and_user_placeholders_combine() {
        let policy = TvfPolicy {
            allowed_object_store_prefixes: vec!["s3://scratch/{catalog}/{user}/".to_string()],
            ..TvfPolicy::default()
        };
        let alice_ws1 = caller("alice")
            .with_catalog_scope("ws1", &[])
            .with_proven_catalog_access();
        policy
            .check_path("s3://scratch/ws1/alice/x.csv", &alice_ws1, false)
            .expect("own area in own workspace");
        for path in ["s3://scratch/ws1/bob/x.csv", "s3://scratch/ws2/alice/x.csv"] {
            assert!(
                policy.check_path(path, &alice_ws1, false).is_err(),
                "{path}"
            );
        }
        let anonymous = TvfCaller {
            username: None,
            ..alice_ws1.clone()
        };
        assert!(policy
            .check_path("s3://scratch/ws1/alice/x.csv", &anonymous, false)
            .is_err());
        // A catalog's own `tvf_prefixes` expand it too.
        let scoped = caller("alice")
            .with_catalog_scope("ws1", &["s3://landing/{catalog}/{user}/".to_string()])
            .with_proven_catalog_access();
        TvfPolicy::default()
            .check_path("s3://landing/ws1/alice/x.csv", &scoped, false)
            .expect("catalog tvf_prefixes expand {catalog}");
    }

    /// Inline credentials and `allow_all_object_stores` still bypass the
    /// prefixes, `{catalog}` entries included.
    #[test]
    fn tvf_catalog_placeholder_keeps_the_bypasses() {
        let no_scope = caller("alice");
        let path = "s3://elsewhere/x.parquet";
        catalog_staging_policy()
            .check_path(path, &no_scope, true)
            .expect("inline credentials bypass");
        let all = TvfPolicy {
            allow_all_object_stores: true,
            ..catalog_staging_policy()
        };
        all.check_path(path, &no_scope, false)
            .expect("allow_all_object_stores");
    }

    #[test]
    fn tvf_object_store_prefix_allows_exact_subtree() {
        let policy = TvfPolicy {
            allowed_object_store_prefixes: vec![
                "s3://data-platform-staging/_table-load-staging/".to_string()
            ],
            ..Default::default()
        };
        assert!(check_remote(
            &policy,
            "s3://data-platform-staging/_table-load-staging/abc-123/data.csv"
        )
        .is_ok());
        // s3a:// alias is covered by an s3:// prefix.
        assert!(check_remote(
            &policy,
            "s3a://data-platform-staging/_table-load-staging/abc-123/data.csv"
        )
        .is_ok());
        // Same bucket outside the prefix: denied.
        assert!(check_remote(&policy, "s3://data-platform-staging/secret.parquet").is_err());
        // Other buckets: denied.
        assert!(check_remote(&policy, "s3://prod-data/customers.parquet").is_err());
    }

    #[test]
    fn tvf_object_store_prefix_requires_segment_boundary() {
        // `s3://staging-bucket` must not match `s3://staging-bucket-evil`.
        let policy = TvfPolicy {
            allowed_object_store_prefixes: vec!["s3://staging-bucket".to_string()],
            ..Default::default()
        };
        assert!(check_remote(&policy, "s3://staging-bucket/x.csv").is_ok());
        assert!(check_remote(&policy, "s3://staging-bucket").is_ok());
        assert!(check_remote(&policy, "s3://staging-bucket-evil/x.csv").is_err());
        // Same trickery against a prefix with a key part.
        let policy = TvfPolicy {
            allowed_object_store_prefixes: vec!["s3://b/staging".to_string()],
            ..Default::default()
        };
        assert!(check_remote(&policy, "s3://b/staging/x.csv").is_ok());
        assert!(check_remote(&policy, "s3://b/staging-evil/x.csv").is_err());
    }

    #[test]
    fn tvf_object_store_rejects_dot_segment_traversal() {
        let policy = TvfPolicy {
            allowed_object_store_prefixes: vec!["s3://staging/uploads/".to_string()],
            ..Default::default()
        };
        // Literal dot segments would be collapsed by url/HTTP layers AFTER
        // the prefix check — reject outright.
        let err = check_remote(&policy, "s3://staging/uploads/../../etc/x.csv").unwrap_err();
        assert!(err.contains("dot path segment"), "got: {err}");
        // Percent-encoded variants too.
        let err = check_remote(&policy, "s3://staging/uploads/%2e%2e/secret.csv").unwrap_err();
        assert!(err.contains("dot path segment"), "got: {err}");
        let err = check_remote(&policy, "s3://staging/uploads/.%2E/secret.csv").unwrap_err();
        assert!(err.contains("dot path segment"), "got: {err}");
        // Percent-encoded slash could smuggle a hidden segment boundary.
        let err = check_remote(&policy, "s3://staging/uploads%2f..%2fsecret.csv").unwrap_err();
        assert!(
            err.contains("percent-encoded slash") || err.contains("dot path segment"),
            "got: {err}"
        );
        // Double-encoding (`%252e` -> `%2e` -> `.`).
        let err = check_remote(&policy, "s3://staging/uploads/%252e%252e/secret.csv").unwrap_err();
        assert!(
            err.contains("double-encoding") || err.contains("dot path segment"),
            "got: {err}"
        );
    }

    #[test]
    fn tvf_object_store_user_placeholder_substitution() {
        let policy = TvfPolicy {
            allowed_object_store_prefixes: vec!["s3://scratch/{user}/".to_string()],
            ..Default::default()
        };
        // Own prefix: allowed.
        assert!(policy
            .check_path("s3://scratch/alice/data.csv", &caller("alice"), false)
            .is_ok());
        // Another user's prefix: denied.
        assert!(policy
            .check_path("s3://scratch/alice/data.csv", &caller("bob"), false)
            .is_err());
        // No username available -> `{user}` prefixes never match.
        assert!(policy
            .check_path("s3://scratch/alice/data.csv", &TvfCaller::default(), false)
            .is_err());
        // A username with URL-shape-rewriting characters is not substituted.
        assert!(policy
            .check_path("s3://scratch/a/b/data.csv", &caller("a/b"), false)
            .is_err());
        assert!(policy
            .check_path("s3://scratch/../x/data.csv", &caller(".."), false)
            .is_err());
    }

    #[test]
    fn tvf_object_store_admin_role_override() {
        let policy = TvfPolicy {
            object_store_admin_roles: vec!["sqe-storage-admin".to_string()],
            ..Default::default()
        };
        let admin = TvfCaller::for_user(
            "root".to_string(),
            vec![
                "uma_authorization".to_string(),
                "SQE-Storage-Admin".to_string(),
            ],
        );
        assert!(policy
            .check_path("s3://any-bucket/any.parquet", &admin, false)
            .is_ok());
        // Without the role: denied.
        assert!(check_remote(&policy, "s3://any-bucket/any.parquet").is_err());
        // Empty role list never matches anything.
        let no_roles = TvfPolicy::default();
        assert!(no_roles
            .check_path("s3://any-bucket/any.parquet", &admin, false)
            .is_err());
    }

    #[test]
    fn tvf_object_store_inline_credentials_bypass() {
        // Complete inline credentials = the engine's storage key is not
        // used; the object store enforces access itself.
        let policy = TvfPolicy::default();
        assert!(policy
            .check_path("s3://their-own-bucket/x.csv", &caller("alice"), true)
            .is_ok());
    }

    #[test]
    fn tvf_inline_credentials_bypass_disabled_denies_unlisted_path() {
        // SEC-04: with allow_inline_credentials = false, inline creds no longer
        // bypass the prefix allowlist. A path outside the allowlist is denied
        // even when the call carries its own credentials.
        let policy = TvfPolicy {
            allow_inline_credentials: false,
            ..TvfPolicy::default()
        };
        let err = policy
            .check_path("s3://their-own-bucket/x.csv", &caller("alice"), true)
            .unwrap_err();
        assert!(err.contains("denied"));
        assert!(err.contains("allow_inline_credentials = false"));
    }

    #[test]
    fn tvf_inline_credentials_bypass_disabled_still_allows_listed_path() {
        // With the bypass off, an inline-credentialed read to an allowlisted
        // prefix still succeeds (the path passes the normal gate).
        let policy = TvfPolicy {
            allow_inline_credentials: false,
            allowed_object_store_prefixes: vec!["s3://scratch/{user}/".to_string()],
            ..TvfPolicy::default()
        };
        assert!(policy
            .check_path("s3://scratch/alice/data.csv", &caller("alice"), true)
            .is_ok());
    }

    #[test]
    fn tvf_object_store_trusted_local_bypass() {
        let policy = TvfPolicy::default();
        assert!(policy
            .check_path("s3://bucket/x.csv", &TvfCaller::trusted_local(), false)
            .is_ok());
    }

    /// `trusted_startup_config` must bypass the object-store gate the same
    /// way `trusted_local` does -- coordinator startup has no session to
    /// build a `for_user` caller from, so a fail-closed default TvfPolicy
    /// must not reject every operator-declared `[[hive.roots]]` `s3://` URL.
    #[test]
    fn tvf_object_store_trusted_startup_config_bypass() {
        let policy = TvfPolicy::default();
        assert!(policy
            .check_path(
                "s3://bucket/warehouse/",
                &TvfCaller::trusted_startup_config(),
                false
            )
            .is_ok());
    }

    /// Unlike the object-store branch, `trusted` has no effect on the
    /// local-path gate (`check_path`'s `allow_local_paths` branch never
    /// reads `caller.trusted`), so a startup-config caller reading a local
    /// path still needs the operator to opt in explicitly. Pins that
    /// asymmetry so a future `check_path` refactor cannot silently start
    /// trusting local paths too.
    #[test]
    fn tvf_local_path_ignores_trusted_startup_config() {
        let policy = TvfPolicy::default();
        assert!(policy
            .check_path(
                "/warehouse/lake",
                &TvfCaller::trusted_startup_config(),
                false
            )
            .is_err());
    }

    #[test]
    fn tvf_inline_credentials_default_is_enabled() {
        // Back-compat: the serde default and the Rust Default agree (true).
        assert!(TvfPolicy::default().allow_inline_credentials);
        let w: TvfPolicy = toml::from_str("").expect("empty [storage.tvf] deserializes");
        assert!(w.allow_inline_credentials);
    }

    #[test]
    fn tvf_default_rejects_local_absolute_paths() {
        let policy = TvfPolicy::default();
        let err = check_remote(&policy, "/etc/shadow").unwrap_err();
        assert!(err.contains("local filesystem paths are disabled"));
        let err = check_remote(&policy, "/proc/self/environ").unwrap_err();
        assert!(err.contains("local filesystem"));
        let err = check_remote(&policy, "file:///root/.aws/credentials").unwrap_err();
        assert!(err.contains("local filesystem"));
    }

    #[test]
    fn tvf_default_rejects_arbitrary_http_hosts() {
        let policy = TvfPolicy::default();
        // The IMDS scenario from the issue.
        let err = check_remote(
            &policy,
            "http://169.254.169.254/latest/meta-data/iam/security-credentials/",
        )
        .unwrap_err();
        assert!(err.contains("not in `[storage.tvf] allowed_http_hosts`"));
        assert!(err.contains("169.254.169.254"));
    }

    #[test]
    fn tvf_allowed_http_host_is_accepted_exact_match() {
        let policy = TvfPolicy {
            allowed_http_hosts: vec!["data.example.com".to_string(), "huggingface.co".to_string()],
            ..Default::default()
        };
        assert!(check_remote(&policy, "https://data.example.com/file.parquet").is_ok());
        // Case-insensitive host comparison.
        assert!(check_remote(&policy, "https://DATA.EXAMPLE.COM/file.parquet").is_ok());
        // Different port is still allowed (host match only).
        assert!(check_remote(&policy, "https://data.example.com:8080/file.parquet").is_ok());
        // Subdomain that isn't allowlisted is rejected (no wildcards).
        assert!(check_remote(&policy, "https://api.data.example.com/file.parquet").is_err());
    }

    #[test]
    fn tvf_allow_http_true_bypasses_allowlist() {
        let policy = TvfPolicy {
            allow_http: true,
            ..Default::default()
        };
        assert!(check_remote(&policy, "http://169.254.169.254/").is_ok());
        assert!(check_remote(&policy, "https://anything.example/x").is_ok());
    }

    #[test]
    fn tvf_allow_local_paths_true_permits_filesystem() {
        let policy = TvfPolicy {
            allow_local_paths: true,
            ..Default::default()
        };
        assert!(check_remote(&policy, "/var/data/foo.parquet").is_ok());
        assert!(check_remote(&policy, "file:///var/data/foo.parquet").is_ok());
    }

    #[test]
    fn tvf_malformed_http_url_returns_error() {
        let policy = TvfPolicy {
            allowed_http_hosts: vec!["example.com".to_string()],
            ..Default::default()
        };
        let err = check_remote(&policy, "http:///just-a-path").unwrap_err();
        assert!(err.contains("malformed URL") || err.contains("missing host"));
    }

    // --- TvfPolicy::check_endpoint (issue #46 regression tests) ---

    #[test]
    fn tvf_endpoint_empty_is_allowed() {
        let policy = TvfPolicy::default();
        assert!(policy.check_endpoint("").is_ok());
    }

    #[test]
    fn tvf_endpoint_imds_url_is_rejected_by_default() {
        let policy = TvfPolicy::default();
        let err = policy
            .check_endpoint("http://169.254.169.254/latest/meta-data/")
            .unwrap_err();
        assert!(err.contains("not in `[storage.tvf] allowed_http_hosts`"));
        assert!(err.contains("169.254.169.254"));
    }

    #[test]
    fn tvf_endpoint_bare_host_is_allowed() {
        // MinIO / Ceph deployments use bare host:port endpoints. Allowed
        // because they cannot be the IMDS http://169.254.169.254 URL.
        let policy = TvfPolicy::default();
        assert!(policy.check_endpoint("minio.local:9000").is_ok());
        assert!(policy.check_endpoint("s3.example:9000").is_ok());
    }

    #[test]
    fn tvf_endpoint_allowed_http_host_passes() {
        let policy = TvfPolicy {
            allowed_http_hosts: vec!["s3.us-east-1.amazonaws.com".to_string()],
            ..Default::default()
        };
        assert!(policy
            .check_endpoint("https://s3.us-east-1.amazonaws.com")
            .is_ok());
        let err = policy
            .check_endpoint("https://other-region.amazonaws.com")
            .unwrap_err();
        assert!(err.contains("not in `[storage.tvf] allowed_http_hosts`"));
    }

    #[test]
    fn tvf_endpoint_allow_http_true_bypasses_allowlist() {
        let policy = TvfPolicy {
            allow_http: true,
            ..Default::default()
        };
        // Defense-in-depth surrenders when allow_http = true.
        assert!(policy.check_endpoint("http://169.254.169.254").is_ok());
    }

    #[test]
    fn tvf_http_destinations_reject_userinfo_and_ambiguous_authorities() {
        for allow_http in [false, true] {
            let policy = TvfPolicy {
                allow_http,
                allowed_http_hosts: vec!["allowed.example".into()],
                ..Default::default()
            };
            for destination in [
                "https://allowed.example:password@blocked.example/",
                "https://user@allowed.example/",
                "https://allowed.example\\@blocked.example/",
                "http:///allowed.example/",
                "https://allowed.example:invalid/",
            ] {
                assert!(check_remote(&policy, destination).is_err(), "{destination}");
                assert!(policy.check_endpoint(destination).is_err(), "{destination}");
            }
        }
    }

    /// Finding 753: an allow-listed hostname that resolves to IMDS, loopback,
    /// link-local or unspecified is refused unless the operator also
    /// allow-listed that literal IP. Loopback names and literal IPs are the
    /// operator's own choice and are not resolved.
    #[test]
    fn tvf_allowed_hostname_rebound_to_internal_address_is_refused() {
        let by_name = TvfPolicy {
            allowed_http_hosts: vec!["data.example.com".into(), "localhost".into()],
            ..Default::default()
        };
        use std::net::IpAddr;
        let imds: IpAddr = "169.254.169.254".parse().unwrap();
        let imds_v6: IpAddr = "fd00:ec2::254".parse().unwrap();
        let lo: IpAddr = "127.0.0.1".parse().unwrap();
        let public: IpAddr = "93.184.216.34".parse().unwrap();
        assert_eq!(by_name.rebinding_target(&[public, imds]), Some(imds));
        assert_eq!(by_name.rebinding_target(&[imds_v6]), Some(imds_v6));
        assert_eq!(by_name.rebinding_target(&[lo]), Some(lo));
        assert!(by_name
            .rebinding_target(&["fe80::1".parse().unwrap()])
            .is_some());
        assert!(by_name
            .rebinding_target(&["0.0.0.0".parse().unwrap()])
            .is_some());
        assert_eq!(by_name.rebinding_target(&[public]), None);
        // Named loopback and literal IPs are not resolved: the operator chose them.
        assert!(check_remote(&by_name, "http://localhost:9000/x").is_ok());

        let literal = TvfPolicy {
            allowed_http_hosts: vec!["127.0.0.1".into(), "169.254.169.254".into()],
            ..Default::default()
        };
        assert_eq!(literal.rebinding_target(&[lo, imds]), None);
        assert!(check_remote(&literal, "http://127.0.0.1:9000/x").is_ok());
        assert!(check_remote(&literal, "http://169.254.169.254/latest").is_ok());
    }

    #[test]
    fn tvf_http_destinations_parse_ipv6_ports_and_host_case() {
        let policy = TvfPolicy {
            allowed_http_hosts: vec!["allowed.example".into(), "[::1]".into()],
            ..Default::default()
        };
        for destination in ["https://ALLOWED.example:443/x", "http://[::1]:9000/x"] {
            assert!(check_remote(&policy, destination).is_ok(), "{destination}");
            assert!(policy.check_endpoint(destination).is_ok(), "{destination}");
        }
    }
}
