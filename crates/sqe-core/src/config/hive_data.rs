//! Where Hive table data may live, and what file TVFs may not read (#886).
//!
//! Two lists used to be one. `[storage.tvf] allowed_object_store_prefixes`
//! said both "a user may `read_parquet` here" and "a Hive table may live
//! here", so every deployment with a Hive table also opened its data to raw
//! reads that skip the table's row filters and column masks.
//!
//! - [`HiveDataPolicy`] gates Hive table and partition locations against
//!   `[hive] allowed_data_prefixes` plus the root's own `data_prefixes`,
//!   with the same caller, placeholders and canonicalisation as the TVF
//!   gate. With neither set it falls back to the TVF list (deprecated).
//! - [`ProtectedLocations`] is the process-wide set of locations file TVFs
//!   refuse whoever asks: every configured Hive data prefix, and every Hive
//!   table location SQE has created or loaded. [`TvfPolicy`] consults it
//!   after the trusted and inline-credential exits and before allow-all,
//!   admin roles and the allow-list. Hive's own gate never consults it, so a
//!   loaded table cannot lock itself out.

use std::sync::{Arc, OnceLock, PoisonError, RwLock};

use super::{
    canonicalize_object_url, is_object_store_url, object_prefix_matches, substitute_placeholders,
    HiveConfig, HiveRootConfig, SqeConfig, TvfCaller, TvfPolicy,
};

/// One protected location, split into path segments after the scheme. The
/// first segment is the bucket (or container); `None` is a placeholder
/// segment (`{user}`, `{catalog}`), which matches any one segment.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ProtectedEntry {
    scheme: String,
    segments: Vec<Option<String>>,
    /// The configured prefix this entry came from. `None` for a table
    /// location SQE loaded, which an error message never names: the caller
    /// may not be allowed to know the table exists.
    configured: Option<String>,
}

/// Locations file TVFs must not read with the engine's storage credentials.
///
/// Process-wide: [`ProtectedLocations::global`] is what every
/// [`TvfPolicy`] reaches unless a test injects its own instance, so a policy
/// built with `TvfPolicy::default()` or deserialized afresh can never miss
/// it. Entries are only added; the set is bounded by the configured prefixes
/// and the Hive tables this process has created or loaded. A restart starts
/// with the configured prefixes only, which are the real boundary.
#[derive(Default)]
pub struct ProtectedLocations {
    entries: RwLock<Vec<ProtectedEntry>>,
}

impl std::fmt::Debug for ProtectedLocations {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Table locations do not belong in a debug dump of the config.
        f.debug_struct("ProtectedLocations")
            .field("entries", &self.read().len())
            .finish()
    }
}

static GLOBAL: OnceLock<Arc<ProtectedLocations>> = OnceLock::new();

impl ProtectedLocations {
    /// The process-wide registry.
    pub fn global() -> Arc<Self> {
        Arc::clone(GLOBAL.get_or_init(|| Arc::new(Self::default())))
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, Vec<ProtectedEntry>> {
        self.entries.read().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn is_empty(&self) -> bool {
        self.read().is_empty()
    }

    /// Protect a configured Hive data prefix. A placeholder segment becomes
    /// a wildcard for that whole segment, so `s3://lake/{user}/` protects
    /// every user's folder. A prefix that does not canonicalise is skipped;
    /// `SqeConfig::validate` has already refused it at startup.
    pub fn protect_prefix(&self, prefix: &str) {
        let Ok(canon) = canonicalize_object_url(prefix) else {
            return;
        };
        let Some((scheme, segments)) = split_prefix(&canon) else {
            return;
        };
        self.insert(ProtectedEntry {
            scheme,
            segments,
            configured: Some(prefix.to_string()),
        });
    }

    /// Protect a Hive table or partition location SQE has created or
    /// loaded. Local paths are not protected: file TVFs never read them with
    /// object-store credentials.
    pub fn protect_location(&self, location: &str) {
        if !is_object_store_url(location) {
            return;
        }
        let Ok(canon) = canonicalize_object_url(location) else {
            return;
        };
        let Some((scheme, segments)) = split_path(&canon) else {
            return;
        };
        let segments: Vec<Option<String>> = segments.into_iter().map(Some).collect();
        if segments.is_empty() {
            return;
        }
        // Skip a location an existing entry already covers (a partition
        // under its table, a table under a configured prefix), so the set
        // grows with tables rather than with partitions.
        let covered = self.read().iter().any(|e| {
            e.scheme == scheme
                && e.segments.len() <= segments.len()
                && segments_overlap(&e.segments, &segments)
        });
        if !covered {
            self.insert(ProtectedEntry {
                scheme,
                segments,
                configured: None,
            });
        }
    }

    fn insert(&self, entry: ProtectedEntry) {
        let mut entries = self.entries.write().unwrap_or_else(PoisonError::into_inner);
        if !entries.contains(&entry) {
            entries.push(entry);
        }
    }

    /// Why a canonical object-store path may not be read, or `None`. A path
    /// is refused when it lies under a protected entry or contains one: a
    /// directory or glob above a table lists the table's files too. A glob
    /// is cut at its first wildcard segment.
    fn refusal(&self, canon: &str) -> Option<String> {
        let (scheme, segments) = split_path(canon)?;
        let segments: Vec<Option<String>> = segments.into_iter().map(Some).collect();
        let entries = self.read();
        let hit = entries
            .iter()
            .filter(|e| e.scheme == scheme && segments_overlap(&e.segments, &segments))
            // A configured prefix is named; a table location never is.
            .min_by_key(|e| e.configured.is_none())?;
        Some(match &hit.configured {
            Some(prefix) => format!("overlaps the Hive data prefix '{prefix}'"),
            None => "overlaps the location of a Hive table".to_string(),
        })
    }
}

/// `scheme` and non-empty path segments of a canonical URL, with every
/// segment from the first glob character on dropped.
fn split_path(canon: &str) -> Option<(String, Vec<String>)> {
    let (scheme, rest) = canon.split_once("://")?;
    let segments = rest
        .split('/')
        .filter(|s| !s.is_empty())
        .take_while(|s| !s.contains(['*', '?', '[', '{']))
        .map(str::to_string)
        .collect();
    Some((scheme.to_string(), segments))
}

/// `scheme` and segments of a configured prefix; a segment holding a
/// placeholder (or any `{`) matches any one segment.
fn split_prefix(canon: &str) -> Option<(String, Vec<Option<String>>)> {
    let (scheme, rest) = canon.split_once("://")?;
    let segments = rest
        .split('/')
        .filter(|s| !s.is_empty())
        .map(|s| (!s.contains('{')).then(|| s.to_string()))
        .collect();
    Some((scheme.to_string(), segments))
}

/// `true` when the shorter list is a prefix of the longer, segment by
/// segment: one location lies under the other. A `None` matches any
/// segment; the bucket (first segment) compares without case.
fn segments_overlap(a: &[Option<String>], b: &[Option<String>]) -> bool {
    a.iter().zip(b).enumerate().all(|(i, pair)| match pair {
        (Some(x), Some(y)) if i == 0 => x.eq_ignore_ascii_case(y),
        (Some(x), Some(y)) => x == y,
        _ => true,
    })
}

/// `true` when one configured prefix lies under the other, placeholders
/// matching any segment on either side. Unparseable prefixes never overlap;
/// validation reports them.
pub fn prefixes_overlap(a: &str, b: &str) -> bool {
    let parse = |p: &str| {
        canonicalize_object_url(p)
            .ok()
            .and_then(|c| split_prefix(&c))
    };
    match (parse(a), parse(b)) {
        (Some((sa, a)), Some((sb, b))) => sa == sb && segments_overlap(&a, &b),
        _ => false,
    }
}

impl TvfPolicy {
    /// The file-TVF refusal for Hive table data: runs after the trusted and
    /// inline-credential exits and before allow-all, admin roles and the
    /// allow-list, so no TVF setting reopens it.
    pub(super) fn refuse_protected(&self, path: &str, caller: &TvfCaller) -> Result<(), String> {
        if self.protected.is_empty() {
            return Ok(());
        }
        let principal = caller.username.as_deref().unwrap_or("<anonymous>");
        // With something to protect, a path that cannot be compared is not
        // read: allow-all and admin roles used to skip canonicalisation.
        let what = canonicalize_object_url(path).map(|canon| self.protected.refusal(&canon));
        let what = match what {
            Ok(None) => return Ok(()),
            Ok(Some(what)) => what,
            Err(e) => {
                tracing::warn!(principal = %principal, path = %path, "TVF object-store access denied: {e}");
                return Err(e);
            }
        };
        tracing::warn!(
            principal = %principal,
            path = %path,
            "TVF object-store read refused: the path {what}"
        );
        let inline_hint = if self.allow_inline_credentials {
            ", or pass complete inline credentials so the engine's storage key is not used"
        } else {
            ""
        };
        Err(format!(
            "TVF: object-store path '{path}' {what}. File TVFs do not read Hive \
             table data with the engine's storage credentials; query the Hive \
             table instead{inline_hint}."
        ))
    }
}

/// Protect every configured Hive data prefix (global and per root) in the
/// registry `tvf` reaches. Idempotent; call it wherever a coordinator is
/// built from config, before the first query.
pub fn protect_hive_data_prefixes(hive: &HiveConfig, tvf: &TvfPolicy) {
    for prefix in configured_hive_prefixes(hive) {
        tvf.protected.protect_prefix(prefix);
    }
}

/// `[hive] allowed_data_prefixes` followed by every root's `data_prefixes`.
pub fn configured_hive_prefixes(hive: &HiveConfig) -> impl Iterator<Item = &String> {
    hive.allowed_data_prefixes
        .iter()
        .chain(hive.roots.iter().flat_map(|r| &r.data_prefixes))
}

/// Every (TVF prefix, Hive data prefix) pair where one lies under the
/// other, with the TVF prefix's config section: the global list and every
/// catalog's `tvf_prefixes`. Startup logs each as an error; the TVF refusal
/// already keeps the overlap from being read.
pub fn hive_tvf_overlaps(config: &SqeConfig) -> Vec<(String, String, String)> {
    let mut tvf: Vec<(String, &String)> = config
        .storage
        .tvf
        .allowed_object_store_prefixes
        .iter()
        .map(|p| ("[storage.tvf] allowed_object_store_prefixes".to_string(), p))
        .collect();
    tvf.extend(
        config
            .catalog
            .tvf_prefixes
            .iter()
            .map(|p| ("[catalog] tvf_prefixes".to_string(), p)),
    );
    let mut catalogs: Vec<_> = config.catalogs.iter().collect();
    catalogs.sort_by(|a, b| a.0.cmp(b.0));
    for (name, catalog) in catalogs {
        tvf.extend(
            catalog
                .tvf_prefixes
                .iter()
                .map(|p| (format!("[catalogs.{name}] tvf_prefixes"), p)),
        );
    }
    let mut out = Vec::new();
    for hive in configured_hive_prefixes(&config.hive) {
        for (section, prefix) in &tvf {
            if prefixes_overlap(prefix, hive) {
                out.push((section.clone(), (*prefix).clone(), hive.clone()));
            }
        }
    }
    out
}

/// Gate for Hive table and partition locations (#886): which locations a
/// Hive table may use, checked for the session user on create,
/// `ADD PARTITION` and every load.
#[derive(Debug, Clone)]
pub struct HiveDataPolicy {
    tvf: TvfPolicy,
    prefixes: Vec<String>,
    fallback: bool,
}

/// The deprecated fallback over `tvf`'s list: what a Hive store built
/// without a `HiveConfig` (embedded tools, tests) has always checked.
impl From<TvfPolicy> for HiveDataPolicy {
    fn from(tvf: TvfPolicy) -> Self {
        Self {
            tvf,
            prefixes: Vec::new(),
            fallback: true,
        }
    }
}

impl HiveDataPolicy {
    /// The policy for an attached root: `[hive] allowed_data_prefixes`, or
    /// the TVF list when no Hive prefix is configured anywhere (the
    /// deprecated fallback). The fallback is a property of the whole
    /// config: a root without its own `data_prefixes` gets the global list,
    /// not the TVF list, as soon as any Hive prefix is set.
    pub fn from_config(hive: &HiveConfig, tvf: &TvfPolicy) -> Self {
        Self {
            tvf: tvf.clone(),
            prefixes: hive.allowed_data_prefixes.clone(),
            fallback: configured_hive_prefixes(hive).next().is_none(),
        }
    }

    /// The policy for a configured root: the global list plus the root's
    /// own `data_prefixes`.
    pub fn for_root(&self, root: &HiveRootConfig) -> Self {
        let mut policy = self.clone();
        policy.prefixes.extend(root.data_prefixes.iter().cloned());
        policy
    }

    /// `true` while no Hive prefix is configured and the TVF list decides.
    pub fn uses_tvf_fallback(&self) -> bool {
        self.fallback
    }

    /// The TVF policy the gate was built from (local and HTTP paths, and
    /// the fallback list, are decided by it).
    pub fn tvf(&self) -> &TvfPolicy {
        &self.tvf
    }

    /// Record a location that passed this gate and is now Hive table data,
    /// so file TVFs refuse it from here on.
    pub fn protect_location(&self, location: &str) {
        self.tvf.protected.protect_location(location);
    }

    /// Whether `caller` may use `location` for Hive table data. Local and
    /// HTTP paths keep the TVF rules (`allow_local_paths`, ...). An
    /// object-store URL must lie under a Hive data prefix expanded for the
    /// caller; trusted callers, `allow_all_object_stores` and
    /// `object_store_admin_roles` pass, as they do for the TVF gate.
    pub fn check_location(&self, location: &str, caller: &TvfCaller) -> Result<(), String> {
        if !is_object_store_url(location) {
            return self.tvf.check_path(location, caller, false);
        }
        if self.fallback {
            return self.tvf.check_object_store_allowlist(location, caller);
        }
        if caller.trusted
            || self.tvf.allow_all_object_stores
            || self.tvf.is_object_store_admin(caller)
        {
            return Ok(());
        }
        let principal = caller.username.as_deref().unwrap_or("<anonymous>");
        let canon = canonicalize_object_url(location)
            .map_err(|e| e.replacen("TVF: object-store path", "Hive: location", 1))?;
        for raw in &self.prefixes {
            let Some(substituted) = substitute_placeholders(
                raw,
                caller.username.as_deref(),
                caller.catalog_with_access.as_deref(),
            ) else {
                continue;
            };
            let Ok(canon_prefix) = canonicalize_object_url(&substituted) else {
                continue;
            };
            if object_prefix_matches(&canon, &canon_prefix) {
                return Ok(());
            }
        }
        tracing::warn!(
            principal = %principal,
            location = %location,
            "Hive location denied: no matching `[hive] allowed_data_prefixes` \
             or root `data_prefixes` entry"
        );
        Err(format!(
            "Hive: location '{location}' denied for user '{principal}'. Hive \
             table data must lie under `[hive] allowed_data_prefixes` or the \
             root's `data_prefixes` ({} configured for this root; the `{{user}}` \
             placeholder expands to the authenticated username).",
            self.prefixes.len()
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry() -> Arc<ProtectedLocations> {
        Arc::new(ProtectedLocations::default())
    }

    fn tvf(prefixes: &[&str], protected: &Arc<ProtectedLocations>) -> TvfPolicy {
        TvfPolicy {
            allowed_object_store_prefixes: prefixes.iter().map(|p| p.to_string()).collect(),
            protected: Arc::clone(protected),
            ..TvfPolicy::default()
        }
    }

    fn hive(global: &[&str], roots: &[(&str, &[&str])]) -> HiveConfig {
        HiveConfig {
            allowed_data_prefixes: global.iter().map(|p| p.to_string()).collect(),
            roots: roots
                .iter()
                .map(|(name, prefixes)| HiveRootConfig {
                    name: name.to_string(),
                    kind: Default::default(),
                    location: format!("s3://lake/{name}/"),
                    region: None,
                    catalog_id: None,
                    databases: Vec::new(),
                    secret: None,
                    data_prefixes: prefixes.iter().map(|p| p.to_string()).collect(),
                })
                .collect(),
            ..HiveConfig::default()
        }
    }

    fn alice() -> TvfCaller {
        TvfCaller::for_user("alice".into(), vec!["analyst".into()])
    }

    #[test]
    fn hive_location_needs_a_hive_prefix_not_a_tvf_prefix() {
        let reg = registry();
        let policy = HiveDataPolicy::from_config(
            &hive(&["s3://lake/hive/"], &[]),
            &tvf(&["s3://staging/"], &reg),
        );
        assert!(!policy.uses_tvf_fallback());
        policy
            .check_location("s3://lake/hive/sales/", &alice())
            .expect("under the Hive prefix");
        let err = policy
            .check_location("s3://staging/sales/", &alice())
            .unwrap_err();
        assert!(err.contains("allowed_data_prefixes"), "{err}");
        assert!(policy
            .check_location("s3://lake/hive-evil/x/", &alice())
            .is_err());
    }

    #[test]
    fn hive_prefix_expands_the_user_placeholder() {
        let reg = registry();
        let policy =
            HiveDataPolicy::from_config(&hive(&["s3://lake/home/{user}/"], &[]), &tvf(&[], &reg));
        policy
            .check_location("s3://lake/home/alice/t/", &alice())
            .expect("own folder");
        assert!(policy
            .check_location("s3://lake/home/bob/t/", &alice())
            .is_err());
        assert!(policy
            .check_location("s3://lake/home/alice/../bob/t/", &alice())
            .is_err());
    }

    #[test]
    fn root_prefixes_add_to_the_global_list_and_end_the_fallback() {
        let reg = registry();
        let config = hive(&[], &[("sales", &["s3://sales-data/"]), ("ops", &[])]);
        let global = HiveDataPolicy::from_config(&config, &tvf(&["s3://lake/"], &reg));
        assert!(
            !global.uses_tvf_fallback(),
            "one root's prefix ends the fallback"
        );
        let sales = global.for_root(&config.roots[0]);
        let ops = global.for_root(&config.roots[1]);
        sales
            .check_location("s3://sales-data/t/", &alice())
            .expect("root prefix");
        // `ops` has no prefixes of its own and the global list is empty: the
        // TVF list no longer applies to it.
        assert!(ops.check_location("s3://lake/t/", &alice()).is_err());
        assert!(ops.check_location("s3://sales-data/t/", &alice()).is_err());
    }

    #[test]
    fn fallback_uses_the_tvf_list_and_is_not_refused_by_its_own_tables() {
        let reg = registry();
        let policy = HiveDataPolicy::from_config(&hive(&[], &[]), &tvf(&["s3://lake/"], &reg));
        assert!(policy.uses_tvf_fallback());
        policy
            .check_location("s3://lake/t/", &alice())
            .expect("TVF list");
        policy.protect_location("s3://lake/t/");
        // The second load of the same table: Hive's gate never consults the
        // registry, so a loaded table does not lock itself out.
        policy
            .check_location("s3://lake/t/", &alice())
            .expect("second load");
        assert!(policy.check_location("s3://other/t/", &alice()).is_err());
        // The TVF gate refuses it now.
        let err = policy
            .tvf()
            .check_path("s3://lake/t/part-0.parquet", &alice(), false)
            .unwrap_err();
        assert!(err.contains("location of a Hive table"), "{err}");
        assert!(
            !err.contains("s3://lake/t/'"),
            "the table location is not named: {err}"
        );
    }

    #[test]
    fn admin_and_allow_all_bypass_the_hive_gate() {
        let reg = registry();
        let mut tvf_policy = tvf(&[], &reg);
        tvf_policy.object_store_admin_roles = vec!["admin".into()];
        let policy = HiveDataPolicy::from_config(&hive(&["s3://lake/hive/"], &[]), &tvf_policy);
        let admin = TvfCaller::for_user("root".into(), vec!["ADMIN".into()]);
        policy
            .check_location("s3://anywhere/t/", &admin)
            .expect("admin role");
        tvf_policy.allow_all_object_stores = true;
        let policy = HiveDataPolicy::from_config(&hive(&["s3://lake/hive/"], &[]), &tvf_policy);
        policy
            .check_location("s3://anywhere/t/", &alice())
            .expect("allow-all");
    }

    #[test]
    fn local_paths_keep_the_tvf_rule() {
        let reg = registry();
        let mut tvf_policy = tvf(&[], &reg);
        let config = hive(&["s3://lake/hive/"], &[]);
        assert!(HiveDataPolicy::from_config(&config, &tvf_policy)
            .check_location("/tmp/wh/t", &alice())
            .is_err());
        tvf_policy.allow_local_paths = true;
        HiveDataPolicy::from_config(&config, &tvf_policy)
            .check_location("/tmp/wh/t", &alice())
            .expect("allow_local_paths");
    }

    #[test]
    fn tvf_refuses_hive_prefixes_even_for_admin_and_allow_all() {
        let reg = registry();
        let mut policy = tvf(&["s3://lake/"], &reg);
        policy.object_store_admin_roles = vec!["admin".into()];
        protect_hive_data_prefixes(&hive(&["s3://lake/hive/"], &[]), &policy);
        let admin = TvfCaller::for_user("root".into(), vec!["admin".into()]);
        let file = "s3://lake/hive/sales/part-0.parquet";
        for caller in [&alice(), &admin] {
            let err = policy.check_path(file, caller, false).unwrap_err();
            assert!(err.contains("Hive data prefix 's3://lake/hive/'"), "{err}");
        }
        policy.allow_all_object_stores = true;
        assert!(policy.check_path(file, &alice(), false).is_err());
        // The scheme alias does not get around it.
        assert!(policy
            .check_path("s3a://lake/hive/sales/part-0.parquet", &alice(), false)
            .is_err());
    }

    #[test]
    fn tvf_reads_beside_the_hive_prefix_and_with_inline_credentials_pass() {
        let reg = registry();
        let policy = tvf(&["s3://lake/"], &reg);
        protect_hive_data_prefixes(&hive(&["s3://lake/hive/"], &[]), &policy);
        policy
            .check_path("s3://lake/staging/x.parquet", &alice(), false)
            .expect("staging beside the Hive prefix");
        policy
            .check_path("s3://lake/hive-2/x.parquet", &alice(), false)
            .expect("segment boundary");
        policy
            .check_path("s3://lake/hive/sales/part-0.parquet", &alice(), true)
            .expect("inline credentials");
        policy
            .check_path(
                "s3://lake/hive/sales/part-0.parquet",
                &TvfCaller::trusted_local(),
                false,
            )
            .expect("trusted local");
    }

    #[test]
    fn tvf_refuses_directories_and_globs_above_hive_data() {
        let reg = registry();
        let policy = tvf(&["s3://lake/"], &reg);
        reg.protect_location("s3://lake/tables/sales/");
        for path in [
            "s3://lake/",
            "s3://lake/tables",
            "s3://lake/tables/*/part-0.parquet",
            "s3://lake/*",
            "s3://lake/tables/sales/dt=1/part-0.parquet",
            "s3://lake//tables/sales/part-0.parquet",
            "s3://LAKE/tables/sales/part-0.parquet",
        ] {
            assert!(policy.check_path(path, &alice(), false).is_err(), "{path}");
        }
        policy
            .check_path("s3://lake/tables/sales-old/x.parquet", &alice(), false)
            .expect("sibling");
        policy
            .check_path("s3://lake/other/*.parquet", &alice(), false)
            .expect("glob beside the table");
    }

    #[test]
    fn placeholder_segments_protect_every_value() {
        let reg = registry();
        let policy = tvf(&["s3://lake/"], &reg);
        protect_hive_data_prefixes(&hive(&["s3://lake/home/{user}/hive/"], &[]), &policy);
        assert!(policy
            .check_path("s3://lake/home/bob/hive/t/x.parquet", &alice(), false)
            .is_err());
        assert!(policy
            .check_path("s3://lake/home/", &alice(), false)
            .is_err());
        policy
            .check_path("s3://lake/home/bob/staging/x.parquet", &alice(), false)
            .expect("beside the Hive folder");
    }

    #[test]
    fn partitions_under_a_protected_table_do_not_grow_the_registry() {
        let reg = registry();
        reg.protect_location("s3://lake/t/");
        reg.protect_location("s3://lake/t/dt=1/");
        reg.protect_location("s3://lake/t/dt=2/");
        reg.protect_location("/local/t/");
        assert_eq!(reg.read().len(), 1);
        reg.protect_location("s3://lake/elsewhere/dt=3/");
        assert_eq!(reg.read().len(), 2);
    }

    #[test]
    fn overlapping_prefixes() {
        assert!(prefixes_overlap("s3://lake/", "s3://lake/hive/"));
        assert!(prefixes_overlap("s3://lake/hive/x/", "s3a://lake/hive"));
        assert!(prefixes_overlap(
            "s3://lake/{user}/",
            "s3://lake/alice/hive/"
        ));
        assert!(!prefixes_overlap("s3://lake/staging/", "s3://lake/hive/"));
        assert!(!prefixes_overlap("s3://lake/hive-2/", "s3://lake/hive/"));
        assert!(!prefixes_overlap("gs://lake/", "s3://lake/"));
    }
}
