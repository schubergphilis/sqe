//! Profile-driven grant planning from the vendored `grant-profile.json`.
//!
//! SQE and the data-platform control plane write Ranger policies to the SAME
//! `polaris` service. If they disagree about what a privilege confers, "who
//! granted this, and does it mean the same thing" becomes unanswerable. So the
//! privilege vocabulary is not written in Rust: it is vendored from the platform
//! and read at startup, and the profile's own fixtures are the test.
//!
//! One file since v5. `privileges` ships **seeds**, and `access_types` carries the
//! implication graph that turns a seed into the access types Polaris actually
//! checks; the closure is applied here, at write time. Until v5 that graph lived in
//! a second vendored file, `servicedef-polaris.json`, which is still the Ranger
//! service DEFINITION (the quickstarts register it with Ranger Admin) but is no
//! longer an input to planning.
//!
//! What the fold does NOT do is pre-expand the privileges, and that distinction is
//! the reason the fixtures remain a test rather than an echo. If the generator
//! shipped finished access-type sets per privilege, the fixtures would be
//! self-satisfying: this code reading a set and asserting it read it. Instead SQE
//! computes the closure and compares against `expect`, which the platform computed
//! with its own code. The closure is exactly what drifted before, when SQE's
//! hand-written `WRITE_ACCESS` carried `table-properties-write`, which
//! `table-data-write` does not imply.
//!
//! Keep this file byte-identical to data-platform's. `scripts/check-vendored-profile.sh`
//! is the gate; the fixtures only prove SQE agrees with the profile it HOLDS, not
//! that the profile it holds is current.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::Deserialize;

/// The vendored profile, parsed once.
static PROFILE: std::sync::LazyLock<GrantProfile> = std::sync::LazyLock::new(|| {
    GrantProfile::load().expect("vendored grant profile must parse; it is a build asset")
});

/// The profile SQE plans against.
pub fn profile() -> &'static GrantProfile {
    &PROFILE
}

const PROFILE_JSON: &str = include_str!("../../assets/grant-profile.json");

/// Resource levels, ordered outermost first. Order IS the semantics: a plan is
/// truncated at the level the statement names, and a statement naming something
/// deeper than a privilege's deepest level is refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Level {
    Catalog,
    Namespace,
    Table,
}

impl Level {
    pub fn as_str(self) -> &'static str {
        match self {
            Level::Catalog => "catalog",
            Level::Namespace => "namespace",
            Level::Table => "table",
        }
    }
}

#[derive(Debug, Deserialize)]
struct LevelPlan {
    level: Level,
    seeds: Vec<String>,
    /// Subtracted AFTER expansion, never held out of the seeds. Order is
    /// load-bearing: `table-data-write`'s closure is required to commit an
    /// Iceberg snapshot, and it also drags in `table-location-set`, which
    /// `INSERT` must not confer. Holding it out of the seeds instead would lose
    /// the rest of the closure.
    #[serde(default)]
    exclude: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ProfileFile {
    version: u32,
    privileges: BTreeMap<String, Vec<LevelPlan>>,
    aliases: BTreeMap<String, String>,
    /// Access type -> what holding it implies, the graph the closure walks.
    ///
    /// Deliberately NOT `#[serde(default)]`. A profile missing this would expand
    /// every seed to itself, so `INSERT` would confer `table-data-write` alone and
    /// every Iceberg commit would fail an authorization check. Under-granting is
    /// the safe direction but a silent one, and the whole point of vendoring is
    /// that the two writers agree; refusing to parse says so at startup.
    access_types: HashMap<String, Vec<String>>,
    #[serde(default)]
    fixtures: Vec<Fixture>,
    #[serde(default)]
    rejects: Vec<Reject>,
}

/// Read only by `golden_fixtures_match_the_platform`. Kept on the parsed profile
/// rather than re-read in the test so the test cannot accidentally check a
/// different file from the one the planner uses.
#[derive(Debug, Deserialize)]
#[cfg_attr(not(test), allow(dead_code))]
struct Fixture {
    privilege: String,
    catalog: Option<String>,
    namespace: Option<String>,
    table: Option<String>,
    expect: Vec<ExpectedPolicy>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct ExpectedPolicy {
    resource: BTreeMap<String, String>,
    access_types: Vec<String>,
}

/// Read only by `rejects_are_refused`. See `Fixture`.
#[derive(Debug, Deserialize)]
#[cfg_attr(not(test), allow(dead_code))]
struct Reject {
    privilege: String,
    catalog: Option<String>,
    namespace: Option<String>,
    table: Option<String>,
}

/// One policy a grant has to write: a Ranger resource map and its access types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedPolicy {
    pub resource: BTreeMap<String, String>,
    pub access_types: Vec<String>,
}

pub struct GrantProfile {
    version: u32,
    privileges: BTreeMap<String, Vec<LevelPlan>>,
    aliases: BTreeMap<String, String>,
    implied: HashMap<String, Vec<String>>,
    #[cfg_attr(not(test), allow(dead_code))]
    fixtures: Vec<Fixture>,
    #[cfg_attr(not(test), allow(dead_code))]
    rejects: Vec<Reject>,
}

impl GrantProfile {
    fn load() -> Result<Self, String> {
        let p: ProfileFile =
            serde_json::from_str(PROFILE_JSON).map_err(|e| format!("grant-profile.json: {e}"))?;
        Ok(Self {
            version: p.version,
            privileges: p.privileges,
            aliases: p.aliases,
            implied: p.access_types,
            fixtures: p.fixtures,
            rejects: p.rejects,
        })
    }

    pub fn version(&self) -> u32 {
        self.version
    }

    /// Privileges with an explicit plan, for error messages. Aliases included, so
    /// an operator who typed `MODIFY` or `USAGE` is told those are accepted.
    pub fn known_privileges(&self) -> Vec<String> {
        let mut out: Vec<String> = self.privileges.keys().cloned().collect();
        out.extend(self.aliases.keys().cloned());
        out.sort();
        out
    }

    /// Resolve aliases and normalise spelling: whitespace collapsed, upper-cased.
    /// Mirrors the platform's `canonical_privilege`.
    pub fn canonical_privilege(&self, sql_priv: &str) -> String {
        let normalised = sql_priv.split_whitespace().collect::<Vec<_>>().join(" ").to_uppercase();
        self.aliases
            .get(&normalised)
            .cloned()
            .unwrap_or(normalised)
    }

    /// Transitive `impliedGrants` closure of `seeds`, sorted and deduped.
    ///
    /// Sorting matters more than it looks: it is what makes two writers produce
    /// byte-identical policies, and the fixtures compare exactly. The walk carries
    /// a seen-set because nothing in a service definition prevents an
    /// implied-grant cycle, and a naive walk would not terminate.
    pub fn expand_access_types(&self, seeds: &[String]) -> Vec<String> {
        let mut out: BTreeSet<String> = BTreeSet::new();
        let mut stack: Vec<String> = seeds.to_vec();
        while let Some(t) = stack.pop() {
            if !out.insert(t.clone()) {
                continue;
            }
            if let Some(more) = self.implied.get(&t) {
                stack.extend(more.iter().cloned());
            }
        }
        out.into_iter().collect()
    }

    /// The SEEDS of a privilege's deepest level, before expansion.
    ///
    /// The first seed is the access type that DEFINES the privilege (`SELECT` ->
    /// `table-data-read`, `INSERT` -> `table-data-write`), which is what an
    /// introspection answer should name. Taking the first element of the expanded
    /// set would pick alphabetically instead, and for `INSERT` that is
    /// `table-data-read` -- reporting a write privilege as though it were a read.
    pub fn deepest_seeds(&self, canonical: &str) -> Option<&[String]> {
        self.privileges
            .get(canonical)
            .and_then(|ls| ls.last())
            .map(|l| l.seeds.as_slice())
    }

    /// The deepest level a privilege binds to.
    pub fn deepest_level(&self, canonical: &str) -> Option<Level> {
        self.privileges.get(canonical).and_then(|ls| ls.last()).map(|l| l.level)
    }

    /// Every policy one GRANT has to write, outermost level first.
    ///
    /// Truncated at the level the statement NAMES: a table-level policy has no
    /// name to bind to when no table was given, so `GRANT SELECT ON SCHEMA cat.ns`
    /// is a two-policy plan and `GRANT SELECT ON cat` is one.
    ///
    /// Refused, never widened, when the statement names something DEEPER than the
    /// privilege's deepest level. `build_resource_map` would silently drop the
    /// extra components and write a policy broader than the object named:
    /// `GRANT ALL ON wh.sales.orders` reported success on one table and conferred
    /// `catalog-content-manage` over every table in `wh`.
    pub fn plan_grant(
        &self,
        sql_priv: &str,
        realm: &str,
        catalog: &str,
        namespace: Option<&str>,
        table: Option<&str>,
    ) -> Result<Vec<PlannedPolicy>, String> {
        let canonical = self.canonical_privilege(sql_priv);
        let levels = self.privileges.get(&canonical).ok_or_else(|| {
            format!(
                "Privilege '{sql_priv}' has no plan in grant-profile.json v{}. Known: {}",
                self.version,
                self.known_privileges().join(", ")
            )
        })?;
        let deepest = levels.last().map(|l| l.level).ok_or_else(|| {
            format!("Privilege '{canonical}' has an empty plan in grant-profile.json")
        })?;

        let named = match (namespace, table) {
            (_, Some(_)) => Level::Table,
            (Some(_), None) => Level::Namespace,
            (None, None) => Level::Catalog,
        };
        if named > deepest {
            let honoured = match deepest {
                Level::Catalog => catalog.to_string(),
                Level::Namespace => {
                    format!("{catalog}.{}", namespace.unwrap_or("<namespace>"))
                }
                Level::Table => unreachable!("Table is the deepest level"),
            };
            return Err(format!(
                "Privilege '{sql_priv}' binds no deeper than the {} level, but the \
                 statement names a {}. The policy would apply to '{honoured}' and \
                 everything under it, which is wider than the object named. Re-issue \
                 the statement against '{honoured}', or name a privilege that binds \
                 to the object you meant.",
                deepest.as_str(),
                named.as_str(),
            ));
        }

        let mut out = Vec::new();
        for lp in levels {
            if lp.level > named {
                break; // nothing to bind a deeper policy to
            }
            let mut resource = BTreeMap::new();
            if !realm.is_empty() {
                resource.insert("root".to_string(), realm.to_string());
            }
            resource.insert("catalog".to_string(), catalog.to_string());
            if matches!(lp.level, Level::Namespace | Level::Table) {
                if let Some(ns) = namespace {
                    resource.insert("namespace".to_string(), ns.to_string());
                }
            }
            if lp.level == Level::Table {
                if let Some(t) = table {
                    resource.insert("table".to_string(), t.to_string());
                }
            }
            let excluded: BTreeSet<&str> = lp.exclude.iter().map(String::as_str).collect();
            let access_types: Vec<String> = self
                .expand_access_types(&lp.seeds)
                .into_iter()
                .filter(|t| !excluded.contains(t.as_str()))
                .collect();
            out.push(PlannedPolicy {
                resource,
                access_types,
            });
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_pinned() {
        // Bumping the vendored profile is a deliberate act: the fixtures below and
        // the platform's contract move together. This fails on an accidental
        // refresh.
        assert_eq!(profile().version(), 5);
    }

    /// A profile with no `access_types` is refused at parse rather than treated as
    /// an empty graph.
    ///
    /// The failure it prevents is quiet. With an empty graph every seed expands to
    /// itself, so `INSERT` would confer `table-data-write` and nothing else, and
    /// every Iceberg commit would fail an authorization check inside Polaris with no
    /// hint that the profile was the cause. That is the safe direction and the
    /// undiagnosable one. Before v5 the graph came from a second file whose absence
    /// broke the build; now it is a field, and only `serde` stands there.
    #[test]
    fn a_profile_without_the_implication_graph_is_refused() {
        let no_graph = r#"{
            "version": 5,
            "privileges": {"SELECT": [{"level": "table", "seeds": ["table-data-read"]}]},
            "aliases": {}
        }"#;
        let err = serde_json::from_str::<ProfileFile>(no_graph)
            .expect_err("a profile with no access_types must not parse");
        assert!(
            err.to_string().contains("access_types"),
            "the error must name the missing field, got: {err}"
        );
        // Control: the same document parses once the graph is there, so the refusal
        // above is about that field and not about the rest of the shape.
        let with_graph = r#"{
            "version": 5,
            "privileges": {"SELECT": [{"level": "table", "seeds": ["table-data-read"]}]},
            "aliases": {},
            "access_types": {"table-data-read": ["table-list"]}
        }"#;
        serde_json::from_str::<ProfileFile>(with_graph).expect("parses with the graph");
    }

    #[test]
    fn golden_fixtures_match_the_platform() {
        // THE test for this module. Every fixture's `expect` is the platform's own
        // expansion, so this compares SQE's closure against theirs rather than
        // against itself.
        let p = profile();
        let mut checked = 0;
        for f in &p.fixtures {
            let catalog = f.catalog.as_deref().unwrap_or_default();
            let got = p
                .plan_grant(
                    &f.privilege,
                    "*",
                    catalog,
                    f.namespace.as_deref(),
                    f.table.as_deref(),
                )
                .unwrap_or_else(|e| {
                    panic!("fixture {} on {catalog:?} failed to plan: {e}", f.privilege)
                });
            let got: Vec<ExpectedPolicy> = got
                .into_iter()
                .map(|p| ExpectedPolicy {
                    resource: p.resource,
                    access_types: p.access_types,
                })
                .collect();
            assert_eq!(
                got, f.expect,
                "fixture mismatch for {} (namespace={:?}, table={:?})",
                f.privilege, f.namespace, f.table
            );
            checked += 1;
        }
        // A profile whose fixtures vanished would otherwise make this vacuous.
        assert_eq!(checked, 26, "expected 26 fixtures in v4");
    }

    #[test]
    fn rejects_are_refused() {
        let p = profile();
        let mut checked = 0;
        for r in &p.rejects {
            let got = p.plan_grant(
                &r.privilege,
                "*",
                r.catalog.as_deref().unwrap_or_default(),
                r.namespace.as_deref(),
                r.table.as_deref(),
            );
            assert!(
                got.is_err(),
                "{} on {:?}.{:?}.{:?} must be refused, got {got:?}",
                r.privilege,
                r.catalog,
                r.namespace,
                r.table
            );
            checked += 1;
        }
        assert_eq!(checked, 9, "expected 9 rejects in v4");
    }

    #[test]
    fn aliases_resolve_the_way_the_platform_spells_them() {
        let p = profile();
        for (alias, canonical) in [
            ("UPDATE", "INSERT"),
            ("DELETE", "INSERT"),
            ("MODIFY", "MODIFY"),
            ("USAGE", "USE"),
            ("CREATE TABLE", "CREATE"),
            ("CREATE SCHEMA", "CREATE NAMESPACE"),
            ("ALL PRIVILEGES", "ALL"),
        ] {
            assert_eq!(p.canonical_privilege(alias), canonical, "alias {alias}");
        }
        // Spelling is normalised, not just matched.
        assert_eq!(p.canonical_privilege("  create   table "), "CREATE");
        assert_eq!(p.canonical_privilege("select"), "SELECT");
    }

    #[test]
    fn exclude_is_subtracted_after_expansion() {
        // INSERT seeds `table-data-write`, whose closure is needed to commit an
        // Iceberg snapshot but also contains `table-location-set` -- repointing a
        // table's storage, which an append-only grantee must not get. Holding it
        // out of the seeds would have lost the rest of the closure, so it is
        // removed afterwards.
        let p = profile();
        let insert = p.plan_grant("INSERT", "*", "wh", Some("sales"), Some("orders")).expect("plan");
        let table = insert.last().expect("table level");
        assert!(
            table.access_types.contains(&"table-data-write".to_string()),
            "the seed itself must survive"
        );
        assert!(
            table.access_types.len() > 5,
            "the closure must have expanded, got {:?}",
            table.access_types
        );
        for banned in ["table-location-set", "table-uuid-assign", "table-format-version-upgrade"] {
            assert!(
                !table.access_types.contains(&banned.to_string()),
                "INSERT must not confer {banned}"
            );
        }
        // MODIFY carries no exclude, so it DOES get them. That contrast is the
        // reason INSERT and MODIFY are separate privileges.
        let modify = p.plan_grant("MODIFY", "*", "wh", Some("sales"), Some("orders")).expect("plan");
        assert!(
            modify.last().expect("table level").access_types.contains(&"table-location-set".to_string()),
            "MODIFY is the privilege that may repoint storage"
        );
    }

    #[test]
    fn a_plan_is_truncated_at_the_level_the_statement_names() {
        let p = profile();
        let three = p.plan_grant("SELECT", "*", "wh", Some("sales"), Some("orders")).expect("plan");
        assert_eq!(three.len(), 3);
        let two = p.plan_grant("SELECT", "*", "wh", Some("sales"), None).expect("plan");
        assert_eq!(two.len(), 2, "no table named, so no table-level policy");
        assert_eq!(two[1].resource.get("table"), None);
        let one = p.plan_grant("SELECT", "*", "wh", None, None).expect("plan");
        assert_eq!(one.len(), 1, "catalog only");
        assert_eq!(one[0].resource.get("namespace"), None);
    }

    #[test]
    fn an_unknown_privilege_is_refused_and_the_error_lists_what_is_known() {
        let p = profile();
        let err = p
            .plan_grant("TELEPORT", "*", "wh", Some("sales"), Some("orders"))
            .expect_err("unknown privilege");
        assert!(err.contains("TELEPORT"), "must name what was asked: {err}");
        assert!(err.contains("SELECT"), "must list known privileges: {err}");
    }

    #[test]
    fn the_realm_is_omitted_when_empty() {
        let p = profile();
        let plan = p.plan_grant("SELECT", "", "wh", Some("sales"), Some("orders")).expect("plan");
        assert!(plan.iter().all(|x| !x.resource.contains_key("root")));
    }

    #[test]
    fn expansion_terminates_on_a_cycle() {
        // Nothing in a service definition prevents an implied-grant cycle, and a
        // naive walk would hang the grant rather than fail it. Built by hand
        // because the real servicedef has no cycle to exercise.
        let mut implied = HashMap::new();
        implied.insert("a".to_string(), vec!["b".to_string()]);
        implied.insert("b".to_string(), vec!["a".to_string()]);
        let gp = GrantProfile {
            version: 0,
            privileges: BTreeMap::new(),
            aliases: BTreeMap::new(),
            implied,
            fixtures: vec![],
            rejects: vec![],
        };
        assert_eq!(
            gp.expand_access_types(&["a".to_string()]),
            vec!["a".to_string(), "b".to_string()]
        );
    }
}
