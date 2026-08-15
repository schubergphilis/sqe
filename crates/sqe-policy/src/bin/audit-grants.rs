//! Find, and optionally remove, access types a grantee holds beyond what the
//! current grant profile would give them.
//!
//! WHY THIS EXISTS. Ranger's grant endpoint MERGES access types into the policy
//! for a resource, and REVOKE removes only the types it names. So when SQE narrows
//! what a privilege confers, policies written by the older version keep the wider
//! set and no REVOKE issued afterwards can clear it. Adopting `grant-profile.json`
//! v4 narrowed `INSERT` by four access types, including `table-location-set` --
//! which lets its holder REPOINT a table's storage location. Every INSERT granted
//! before that change still carries it.
//!
//! WHAT IT DOES. For each `polaris` policy carrying SQE/platform provenance labels
//! (`chm:<TYPE>:<name>:<PRIVILEGE>`), it recomputes what the profile says that
//! grantee should hold at that resource, and reports anything extra.
//!
//! DRY RUN BY DEFAULT. Rewriting live access-control policy in bulk is not
//! something to do as a side effect of running a tool. `--apply` is required to
//! write, and it prints what it changed.
//!
//! WHAT IT DELIBERATELY WILL NOT TOUCH:
//!
//! - Policies with no `chm:` label. Without provenance there is no basis for
//!   deciding what SHOULD be there, and a hand-written operator policy is not this
//!   tool's business. Counted and reported, never modified.
//!
//! - Policy items for grantees the labels do not name. One policy can carry items
//!   from several sources; only labelled grantees are reasoned about.
//!
//! - Deny items. Narrowing a deny would GRANT access, which is the opposite of
//!   this tool's direction.
//!
//! Planning goes through `profile()`, the same code path a live GRANT uses, so this
//! cannot drift from what the engine would write today. That is the whole reason
//! this is a Rust binary and not a shell script over the JSON.
//!
//! Usage:
//!   RANGER_URL=http://localhost:26080 RANGER_PASSWORD=... \
//!     cargo run -p sqe-policy --bin audit-grants [-- --apply]

use std::collections::{BTreeMap, BTreeSet};

use sqe_policy::grants::profile::profile;

/// Provenance prefixes recognised on READ.
///
/// `chm` is the current one, shared with the data-platform control plane. `sqe` is
/// legacy: SQE briefly wrote its own prefix before the two were aligned, and this
/// tool exists precisely to clean up policies written by that older code, so
/// refusing to read them would make it report "nothing to do" on exactly the
/// deployments that need it. Verified against a live stack where every over-broad
/// item carried an `sqe:` label and the first version of this tool found none.
///
/// Read-only tolerance. Nothing here writes a label.
const LABEL_PREFIXES: &[&str] = &["chm", "sqe"];

/// (grantee name, PRIVILEGE) from a provenance label, or `None`.
///
/// `sqe:traversal:<TYPE>:<name>` markers are not grants -- an earlier version wrote
/// them on shared catalog/namespace policies -- and must not be read as one. Their
/// third segment is a grantee name, so treating it as a privilege would compute an
/// "expected" set from nonsense and could strip real access.
fn parse_label(label: &str) -> Option<(String, String)> {
    let rest = LABEL_PREFIXES
        .iter()
        .find_map(|p| label.strip_prefix(&format!("{p}:")))?;
    let (kind, rest) = rest.split_once(':')?;
    let kind = kind.trim().to_uppercase();
    if kind == "TRAVERSAL" {
        return None;
    }
    if !matches!(kind.as_str(), "USER" | "ROLE" | "GROUP") {
        return None;
    }
    let (name, privilege) = rest.rsplit_once(':')?;
    if name.is_empty() || privilege.is_empty() {
        return None;
    }
    Some((name.to_string(), privilege.trim().to_uppercase()))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let apply = std::env::args().any(|a| a == "--apply");
    let url = std::env::var("RANGER_URL").unwrap_or_else(|_| "http://localhost:26080".into());
    let url = url.trim_end_matches('/').to_string();
    let user = std::env::var("RANGER_USER").unwrap_or_else(|_| "admin".into());
    let password = std::env::var("RANGER_PASSWORD")
        .or_else(|_| std::env::var("RANGER_ADMIN_PASSWORD"))
        .map_err(|_| "set RANGER_PASSWORD (or RANGER_ADMIN_PASSWORD)")?;
    let service = std::env::var("RANGER_SERVICE").unwrap_or_else(|_| "polaris".into());
    let realm = std::env::var("RANGER_REALM").unwrap_or_else(|_| "*".into());

    let client = reqwest::Client::builder().build()?;
    let policies: Vec<serde_json::Value> = client
        .get(format!(
            "{url}/service/public/v2/api/policy?serviceName={service}"
        ))
        .basic_auth(&user, Some(&password))
        .send()
        .await?
        .json()
        .await?;

    println!(
        "audit-grants: {} policies on service '{service}' ({})\n",
        policies.len(),
        if apply { "APPLY" } else { "dry run" }
    );

    let mut unlabelled = 0usize;
    let mut findings = 0usize;
    let mut rewritten = 0usize;

    for mut policy in policies {
        let labels: Vec<String> = policy
            .get("policyLabels")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let provenance: Vec<(String, String)> =
            labels.iter().filter_map(|l| parse_label(l)).collect();
        if provenance.is_empty() {
            unlabelled += 1;
            continue;
        }

        // The resource this policy binds to, as the profile would name it.
        let res = policy.get("resources").cloned().unwrap_or_default();
        let val = |k: &str| -> Option<String> {
            res.get(k)?
                .get("values")?
                .as_array()?
                .first()?
                .as_str()
                .map(str::to_string)
        };
        let Some(catalog) = val("catalog") else {
            continue;
        };
        let namespace = val("namespace");
        let table = val("table");

        // What each labelled grantee SHOULD hold here, per the current profile.
        let mut expected: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for (name, privilege) in &provenance {
            match profile().plan_grant(
                privilege,
                &realm,
                &catalog,
                namespace.as_deref(),
                table.as_deref(),
            ) {
                Ok(plan) => {
                    if let Some(deepest) = plan.last() {
                        expected
                            .entry(name.clone())
                            .or_default()
                            .extend(deepest.access_types.iter().cloned());
                    }
                }
                Err(e) => {
                    // A label the profile no longer plans. Reported, never used as
                    // a basis for removal: treating it as "grants nothing" would
                    // strip access the operator did ask for.
                    println!("  ?  {name}: label privilege '{privilege}' does not plan ({e}); policy skipped");
                    expected.clear();
                    break;
                }
            }
        }
        if expected.is_empty() {
            continue;
        }

        let resource_label = [Some(catalog.clone()), namespace.clone(), table.clone()]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join(".");

        let mut changed = false;
        if let Some(items) = policy.get_mut("policyItems").and_then(|v| v.as_array_mut()) {
            for item in items.iter_mut() {
                for field in ["users", "roles"] {
                    let named: Vec<String> = item
                        .get(field)
                        .and_then(|v| v.as_array())
                        .map(|a| {
                            a.iter()
                                .filter_map(|v| v.as_str().map(str::to_string))
                                .collect()
                        })
                        .unwrap_or_default();
                    // Only an item naming EXACTLY one labelled grantee can be
                    // narrowed: with several, an access type may be owed to one of
                    // them and removing it would revoke from the wrong principal.
                    let labelled: Vec<&String> =
                        named.iter().filter(|n| expected.contains_key(*n)).collect();
                    if labelled.len() != 1 || named.len() != 1 {
                        continue;
                    }
                    let who = labelled[0].clone();
                    let allowed = &expected[&who];
                    let held: Vec<String> = item
                        .get("accesses")
                        .and_then(|v| v.as_array())
                        .map(|a| {
                            a.iter()
                                .filter_map(|x| {
                                    x.get("type").and_then(|t| t.as_str()).map(str::to_string)
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    let extra: Vec<&String> =
                        held.iter().filter(|t| !allowed.contains(*t)).collect();
                    if extra.is_empty() {
                        continue;
                    }
                    // FAIL-SAFE for labels written before they recorded the object
                    // kind. A view grant used to be labelled plain `SELECT`, so
                    // planning it yields TABLE access types and every `view-*` type
                    // the grantee legitimately holds looks like residue. Observed
                    // live on a view labelled `sqe:ROLE:analyst:SELECT`, where the
                    // first version of this tool would have stripped the whole
                    // grant.
                    //
                    // Two shapes are refused rather than guessed: a disjoint held
                    // set (nothing the plan produced is present, so the label almost
                    // certainly names a different privilege than was granted), and a
                    // strip that would empty the item. Both mean "the provenance does
                    // not describe this item", and removal is not recoverable.
                    let overlap = held.iter().filter(|t| allowed.contains(*t)).count();
                    if overlap == 0 {
                        println!(
                            "  ?  {resource_label}  {field}={who}\n     SKIPPED: label(s) plan \
                             {:?} but the item holds none of them. Most likely a label \
                             written before they recorded the object kind (a view grant \
                             labelled plain SELECT). Re-grant to refresh the label, then \
                             re-run.",
                            allowed.iter().take(3).collect::<Vec<_>>()
                        );
                        continue;
                    }
                    findings += 1;
                    println!(
                        "  !  {resource_label}  {field}={who}\n     beyond the profile: {}",
                        extra
                            .iter()
                            .map(|s| s.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                    if apply {
                        if let Some(arr) = item.get_mut("accesses").and_then(|v| v.as_array_mut()) {
                            arr.retain(|a| {
                                a.get("type")
                                    .and_then(|t| t.as_str())
                                    .is_some_and(|t| allowed.contains(t))
                            });
                            changed = true;
                        }
                    }
                }
            }
        }

        if apply && changed {
            let id = policy
                .get("id")
                .and_then(|v| v.as_i64())
                .ok_or("policy has no id")?;
            let resp = client
                .put(format!("{url}/service/public/v2/api/policy/{id}"))
                .basic_auth(&user, Some(&password))
                .header("X-XSRF-HEADER", "x")
                .json(&policy)
                .send()
                .await?;
            if resp.status().is_success() {
                rewritten += 1;
                println!("     -> policy {id} updated");
            } else {
                println!("     -> policy {id} FAILED (HTTP {})", resp.status());
            }
        }
    }

    println!(
        "\n{findings} over-broad item(s); {unlabelled} policy(ies) skipped for having no \
         provenance label{}",
        if apply {
            format!("; {rewritten} policy(ies) rewritten")
        } else {
            "\n\nDry run. Re-run with --apply to write these changes.".to_string()
        }
    );
    // Non-zero when something was found and not fixed, so this can gate.
    if findings > 0 && !apply {
        std::process::exit(1);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_prefixes_parse_and_traversal_markers_do_not() {
        assert_eq!(
            parse_label("chm:USER:dave:SELECT"),
            Some(("dave".into(), "SELECT".into()))
        );
        // The legacy prefix must still be readable, or this tool reports nothing to
        // do on the deployments it was written for.
        assert_eq!(
            parse_label("sqe:ROLE:analyst:INSERT"),
            Some(("analyst".into(), "INSERT".into()))
        );
        // A traversal marker is not a grant. Read as one, its third segment (a
        // grantee name) would be taken for a privilege.
        assert_eq!(parse_label("sqe:traversal:USER:dave"), None);
        assert_eq!(parse_label("other:USER:dave:SELECT"), None);
        assert_eq!(parse_label("chm:WIZARD:dave:SELECT"), None);
        // A grantee name may contain a colon.
        assert_eq!(
            parse_label("chm:USER:realm:dave:SELECT"),
            Some(("realm:dave".into(), "SELECT".into()))
        );
    }
}
