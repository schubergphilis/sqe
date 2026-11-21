//! `[access_control]` settings for column tag writes: who may write tags,
//! which privilege and names that takes, and what removing a tag needs.

use serde::Deserialize;

/// Where the authority to write column tags comes from: every statement that
/// writes the `sqe.column-tags` table property (`ALTER TABLE ... SET / UNSET
/// TAGS`, `SET / UNSET MASKING POLICY`, `SET TBLPROPERTIES`, `CREATE [OR
/// REPLACE] TABLE ... TBLPROPERTIES`).
///
/// - `admin-role` (default) requires an `[auth] admin_roles` role for the tag
///   statements and for `TBLPROPERTIES` that change the reserved key.
/// - `ranger-delegate` also lets a non-admin tag a column when the Ranger
///   policies grant them [`TagPrivilege`] on that column. With the default
///   [`TagRemoval`], a broad grant then becomes the right to untag a column
///   and so remove its mask; review the grants, or set `tag_removal`, before
///   turning it on.
///
/// Admins pass in both modes. Backends without a column privilege model grant
/// nothing, so only admins can tag there.
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum TagAuthority {
    #[default]
    AdminRole,
    RangerDelegate,
}

/// The Ranger `hive` access type a non-admin needs on a column to change its
/// tags under `tag_authority = "ranger-delegate"`.
///
/// - `alter` (default) is the 0.55 behaviour: whoever may alter the table may
///   tag its columns.
/// - `apply_tag` asks for the custom access type `apply_tag` instead, the
///   Databricks `APPLY TAG` model: tagging is its own grant, held apart from
///   the right to change data or schema. It is not implied by `alter`, by any
///   write type, or by an allow item's `all`; a deny item naming `apply_tag`
///   or `all` still denies it. The access type has to exist in the `hive`
///   servicedef (`{"name":"apply_tag","impliedGrants":[]}`) before Ranger
///   accepts a policy that names it.
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
pub enum TagPrivilege {
    #[default]
    #[serde(rename = "alter")]
    Alter,
    #[serde(rename = "apply_tag", alias = "apply-tag")]
    ApplyTag,
}

/// What a non-admin needs, under `tag_authority = "ranger-delegate"`, to
/// remove a tag key from a column (#927). Removing a tag removes the mask it
/// drives, so this can be a stronger right than adding one.
///
/// - `same` (default): the [`TagPrivilege`] alone, as before.
/// - `remove_tag`: also the custom Ranger access type `remove_tag` on the
///   column. Like `apply_tag`, nothing implies it (`alter`, `apply_tag`, a
///   write type, an allow item's `all`), and it has to exist in the `hive`
///   servicedef.
/// - `admin`: an `[auth] admin_roles` role.
///
/// A removal is `UNSET TAGS`, `UNSET MASKING POLICY`, or a `sqe.column-tags`
/// write that drops a key a column carries. A changed value on a kept key is
/// not one. Under `admin-role` this has no effect.
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
pub enum TagRemoval {
    #[default]
    #[serde(rename = "same")]
    Same,
    #[serde(rename = "remove_tag", alias = "remove-tag")]
    RemoveTag,
    #[serde(rename = "admin")]
    Admin,
}

/// Which tag names a non-admin may write to `sqe.column-tags`.
///
/// - `any` (default) accepts every name, as before. A typo (`live.pi` for
///   `live.pii`) then tags the column with a name no tag policy matches, and
///   the column stays unmasked.
/// - `ranger-tagdefs` accepts only a name Ranger holds a tag definition for
///   (`GET /service/tags/tagdefs`). Under `[policy.ranger] catalog_scoped`, a
///   definition named `<catalog>.<tag>` also defines `<tag>` in that catalog.
///   The check fails closed: when the definitions cannot be read, or the
///   policy backend is not Ranger, a non-admin tag write is refused.
///
/// Admins are exempt in both modes. Only names a write sets are checked:
/// `ALTER TABLE ... SET TAGS` checks every name it sets, a raw `sqe.column-tags`
/// write the tags it adds to a column, and `UNSET TAGS` is never refused by
/// this check ([`TagRemoval`] decides who may remove a tag).
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum TagNames {
    #[default]
    Any,
    RangerTagdefs,
}

#[cfg(test)]
mod tests {
    use super::super::AccessControlConfig;
    use super::*;

    #[test]
    fn tag_removal_defaults_to_same_and_spells_remove_tag() {
        let c: AccessControlConfig = toml::from_str(r#"backend = "ranger""#).unwrap();
        assert_eq!(c.tag_removal, TagRemoval::Same, "unchanged by default");
        for (line, want) in [
            (r#"tag_removal = "same""#, TagRemoval::Same),
            (r#"tag_removal = "remove_tag""#, TagRemoval::RemoveTag),
            (r#"tag-removal = "remove-tag""#, TagRemoval::RemoveTag),
            (r#"tag_removal = "admin""#, TagRemoval::Admin),
        ] {
            let c: AccessControlConfig =
                toml::from_str(&format!("backend = \"ranger\"\n{line}\n")).unwrap();
            assert_eq!(c.tag_removal, want, "{line}");
        }
        for bad in ["REMOVE_TAG", "removetag", "admin-role", "all", ""] {
            let line = format!("tag_removal = \"{bad}\"");
            assert!(
                toml::from_str::<AccessControlConfig>(&line).is_err(),
                "{bad:?} must be refused, not defaulted"
            );
        }
    }
}
