//! Tags on a table, schema or catalog itself, Databricks' spelling (#922):
//!
//! ```sql
//! ALTER TABLE c.s.t SET TAGS ('owner' = 'finance', 'gold')
//! ALTER TABLE c.s.t UNSET TAGS ('owner')
//! ALTER SCHEMA c.s SET TAGS ('domain' = 'sales')
//! ALTER CATALOG c SET TAGS ('env' = 'prod')
//! ```
//!
//! On a table the list must start with a string literal: the native column
//! form `SET TAGS (col = ('tag'))` starts with an identifier and stays with
//! [`crate::tags::try_parse_set_tags`]. On a schema or catalog there is no
//! column form, so bare and double-quoted keys are accepted too.

use std::collections::BTreeMap;

use sqe_core::{Result, SqeError, SqeErrorCode};

use crate::tags::{
    is_object_tag_list, keys_and_values, parse_databricks_tag_list, split_identifier, strip_kw,
    TagAction,
};

/// The object a tag statement names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TagObject {
    Table(String),
    /// `ALTER SCHEMA` or `ALTER DATABASE`.
    Schema(String),
    Catalog(String),
}

impl TagObject {
    /// The name as written (dotted, quotes removed).
    pub fn name(&self) -> &str {
        match self {
            Self::Table(n) | Self::Schema(n) | Self::Catalog(n) => n,
        }
    }

    /// `TABLE`, `SCHEMA` or `CATALOG`, for messages.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Table(_) => "TABLE",
            Self::Schema(_) => "SCHEMA",
            Self::Catalog(_) => "CATALOG",
        }
    }
}

/// `ALTER TABLE|SCHEMA|DATABASE|CATALOG <name> SET|UNSET TAGS (...)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectTagsStatement {
    pub object: TagObject,
    pub action: TagAction,
    /// The keys named, in order, without duplicates.
    pub tags: Vec<String>,
    /// Values of the keys given one, by key. A `Set` key with no entry
    /// stores the key alone (`'k' = ''` included). Always empty for `Unset`.
    pub values: BTreeMap<String, String>,
}

/// Parse an object tag statement. `Ok(None)` when `sql` is not one, so the
/// caller tries its other parsers. `Err` when it is one but malformed.
pub fn try_parse_object_tags(sql: &str) -> Result<Option<ObjectTagsStatement>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    let (keyword, make): (&str, fn(String) -> TagObject) = if upper.starts_with("ALTER TABLE ") {
        ("ALTER TABLE ", TagObject::Table)
    } else if upper.starts_with("ALTER SCHEMA ") {
        ("ALTER SCHEMA ", TagObject::Schema)
    } else if upper.starts_with("ALTER DATABASE ") {
        ("ALTER DATABASE ", TagObject::Schema)
    } else if upper.starts_with("ALTER CATALOG ") {
        ("ALTER CATALOG ", TagObject::Catalog)
    } else {
        return Ok(None);
    };
    let (name, rest) = split_identifier(&trimmed[keyword.len()..])?;
    let rest = rest.trim_start();
    let rest_upper = rest.to_ascii_uppercase();
    let (action, context, body) = if let Some(b) = strip_kw(rest, &rest_upper, "SET TAGS") {
        (TagAction::Set, "SET TAGS", b)
    } else if let Some(b) = strip_kw(rest, &rest_upper, "UNSET TAGS") {
        (TagAction::Unset, "UNSET TAGS", b)
    } else {
        return Ok(None);
    };
    let object = make(name);
    if matches!(object, TagObject::Table(_)) && !is_object_tag_list(body, context)? {
        return Ok(None);
    }
    let pairs = parse_databricks_tag_list(body, context)?;
    if action == TagAction::Unset {
        if let Some((key, _)) = pairs.iter().find(|(_, v)| v.is_some()) {
            return Err(SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                format!("UNSET TAGS: name the tag to remove without a value, got `{key} = ...`"),
            ));
        }
    }
    let (tags, values) = keys_and_values(pairs);
    let values = match action {
        TagAction::Set => values,
        TagAction::Unset => BTreeMap::new(),
    };
    Ok(Some(ObjectTagsStatement {
        object,
        action,
        tags,
        values,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(sql: &str) -> ObjectTagsStatement {
        try_parse_object_tags(sql)
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .unwrap_or_else(|| panic!("{sql}: not recognised"))
    }

    #[test]
    fn table_set_keeps_order_values_and_key_only_tags() {
        let s = parse("ALTER TABLE c.s.t SET TAGS ('owner' = 'finance', 'gold', 'blank' = '');");
        assert_eq!(s.object, TagObject::Table("c.s.t".into()));
        assert_eq!(s.action, TagAction::Set);
        assert_eq!(s.tags, ["owner", "gold", "blank"]);
        assert_eq!(
            s.values,
            BTreeMap::from([("owner".to_string(), "finance".to_string())])
        );
    }

    #[test]
    fn schema_database_and_catalog_forms() {
        let s = parse("alter schema c.s set tags ('domain' = 'sales')");
        assert_eq!(s.object, TagObject::Schema("c.s".into()));
        let s = parse("ALTER DATABASE s UNSET TAGS ('domain', \"other\")");
        assert_eq!(s.object, TagObject::Schema("s".into()));
        assert_eq!(s.action, TagAction::Unset);
        assert_eq!(s.tags, ["domain", "other"]);
        // No column form on a schema: a bare key is a key.
        let s = parse("ALTER SCHEMA s SET TAGS (gold)");
        assert_eq!(s.tags, ["gold"]);
        let s = parse("ALTER CATALOG c SET TAGS ('env' = 'prod')");
        assert_eq!(s.object, TagObject::Catalog("c".into()));
    }

    #[test]
    fn the_native_column_forms_are_not_claimed() {
        for sql in [
            "ALTER TABLE t SET TAGS (email = ('pii'))",
            "ALTER TABLE t UNSET TAGS (email)",
            "ALTER TABLE t ALTER COLUMN email SET TAGS ('k' = 'v')",
            "ALTER TABLE t SET TBLPROPERTIES ('k' = 'v')",
            "ALTER SCHEMA s RENAME TO s2",
            "SELECT 1",
        ] {
            assert_eq!(try_parse_object_tags(sql).expect(sql), None, "{sql}");
        }
    }

    #[test]
    fn malformed_lists_are_errors() {
        for sql in [
            "ALTER TABLE t SET TAGS ('k' = 'v', email = ('pii'))",
            "ALTER TABLE t UNSET TAGS ('k' = 'v')",
            "ALTER SCHEMA s SET TAGS ()",
            "ALTER SCHEMA s SET TAGS ('k' = )",
            "ALTER CATALOG c SET TAGS 'k'",
        ] {
            assert!(try_parse_object_tags(sql).is_err(), "must error: {sql}");
        }
    }
}
