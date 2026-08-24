//! Partition-index pruning on typed values.
//!
//! Equality, IN-list, range, and IS NULL (after the cycle-1 sentinel rewrite
//! that turns `IS NULL` into `= __HIVE_DEFAULT_PARTITION__`) are decided
//! against each entry before any file group is built. An expression this
//! module cannot evaluate does not drop the entry.

use chrono::NaiveDate;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{Expr, Operator};

use super::manifest::{PartitionIndexEntry, HIVE_DEFAULT_PARTITION};

/// Keep the index entries that can satisfy `filters`. Filters should already
/// have gone through [`super::provider`]'s NULL-sentinel rewrite so `IS NULL`
/// on a partition column is an equality against the sentinel.
pub fn prune_entries<'a>(
    entries: &'a [PartitionIndexEntry],
    filters: &[Expr],
    partition_types: &[(String, String)],
) -> Vec<&'a PartitionIndexEntry> {
    if filters.is_empty() {
        return entries.iter().collect();
    }
    entries
        .iter()
        .filter(|e| filters.iter().all(|f| matches_or_unknown(e, f, partition_types)))
        .collect()
}

fn matches_or_unknown(
    entry: &PartitionIndexEntry,
    expr: &Expr,
    types: &[(String, String)],
) -> bool {
    eval(entry, expr, types).unwrap_or(true)
}

fn eval(entry: &PartitionIndexEntry, expr: &Expr, types: &[(String, String)]) -> Option<bool> {
    match expr {
        Expr::BinaryExpr(b) => match b.op {
            Operator::And => match (eval(entry, &b.left, types), eval(entry, &b.right, types)) {
                (Some(false), _) | (_, Some(false)) => Some(false),
                (Some(true), Some(true)) => Some(true),
                _ => None,
            },
            Operator::Or => match (eval(entry, &b.left, types), eval(entry, &b.right, types)) {
                (Some(true), _) | (_, Some(true)) => Some(true),
                (Some(false), Some(false)) => Some(false),
                _ => None,
            },
            Operator::Eq | Operator::NotEq | Operator::Lt | Operator::LtEq | Operator::Gt
            | Operator::GtEq => cmp_entry(entry, &b.left, &b.right, b.op, types),
            _ => None,
        },
        Expr::InList(list) => {
            let Expr::Column(c) = list.expr.as_ref() else {
                return None;
            };
            let raw = entry.values.get(&c.name)?;
            let sql_type = type_of(&c.name, types);
            let mut any = false;
            let mut all_known = true;
            for item in &list.list {
                let Expr::Literal(sv, _) = item else {
                    all_known = false;
                    continue;
                };
                match compare_raw(raw, sv, sql_type) {
                    Some(0) => any = true,
                    Some(_) => {}
                    None => all_known = false,
                }
            }
            if list.negated {
                if any {
                    Some(false)
                } else if all_known {
                    Some(true)
                } else {
                    None
                }
            } else if any {
                Some(true)
            } else if all_known {
                Some(false)
            } else {
                None
            }
        }
        Expr::IsNull(inner) => {
            let Expr::Column(c) = inner.as_ref() else {
                return None;
            };
            Some(entry.values.get(&c.name).map(String::as_str) == Some(HIVE_DEFAULT_PARTITION))
        }
        Expr::IsNotNull(inner) => {
            let Expr::Column(c) = inner.as_ref() else {
                return None;
            };
            Some(entry.values.get(&c.name).map(String::as_str) != Some(HIVE_DEFAULT_PARTITION))
        }
        _ => None,
    }
}

fn cmp_entry(
    entry: &PartitionIndexEntry,
    left: &Expr,
    right: &Expr,
    op: Operator,
    types: &[(String, String)],
) -> Option<bool> {
    let (col, lit) = match (left, right) {
        (Expr::Column(c), Expr::Literal(sv, _)) => (c, sv),
        (Expr::Literal(sv, _), Expr::Column(c)) => {
            let flipped = match op {
                Operator::Lt => Operator::Gt,
                Operator::LtEq => Operator::GtEq,
                Operator::Gt => Operator::Lt,
                Operator::GtEq => Operator::LtEq,
                other => other,
            };
            return cmp_col_lit(entry, &c.name, sv, flipped, types);
        }
        _ => return None,
    };
    cmp_col_lit(entry, &col.name, lit, op, types)
}

fn cmp_col_lit(
    entry: &PartitionIndexEntry,
    col: &str,
    lit: &ScalarValue,
    op: Operator,
    types: &[(String, String)],
) -> Option<bool> {
    let raw = entry.values.get(col)?;
    if raw == HIVE_DEFAULT_PARTITION {
        // NULL compared with anything (except the sentinel equality rewrite)
        // is unknown; the rewrite turns IS NULL into `= sentinel`, which
        // compares as strings and hits the equal-sentinel case below.
        if let ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) = lit {
            if s == HIVE_DEFAULT_PARTITION {
                return Some(matches!(op, Operator::Eq));
            }
        }
        return Some(false);
    }
    let ord = compare_raw(raw, lit, type_of(col, types))?;
    Some(match op {
        Operator::Eq => ord == 0,
        Operator::NotEq => ord != 0,
        Operator::Lt => ord < 0,
        Operator::LtEq => ord <= 0,
        Operator::Gt => ord > 0,
        Operator::GtEq => ord >= 0,
        _ => return None,
    })
}

fn type_of<'a>(col: &str, types: &'a [(String, String)]) -> &'a str {
    types
        .iter()
        .find(|(n, _)| n == col)
        .map(|(_, t)| t.as_str())
        .unwrap_or("string")
}

fn compare_raw(raw: &str, lit: &ScalarValue, sql_type: &str) -> Option<i32> {
    let base = sql_type
        .trim()
        .to_ascii_lowercase();
    let base = base.split('(').next().unwrap_or(&base);
    match base {
        "tinyint" | "smallint" | "int" | "integer" | "bigint" => {
            let left = raw.parse::<i64>().ok()?;
            let right = scalar_i64(lit)?;
            Some(left.cmp(&right) as i32)
        }
        "date" => {
            let left = NaiveDate::parse_from_str(raw, "%Y-%m-%d").ok()?;
            let right = scalar_date(lit)?;
            Some(left.cmp(&right) as i32)
        }
        _ => {
            let right = scalar_utf8(lit)?;
            Some(raw.cmp(right) as i32)
        }
    }
}

fn scalar_i64(sv: &ScalarValue) -> Option<i64> {
    use ScalarValue::*;
    match sv {
        Int8(Some(v)) => Some(*v as i64),
        Int16(Some(v)) => Some(*v as i64),
        Int32(Some(v)) => Some(*v as i64),
        Int64(Some(v)) => Some(*v),
        UInt8(Some(v)) => Some(*v as i64),
        UInt16(Some(v)) => Some(*v as i64),
        UInt32(Some(v)) => Some(*v as i64),
        UInt64(Some(v)) if *v <= i64::MAX as u64 => Some(*v as i64),
        Utf8(Some(s)) | LargeUtf8(Some(s)) => s.parse().ok(),
        _ => None,
    }
}

fn scalar_date(sv: &ScalarValue) -> Option<NaiveDate> {
    use ScalarValue::*;
    match sv {
        Date32(Some(d)) => NaiveDate::from_ymd_opt(1970, 1, 1)
            .and_then(|e| e.checked_add_signed(chrono::Duration::days(*d as i64))),
        Utf8(Some(s)) | LargeUtf8(Some(s)) => NaiveDate::parse_from_str(s, "%Y-%m-%d").ok(),
        _ => None,
    }
}

fn scalar_utf8(sv: &ScalarValue) -> Option<&str> {
    use ScalarValue::*;
    match sv {
        Utf8(Some(s)) | LargeUtf8(Some(s)) | Utf8View(Some(s)) => Some(s.as_str()),
        _ => None,
    }
}

/// Owned copy of pruned entries, used when the caller needs to store them.
pub fn prune_entries_owned(
    entries: &[PartitionIndexEntry],
    filters: &[Expr],
    partition_types: &[(String, String)],
) -> Vec<PartitionIndexEntry> {
    prune_entries(entries, filters, partition_types)
        .into_iter()
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use datafusion::common::Column;
    use datafusion::logical_expr::lit;

    fn entry(dt: &str) -> PartitionIndexEntry {
        let mut values = BTreeMap::new();
        values.insert("dt".into(), dt.into());
        PartitionIndexEntry {
            values,
            location: format!("s3://x/dt={dt}/"),
            file_count: 1,
            total_bytes: 10,
        }
    }

    fn types() -> Vec<(String, String)> {
        vec![("dt".into(), "string".into())]
    }

    fn col(name: &str) -> Expr {
        Expr::Column(Column::new_unqualified(name))
    }

    #[test]
    fn equality_keeps_only_the_matching_entry() {
        let entries = vec![entry("2026-01-01"), entry("2026-01-02")];
        let filters = vec![col("dt").eq(lit("2026-01-02"))];
        let out = prune_entries(&entries, &filters, &types());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].values.get("dt").unwrap(), "2026-01-02");
    }

    #[test]
    fn in_list_keeps_listed_values() {
        let entries = vec![entry("a"), entry("b"), entry("c")];
        let filters = vec![col("dt").in_list(vec![lit("a"), lit("c")], false)];
        let out = prune_entries(&entries, &filters, &types());
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn range_on_int_column() {
        let mut entries = Vec::new();
        for n in 1..=5 {
            let mut values = BTreeMap::new();
            values.insert("n".into(), n.to_string());
            entries.push(PartitionIndexEntry {
                values,
                location: format!("s3://x/n={n}/"),
                file_count: 1,
                total_bytes: 1,
            });
        }
        let types = vec![("n".into(), "int".into())];
        let filters = vec![col("n").gt_eq(lit(3i32)).and(col("n").lt(lit(5i32)))];
        let out = prune_entries(&entries, &filters, &types);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].values.get("n").unwrap(), "3");
        assert_eq!(out[1].values.get("n").unwrap(), "4");
    }

    #[test]
    fn sentinel_equality_matches_null_partition() {
        let entries = vec![entry("2026-01-01"), entry(HIVE_DEFAULT_PARTITION)];
        let filters = vec![col("dt").eq(lit(HIVE_DEFAULT_PARTITION))];
        let out = prune_entries(&entries, &filters, &types());
        assert_eq!(out.len(), 1);
        assert_eq!(
            out[0].values.get("dt").unwrap(),
            HIVE_DEFAULT_PARTITION
        );
    }

    #[test]
    fn unknown_filter_does_not_drop_entries() {
        let entries = vec![entry("a"), entry("b")];
        let filters = vec![col("amount").gt(lit(1.0))];
        let out = prune_entries(&entries, &filters, &types());
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn and_of_equality_and_unknown_keeps_the_match() {
        let entries = vec![entry("a"), entry("b")];
        let filters = vec![col("dt").eq(lit("a")).and(col("amount").gt(lit(1.0)))];
        let out = prune_entries(&entries, &filters, &types());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].values.get("dt").unwrap(), "a");
    }

    #[test]
    fn or_keeps_either_side() {
        let entries = vec![entry("a"), entry("b"), entry("c")];
        let filters = vec![col("dt").eq(lit("a")).or(col("dt").eq(lit("c")))];
        let out = prune_entries(&entries, &filters, &types());
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn not_in_drops_listed_values() {
        let entries = vec![entry("a"), entry("b"), entry("c")];
        let filters = vec![col("dt").in_list(vec![lit("a"), lit("c")], true)];
        let out = prune_entries(&entries, &filters, &types());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].values.get("dt").unwrap(), "b");
    }

    #[test]
    fn date_equality_types_the_literal() {
        let mut values = BTreeMap::new();
        values.insert("dt".into(), "2026-01-02".into());
        let entries = vec![PartitionIndexEntry {
            values,
            location: "s3://x/dt=2026-01-02/".into(),
            file_count: 1,
            total_bytes: 1,
        }];
        let types = vec![("dt".into(), "date".into())];
        let filters = vec![col("dt").eq(lit("2026-01-02"))];
        assert_eq!(prune_entries(&entries, &filters, &types).len(), 1);
        let miss = vec![col("dt").eq(lit("2026-01-03"))];
        assert!(prune_entries(&entries, &miss, &types).is_empty());
    }

    #[test]
    fn zero_matches_is_empty_not_an_error() {
        let entries = vec![entry("a")];
        let filters = vec![col("dt").eq(lit("zzz"))];
        assert!(prune_entries(&entries, &filters, &types()).is_empty());
    }

    #[test]
    fn is_null_keeps_only_the_sentinel() {
        let entries = vec![entry("2026-01-01"), entry(HIVE_DEFAULT_PARTITION)];
        let filters = vec![col("dt").is_null()];
        let out = prune_entries(&entries, &filters, &types());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].values.get("dt").unwrap(), HIVE_DEFAULT_PARTITION);
    }

    #[test]
    fn is_not_null_drops_the_sentinel() {
        let entries = vec![entry("2026-01-01"), entry(HIVE_DEFAULT_PARTITION)];
        let filters = vec![col("dt").is_not_null()];
        let out = prune_entries(&entries, &filters, &types());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].values.get("dt").unwrap(), "2026-01-01");
    }

    #[test]
    fn flipped_literal_on_the_left_compares_with_the_operator_reversed() {
        let mut entries = Vec::new();
        for n in 1..=3 {
            let mut values = BTreeMap::new();
            values.insert("n".into(), n.to_string());
            entries.push(PartitionIndexEntry {
                values,
                location: format!("s3://x/n={n}/"),
                file_count: 1,
                total_bytes: 1,
            });
        }
        let types = vec![("n".into(), "int".into())];
        // `2 < n` keeps 3 only.
        let filters = vec![lit(2i32).lt(col("n"))];
        let out = prune_entries(&entries, &filters, &types);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].values.get("n").unwrap(), "3");
    }

    #[test]
    fn sentinel_does_not_satisfy_a_range() {
        let mut values = BTreeMap::new();
        values.insert("n".into(), HIVE_DEFAULT_PARTITION.into());
        let entries = vec![PartitionIndexEntry {
            values,
            location: "s3://x/n=__HIVE_DEFAULT_PARTITION__/".into(),
            file_count: 1,
            total_bytes: 1,
        }];
        let types = vec![("n".into(), "int".into())];
        let filters = vec![col("n").gt(lit(0i32))];
        assert!(prune_entries(&entries, &filters, &types).is_empty());
    }

    #[test]
    fn empty_filters_keep_every_entry() {
        let entries = vec![entry("a"), entry("b")];
        assert_eq!(prune_entries(&entries, &[], &types()).len(), 2);
    }

    #[test]
    fn varchar_compares_as_string() {
        let entries = vec![entry("eu"), entry("us")];
        let types = vec![("dt".into(), "varchar".into())];
        let filters = vec![col("dt").eq(lit("eu"))];
        assert_eq!(prune_entries(&entries, &filters, &types).len(), 1);
    }

    #[test]
    fn bigint_equality_accepts_an_i64_literal() {
        let mut values = BTreeMap::new();
        values.insert("n".into(), "42".into());
        let entries = vec![PartitionIndexEntry {
            values,
            location: "s3://x/n=42/".into(),
            file_count: 1,
            total_bytes: 1,
        }];
        let types = vec![("n".into(), "bigint".into())];
        let hit = vec![col("n").eq(lit(42i64))];
        assert_eq!(prune_entries(&entries, &hit, &types).len(), 1);
        let miss = vec![col("n").eq(lit(7i64))];
        assert!(prune_entries(&entries, &miss, &types).is_empty());
    }

    #[test]
    fn date_range_keeps_the_inclusive_start() {
        let mut entries = Vec::new();
        for d in ["2026-01-01", "2026-01-02", "2026-01-03"] {
            let mut values = BTreeMap::new();
            values.insert("dt".into(), d.into());
            entries.push(PartitionIndexEntry {
                values,
                location: format!("s3://x/dt={d}/"),
                file_count: 1,
                total_bytes: 1,
            });
        }
        let types = vec![("dt".into(), "date".into())];
        let filters = vec![col("dt")
            .gt_eq(lit("2026-01-02"))
            .and(col("dt").lt(lit("2026-01-03")))];
        let out = prune_entries(&entries, &filters, &types);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].values.get("dt").unwrap(), "2026-01-02");
    }

    #[test]
    fn unknown_is_null_shape_does_not_drop_entries() {
        let entries = vec![entry("a"), entry("b")];
        // IS NULL on a non-column expression is unevaluable -> keep.
        let filters = vec![lit("x").is_null()];
        assert_eq!(prune_entries(&entries, &filters, &types()).len(), 2);
    }
}
