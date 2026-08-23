//! Athena partition projection: parse/validate `projection.*` TBLPROPERTIES
//! and materialize a bounded virtual partition set at plan time.
//!
//! Validated at CREATE and on every manifest load (untrusted input). Unknown
//! keys error by name. Projected tables never list the table prefix to
//! discover partitions; the computed set is the plan.

use std::collections::BTreeMap;

use chrono::{Datelike, Duration, NaiveDate};
use datafusion::logical_expr::{Expr, Operator};

use super::manifest::{
    GlueColumn, PartitionIndexEntry, HIVE_DEFAULT_PARTITION,
};
use super::HiveError;

/// Parsed projection configuration for one table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionConfig {
    pub enabled: bool,
    pub columns: Vec<ProjectedColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedColumn {
    pub name: String,
    pub kind: ProjectionKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionKind {
    Enum {
        values: Vec<String>,
    },
    Integer {
        start: i64,
        end: i64,
        interval: i64,
        digits: Option<usize>,
    },
    Date {
        start: NaiveDate,
        end: NaiveDate,
        format: String,
        interval: i64,
        unit: DateUnit,
    },
    Injected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DateUnit {
    Days,
    Months,
    Years,
}

/// `true` when `projection.enabled` is the string `true` (case-insensitive).
pub fn projection_enabled(parameters: &BTreeMap<String, String>) -> bool {
    parameters
        .get("projection.enabled")
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Parse and validate every `projection.*` property. `Ok(None)` when the
/// table does not declare any projection property. An unknown key or a
/// malformed value is an error naming the property.
pub fn parse_projection(
    parameters: &BTreeMap<String, String>,
    partition_keys: &[GlueColumn],
) -> Result<Option<ProjectionConfig>, HiveError> {
    let mut proj_keys: Vec<(&str, &str)> = parameters
        .iter()
        .filter(|(k, _)| k.starts_with("projection."))
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    if proj_keys.is_empty() {
        return Ok(None);
    }
    proj_keys.sort_by_key(|(k, _)| *k);

    let enabled = match parameters.get("projection.enabled") {
        Some(v) if v.eq_ignore_ascii_case("true") => true,
        Some(v) if v.eq_ignore_ascii_case("false") => false,
        Some(v) => {
            return Err(HiveError::Validation(format!(
                "projection.enabled={v} is not true or false"
            )))
        }
        None => {
            return Err(HiveError::Validation(
                "projection.* properties require projection.enabled=true (or false)".to_string(),
            ))
        }
    };

    let declared: BTreeMap<&str, &GlueColumn> = partition_keys
        .iter()
        .map(|k| (k.name.as_str(), k))
        .collect();

    let mut by_col: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    for (key, value) in proj_keys {
        if key == "projection.enabled" {
            continue;
        }
        let rest = key.strip_prefix("projection.").unwrap_or(key);
        let (col, attr) = rest.split_once('.').ok_or_else(|| {
            HiveError::Validation(format!("unknown projection property `{key}`"))
        })?;
        if !matches!(
            attr,
            "type" | "values" | "range" | "interval" | "format" | "digits"
        ) && attr != "interval.unit"
            && !rest.ends_with(".interval.unit")
        {
            // `projection.dt.interval.unit` splits as col=dt, attr=interval.unit
            // after the first dot only if we special-case it. `rest` is
            // `dt.interval.unit` -> col=dt, attr=interval.unit.
            if rest.matches('.').count() == 2 && rest.ends_with(".interval.unit") {
                let col = rest.trim_end_matches(".interval.unit");
                if !declared.contains_key(col) {
                    return Err(HiveError::Validation(format!(
                        "projection.{col}.interval.unit names `{col}`, which is not a declared \
                         partition key"
                    )));
                }
                by_col
                    .entry(col.to_string())
                    .or_default()
                    .insert("interval.unit".to_string(), value.to_string());
                continue;
            }
            return Err(HiveError::Validation(format!(
                "unknown projection property `{key}`"
            )));
        }
        if !declared.contains_key(col) {
            return Err(HiveError::Validation(format!(
                "`{key}` names `{col}`, which is not a declared partition key"
            )));
        }
        by_col
            .entry(col.to_string())
            .or_default()
            .insert(attr.to_string(), value.to_string());
    }

    if enabled {
        for key in declared.keys() {
            if !by_col.contains_key(*key) || !by_col[*key].contains_key("type") {
                return Err(HiveError::Validation(format!(
                    "projection.enabled=true requires projection.{key}.type on every \
                     partition key"
                )));
            }
        }
    }

    let mut columns = Vec::new();
    for (name, attrs) in by_col {
        let kind_raw = attrs.get("type").ok_or_else(|| {
            HiveError::Validation(format!("projection.{name}.type is required"))
        })?;
        let kind = match kind_raw.to_ascii_lowercase().as_str() {
            "enum" => {
                let values = attrs.get("values").ok_or_else(|| {
                    HiveError::Validation(format!(
                        "projection.{name}.values is required for type=enum"
                    ))
                })?;
                let values: Vec<String> = values
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                if values.is_empty() {
                    return Err(HiveError::Validation(format!(
                        "projection.{name}.values must list at least one value"
                    )));
                }
                ProjectionKind::Enum { values }
            }
            "integer" => {
                let range = attrs.get("range").ok_or_else(|| {
                    HiveError::Validation(format!(
                        "projection.{name}.range is required for type=integer"
                    ))
                })?;
                let (start, end) = parse_int_range(range, &name)?;
                let interval = attrs
                    .get("interval")
                    .map(|s| parse_positive_i64(s, &format!("projection.{name}.interval")))
                    .transpose()?
                    .unwrap_or(1);
                let digits = attrs
                    .get("digits")
                    .map(|s| {
                        s.parse::<usize>().map_err(|_| {
                            HiveError::Validation(format!(
                                "projection.{name}.digits={s} is not a non-negative integer"
                            ))
                        })
                    })
                    .transpose()?;
                ProjectionKind::Integer {
                    start,
                    end,
                    interval,
                    digits,
                }
            }
            "date" => {
                let range = attrs.get("range").ok_or_else(|| {
                    HiveError::Validation(format!(
                        "projection.{name}.range is required for type=date"
                    ))
                })?;
                let format = attrs
                    .get("format")
                    .cloned()
                    .unwrap_or_else(|| "yyyy-MM-dd".to_string());
                let (start, end) = parse_date_range(range, &format, &name)?;
                let interval = attrs
                    .get("interval")
                    .map(|s| parse_positive_i64(s, &format!("projection.{name}.interval")))
                    .transpose()?
                    .unwrap_or(1);
                let unit = match attrs
                    .get("interval.unit")
                    .map(|s| s.to_ascii_uppercase())
                    .as_deref()
                    .unwrap_or("DAYS")
                {
                    "DAYS" | "DAY" => DateUnit::Days,
                    "MONTHS" | "MONTH" => DateUnit::Months,
                    "YEARS" | "YEAR" => DateUnit::Years,
                    other => {
                        return Err(HiveError::Validation(format!(
                            "projection.{name}.interval.unit={other} is not DAYS, MONTHS, or YEARS"
                        )))
                    }
                };
                ProjectionKind::Date {
                    start,
                    end,
                    format,
                    interval,
                    unit,
                }
            }
            "injected" => ProjectionKind::Injected,
            other => {
                return Err(HiveError::Validation(format!(
                    "projection.{name}.type={other} is not enum, integer, date, or injected"
                )))
            }
        };
        columns.push(ProjectedColumn { name, kind });
    }

    Ok(Some(ProjectionConfig { enabled, columns }))
}

/// Materialize virtual index entries from a projection config. Locations
/// follow the standard `k=v/` layout under `table_location`. Injected
/// columns take equality/IN literals from `filters`, else a single NULL
/// (the sentinel). Exceeding `max_entries` is an error naming the cap.
pub fn evaluate_projection(
    config: &ProjectionConfig,
    table_location: &str,
    filters: &[Expr],
    max_entries: usize,
) -> Result<Vec<PartitionIndexEntry>, HiveError> {
    if !config.enabled {
        return Ok(Vec::new());
    }

    let mut axes: Vec<Vec<(String, String)>> = Vec::new();
    for col in &config.columns {
        let values = match &col.kind {
            ProjectionKind::Enum { values } => values.clone(),
            ProjectionKind::Integer {
                start,
                end,
                interval,
                digits,
            } => integer_values(*start, *end, *interval, *digits)?,
            ProjectionKind::Date {
                start,
                end,
                format,
                interval,
                unit,
            } => date_values(*start, *end, format, *interval, *unit)?,
            ProjectionKind::Injected => injected_values(&col.name, filters),
        };
        axes.push(
            values
                .into_iter()
                .map(|v| (col.name.clone(), v))
                .collect(),
        );
    }

    let mut combos: Vec<BTreeMap<String, String>> = vec![BTreeMap::new()];
    for axis in &axes {
        let mut next = Vec::new();
        for prefix in &combos {
            for (k, v) in axis {
                let mut row = prefix.clone();
                row.insert(k.clone(), v.clone());
                next.push(row);
                if next.len() > max_entries {
                    return Err(HiveError::Validation(format!(
                        "partition projection produced more than {max_entries} partitions. Raise \
                         [hive] manifest_max_partition_index_entries if the table genuinely has \
                         this many."
                    )));
                }
            }
        }
        combos = next;
    }

    let mut base = table_location.to_string();
    if !base.ends_with('/') {
        base.push('/');
    }

    Ok(combos
        .into_iter()
        .map(|values| {
            let mut location = base.clone();
            for col in &config.columns {
                let v = values.get(&col.name).map(String::as_str).unwrap_or(HIVE_DEFAULT_PARTITION);
                location.push_str(&format!("{}={v}/", col.name));
            }
            PartitionIndexEntry {
                values,
                location,
                file_count: 0,
                total_bytes: 0,
            }
        })
        .collect())
}

fn injected_values(col: &str, filters: &[Expr]) -> Vec<String> {
    let mut found = Vec::new();
    for f in filters {
        collect_injected_literals(f, col, &mut found);
    }
    if found.is_empty() {
        vec![HIVE_DEFAULT_PARTITION.to_string()]
    } else {
        found.sort();
        found.dedup();
        found
    }
}

fn collect_injected_literals(expr: &Expr, col: &str, out: &mut Vec<String>) {
    match expr {
        Expr::BinaryExpr(b) if b.op == Operator::Eq => {
            if let (Expr::Column(c), Expr::Literal(sv, _)) | (Expr::Literal(sv, _), Expr::Column(c)) =
                (b.left.as_ref(), b.right.as_ref())
            {
                if c.name == col {
                    if let Some(s) = scalar_as_string(sv) {
                        out.push(s);
                    }
                }
            }
        }
        Expr::InList(list) => {
            if let Expr::Column(c) = list.expr.as_ref() {
                if c.name == col && !list.negated {
                    for e in &list.list {
                        if let Expr::Literal(sv, _) = e {
                            if let Some(s) = scalar_as_string(sv) {
                                out.push(s);
                            }
                        }
                    }
                }
            }
        }
        Expr::BinaryExpr(b) if matches!(b.op, Operator::And | Operator::Or) => {
            collect_injected_literals(&b.left, col, out);
            collect_injected_literals(&b.right, col, out);
        }
        _ => {}
    }
}

fn scalar_as_string(sv: &datafusion::common::ScalarValue) -> Option<String> {
    use datafusion::common::ScalarValue::*;
    match sv {
        Utf8(Some(s)) | LargeUtf8(Some(s)) | Utf8View(Some(s)) => Some(s.clone()),
        Int8(Some(v)) => Some(v.to_string()),
        Int16(Some(v)) => Some(v.to_string()),
        Int32(Some(v)) => Some(v.to_string()),
        Int64(Some(v)) => Some(v.to_string()),
        UInt8(Some(v)) => Some(v.to_string()),
        UInt16(Some(v)) => Some(v.to_string()),
        UInt32(Some(v)) => Some(v.to_string()),
        UInt64(Some(v)) => Some(v.to_string()),
        Date32(Some(d)) => NaiveDate::from_ymd_opt(1970, 1, 1)
            .and_then(|e| e.checked_add_signed(Duration::days(*d as i64)))
            .map(|d| d.format("%Y-%m-%d").to_string()),
        _ => None,
    }
}

fn integer_values(
    start: i64,
    end: i64,
    interval: i64,
    digits: Option<usize>,
) -> Result<Vec<String>, HiveError> {
    if interval <= 0 {
        return Err(HiveError::Validation(
            "integer projection interval must be positive".to_string(),
        ));
    }
    let mut out = Vec::new();
    let mut v = start;
    while v <= end {
        let s = match digits {
            Some(d) => format!("{v:0width$}", width = d),
            None => v.to_string(),
        };
        out.push(s);
        v = v.saturating_add(interval);
        if out.len() > 1_000_000 {
            break;
        }
    }
    Ok(out)
}

fn date_values(
    start: NaiveDate,
    end: NaiveDate,
    format: &str,
    interval: i64,
    unit: DateUnit,
) -> Result<Vec<String>, HiveError> {
    if interval <= 0 {
        return Err(HiveError::Validation(
            "date projection interval must be positive".to_string(),
        ));
    }
    let chrono_fmt = java_date_format_to_chrono(format);
    let mut out = Vec::new();
    let mut cur = start;
    while cur <= end {
        out.push(cur.format(&chrono_fmt).to_string());
        cur = match unit {
            DateUnit::Days => cur
                .checked_add_signed(Duration::days(interval))
                .ok_or_else(|| HiveError::Validation("date projection overflow".into()))?,
            DateUnit::Months => add_months(cur, interval)?,
            DateUnit::Years => add_months(cur, interval * 12)?,
        };
        if out.len() > 1_000_000 {
            break;
        }
    }
    Ok(out)
}

fn add_months(d: NaiveDate, months: i64) -> Result<NaiveDate, HiveError> {
    let mut year = d.year() as i64;
    let mut month = d.month() as i64 + months;
    while month > 12 {
        month -= 12;
        year += 1;
    }
    while month < 1 {
        month += 12;
        year -= 1;
    }
    let year: i32 = year
        .try_into()
        .map_err(|_| HiveError::Validation("date projection year overflow".into()))?;
    NaiveDate::from_ymd_opt(year, month as u32, d.day().min(28)).ok_or_else(|| {
        HiveError::Validation(format!("invalid projected date {year}-{month}-{}", d.day()))
    })
}

fn parse_int_range(range: &str, col: &str) -> Result<(i64, i64), HiveError> {
    let (a, b) = range.split_once(',').ok_or_else(|| {
        HiveError::Validation(format!(
            "projection.{col}.range={range} must be `start,end`"
        ))
    })?;
    let start = parse_i64(a.trim(), &format!("projection.{col}.range"))?;
    let end = parse_i64(b.trim(), &format!("projection.{col}.range"))?;
    if end < start {
        return Err(HiveError::Validation(format!(
            "projection.{col}.range start is greater than end"
        )));
    }
    Ok((start, end))
}

fn parse_date_range(
    range: &str,
    format: &str,
    col: &str,
) -> Result<(NaiveDate, NaiveDate), HiveError> {
    let (a, b) = range.split_once(',').ok_or_else(|| {
        HiveError::Validation(format!(
            "projection.{col}.range={range} must be `start,end`"
        ))
    })?;
    let fmt = java_date_format_to_chrono(format);
    let start = NaiveDate::parse_from_str(a.trim(), &fmt).map_err(|_| {
        HiveError::Validation(format!(
            "projection.{col}.range start `{a}` does not match format `{format}`"
        ))
    })?;
    let end = NaiveDate::parse_from_str(b.trim(), &fmt).map_err(|_| {
        HiveError::Validation(format!(
            "projection.{col}.range end `{b}` does not match format `{format}`"
        ))
    })?;
    if end < start {
        return Err(HiveError::Validation(format!(
            "projection.{col}.range start is greater than end"
        )));
    }
    Ok((start, end))
}

fn java_date_format_to_chrono(fmt: &str) -> String {
    fmt.replace("yyyy", "%Y")
        .replace("MM", "%m")
        .replace("dd", "%d")
}

fn parse_positive_i64(s: &str, name: &str) -> Result<i64, HiveError> {
    let v = parse_i64(s, name)?;
    if v <= 0 {
        return Err(HiveError::Validation(format!("{name}={s} must be positive")));
    }
    Ok(v)
}

fn parse_i64(s: &str, name: &str) -> Result<i64, HiveError> {
    s.parse::<i64>()
        .map_err(|_| HiveError::Validation(format!("{name}={s} is not an integer")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::Column;
    use datafusion::logical_expr::lit;

    fn key(name: &str) -> GlueColumn {
        GlueColumn {
            name: name.to_string(),
            r#type: "string".to_string(),
            comment: None,
        }
    }

    #[test]
    fn unknown_projection_key_is_named() {
        let mut p = BTreeMap::new();
        p.insert("projection.enabled".into(), "true".into());
        p.insert("projection.dt.type".into(), "enum".into());
        p.insert("projection.dt.values".into(), "a,b".into());
        p.insert("projection.dt.bogus".into(), "x".into());
        let err = parse_projection(&p, &[key("dt")]).unwrap_err();
        assert!(err.to_string().contains("projection.dt.bogus"), "{err}");
    }

    #[test]
    fn malformed_type_is_named() {
        let mut p = BTreeMap::new();
        p.insert("projection.enabled".into(), "true".into());
        p.insert("projection.dt.type".into(), "banana".into());
        let err = parse_projection(&p, &[key("dt")]).unwrap_err();
        assert!(err.to_string().contains("projection.dt.type"), "{err}");
    }

    #[test]
    fn enum_materializes_standard_layout() {
        let mut p = BTreeMap::new();
        p.insert("projection.enabled".into(), "true".into());
        p.insert("projection.region.type".into(), "enum".into());
        p.insert("projection.region.values".into(), "eu,us".into());
        let cfg = parse_projection(&p, &[key("region")]).unwrap().unwrap();
        let entries =
            evaluate_projection(&cfg, "s3://raw/orders", &[], 100).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].location, "s3://raw/orders/region=eu/");
        assert_eq!(entries[1].location, "s3://raw/orders/region=us/");
    }

    #[test]
    fn injected_without_context_is_the_null_sentinel() {
        let mut p = BTreeMap::new();
        p.insert("projection.enabled".into(), "true".into());
        p.insert("projection.tenant.type".into(), "injected".into());
        let cfg = parse_projection(&p, &[key("tenant")]).unwrap().unwrap();
        let entries = evaluate_projection(&cfg, "s3://raw/t", &[], 10).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].values.get("tenant").unwrap(),
            HIVE_DEFAULT_PARTITION
        );
    }

    #[test]
    fn injected_picks_equality_from_filters() {
        let mut p = BTreeMap::new();
        p.insert("projection.enabled".into(), "true".into());
        p.insert("projection.tenant.type".into(), "injected".into());
        let cfg = parse_projection(&p, &[key("tenant")]).unwrap().unwrap();
        let filters = vec![Expr::Column(Column::new_unqualified("tenant")).eq(lit("acme"))];
        let entries = evaluate_projection(&cfg, "s3://raw/t", &filters, 10).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].values.get("tenant").unwrap(), "acme");
    }

    #[test]
    fn cap_is_enforced() {
        let mut p = BTreeMap::new();
        p.insert("projection.enabled".into(), "true".into());
        p.insert("projection.n.type".into(), "integer".into());
        p.insert("projection.n.range".into(), "1,10".into());
        let cfg = parse_projection(&p, &[key("n")]).unwrap().unwrap();
        let err = evaluate_projection(&cfg, "s3://raw/t", &[], 3).unwrap_err();
        assert!(err.to_string().contains("manifest_max_partition_index_entries"));
    }

    #[test]
    fn integer_range_and_digits_pad_the_path() {
        let mut p = BTreeMap::new();
        p.insert("projection.enabled".into(), "true".into());
        p.insert("projection.n.type".into(), "integer".into());
        p.insert("projection.n.range".into(), "1,3".into());
        p.insert("projection.n.digits".into(), "2".into());
        let cfg = parse_projection(&p, &[key("n")]).unwrap().unwrap();
        let entries = evaluate_projection(&cfg, "s3://raw/t", &[], 10).unwrap();
        let locs: Vec<_> = entries.iter().map(|e| e.location.as_str()).collect();
        assert_eq!(
            locs,
            vec![
                "s3://raw/t/n=01/",
                "s3://raw/t/n=02/",
                "s3://raw/t/n=03/"
            ]
        );
    }

    #[test]
    fn date_range_materializes_each_day() {
        let mut p = BTreeMap::new();
        p.insert("projection.enabled".into(), "true".into());
        p.insert("projection.dt.type".into(), "date".into());
        p.insert("projection.dt.range".into(), "2026-01-01,2026-01-03".into());
        p.insert("projection.dt.format".into(), "yyyy-MM-dd".into());
        let cfg = parse_projection(&p, &[key("dt")]).unwrap().unwrap();
        let entries = evaluate_projection(&cfg, "s3://raw/t", &[], 10).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[1].values.get("dt").unwrap(), "2026-01-02");
    }

    #[test]
    fn cartesian_product_of_two_axes() {
        let mut p = BTreeMap::new();
        p.insert("projection.enabled".into(), "true".into());
        p.insert("projection.dt.type".into(), "enum".into());
        p.insert("projection.dt.values".into(), "a,b".into());
        p.insert("projection.region.type".into(), "enum".into());
        p.insert("projection.region.values".into(), "eu,us".into());
        let cfg = parse_projection(&p, &[key("dt"), key("region")]).unwrap().unwrap();
        let entries = evaluate_projection(&cfg, "s3://raw/t", &[], 10).unwrap();
        assert_eq!(entries.len(), 4);
    }

    #[test]
    fn enabled_false_evaluates_to_nothing() {
        let mut p = BTreeMap::new();
        p.insert("projection.enabled".into(), "false".into());
        p.insert("projection.dt.type".into(), "enum".into());
        p.insert("projection.dt.values".into(), "a".into());
        let cfg = parse_projection(&p, &[key("dt")]).unwrap().unwrap();
        assert!(!cfg.enabled);
        let entries = evaluate_projection(&cfg, "s3://raw/t", &[], 10).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn enabled_true_requires_a_type_on_every_partition_key() {
        let mut p = BTreeMap::new();
        p.insert("projection.enabled".into(), "true".into());
        p.insert("projection.dt.type".into(), "enum".into());
        p.insert("projection.dt.values".into(), "a".into());
        let err = parse_projection(&p, &[key("dt"), key("region")]).unwrap_err();
        assert!(err.to_string().contains("projection.region.type"), "{err}");
    }

    #[test]
    fn projected_column_must_be_a_declared_partition_key() {
        let mut p = BTreeMap::new();
        p.insert("projection.enabled".into(), "true".into());
        p.insert("projection.nope.type".into(), "enum".into());
        p.insert("projection.nope.values".into(), "a".into());
        let err = parse_projection(&p, &[key("dt")]).unwrap_err();
        assert!(err.to_string().contains("projection.nope.type"), "{err}");
    }

    #[test]
    fn inverted_integer_range_is_named() {
        let mut p = BTreeMap::new();
        p.insert("projection.enabled".into(), "true".into());
        p.insert("projection.n.type".into(), "integer".into());
        p.insert("projection.n.range".into(), "10,1".into());
        let err = parse_projection(&p, &[key("n")]).unwrap_err();
        assert!(err.to_string().contains("projection.n.range"), "{err}");
    }

    #[test]
    fn injected_in_list_materializes_each_literal() {
        let mut p = BTreeMap::new();
        p.insert("projection.enabled".into(), "true".into());
        p.insert("projection.tenant.type".into(), "injected".into());
        let cfg = parse_projection(&p, &[key("tenant")]).unwrap().unwrap();
        let filters = vec![Expr::Column(Column::new_unqualified("tenant")).in_list(
            vec![lit("acme"), lit("globex")],
            false,
        )];
        let entries = evaluate_projection(&cfg, "s3://raw/t", &filters, 10).unwrap();
        let vals: Vec<_> = entries
            .iter()
            .map(|e| e.values.get("tenant").unwrap().as_str())
            .collect();
        assert_eq!(vals, vec!["acme", "globex"]);
    }

    #[test]
    fn empty_enum_values_are_named() {
        let mut p = BTreeMap::new();
        p.insert("projection.enabled".into(), "true".into());
        p.insert("projection.dt.type".into(), "enum".into());
        p.insert("projection.dt.values".into(), " , , ".into());
        let err = parse_projection(&p, &[key("dt")]).unwrap_err();
        assert!(err.to_string().contains("projection.dt.values"), "{err}");
    }

    #[test]
    fn projection_properties_require_enabled() {
        let mut p = BTreeMap::new();
        p.insert("projection.dt.type".into(), "enum".into());
        p.insert("projection.dt.values".into(), "a".into());
        let err = parse_projection(&p, &[key("dt")]).unwrap_err();
        assert!(err.to_string().contains("projection.enabled"), "{err}");
    }

    #[test]
    fn enabled_must_be_true_or_false() {
        let mut p = BTreeMap::new();
        p.insert("projection.enabled".into(), "maybe".into());
        p.insert("projection.dt.type".into(), "enum".into());
        p.insert("projection.dt.values".into(), "a".into());
        let err = parse_projection(&p, &[key("dt")]).unwrap_err();
        assert!(err.to_string().contains("projection.enabled"), "{err}");
    }

    #[test]
    fn unknown_interval_unit_is_named() {
        let mut p = BTreeMap::new();
        p.insert("projection.enabled".into(), "true".into());
        p.insert("projection.dt.type".into(), "date".into());
        p.insert("projection.dt.range".into(), "2026-01-01,2026-01-03".into());
        p.insert("projection.dt.interval.unit".into(), "WEEKS".into());
        let err = parse_projection(&p, &[key("dt")]).unwrap_err();
        assert!(
            err.to_string().contains("projection.dt.interval.unit"),
            "{err}"
        );
    }

    #[test]
    fn zero_interval_is_refused() {
        let mut p = BTreeMap::new();
        p.insert("projection.enabled".into(), "true".into());
        p.insert("projection.n.type".into(), "integer".into());
        p.insert("projection.n.range".into(), "1,3".into());
        p.insert("projection.n.interval".into(), "0".into());
        let err = parse_projection(&p, &[key("n")]).unwrap_err();
        assert!(err.to_string().contains("projection.n.interval"), "{err}");
    }

    #[test]
    fn integer_interval_skips_values() {
        let mut p = BTreeMap::new();
        p.insert("projection.enabled".into(), "true".into());
        p.insert("projection.n.type".into(), "integer".into());
        p.insert("projection.n.range".into(), "1,5".into());
        p.insert("projection.n.interval".into(), "2".into());
        let cfg = parse_projection(&p, &[key("n")]).unwrap().unwrap();
        let entries = evaluate_projection(&cfg, "s3://raw/t", &[], 10).unwrap();
        let vals: Vec<_> = entries
            .iter()
            .map(|e| e.values.get("n").unwrap().as_str())
            .collect();
        assert_eq!(vals, vec!["1", "3", "5"]);
    }

    #[test]
    fn date_monthly_interval() {
        let mut p = BTreeMap::new();
        p.insert("projection.enabled".into(), "true".into());
        p.insert("projection.dt.type".into(), "date".into());
        p.insert("projection.dt.range".into(), "2026-01-01,2026-03-01".into());
        p.insert("projection.dt.interval".into(), "1".into());
        p.insert("projection.dt.interval.unit".into(), "MONTHS".into());
        let cfg = parse_projection(&p, &[key("dt")]).unwrap().unwrap();
        let entries = evaluate_projection(&cfg, "s3://raw/t", &[], 10).unwrap();
        let vals: Vec<_> = entries
            .iter()
            .map(|e| e.values.get("dt").unwrap().as_str())
            .collect();
        assert_eq!(vals, vec!["2026-01-01", "2026-02-01", "2026-03-01"]);
    }

    #[test]
    fn range_without_a_comma_is_named() {
        let mut p = BTreeMap::new();
        p.insert("projection.enabled".into(), "true".into());
        p.insert("projection.n.type".into(), "integer".into());
        p.insert("projection.n.range".into(), "1-10".into());
        let err = parse_projection(&p, &[key("n")]).unwrap_err();
        assert!(err.to_string().contains("projection.n.range"), "{err}");
    }

    #[test]
    fn no_projection_properties_is_none_not_an_error() {
        let p = BTreeMap::new();
        assert!(parse_projection(&p, &[key("dt")]).unwrap().is_none());
    }

    #[test]
    fn enum_values_trim_whitespace() {
        let mut p = BTreeMap::new();
        p.insert("projection.enabled".into(), "true".into());
        p.insert("projection.dt.type".into(), "enum".into());
        p.insert("projection.dt.values".into(), " a , b ".into());
        let cfg = parse_projection(&p, &[key("dt")]).unwrap().unwrap();
        let entries = evaluate_projection(&cfg, "s3://raw/t", &[], 10).unwrap();
        let vals: Vec<_> = entries
            .iter()
            .map(|e| e.values.get("dt").unwrap().as_str())
            .collect();
        assert_eq!(vals, vec!["a", "b"]);
    }
}
