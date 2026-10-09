//! Athena partition projection: parse/validate `projection.*` TBLPROPERTIES
//! and materialize a bounded virtual partition set at plan time.
//!
//! Validated at CREATE and on every manifest load (untrusted input). Unknown
//! keys error by name. Projected tables never list the table prefix to
//! discover partitions; the computed set is the plan.

use std::collections::BTreeMap;

use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, Utc};
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
        /// Instants, not dates: Athena's `interval.unit` reaches down to
        /// MILLIS, and a `NaiveDate` would silently round those to midnight.
        start: NaiveDateTime,
        end: NaiveDateTime,
        format: String,
        interval: i64,
        unit: DateUnit,
    },
    Injected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DateUnit {
    Years,
    Months,
    Weeks,
    Days,
    Hours,
    Minutes,
    Seconds,
    Millis,
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

    // Athena lets a projected table relocate its partitions with a
    // template. Reading one as if it used the default `k=v/` layout would
    // scan prefixes that do not exist and return zero rows, so refuse
    // rather than answer wrongly. No table in the surveyed catalog sets it.
    if parameters.contains_key("storage.location.template") {
        return Err(HiveError::Validation(
            "storage.location.template is not supported yet: partition projection here assumes \
             the default `<location>/<key>=<value>/` layout, and honoring a template silently \
             would read the wrong prefixes"
                .to_string(),
        ));
    }

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
                // Athena defaults the unit to the format's own precision:
                // a month-precision format steps by months, otherwise days.
                let default_unit = if format_precision(&format) == Precision::Month {
                    DateUnit::Months
                } else {
                    DateUnit::Days
                };
                let unit = match attrs.get("interval.unit") {
                    None => default_unit,
                    Some(raw) => parse_date_unit(raw).ok_or_else(|| {
                        HiveError::Validation(format!(
                            "projection.{name}.interval.unit={raw} is not one of YEARS, MONTHS, \
                             WEEKS, DAYS, HOURS, MINUTES, SECONDS, MILLIS"
                        ))
                    })?,
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
    partition_types: &[(String, String)],
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
        // Prune each axis before it multiplies into the product, not after.
        // A real table here declares snapshot_date x snapshot_hour x
        // snapshot_minute, which is 609 x 24 x 31 = 453,096 combinations:
        // materializing that first blows `max_entries` and fails the query
        // even when the predicate selects a single day. Pruning per axis
        // costs 664 checks instead. A filter naming another column
        // evaluates to unknown, which keeps the value, so this never
        // narrows an axis it cannot reason about.
        let values = prune_axis(&col.name, values, filters, partition_types);
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
    start: NaiveDateTime,
    end: NaiveDateTime,
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
        // Rendered with the table's own format, so a month-precision
        // pattern emits `202601` and the generated prefix matches what the
        // writer created.
        out.push(cur.format(&chrono_fmt).to_string());
        cur = step(cur, interval, unit)?;
        if out.len() > 1_000_000 {
            break;
        }
    }
    Ok(out)
}

/// Advance (or rewind, for a negative `n`) one instant by `n` units.
fn step(from: NaiveDateTime, n: i64, unit: DateUnit) -> Result<NaiveDateTime, HiveError> {
    let overflow = || HiveError::Validation("date projection overflow".to_string());
    let by = |d: Duration| from.checked_add_signed(d).ok_or_else(overflow);
    match unit {
        DateUnit::Years => add_months(from, n.checked_mul(12).ok_or_else(overflow)?),
        DateUnit::Months => add_months(from, n),
        DateUnit::Weeks => by(Duration::weeks(n)),
        DateUnit::Days => by(Duration::days(n)),
        DateUnit::Hours => by(Duration::hours(n)),
        DateUnit::Minutes => by(Duration::minutes(n)),
        DateUnit::Seconds => by(Duration::seconds(n)),
        DateUnit::Millis => by(Duration::milliseconds(n)),
    }
}

fn add_months(d: NaiveDateTime, months: i64) -> Result<NaiveDateTime, HiveError> {
    let total = (d.year() as i64) * 12 + (d.month() as i64 - 1) + months;
    let year: i32 = (total.div_euclid(12))
        .try_into()
        .map_err(|_| HiveError::Validation("date projection year overflow".into()))?;
    let month = total.rem_euclid(12) as u32 + 1;
    // Clamp into the target month: stepping monthly from the 31st must not
    // skip February. Athena's own behaviour is to stay within the month.
    let last = days_in_month(year, month);
    let day = d.day().min(last);
    let date = NaiveDate::from_ymd_opt(year, month, day).ok_or_else(|| {
        HiveError::Validation(format!("invalid projected date {year}-{month}-{day}"))
    })?;
    Ok(date.and_time(d.time()))
}

fn days_in_month(year: i32, month: u32) -> u32 {
    let (ny, nm) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    match (
        NaiveDate::from_ymd_opt(ny, nm, 1),
        NaiveDate::from_ymd_opt(year, month, 1),
    ) {
        (Some(next), Some(this)) => (next - this).num_days() as u32,
        _ => 28,
    }
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

/// Keep only the values of one projected column that the query's filters
/// can still admit, judged one value at a time against a single-column
/// probe entry. Reuses the index pruner so projected and indexed partitions
/// answer a predicate identically.
fn prune_axis(
    name: &str,
    values: Vec<String>,
    filters: &[Expr],
    partition_types: &[(String, String)],
) -> Vec<String> {
    if filters.is_empty() || values.is_empty() {
        return values;
    }
    let probes: Vec<PartitionIndexEntry> = values
        .iter()
        .map(|v| PartitionIndexEntry {
            values: BTreeMap::from([(name.to_string(), v.clone())]),
            location: String::new(),
            file_count: 0,
            total_bytes: 0,
        })
        .collect();
    super::prune::prune_entries(&probes, filters, partition_types)
        .into_iter()
        .filter_map(|e| e.values.get(name).cloned())
        .collect()
}

fn parse_date_unit(raw: &str) -> Option<DateUnit> {
    // Athena accepts singular and plural, case-insensitively.
    Some(match raw.trim().to_ascii_uppercase().trim_end_matches('S') {
        "YEAR" => DateUnit::Years,
        "MONTH" => DateUnit::Months,
        "WEEK" => DateUnit::Weeks,
        "DAY" => DateUnit::Days,
        "HOUR" => DateUnit::Hours,
        "MINUTE" => DateUnit::Minutes,
        "SECOND" => DateUnit::Seconds,
        "MILLI" | "MILLISECOND" => DateUnit::Millis,
        _ => return None,
    })
}

/// How much of an instant a format string actually writes down. Drives both
/// the default `interval.unit` and which fields a parsed value may omit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Precision {
    Year,
    Month,
    Day,
    Time,
}

fn format_precision(java_fmt: &str) -> Precision {
    let f = java_fmt;
    if f.contains('H') || f.contains('m') || f.contains('s') {
        Precision::Time
    } else if f.contains('d') {
        Precision::Day
    } else if f.contains('M') {
        Precision::Month
    } else {
        Precision::Year
    }
}

/// Translate a Java `DateTimeFormatter` pattern to a chrono one.
///
/// Tokenized rather than a chain of `str::replace`, because replacing `MM`
/// before `mm` mangles any pattern carrying both months and minutes, and
/// Java distinguishes them only by case.
fn java_date_format_to_chrono(fmt: &str) -> String {
    let chars: Vec<char> = fmt.chars().collect();
    let mut out = String::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        // Single quotes fence literal text in Java patterns; '' is a quote.
        if c == '\'' {
            i += 1;
            while i < chars.len() && chars[i] != '\'' {
                if chars[i] == '%' {
                    out.push('%');
                }
                out.push(chars[i]);
                i += 1;
            }
            i += 1;
            continue;
        }
        if !c.is_ascii_alphabetic() {
            if c == '%' {
                out.push('%');
            }
            out.push(c);
            i += 1;
            continue;
        }
        let start = i;
        while i < chars.len() && chars[i] == c {
            i += 1;
        }
        let run = i - start;
        out.push_str(match (c, run) {
            ('y', 2) => "%y",
            ('y', _) => "%Y",
            ('M', 1..=2) => "%m",
            ('M', 3) => "%b",
            ('M', _) => "%B",
            ('d', _) => "%d",
            ('H', _) => "%H",
            ('h', _) => "%I",
            ('m', _) => "%M",
            ('s', _) => "%S",
            ('S', _) => "%3f",
            ('a', _) => "%p",
            ('E', _) => "%a",
            ('D', _) => "%j",
            ('Z', _) | ('X', _) => "%z",
            _ => "",
        });
    }
    out
}

/// Resolve one endpoint of a date range: either a literal in the table's
/// own format, or Athena's relative form `NOW[+-]<n><unit>`.
///
/// `NOW` makes this clock-dependent, so a parsed `ProjectionConfig` is only
/// valid for as long as its cache entry: a range ending at `NOW` gains a new
/// partition every interval. Cache accordingly.
fn resolve_range_endpoint(
    raw: &str,
    java_fmt: &str,
    col: &str,
    which: &str,
) -> Result<NaiveDateTime, HiveError> {
    let s = raw.trim();
    if s.len() >= 3 && s[..3].eq_ignore_ascii_case("NOW") {
        // Projected dates are generated in UTC at query time, per Athena.
        let now = Utc::now().naive_utc();
        let rest = s[3..].trim();
        if rest.is_empty() {
            return Ok(now);
        }
        let (sign, tail) = match rest.split_at(1) {
            ("+", t) => (1i64, t),
            ("-", t) => (-1i64, t),
            _ => {
                return Err(HiveError::Validation(format!(
                    "projection.{col}.range {which} `{raw}` must be NOW, NOW+<n><unit>, or \
                     NOW-<n><unit>"
                )))
            }
        };
        let tail = tail.trim();
        let split = tail
            .find(|ch: char| !ch.is_ascii_digit())
            .unwrap_or(tail.len());
        let (digits, unit_raw) = tail.split_at(split);
        let n: i64 = digits.trim().parse().map_err(|_| {
            HiveError::Validation(format!(
                "projection.{col}.range {which} `{raw}` has no number after the sign"
            ))
        })?;
        let unit = parse_date_unit(unit_raw).ok_or_else(|| {
            HiveError::Validation(format!(
                "projection.{col}.range {which} `{raw}` has unit `{}`, expected YEARS, MONTHS, \
                 WEEKS, DAYS, HOURS, MINUTES or SECONDS",
                unit_raw.trim()
            ))
        })?;
        return step(now, sign * n, unit);
    }
    parse_literal_instant(s, java_fmt).ok_or_else(|| {
        HiveError::Validation(format!(
            "projection.{col}.range {which} `{raw}` does not match format `{java_fmt}`"
        ))
    })
}

/// Parse a literal endpoint, defaulting whatever the format omits.
///
/// Athena patterns are routinely partial: `yyyyMM` has no day, `yyyy-MM-dd-HH`
/// has no minute. `NaiveDate`/`NaiveDateTime::parse_from_str` reject both,
/// the first for want of a day and the second for want of a minute, so parse
/// into the field bag and fill the gaps with the start of the period.
fn parse_literal_instant(value: &str, java_fmt: &str) -> Option<NaiveDateTime> {
    use chrono::format::{parse as parse_into, Parsed, StrftimeItems};

    let fmt = java_date_format_to_chrono(java_fmt);
    let mut parsed = Parsed::new();
    parse_into(&mut parsed, value, StrftimeItems::new(&fmt)).ok()?;
    // Each setter conflicts only if the pattern already supplied the field,
    // in which case the parsed value stands and the default is discarded.
    let _ = parsed.set_month(1);
    let _ = parsed.set_day(1);
    let _ = parsed.set_hour(0);
    let _ = parsed.set_minute(0);
    let _ = parsed.set_second(0);
    parsed.to_naive_datetime_with_offset(0).ok()
}

fn parse_date_range(
    range: &str,
    format: &str,
    col: &str,
) -> Result<(NaiveDateTime, NaiveDateTime), HiveError> {
    let (a, b) = range.split_once(',').ok_or_else(|| {
        HiveError::Validation(format!(
            "projection.{col}.range={range} must be `start,end`"
        ))
    })?;
    let start = resolve_range_endpoint(a, format, col, "start")?;
    let end = resolve_range_endpoint(b, format, col, "end")?;
    if end < start {
        return Err(HiveError::Validation(format!(
            "projection.{col}.range start is greater than end"
        )));
    }
    Ok((start, end))
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

    fn col_expr(name: &str) -> Expr {
        Expr::Column(Column::new_unqualified(name))
    }

    fn key(name: &str) -> GlueColumn {
        GlueColumn {
            name: name.to_string(),
            r#type: "string".to_string(),
            comment: None,
            parameters: Default::default(),
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
            evaluate_projection(&cfg, "s3://raw/orders", &[], &[], 100).unwrap();
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
        let entries = evaluate_projection(&cfg, "s3://raw/t", &[], &[], 10).unwrap();
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
        let entries = evaluate_projection(&cfg, "s3://raw/t", &filters, &[], 10).unwrap();
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
        let err = evaluate_projection(&cfg, "s3://raw/t", &[], &[], 3).unwrap_err();
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
        let entries = evaluate_projection(&cfg, "s3://raw/t", &[], &[], 10).unwrap();
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
        let entries = evaluate_projection(&cfg, "s3://raw/t", &[], &[], 10).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[1].values.get("dt").unwrap(), "2026-01-02");
    }

    /// Every date projection in the surveyed Glue catalog ends its range at
    /// `NOW` (189 of 189 columns), so a parser that only accepts literals
    /// rejects the whole population.
    #[test]
    fn now_resolves_to_today_in_utc() {
        let mut p = BTreeMap::new();
        p.insert("projection.enabled".into(), "true".into());
        p.insert("projection.dt.type".into(), "date".into());
        p.insert("projection.dt.format".into(), "yyyy-MM-dd".into());
        p.insert("projection.dt.range".into(), "2025-01-01,NOW".into());
        let cfg = parse_projection(&p, &[key("dt")]).unwrap().unwrap();
        let ProjectionKind::Date { start, end, .. } = &cfg.columns[0].kind else {
            panic!("expected a date projection");
        };
        assert_eq!(start.date(), NaiveDate::from_ymd_opt(2025, 1, 1).unwrap());
        assert_eq!(end.date(), Utc::now().date_naive());
    }

    #[test]
    fn now_accepts_signed_offsets_at_either_end() {
        let mut p = BTreeMap::new();
        p.insert("projection.enabled".into(), "true".into());
        p.insert("projection.dt.type".into(), "date".into());
        p.insert("projection.dt.format".into(), "yyyy-MM-dd".into());
        p.insert("projection.dt.range".into(), "NOW-2DAYS,NOW".into());
        let cfg = parse_projection(&p, &[key("dt")]).unwrap().unwrap();
        let entries = evaluate_projection(&cfg, "s3://raw/t", &[], &[], 10).unwrap();
        // Inclusive at both ends: today, yesterday, the day before.
        assert_eq!(entries.len(), 3);
        assert_eq!(
            entries[2].values.get("dt").unwrap(),
            &Utc::now().date_naive().format("%Y-%m-%d").to_string()
        );
    }

    /// `yyyyMM` carries no day, so a parser that demands one rejects it, and
    /// a renderer that emits `2026-01-01` builds a prefix that does not
    /// exist. Four tables in the surveyed catalog use this format.
    #[test]
    fn month_precision_formats_round_trip() {
        let mut p = BTreeMap::new();
        p.insert("projection.enabled".into(), "true".into());
        p.insert("projection.m.type".into(), "date".into());
        p.insert("projection.m.format".into(), "yyyyMM".into());
        p.insert("projection.m.range".into(), "202601,202603".into());
        p.insert("projection.m.interval".into(), "1".into());
        p.insert("projection.m.interval.unit".into(), "MONTHS".into());
        let cfg = parse_projection(&p, &[key("m")]).unwrap().unwrap();
        let entries = evaluate_projection(&cfg, "s3://raw/t", &[], &[], 10).unwrap();
        let vals: Vec<_> = entries
            .iter()
            .map(|e| e.values.get("m").unwrap().as_str())
            .collect();
        assert_eq!(vals, vec!["202601", "202602", "202603"]);
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
        let entries = evaluate_projection(&cfg, "s3://raw/t", &[], &[], 10).unwrap();
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
        let entries = evaluate_projection(&cfg, "s3://raw/t", &[], &[], 10).unwrap();
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
        let entries = evaluate_projection(&cfg, "s3://raw/t", &filters, &[], 10).unwrap();
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
        p.insert("projection.dt.interval.unit".into(), "FORTNIGHTS".into());
        let err = parse_projection(&p, &[key("dt")]).unwrap_err();
        assert!(
            err.to_string().contains("projection.dt.interval.unit"),
            "{err}"
        );
    }

    /// Athena's unit list runs from YEARS down to MILLIS. Anything below a
    /// day needs the instant representation to survive the round trip.
    #[test]
    fn sub_day_units_keep_their_time_of_day() {
        let mut p = BTreeMap::new();
        p.insert("projection.enabled".into(), "true".into());
        p.insert("projection.ts.type".into(), "date".into());
        p.insert("projection.ts.format".into(), "yyyy-MM-dd-HH".into());
        p.insert(
            "projection.ts.range".into(),
            "2026-01-01-00,2026-01-01-03".into(),
        );
        p.insert("projection.ts.interval".into(), "1".into());
        p.insert("projection.ts.interval.unit".into(), "HOURS".into());
        let cfg = parse_projection(&p, &[key("ts")]).unwrap().unwrap();
        let entries = evaluate_projection(&cfg, "s3://raw/t", &[], &[], 10).unwrap();
        let vals: Vec<_> = entries
            .iter()
            .map(|e| e.values.get("ts").unwrap().as_str())
            .collect();
        assert_eq!(
            vals,
            vec![
                "2026-01-01-00",
                "2026-01-01-01",
                "2026-01-01-02",
                "2026-01-01-03"
            ]
        );
    }

    #[test]
    fn weeks_are_a_supported_unit() {
        let mut p = BTreeMap::new();
        p.insert("projection.enabled".into(), "true".into());
        p.insert("projection.dt.type".into(), "date".into());
        p.insert("projection.dt.format".into(), "yyyy-MM-dd".into());
        p.insert("projection.dt.range".into(), "2026-01-01,2026-01-15".into());
        p.insert("projection.dt.interval".into(), "1".into());
        p.insert("projection.dt.interval.unit".into(), "WEEKS".into());
        let cfg = parse_projection(&p, &[key("dt")]).unwrap().unwrap();
        let entries = evaluate_projection(&cfg, "s3://raw/t", &[], &[], 10).unwrap();
        let vals: Vec<_> = entries
            .iter()
            .map(|e| e.values.get("dt").unwrap().as_str())
            .collect();
        assert_eq!(vals, vec!["2026-01-01", "2026-01-08", "2026-01-15"]);
    }

    /// `MM` (months) and `mm` (minutes) differ only by case, so a
    /// replace-in-sequence translator mangles any pattern holding both.
    #[test]
    fn month_and_minute_are_not_confused_by_the_format_translator() {
        assert_eq!(
            java_date_format_to_chrono("yyyy-MM-dd HH:mm:ss"),
            "%Y-%m-%d %H:%M:%S"
        );
        assert_eq!(java_date_format_to_chrono("yyyyMM"), "%Y%m");
    }

    /// Stepping monthly from the 31st must not skip February.
    #[test]
    fn monthly_steps_clamp_into_short_months() {
        let mut p = BTreeMap::new();
        p.insert("projection.enabled".into(), "true".into());
        p.insert("projection.dt.type".into(), "date".into());
        p.insert("projection.dt.format".into(), "yyyy-MM-dd".into());
        p.insert("projection.dt.range".into(), "2026-01-31,2026-03-31".into());
        p.insert("projection.dt.interval".into(), "1".into());
        p.insert("projection.dt.interval.unit".into(), "MONTHS".into());
        let cfg = parse_projection(&p, &[key("dt")]).unwrap().unwrap();
        let entries = evaluate_projection(&cfg, "s3://raw/t", &[], &[], 10).unwrap();
        let vals: Vec<_> = entries
            .iter()
            .map(|e| e.values.get("dt").unwrap().as_str())
            .collect();
        assert_eq!(vals, vec!["2026-01-31", "2026-02-28", "2026-03-28"]);
    }

    /// The shape 82 tables in the surveyed catalog actually declare:
    /// snapshot_date x snapshot_hour x snapshot_minute. Unpruned that is
    /// 609 x 24 x 31 = 453,096 combinations, well past the 100,000 default
    /// cap, so the query would fail rather than return a day's rows.
    #[test]
    fn a_predicate_cuts_each_axis_before_the_product() {
        let mut p = BTreeMap::new();
        p.insert("projection.enabled".into(), "true".into());
        p.insert("projection.snapshot_date.type".into(), "date".into());
        p.insert("projection.snapshot_date.format".into(), "yyyy-MM-dd".into());
        p.insert(
            "projection.snapshot_date.range".into(),
            "2025-01-01,NOW".into(),
        );
        p.insert("projection.snapshot_hour.type".into(), "integer".into());
        p.insert("projection.snapshot_hour.range".into(), "0,23".into());
        p.insert("projection.snapshot_hour.digits".into(), "2".into());
        p.insert("projection.snapshot_minute.type".into(), "integer".into());
        p.insert("projection.snapshot_minute.range".into(), "0,30".into());
        p.insert("projection.snapshot_minute.digits".into(), "2".into());

        let keys = [
            key("snapshot_date"),
            key("snapshot_hour"),
            key("snapshot_minute"),
        ];
        let cfg = parse_projection(&p, &keys).unwrap().unwrap();
        let types = vec![
            ("snapshot_date".to_string(), "string".to_string()),
            ("snapshot_hour".to_string(), "string".to_string()),
            ("snapshot_minute".to_string(), "string".to_string()),
        ];

        // Unfiltered, the product is far past the cap and must say so.
        let err = evaluate_projection(&cfg, "s3://raw/t", &[], &types, 100_000).unwrap_err();
        assert!(
            err.to_string().contains("manifest_max_partition_index_entries"),
            "{err}"
        );

        // One day pinned: 1 x 24 x 31 survives comfortably.
        let filters = vec![col_expr("snapshot_date").eq(lit("2026-02-03"))];
        let entries =
            evaluate_projection(&cfg, "s3://raw/t", &filters, &types, 100_000).unwrap();
        assert_eq!(entries.len(), 24 * 31);
        assert!(entries
            .iter()
            .all(|e| e.values.get("snapshot_date").unwrap() == "2026-02-03"));

        // Pinning the hour too narrows it further.
        let filters = vec![
            col_expr("snapshot_date").eq(lit("2026-02-03")),
            col_expr("snapshot_hour").eq(lit("07")),
        ];
        let entries =
            evaluate_projection(&cfg, "s3://raw/t", &filters, &types, 100_000).unwrap();
        assert_eq!(entries.len(), 31);
        assert_eq!(entries[0].location, "s3://raw/t/snapshot_date=2026-02-03/snapshot_hour=07/snapshot_minute=00/");
    }

    /// A filter naming a column outside the projection must not narrow any
    /// axis: unknown keeps the value.
    #[test]
    fn a_filter_on_another_column_leaves_the_axes_alone() {
        let mut p = BTreeMap::new();
        p.insert("projection.enabled".into(), "true".into());
        p.insert("projection.dt.type".into(), "enum".into());
        p.insert("projection.dt.values".into(), "a,b,c".into());
        let cfg = parse_projection(&p, &[key("dt")]).unwrap().unwrap();
        let types = vec![("dt".to_string(), "string".to_string())];
        let filters = vec![col_expr("unrelated").eq(lit("zzz"))];
        let entries = evaluate_projection(&cfg, "s3://raw/t", &filters, &types, 100).unwrap();
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn a_location_template_is_refused_rather_than_ignored() {
        let mut p = BTreeMap::new();
        p.insert("projection.enabled".into(), "true".into());
        p.insert("projection.dt.type".into(), "enum".into());
        p.insert("projection.dt.values".into(), "a".into());
        p.insert(
            "storage.location.template".into(),
            "s3://raw/t/${dt}/".into(),
        );
        let err = parse_projection(&p, &[key("dt")]).unwrap_err();
        assert!(err.to_string().contains("storage.location.template"), "{err}");
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
        let entries = evaluate_projection(&cfg, "s3://raw/t", &[], &[], 10).unwrap();
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
        let entries = evaluate_projection(&cfg, "s3://raw/t", &[], &[], 10).unwrap();
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
        let entries = evaluate_projection(&cfg, "s3://raw/t", &[], &[], 10).unwrap();
        let vals: Vec<_> = entries
            .iter()
            .map(|e| e.values.get("dt").unwrap().as_str())
            .collect();
        assert_eq!(vals, vec!["a", "b"]);
    }
}
