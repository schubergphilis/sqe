//! Pattern engines behind `format_datetime` / `parse_datetime` (Joda) and
//! `date_format` / `date_parse` (MySQL `%` specifiers).
//!
//! Both pattern languages are walked by hand rather than translated to chrono
//! strftime: chrono has no equivalent for half of the Joda tokens (`D`, `e`,
//! `S` with a count, quoted literals) and panics or refuses on others, and
//! Trino fills a field the pattern does not name with its lowest value,
//! which `NaiveDateTime::parse_from_str` cannot do.

use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use datafusion::error::{DataFusionError, Result as DFResult};

use crate::helpers::Zone;

/// A wall-clock time plus the zone facts the zone tokens need.
pub(super) struct Local {
    pub dt: NaiveDateTime,
    pub offset_secs: i32,
    /// Zone id as tagged on the value (`UTC`, `Europe/Amsterdam`, `+01:00`).
    pub id: String,
    /// Short zone name (`CET`); the offset text for a fixed-offset zone.
    pub abbrev: String,
}

const MONTHS: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];
const DAYS: [&str; 7] = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
];

fn execution(msg: String) -> DataFusionError {
    DataFusionError::Execution(msg)
}

fn short(name: &str) -> &str {
    &name[..3]
}

fn pad(out: &mut String, n: i64, width: usize) {
    if n < 0 {
        out.push('-');
    }
    out.push_str(&format!("{:0width$}", n.unsigned_abs()));
}

/// `+hh:mm` (colon) or `+hhmm` offset text.
pub(super) fn offset_text(secs: i32, colon: bool) -> String {
    let sign = if secs < 0 { '-' } else { '+' };
    let a = secs.unsigned_abs();
    let (h, m) = (a / 3600, a % 3600 / 60);
    if colon {
        format!("{sign}{h:02}:{m:02}")
    } else {
        format!("{sign}{h:02}{m:02}")
    }
}

// ───────────────────────────────────────────────────────────────────
// Parsed fields (shared by both parsers)
// ───────────────────────────────────────────────────────────────────

pub(super) enum ParsedZone {
    Offset(i32),
    Named(Zone),
}

#[derive(Default)]
pub(super) struct Parsed {
    year: Option<i64>,
    bc: bool,
    month: Option<u32>,
    day: Option<u32>,
    doy: Option<u32>,
    hour: Option<u32>,
    /// Hour of half-day, already reduced to 0..=11.
    hour12: Option<u32>,
    pm: Option<bool>,
    minute: Option<u32>,
    second: Option<u32>,
    nanos: Option<u32>,
    zone: Option<ParsedZone>,
}

impl Parsed {
    /// Combine the parsed fields. Fields the pattern did not name take the
    /// lowest value (1970-01-01 00:00:00), as in Trino.
    pub(super) fn finish(self) -> Result<(NaiveDateTime, Option<ParsedZone>), String> {
        let mut year = self.year.unwrap_or(1970);
        if self.bc {
            year = 1 - year;
        }
        let year = i32::try_from(year).map_err(|_| "year out of range".to_string())?;
        let date = match (self.doy, self.month, self.day) {
            (Some(doy), None, None) => NaiveDate::from_yo_opt(year, doy),
            _ => NaiveDate::from_ymd_opt(year, self.month.unwrap_or(1), self.day.unwrap_or(1)),
        }
        .ok_or_else(|| "date field out of range".to_string())?;
        let hour = match (self.hour, self.hour12) {
            (Some(h), _) => h,
            (None, Some(h)) => h + if self.pm == Some(true) { 12 } else { 0 },
            (None, None) => 0,
        };
        let time = NaiveTime::from_hms_nano_opt(
            hour,
            self.minute.unwrap_or(0),
            self.second.unwrap_or(0),
            self.nanos.unwrap_or(0),
        )
        .ok_or_else(|| "time field out of range".to_string())?;
        Ok((date.and_time(time), self.zone))
    }
}

struct Cursor<'a> {
    s: &'a str,
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn rest(&self) -> &'a str {
        &self.s[self.pos..]
    }

    fn peek(&self) -> Option<char> {
        self.rest().chars().next()
    }

    fn digits(&mut self, max: usize) -> Option<i64> {
        let n = self
            .rest()
            .bytes()
            .take(max)
            .take_while(u8::is_ascii_digit)
            .count();
        if n == 0 {
            return None;
        }
        let v = self.rest()[..n].parse().ok()?;
        self.pos += n;
        Some(v)
    }

    /// Case-insensitive match of the longest candidate; returns its index.
    fn word(&mut self, candidates: &[&str]) -> Option<usize> {
        let mut best: Option<(usize, usize)> = None;
        for (i, c) in candidates.iter().enumerate() {
            if self.rest().len() >= c.len()
                && self.rest().is_char_boundary(c.len())
                && self.rest()[..c.len()].eq_ignore_ascii_case(c)
                && best.is_none_or(|(_, l)| c.len() > l)
            {
                best = Some((i, c.len()));
            }
        }
        let (i, l) = best?;
        self.pos += l;
        Some(i)
    }

    fn literal(&mut self, c: char) -> bool {
        match self.peek() {
            Some(p) if p == c || p.eq_ignore_ascii_case(&c) => {
                self.pos += p.len_utf8();
                true
            }
            _ => false,
        }
    }

    fn done(&self) -> bool {
        self.pos >= self.s.len()
    }
}

fn month_word(cur: &mut Cursor) -> Option<u32> {
    let mut all: Vec<&str> = MONTHS.to_vec();
    all.extend(MONTHS.iter().map(|m| short(m)));
    cur.word(&all).map(|i| (i % 12) as u32 + 1)
}

fn day_word(cur: &mut Cursor) -> Option<()> {
    let mut all: Vec<&str> = DAYS.to_vec();
    all.extend(DAYS.iter().map(|d| short(d)));
    cur.word(&all).map(|_| ())
}

/// Two-digit year `yy` with a window of 100 years starting at `base`.
fn two_digit_year(yy: i64, base: i64) -> i64 {
    let cand = base.div_euclid(100) * 100 + yy;
    if cand < base {
        cand + 100
    } else {
        cand
    }
}

fn bounded(v: i64, lo: i64, hi: i64, what: &str) -> Result<u32, String> {
    if (lo..=hi).contains(&v) {
        Ok(v as u32)
    } else {
        Err(format!("{what} {v} is out of range"))
    }
}

fn fraction(cur: &mut Cursor, max: usize) -> Option<u32> {
    let start = cur.pos;
    cur.digits(max)?;
    let txt = &cur.s[start..cur.pos];
    let mut padded = txt.to_string();
    padded.truncate(9);
    while padded.len() < 9 {
        padded.push('0');
    }
    padded.parse().ok()
}

fn zone_token(cur: &mut Cursor) -> Option<ParsedZone> {
    if cur.literal('Z') {
        return Some(ParsedZone::Offset(0));
    }
    if matches!(cur.peek(), Some('+' | '-')) {
        let neg = cur.peek() == Some('-');
        cur.pos += 1;
        let hh = cur.digits(2)?;
        cur.literal(':');
        let mm = cur.digits(2).unwrap_or(0);
        if hh > 23 || mm > 59 {
            return None;
        }
        let secs = (hh * 3600 + mm * 60) as i32;
        return Some(ParsedZone::Offset(if neg { -secs } else { secs }));
    }
    let n = cur
        .rest()
        .bytes()
        .take_while(|b| b.is_ascii_alphanumeric() || matches!(b, b'/' | b'_' | b'+' | b'-'))
        .count();
    if n == 0 {
        return None;
    }
    let z = Zone::parse(&cur.rest()[..n]).ok()?;
    cur.pos += n;
    Some(ParsedZone::Named(z))
}

// ───────────────────────────────────────────────────────────────────
// Joda
// ───────────────────────────────────────────────────────────────────

pub(super) enum Piece {
    Lit(String),
    /// A pattern letter and how many times it repeats.
    Field(char, usize),
}

/// Split a Joda pattern into literals and fields. Single quotes quote text
/// (`'T'`), a doubled quote is one quote. An unknown letter is an error,
/// like Joda's `Illegal pattern component`.
pub(super) fn joda_compile(pattern: &str) -> DFResult<Vec<Piece>> {
    let chars: Vec<char> = pattern.chars().collect();
    let mut out: Vec<Piece> = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '\'' {
            let mut lit = String::new();
            if chars.get(i + 1) == Some(&'\'') {
                lit.push('\'');
                i += 2;
            } else {
                i += 1;
                loop {
                    match chars.get(i) {
                        None => {
                            return Err(DataFusionError::Plan(
                                "Pattern ends with an incomplete string literal".into(),
                            ))
                        }
                        Some('\'') if chars.get(i + 1) == Some(&'\'') => {
                            lit.push('\'');
                            i += 2;
                        }
                        Some('\'') => {
                            i += 1;
                            break;
                        }
                        Some(&ch) => {
                            lit.push(ch);
                            i += 1;
                        }
                    }
                }
            }
            out.push(Piece::Lit(lit));
        } else if c.is_ascii_alphabetic() {
            let n = chars[i..].iter().take_while(|&&x| x == c).count();
            if !"GCYxwezEyDMdaKhHkmsSZ".contains(c) {
                return Err(DataFusionError::Plan(format!(
                    "Illegal pattern component: {c}"
                )));
            }
            out.push(Piece::Field(c, n));
            i += n;
        } else {
            out.push(Piece::Lit(c.to_string()));
            i += 1;
        }
    }
    Ok(out)
}

pub(super) fn joda_format(pieces: &[Piece], l: &Local) -> String {
    let dt = &l.dt;
    let mut out = String::new();
    for p in pieces {
        let (c, n) = match p {
            Piece::Lit(s) => {
                out.push_str(s);
                continue;
            }
            Piece::Field(c, n) => (*c, *n),
        };
        let year = dt.year() as i64;
        let era_year = if year > 0 { year } else { 1 - year };
        match c {
            'G' => out.push_str(if year > 0 { "AD" } else { "BC" }),
            'C' => pad(&mut out, era_year / 100, n),
            'y' | 'Y' | 'x' => {
                let y = match c {
                    'y' => year,
                    'Y' => era_year,
                    _ => dt.iso_week().year() as i64,
                };
                if n == 2 {
                    pad(&mut out, y.rem_euclid(100), 2);
                } else {
                    pad(&mut out, y, n);
                }
            }
            'w' => pad(&mut out, dt.iso_week().week() as i64, n),
            'e' => pad(&mut out, dt.weekday().number_from_monday() as i64, n),
            'E' => {
                let name = DAYS[dt.weekday().num_days_from_monday() as usize];
                out.push_str(if n >= 4 { name } else { short(name) });
            }
            'D' => pad(&mut out, dt.ordinal() as i64, n),
            'M' => {
                if n >= 3 {
                    let name = MONTHS[dt.month0() as usize];
                    out.push_str(if n == 3 { short(name) } else { name });
                } else {
                    pad(&mut out, dt.month() as i64, n);
                }
            }
            'd' => pad(&mut out, dt.day() as i64, n),
            'a' => out.push_str(if dt.hour() < 12 { "AM" } else { "PM" }),
            'K' => pad(&mut out, (dt.hour() % 12) as i64, n),
            'h' => pad(
                &mut out,
                match dt.hour() % 12 {
                    0 => 12,
                    h => h as i64,
                },
                n,
            ),
            'H' => pad(&mut out, dt.hour() as i64, n),
            'k' => pad(
                &mut out,
                match dt.hour() {
                    0 => 24,
                    h => h as i64,
                },
                n,
            ),
            'm' => pad(&mut out, dt.minute() as i64, n),
            's' => pad(&mut out, dt.second() as i64, n),
            'S' => {
                let frac = format!("{:09}", dt.nanosecond() % 1_000_000_000);
                for k in 0..n {
                    out.push(frac.as_bytes().get(k).map_or('0', |b| *b as char));
                }
            }
            'z' => out.push_str(if n >= 4 { &l.id } else { &l.abbrev }),
            'Z' => match n {
                1 => out.push_str(&offset_text(l.offset_secs, false)),
                2 => out.push_str(&offset_text(l.offset_secs, true)),
                _ => out.push_str(&l.id),
            },
            _ => {}
        }
    }
    out
}

fn is_numeric_field(p: Option<&Piece>) -> bool {
    matches!(p, Some(Piece::Field(c, n)) if "CYxwedyDdKhHkmsS".contains(*c) || (*c == 'M' && *n <= 2))
}

pub(super) fn joda_parse(pieces: &[Piece], s: &str) -> Result<Parsed, String> {
    let mut cur = Cursor { s, pos: 0 };
    let mut p = Parsed::default();
    let malformed =
        |cur: &Cursor| format!("Invalid format: \"{s}\" is malformed at \"{}\"", cur.rest());
    for (idx, piece) in pieces.iter().enumerate() {
        let (c, n) = match piece {
            Piece::Lit(l) => {
                for ch in l.chars() {
                    if !cur.literal(ch) {
                        return Err(malformed(&cur));
                    }
                }
                continue;
            }
            Piece::Field(c, n) => (*c, *n),
        };
        let bad = |cur: &Cursor| malformed(cur);
        match c {
            'y' | 'Y' | 'x' => {
                let neg = c != 'x' && cur.peek() == Some('-') && {
                    cur.pos += 1;
                    true
                };
                if n == 2 {
                    let yy = cur.digits(2).ok_or_else(|| bad(&cur))?;
                    p.year = Some(two_digit_year(yy, current_year() - 30));
                } else {
                    let max = if is_numeric_field(pieces.get(idx + 1)) {
                        n
                    } else {
                        9
                    };
                    let y = cur.digits(max).ok_or_else(|| bad(&cur))?;
                    p.year = Some(if neg { -y } else { y });
                }
            }
            'C' | 'w' | 'e' => {
                cur.digits(if c == 'e' { 1 } else { 2 })
                    .ok_or_else(|| bad(&cur))?;
            }
            'G' => match cur.word(&["AD", "BC"]) {
                Some(i) => p.bc = i == 1,
                None => return Err(bad(&cur)),
            },
            'M' => {
                if n >= 3 {
                    p.month = Some(month_word(&mut cur).ok_or_else(|| bad(&cur))?);
                } else {
                    let v = cur.digits(2).ok_or_else(|| bad(&cur))?;
                    p.month = Some(bounded(v, 1, 12, "month")?);
                }
            }
            'E' => {
                day_word(&mut cur).ok_or_else(|| bad(&cur))?;
            }
            'd' => {
                let v = cur.digits(2).ok_or_else(|| bad(&cur))?;
                p.day = Some(bounded(v, 1, 31, "day")?);
            }
            'D' => {
                let v = cur.digits(3).ok_or_else(|| bad(&cur))?;
                p.doy = Some(bounded(v, 1, 366, "day of year")?);
            }
            'a' => match cur.word(&["AM", "PM"]) {
                Some(i) => p.pm = Some(i == 1),
                None => return Err(bad(&cur)),
            },
            'K' => {
                let v = cur.digits(2).ok_or_else(|| bad(&cur))?;
                p.hour12 = Some(bounded(v, 0, 11, "hour of half day")?);
            }
            'h' => {
                let v = cur.digits(2).ok_or_else(|| bad(&cur))?;
                p.hour12 = Some(bounded(v, 1, 12, "clock hour of half day")? % 12);
            }
            'H' => {
                let v = cur.digits(2).ok_or_else(|| bad(&cur))?;
                p.hour = Some(bounded(v, 0, 23, "hour")?);
            }
            'k' => {
                let v = cur.digits(2).ok_or_else(|| bad(&cur))?;
                p.hour = Some(bounded(v, 1, 24, "clock hour")? % 24);
            }
            'm' => {
                let v = cur.digits(2).ok_or_else(|| bad(&cur))?;
                p.minute = Some(bounded(v, 0, 59, "minute")?);
            }
            's' => {
                let v = cur.digits(2).ok_or_else(|| bad(&cur))?;
                p.second = Some(bounded(v, 0, 59, "second")?);
            }
            'S' => p.nanos = Some(fraction(&mut cur, n).ok_or_else(|| bad(&cur))?),
            'z' | 'Z' => p.zone = Some(zone_token(&mut cur).ok_or_else(|| bad(&cur))?),
            _ => return Err(bad(&cur)),
        }
    }
    if !cur.done() {
        return Err(malformed(&cur));
    }
    Ok(p)
}

fn current_year() -> i64 {
    chrono::Utc::now().year() as i64
}

// ───────────────────────────────────────────────────────────────────
// MySQL specifiers
// ───────────────────────────────────────────────────────────────────

fn not_supported(c: char) -> DataFusionError {
    DataFusionError::NotImplemented(format!("%{c} not supported in date format string"))
}

fn ordinal_suffix(d: u32) -> &'static str {
    match (d % 100, d % 10) {
        (11..=13, _) => "th",
        (_, 1) => "st",
        (_, 2) => "nd",
        (_, 3) => "rd",
        _ => "th",
    }
}

/// Render `l` with a MySQL-style pattern (Trino's `date_format`). An unknown
/// specifier prints its letter, as in Trino; `%U %u %V %X` are refused.
pub(super) fn mysql_format(fmt: &str, l: &Local) -> DFResult<String> {
    let dt = &l.dt;
    let mut out = String::new();
    let mut chars = fmt.chars();
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        let Some(spec) = chars.next() else {
            return Err(execution(
                "date format string must not end with a trailing '%'".into(),
            ));
        };
        let month_name = MONTHS[dt.month0() as usize];
        let day_name = DAYS[dt.weekday().num_days_from_monday() as usize];
        let h12 = match dt.hour() % 12 {
            0 => 12,
            h => h,
        };
        match spec {
            'a' => out.push_str(short(day_name)),
            'b' => out.push_str(short(month_name)),
            'c' => out.push_str(&dt.month().to_string()),
            'D' => out.push_str(&format!("{}{}", dt.day(), ordinal_suffix(dt.day()))),
            'd' => out.push_str(&format!("{:02}", dt.day())),
            'e' => out.push_str(&dt.day().to_string()),
            'f' => out.push_str(&format!("{:06}", dt.nanosecond() % 1_000_000_000 / 1000)),
            'H' => out.push_str(&format!("{:02}", dt.hour())),
            'h' | 'I' => out.push_str(&format!("{h12:02}")),
            'i' => out.push_str(&format!("{:02}", dt.minute())),
            'j' => out.push_str(&format!("{:03}", dt.ordinal())),
            'k' => out.push_str(&dt.hour().to_string()),
            'l' => out.push_str(&h12.to_string()),
            'M' => out.push_str(month_name),
            'm' => out.push_str(&format!("{:02}", dt.month())),
            'p' => out.push_str(if dt.hour() < 12 { "AM" } else { "PM" }),
            'r' => out.push_str(&format!(
                "{h12:02}:{:02}:{:02} {}",
                dt.minute(),
                dt.second(),
                if dt.hour() < 12 { "AM" } else { "PM" }
            )),
            'S' | 's' => out.push_str(&format!("{:02}", dt.second())),
            'T' => out.push_str(&format!(
                "{:02}:{:02}:{:02}",
                dt.hour(),
                dt.minute(),
                dt.second()
            )),
            'v' => out.push_str(&format!("{:02}", dt.iso_week().week())),
            'W' => out.push_str(day_name),
            'w' => out.push_str(&dt.weekday().num_days_from_sunday().to_string()),
            'x' => pad(&mut out, dt.iso_week().year() as i64, 4),
            'Y' => pad(&mut out, dt.year() as i64, 4),
            'y' => out.push_str(&format!("{:02}", dt.year().rem_euclid(100))),
            '%' => out.push('%'),
            'U' | 'u' | 'V' | 'X' => return Err(not_supported(spec)),
            other => out.push(other),
        }
    }
    Ok(out)
}

/// Parse `s` with a MySQL-style pattern (Trino's `date_parse`).
pub(super) fn mysql_parse(fmt: &str, s: &str) -> Result<Parsed, String> {
    let mut cur = Cursor { s, pos: 0 };
    let mut p = Parsed::default();
    let malformed =
        |cur: &Cursor| format!("Invalid format: \"{s}\" is malformed at \"{}\"", cur.rest());
    let mut chars = fmt.chars();
    while let Some(c) = chars.next() {
        if c != '%' {
            if !cur.literal(c) {
                return Err(malformed(&cur));
            }
            continue;
        }
        let spec = chars
            .next()
            .ok_or_else(|| "date format string must not end with a trailing '%'".to_string())?;
        match spec {
            'a' | 'W' => {
                day_word(&mut cur).ok_or_else(|| malformed(&cur))?;
            }
            'w' => {
                cur.digits(1).ok_or_else(|| malformed(&cur))?;
            }
            'b' | 'M' => p.month = Some(month_word(&mut cur).ok_or_else(|| malformed(&cur))?),
            'c' | 'm' => {
                let v = cur.digits(2).ok_or_else(|| malformed(&cur))?;
                p.month = Some(bounded(v, 1, 12, "month")?);
            }
            'D' => {
                let v = cur.digits(2).ok_or_else(|| malformed(&cur))?;
                p.day = Some(bounded(v, 1, 31, "day")?);
                cur.word(&["st", "nd", "rd", "th"])
                    .ok_or_else(|| malformed(&cur))?;
            }
            'd' | 'e' => {
                let v = cur.digits(2).ok_or_else(|| malformed(&cur))?;
                p.day = Some(bounded(v, 1, 31, "day")?);
            }
            'f' => p.nanos = Some(fraction(&mut cur, 9).ok_or_else(|| malformed(&cur))?),
            'H' | 'k' => {
                let v = cur.digits(2).ok_or_else(|| malformed(&cur))?;
                p.hour = Some(bounded(v, 0, 23, "hour")?);
            }
            'h' | 'I' | 'l' => {
                let v = cur.digits(2).ok_or_else(|| malformed(&cur))?;
                p.hour12 = Some(bounded(v, 1, 12, "clock hour of half day")? % 12);
            }
            'i' => {
                let v = cur.digits(2).ok_or_else(|| malformed(&cur))?;
                p.minute = Some(bounded(v, 0, 59, "minute")?);
            }
            'j' => {
                let v = cur.digits(3).ok_or_else(|| malformed(&cur))?;
                p.doy = Some(bounded(v, 1, 366, "day of year")?);
            }
            'p' => match cur.word(&["AM", "PM"]) {
                Some(i) => p.pm = Some(i == 1),
                None => return Err(malformed(&cur)),
            },
            'r' => {
                let h = cur.digits(2).ok_or_else(|| malformed(&cur))?;
                p.hour12 = Some(bounded(h, 1, 12, "clock hour of half day")? % 12);
                if !cur.literal(':') {
                    return Err(malformed(&cur));
                }
                let m = cur.digits(2).ok_or_else(|| malformed(&cur))?;
                p.minute = Some(bounded(m, 0, 59, "minute")?);
                if !cur.literal(':') {
                    return Err(malformed(&cur));
                }
                let sec = cur.digits(2).ok_or_else(|| malformed(&cur))?;
                p.second = Some(bounded(sec, 0, 59, "second")?);
                if !cur.literal(' ') {
                    return Err(malformed(&cur));
                }
                match cur.word(&["AM", "PM"]) {
                    Some(i) => p.pm = Some(i == 1),
                    None => return Err(malformed(&cur)),
                }
            }
            'S' | 's' => {
                let v = cur.digits(2).ok_or_else(|| malformed(&cur))?;
                p.second = Some(bounded(v, 0, 59, "second")?);
            }
            'T' => {
                let h = cur.digits(2).ok_or_else(|| malformed(&cur))?;
                p.hour = Some(bounded(h, 0, 23, "hour")?);
                if !cur.literal(':') {
                    return Err(malformed(&cur));
                }
                let m = cur.digits(2).ok_or_else(|| malformed(&cur))?;
                p.minute = Some(bounded(m, 0, 59, "minute")?);
                if !cur.literal(':') {
                    return Err(malformed(&cur));
                }
                let sec = cur.digits(2).ok_or_else(|| malformed(&cur))?;
                p.second = Some(bounded(sec, 0, 59, "second")?);
            }
            'Y' => p.year = Some(cur.digits(4).ok_or_else(|| malformed(&cur))?),
            'y' => {
                let yy = cur.digits(2).ok_or_else(|| malformed(&cur))?;
                // MySQL convention: 70..99 are 19xx, 00..69 are 20xx.
                p.year = Some(two_digit_year(yy, 1970));
            }
            '%' => {
                if !cur.literal('%') {
                    return Err(malformed(&cur));
                }
            }
            'U' | 'u' | 'V' | 'X' | 'v' | 'x' => {
                return Err(format!("%{spec} not supported in date parse string"))
            }
            other => {
                if !cur.literal(other) {
                    return Err(malformed(&cur));
                }
            }
        }
    }
    if !cur.done() {
        return Err(malformed(&cur));
    }
    Ok(p)
}
