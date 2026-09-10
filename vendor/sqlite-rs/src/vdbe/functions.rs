// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! The V2 scalar function set (spec 008, Requirement 6): pure
//! `fn(&[Value]) -> Result<Value, FunctionError>` implementations plus a
//! name+arity registry, ready for phase 3's `Function` opcode. Mirrors
//! SQLite's `func.c` behavior for the built-ins listed in issue #79 —
//! `length`, `upper`/`lower`, `substr`, `abs`, `coalesce`/`ifnull`/
//! `nullif`, `typeof`, `hex`/`unhex`, `quote`, scalar `min`/`max`,
//! `round`, `sign`, `instr`, `trim`/`ltrim`/`rtrim`, `replace`,
//! `zeroblob`, `iif`.
//!
//! Known gap: `quote()`'s REAL rendering reuses [`format_real`]'s
//! 15-significant-digit rule rather than SQLite's own higher-precision
//! `quote()` routine (observed up to ~19 significant digits on
//! irrational sums) — the same divergence `src/format.rs` already scopes
//! out of `.dump`/`-list` rendering (issue #37). Tracked as a follow-up
//! rather than solved here.

// Every `args[n]` index below is provably in-bounds: `call()`'s registry
// match arms gate on exact arity before dispatching, so each function
// body only indexes positions its own arm guarantees are present.
#![allow(clippy::indexing_slicing)]

use std::cmp::Ordering;

use crate::format::{format_blob, format_real};
use crate::record::{Collation, Value};
use crate::vdbe::compare::compare;

/// The ways a scalar/aggregate function call can fail to evaluate.
#[derive(Debug, PartialEq, Eq)]
pub enum FunctionError {
    /// No registered function matches `name` at the given `arity`.
    Unknown {
        /// The unrecognized function name.
        name: String,
        /// The argument count it was called with.
        arity: usize,
    },

    /// `name` is a known function but was not called with a supported
    /// argument count.
    WrongArity {
        /// The function name.
        name: String,
    },

    /// An arithmetic result overflowed `i64`.
    IntegerOverflow,
}

impl std::fmt::Display for FunctionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FunctionError::Unknown { name, arity } => {
                write!(f, "unknown function {name} with {arity} argument(s)")
            }
            FunctionError::WrongArity { name } => {
                write!(f, "wrong number of arguments to function {name}()")
            }
            FunctionError::IntegerOverflow => write!(f, "integer overflow"),
        }
    }
}

impl std::error::Error for FunctionError {}

/// Renders `v` the way `CAST(v AS TEXT)` would, for `length()`/`hex()` on
/// non-blob arguments — an integer/real's *text* representation, not its
/// storage bytes.
fn as_text(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::Integer(i) => i.to_string(),
        Value::Real(r) => format_real(*r),
        Value::Text(s) => s.to_string(),
        Value::Blob(b) => String::from_utf8_lossy(b).into_owned(),
    }
}

fn length(args: &[Value]) -> Result<Value, FunctionError> {
    Ok(match &args[0] {
        Value::Null => Value::Null,
        Value::Blob(b) => Value::Integer(b.len() as i64),
        Value::Text(s) => Value::Integer(s.chars().count() as i64),
        other => Value::Integer(as_text(other).chars().count() as i64),
    })
}

fn upper(args: &[Value]) -> Result<Value, FunctionError> {
    Ok(match &args[0] {
        Value::Null => Value::Null,
        Value::Text(s) => Value::Text(s.to_ascii_uppercase().into()),
        other => other.clone(),
    })
}

fn lower(args: &[Value]) -> Result<Value, FunctionError> {
    Ok(match &args[0] {
        Value::Null => Value::Null,
        Value::Text(s) => Value::Text(s.to_ascii_lowercase().into()),
        other => other.clone(),
    })
}

#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn value_int(v: &Value) -> i64 {
    match v {
        Value::Integer(i) => *i,
        Value::Real(r) => *r as i64,
        Value::Text(_) => crate::vdbe::coerce::cast_to_integer(v),
        Value::Null | Value::Blob(_) => 0,
    }
}

/// Faithful port of SQLite's `substrFunc` (`func.c`): 1-based indexing,
/// negative `Y` counts from the end, negative `Z` takes the `abs(Z)`
/// characters *preceding* position `Y`, `Y == 0` shifts `Z` down by one.
/// Character-indexed for text, byte-indexed for blobs.
fn substr(args: &[Value]) -> Result<Value, FunctionError> {
    if matches!(args[1], Value::Null) || args.get(2).is_some_and(|v| matches!(v, Value::Null)) {
        return Ok(Value::Null);
    }
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    let mut p1 = value_int(&args[1]);
    let (mut p2, neg_p2) = match args.get(2) {
        Some(z) => {
            let raw = value_int(z);
            if raw < 0 {
                (raw.saturating_neg(), true)
            } else {
                (raw, false)
            }
        }
        None => (i64::MAX / 2, false),
    };

    let blob = match &args[0] {
        Value::Blob(b) => Some(b),
        _ => None,
    };
    let len: i64 = if let Some(b) = blob {
        b.len() as i64
    } else if p1 < 0 {
        as_text(&args[0]).chars().count() as i64
    } else {
        0
    };

    if p1 < 0 {
        p1 = p1.saturating_add(len);
        if p1 < 0 {
            p2 = p2.saturating_add(p1);
            if p2 < 0 {
                p2 = 0;
            }
            p1 = 0;
        }
    } else if p1 > 0 {
        p1 = p1.saturating_sub(1);
    } else if p2 > 0 {
        p2 = p2.saturating_sub(1);
    }

    if neg_p2 {
        p1 = p1.saturating_sub(p2);
        if p1 < 0 {
            p2 = p2.saturating_add(p1);
            p1 = 0;
        }
    }
    let p1 = p1.max(0) as usize;
    let p2 = p2.max(0) as usize;

    if let Some(b) = blob {
        let start = p1.min(b.len());
        let end = start.saturating_add(p2).min(b.len());
        Ok(Value::Blob(b[start..end].to_vec().into()))
    } else {
        let text = as_text(&args[0]);
        let out: String = text.chars().skip(p1).take(p2).collect();
        Ok(Value::Text(out.into()))
    }
}

/// Returns the pinned oracle version (`Cargo.toml`'s
/// `[package.metadata.oracle] version`) — `tools/version_pin.py`
/// checks this literal for drift against that pin. Can't read the pin
/// via `env!` at compile time here: `env!` is outside `src/`'s
/// qualified-subset allowlist (`make check-mvl-limit`).
fn sqlite_version(_args: &[Value]) -> Result<Value, FunctionError> {
    Ok(Value::Text("3.53.4".to_string().into()))
}

fn abs(args: &[Value]) -> Result<Value, FunctionError> {
    Ok(match &args[0] {
        Value::Null => Value::Null,
        Value::Integer(i) => Value::Integer(i.checked_abs().ok_or(FunctionError::IntegerOverflow)?),
        Value::Real(r) => Value::Real(r.abs()),
        // Text/blob arguments always coerce through the REAL path — even
        // a clean integer-looking string like '5' yields REAL 5.0, per
        // the oracle (abs() does not attempt the INTEGER-preserving path
        // for non-numeric-typed inputs).
        other => Value::Real(value_f64(other).abs()),
    })
}

fn coalesce(args: &[Value]) -> Result<Value, FunctionError> {
    Ok(args
        .iter()
        .find(|v| !matches!(v, Value::Null))
        .cloned()
        .unwrap_or(Value::Null))
}

fn nullif(args: &[Value]) -> Result<Value, FunctionError> {
    let (a, b) = (&args[0], &args[1]);
    if matches!(a, Value::Null) || matches!(b, Value::Null) {
        return Ok(a.clone());
    }
    if compare(a, b, Collation::Binary) == Ordering::Equal {
        Ok(Value::Null)
    } else {
        Ok(a.clone())
    }
}

fn typeof_fn(args: &[Value]) -> Result<Value, FunctionError> {
    let s = match &args[0] {
        Value::Null => "null",
        Value::Integer(_) => "integer",
        Value::Real(_) => "real",
        Value::Text(_) => "text",
        Value::Blob(_) => "blob",
    };
    Ok(Value::Text(s.to_string().into()))
}

fn hex(args: &[Value]) -> Result<Value, FunctionError> {
    let bytes: Vec<u8> = match &args[0] {
        Value::Blob(b) => b.to_vec(),
        other => as_text(other).into_bytes(),
    };
    let mut out = String::with_capacity(bytes.len().saturating_mul(2));
    for b in bytes {
        out.push_str(&format!("{b:02X}"));
    }
    Ok(Value::Text(out.into()))
}

fn hex_digit(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c.saturating_sub(b'0')),
        b'a'..=b'f' => Some(c.saturating_sub(b'a').saturating_add(10)),
        b'A'..=b'F' => Some(c.saturating_sub(b'A').saturating_add(10)),
        _ => None,
    }
}

fn unhex(args: &[Value]) -> Result<Value, FunctionError> {
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    let text = as_text(&args[0]);
    let bytes = text.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return Ok(Value::Null);
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks(2) {
        let (Some(hi), Some(lo)) = (hex_digit(pair[0]), hex_digit(pair[1])) else {
            return Ok(Value::Null);
        };
        out.push((hi << 4) | lo);
    }
    Ok(Value::Blob(out.into()))
}

fn sql_quote_text(s: &str) -> String {
    let mut out = String::with_capacity(s.len().saturating_add(2));
    out.push('\'');
    for c in s.chars() {
        if c == '\'' {
            out.push('\'');
        }
        out.push(c);
    }
    out.push('\'');
    out
}

fn quote(args: &[Value]) -> Result<Value, FunctionError> {
    Ok(Value::Text(match &args[0] {
        Value::Null => "NULL".to_string().into(),
        Value::Integer(i) => i.to_string().into(),
        Value::Real(r) => format_real(*r).into(),
        Value::Text(s) => sql_quote_text(s).into(),
        Value::Blob(b) => format_blob(b).into(),
    }))
}

fn scalar_min(args: &[Value]) -> Result<Value, FunctionError> {
    if args.iter().any(|v| matches!(v, Value::Null)) {
        return Ok(Value::Null);
    }
    Ok(args
        .iter()
        .min_by(|a, b| compare(a, b, Collation::Binary))
        .cloned()
        .unwrap_or(Value::Null))
}

fn scalar_max(args: &[Value]) -> Result<Value, FunctionError> {
    if args.iter().any(|v| matches!(v, Value::Null)) {
        return Ok(Value::Null);
    }
    Ok(args
        .iter()
        .max_by(|a, b| compare(a, b, Collation::Binary))
        .cloned()
        .unwrap_or(Value::Null))
}

fn value_f64(v: &Value) -> f64 {
    match v {
        Value::Integer(i) => *i as f64,
        Value::Real(r) => *r,
        Value::Text(s) => match crate::vdbe::coerce::coerce_text_to_numeric(s) {
            Value::Integer(i) => i as f64,
            Value::Real(r) => r,
            _ => 0.0,
        },
        Value::Null | Value::Blob(_) => 0.0,
    }
}

/// Half-away-from-zero rounding to `digits` decimal places, always
/// returning REAL (matches SQLite's `round()`, which never returns
/// INTEGER even for a whole-number result).
fn round_fn(args: &[Value]) -> Result<Value, FunctionError> {
    if matches!(args[0], Value::Null) || matches!(args.get(1), Some(Value::Null)) {
        return Ok(Value::Null);
    }
    let x = value_f64(&args[0]);
    let digits = args.get(1).map_or(0, value_int).clamp(0, 30);
    #[allow(clippy::cast_precision_loss)]
    let scale = 10f64.powi(digits as i32);
    let scaled = x * scale;
    let rounded = if scaled >= 0.0 {
        (scaled + 0.5).floor()
    } else {
        (scaled - 0.5).ceil()
    };
    Ok(Value::Real(rounded / scale))
}

fn sign(args: &[Value]) -> Result<Value, FunctionError> {
    Ok(match &args[0] {
        Value::Null => Value::Null,
        other => {
            let n = value_f64(other);
            Value::Integer(if n > 0.0 {
                1
            } else if n < 0.0 {
                -1
            } else {
                0
            })
        }
    })
}

fn instr(args: &[Value]) -> Result<Value, FunctionError> {
    if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) {
        return Ok(Value::Null);
    }
    let pos = if let Value::Blob(hay) = &args[0] {
        match &args[1] {
            Value::Blob(b) => find_bytes(hay, b),
            other => find_bytes(hay, as_text(other).as_bytes()),
        }
    } else {
        let haystack = as_text(&args[0]);
        let needle = as_text(&args[1]);
        haystack
            .char_indices()
            .map(|(i, _)| i)
            .chain(std::iter::once(haystack.len()))
            .position(|i| haystack[i..].starts_with(&needle))
    };
    Ok(Value::Integer(
        pos.map_or(0, |p| (p as i64).saturating_add(1)),
    ))
}

fn find_bytes(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    hay.windows(needle.len()).position(|w| w == needle)
}

fn trim_charset(args: &[Value]) -> String {
    args.get(1).map_or(" ".to_string(), as_text)
}

fn trim_fn(args: &[Value]) -> Result<Value, FunctionError> {
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    let charset: Vec<char> = trim_charset(args).chars().collect();
    let s = as_text(&args[0]);
    Ok(Value::Text(
        s.trim_matches(|c| charset.contains(&c)).to_string().into(),
    ))
}

fn ltrim_fn(args: &[Value]) -> Result<Value, FunctionError> {
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    let charset: Vec<char> = trim_charset(args).chars().collect();
    let s = as_text(&args[0]);
    Ok(Value::Text(
        s.trim_start_matches(|c| charset.contains(&c))
            .to_string()
            .into(),
    ))
}

fn rtrim_fn(args: &[Value]) -> Result<Value, FunctionError> {
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    let charset: Vec<char> = trim_charset(args).chars().collect();
    let s = as_text(&args[0]);
    Ok(Value::Text(
        s.trim_end_matches(|c| charset.contains(&c))
            .to_string()
            .into(),
    ))
}

fn replace_fn(args: &[Value]) -> Result<Value, FunctionError> {
    if args.iter().any(|v| matches!(v, Value::Null)) {
        return Ok(Value::Null);
    }
    let s = as_text(&args[0]);
    let from = as_text(&args[1]);
    let to = as_text(&args[2]);
    if from.is_empty() {
        return Ok(Value::Text(s.into()));
    }
    Ok(Value::Text(s.replace(&from, &to).into()))
}

/// SQLite's default `SQLITE_MAX_LENGTH` — the largest blob/string this
/// build will materialize. Bounds `zeroblob()` so a huge requested size
/// returns an error instead of an unbounded allocation.
const MAX_BLOB_LEN: i64 = 1_000_000_000;

#[allow(clippy::cast_sign_loss)]
fn zeroblob(args: &[Value]) -> Result<Value, FunctionError> {
    let n = value_int(&args[0]).clamp(0, MAX_BLOB_LEN);
    Ok(Value::Blob(vec![0u8; n as usize].into()))
}

fn iif(args: &[Value]) -> Result<Value, FunctionError> {
    let cond = match &args[0] {
        Value::Null => false,
        Value::Integer(i) => *i != 0,
        Value::Real(r) => *r != 0.0,
        Value::Text(s) => match crate::vdbe::coerce::coerce_text_to_numeric(s) {
            Value::Integer(i) => i != 0,
            Value::Real(r) => r != 0.0,
            _ => false,
        },
        Value::Blob(_) => false,
    };
    Ok(if cond {
        args[1].clone()
    } else {
        args[2].clone()
    })
}

/// Matches `text` against a SQL `LIKE` `pattern` (`%`/`_` wildcards,
/// optional `ESCAPE` character), case-insensitively per SQLite's default
/// `LIKE` behavior.
pub fn like_match(text: &str, pattern: &str, escape: Option<char>) -> bool {
    let t: Vec<char> = text.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    like_rec(&t, &p, escape, 0, 0)
}

fn like_rec(t: &[char], p: &[char], escape: Option<char>, mut ti: usize, mut pi: usize) -> bool {
    loop {
        if pi == p.len() {
            return ti == t.len();
        }
        let pc = p[pi];
        if Some(pc) == escape && pi.saturating_add(1) < p.len() {
            let literal = p[pi.saturating_add(1)];
            if ti >= t.len() || !ascii_eq(t[ti], literal) {
                return false;
            }
            ti = ti.saturating_add(1);
            pi = pi.saturating_add(2);
            continue;
        }
        match pc {
            '%' => {
                // Collapse consecutive '%' (a run behaves as one).
                while pi < p.len() && p[pi] == '%' {
                    pi = pi.saturating_add(1);
                }
                if pi == p.len() {
                    return true;
                }
                for start in ti..=t.len() {
                    if like_rec(t, p, escape, start, pi) {
                        return true;
                    }
                }
                return false;
            }
            '_' => {
                if ti >= t.len() {
                    return false;
                }
                ti = ti.saturating_add(1);
                pi = pi.saturating_add(1);
            }
            _ => {
                if ti >= t.len() || !ascii_eq(t[ti], pc) {
                    return false;
                }
                ti = ti.saturating_add(1);
                pi = pi.saturating_add(1);
            }
        }
    }
}

fn ascii_eq(a: char, b: char) -> bool {
    a.eq_ignore_ascii_case(&b)
}

/// SQLite `GLOB`: case-sensitive, `*` = any run, `?` = any one char,
/// `[...]`/`[^...]` character classes (with `-` ranges).
fn glob_match(text: &str, pattern: &str) -> bool {
    let t: Vec<char> = text.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    glob_rec(&t, &p, 0, 0)
}

fn glob_rec(t: &[char], p: &[char], mut ti: usize, mut pi: usize) -> bool {
    loop {
        if pi == p.len() {
            return ti == t.len();
        }
        match p[pi] {
            '*' => {
                while pi < p.len() && p[pi] == '*' {
                    pi = pi.saturating_add(1);
                }
                if pi == p.len() {
                    return true;
                }
                for start in ti..=t.len() {
                    if glob_rec(t, p, start, pi) {
                        return true;
                    }
                }
                return false;
            }
            '?' => {
                if ti >= t.len() {
                    return false;
                }
                ti = ti.saturating_add(1);
                pi = pi.saturating_add(1);
            }
            '[' => {
                let Some((matches, next_pi)) = glob_class(p, pi, t.get(ti).copied()) else {
                    return false;
                };
                if ti >= t.len() || !matches {
                    return false;
                }
                ti = ti.saturating_add(1);
                pi = next_pi;
            }
            c => {
                if ti >= t.len() || t[ti] != c {
                    return false;
                }
                ti = ti.saturating_add(1);
                pi = pi.saturating_add(1);
            }
        }
    }
}

/// Parses a `[...]`/`[^...]` class starting at `p[start]` (`p[start] ==
/// '['`); returns whether `c` matched and the index just past the `]`.
fn glob_class(p: &[char], start: usize, c: Option<char>) -> Option<(bool, usize)> {
    let mut i = start.saturating_add(1);
    let negate = p.get(i) == Some(&'^');
    if negate {
        i = i.saturating_add(1);
    }
    let class_start = i;
    let mut matched = false;
    loop {
        if i >= p.len() {
            return None; // unterminated class: treat as no match
        }
        if p[i] == ']' && i > class_start {
            i = i.saturating_add(1);
            break;
        }
        if i.saturating_add(2) < p.len()
            && p[i.saturating_add(1)] == '-'
            && p[i.saturating_add(2)] != ']'
        {
            let (lo, hi) = (p[i], p[i.saturating_add(2)]);
            if let Some(c) = c {
                if c >= lo && c <= hi {
                    matched = true;
                }
            }
            i = i.saturating_add(3);
        } else {
            if Some(p[i]) == c {
                matched = true;
            }
            i = i.saturating_add(1);
        }
    }
    Some((matched != negate && c.is_some(), i))
}

/// `like(pattern, text[, escape])` — note SQLite's argument order is
/// (pattern, text), the reverse of the `text LIKE pattern` syntax.
fn like_fn(args: &[Value]) -> Result<Value, FunctionError> {
    if args.iter().any(|v| matches!(v, Value::Null)) {
        return Ok(Value::Null);
    }
    let escape = match args.get(2) {
        Some(e) => as_text(e).chars().next(),
        None => None,
    };
    let pattern = as_text(&args[0]);
    let text = as_text(&args[1]);
    Ok(Value::Integer(i64::from(like_match(
        &text, &pattern, escape,
    ))))
}

/// `glob(pattern, text)` — same reversed argument order as `like()`.
fn glob_fn(args: &[Value]) -> Result<Value, FunctionError> {
    if args.iter().any(|v| matches!(v, Value::Null)) {
        return Ok(Value::Null);
    }
    let pattern = as_text(&args[0]);
    let text = as_text(&args[1]);
    Ok(Value::Integer(i64::from(glob_match(&text, &pattern))))
}

type ScalarFn = fn(&[Value]) -> Result<Value, FunctionError>;

/// Looks up a scalar function by case-insensitive name and arity,
/// invokes it, and reports an unknown name/arity combination.
pub fn call(name: &str, args: &[Value]) -> Result<Value, FunctionError> {
    let arity = args.len();
    let f: Option<ScalarFn> = match (name.to_ascii_lowercase().as_str(), arity) {
        ("length", 1) => Some(length),
        ("upper", 1) => Some(upper),
        ("lower", 1) => Some(lower),
        ("substr", 2 | 3) => Some(substr),
        ("sqlite_version", 0) => Some(sqlite_version),
        ("abs", 1) => Some(abs),
        ("coalesce", n) if n >= 2 => Some(coalesce),
        ("ifnull", 2) => Some(coalesce),
        ("nullif", 2) => Some(nullif),
        ("typeof", 1) => Some(typeof_fn),
        ("hex", 1) => Some(hex),
        ("unhex", 1) => Some(unhex),
        ("quote", 1) => Some(quote),
        ("min", n) if n >= 1 => Some(scalar_min),
        ("max", n) if n >= 1 => Some(scalar_max),
        ("round", 1 | 2) => Some(round_fn),
        ("sign", 1) => Some(sign),
        ("instr", 2) => Some(instr),
        ("trim", 1 | 2) => Some(trim_fn),
        ("ltrim", 1 | 2) => Some(ltrim_fn),
        ("rtrim", 1 | 2) => Some(rtrim_fn),
        ("replace", 3) => Some(replace_fn),
        ("zeroblob", 1) => Some(zeroblob),
        ("iif", 3) => Some(iif),
        ("like", 2 | 3) => Some(like_fn),
        ("glob", 2) => Some(glob_fn),
        _ => None,
    };
    match f {
        Some(f) => f(args),
        None => Err(FunctionError::Unknown {
            name: name.to_string(),
            arity,
        }),
    }
}

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used)]
mod tests {
    use super::*;

    fn v(name: &str, args: &[Value]) -> Value {
        call(name, args).unwrap()
    }

    #[test]
    fn length_counts_chars_for_text_bytes_for_blob() {
        assert_eq!(
            v("length", &[Value::Text("héllo".to_string().into())]),
            Value::Integer(5)
        );
        assert_eq!(
            v("length", &[Value::Blob(vec![0, 1, 2].into())]),
            Value::Integer(3)
        );
        assert_eq!(v("length", &[Value::Integer(12345)]), Value::Integer(5));
        assert_eq!(v("length", &[Value::Null]), Value::Null);
    }

    #[test]
    fn upper_lower_are_ascii_only() {
        assert_eq!(
            v("upper", &[Value::Text("café".to_string().into())]),
            Value::Text("CAFé".to_string().into())
        );
        assert_eq!(
            v("lower", &[Value::Text("CAFÉ".to_string().into())]),
            Value::Text("cafÉ".to_string().into())
        );
    }

    #[test]
    fn substr_negative_and_zero_index_rules() {
        assert_eq!(
            v(
                "substr",
                &[Value::Text("hello".to_string().into()), Value::Integer(-3)]
            ),
            Value::Text("llo".to_string().into())
        );
        assert_eq!(
            v(
                "substr",
                &[Value::Text("hello".to_string().into()), Value::Integer(0)]
            ),
            Value::Text("hello".to_string().into())
        );
        assert_eq!(
            v(
                "substr",
                &[
                    Value::Text("hello".to_string().into()),
                    Value::Integer(2),
                    Value::Integer(-1)
                ]
            ),
            Value::Text("h".to_string().into())
        );
        assert_eq!(
            v(
                "substr",
                &[
                    Value::Text("hello".to_string().into()),
                    Value::Integer(-100),
                    Value::Integer(2)
                ]
            ),
            Value::Text(String::new().into())
        );
    }

    #[test]
    fn round_half_away_from_zero() {
        assert_eq!(v("round", &[Value::Real(2.5)]), Value::Real(3.0));
        assert_eq!(v("round", &[Value::Real(-2.5)]), Value::Real(-3.0));
    }

    #[test]
    fn coalesce_and_ifnull_are_the_null_propagation_exception() {
        assert_eq!(
            v("coalesce", &[Value::Null, Value::Null, Value::Integer(3)]),
            Value::Integer(3)
        );
        assert_eq!(
            v("ifnull", &[Value::Null, Value::Integer(5)]),
            Value::Integer(5)
        );
    }

    #[test]
    fn min_max_scalar_null_propagates() {
        assert_eq!(
            v(
                "min",
                &[Value::Integer(3), Value::Integer(1), Value::Integer(2)]
            ),
            Value::Integer(1)
        );
        assert_eq!(v("min", &[Value::Integer(1), Value::Null]), Value::Null);
        assert_eq!(v("max", &[Value::Integer(1), Value::Null]), Value::Null);
    }

    #[test]
    fn quote_escapes_single_quotes_and_renders_blob_hex() {
        assert_eq!(
            v("quote", &[Value::Text("it's".to_string().into())]),
            Value::Text("'it''s'".to_string().into())
        );
        assert_eq!(
            v("quote", &[Value::Blob(vec![0x00, 0x11].into())]),
            Value::Text("X'0011'".to_string().into())
        );
        assert_eq!(
            v("quote", &[Value::Null]),
            Value::Text("NULL".to_string().into())
        );
    }

    #[test]
    fn hex_and_unhex_roundtrip() {
        assert_eq!(
            v("hex", &[Value::Text("AB".to_string().into())]),
            Value::Text("4142".to_string().into())
        );
        assert_eq!(
            v("hex", &[Value::Integer(5)]),
            Value::Text("35".to_string().into())
        );
        assert_eq!(
            v("unhex", &[Value::Text("4142".to_string().into())]),
            Value::Blob(vec![0x41, 0x42].into())
        );
        assert_eq!(
            v("unhex", &[Value::Text("xyz".to_string().into())]),
            Value::Null
        );
    }

    #[test]
    fn abs_overflow_errors_instead_of_wrapping() {
        assert_eq!(
            call("abs", &[Value::Integer(i64::MIN)]),
            Err(FunctionError::IntegerOverflow)
        );
    }

    #[test]
    fn iif_and_typeof() {
        assert_eq!(
            v(
                "iif",
                &[
                    Value::Integer(1),
                    Value::Text("a".to_string().into()),
                    Value::Text("b".to_string().into())
                ]
            ),
            Value::Text("a".to_string().into())
        );
        assert_eq!(
            v("typeof", &[Value::Null]),
            Value::Text("null".to_string().into())
        );
    }

    #[test]
    fn iif_treats_real_zero_coerced_text_as_falsy() {
        assert_eq!(
            v(
                "iif",
                &[
                    Value::Text("0.0".to_string().into()),
                    Value::Text("a".to_string().into()),
                    Value::Text("b".to_string().into())
                ]
            ),
            Value::Text("b".to_string().into())
        );
    }

    #[test]
    fn round_clamps_digits_and_propagates_null_digits() {
        assert_eq!(v("round", &[Value::Real(1.5), Value::Null]), Value::Null);
        let Value::Real(r) = v("round", &[Value::Real(1.5), Value::Integer(40)]) else {
            panic!("expected real");
        };
        assert!((r - 1.5).abs() < 1e-9, "digits clamped to 30, got {r}");
    }

    #[test]
    fn zeroblob_clamps_oversized_length() {
        let Value::Blob(b) = v("zeroblob", &[Value::Integer(i64::MAX)]) else {
            panic!("expected blob");
        };
        assert_eq!(b.len() as i64, MAX_BLOB_LEN);
        assert_eq!(
            v("zeroblob", &[Value::Integer(-1)]),
            Value::Blob(vec![].into())
        );
    }

    #[test]
    fn like_and_glob_match_oracle_semantics() {
        let t = |s: &str| Value::Text(s.to_string().into());
        // LIKE is ASCII case-insensitive; GLOB is case-sensitive.
        assert_eq!(v("like", &[t("abc"), t("ABC")]), Value::Integer(1));
        assert_eq!(v("glob", &[t("abc"), t("ABC")]), Value::Integer(0));
        assert_eq!(v("like", &[t("a%b"), t("axxb")]), Value::Integer(1));
        // ESCAPE makes the following wildcard literal.
        assert_eq!(
            v("like", &[t("a\\%b"), t("a%b"), t("\\")]),
            Value::Integer(1)
        );
        // GLOB character classes, including negation.
        assert_eq!(v("glob", &[t("a[^b]c"), t("abc")]), Value::Integer(0));
        assert_eq!(v("glob", &[t("a[^b]c"), t("axc")]), Value::Integer(1));
        assert_eq!(v("glob", &[t("a?c"), t("abc")]), Value::Integer(1));
        assert_eq!(v("like", &[t("x"), Value::Null]), Value::Null);
    }

    #[test]
    fn registry_dispatch_never_panics_on_unknown_name_or_arity() {
        assert!(matches!(
            call("nope", &[]),
            Err(FunctionError::Unknown { .. })
        ));
    }

    #[test]
    fn nullif_returns_null_on_equal_else_first_arg() {
        assert_eq!(
            v("nullif", &[Value::Integer(1), Value::Integer(1)]),
            Value::Null
        );
        assert_eq!(
            v("nullif", &[Value::Integer(1), Value::Integer(2)]),
            Value::Integer(1)
        );
        assert_eq!(v("nullif", &[Value::Null, Value::Integer(2)]), Value::Null);
    }

    #[test]
    fn sign_reports_negative_zero_positive_and_propagates_null() {
        assert_eq!(v("sign", &[Value::Integer(-5)]), Value::Integer(-1));
        assert_eq!(v("sign", &[Value::Integer(0)]), Value::Integer(0));
        assert_eq!(v("sign", &[Value::Real(2.5)]), Value::Integer(1));
        assert_eq!(v("sign", &[Value::Null]), Value::Null);
    }

    #[test]
    fn instr_finds_substring_position_or_zero() {
        assert_eq!(
            v(
                "instr",
                &[
                    Value::Text("hello world".to_string().into()),
                    Value::Text("world".to_string().into())
                ]
            ),
            Value::Integer(7)
        );
        assert_eq!(
            v(
                "instr",
                &[
                    Value::Text("hello".to_string().into()),
                    Value::Text("xyz".to_string().into())
                ]
            ),
            Value::Integer(0)
        );
        assert_eq!(
            v(
                "instr",
                &[
                    Value::Blob(vec![1, 2, 3, 4].into()),
                    Value::Blob(vec![3, 4].into())
                ]
            ),
            Value::Integer(3)
        );
        assert_eq!(
            v("instr", &[Value::Null, Value::Text("x".to_string().into())]),
            Value::Null
        );
    }

    #[test]
    fn trim_ltrim_rtrim_default_to_whitespace_or_use_given_charset() {
        assert_eq!(
            v("trim", &[Value::Text("  hi  ".to_string().into())]),
            Value::Text("hi".to_string().into())
        );
        assert_eq!(
            v("ltrim", &[Value::Text("  hi  ".to_string().into())]),
            Value::Text("hi  ".to_string().into())
        );
        assert_eq!(
            v("rtrim", &[Value::Text("  hi  ".to_string().into())]),
            Value::Text("  hi".to_string().into())
        );
        assert_eq!(
            v(
                "trim",
                &[
                    Value::Text("xxhixx".to_string().into()),
                    Value::Text("x".to_string().into())
                ]
            ),
            Value::Text("hi".to_string().into())
        );
        assert_eq!(v("trim", &[Value::Null]), Value::Null);
    }

    #[test]
    fn replace_substitutes_all_occurrences_and_handles_empty_from() {
        assert_eq!(
            v(
                "replace",
                &[
                    Value::Text("banana".to_string().into()),
                    Value::Text("a".to_string().into()),
                    Value::Text("o".to_string().into())
                ]
            ),
            Value::Text("bonono".to_string().into())
        );
        assert_eq!(
            v(
                "replace",
                &[
                    Value::Text("hi".to_string().into()),
                    Value::Text("".to_string().into()),
                    Value::Text("x".to_string().into())
                ]
            ),
            Value::Text("hi".to_string().into())
        );
        assert_eq!(
            v(
                "replace",
                &[
                    Value::Null,
                    Value::Text("a".to_string().into()),
                    Value::Text("b".to_string().into())
                ]
            ),
            Value::Null
        );
    }

    /// #368 tagged MC/DC vector (obligation `functions_121`, `substr`'s
    /// decision `matches!(args[1], Null) || args.get(2).is_some_and(is_null)`):
    /// leaf A true.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_121__v1_start_arg_null() {
        assert_eq!(
            v(
                "substr",
                &[Value::Text("hi".to_string().into()), Value::Null]
            ),
            Value::Null
        );
    }

    /// #368 tagged MC/DC vector (obligation `functions_121`): both leaves
    /// false. Independence pair for A against
    /// `mcdc__functions_121__v1_start_arg_null`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_121__v2_neither_null() {
        assert_eq!(
            v(
                "substr",
                &[Value::Text("hi".to_string().into()), Value::Integer(1)]
            ),
            Value::Text("hi".to_string().into())
        );
    }

    /// #368 tagged MC/DC vector (obligation `functions_121`): leaf B true,
    /// leaf A false. Independence pair for B against
    /// `mcdc__functions_121__v2_neither_null`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_121__v3_length_arg_present_and_null() {
        assert_eq!(
            v(
                "substr",
                &[
                    Value::Text("hi".to_string().into()),
                    Value::Integer(1),
                    Value::Null
                ]
            ),
            Value::Null
        );
    }

    /// #368 tagged MC/DC vector (obligation `functions_220`, `nullif`'s
    /// decision `matches!(a, Null) || matches!(b, Null)`): leaf A true.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_220__v1_left_operand_null() {
        assert_eq!(v("nullif", &[Value::Null, Value::Integer(1)]), Value::Null);
    }

    /// #368 tagged MC/DC vector (obligation `functions_220`): both
    /// leaves false. Independence pair for A against
    /// `mcdc__functions_220__v1_left_operand_null`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_220__v2_neither_null() {
        assert_eq!(
            v("nullif", &[Value::Integer(1), Value::Integer(2)]),
            Value::Integer(1)
        );
    }

    /// #368 tagged MC/DC vector (obligation `functions_220`): leaf B
    /// true, leaf A false. Independence pair for B against
    /// `mcdc__functions_220__v2_neither_null`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_220__v3_right_operand_null() {
        assert_eq!(
            v("nullif", &[Value::Integer(1), Value::Null]),
            Value::Integer(1)
        );
    }

    /// #368 tagged MC/DC vector (obligation `functions_343`, `round`'s
    /// decision `matches!(args[0], Null) || matches!(args.get(1), Some(Null))`):
    /// leaf A true.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_343__v1_value_arg_null() {
        assert_eq!(v("round", &[Value::Null]), Value::Null);
    }

    /// #368 tagged MC/DC vector (obligation `functions_343`): both
    /// leaves false. Independence pair for A against
    /// `mcdc__functions_343__v1_value_arg_null`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_343__v2_neither_null() {
        assert_eq!(v("round", &[Value::Real(1.5)]), Value::Real(2.0));
    }

    /// #368 tagged MC/DC vector (obligation `functions_343`): leaf B
    /// true, leaf A false. Independence pair for B against
    /// `mcdc__functions_343__v2_neither_null`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_343__v3_decimals_arg_present_and_null() {
        assert_eq!(v("round", &[Value::Real(1.5), Value::Null]), Value::Null);
    }

    /// #368 tagged MC/DC vector (obligation `functions_376`, `instr`'s
    /// decision `matches!(args[0], Null) || matches!(args[1], Null)`):
    /// leaf A true.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_376__v1_haystack_null() {
        assert_eq!(
            v("instr", &[Value::Null, Value::Text("a".to_string().into())]),
            Value::Null
        );
    }

    /// #368 tagged MC/DC vector (obligation `functions_376`): both
    /// leaves false. Independence pair for A against
    /// `mcdc__functions_376__v1_haystack_null`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_376__v2_neither_null() {
        assert_eq!(
            v(
                "instr",
                &[
                    Value::Text("abc".to_string().into()),
                    Value::Text("b".to_string().into())
                ]
            ),
            Value::Integer(2)
        );
    }

    /// #368 tagged MC/DC vector (obligation `functions_376`): leaf B
    /// true, leaf A false. Independence pair for B against
    /// `mcdc__functions_376__v2_neither_null`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_376__v3_needle_null() {
        assert_eq!(
            v(
                "instr",
                &[Value::Text("abc".to_string().into()), Value::Null]
            ),
            Value::Null
        );
    }

    /// #368 tagged MC/DC vector (obligation `functions_504`, `like_rec`'s
    /// escape-pair guard `Some(pc) == escape && pi.saturating_add(1) < p.len()`):
    /// both leaves true — an escape character followed by a literal.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_504__v1_escape_with_literal_following() {
        assert!(like_rec(&['a'], &['\\', 'a'], Some('\\'), 0, 0));
    }

    /// #368 tagged MC/DC vector (obligation `functions_504`): leaf A
    /// true, leaf B false — a trailing escape character with nothing
    /// after it, treated as a literal `\`. Independence pair for B
    /// against `mcdc__functions_504__v1_escape_with_literal_following`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_504__v2_trailing_escape_with_nothing_after() {
        assert!(like_rec(&['\\'], &['\\'], Some('\\'), 0, 0));
    }

    /// #368 tagged MC/DC vector (obligation `functions_504`): leaf A
    /// false — the pattern character isn't the escape character.
    /// Independence pair for A against
    /// `mcdc__functions_504__v1_escape_with_literal_following`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_504__v3_not_escape_character() {
        assert!(like_rec(&['a', 'b'], &['a', 'b'], Some('\\'), 0, 0));
    }

    /// #368 tagged MC/DC vector (obligation `functions_506`, the
    /// escaped-literal match check `ti >= t.len() || !ascii_eq(t[ti], literal)`):
    /// leaf A true — text exhausted right at the escaped literal.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_506__v1_text_exhausted() {
        assert!(!like_rec(&[], &['\\', 'a'], Some('\\'), 0, 0));
    }

    /// #368 tagged MC/DC vector (obligation `functions_506`): both
    /// leaves false — the escaped literal matches the next text char.
    /// Independence pair for A against
    /// `mcdc__functions_506__v1_text_exhausted`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_506__v2_literal_matches() {
        assert!(like_rec(&['a'], &['\\', 'a'], Some('\\'), 0, 0));
    }

    /// #368 tagged MC/DC vector (obligation `functions_506`): leaf B
    /// true, leaf A false — text present but doesn't match the escaped
    /// literal. Independence pair for B against
    /// `mcdc__functions_506__v2_literal_matches`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_506__v3_literal_does_not_match() {
        assert!(!like_rec(&['y'], &['\\', 'x'], Some('\\'), 0, 0));
    }

    /// #368 tagged MC/DC vector (obligation `functions_516`, the `%`-run
    /// collapse `pi < p.len() && p[pi] == '%'`): both leaves true on
    /// entry.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_516__v1_percent_run_continues() {
        assert!(like_rec(&['a'], &['%', 'a'], None, 0, 0));
    }

    /// #368 tagged MC/DC vector (obligation `functions_516`): leaf A
    /// false — the loop reaches the end of the pattern (a pattern of
    /// only `%`). Independence pair for A against
    /// `mcdc__functions_516__v1_percent_run_continues`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_516__v2_percent_runs_to_end_of_pattern() {
        assert!(like_rec(&['x'], &['%'], None, 0, 0));
    }

    /// #368 tagged MC/DC vector (obligation `functions_516`): leaf A
    /// true, leaf B false — the run stops because the next character
    /// isn't `%`. Independence pair for B against
    /// `mcdc__functions_516__v1_percent_run_continues`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_516__v3_percent_run_stops_before_non_percent() {
        assert!(like_rec(&['b'], &['%', 'b'], None, 0, 0));
    }

    /// #368 tagged MC/DC vector (obligation `functions_537`, `like_rec`'s
    /// default-char match `ti >= t.len() || !ascii_eq(t[ti], pc)`): leaf
    /// A true.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_537__v1_text_exhausted() {
        assert!(!like_rec(&[], &['a'], None, 0, 0));
    }

    /// #368 tagged MC/DC vector (obligation `functions_537`): both
    /// leaves false. Independence pair for A against
    /// `mcdc__functions_537__v1_text_exhausted`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_537__v2_char_matches() {
        assert!(like_rec(&['a'], &['a'], None, 0, 0));
    }

    /// #368 tagged MC/DC vector (obligation `functions_537`): leaf B
    /// true, leaf A false. Independence pair for B against
    /// `mcdc__functions_537__v2_char_matches`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_537__v3_char_does_not_match() {
        assert!(!like_rec(&['b'], &['a'], None, 0, 0));
    }

    /// #368 tagged MC/DC vector (obligation `functions_566`, `glob_rec`'s
    /// `*`-run collapse `pi < p.len() && p[pi] == '*'`): both leaves true
    /// on entry.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_566__v1_star_run_continues() {
        assert!(glob_rec(&['a'], &['*', 'a'], 0, 0));
    }

    /// #368 tagged MC/DC vector (obligation `functions_566`): leaf A
    /// false — a pattern of only `*`. Independence pair for A against
    /// `mcdc__functions_566__v1_star_run_continues`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_566__v2_star_runs_to_end_of_pattern() {
        assert!(glob_rec(&['x'], &['*'], 0, 0));
    }

    /// #368 tagged MC/DC vector (obligation `functions_566`): leaf A
    /// true, leaf B false — the run stops before a non-`*` char.
    /// Independence pair for B against
    /// `mcdc__functions_566__v1_star_run_continues`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_566__v3_star_run_stops_before_non_star() {
        assert!(glob_rec(&['a', 'b'], &['*', 'b'], 0, 0));
    }

    /// #368 tagged MC/DC vector (obligation `functions_590`, `glob_rec`'s
    /// `[...]` class-match check `ti >= t.len() || !matches`): leaf A
    /// true.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_590__v1_text_exhausted() {
        assert!(!glob_rec(&[], &['[', 'a', '-', 'c', ']'], 0, 0));
    }

    /// #368 tagged MC/DC vector (obligation `functions_590`): both
    /// leaves false — the next char is inside the class range.
    /// Independence pair for A against
    /// `mcdc__functions_590__v1_text_exhausted`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_590__v2_class_matches() {
        assert!(glob_rec(&['b'], &['[', 'a', '-', 'c', ']'], 0, 0));
    }

    /// #368 tagged MC/DC vector (obligation `functions_590`): leaf B
    /// true, leaf A false — text present but outside the class range.
    /// Independence pair for B against
    /// `mcdc__functions_590__v2_class_matches`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_590__v3_class_does_not_match() {
        assert!(!glob_rec(&['z'], &['[', 'a', '-', 'c', ']'], 0, 0));
    }

    /// #368 tagged MC/DC vector (obligation `functions_597`, `glob_rec`'s
    /// default-char match `ti >= t.len() || t[ti] != c`): leaf A true.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_597__v1_text_exhausted() {
        assert!(!glob_rec(&[], &['a'], 0, 0));
    }

    /// #368 tagged MC/DC vector (obligation `functions_597`): both
    /// leaves false. Independence pair for A against
    /// `mcdc__functions_597__v1_text_exhausted`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_597__v2_char_matches() {
        assert!(glob_rec(&['a'], &['a'], 0, 0));
    }

    /// #368 tagged MC/DC vector (obligation `functions_597`): leaf B
    /// true, leaf A false. Independence pair for B against
    /// `mcdc__functions_597__v2_char_matches`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_597__v3_char_does_not_match() {
        assert!(!glob_rec(&['b'], &['a'], 0, 0));
    }

    /// #368 tagged MC/DC vector (obligation `functions_621`, `glob_class`'s
    /// terminator check `p[i] == ']' && i > class_start`): both leaves
    /// true — an ordinary class terminated by `]`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_621__v1_terminates_past_class_start() {
        assert!(glob_class(&['[', 'a', 'b', ']'], 0, Some('a')).is_some());
    }

    /// #368 tagged MC/DC vector (obligation `functions_621`): leaf A
    /// false — an ordinary member character, not `]`. Independence pair
    /// for A against `mcdc__functions_621__v1_terminates_past_class_start`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_621__v2_non_terminator_char() {
        assert!(glob_class(&['[', 'a', 'b', ']'], 0, Some('b')).is_some());
    }

    /// #368 tagged MC/DC vector (obligation `functions_621`): leaf A
    /// true, leaf B false — a literal `]` as the class's first member
    /// (`i == class_start`), per the `[]a]` SQLite convention.
    /// Independence pair for B against
    /// `mcdc__functions_621__v1_terminates_past_class_start`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_621__v3_literal_close_bracket_as_first_member() {
        assert_eq!(
            glob_class(&['[', ']', 'a', ']'], 0, Some(']')),
            Some((true, 4))
        );
    }

    /// #368 tagged MC/DC vector (obligation `functions_625`, `glob_class`'s
    /// range-detection decision `i.saturating_add(2) < p.len() &&
    /// p[i.saturating_add(1)] == '-' && p[i.saturating_add(2)] != ']'`,
    /// 3 leaves / 4 required vectors): all three leaves true — an actual
    /// `a-c` range.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_625__v1_all_true_actual_range() {
        assert_eq!(
            glob_class(&['[', 'a', '-', 'c', ']'], 0, Some('b')),
            Some((true, 5))
        );
    }

    /// #368 tagged MC/DC vector (obligation `functions_625`): leaf A
    /// (`i+2 < p.len()`) false — too few characters left for a range.
    /// Independence pair for A against
    /// `mcdc__functions_625__v1_all_true_actual_range`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_625__v2_too_short_for_a_range() {
        assert_eq!(glob_class(&['[', 'a', ']'], 0, Some('a')), Some((true, 3)));
    }

    /// #368 tagged MC/DC vector (obligation `functions_625`): leaf A
    /// true, leaf B (`p[i+1] == '-'`) false — no dash follows.
    /// Independence pair for B against
    /// `mcdc__functions_625__v1_all_true_actual_range`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_625__v3_no_dash_follows() {
        assert_eq!(
            glob_class(&['[', 'a', 'b', ']'], 0, Some('a')),
            Some((true, 4))
        );
    }

    /// #368 tagged MC/DC vector (obligation `functions_625`): leaves A
    /// and B true, leaf C (`p[i+2] != ']'`) false — a dash immediately
    /// followed by the closing bracket, not a real range. Independence
    /// pair for C against `mcdc__functions_625__v1_all_true_actual_range`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_625__v4_dash_immediately_before_close() {
        assert_eq!(
            glob_class(&['[', 'a', '-', ']'], 0, Some('a')),
            Some((true, 4))
        );
    }

    /// #368 tagged MC/DC vector (obligation `functions_631`, the range
    /// membership check `c >= lo && c <= hi`): both leaves true — inside
    /// the range.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_631__v1_within_range() {
        assert_eq!(
            glob_class(&['[', 'a', '-', 'c', ']'], 0, Some('b')),
            Some((true, 5))
        );
    }

    /// #368 tagged MC/DC vector (obligation `functions_631`): leaf A
    /// (`c >= lo`) false. Independence pair for A against
    /// `mcdc__functions_631__v1_within_range`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_631__v2_below_range() {
        assert_eq!(
            glob_class(&['[', 'a', '-', 'c', ']'], 0, Some('0')),
            Some((false, 5))
        );
    }

    /// #368 tagged MC/DC vector (obligation `functions_631`): leaf A
    /// true, leaf B (`c <= hi`) false — above the range. Independence
    /// pair for B against `mcdc__functions_631__v1_within_range`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__functions_631__v3_above_range() {
        assert_eq!(
            glob_class(&['[', 'a', '-', 'c', ']'], 0, Some('d')),
            Some((false, 5))
        );
    }
}
