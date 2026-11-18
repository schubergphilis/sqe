//! Trino `url_extract_*` functions.
//!
//! Trino parses with `java.net.URI`, not a WHATWG parser: relative references
//! (`/a/b?x=1`) parse, nothing is normalised (the host keeps its case, an
//! empty path stays empty) and path, query and fragment come back
//! percent-decoded. An absent component is `''` (`url_extract_port`: NULL);
//! input that does not parse is NULL.

use datafusion::arrow::datatypes::DataType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;

use crate::helpers::{arg, lift1, lift2, StrArg};

type DFResult<T> = Result<T, DataFusionError>;

pub fn register_url_fns(ctx: &SessionContext) {
    // Trino semantics (java.net.URI): replaces the `url`-crate based versions.
    for kind in [
        Kind::Host,
        Kind::Path,
        Kind::Port,
        Kind::Protocol,
        Kind::Query,
        Kind::Fragment,
        Kind::Parameter,
    ] {
        ctx.register_udf(ScalarUDF::from(UrlFn::new(kind)));
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Kind {
    Host,
    Path,
    Port,
    Protocol,
    Query,
    Fragment,
    Parameter,
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct UrlFn {
    kind: Kind,
    name: &'static str,
    signature: Signature,
}

impl UrlFn {
    fn new(kind: Kind) -> Self {
        let (name, n) = match kind {
            Kind::Host => ("url_extract_host", 1),
            Kind::Path => ("url_extract_path", 1),
            Kind::Port => ("url_extract_port", 1),
            Kind::Protocol => ("url_extract_protocol", 1),
            Kind::Query => ("url_extract_query", 1),
            Kind::Fragment => ("url_extract_fragment", 1),
            Kind::Parameter => ("url_extract_parameter", 2),
        };
        Self {
            kind,
            name,
            signature: Signature::any(n, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for UrlFn {
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(if self.kind == Kind::Port {
            DataType::Int64
        } else {
            DataType::Utf8
        })
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let expected = if self.kind == Kind::Parameter {
            "varchar, varchar"
        } else {
            "varchar"
        };
        let url: StrArg = arg(self.name, expected, &args.args, 0)?;
        match self.kind {
            Kind::Port => lift1(&url, |u| Ok(parse(u).and_then(|p| p.port()))),
            Kind::Parameter => {
                let name: StrArg = arg(self.name, expected, &args.args, 1)?;
                // NULL when the URL does not parse or the parameter is absent.
                lift2(
                    &url,
                    &name,
                    |u, n| Ok(parse(u).and_then(|p| p.parameter(n))),
                )
            }
            kind => lift1(&url, |u| {
                Ok(parse(u).map(|p| match kind {
                    Kind::Host => p.host(),
                    Kind::Path => p.path(),
                    Kind::Protocol => p.scheme.unwrap_or("").to_string(),
                    Kind::Query => p.query(),
                    Kind::Fragment => p.fragment(),
                    Kind::Port | Kind::Parameter => String::new(),
                }))
            }),
        }
    }
}

// ─── java.net.URI style parser ───────────────────────────────────────────────

struct Uri<'a> {
    scheme: Option<&'a str>,
    /// Hierarchical URI with `//authority`.
    authority: Option<&'a str>,
    path: &'a str,
    query: Option<&'a str>,
    fragment: Option<&'a str>,
    /// `mailto:a@b`: a scheme without a following `/`. No path, no query.
    opaque: bool,
}

fn is_hex(b: u8) -> bool {
    b.is_ascii_hexdigit()
}

/// Characters `java.net.URI` rejects outright.
fn legal(s: &str) -> bool {
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            0..=0x20 | 0x7f | b'"' | b'<' | b'>' | b'\\' | b'^' | b'`' | b'{' | b'|' | b'}' => {
                return false
            }
            b'%' => {
                if i + 2 >= b.len() || !(is_hex(b[i + 1]) && is_hex(b[i + 2])) {
                    return false;
                }
                i += 2;
            }
            _ => {}
        }
        i += 1;
    }
    true
}

fn valid_scheme(s: &str) -> bool {
    let mut chars = s.chars();
    chars.next().is_some_and(|c| c.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
}

fn parse(input: &str) -> Option<Uri<'_>> {
    if !legal(input) {
        return None;
    }
    let (rest, fragment) = match input.split_once('#') {
        Some((r, f)) => (r, Some(f)),
        None => (input, None),
    };
    if fragment.is_some_and(|f| f.contains('#') || f.contains('[') || f.contains(']')) {
        return None;
    }
    // A scheme ends at the first ':' that comes before any '/', '?' or '#'.
    let colon = rest.find(':');
    let first_delim = rest.find(['/', '?']);
    let (scheme, rest) = match colon {
        Some(c) if first_delim.is_none_or(|d| c < d) => {
            let scheme = &rest[..c];
            if !valid_scheme(scheme) {
                return None;
            }
            (Some(scheme), &rest[c + 1..])
        }
        _ => (None, rest),
    };
    if scheme.is_some() && !rest.starts_with('/') {
        // Opaque: the scheme-specific part must be non-empty.
        if rest.is_empty() || rest.contains(['[', ']']) {
            return None;
        }
        return Some(Uri {
            scheme,
            authority: None,
            path: "",
            query: None,
            fragment,
            opaque: true,
        });
    }
    let (authority, rest) = match rest.strip_prefix("//") {
        Some(after) => {
            let end = after.find(['/', '?']).unwrap_or(after.len());
            (Some(&after[..end]), &after[end..])
        }
        None => (None, rest),
    };
    let (path, query) = match rest.split_once('?') {
        Some((p, q)) => (p, Some(q)),
        None => (rest, None),
    };
    if path.contains(['[', ']']) || query.is_some_and(|q| q.contains(['[', ']'])) {
        return None;
    }
    Some(Uri {
        scheme,
        authority,
        path,
        query,
        fragment,
        opaque: false,
    })
}

fn decode(s: &str, plus_is_space: bool) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' if i + 2 < b.len() => match (hex_val(b[i + 1]), hex_val(b[i + 2])) {
                (Some(h), Some(l)) => {
                    out.push(h * 16 + l);
                    i += 3;
                    continue;
                }
                _ => out.push(b'%'),
            },
            b'+' if plus_is_space => out.push(b' '),
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(b: u8) -> Option<u8> {
    (b as char).to_digit(16).map(|d| d as u8)
}

/// `host`, `port` of a `[userinfo@]host[:port]` authority, when it is a valid
/// server-based authority. Anything else is registry-based: no host, no port.
fn server(authority: &str) -> Option<(&str, Option<i64>)> {
    let hostport = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    let (host, port) = if let Some(rest) = hostport.strip_prefix('[') {
        let (inner, after) = rest.split_once(']')?;
        if inner.is_empty()
            || !inner
                .chars()
                .all(|c| c.is_ascii_hexdigit() || matches!(c, ':' | '.'))
        {
            return None;
        }
        let host = &hostport[..inner.len() + 2];
        match after {
            "" => (host, None),
            a => (host, Some(a.strip_prefix(':')?)),
        }
    } else {
        match hostport.rsplit_once(':') {
            Some((h, p)) => (h, Some(p)),
            None => (hostport, None),
        }
    };
    if !host.starts_with('[') && !valid_hostname(host) {
        return None;
    }
    let port = match port {
        None | Some("") => None,
        Some(p) if p.bytes().all(|b| b.is_ascii_digit()) => Some(p.parse::<i32>().ok()? as i64),
        Some(_) => return None,
    };
    Some((host, port))
}

fn valid_hostname(host: &str) -> bool {
    if host.is_empty() {
        return false;
    }
    host.trim_end_matches('.').split('.').all(|label| {
        !label.is_empty()
            && !label.starts_with('-')
            && !label.ends_with('-')
            && label
                .chars()
                .all(|c| c.is_alphanumeric() || c == '-' || c == '%')
    })
}

impl Uri<'_> {
    fn host(&self) -> String {
        self.authority
            .and_then(server)
            .map(|(h, _)| h.to_string())
            .unwrap_or_default()
    }
    fn port(&self) -> Option<i64> {
        self.authority.and_then(server).and_then(|(_, p)| p)
    }
    fn path(&self) -> String {
        if self.opaque {
            String::new()
        } else {
            decode(self.path, false)
        }
    }
    fn query(&self) -> String {
        self.query.map(|q| decode(q, false)).unwrap_or_default()
    }
    fn fragment(&self) -> String {
        self.fragment.map(|f| decode(f, false)).unwrap_or_default()
    }
    /// First value of `name` in the raw query; `''` for `name` without `=`.
    fn parameter(&self, name: &str) -> Option<String> {
        for pair in self.query?.split('&') {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            if decode(k, true) == name {
                return Some(decode(v, true));
            }
        }
        None
    }
}

#[cfg(test)]
mod tests;
