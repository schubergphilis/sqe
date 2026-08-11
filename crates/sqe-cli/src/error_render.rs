//! Turn a coordinator error into something a person can read.
//!
//! Errors reach the CLI as one flat `Display` string that has been through
//! several layers, each of which quoted the one below it. A Polaris 403 arrives
//! looking like this (single line, abbreviated):
//!
//! ```text
//! "Query failed: Tonic error: code: 'The caller does not have permission ...',
//!  message: \"Access denied: Failed to load table: Unexpected, context:
//!  { status: 403 Forbidden, headers: {\\\"content-length\\\": \\\"116\\\"},
//!  json: {\\\"error\\\":{\\\"message\\\":\\\"Principal 'bob' is not authorized
//!  for op 'LOAD_TABLE'\\\",\\\"type\\\":\\\"ForbiddenException\\\",
//!  \\\"code\\\":403}} } => Received response with unexpected status code\""
//! ```
//!
//! The one sentence a user needs is buried at the third level of escaping. This
//! module digs it out and prints the shape Spark prints:
//!
//! ```text
//! Error: Access denied: Failed to load table
//!        Principal 'bob' is not authorized for op 'LOAD_TABLE'  [ForbiddenException, HTTP 403]
//! ```
//!
//! Nothing is discarded: `--raw-errors` prints the original string, and the
//! renderer degrades to it whenever the shape is unfamiliar. Extraction is
//! deliberately tolerant rather than a strict parse, because these strings are
//! assembled by four independent layers (iceberg-rust, reqwest, tonic, SQE) and
//! any of them can change its quoting without notice.

/// Boilerplate tails that carry no information for a user. iceberg-rust appends
/// the first one to every non-2xx REST response, so it is always present next to
/// the real reason and never useful beside it.
const NOISE_TAILS: &[&str] = &[
    "Received response with unexpected status code",
    "Unexpected => Received response with unexpected status code",
];

/// A coordinator error split into the parts worth showing.
#[derive(Debug, PartialEq, Eq)]
pub struct Rendered {
    /// What the engine was doing, e.g. `Access denied: Failed to load table`.
    pub context: String,
    /// The specific reason, e.g. `Principal 'bob' is not authorized for op 'LOAD_TABLE'`.
    pub detail: Option<String>,
    /// Exception type reported by the catalog, e.g. `ForbiddenException`.
    pub kind: Option<String>,
    /// HTTP status the catalog answered with.
    pub status: Option<u16>,
}

impl std::fmt::Display for Rendered {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.context)?;
        if let Some(detail) = &self.detail {
            write!(f, "\n       {detail}")?;
            let tag = match (&self.kind, self.status) {
                (Some(k), Some(s)) => Some(format!("[{k}, HTTP {s}]")),
                (Some(k), None) => Some(format!("[{k}]")),
                (None, Some(s)) => Some(format!("[HTTP {s}]")),
                (None, None) => None,
            };
            if let Some(tag) = tag {
                write!(f, "  {tag}")?;
            }
        }
        Ok(())
    }
}

/// Strip backslashes so nested quoting does not hide the payload.
///
/// This is lossy and only ever used for *finding* substrings, never for
/// reconstructing the original. A literal backslash inside a message (a Windows
/// path, a regex) is collapsed too, which is acceptable in a human-facing
/// summary and is why `--raw-errors` exists.
fn flatten_escapes(raw: &str) -> String {
    raw.replace('\\', "")
}

/// Pull `"<field>":"<value>"` out of an already-flattened fragment, preferring
/// the LAST match. Layers wrap outward, so the deepest (most specific) body is
/// the one furthest right. Callers must pass text that has been through
/// [`flatten_escapes`].
fn last_json_string(flat: &str, field: &str) -> Option<String> {
    let needle = format!("\"{field}\":\"");
    let start = flat.rfind(&needle)? + needle.len();
    let rest = &flat[start..];
    let end = rest.find('"')?;
    let value = rest[..end].trim();
    (!value.is_empty()).then(|| value.to_string())
}

/// Pull a numeric `"<field>":<n>` out of a flattened JSON fragment.
fn last_json_number(flat: &str, field: &str) -> Option<u16> {
    let needle = format!("\"{field}\":");
    let start = flat.rfind(&needle)? + needle.len();
    let digits: String = flat[start..]
        .chars()
        .skip_while(|c| c.is_whitespace())
        .take_while(char::is_ascii_digit)
        .collect();
    digits.parse().ok()
}

/// Recover the HTTP status from reqwest's `status: 403 Forbidden` context when
/// the JSON body did not carry a `code`.
fn status_from_context(flat: &str) -> Option<u16> {
    let start = flat.find("status: ")? + "status: ".len();
    let digits: String = flat[start..].chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok()
}

/// Peel the transport wrapper and return the innermost message tonic carried.
///
/// `Tonic error: code: '...', message: "<inner>"` keeps the gRPC status name,
/// which restates the code the CLI already exits on and adds nothing a user
/// needs, so only `<inner>` survives.
fn tonic_inner(raw: &str) -> &str {
    let trimmed = raw.trim().trim_matches('"');
    match trimmed.find("message: ") {
        Some(at) => trimmed[at + "message: ".len()..].trim().trim_matches('"'),
        None => trimmed,
    }
}

/// Split `<context>: Unexpected, context: {...}` into just `<context>`.
///
/// Every boundary marker here is a separator SQE or iceberg-rust inserts before
/// machine detail begins. The earliest one that appears wins, so the context
/// line stops at the first piece of plumbing.
fn context_of(inner: &str) -> &str {
    const BOUNDARIES: &[&str] = &[": Unexpected", ", context:", " => ", ": HTTP "];
    let cut = BOUNDARIES
        .iter()
        .filter_map(|b| inner.find(b))
        .min()
        .unwrap_or(inner.len());
    inner[..cut].trim().trim_end_matches(':').trim()
}

/// Text after the last ` => `, which is where iceberg-rust puts a cause.
fn cause_tail(inner: &str) -> Option<String> {
    let tail = inner.rsplit(" => ").next()?.trim();
    if tail.is_empty() || NOISE_TAILS.contains(&tail) {
        return None;
    }
    // Only a tail that differs from the context carries new information.
    (tail != context_of(inner)).then(|| tail.to_string())
}

/// Render a coordinator error string for a human.
///
/// Returns `None` when the string has no recognizable structure, which tells the
/// caller to print it verbatim rather than guess.
pub fn render(raw: &str) -> Option<Rendered> {
    // Flatten before anything else. Each layer escaped the one below it, so the
    // same quote can arrive as `"`, `\"` or `\\\"` depending on depth, and every
    // later step (quote trimming, JSON field lookup, boundary search) would
    // otherwise need to handle all three.
    let flat = flatten_escapes(raw);
    let inner = tonic_inner(&flat);
    if inner.trim().is_empty() {
        return None;
    }

    let context = context_of(inner).to_string();
    if context.is_empty() {
        return None;
    }

    // A catalog JSON body is the most specific reason available; an iceberg
    // cause tail is the fallback.
    let detail = last_json_string(inner, "message").or_else(|| cause_tail(inner));
    let kind = last_json_string(inner, "type");
    let status =
        last_json_number(inner, "code").or_else(|| status_from_context(inner));

    if detail.is_none() && kind.is_none() && status.is_none() {
        // Nothing structured to pull out. Peeling the transport wrapper is still
        // an improvement when there was one, but `context` must not be used
        // here: it truncates at a boundary marker, and with no detail to replace
        // what it cut, truncating would just lose text.
        let bare = flat.trim().trim_matches('"');
        if inner == bare {
            return None;
        }
        return Some(Rendered {
            context: inner.trim().to_string(),
            detail: None,
            kind: None,
            status: None,
        });
    }

    Some(Rendered { context, detail, kind, status })
}

/// Format an error for `eprintln!`, falling back to the raw string.
pub fn format_error(raw: &str, raw_errors: bool) -> String {
    if raw_errors {
        return raw.to_string();
    }
    match render(raw) {
        Some(r) => r.to_string(),
        None => raw.trim().trim_matches('"').to_string(),
    }
}

/// `--raw-errors`, kept process-global rather than threaded through every
/// call site. It is an output preference set once from argv before any query
/// runs, and the alternative is an extra parameter on the REPL, the script
/// runner and the dot-command dispatcher for a value none of them decide.
static RAW_ERRORS: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Record `--raw-errors`. Call once, from `main`, before executing anything.
pub fn set_raw_errors(on: bool) {
    RAW_ERRORS.store(on, std::sync::atomic::Ordering::Relaxed);
}

fn raw_errors() -> bool {
    RAW_ERRORS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Print an error the way the CLI should: readable by default, verbatim under
/// `--raw-errors`. `prefix` lets the script runner keep its statement number.
pub fn print_error(prefix: &str, err: &impl std::fmt::Display) {
    let raw = err.to_string();
    eprintln!("{prefix}{}", format_error(&raw, raw_errors()));
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The case that motivated this module: a Polaris 403 three levels deep.
    /// Captured verbatim from `sqe-cli` against the quickstart stack.
    const POLARIS_403: &str = r#""Query failed: Tonic error: code: 'The caller does not have permission to execute the specified operation', message: \"Access denied: Failed to load table: Unexpected, context: { status: 403 Forbidden, headers: {\\\"content-length\\\": \\\"116\\\", \\\"content-type\\\": \\\"application/json\\\"}, json: {\\\"error\\\":{\\\"message\\\":\\\"Principal 'bob' is not authorized for op 'LOAD_TABLE'\\\",\\\"type\\\":\\\"ForbiddenException\\\",\\\"code\\\":403}} } => Received response with unexpected status code\"""#;

    #[test]
    fn polaris_403_reduces_to_one_sentence() {
        let r = render(POLARIS_403).expect("recognizable shape");
        assert_eq!(r.context, "Access denied: Failed to load table");
        assert_eq!(
            r.detail.as_deref(),
            Some("Principal 'bob' is not authorized for op 'LOAD_TABLE'")
        );
        assert_eq!(r.kind.as_deref(), Some("ForbiddenException"));
        assert_eq!(r.status, Some(403));
    }

    #[test]
    fn polaris_403_display_is_two_lines_and_drops_the_plumbing() {
        let out = format_error(POLARIS_403, false);
        assert_eq!(out.lines().count(), 2, "got: {out}");
        for noise in ["Tonic error", "headers", "content-length", "\\\"", "context: {"] {
            assert!(!out.contains(noise), "{noise} survived in: {out}");
        }
        assert!(out.contains("not authorized for op 'LOAD_TABLE'"));
        assert!(out.contains("[ForbiddenException, HTTP 403]"));
    }

    /// A refused commit names a different Polaris op, and the op is the whole
    /// point of the message: LOAD_TABLE versus ADD_TABLE_SNAPSHOT is how a user
    /// tells "cannot read" from "cannot write".
    #[test]
    fn refused_commit_keeps_the_operation_name() {
        let raw = r#""Failed to commit INSERT: Unexpected, context: { status: 403 Forbidden, headers: {\"content-length\": \"126\"}, json: {\"error\":{\"message\":\"Principal 'alice' is not authorized for op 'ADD_TABLE_SNAPSHOT'\",\"type\":\"ForbiddenException\",\"code\":403}} } => Received response with unexpected status code""#;
        let r = render(raw).expect("recognizable shape");
        assert_eq!(r.context, "Failed to commit INSERT");
        assert_eq!(
            r.detail.as_deref(),
            Some("Principal 'alice' is not authorized for op 'ADD_TABLE_SNAPSHOT'")
        );
        assert_eq!(r.status, Some(403));
    }

    /// No JSON body: the reason lives in iceberg-rust's ` => ` cause tail.
    #[test]
    fn cause_tail_is_used_when_there_is_no_json_body() {
        let raw = r#""Query failed: Tonic error: code: 'Internal error', message: \"Failed to load table: Unexpected => Tried to load a table that does not exist\"""#;
        let r = render(raw).expect("recognizable shape");
        assert_eq!(r.context, "Failed to load table");
        assert_eq!(r.detail.as_deref(), Some("Tried to load a table that does not exist"));
        assert_eq!(r.status, None);
    }

    /// An already-clean message must not be decorated or truncated.
    #[test]
    fn plain_message_passes_through_unchanged() {
        let raw = r#""Failed to fetch results: Tonic error: code: 'Operation is not implemented or not supported', message: \"Utility statement not supported: SHOW VIEWS IN sales_wh.acparity\"""#;
        let out = format_error(raw, false);
        assert_eq!(out, "Utility statement not supported: SHOW VIEWS IN sales_wh.acparity");
    }

    /// The boilerplate tail alone is not a reason, so it must not be promoted
    /// into the detail line where it would displace nothing with noise.
    #[test]
    fn noise_tail_is_not_promoted_to_detail() {
        let raw = "Failed to load table: Unexpected => Received response with unexpected status code";
        let r = render(raw);
        assert!(
            r.as_ref().and_then(|r| r.detail.as_ref()).is_none(),
            "boilerplate leaked into detail: {r:?}"
        );
    }

    #[test]
    fn raw_errors_flag_returns_the_original_untouched() {
        assert_eq!(format_error(POLARIS_403, true), POLARIS_403);
    }

    #[test]
    fn unstructured_input_is_returned_rather_than_guessed_at() {
        assert_eq!(format_error("connection refused", false), "connection refused");
        assert_eq!(format_error("", false), "");
    }

    /// Status must still be reported when the body has no numeric `code`.
    #[test]
    fn status_falls_back_to_the_reqwest_context() {
        let raw = r#""Failed to load table: Unexpected, context: { status: 404 Not Found, json: {\"error\":{\"message\":\"Table does not exist\",\"type\":\"NoSuchTableException\"}} }""#;
        let r = render(raw).expect("recognizable shape");
        assert_eq!(r.status, Some(404));
        assert_eq!(r.detail.as_deref(), Some("Table does not exist"));
    }
}
