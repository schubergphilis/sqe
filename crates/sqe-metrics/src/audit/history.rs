//! Read finished queries back from the native audit log (#812).
//!
//! The coordinator keeps finished queries in memory only; the audit log is
//! the durable record. [`read_query_events`] returns the `kind = "query"`
//! events whose `time` (the moment the query ended) falls in a window,
//! reading the rotated generations and then the active file.
//!
//! Rotation is by size, so a file's name says nothing about its time range.
//! Every line starts with its `time`, so the first line of the next
//! generation is an upper bound for this one: a generation whose successor
//! starts before the window is skipped without reading it, and reading stops
//! at the first generation that starts after the window. [`CLOCK_SLACK`]
//! covers events queued slightly out of order.
//!
//! Only the native format is read. Under `format = "ocsf"` the native file
//! holds no query events, so the history is empty.

use std::collections::VecDeque;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Duration, Utc};

use super::event::{AuditEvent, AuditKind};
use super::logger::rotated_audit_files;

/// How far out of order two events in one file can be: `time` is set when
/// the event is built, before it is queued for the writer.
const CLOCK_SLACK: Duration = Duration::seconds(60);

/// Prefix of a native [`AuditEvent`] line (its first field is `time`). Legacy
/// `AuditEntry` lines start with `{"timestamp":` and carry no query id.
const EVENT_PREFIX: &str = "{\"time\":\"";

/// The query events that ended in `[from, to]`, oldest first, at most
/// `limit` of them (the newest are kept). `active` is
/// `metrics.audit_log_path`. A missing or unreadable file, a torn last line
/// and a line that is not a query event are skipped.
pub fn read_query_events(
    active: &Path,
    from: DateTime<Utc>,
    to: Option<DateTime<Utc>>,
    limit: usize,
) -> Vec<AuditEvent> {
    let mut files: Vec<PathBuf> = rotated_audit_files(active)
        .into_iter()
        .map(|(_, path)| path)
        .collect();
    files.push(active.to_path_buf());
    let starts: Vec<Option<DateTime<Utc>>> = files.iter().map(|f| first_time(f)).collect();

    let mut kept = VecDeque::new();
    if limit == 0 {
        return Vec::new();
    }
    for (index, file) in files.iter().enumerate() {
        if let (Some(to), Some(start)) = (to, starts[index]) {
            if to
                .checked_add_signed(CLOCK_SLACK)
                .is_some_and(|to| start > to)
            {
                break;
            }
        }
        // Every event here is older than the next generation's first one.
        let next_start = starts[index + 1..].iter().flatten().next();
        if next_start.is_some_and(|next| {
            next.checked_add_signed(CLOCK_SLACK)
                .is_some_and(|next| next < from)
        }) {
            continue;
        }
        let Ok(handle) = std::fs::File::open(file) else {
            continue;
        };
        for line in BufReader::new(handle).lines() {
            let Ok(line) = line else { break };
            let Some(time) = line_time(&line) else {
                continue;
            };
            if time < from || to.is_some_and(|to| time > to) {
                continue;
            }
            let Ok(event) = serde_json::from_str::<AuditEvent>(&line) else {
                continue;
            };
            if !matches!(event.kind, AuditKind::Query) || event.query_id.is_none() {
                continue;
            }
            if kept.len() == limit {
                kept.pop_front();
            }
            kept.push_back(event);
        }
    }
    kept.into()
}

/// The `time` a native event line starts with, read without parsing the
/// rest of the line.
fn line_time(line: &str) -> Option<DateTime<Utc>> {
    let rest = line.strip_prefix(EVENT_PREFIX)?;
    let end = rest.find('"')?;
    DateTime::parse_from_rfc3339(&rest[..end])
        .ok()
        .map(|t| t.with_timezone(&Utc))
}

/// The time of the first event line in `path`.
fn first_time(path: &Path) -> Option<DateTime<Utc>> {
    let handle = std::fs::File::open(path).ok()?;
    BufReader::new(handle)
        .lines()
        .map_while(Result::ok)
        .find_map(|line| line_time(&line))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::{AuditFormat, AuditLogger, AuditLoggerOptions, AuditRotation, Outcome};
    use chrono::TimeZone;

    fn at(minute: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 6, 20, 12, minute, 0).unwrap()
    }

    fn query(id: &str, minute: u32) -> AuditEvent {
        let mut event = crate::audit::event::sample_query_event();
        event.time = at(minute);
        event.query_id = Some(id.to_string());
        event
    }

    /// Writes `events` through a logger that rotates after every event, so
    /// each lands in its own generation, as a long-running log would.
    fn rotated_log(events: Vec<AuditEvent>) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");
        let logger = AuditLogger::with_options(
            path.to_str().unwrap(),
            AuditFormat::Native,
            AuditLoggerOptions {
                rotation: AuditRotation {
                    max_file_bytes: 1,
                    max_files: 0,
                    min_free_bytes: 0,
                },
                ..AuditLoggerOptions::default()
            },
        )
        .unwrap();
        for event in events {
            logger.log_event(event);
            logger.flush();
        }
        drop(logger);
        (dir, path)
    }

    fn ids(events: &[AuditEvent]) -> Vec<&str> {
        events
            .iter()
            .map(|e| e.query_id.as_deref().unwrap())
            .collect()
    }

    #[test]
    fn reads_the_window_across_rotated_generations() {
        let (_dir, path) = rotated_log(vec![
            query("q1", 1),
            query("q2", 10),
            query("q3", 20),
            query("q4", 30),
        ]);
        assert!(rotated_audit_files(&path).len() >= 3, "one event per file");
        let all = read_query_events(&path, at(0), None, 100);
        assert_eq!(ids(&all), ["q1", "q2", "q3", "q4"]);
        let window = read_query_events(&path, at(5), Some(at(25)), 100);
        assert_eq!(ids(&window), ["q2", "q3"]);
        // The limit keeps the newest.
        assert_eq!(ids(&read_query_events(&path, at(0), None, 2)), ["q3", "q4"]);
        assert!(read_query_events(&path, at(31), None, 100).is_empty());
        // A window at chrono's range ends does not overflow the slack.
        let (min, max) = (DateTime::<Utc>::MIN_UTC, DateTime::<Utc>::MAX_UTC);
        let edge = read_query_events(&path, min, Some(max), 100);
        assert_eq!(edge.len(), 4);
    }

    #[test]
    fn keeps_failures_and_skips_other_kinds_and_torn_lines() {
        let mut failed = query("bad", 2);
        failed.outcome = Outcome::Failure {
            error_type: Some("planning".into()),
            error_code: Some("SQE-1".into()),
            message: Some("no such table".into()),
        };
        let mut auth = query("auth", 3);
        auth.kind = AuditKind::Auth;
        let mut no_id = query("x", 4);
        no_id.query_id = None;
        let (_dir, path) = rotated_log(vec![query("ok", 1), failed, auth, no_id]);
        // A writer killed mid-line leaves a torn tail on the active file.
        let mut active = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        std::io::Write::write_all(&mut active, b"{\"time\":\"2026-06-20T12:05:00Z\",\"ki").unwrap();
        let events = read_query_events(&path, at(0), None, 100);
        assert_eq!(ids(&events), ["ok", "bad"]);
        assert!(matches!(events[1].outcome, Outcome::Failure { .. }));
    }

    #[test]
    fn a_missing_log_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("none.jsonl");
        assert!(read_query_events(&path, at(0), None, 10).is_empty());
    }
}
