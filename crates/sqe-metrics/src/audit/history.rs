//! Read finished queries back from the native audit log (#812).
//!
//! The coordinator keeps finished queries in memory only; the audit log is
//! the durable record. [`read_query_events`] returns the `kind = "query"`
//! events whose `time` (the moment the query ended) falls in a window,
//! reading the rotated generations and then the active file.
//!
//! Rotation is by size, so a file's name says nothing about its time range.
//! Every line starts with its `time`, and lines are appended in time order
//! give or take [`CLOCK_SLACK`] (events queued slightly out of order). So:
//!
//! - the first line of the next generation bounds this one from above, and a
//!   generation that ends before the window is not opened;
//! - within a file, the end of the window is found by bisecting byte offsets
//!   instead of reading up to it;
//! - files and lines are read newest first, and reading stops once no older
//!   line can be in the window or displace one of the `limit` newest events.
//!
//! A page of the newest 50 queries therefore costs the same on a day of log
//! as on a year of it.
//!
//! Only the native format is read. Under `format = "ocsf"` the native file
//! holds no query events, so the history is empty.

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
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

/// Cheap pre-filter before the full parse; the parse still checks the kind.
const QUERY_KIND: &str = "\"kind\":\"query\"";

/// Bisection stops when the range is this small; the backward scan covers
/// the rest.
const BISECT_FLOOR: u64 = 64 << 10;

/// Backward read block size.
const BLOCK: u64 = 256 << 10;

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
    if limit == 0 {
        return Vec::new();
    }
    let mut files: Vec<PathBuf> = rotated_audit_files(active)
        .into_iter()
        .map(|(_, path)| path)
        .collect();
    files.push(active.to_path_buf());
    let starts: Vec<Option<DateTime<Utc>>> = files.iter().map(|f| first_time(f)).collect();

    let mut newest = Newest::new(limit);
    let mut seq = 0u64;
    // Every line of the files after the current one is at least this old
    // minus the slack: the first time of the next non-empty generation.
    let mut next_start: Option<DateTime<Utc>> = None;
    for (file, start) in files.iter().zip(&starts).rev() {
        let Some(start) = *start else {
            continue;
        };
        let upper = next_start.map(|next| add(next, CLOCK_SLACK));
        next_start = Some(start);
        // This file and every older one end before the window or before
        // the oldest event kept.
        if upper.is_some_and(|upper| upper < from || newest.excludes(upper)) {
            break;
        }
        if to.is_some_and(|to| sub(start, CLOCK_SLACK) > to) {
            continue;
        }
        let done = scan_file(file, from, to, &mut newest, &mut seq);
        if done {
            break;
        }
    }
    newest.into_oldest_first()
}

/// Reads `file` backwards from the end of the window. Returns true when no
/// older line, in this file or an older one, can be kept.
fn scan_file(
    file: &Path,
    from: DateTime<Utc>,
    to: Option<DateTime<Utc>>,
    newest: &mut Newest,
    seq: &mut u64,
) -> bool {
    let Ok(mut handle) = File::open(file) else {
        return false;
    };
    let Ok(len) = handle.metadata().map(|m| m.len()) else {
        return false;
    };
    let end = match to {
        Some(to) => window_end(&mut handle, len, add(to, CLOCK_SLACK)).unwrap_or(len),
        None => len,
    };
    let mut done = false;
    let _ = for_each_line_backwards(&mut handle, end, |line| {
        let Some(time) = line_time(line) else {
            return true;
        };
        // Every earlier line is at most `time + slack`.
        let ceiling = add(time, CLOCK_SLACK);
        if ceiling < from || newest.excludes(ceiling) {
            done = true;
            return false;
        }
        if time < from || to.is_some_and(|to| time > to) || newest.excludes(time) {
            return true;
        }
        if !line.contains(QUERY_KIND) {
            return true;
        }
        let Ok(event) = serde_json::from_str::<AuditEvent>(line) else {
            return true;
        };
        if matches!(event.kind, AuditKind::Query) && event.query_id.is_some() {
            *seq += 1;
            newest.push(time, *seq, event);
        }
        true
    });
    done
}

/// The byte offset of a line start such that every line from it on is later
/// than `bound - slack`, found by bisecting on line starts; `None` when the
/// file cannot be read.
fn window_end(handle: &mut File, len: u64, bound: DateTime<Utc>) -> Option<u64> {
    // `lo`: a line start at or before the end; `hi`: a line start (or EOF)
    // whose line is after `bound`, so everything from it on is after
    // `bound - slack`.
    let (mut lo, mut hi) = (0u64, len);
    while hi - lo > BISECT_FLOOR {
        let mid = lo + (hi - lo) / 2;
        let Some((start, time)) = line_after(handle, mid, hi)? else {
            // One line spans [mid, hi): the backward scan reads it.
            break;
        };
        if time.is_some_and(|t| t > bound) {
            hi = start;
        } else {
            lo = start;
        }
    }
    Some(hi)
}

/// The first line that starts at or after `offset` and before `limit`, with
/// its time (`None` for a line that is not an event). `Ok(None)` when no line
/// starts in that range; `None` on an I/O error.
#[allow(clippy::type_complexity)]
fn line_after(
    handle: &mut File,
    offset: u64,
    limit: u64,
) -> Option<Option<(u64, Option<DateTime<Utc>>)>> {
    handle.seek(SeekFrom::Start(offset)).ok()?;
    let mut reader = BufReader::new(&mut *handle);
    let mut start = offset;
    let mut buf = Vec::new();
    if offset > 0 {
        // Skip the rest of the line `offset` falls in; `offset - 1` may
        // itself be the newline, so read from there.
        reader.seek_relative(-1).ok()?;
        start -= 1;
        start += reader.read_until(b'\n', &mut buf).ok()? as u64;
        buf.clear();
    }
    if start >= limit {
        return Some(None);
    }
    reader.read_until(b'\n', &mut buf).ok()?;
    let time = std::str::from_utf8(&buf).ok().and_then(line_time);
    Some(Some((start, time)))
}

/// Calls `visit` on each newline-terminated line of `handle[..end]`, last
/// line first, until it returns false. The bytes after the last newline are
/// a line the writer did not finish (the logger ends every line with one)
/// and are skipped, as are lines that are not UTF-8.
fn for_each_line_backwards(
    handle: &mut File,
    end: u64,
    mut visit: impl FnMut(&str) -> bool,
) -> std::io::Result<()> {
    let mut pos = end;
    // Bytes before the first newline seen in the blocks read so far: the
    // end of a line whose beginning is still to be read.
    let mut head: Vec<u8> = Vec::new();
    let mut block = Vec::new();
    let mut unterminated = true;
    while pos > 0 {
        let size = BLOCK.min(pos);
        pos -= size;
        handle.seek(SeekFrom::Start(pos))?;
        block.resize(size as usize, 0);
        handle.read_exact(&mut block)?;
        block.extend_from_slice(&head);
        head.clear();
        let mut line_end = block.len();
        while let Some(newline) = block[..line_end].iter().rposition(|&b| b == b'\n') {
            let line = &block[newline + 1..line_end];
            line_end = newline;
            if std::mem::take(&mut unterminated) || line.is_empty() {
                continue;
            }
            if let Ok(line) = std::str::from_utf8(line) {
                if !visit(line) {
                    return Ok(());
                }
            }
        }
        head.extend_from_slice(&block[..line_end]);
    }
    if !unterminated && !head.is_empty() {
        if let Ok(line) = std::str::from_utf8(&head) {
            visit(line);
        }
    }
    Ok(())
}

/// The `limit` newest events seen so far, as a min-heap on time.
struct Newest {
    limit: usize,
    heap: BinaryHeap<Reverse<Kept>>,
}

struct Kept {
    time: DateTime<Utc>,
    /// Scan order: higher is earlier in the log.
    seq: u64,
    event: AuditEvent,
}

impl Kept {
    /// Later time first; for equal times, later in the log first.
    fn key(&self) -> (DateTime<Utc>, Reverse<u64>) {
        (self.time, Reverse(self.seq))
    }
}

impl PartialEq for Kept {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}
impl Eq for Kept {}
impl PartialOrd for Kept {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Kept {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key().cmp(&other.key())
    }
}

impl Newest {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            heap: BinaryHeap::with_capacity(limit.min(1 << 16)),
        }
    }

    /// True when an event at `time` or earlier cannot be kept: the heap is
    /// full and its oldest event is newer.
    fn excludes(&self, time: DateTime<Utc>) -> bool {
        self.heap.len() == self.limit
            && self
                .heap
                .peek()
                .is_some_and(|Reverse(oldest)| time < oldest.time)
    }

    fn push(&mut self, time: DateTime<Utc>, seq: u64, event: AuditEvent) {
        let kept = Kept { time, seq, event };
        if self.heap.len() < self.limit {
            self.heap.push(Reverse(kept));
        } else if self
            .heap
            .peek()
            .is_some_and(|Reverse(oldest)| kept > *oldest)
        {
            self.heap.pop();
            self.heap.push(Reverse(kept));
        }
    }

    fn into_oldest_first(self) -> Vec<AuditEvent> {
        // `into_sorted_vec` on `Reverse` is newest first.
        let mut kept: Vec<AuditEvent> = self
            .heap
            .into_sorted_vec()
            .into_iter()
            .map(|Reverse(kept)| kept.event)
            .collect();
        kept.reverse();
        kept
    }
}

fn add(time: DateTime<Utc>, delta: Duration) -> DateTime<Utc> {
    time.checked_add_signed(delta)
        .unwrap_or(DateTime::<Utc>::MAX_UTC)
}

fn sub(time: DateTime<Utc>, delta: Duration) -> DateTime<Utc> {
    time.checked_sub_signed(delta)
        .unwrap_or(DateTime::<Utc>::MIN_UTC)
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
    let handle = File::open(path).ok()?;
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

    /// The backward, bisecting reader agrees with a plain forward filter on
    /// random windows and limits. The log has three generations, lines up to
    /// 30 s out of order, other kinds, a legacy line and a torn tail.
    #[test]
    fn matches_a_forward_scan_on_random_windows() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");
        let base = at(0);
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move |modulo: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % modulo
        };
        // (time, id) in file order, query events only.
        let mut written: Vec<(DateTime<Utc>, String)> = Vec::new();
        let mut event = crate::audit::event::sample_query_event();
        let files = [
            format!("{}.00000001", path.display()),
            format!("{}.00000002", path.display()),
            path.display().to_string(),
        ];
        let mut i = 0i64;
        for (n, file) in files.iter().enumerate() {
            let mut out = std::fs::File::create(file).unwrap();
            if n == 0 {
                writeln!(out, "{{\"timestamp\":\"2026-06-20T11:00:00Z\"}}").unwrap();
            }
            for _ in 0..1500 {
                // Two seconds apart, jittered back by up to 30 s.
                let time = base + Duration::seconds(2 * i) - Duration::seconds(next(31) as i64);
                event.time = time;
                event.kind = if next(10) == 0 {
                    AuditKind::Auth
                } else {
                    AuditKind::Query
                };
                event.query_id = Some(format!("q{i}"));
                writeln!(out, "{}", serde_json::to_string(&event).unwrap()).unwrap();
                if matches!(event.kind, AuditKind::Query) {
                    written.push((time, format!("q{i}")));
                }
                i += 1;
            }
            if n == 2 {
                write!(out, "{{\"time\":\"2026-06-20T14:00:00Z\",\"ki").unwrap();
            }
        }
        let span = 2 * i;
        for _ in 0..200 {
            let a = base + Duration::seconds(next(span as u64 + 100) as i64 - 50);
            let b = a + Duration::seconds(next(span as u64 / 2) as i64);
            let to = (next(4) != 0).then_some(b);
            let limit = [1, 7, 50, 400, 10_000][next(5) as usize];
            let mut expected: Vec<(DateTime<Utc>, usize, &str)> = written
                .iter()
                .enumerate()
                .filter(|(_, (t, _))| *t >= a && to.is_none_or(|to| *t <= to))
                .map(|(pos, (t, id))| (*t, pos, id.as_str()))
                .collect();
            expected.sort();
            let skip = expected.len().saturating_sub(limit);
            let expected: Vec<&str> = expected[skip..].iter().map(|(_, _, id)| *id).collect();
            let got = read_query_events(&path, a, to, limit);
            assert_eq!(ids(&got), expected, "from {a} to {to:?} limit {limit}");
        }
    }

    /// Seven days of audit log, one query event every 0.6 s (about 1M
    /// events, 1.3 KB each), rotated at 256 MiB like the default config.
    /// Prints the read time per window; run in release:
    /// `cargo test -p sqe-metrics --release --lib history_seven_day_read_time -- --ignored --nocapture`.
    #[test]
    #[ignore = "bench; writes ~1.3 GB of audit log"]
    fn history_seven_day_read_time() {
        use std::io::Write;
        const EVENTS: i64 = 1_008_000;
        const STEP_MS: i64 = 600;
        const ROTATE_BYTES: usize = 256 << 20;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");
        let end = Utc::now();
        let start = end - Duration::milliseconds(EVENTS * STEP_MS);
        let mut event = crate::audit::event::sample_query_event();
        event.query.as_mut().unwrap().text = Some(format!(
            "SELECT o.order_id, c.name, sum(l.amount) FROM sales.orders o JOIN sales.customers c \
             ON o.customer_id = c.id JOIN sales.lines l ON l.order_id = o.order_id WHERE \
             o.order_date >= DATE '2026-01-01' GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 100 -- {}",
            "x".repeat(400)
        ));
        let started = std::time::Instant::now();
        let (mut generation, mut written) = (1u64, 0usize);
        let mut out = std::io::BufWriter::new(std::fs::File::create(&path).unwrap());
        for i in 0..EVENTS {
            event.time = start + Duration::milliseconds(i * STEP_MS);
            event.query_id = Some(format!("{i:032}"));
            let line = serde_json::to_string(&event).unwrap();
            if written + line.len() > ROTATE_BYTES {
                out.flush().unwrap();
                drop(out);
                let rotated = format!("{}.{generation:08}", path.display());
                std::fs::rename(&path, rotated).unwrap();
                out = std::io::BufWriter::new(std::fs::File::create(&path).unwrap());
                (generation, written) = (generation + 1, 0);
            }
            writeln!(out, "{line}").unwrap();
            written += line.len() + 1;
        }
        out.flush().unwrap();
        println!(
            "wrote {EVENTS} events in {} files ({:?})",
            generation,
            started.elapsed()
        );

        let cases = [
            ("last hour", end - Duration::hours(1), None, 100_000),
            ("last day", end - Duration::days(1), None, 100_000),
            ("seven days, cap 100k", start, None, 100_000),
            (
                "one hour, 3 days back",
                end - Duration::days(3) - Duration::hours(1),
                Some(end - Duration::days(3)),
                100_000,
            ),
            ("page of 50, newest", start, None, 50),
            (
                "page of 50, 5 days back",
                start,
                Some(end - Duration::days(5)),
                50,
            ),
        ];
        for (name, from, to, limit) in cases {
            let t = std::time::Instant::now();
            let events = read_query_events(&path, from, to, limit);
            println!(
                "{name:<28} {:>7} events {:>10.3?}",
                events.len(),
                t.elapsed()
            );
        }
    }

    #[test]
    fn a_missing_log_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("none.jsonl");
        assert!(read_query_events(&path, at(0), None, 10).is_empty());
    }
}
