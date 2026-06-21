use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use chrono::DateTime;
use serde::{Deserialize, Serialize};
use tracing::info;

use super::chain::HashChain;
use super::event::{
    Actor, AuditEvent, AuditKind, Integrity, Outcome, PolicyAudit, QueryInfo, QueryStats, Timing,
};
use super::redact::{mask_gdpr_columns, redact_pii, strip_sql_literals, GdprIdentifierMode};
use super::sink::{AuditFormat, AuditSink, NativeJsonlSink, OcsfJsonlSink};
use super::tag_lookup::TagLookup;

/// Snapshot of GDPR masking config captured from the writer thread per recv.
///
/// Tuple: `(gdpr_tags, identifier_mode, salt, lookup)`. Kept as a type alias
/// so the complex type appears once rather than at every function boundary.
type GdprSnap = (Vec<String>, GdprIdentifierMode, String, Arc<dyn TagLookup>);

/// Configuration for GDPR-tag masking on the audit writer thread.
///
/// Held behind `Arc<Mutex<Option<GdprConfig>>>` so the builder method
/// `with_gdpr` can set it after the worker thread has already been spawned.
/// The worker reads a snapshot on each event batch; there is no hot-path
/// lock contention because tag config is write-once at startup.
struct GdprConfig {
    /// Tag labels that mark a column as GDPR-sensitive. A column whose tag set
    /// intersects this list will have its identifier (and adjacent literals)
    /// masked before the event reaches the chain.
    tags: Vec<String>,
    /// How tagged identifiers are represented after masking.
    mode: GdprIdentifierMode,
    /// Stable per-deployment salt used by `Tokenize` mode. Not secret-grade;
    /// it makes tokens correlatable across log lines within a deployment but
    /// will differ across restarts when derived at startup.
    salt: String,
    /// Tag lookup backend. The coordinator wires `AuditTagAdapter` wrapping
    /// `CacheTagSource`; tests inject a stub.
    lookup: Arc<dyn TagLookup>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AuditEntry {
    pub timestamp: String,
    pub username: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// SHA-256 hash of normalised SQL (whitespace-collapsed, uppercase keywords).
    pub query_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_text: Option<String>,
    pub statement_type: String,
    pub duration_ms: u64,
    pub rows_returned: usize,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_ip: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tables_touched: Vec<String>,

    // --- Policy-decision fields (issue: no-policy-decision-audit) ---
    // Populated from the `PolicySummary` the enforcer returns. `serde(default)`
    // so existing call sites and log consumers that don't set them keep working,
    // and a deny-all (zero rows) is no longer indistinguishable from a
    // legitimate empty result.
    /// Count of row-filter expressions injected by policy (excludes the
    /// deny-all sentinel; a deny is reflected in `policy_denied`).
    #[serde(default)]
    pub row_filters_applied: usize,
    /// Columns masked by policy (sorted names).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns_masked: Vec<String>,
    /// Columns restricted/dropped by policy (sorted names).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns_restricted: Vec<String>,
    /// True when at least one scanned table was denied (deny-all row filter:
    /// resolve failure, breaker-open, unknown-tag state, or fully-restricted).
    #[serde(default)]
    pub policy_denied: bool,
}

/// Compute SHA-256 hash of normalised SQL (whitespace-collapsed, uppercase keywords).
pub fn query_hash(sql: &str) -> String {
    use sha2::{Digest, Sha256};
    let normalised: String = sql
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_uppercase();
    let hash = Sha256::digest(normalised.as_bytes());
    format!("{hash:x}")
}

/// Compute a chain hash over raw bytes: `sha256(prev_hash || body_bytes)`.
///
/// Used to chain legacy `AuditEntry` records without converting them to `AuditEvent`.
fn chain_hash_raw(prev_hash: &str, body: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(prev_hash.as_bytes());
    hasher.update(body);
    format!("{:x}", hasher.finalize())
}

/// Inject an `Integrity` block into a serialized JSON object string.
///
/// Expects `json` to be a `{...}` object. Strips the closing `}`, appends
/// `,"integrity":{...}}`. If `json` is empty or malformed, appends the
/// integrity as a standalone object (best-effort, never panics).
fn inject_integrity(json: &str, integrity: &Integrity) -> String {
    let integrity_json = match serde_json::to_string(integrity) {
        Ok(s) => s,
        Err(_) => return json.to_string(),
    };
    let trimmed = json.trim_end();
    if let Some(body) = trimmed.strip_suffix('}') {
        format!("{body},\"integrity\":{integrity_json}}}")
    } else {
        // Malformed JSON: append best-effort.
        format!("{json},\"integrity\":{integrity_json}")
    }
}

/// Convert a (pre-redacted) `AuditEntry` to the canonical `AuditEvent` type.
///
/// Used by `log_event` paths that need the structured event. The legacy `log()`
/// path preserves the flat `AuditEntry` JSON format.
///
/// Mapping notes:
/// - `timestamp: String` parsed as RFC-3339; falls back to `Utc::now()` on failure.
/// - `status: String` "success" -> `Outcome::Success`; anything else -> `Outcome::Failure`.
/// - `tables_touched: Vec<String>` has no exact target in the structured event model.
///   The field is noted but currently dropped from the AuditEvent representation.
///   It will be wired into `resources` in a follow-up task.
impl From<AuditEntry> for AuditEvent {
    fn from(e: AuditEntry) -> Self {
        let time = DateTime::parse_from_rfc3339(&e.timestamp)
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or_else(|_| chrono::Utc::now());

        let outcome = if e.status.eq_ignore_ascii_case("success") {
            Outcome::Success
        } else {
            Outcome::Failure {
                error_type: Some(e.status.clone()),
                error_code: None,
                message: None,
            }
        };

        let policy = if e.row_filters_applied > 0
            || !e.columns_masked.is_empty()
            || !e.columns_restricted.is_empty()
            || e.policy_denied
        {
            Some(PolicyAudit {
                row_filters_applied: e.row_filters_applied,
                columns_masked: e.columns_masked,
                columns_restricted: e.columns_restricted,
                denied: e.policy_denied,
            })
        } else {
            None
        };

        AuditEvent {
            time,
            kind: AuditKind::Query,
            actor: Actor {
                username: e.username,
                ..Default::default()
            },
            outcome,
            resources: Vec::new(),
            policy,
            timing: Some(Timing {
                duration_ms: e.duration_ms,
                ..Default::default()
            }),
            stats: Some(QueryStats {
                rows_returned: e.rows_returned,
                ..Default::default()
            }),
            query: Some(QueryInfo {
                text: e.query_text,
                query_hash: e.query_hash,
                statement_type: e.statement_type,
            }),
            session_id: e.session_id,
            client_ip: e.client_ip,
            integrity: Integrity::default(),
        }
    }
}

/// Message type sent to the audit writer thread.
enum AuditMsg {
    /// Legacy `AuditEntry` path from `log()`. Already PII-redacted.
    /// Written as flat `AuditEntry` JSON + an appended `integrity` block,
    /// preserving the on-disk format for consumers that parse the flat shape.
    Legacy(Box<AuditEntry>),
    /// Canonical event from `log_event()`. Fan-out to all sinks with chain stamp.
    Event(Box<AuditEvent>),
    Flush(Sender<()>),
}

pub struct AuditLogger {
    tx: Option<Sender<AuditMsg>>,
    worker: Mutex<Option<JoinHandle<()>>>,
    /// Shared with the worker thread. `None` until `with_gdpr` is called;
    /// the worker reads a snapshot each time it processes events.
    gdpr: Arc<Mutex<Option<GdprConfig>>>,
}

/// Derive the OCSF file path from the native path.
///
/// Given `audit.jsonl`, produces `audit.ocsf.jsonl`.
/// Given `audit` (no extension), produces `audit.ocsf.jsonl`.
/// This keeps both files in the same directory with clear naming.
fn ocsf_path_from(native_path: &std::path::Path) -> std::path::PathBuf {
    let stem = native_path
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy();
    let dir = native_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    dir.join(format!("{stem}.ocsf.jsonl"))
}

fn open_sink_file(path: &std::path::Path) -> Result<std::fs::File, String> {
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| format!("Failed to open audit log file '{}': {e}", path.display()))
}

impl AuditLogger {
    /// Create a logger writing native JSONL to `path`.
    ///
    /// An empty path disables logging (no-op logger).
    pub fn new(path: &str) -> Result<Self, String> {
        Self::with_config(path, AuditFormat::Native)
    }

    /// Create a logger with a configurable output format.
    ///
    /// - `Native`: writes to `path`.
    /// - `Ocsf`: writes OCSF JSON to `path`.
    /// - `Both`: writes native JSON to `path` and OCSF JSON to the derived
    ///   path (see [`ocsf_path_from`]).
    ///
    /// The legacy `log(&AuditEntry)` path always writes flat `AuditEntry` JSON
    /// (with an appended `integrity` block) to the native/ocsf sink's underlying
    /// file. `log_event` fans out to all sinks via the `AuditSink` trait.
    ///
    /// An empty `path` disables logging regardless of format.
    pub fn with_config(path: &str, format: AuditFormat) -> Result<Self, String> {
        if path.is_empty() {
            return Ok(Self {
                tx: None,
                worker: Mutex::new(None),
                gdpr: Arc::new(Mutex::new(None)),
            });
        }

        let native_path = std::path::Path::new(path);

        // Open exactly one handle per physical file. The legacy `log()` path
        // routes through `native_sink.write_raw_line()` so there is never a
        // second writer on the native file.
        //
        // Layout by format:
        //   Native: native_sink -> path (legacy flat + canonical native JSON)
        //   Ocsf:   native_sink -> path (legacy flat only), ocsf_sink -> path.ocsf.jsonl (canonical OCSF)
        //   Both:   native_sink -> path (legacy flat + canonical native JSON), ocsf_sink -> path.ocsf.jsonl (canonical OCSF)
        //
        // `native_events` controls whether canonical events are also written to
        // native_sink (true for Native/Both; false for Ocsf where path is legacy-only).
        let native_file = open_sink_file(native_path)?;
        let native_sink = NativeJsonlSink::from_writer(Box::new(
            std::io::BufWriter::new(native_file),
        ));

        let (ocsf_sink, native_events): (Option<OcsfJsonlSink>, bool) = match format {
            AuditFormat::Native => (None, true),
            AuditFormat::Ocsf => {
                // Canonical events go to the derived .ocsf.jsonl file only;
                // `path` carries legacy flat entries exclusively.
                let ocsf_p = ocsf_path_from(native_path);
                let ocsf_file = open_sink_file(&ocsf_p)?;
                (Some(OcsfJsonlSink::from_writer(Box::new(ocsf_file))), false)
            }
            AuditFormat::Both => {
                let ocsf_p = ocsf_path_from(native_path);
                let ocsf_file = open_sink_file(&ocsf_p)?;
                (Some(OcsfJsonlSink::from_writer(Box::new(ocsf_file))), true)
            }
        };

        let path_owned = path.to_string();
        let gdpr: Arc<Mutex<Option<GdprConfig>>> = Arc::new(Mutex::new(None));
        let gdpr_worker = Arc::clone(&gdpr);
        let (tx, rx) = mpsc::channel::<AuditMsg>();
        let worker = std::thread::Builder::new()
            .name("sqe-audit-writer".to_string())
            .spawn(move || {
                let mut native_sink = native_sink;
                let mut ocsf_sink = ocsf_sink;
                let mut chain = HashChain::new();
                while let Ok(msg) = rx.recv() {
                    // Snapshot GDPR config once per recv iteration. Config is
                    // written once at startup (with_gdpr); this lock is never
                    // contended on the hot path.
                    let gdpr_snap: Option<GdprSnap> = {
                        if let Ok(guard) = gdpr_worker.lock() {
                            guard.as_ref().map(|c| {
                                (c.tags.clone(), c.mode, c.salt.clone(), Arc::clone(&c.lookup))
                            })
                        } else {
                            None
                        }
                    };
                    match msg {
                        AuditMsg::Legacy(entry) => {
                            let mut batch = vec![*entry];
                            while let Ok(more) = rx.try_recv() {
                                match more {
                                    AuditMsg::Legacy(e) => batch.push(*e),
                                    AuditMsg::Event(e) => {
                                        // Write and clear the pending legacy batch first so
                                        // that the interleaved event gets a higher seq than
                                        // all legacy entries that arrived before it.
                                        Self::write_legacy_batch(
                                            &mut native_sink,
                                            &mut chain,
                                            &batch,
                                            &path_owned,
                                        );
                                        batch.clear();
                                        Self::flush_sinks(&mut native_sink, &mut ocsf_sink, &path_owned);
                                        // Now stamp and write the canonical event.
                                        Self::write_event(
                                            &mut native_sink,
                                            native_events,
                                            &mut ocsf_sink,
                                            &mut chain,
                                            *e,
                                            gdpr_snap.as_ref(),
                                            &path_owned,
                                        );
                                        Self::flush_sinks(&mut native_sink, &mut ocsf_sink, &path_owned);
                                    }
                                    AuditMsg::Flush(ack) => {
                                        Self::write_legacy_batch(
                                            &mut native_sink,
                                            &mut chain,
                                            &batch,
                                            &path_owned,
                                        );
                                        batch.clear();
                                        Self::flush_sinks(&mut native_sink, &mut ocsf_sink, &path_owned);
                                        let _ = ack.send(());
                                    }
                                }
                            }
                            if !batch.is_empty() {
                                Self::write_legacy_batch(
                                    &mut native_sink,
                                    &mut chain,
                                    &batch,
                                    &path_owned,
                                );
                                Self::flush_sinks(&mut native_sink, &mut ocsf_sink, &path_owned);
                            }
                        }
                        AuditMsg::Event(event) => {
                            let mut batch = vec![*event];
                            while let Ok(more) = rx.try_recv() {
                                match more {
                                    AuditMsg::Event(e) => batch.push(*e),
                                    AuditMsg::Legacy(e) => {
                                        Self::write_events(
                                            &mut native_sink,
                                            native_events,
                                            &mut ocsf_sink,
                                            &mut chain,
                                            &mut batch,
                                            gdpr_snap.as_ref(),
                                            &path_owned,
                                        );
                                        batch.clear();
                                        Self::flush_sinks(&mut native_sink, &mut ocsf_sink, &path_owned);
                                        // Process this legacy entry immediately.
                                        Self::write_legacy_batch(
                                            &mut native_sink,
                                            &mut chain,
                                            &[*e],
                                            &path_owned,
                                        );
                                        Self::flush_sinks(&mut native_sink, &mut ocsf_sink, &path_owned);
                                    }
                                    AuditMsg::Flush(ack) => {
                                        Self::write_events(
                                            &mut native_sink,
                                            native_events,
                                            &mut ocsf_sink,
                                            &mut chain,
                                            &mut batch,
                                            gdpr_snap.as_ref(),
                                            &path_owned,
                                        );
                                        batch.clear();
                                        Self::flush_sinks(&mut native_sink, &mut ocsf_sink, &path_owned);
                                        let _ = ack.send(());
                                    }
                                }
                            }
                            if !batch.is_empty() {
                                Self::write_events(
                                    &mut native_sink,
                                    native_events,
                                    &mut ocsf_sink,
                                    &mut chain,
                                    &mut batch,
                                    gdpr_snap.as_ref(),
                                    &path_owned,
                                );
                                Self::flush_sinks(&mut native_sink, &mut ocsf_sink, &path_owned);
                            }
                        }
                        AuditMsg::Flush(ack) => {
                            Self::flush_sinks(&mut native_sink, &mut ocsf_sink, &path_owned);
                            let _ = ack.send(());
                        }
                    }
                }
                Self::flush_sinks(&mut native_sink, &mut ocsf_sink, &path_owned);
            })
            .map_err(|e| format!("Failed to start audit writer thread: {e}"))?;

        info!(path = path, "Audit log initialized");
        Ok(Self {
            tx: Some(tx),
            worker: Mutex::new(Some(worker)),
            gdpr,
        })
    }

    /// Write a batch of legacy `AuditEntry` records as flat JSON + integrity block.
    ///
    /// Routes through `native_sink.write_raw_line` so there is exactly one writer
    /// on the native file (Defect 2 fix).
    fn write_legacy_batch(
        native_sink: &mut NativeJsonlSink,
        chain: &mut HashChain,
        batch: &[AuditEntry],
        path: &str,
    ) {
        for entry in batch {
            let body_json = match serde_json::to_string(entry) {
                Ok(j) => j,
                Err(e) => {
                    tracing::error!("AUDIT: serialization failed: {e}");
                    continue;
                }
            };
            // Build integrity: seq + prev_hash are set by the chain state;
            // hash = sha256(prev_hash || body_json).
            let seq = chain.next_seq();
            let prev = chain.current_prev_hash().to_string();
            let hash = chain_hash_raw(&prev, body_json.as_bytes());
            chain.advance(hash.clone());
            let integrity = Integrity { seq, prev_hash: prev, hash };
            let line = inject_integrity(&body_json, &integrity);
            if let Err(e) = native_sink.write_raw_line(&line) {
                tracing::error!("AUDIT: write failed for '{path}': {e}");
            }
        }
    }

    /// Apply GDPR masking + PII redaction to an event's query text.
    ///
    /// Called on the writer thread BEFORE chain stamping so the chain covers the
    /// redacted bytes. Masking order:
    ///
    /// 1. For each `Resource`, call `lookup.column_tags(...)`.
    ///    - `Some(map)`: collect columns whose tag set intersects `gdpr_tags`.
    ///    - `None`: set a `fallback` flag (unknown tag state, must fail closed).
    /// 2. If any masked columns found, run `mask_gdpr_columns`.
    /// 3. Always run `redact_pii` (belt-and-suspenders for known PII patterns).
    /// 4. If `fallback` is set and no columns were matched, run
    ///    `strip_sql_literals` (conservative: unknown tag state may hide a GDPR
    ///    column mask we cannot see).
    fn apply_gdpr_masking(event: &mut AuditEvent, gdpr: &GdprSnap) {
        let (tags, mode, salt, lookup) = gdpr;
        let Some(ref mut query) = event.query else {
            return;
        };
        let Some(ref text) = query.text.clone() else {
            return;
        };

        let mut masked_cols: Vec<String> = Vec::new();
        let mut fallback = false;

        for resource in &event.resources {
            match lookup.column_tags(resource.catalog.as_deref(), &resource.namespace, &resource.name) {
                Some(col_map) => {
                    for (col, col_tags) in &col_map {
                        if col_tags.iter().any(|t| tags.iter().any(|g| g.eq_ignore_ascii_case(t)))
                            && !masked_cols.contains(col)
                        {
                            masked_cols.push(col.clone());
                        }
                    }
                }
                None => {
                    fallback = true;
                }
            }
        }

        let mut out = text.clone();

        if !masked_cols.is_empty() {
            out = mask_gdpr_columns(&out, &masked_cols, *mode, salt);
        }

        // Always apply PII pattern redaction.
        out = redact_pii(&out);

        // If any resource had unknown tag state and no columns were masked,
        // strip all literals as the conservative fail-closed path.
        if fallback && masked_cols.is_empty() {
            out = strip_sql_literals(&out);
        }

        query.text = Some(out);
    }

    /// Apply `redact_pii` to `event.query.text` when no GDPR masking is active.
    ///
    /// When GDPR masking IS active, `apply_gdpr_masking` runs `redact_pii`
    /// internally (step 3 of its masking order). This method covers the
    /// complementary case so that `redact_pii` runs unconditionally on every
    /// `log_event` path, matching the redaction contract of the legacy `log()` path.
    fn redact_event_query(event: &mut AuditEvent) {
        if let Some(ref mut query) = event.query {
            if let Some(ref text) = query.text.clone() {
                query.text = Some(redact_pii(text));
            }
        }
    }

    /// Write a single canonical `AuditEvent` to active sinks with chain stamping.
    ///
    /// `native_events` controls whether the event is also written to `native_sink`
    /// (native JSON format). When false (Ocsf-only mode), only the OCSF sink
    /// receives canonical events; `native_sink` still handles legacy flat entries.
    ///
    /// Redaction order (applied before chain stamping so the chain covers redacted bytes):
    /// - When `gdpr` is `Some`: GDPR-tag masking runs via `apply_gdpr_masking`,
    ///   which internally calls `redact_pii` as its step 3.
    /// - When `gdpr` is `None`: `redact_pii` runs unconditionally via
    ///   `redact_event_query`, matching the legacy `log()` redaction contract.
    fn write_event(
        native_sink: &mut NativeJsonlSink,
        native_events: bool,
        ocsf_sink: &mut Option<OcsfJsonlSink>,
        chain: &mut HashChain,
        mut event: AuditEvent,
        gdpr: Option<&GdprSnap>,
        path: &str,
    ) {
        if let Some(g) = gdpr {
            Self::apply_gdpr_masking(&mut event, g);
        } else {
            Self::redact_event_query(&mut event);
        }
        chain.stamp(&mut event);
        if native_events {
            if let Err(e) = native_sink.write_line(&event) {
                tracing::error!("AUDIT: write failed for '{path}': {e}");
            }
        }
        if let Some(sink) = ocsf_sink.as_mut() {
            if let Err(e) = sink.write_line(&event) {
                tracing::error!("AUDIT: write failed for '{path}': {e}");
            }
        }
    }

    /// Write a batch of canonical `AuditEvent` records to active sinks with chain stamping.
    ///
    /// See `write_event` for the `native_events` and `gdpr` semantics.
    fn write_events(
        native_sink: &mut NativeJsonlSink,
        native_events: bool,
        ocsf_sink: &mut Option<OcsfJsonlSink>,
        chain: &mut HashChain,
        batch: &mut [AuditEvent],
        gdpr: Option<&GdprSnap>,
        path: &str,
    ) {
        for event in batch.iter_mut() {
            if let Some(g) = gdpr {
                Self::apply_gdpr_masking(event, g);
            } else {
                Self::redact_event_query(event);
            }
            chain.stamp(event);
            if native_events {
                if let Err(e) = native_sink.write_line(event) {
                    tracing::error!("AUDIT: write failed for '{path}': {e}");
                }
            }
            if let Some(sink) = ocsf_sink.as_mut() {
                if let Err(e) = sink.write_line(event) {
                    tracing::error!("AUDIT: write failed for '{path}': {e}");
                }
            }
        }
    }

    fn flush_sinks(
        native_sink: &mut NativeJsonlSink,
        ocsf_sink: &mut Option<OcsfJsonlSink>,
        path: &str,
    ) {
        if let Err(e) = native_sink.flush() {
            tracing::error!("AUDIT: flush failed for '{path}': {e}");
        }
        if let Some(sink) = ocsf_sink.as_mut() {
            if let Err(e) = sink.flush() {
                tracing::error!("AUDIT: flush failed for '{path}': {e}");
            }
        }
    }

    /// Configure GDPR-tag masking on the audit writer.
    ///
    /// Must be called after `with_config` and before the logger is shared.
    /// When `tags` is empty, this call is a no-op (masking stays disabled).
    ///
    /// The `lookup` backend is called on the worker thread for each event that
    /// carries resources. The coordinator wires `AuditTagAdapter` (wrapping
    /// `CacheTagSource`) so tag resolution uses the existing metadata cache with
    /// zero extra network calls.
    ///
    /// `salt` should be stable within a deployment (set once at startup, derived
    /// from a random UUID or a config field). It enables correlation of the same
    /// column token across log lines but is NOT secret-grade.
    pub fn with_gdpr(
        self,
        tags: Vec<String>,
        mode: GdprIdentifierMode,
        salt: String,
        lookup: Arc<dyn TagLookup>,
    ) -> Self {
        if !tags.is_empty() {
            if let Ok(mut guard) = self.gdpr.lock() {
                *guard = Some(GdprConfig { tags, mode, salt, lookup });
            }
        }
        self
    }

    /// Log an `AuditEntry` (legacy path). Redaction runs here before the entry
    /// is sent to the worker. The worker writes the flat `AuditEntry` JSON format
    /// plus an appended `integrity` block, preserving backward compatibility with
    /// consumers that parse the flat shape.
    pub fn log(&self, entry: &AuditEntry) {
        let Some(ref tx) = self.tx else {
            return;
        };

        // Redaction runs on the caller side, before the entry is queued.
        // Task 7 will add redaction for the `log_event` path.
        let redacted = if entry.query_text.is_some() {
            AuditEntry {
                query_text: entry.query_text.as_deref().map(redact_pii),
                timestamp: entry.timestamp.clone(),
                username: entry.username.clone(),
                session_id: entry.session_id.clone(),
                query_hash: entry.query_hash.clone(),
                statement_type: entry.statement_type.clone(),
                duration_ms: entry.duration_ms,
                rows_returned: entry.rows_returned,
                status: entry.status.clone(),
                client_ip: entry.client_ip.clone(),
                tables_touched: entry.tables_touched.clone(),
                row_filters_applied: entry.row_filters_applied,
                columns_masked: entry.columns_masked.clone(),
                columns_restricted: entry.columns_restricted.clone(),
                policy_denied: entry.policy_denied,
            }
        } else {
            AuditEntry {
                timestamp: entry.timestamp.clone(),
                username: entry.username.clone(),
                session_id: entry.session_id.clone(),
                query_hash: entry.query_hash.clone(),
                query_text: None,
                statement_type: entry.statement_type.clone(),
                duration_ms: entry.duration_ms,
                rows_returned: entry.rows_returned,
                status: entry.status.clone(),
                client_ip: entry.client_ip.clone(),
                tables_touched: entry.tables_touched.clone(),
                row_filters_applied: entry.row_filters_applied,
                columns_masked: entry.columns_masked.clone(),
                columns_restricted: entry.columns_restricted.clone(),
                policy_denied: entry.policy_denied,
            }
        };

        if let Err(e) = tx.send(AuditMsg::Legacy(Box::new(redacted))) {
            tracing::error!("AUDIT: writer dropped, entry not recorded: {e}");
        }
    }

    /// Log a canonical `AuditEvent` directly. Redaction (Task 7) is not yet
    /// applied here; callers are responsible for sanitising PII before calling
    /// this method.
    pub fn log_event(&self, event: AuditEvent) {
        let Some(ref tx) = self.tx else {
            return;
        };
        if let Err(e) = tx.send(AuditMsg::Event(Box::new(event))) {
            tracing::error!("AUDIT: writer dropped, event not recorded: {e}");
        }
    }

    /// Block until every entry sent before this call has been flushed to disk.
    /// Intended for shutdown paths and tests that read the file synchronously.
    pub fn flush(&self) {
        let Some(ref tx) = self.tx else {
            return;
        };
        let (ack_tx, ack_rx) = mpsc::channel();
        if tx.send(AuditMsg::Flush(ack_tx)).is_err() {
            return;
        }
        let _ = ack_rx.recv();
    }
}

impl Drop for AuditLogger {
    fn drop(&mut self) {
        self.flush();
        // Drop the sender so the writer thread exits, then join.
        self.tx.take();
        if let Ok(mut guard) = self.worker.lock() {
            if let Some(handle) = guard.take() {
                let _ = handle.join();
            }
        }
    }
}

impl std::fmt::Debug for AuditLogger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuditLogger")
            .field("active", &self.tx.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use tempfile::TempDir;

    fn fresh_audit_path(name: &str) -> (TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join(name);
        (dir, path)
    }

    fn test_entry() -> AuditEntry {
        AuditEntry {
            timestamp: Utc::now().to_rfc3339(),
            username: "test".to_string(),
            session_id: Some("sess-123".to_string()),
            query_hash: query_hash("SELECT 1"),
            query_text: Some("SELECT 1".to_string()),
            statement_type: "query".to_string(),
            duration_ms: 42,
            rows_returned: 1,
            status: "success".to_string(),
            client_ip: Some("127.0.0.1".to_string()),
            tables_touched: Vec::new(),
            row_filters_applied: 0,
            columns_masked: Vec::new(),
            columns_restricted: Vec::new(),
            policy_denied: false,
        }
    }

    #[test]
    fn test_noop_logger() {
        let logger = AuditLogger::new("").unwrap();
        logger.log(&test_entry());
    }

    #[test]
    fn test_audit_entry_serialization() {
        let entry = test_entry();
        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("\"username\":\"test\""));
        assert!(json.contains("\"query_hash\":"));
        assert!(json.contains("\"session_id\":\"sess-123\""));
    }

    #[test]
    fn test_audit_entry_omits_none_fields() {
        let mut entry = test_entry();
        entry.query_text = None;
        entry.client_ip = None;
        entry.session_id = None;
        let json = serde_json::to_string(&entry).unwrap();
        assert!(!json.contains("query_text"));
        assert!(!json.contains("client_ip"));
        assert!(!json.contains("session_id"));
    }

    #[test]
    fn test_audit_entry_policy_fields_serialize() {
        let mut entry = test_entry();
        entry.row_filters_applied = 2;
        entry.columns_masked = vec!["salary".to_string(), "ssn".to_string()];
        entry.columns_restricted = vec!["notes".to_string()];
        entry.policy_denied = false;
        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("\"row_filters_applied\":2"));
        assert!(json.contains("\"columns_masked\":[\"salary\",\"ssn\"]"));
        assert!(json.contains("\"columns_restricted\":[\"notes\"]"));
        assert!(json.contains("\"policy_denied\":false"));
    }

    #[test]
    fn test_audit_entry_policy_denied_serialize() {
        let mut entry = test_entry();
        entry.policy_denied = true;
        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("\"policy_denied\":true"));
    }

    #[test]
    fn test_audit_entry_deserializes_legacy_without_policy_fields() {
        // A pre-existing log line (no policy fields) must still parse, defaulting
        // the new fields. Guards the `serde(default)` contract for log consumers.
        let legacy = r#"{
            "timestamp": "2026-01-01T00:00:00Z",
            "username": "bob",
            "query_hash": "abc",
            "statement_type": "query",
            "duration_ms": 1,
            "rows_returned": 0,
            "status": "success"
        }"#;
        let entry: AuditEntry = serde_json::from_str(legacy).unwrap();
        assert_eq!(entry.row_filters_applied, 0);
        assert!(entry.columns_masked.is_empty());
        assert!(entry.columns_restricted.is_empty());
        assert!(!entry.policy_denied);
    }

    #[test]
    fn test_query_hash_normalises() {
        let h1 = query_hash("select  1  from   t");
        let h2 = query_hash("SELECT 1 FROM T");
        assert_eq!(h1, h2, "Hashes should match after normalisation");
    }

    #[test]
    fn test_query_hash_differs_for_different_sql() {
        let h1 = query_hash("SELECT 1");
        let h2 = query_hash("SELECT 2");
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_file_logger_writes() {
        let (_dir, path) = fresh_audit_path("sqe-audit-test.jsonl");
        let path_str = path.to_str().unwrap();

        let logger = AuditLogger::new(path_str).unwrap();
        logger.log(&test_entry());
        logger.flush();

        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 1, "exactly one entry written, got: {content}");
        assert!(lines[0].contains("\"username\":\"test\""));
        assert!(lines[0].contains("query_hash"));
    }

    #[test]
    fn test_flush_blocks_until_written() {
        let (_dir, path) = fresh_audit_path("sqe-audit-flush-test.jsonl");
        let path_str = path.to_str().unwrap();

        let logger = AuditLogger::new(path_str).unwrap();
        for _ in 0..50 {
            logger.log(&test_entry());
        }
        logger.flush();

        let content = std::fs::read_to_string(&path).unwrap();
        let lines = content.lines().count();
        assert_eq!(lines, 50, "all entries visible after flush");
    }

    #[test]
    fn test_audit_log_does_not_contain_create_secret_literal() {
        let (_dir, path) = fresh_audit_path("sqe-audit-secret-test.jsonl");
        let path_str = path.to_str().unwrap();

        let logger = AuditLogger::new(path_str).unwrap();
        let mut entry = test_entry();
        entry.statement_type = "create_secret".to_string();
        entry.query_text = Some(
            "CREATE SECRET prod_token (TYPE bearer, TOKEN 'eyJSECRETJWTPAYLOAD')".to_string(),
        );
        logger.log(&entry);
        logger.flush();

        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(
            lines.len(),
            1,
            "exactly one entry must exist so the negative assertion below is meaningful, got: {content}"
        );
        assert!(
            !content.contains("eyJSECRETJWTPAYLOAD"),
            "secret literal leaked to audit log: {content}"
        );
        assert!(content.contains("[REDACTED]"));
    }

    #[test]
    fn test_log_redacts_pii_in_written_file() {
        let (_dir, path) = fresh_audit_path("sqe-audit-pii-test.jsonl");
        let path_str = path.to_str().unwrap();

        let logger = AuditLogger::new(path_str).unwrap();
        let mut entry = test_entry();
        entry.query_text = Some(
            "SELECT * FROM users WHERE email = 'carol@example.com'".to_string(),
        );
        logger.log(&entry);
        logger.flush();

        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(
            lines.len(),
            1,
            "exactly one entry must exist so the negative assertion below is meaningful, got: {content}"
        );
        assert!(!content.contains("carol@example.com"), "PII must not appear in audit log");
        assert!(content.contains("[EMAIL]"));
    }

    /// Regression test for two defects fixed in this commit:
    ///
    /// Defect 1 (ordering): interleaved log()/log_event() must preserve arrival order in
    /// the file (seq strictly increasing, event at position 2 not position 0).
    ///
    /// Defect 2 (torn lines): every line in the native file must parse as valid JSON
    /// (single-writer guarantee; a torn line would produce a partial JSON object).
    ///
    /// Pattern: log(), log(), log_event(), log() -> flush -> read file.
    /// Expected file order: legacy(seq=0), legacy(seq=1), event(seq=2), legacy(seq=3).
    #[test]
    fn interleave_ordering_and_no_torn_lines() {
        let (_dir, path) = fresh_audit_path("sqe-audit-interleave.jsonl");
        let path_str = path.to_str().unwrap();

        let logger = AuditLogger::with_config(path_str, crate::audit::AuditFormat::Native).unwrap();

        // Send all four messages before calling flush so they queue up together
        // and exercise the interleaved-batch code path in the worker.
        logger.log(&test_entry());
        logger.log(&test_entry());
        logger.log_event(crate::audit::sample_query_event());
        logger.log(&test_entry());
        logger.flush();

        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 4, "expected 4 lines, got:\n{content}");

        // Defect 2: every line must be valid JSON (no torn lines).
        let parsed: Vec<serde_json::Value> = lines
            .iter()
            .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("line {l:?} is not valid JSON: {e}")))
            .collect();

        // Extract seq from each line's integrity block.
        let seqs: Vec<u64> = parsed
            .iter()
            .map(|v| {
                v.get("integrity")
                    .and_then(|i| i.get("seq"))
                    .and_then(|s| s.as_u64())
                    .unwrap_or_else(|| panic!("missing integrity.seq in {v}"))
            })
            .collect();

        // Defect 1: seqs must be strictly increasing 0,1,2,3 in file order
        // (the event must not be stamped before the legacy entries that preceded it).
        assert_eq!(seqs, vec![0, 1, 2, 3], "seq must be 0,1,2,3 in file order; got {seqs:?}");

        // Position check: line[2] is the canonical event (has "kind":"query"),
        // lines 0,1,3 are legacy flat entries (have "username" but no top-level "kind").
        let event_line = &parsed[2];
        assert_eq!(
            event_line.get("kind").and_then(|k| k.as_str()),
            Some("query"),
            "line 2 must be the log_event (kind:query), got: {event_line}"
        );
        for i in [0usize, 1, 3] {
            let line = &parsed[i];
            assert!(
                line.get("username").is_some(),
                "line {i} must be a legacy flat entry (has 'username'), got: {line}"
            );
            assert!(
                line.get("kind").is_none(),
                "line {i} must not have top-level 'kind' (legacy flat format), got: {line}"
            );
        }
    }

    #[test]
    fn both_format_writes_native_and_ocsf_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");
        let logger = AuditLogger::with_config(
            path.to_str().unwrap(),
            crate::audit::AuditFormat::Both,
        )
        .unwrap();
        logger.log_event(crate::audit::sample_query_event());
        logger.flush();
        let native = std::fs::read_to_string(&path).unwrap();
        let ocsf = std::fs::read_to_string(dir.path().join("audit.ocsf.jsonl")).unwrap();
        assert!(native.contains("\"kind\":\"query\""));
        assert!(ocsf.contains("\"class_uid\":6005"));
        // Every native record carries an integrity hash.
        assert!(native.contains("\"hash\":"));
    }

    #[test]
    fn gdpr_tagged_column_is_masked_in_written_log() {
        use std::collections::HashMap;
        struct Stub;
        impl crate::audit::TagLookup for Stub {
            fn column_tags(&self, _c: Option<&str>, _n: &[String], _t: &str) -> Option<HashMap<String, Vec<String>>> {
                let mut m = HashMap::new();
                m.insert("email".to_string(), vec!["gdpr".to_string()]);
                Some(m)
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");
        let logger = AuditLogger::with_config(path.to_str().unwrap(), crate::audit::AuditFormat::Native)
            .unwrap()
            .with_gdpr(vec!["gdpr".into()], crate::audit::GdprIdentifierMode::Tokenize, "salt".into(), std::sync::Arc::new(Stub));
        let mut ev = crate::audit::sample_query_event();
        ev.resources = vec![crate::audit::Resource { catalog: Some("polaris".into()), namespace: vec!["hr".into()], name: "users".into(), object_type: crate::audit::ObjectType::Table }];
        ev.query = Some(crate::audit::QueryInfo { text: Some("SELECT id FROM users WHERE email = 'alice@x.io'".into()), query_hash: "h".into(), statement_type: "query".into() });
        logger.log_event(ev);
        logger.flush();
        let content = std::fs::read_to_string(&path).unwrap();
        // Parse the written line as JSON so we can check the right fields.
        let line = content.lines().next().expect("at least one line written");
        let v: serde_json::Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("line is not valid JSON: {e}\n{line}"));
        // The query text must not contain the GDPR column identifier or its value.
        let query_text = v["query"]["text"].as_str().unwrap_or("");
        assert!(
            !query_text.contains("alice@x.io"),
            "GDPR value leaked in query.text: {query_text}"
        );
        assert!(
            !query_text.contains("email"),
            "GDPR column identifier leaked in query.text: {query_text}"
        );
        // Actor identity must be preserved: GDPR masking applies only to queried
        // table columns, not to the authenticated user's own identity.
        let actor_email = v["actor"]["email"].as_str().unwrap_or("");
        assert_eq!(
            actor_email, "alice@corp.example",
            "actor.email must survive GDPR masking (OCSF accountability requirement)"
        );
    }

    /// Regression guard for the PII redaction regression fixed in this commit.
    ///
    /// Without GDPR config (`with_gdpr` not called), `log_event` must STILL
    /// apply `redact_pii` to `query.text` before writing. Prior to the fix,
    /// the redact_pii call was inside `apply_gdpr_masking` which was only
    /// invoked when GDPR was configured, so deployments without GDPR leaked
    /// email/SSN/phone/card literals from WHERE clauses to the audit log.
    #[test]
    fn log_event_redacts_pii_without_gdpr_config() {
        let (_dir, path) = fresh_audit_path("sqe-audit-log-event-pii.jsonl");
        let path_str = path.to_str().unwrap();

        // No .with_gdpr() call: GDPR masking is NOT configured.
        let logger = AuditLogger::with_config(path_str, crate::audit::AuditFormat::Native).unwrap();

        let mut ev = crate::audit::sample_query_event();
        ev.query = Some(crate::audit::QueryInfo {
            text: Some("SELECT id FROM users WHERE email = 'leak@example.com'".into()),
            query_hash: "h".into(),
            statement_type: "query".into(),
        });
        logger.log_event(ev);
        logger.flush();

        let content = std::fs::read_to_string(&path).unwrap();
        let line = content.lines().next().expect("at least one line written");
        let v: serde_json::Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("line is not valid JSON: {e}\n{line}"));

        // PII must not appear in the written audit line.
        assert!(
            !content.contains("leak@example.com"),
            "PII leaked in audit log (no-GDPR path): {content}"
        );
        // redact_pii replaces emails with [EMAIL].
        let query_text = v["query"]["text"].as_str().unwrap_or("");
        assert!(
            query_text.contains("[EMAIL]"),
            "redact_pii must replace email with [EMAIL], got: {query_text}"
        );
    }

    #[test]
    fn unknown_tag_state_falls_back_to_literal_stripping() {
        use std::collections::HashMap;
        struct Unknown;
        impl crate::audit::TagLookup for Unknown {
            fn column_tags(&self, _c: Option<&str>, _n: &[String], _t: &str) -> Option<HashMap<String, Vec<String>>> { None }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");
        let logger = AuditLogger::with_config(path.to_str().unwrap(), crate::audit::AuditFormat::Native)
            .unwrap()
            .with_gdpr(vec!["gdpr".into()], crate::audit::GdprIdentifierMode::Tokenize, "salt".into(), std::sync::Arc::new(Unknown));
        let mut ev = crate::audit::sample_query_event();
        ev.resources = vec![crate::audit::Resource { catalog: None, namespace: vec![], name: "users".into(), object_type: crate::audit::ObjectType::Table }];
        ev.query = Some(crate::audit::QueryInfo { text: Some("SELECT id FROM users WHERE ssn = '123-45-6789'".into()), query_hash: "h".into(), statement_type: "query".into() });
        logger.log_event(ev);
        logger.flush();
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(!content.contains("123-45-6789"), "literal survived unknown-tag fallback: {content}");
    }
}
