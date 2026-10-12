//! Disk-buffered fallback wrapping an HTTP sink.
//!
//! On HTTP failure: append the event JSON to `<path>/spool.jsonl`. A background
//! replay task wakes every `replay_interval` and tries to drain rotated spool
//! segments.
//!
//! Cap policy when an event would take the spool past `max_bytes`: the oldest
//! segments are deleted until it fits, and every event in them is counted in
//! `dropped_count` (and `sqe_lineage_spool_dropped_events_total` when a counter
//! is attached). A long outage therefore keeps the most recent lineage, which
//! is what a consumer catching up needs, and the disk use stays bounded. The
//! cap warning is logged once per crossing, not per event (#816).
//!
//! See spec §6.5.

use crate::event::RunEvent;
use crate::sink::{Sink, SinkError};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;

#[derive(Clone, Debug)]
pub struct SpoolConfig {
    pub path: PathBuf,
    pub max_bytes: u64,
    pub replay_interval: Duration,
}

pub struct SpoolSink {
    inner: Arc<dyn Sink>,
    cfg: SpoolConfig,
    dropped: AtomicU64,
    dropped_metric: Option<prometheus::IntCounter>,
    /// Set while the spool is at its cap, so the warning is logged once per
    /// crossing.
    at_cap: AtomicBool,
    /// Held while the segment set changes (rotate, delete), so the cap never
    /// deletes a segment the replay task is renaming.
    segments: Arc<tokio::sync::Mutex<()>>,
    _replay_task: tokio::task::JoinHandle<()>,
}

impl SpoolSink {
    pub fn wrap(inner: Arc<dyn Sink>, cfg: SpoolConfig) -> Arc<Self> {
        Self::wrap_with_counter(inner, cfg, None)
    }

    /// [`Self::wrap`], also counting dropped events in `dropped_metric`.
    pub fn wrap_with_counter(
        inner: Arc<dyn Sink>,
        cfg: SpoolConfig,
        dropped_metric: Option<prometheus::IntCounter>,
    ) -> Arc<Self> {
        std::fs::create_dir_all(&cfg.path).ok();

        let segments = Arc::new(tokio::sync::Mutex::new(()));
        let inner_for_task = inner.clone();
        let path_for_task = cfg.path.clone();
        let segments_for_task = segments.clone();
        let interval = cfg.replay_interval;

        let task = tokio::spawn(async move {
            replay_loop(path_for_task, inner_for_task, segments_for_task, interval).await;
        });

        Arc::new(Self {
            inner,
            cfg,
            dropped: AtomicU64::new(0),
            dropped_metric,
            at_cap: AtomicBool::new(false),
            segments,
            _replay_task: task,
        })
    }

    pub fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    fn count_dropped(&self, events: u64) {
        if events == 0 {
            return;
        }
        self.dropped.fetch_add(events, Ordering::Relaxed);
        if let Some(metric) = &self.dropped_metric {
            metric.inc_by(events);
        }
    }

    async fn append_to_spool(&self, ev: &RunEvent) -> Result<(), SinkError> {
        let mut line = serde_json::to_string(ev)?;
        line.push('\n');
        let needed = line.len() as u64;

        let _segments = self.segments.lock().await;
        let mut total = total_spool_bytes(&self.cfg.path).unwrap_or(0);
        if total + needed > self.cfg.max_bytes {
            if !self.at_cap.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    spool = %self.cfg.path.display(),
                    max_bytes = self.cfg.max_bytes,
                    "OpenLineage spool full; dropping the oldest spooled events"
                );
            }
            if needed > self.cfg.max_bytes {
                // Could never fit, even in an empty spool.
                self.count_dropped(1);
                return Ok(());
            }
            rotate_live(&self.cfg.path)?;
            for segment in rotated_segments(&self.cfg.path)? {
                if total + needed <= self.cfg.max_bytes {
                    break;
                }
                let content = std::fs::read_to_string(&segment).unwrap_or_default();
                let len = std::fs::metadata(&segment).map(|m| m.len()).unwrap_or(0);
                match std::fs::remove_file(&segment) {
                    Ok(()) => {
                        total = total.saturating_sub(len);
                        self.count_dropped(content.lines().filter(|l| !l.is_empty()).count() as u64);
                    }
                    // The replay task drained it meanwhile.
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        total = total.saturating_sub(len);
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        } else {
            self.at_cap.store(false, Ordering::Relaxed);
        }

        let mut f = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.cfg.path.join("spool.jsonl"))
            .await?;
        f.write_all(line.as_bytes()).await?;
        // A tokio `File` finishes writes in the background; without a flush
        // the line may not be on disk yet when the lock is released (and a
        // dropped half-written line glues two events together).
        f.flush().await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Sink for SpoolSink {
    async fn send(&self, ev: &RunEvent) -> Result<(), SinkError> {
        match self.inner.send(ev).await {
            Ok(()) => Ok(()),
            Err(_) => self.append_to_spool(ev).await,
        }
    }

    fn name(&self) -> &'static str {
        "spool"
    }
}

fn total_spool_bytes(dir: &Path) -> std::io::Result<u64> {
    let mut total = 0u64;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("spool.jsonl") {
            total += entry.metadata()?.len();
        }
    }
    Ok(total)
}

/// Move a non-empty `spool.jsonl` to a new rotated segment.
fn rotate_live(spool_dir: &Path) -> std::io::Result<()> {
    static SEGMENT_SEQ: AtomicU64 = AtomicU64::new(0);
    let live = spool_dir.join("spool.jsonl");
    if live.exists() && std::fs::metadata(&live)?.len() > 0 {
        // The sequence keeps two rotations in one millisecond from renaming
        // onto the same segment, and keeps names sorting oldest first.
        let ts = chrono::Utc::now().format("%Y%m%dT%H%M%S%3f").to_string();
        let seq = SEGMENT_SEQ.fetch_add(1, Ordering::Relaxed);
        std::fs::rename(&live, spool_dir.join(format!("spool.jsonl.{ts}-{seq:010}")))?;
    }
    Ok(())
}

/// Rotated segments, oldest first.
fn rotated_segments(spool_dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(spool_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("spool.jsonl."))
        .map(|e| e.path())
        .collect();
    files.sort();
    Ok(files)
}

async fn replay_loop(
    spool_dir: PathBuf,
    inner: Arc<dyn Sink>,
    segments: Arc<tokio::sync::Mutex<()>>,
    interval: Duration,
) {
    loop {
        tokio::time::sleep(interval).await;
        if let Err(e) = drain_once(&spool_dir, inner.clone(), &segments).await {
            tracing::warn!(error = %e, "spool drain attempt failed");
        }
    }
}

async fn drain_once(
    spool_dir: &Path,
    inner: Arc<dyn Sink>,
    segments: &tokio::sync::Mutex<()>,
) -> std::io::Result<()> {
    // Rotate spool.jsonl -> spool.jsonl.<timestamp>-<seq> if non-empty.
    let rotated_files = {
        let _segments = segments.lock().await;
        rotate_live(spool_dir)?;
        rotated_segments(spool_dir)?
    };

    // Drain each rotated file, oldest first. The lock is not held across
    // sends, so a full spool can still drop its oldest segment meanwhile.
    for file in rotated_files {
        let content = match std::fs::read_to_string(&file) {
            Ok(content) => content,
            // Dropped by the cap since the listing.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        let mut all_ok = true;
        for line in content.lines() {
            if line.is_empty() {
                continue;
            }
            let ev: RunEvent = match serde_json::from_str(line) {
                Ok(e) => e,
                Err(_) => continue, // skip malformed line
            };
            if inner.send(&ev).await.is_err() {
                all_ok = false;
                break;
            }
        }
        if all_ok {
            match std::fs::remove_file(&file) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        if !all_ok {
            // The sink is still down: leave this file and every newer one for
            // the next tick, so events are not replayed out of order.
            break;
        }
    }

    Ok(())
}
