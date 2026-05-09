//! Disk-buffered fallback wrapping an HTTP sink.
//!
//! On HTTP failure: append the event JSON to `<path>/spool.jsonl`. A background
//! replay task wakes every `replay_interval` and tries to drain rotated spool
//! segments. Cap policy when `max_bytes` exceeded: drop newest with metric.
//!
//! See spec §6.5.

use crate::event::RunEvent;
use crate::sink::{Sink, SinkError};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
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
    _replay_task: tokio::task::JoinHandle<()>,
}

impl SpoolSink {
    pub fn wrap(inner: Arc<dyn Sink>, cfg: SpoolConfig) -> Arc<Self> {
        std::fs::create_dir_all(&cfg.path).ok();

        let inner_for_task = inner.clone();
        let path_for_task = cfg.path.clone();
        let interval = cfg.replay_interval;

        let task = tokio::spawn(async move {
            replay_loop(path_for_task, inner_for_task, interval).await;
        });

        Arc::new(Self {
            inner,
            cfg,
            dropped: AtomicU64::new(0),
            _replay_task: task,
        })
    }

    pub fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    async fn append_to_spool(&self, ev: &RunEvent) -> Result<(), SinkError> {
        let live = self.cfg.path.join("spool.jsonl");
        let total = total_spool_bytes(&self.cfg.path).unwrap_or(0);
        if total >= self.cfg.max_bytes {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            tracing::warn!("spool cap reached; dropping event");
            return Ok(());
        }
        let line = serde_json::to_string(ev)?;
        let mut f = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&live)
            .await?;
        f.write_all(line.as_bytes()).await?;
        f.write_all(b"\n").await?;
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

async fn replay_loop(spool_dir: PathBuf, inner: Arc<dyn Sink>, interval: Duration) {
    loop {
        tokio::time::sleep(interval).await;
        if let Err(e) = drain_once(&spool_dir, inner.clone()).await {
            tracing::warn!(error = %e, "spool drain attempt failed");
        }
    }
}

async fn drain_once(spool_dir: &Path, inner: Arc<dyn Sink>) -> std::io::Result<()> {
    // Rotate spool.jsonl -> spool.jsonl.<timestamp> if non-empty.
    let live = spool_dir.join("spool.jsonl");
    if live.exists() {
        let len = std::fs::metadata(&live)?.len();
        if len > 0 {
            let ts = chrono::Utc::now().format("%Y%m%dT%H%M%S%3f").to_string();
            let rotated = spool_dir.join(format!("spool.jsonl.{ts}"));
            std::fs::rename(&live, &rotated)?;
        }
    }

    // Drain each rotated file, oldest first.
    let mut rotated_files: Vec<PathBuf> = std::fs::read_dir(spool_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name();
            let name = name.to_string_lossy();
            name.starts_with("spool.jsonl.")
        })
        .map(|e| e.path())
        .collect();
    rotated_files.sort();

    for file in rotated_files {
        let content = std::fs::read_to_string(&file)?;
        let mut all_ok = true;
        for line in content.lines() {
            if line.is_empty() { continue; }
            let ev: RunEvent = match serde_json::from_str(line) {
                Ok(e) => e,
                Err(_) => continue,  // skip malformed line
            };
            if inner.send(&ev).await.is_err() {
                all_ok = false;
                break;
            }
        }
        if all_ok {
            std::fs::remove_file(&file)?;
        }
        // If !all_ok we leave the file for next tick.
    }

    Ok(())
}
