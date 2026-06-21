# Memory Safety and OOM Prevention Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Guarantee no query can OOM a worker or coordinator at 100 to 300 concurrent queries, by making every memory consumer share a bounded per-node budget or spill to disk, with a minimal admission cap, failing one query rather than the cluster.

**Architecture:** A `NodeMemoryGovernor` owns one shared `FairSpillPool` per node sized from detected memory. An `AdmissionGate` refuses work that cannot be seated with its per-query floor. Every pipeline breaker and the shuffle receiver spill to local NVMe under pressure. A three-class typed failure taxonomy fails one query and reclaims its resources. Metrics, structured logs, and the web UI render one telemetry source. Every mechanism is pressure-triggered, so small deployments are unaffected.

**Tech Stack:** Rust, Apache DataFusion (`FairSpillPool`, `DiskManager`, `MemoryPool`), Arrow IPC, Arrow Flight, `sqe-metrics` (Prometheus), serde/TOML config, moka (cache).

## Global Constraints

- Branch: work continues on `feat/memory-safety-oom-spec`; never push to main; one PR for this subsystem (verbatim from CLAUDE.md git workflow).
- No new mandatory config. Every new `QueryConfig` field is `#[serde(default)]`; existing config files must remain valid.
- Every mechanism is pressure-triggered: zero behavior change and negligible overhead when RAM exceeds working set and concurrency is below cap.
- Blast radius is one query: a query-level error must never be classified as node failure. Only the shuffle-data-loss class may count against a node.
- Failure taxonomy variants live over SQE's own `SqeError`, not Ballista's enum.
- Borrow patterns, not the framework: reuse SQE's `WeightedScheduler`, codec, `FairSpillPool`, `FragmentInfo`, `QueryRecord`, `WorkerState`.
- Logging is per-query-event, never per-batch.
- Clippy strict: `cargo clippy --all-targets --all-features -- -D warnings` must pass.
- Docs voice: no emdash, endash, or Unicode arrows in any prose or doc comments.
- TDD: failing test first, minimal implementation, passing test, commit. Frequent commits.

---

## File Structure

New files:
- `crates/sqe-coordinator/src/memory_governor.rs` - `NodeMemoryGovernor`, reservation handles, floor accounting, `try_admit`.
- `crates/sqe-coordinator/src/admission.rs` - `AdmissionGate` (slots + floor, pass-through fast path, bounded queue).
- `crates/sqe-coordinator/src/stage_plan_cache.rs` - `EncodedStagePlanCache`.
- `crates/sqe-core/src/memory_detect.rs` - cgroup/container memory-limit detection for auto-tuned pool sizing.

Modified files:
- `crates/sqe-core/src/config.rs` - new `QueryConfig` fields + auto-tune default helpers.
- `crates/sqe-core/src/errors.rs` - three-class memory failure taxonomy on `SqeError`.
- `crates/sqe-coordinator/src/memory.rs` - reuse existing pressure model; expose pool to the governor.
- `crates/sqe-coordinator/src/distributed_scan.rs` - abort path, reservation reclaim, stage-plan cache wiring.
- `crates/sqe-worker/src/shuffle.rs` - disk-spill tier on `ShuffleReceiver`.
- `crates/sqe-planner/src/distributed_sort.rs` (and the sort-on-write path) - fix `can_spill=false`.
- `crates/sqe-metrics/src/lib.rs` - new gauges and counters.
- `crates/sqe-coordinator/src/web_ui.rs` - memory/spill/admission panels.

---

## Phase 1: Configuration foundation

### Task 1: Extend QueryConfig with auto-tuned memory-safety fields

**Files:**
- Modify: `crates/sqe-core/src/config.rs`
- Test: `crates/sqe-core/src/config.rs` (inline `#[cfg(test)]` module)

**Interfaces:**
- Produces: new public fields on `QueryConfig`: `memory_headroom_fraction: f64`, `per_query_memory_floor: u64`, `spill_enabled: bool`, `spill_dir: Option<String>`, `stage_plan_cache_entries: usize`. Default helpers: `default_memory_headroom_fraction() -> f64` (0.2), `default_per_query_memory_floor() -> u64` (0 = auto), `default_spill_enabled() -> bool` (true), `default_stage_plan_cache_entries() -> usize` (256).

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn query_config_memory_safety_defaults() {
    let cfg: QueryConfig = toml::from_str("").expect("empty config is valid");
    assert!((cfg.memory_headroom_fraction - 0.2).abs() < f64::EPSILON);
    assert_eq!(cfg.per_query_memory_floor, 0); // 0 = auto-derive
    assert!(cfg.spill_enabled);
    assert_eq!(cfg.spill_dir, None);
    assert_eq!(cfg.stage_plan_cache_entries, 256);
}

#[test]
fn query_config_existing_file_still_valid() {
    // A config with only pre-existing fields must still parse.
    let cfg: QueryConfig =
        toml::from_str("max-concurrent-queries = 32").expect("legacy config valid");
    assert_eq!(cfg.max_concurrent_queries, 32);
    assert!(cfg.spill_enabled); // new field defaulted
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p sqe-core query_config_memory_safety_defaults`
Expected: FAIL, fields `memory_headroom_fraction` etc. do not exist.

- [ ] **Step 3: Add the fields and default helpers**

In `QueryConfig` (kebab-case is already set via `#[serde(rename_all = "kebab-case")]`):

```rust
    /// Fraction of node memory held back from the shared pool. The pool is
    /// sized as detected_memory * (1.0 - memory_headroom_fraction).
    #[serde(default = "default_memory_headroom_fraction")]
    pub memory_headroom_fraction: f64,

    /// Guaranteed minimum reservation per query, in bytes. 0 means auto:
    /// derive from pool size and the concurrency cap with a small floor.
    #[serde(default = "default_per_query_memory_floor")]
    pub per_query_memory_floor: u64,

    /// When false, operators never spill; queries that exceed memory fail.
    #[serde(default = "default_spill_enabled")]
    pub spill_enabled: bool,

    /// Local scratch directory for spill and disk-backed shuffle. None uses
    /// the system temp dir.
    #[serde(default)]
    pub spill_dir: Option<String>,

    /// Bounded entry count for the encoded-stage-plan cache. 0 disables it.
    #[serde(default = "default_stage_plan_cache_entries")]
    pub stage_plan_cache_entries: usize,
```

Default helpers near the other `default_*` fns:

```rust
fn default_memory_headroom_fraction() -> f64 { 0.2 }
fn default_per_query_memory_floor() -> u64 { 0 }
fn default_spill_enabled() -> bool { true }
fn default_stage_plan_cache_entries() -> usize { 256 }
```

Add the fields to the `Default` impl for `QueryConfig` (the block around line 288) using the same helpers.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p sqe-core query_config_memory_safety_defaults query_config_existing_file_still_valid`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-core/src/config.rs
git commit -m "feat(config): add auto-tuned memory-safety fields to QueryConfig"
```

---

### Task 2: Detect node memory limit (cgroup/container aware)

**Files:**
- Create: `crates/sqe-core/src/memory_detect.rs`
- Modify: `crates/sqe-core/src/lib.rs` (add `pub mod memory_detect;`)
- Test: `crates/sqe-core/src/memory_detect.rs` (inline tests)

**Interfaces:**
- Produces: `pub fn detect_memory_limit_bytes() -> u64` (cgroup v2 `memory.max`, then cgroup v1 `memory.limit_in_bytes`, then total system RAM as fallback); `pub fn pool_size_from(detected: u64, headroom_fraction: f64) -> u64`.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn pool_size_applies_headroom() {
    assert_eq!(pool_size_from(10_000, 0.2), 8_000);
    assert_eq!(pool_size_from(10_000, 0.0), 10_000);
}

#[test]
fn pool_size_clamps_insane_headroom() {
    // headroom >= 1.0 would zero the pool; clamp to a usable minimum.
    assert!(pool_size_from(10_000, 1.5) >= 1);
}

#[test]
fn detect_returns_nonzero() {
    assert!(detect_memory_limit_bytes() > 0);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p sqe-core memory_detect`
Expected: FAIL, module does not exist.

- [ ] **Step 3: Implement detection and sizing**

```rust
//! Node memory-limit detection for auto-tuned pool sizing.
//!
//! Prefers the container/cgroup limit so a worker pod sizes itself, then
//! falls back to total system RAM.

use std::fs;

/// Detect the memory ceiling for this node in bytes.
pub fn detect_memory_limit_bytes() -> u64 {
    if let Some(v) = read_cgroup_v2() {
        return v;
    }
    if let Some(v) = read_cgroup_v1() {
        return v;
    }
    system_ram_bytes()
}

fn read_cgroup_v2() -> Option<u64> {
    let s = fs::read_to_string("/sys/fs/cgroup/memory.max").ok()?;
    let s = s.trim();
    if s == "max" {
        return None;
    }
    s.parse::<u64>().ok().filter(|v| *v > 0)
}

fn read_cgroup_v1() -> Option<u64> {
    let s = fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes").ok()?;
    // cgroup v1 reports a huge sentinel when unlimited; ignore absurd values.
    s.trim().parse::<u64>().ok().filter(|v| *v > 0 && *v < (1 << 62))
}

fn system_ram_bytes() -> u64 {
    // Conservative fallback when no cgroup info and no sysinfo dep is wired.
    // Replace with the project's existing system-info source if present.
    const DEFAULT_FALLBACK: u64 = 4 * 1024 * 1024 * 1024;
    DEFAULT_FALLBACK
}

/// Pool size after holding back the configured headroom fraction.
pub fn pool_size_from(detected: u64, headroom_fraction: f64) -> u64 {
    let frac = headroom_fraction.clamp(0.0, 0.95);
    let usable = (detected as f64 * (1.0 - frac)) as u64;
    usable.max(1)
}
```

Note for implementer: if the codebase already depends on `sysinfo` or similar, replace `system_ram_bytes` with that source; do not add a new dependency just for the fallback.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p sqe-core memory_detect`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-core/src/memory_detect.rs crates/sqe-core/src/lib.rs
git commit -m "feat(core): detect cgroup/container memory limit for pool sizing"
```

---

## Phase 2: Failure taxonomy

### Task 3: Three-class memory failure taxonomy on SqeError

**Files:**
- Modify: `crates/sqe-core/src/errors.rs`
- Test: `crates/sqe-core/src/errors.rs` (inline tests)

**Interfaces:**
- Produces: `pub enum MemoryFailureClass { TransientIo, QueryLevel, ShuffleDataLoss }`; methods `retryable(&self) -> bool` and `counts_to_failure(&self) -> bool`; new `SqeError` variants `MemoryExhausted { node: String, operator: String, requested: u64, available: u64, spilled: u64 }` and `Rejected { reason: String, concurrency: usize, queue_depth: usize }`, each exposing `fn failure_class(&self) -> MemoryFailureClass`.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn failure_class_flags() {
    assert!(MemoryFailureClass::TransientIo.retryable());
    assert!(!MemoryFailureClass::TransientIo.counts_to_failure());

    assert!(!MemoryFailureClass::QueryLevel.retryable());
    assert!(!MemoryFailureClass::QueryLevel.counts_to_failure());

    assert!(MemoryFailureClass::ShuffleDataLoss.retryable());
    assert!(MemoryFailureClass::ShuffleDataLoss.counts_to_failure());
}

#[test]
fn memory_exhausted_is_query_level() {
    let e = SqeError::MemoryExhausted {
        node: "w1".into(), operator: "HashJoinBuild".into(),
        requested: 100, available: 10, spilled: 90,
    };
    assert_eq!(e.failure_class(), MemoryFailureClass::QueryLevel);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p sqe-core failure_class_flags memory_exhausted_is_query_level`
Expected: FAIL, types not defined.

- [ ] **Step 3: Implement the taxonomy**

```rust
/// The three failure classes that govern retry and node-eviction policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryFailureClass {
    /// Spill or shuffle-fetch I/O hiccup. Retry silently.
    TransientIo,
    /// Memory-exhausted or admission-rejected. Fail one query, never the node.
    QueryLevel,
    /// A node holding spilled/materialized shuffle partitions is gone.
    /// Re-run the producing stage; may count against the node.
    ShuffleDataLoss,
}

impl MemoryFailureClass {
    pub fn retryable(&self) -> bool {
        matches!(self, Self::TransientIo | Self::ShuffleDataLoss)
    }
    pub fn counts_to_failure(&self) -> bool {
        matches!(self, Self::ShuffleDataLoss)
    }
}
```

Add to the `SqeError` enum (match the crate's existing derive/`thiserror` style):

```rust
    #[error("query exceeded memory on {node} in {operator}: requested {requested} bytes, {available} available after spilling {spilled}")]
    MemoryExhausted {
        node: String,
        operator: String,
        requested: u64,
        available: u64,
        spilled: u64,
    },

    #[error("query rejected at admission: {reason} (concurrency {concurrency}, queue depth {queue_depth})")]
    Rejected {
        reason: String,
        concurrency: usize,
        queue_depth: usize,
    },
```

Add the classifier:

```rust
impl SqeError {
    /// Failure class for memory-safety variants. Non-memory variants return
    /// QueryLevel by default (fail the one operation, never the node).
    pub fn failure_class(&self) -> MemoryFailureClass {
        match self {
            SqeError::MemoryExhausted { .. } | SqeError::Rejected { .. } => {
                MemoryFailureClass::QueryLevel
            }
            _ => MemoryFailureClass::QueryLevel,
        }
    }
}
```

Note for implementer: a future `ShuffleDataLoss`/`SpillIo` variant (subsystem D) maps to the other two classes; the enum is shaped so adding them does not change existing call sites.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p sqe-core failure_class_flags memory_exhausted_is_query_level`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-core/src/errors.rs
git commit -m "feat(core): three-class memory failure taxonomy on SqeError"
```

---

## Phase 3: NodeMemoryGovernor

### Task 4: NodeMemoryGovernor with shared pool, floor, and try_admit

**Files:**
- Create: `crates/sqe-coordinator/src/memory_governor.rs`
- Modify: `crates/sqe-coordinator/src/lib.rs` (add `pub mod memory_governor;`)
- Test: `crates/sqe-coordinator/src/memory_governor.rs` (inline tests)

**Interfaces:**
- Consumes: `sqe_core::memory_detect::{detect_memory_limit_bytes, pool_size_from}`, `sqe_core::config::QueryConfig`, DataFusion `FairSpillPool`/`MemoryPool`.
- Produces:
  - `pub struct NodeMemoryGovernor { pool: Arc<dyn MemoryPool>, floor: u64, max_concurrent: usize, active: AtomicUsize }`
  - `pub fn new(cfg: &QueryConfig) -> Arc<Self>`
  - `pub fn pool(&self) -> Arc<dyn MemoryPool>`
  - `pub fn try_admit(&self) -> Result<QueryReservation, SqeError>` (returns `Rejected` when at cap or floor unavailable)
  - `pub struct QueryReservation` (RAII; decrements `active` on drop)
  - `pub fn available_bytes(&self) -> u64`

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use sqe_core::config::QueryConfig;

    fn cfg(max_concurrent: usize, floor: u64) -> QueryConfig {
        let mut c = QueryConfig::default();
        c.max_concurrent_queries = max_concurrent;
        c.per_query_memory_floor = floor;
        c
    }

    #[test]
    fn admits_until_cap_then_rejects() {
        let g = NodeMemoryGovernor::new(&cfg(2, 1));
        let r1 = g.try_admit().expect("first admit");
        let _r2 = g.try_admit().expect("second admit");
        let third = g.try_admit();
        assert!(matches!(third, Err(SqeError::Rejected { .. })));
        drop(r1);
        assert!(g.try_admit().is_ok(), "slot freed after drop");
    }

    #[test]
    fn rejects_when_floor_exceeds_available() {
        // Floor larger than the whole pool can never be seated.
        let mut c = cfg(100, u64::MAX / 2);
        c.memory_headroom_fraction = 0.0;
        let g = NodeMemoryGovernor::new(&c);
        assert!(matches!(g.try_admit(), Err(SqeError::Rejected { .. })));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p sqe-coordinator memory_governor`
Expected: FAIL, module does not exist.

- [ ] **Step 3: Implement the governor**

```rust
//! Per-node shared memory budget with a per-query floor and admission cap.
//!
//! One FairSpillPool per node, shared by all concurrent queries. Operators
//! reserve from it and spill when it is full. The floor guarantees each
//! admitted query a minimum; the cap makes the no-OOM guarantee real.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use datafusion::execution::memory_pool::{FairSpillPool, MemoryPool};
use sqe_core::config::QueryConfig;
use sqe_core::errors::SqeError;
use sqe_core::memory_detect::{detect_memory_limit_bytes, pool_size_from};

pub struct NodeMemoryGovernor {
    pool: Arc<dyn MemoryPool>,
    pool_size: u64,
    floor: u64,
    max_concurrent: usize,
    active: AtomicUsize,
}

impl NodeMemoryGovernor {
    pub fn new(cfg: &QueryConfig) -> Arc<Self> {
        let detected = detect_memory_limit_bytes();
        let pool_size = pool_size_from(detected, cfg.memory_headroom_fraction);
        let pool: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(pool_size as usize));
        let max_concurrent = cfg.max_concurrent_queries.max(1);
        let floor = if cfg.per_query_memory_floor == 0 {
            // Auto: an even share, with a small absolute minimum.
            (pool_size / max_concurrent as u64).max(8 * 1024 * 1024)
        } else {
            cfg.per_query_memory_floor
        };
        Arc::new(Self {
            pool,
            pool_size,
            floor,
            max_concurrent,
            active: AtomicUsize::new(0),
        })
    }

    pub fn pool(&self) -> Arc<dyn MemoryPool> {
        Arc::clone(&self.pool)
    }

    pub fn available_bytes(&self) -> u64 {
        self.pool_size.saturating_sub(self.pool.reserved() as u64)
    }

    pub fn try_admit(self: &Arc<Self>) -> Result<QueryReservation, SqeError> {
        if self.floor > self.pool_size {
            return Err(SqeError::Rejected {
                reason: "per-query floor exceeds pool size".into(),
                concurrency: self.active.load(Ordering::Acquire),
                queue_depth: 0,
            });
        }
        let mut cur = self.active.load(Ordering::Acquire);
        loop {
            if cur >= self.max_concurrent {
                return Err(SqeError::Rejected {
                    reason: "max concurrent queries reached".into(),
                    concurrency: cur,
                    queue_depth: 0,
                });
            }
            match self.active.compare_exchange_weak(
                cur, cur + 1, Ordering::AcqRel, Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(QueryReservation { gov: Arc::clone(self) });
                }
                Err(observed) => cur = observed,
            }
        }
    }
}

/// RAII admission handle. Frees the concurrency slot on drop.
pub struct QueryReservation {
    gov: Arc<NodeMemoryGovernor>,
}

impl Drop for QueryReservation {
    fn drop(&mut self) {
        self.gov.active.fetch_sub(1, Ordering::AcqRel);
    }
}
```

Note for implementer: confirm `MemoryPool::reserved()` exists in the pinned DataFusion version; the codebase already calls `pool.memory_limit()` in `memory.rs`, so the trait surface is available there to mirror.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p sqe-coordinator memory_governor`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-coordinator/src/memory_governor.rs crates/sqe-coordinator/src/lib.rs
git commit -m "feat(coordinator): NodeMemoryGovernor with shared pool, floor, admission cap"
```

---

## Phase 4: AdmissionGate

### Task 5: AdmissionGate with fast pass-through and bounded queue

**Files:**
- Create: `crates/sqe-coordinator/src/admission.rs`
- Modify: `crates/sqe-coordinator/src/lib.rs` (add `pub mod admission;`)
- Test: `crates/sqe-coordinator/src/admission.rs` (inline tests)

**Interfaces:**
- Consumes: `NodeMemoryGovernor`, `QueryReservation`, `SqeError`.
- Produces: `pub struct AdmissionGate { gov: Arc<NodeMemoryGovernor>, queue_depth: AtomicUsize, max_queue: usize }`; `pub async fn admit(&self) -> Result<QueryReservation, SqeError>` (fast path: immediate `try_admit`; on `Rejected`, wait on a notify up to a bounded queue, else return `Rejected` with real `queue_depth`).

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn fast_path_admits_without_queue() {
    let gov = NodeMemoryGovernor::new(&QueryConfig::default());
    let gate = AdmissionGate::new(gov, 16);
    let r = gate.admit().await.expect("uncontended admit");
    drop(r);
}

#[tokio::test]
async fn rejects_when_queue_full() {
    let mut c = QueryConfig::default();
    c.max_concurrent_queries = 1;
    let gov = NodeMemoryGovernor::new(&c);
    let gate = AdmissionGate::new(gov, 0); // no queue slack
    let _held = gate.admit().await.expect("first admit");
    let second = gate.admit().await;
    assert!(matches!(second, Err(SqeError::Rejected { .. })));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p sqe-coordinator admission`
Expected: FAIL, module does not exist.

- [ ] **Step 3: Implement the gate**

```rust
//! Minimal admission control: floor-availability + max-concurrent only.
//! Not fair-share or priority logic (that is subsystem B).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;

use crate::memory_governor::{NodeMemoryGovernor, QueryReservation};
use sqe_core::errors::SqeError;

pub struct AdmissionGate {
    gov: Arc<NodeMemoryGovernor>,
    notify: Arc<Notify>,
    queue_depth: AtomicUsize,
    max_queue: usize,
}

impl AdmissionGate {
    pub fn new(gov: Arc<NodeMemoryGovernor>, max_queue: usize) -> Self {
        Self {
            gov,
            notify: Arc::new(Notify::new()),
            queue_depth: AtomicUsize::new(0),
            max_queue,
        }
    }

    pub async fn admit(&self) -> Result<QueryReservation, SqeError> {
        // Fast path: uncontended admit, no queueing.
        if let Ok(r) = self.gov.try_admit() {
            return Ok(self.wrap(r));
        }
        // Pressure path: bounded wait.
        let depth = self.queue_depth.fetch_add(1, Ordering::AcqRel) + 1;
        if depth > self.max_queue {
            self.queue_depth.fetch_sub(1, Ordering::AcqRel);
            return Err(SqeError::Rejected {
                reason: "admission queue full".into(),
                concurrency: self.gov.max_concurrent(),
                queue_depth: depth,
            });
        }
        let result = loop {
            // Wait for a freed slot, with a wakeup deadline as backstop.
            let _ = tokio::time::timeout(Duration::from_millis(250), self.notify.notified()).await;
            if let Ok(r) = self.gov.try_admit() {
                break Ok(self.wrap(r));
            }
            // Re-check queue budget each turn; reject if we have overstayed.
            if self.queue_depth.load(Ordering::Acquire) > self.max_queue {
                break Err(SqeError::Rejected {
                    reason: "admission queue full".into(),
                    concurrency: self.gov.max_concurrent(),
                    queue_depth: self.queue_depth.load(Ordering::Acquire),
                });
            }
        };
        self.queue_depth.fetch_sub(1, Ordering::AcqRel);
        result
    }

    fn wrap(&self, r: QueryReservation) -> QueryReservation {
        // Wake one waiter when this reservation is later dropped: the
        // governor drop frees a slot; nudging the notify avoids waiting the
        // full timeout. Implemented by notifying on each admit attempt.
        self.notify.notify_one();
        r
    }

    pub fn queue_depth(&self) -> usize {
        self.queue_depth.load(Ordering::Acquire)
    }
}
```

Add a `max_concurrent()` accessor to `NodeMemoryGovernor`:

```rust
    pub fn max_concurrent(&self) -> usize {
        self.max_concurrent
    }
```

Note for implementer: for production wakeups, hold the `Arc<Notify>` in `NodeMemoryGovernor` and call `notify_one()` from `QueryReservation::drop`, so a freed slot wakes a waiter immediately rather than at the 250ms backstop. The test above passes either way; wire the drop-notify before merging.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p sqe-coordinator admission`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-coordinator/src/admission.rs crates/sqe-coordinator/src/lib.rs crates/sqe-coordinator/src/memory_governor.rs
git commit -m "feat(coordinator): minimal AdmissionGate with fast path and bounded queue"
```

---

## Phase 5: Spillable operators

### Task 6: Fix sort-on-write can_spill=false

**Files:**
- Modify: `crates/sqe-planner/src/distributed_sort.rs` and the sort-on-write CTAS path (search for the `ExternalSorterMerge`/`can_spill` usage with `rg "can_spill"`).
- Test: `crates/sqe-coordinator/tests/it/` new integration test or the existing sort/write test module.

**Interfaces:**
- Consumes: the governor pool (Task 4) as the `RuntimeEnv` memory pool, DataFusion `DiskManager`.
- Produces: sort-on-write that spills to disk instead of OOMing.

- [ ] **Step 1: Write the failing test**

```rust
// Force a tiny memory pool and a sort-on-write CTAS that exceeds it.
// Before the fix this OOMs/aborts; after, it spills and completes.
#[tokio::test]
async fn sort_on_write_spills_instead_of_oom() {
    let result = run_ctas_with_sort_under_memory_limit(
        /* memory_bytes */ 16 * 1024 * 1024,
        /* rows */ 2_000_000,
    ).await;
    assert!(result.is_ok(), "sort-on-write must spill, not fail: {result:?}");
    assert!(spill_count_for_last_query() > 0, "expected at least one spill");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p sqe-coordinator sort_on_write_spills_instead_of_oom`
Expected: FAIL, query aborts with a resource error (the current OOM path).

- [ ] **Step 3: Implement the fix**

Locate the sort construction on the write path. The current code sets the sort exec or sorter with spill disabled. Change it to enable spill and ensure it is built against the governor pool's `RuntimeEnv` with a `DiskManager` configured to `spill_dir`:

```rust
// Build the RuntimeEnv with the governed pool + disk manager once, at
// session/runtime construction, and reuse it on the write path:
let runtime = RuntimeEnvBuilder::new()
    .with_memory_pool(governor.pool())
    .with_disk_manager(DiskManagerConfig::NewSpecified(spill_dirs))
    .build_arc()?;

// On the sort-on-write path, do not force can_spill=false. Use the
// standard SortExec which spills via the runtime's memory pool + disk
// manager when reservations cannot be met:
let sort = SortExec::new(sort_exprs, input)
    .with_preserve_partitioning(preserve);
// (Remove the prior `.with_fetch(None)`-plus-no-spill construction that
// routed through ExternalSorterMerge with can_spill=false.)
```

Note for implementer: confirm the exact constructor and the `spill_dirs` source (`spill_dir` config, default temp). The key change is that the write-path sort must use the same spillable `RuntimeEnv` as the query path, not a bespoke non-spilling sorter.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p sqe-coordinator sort_on_write_spills_instead_of_oom`
Expected: PASS, spill count greater than zero.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-planner/src/distributed_sort.rs crates/sqe-coordinator/tests/
git commit -m "fix(write): sort-on-write spills to disk instead of OOM"
```

---

### Task 7: Verify hash-join, aggregate, and sort use the governed pool and spill

**Files:**
- Modify: runtime construction where `RuntimeEnv`/`SessionContext` is built (search `rg "RuntimeEnvBuilder|with_memory_pool|FairSpillPool" crates/sqe-coordinator crates/sqe-worker`).
- Test: `crates/sqe-coordinator/tests/it/` spill-correctness test.

**Interfaces:**
- Consumes: `NodeMemoryGovernor::pool()`.
- Produces: every node's DataFusion `RuntimeEnv` uses the governed pool + a `DiskManager`, so hash-join build, hash aggregate, and sort spill under pressure with correct results.

- [ ] **Step 1: Write the failing test**

```rust
// With a tiny pool, a hash join + aggregate over a moderate input must
// produce results identical to the same query with a large pool.
#[tokio::test]
async fn join_and_agg_spill_correctly() {
    let big = run_join_agg_query(/* memory_bytes */ 1 << 30).await.unwrap();
    let small = run_join_agg_query(/* memory_bytes */ 16 * 1024 * 1024).await.unwrap();
    assert_eq!(big, small, "spilled results must equal in-memory results");
    assert!(spill_count_for_last_query() > 0, "small pool must spill");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p sqe-coordinator join_and_agg_spill_correctly`
Expected: FAIL if the runtime is not built from the governed pool (either no spill, or a divergent/aborted result).

- [ ] **Step 3: Wire the governed pool into the runtime**

At the single place the worker and coordinator build their `RuntimeEnv`:

```rust
let governor = NodeMemoryGovernor::new(&config.query);
let runtime = RuntimeEnvBuilder::new()
    .with_memory_pool(governor.pool())
    .with_disk_manager(disk_manager_from(&config.query))? // spill_dir or temp
    .build_arc()?;
// Pass `governor` to the coordinator state so AdmissionGate and metrics
// share the same instance.
```

Add a small helper:

```rust
fn disk_manager_from(cfg: &QueryConfig) -> DiskManagerConfig {
    match (cfg.spill_enabled, cfg.spill_dir.as_deref()) {
        (false, _) => DiskManagerConfig::Disabled,
        (true, Some(dir)) => DiskManagerConfig::NewSpecified(vec![dir.into()]),
        (true, None) => DiskManagerConfig::NewOs,
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p sqe-coordinator join_and_agg_spill_correctly`
Expected: PASS, results equal, spill count greater than zero.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-coordinator/src crates/sqe-worker/src crates/sqe-coordinator/tests/
git commit -m "feat(runtime): build DataFusion runtime from the governed pool with disk spill"
```

---

## Phase 6: SpillableShuffleReceiver

### Task 8: Disk-spill tier for the shuffle receiver

**Files:**
- Modify: `crates/sqe-worker/src/shuffle.rs`
- Test: `crates/sqe-worker/src/shuffle.rs` (inline tests)

**Interfaces:**
- Consumes: `spill_dir` (config), Arrow IPC writer/reader, the governed pool for budget checks.
- Produces: `ShuffleReceiver` gains a disk-spill tier: when an in-memory partition buffer exceeds its budget, batches are written to an Arrow IPC file under `spill_dir` and streamed back on read. New: `ShuffleReceiver::with_spill(budget_bytes: u64, spill_dir: PathBuf)`; spilled files are deleted on receiver drop.

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn shuffle_receiver_spills_and_restores_in_order() {
    let dir = tempfile::tempdir().unwrap();
    // Tiny budget forces spill after the first batch.
    let rx = ShuffleReceiver::with_spill(/* budget */ 1, dir.path().to_path_buf());
    let batches = make_test_batches(10);
    for b in &batches { rx.push(0, b.clone()).await.unwrap(); }
    rx.close(0);
    let restored = rx.drain(0).await.unwrap();
    assert_eq!(restored, batches, "spilled batches restore in order");
    // Spill file exists while open.
    assert!(std::fs::read_dir(dir.path()).unwrap().count() > 0);
    drop(rx);
    // Cleanup on drop.
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn shuffle_receiver_stays_in_memory_under_budget() {
    let dir = tempfile::tempdir().unwrap();
    let rx = ShuffleReceiver::with_spill(/* budget */ 1 << 30, dir.path().to_path_buf());
    for b in make_test_batches(3) { rx.push(0, b).await.unwrap(); }
    // No spill file created on the happy path (small-usage no-regression).
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p sqe-worker shuffle_receiver_spills`
Expected: FAIL, `with_spill` and the spill tier do not exist.

- [ ] **Step 3: Implement the spill tier**

Add a per-partition buffer that tracks in-memory bytes and overflows to an Arrow IPC file:

```rust
use std::path::PathBuf;
use arrow_ipc::writer::FileWriter;
use arrow_ipc::reader::FileReader;

struct PartitionBuffer {
    in_mem: Vec<RecordBatch>,
    in_mem_bytes: u64,
    budget: u64,
    spill_path: Option<PathBuf>,
    spill_writer: Option<FileWriter<std::fs::File>>,
    spill_dir: PathBuf,
    schema: Option<SchemaRef>,
}

impl PartitionBuffer {
    fn push(&mut self, batch: RecordBatch) -> Result<(), SqeError> {
        let sz = batch.get_array_memory_size() as u64;
        if self.in_mem_bytes + sz <= self.budget && self.spill_writer.is_none() {
            self.in_mem_bytes += sz;
            self.schema.get_or_insert_with(|| batch.schema());
            self.in_mem.push(batch);
            return Ok(());
        }
        // Over budget: open a spill file once, then append.
        if self.spill_writer.is_none() {
            let path = self.spill_dir.join(format!("shuffle-{}.arrow", uuid_like()));
            let file = std::fs::File::create(&path)
                .map_err(|e| SqeError::MemoryExhausted {
                    node: "worker".into(), operator: "ShuffleReceiver".into(),
                    requested: sz, available: 0, spilled: self.in_mem_bytes,
                })?;
            let schema = self.schema.clone().unwrap_or_else(|| batch.schema());
            let mut w = FileWriter::try_new(file, &schema).map_err(to_transient_io)?;
            // Flush any in-memory batches to disk first to bound memory.
            for b in self.in_mem.drain(..) { w.write(&b).map_err(to_transient_io)?; }
            self.in_mem_bytes = 0;
            self.spill_path = Some(path);
            self.spill_writer = Some(w);
        }
        self.spill_writer.as_mut().unwrap().write(&batch).map_err(to_transient_io)?;
        Ok(())
    }

    fn drain(mut self) -> Result<Vec<RecordBatch>, SqeError> {
        if let Some(mut w) = self.spill_writer.take() {
            w.finish().map_err(to_transient_io)?;
            let path = self.spill_path.clone().unwrap();
            let file = std::fs::File::open(&path).map_err(to_transient_io)?;
            let reader = FileReader::try_new(file, None).map_err(to_transient_io)?;
            let mut out = Vec::new();
            for b in reader { out.push(b.map_err(to_transient_io)?); }
            return Ok(out);
        }
        Ok(self.in_mem)
    }
}

impl Drop for PartitionBuffer {
    fn drop(&mut self) {
        if let Some(p) = &self.spill_path { let _ = std::fs::remove_file(p); }
    }
}
```

Map IPC errors to the transient-I/O class:

```rust
fn to_transient_io(e: arrow_schema::ArrowError) -> SqeError {
    SqeError::Io(format!("shuffle spill io: {e}")) // classified TransientIo
}
```

Wire `ShuffleReceiver::with_spill` to build per-partition `PartitionBuffer`s with the given budget and dir, and route `push`/`drain` through them. Keep the existing bounded-channel API for the in-memory fast path; the buffer only spills when over budget.

Note for implementer: reuse any existing uuid/id helper in the crate for `uuid_like()`; do not add a dependency. Confirm the Arrow IPC module paths against the pinned arrow version.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p sqe-worker shuffle_receiver`
Expected: PASS, both spill-and-restore and stays-in-memory tests green.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-worker/src/shuffle.rs
git commit -m "feat(worker): disk-spill tier for shuffle receiver (Arrow IPC), cleanup on drop"
```

---

## Phase 7: EncodedStagePlanCache

### Task 9: Encoded-stage-plan cache on the dispatch hot path

**Files:**
- Create: `crates/sqe-coordinator/src/stage_plan_cache.rs`
- Modify: `crates/sqe-coordinator/src/lib.rs`, `crates/sqe-coordinator/src/distributed_scan.rs` (dispatch path)
- Test: `crates/sqe-coordinator/src/stage_plan_cache.rs` (inline tests)

**Interfaces:**
- Consumes: `stage_plan_cache_entries` config, SQE's existing physical-plan codec.
- Produces: `pub struct EncodedStagePlanCache` with `pub fn get_or_encode(&self, key: StageKey, encode: impl FnOnce() -> Result<Vec<u8>, SqeError>) -> Result<Arc<Vec<u8>>, SqeError>`; `StageKey = (String /*query_id*/, String /*stage_id*/)`. Disabled (entries == 0) means always-encode, no caching.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn caches_encoding_per_stage_key() {
    let cache = EncodedStagePlanCache::new(8);
    let calls = std::cell::Cell::new(0);
    let key = ("q1".to_string(), "s1".to_string());
    let a = cache.get_or_encode(key.clone(), || { calls.set(calls.get()+1); Ok(vec![1,2,3]) }).unwrap();
    let b = cache.get_or_encode(key.clone(), || { calls.set(calls.get()+1); Ok(vec![9,9,9]) }).unwrap();
    assert_eq!(*a, vec![1,2,3]);
    assert_eq!(*b, vec![1,2,3], "second call served from cache");
    assert_eq!(calls.get(), 1, "encode ran once");
}

#[test]
fn disabled_cache_always_encodes() {
    let cache = EncodedStagePlanCache::new(0);
    let calls = std::cell::Cell::new(0);
    let key = ("q1".to_string(), "s1".to_string());
    let _ = cache.get_or_encode(key.clone(), || { calls.set(calls.get()+1); Ok(vec![1]) }).unwrap();
    let _ = cache.get_or_encode(key, || { calls.set(calls.get()+1); Ok(vec![1]) }).unwrap();
    assert_eq!(calls.get(), 2, "disabled cache does not memoize");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p sqe-coordinator stage_plan_cache`
Expected: FAIL, module does not exist.

- [ ] **Step 3: Implement the cache**

```rust
//! Memoizes serialized physical stage plans so re-binding a stage's tasks
//! does not re-encode the plan per fragment. A high-concurrency dispatch
//! throughput lever; disabled when entries == 0.

use std::sync::Arc;
use moka::sync::Cache;
use sqe_core::errors::SqeError;

pub type StageKey = (String, String);

pub struct EncodedStagePlanCache {
    inner: Option<Cache<StageKey, Arc<Vec<u8>>>>,
}

impl EncodedStagePlanCache {
    pub fn new(entries: usize) -> Self {
        let inner = if entries == 0 {
            None
        } else {
            Some(Cache::builder().max_capacity(entries as u64).build())
        };
        Self { inner }
    }

    pub fn get_or_encode(
        &self,
        key: StageKey,
        encode: impl FnOnce() -> Result<Vec<u8>, SqeError>,
    ) -> Result<Arc<Vec<u8>>, SqeError> {
        match &self.inner {
            None => Ok(Arc::new(encode()?)),
            Some(cache) => {
                if let Some(v) = cache.get(&key) {
                    return Ok(v);
                }
                let v = Arc::new(encode()?);
                cache.insert(key, Arc::clone(&v));
                Ok(v)
            }
        }
    }
}
```

Then in `distributed_scan.rs`, replace the per-fragment `encode_physical_plan(...)` call with `cache.get_or_encode((query_id, stage_id), || encode_physical_plan(...))`. Construct one cache from `config.query.stage_plan_cache_entries` at coordinator startup and share it.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p sqe-coordinator stage_plan_cache`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-coordinator/src/stage_plan_cache.rs crates/sqe-coordinator/src/lib.rs crates/sqe-coordinator/src/distributed_scan.rs
git commit -m "feat(coordinator): encoded-stage-plan cache for dispatch hot path"
```

---

## Phase 8: QueryAbortPath wiring

### Task 10: Abort one query with full reclaim on memory exhaustion

**Files:**
- Modify: `crates/sqe-coordinator/src/distributed_scan.rs` (fragment dispatch error handling), the query lifecycle in `query_handler.rs`.
- Test: `crates/sqe-coordinator/tests/it/` integration test.

**Interfaces:**
- Consumes: `SqeError::MemoryExhausted`, `MemoryFailureClass`, `QueryReservation`, shuffle spill cleanup (Task 8).
- Produces: on `MemoryExhausted`, the one query is cancelled, its pool reservations and spill files are reclaimed, a typed error reaches the client, and the node keeps serving. A query-level error never marks a worker unhealthy.

- [ ] **Step 1: Write the failing test**

```rust
// One oversized query among many small ones: only the big one fails,
// the small ones complete, and the worker stays healthy.
#[tokio::test]
async fn blast_radius_is_one_query() {
    let cluster = start_test_cluster(/* worker_mem */ 64 * 1024 * 1024).await;
    let small = (0..20).map(|_| cluster.run("SELECT count(*) FROM small_t"));
    let big = cluster.run("SELECT * FROM big_t ORDER BY x"); // exceeds memory
    let small_results = futures::future::join_all(small).await;
    assert!(small_results.iter().all(|r| r.is_ok()), "small queries unaffected");
    assert!(matches!(big.await, Err(SqeError::MemoryExhausted { .. })));
    assert!(cluster.all_workers_healthy(), "no worker evicted by a query error");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p sqe-coordinator blast_radius_is_one_query`
Expected: FAIL (today a query-level error may abort more broadly or not be typed).

- [ ] **Step 3: Implement the abort path**

In the fragment dispatch error handler, classify before reacting:

```rust
match fragment_result {
    Err(e) => {
        match e.failure_class() {
            MemoryFailureClass::QueryLevel => {
                // Fail this query only. Do NOT mark the worker unhealthy.
                cancel_query(query_id).await; // drops reservations + spill files via RAII
                return Err(e);
            }
            MemoryFailureClass::ShuffleDataLoss => {
                if attempt < max_retries {
                    // re-run producing stage (subsystem D hook); for now retry fragment
                } else {
                    return Err(e);
                }
            }
            MemoryFailureClass::TransientIo => {
                // retry the fragment silently up to max_retries
            }
        }
    }
    Ok(stream) => { /* existing path */ }
}
```

Ensure `cancel_query` drops the query's `QueryReservation` and that all `PartitionBuffer`s for the query are dropped (RAII deletes spill files). Confirm the existing worker-health path is only triggered by transport/health failures, never by a decoded query-level `SqeError` (this is the Ballista anti-pattern guard).

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p sqe-coordinator blast_radius_is_one_query`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-coordinator/src/distributed_scan.rs crates/sqe-coordinator/src/query_handler.rs crates/sqe-coordinator/tests/
git commit -m "feat(coordinator): fail-query-not-cluster abort path with full reclaim"
```

---

## Phase 9: Observability

### Task 11: Metrics gauges and counters

**Files:**
- Modify: `crates/sqe-metrics/src/lib.rs`, `crates/sqe-coordinator/src/memory.rs` (emit), `crates/sqe-coordinator/src/memory_governor.rs` and `admission.rs` (emit).
- Test: `crates/sqe-metrics/src/lib.rs` (inline test that the metrics register).

**Interfaces:**
- Produces: gauges `sqe_node_memory_used_bytes`, `sqe_node_memory_limit_bytes`, `sqe_node_memory_utilization`, `sqe_admission_concurrency`, `sqe_admission_queue_depth`, `sqe_spill_files_open`; counters `sqe_spilled_bytes_total`, `sqe_queries_killed_oom_total`, `sqe_admissions_rejected_total`, `sqe_stage_plan_cache_hits_total`, `sqe_stage_plan_cache_misses_total`.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn memory_safety_metrics_register() {
    let m = Metrics::new_for_test();
    m.queries_killed_oom_total.inc();
    m.admissions_rejected_total.inc();
    m.node_memory_utilization.set(0.5);
    let text = m.gather_text();
    assert!(text.contains("sqe_queries_killed_oom_total"));
    assert!(text.contains("sqe_admissions_rejected_total"));
    assert!(text.contains("sqe_node_memory_utilization"));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p sqe-metrics memory_safety_metrics_register`
Expected: FAIL, fields not defined.

- [ ] **Step 3: Add the metrics**

Add the gauges/counters to the metrics struct following the existing registration pattern in `sqe-metrics`, then emit them: from `memory.rs` the utilization/used/limit gauges (it already sets `coordinator_memory_limit_bytes`), from `admission.rs` the concurrency/queue-depth gauges and the rejected counter, from the abort path the killed counter, from the shuffle buffer the spilled-bytes counter and open-files gauge, from the cache the hit/miss counters.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p sqe-metrics memory_safety_metrics_register`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-metrics/src/lib.rs crates/sqe-coordinator/src
git commit -m "feat(metrics): memory-safety gauges and counters"
```

---

### Task 12: Structured logging and OCSF audit on kills and rejections

**Files:**
- Modify: the abort path (`distributed_scan.rs`), `admission.rs`, and the audit emitter (search `rg "ocsf|audit" crates/sqe-coordinator`).
- Test: `crates/sqe-coordinator/tests/it/` log/audit assertion test.

**Interfaces:**
- Consumes: `SqeError::{MemoryExhausted, Rejected}`, the existing audit/OCSF emitter.
- Produces: one warn log + one OCSF audit event per kill; one info log per rejection and first-spill-per-query. No per-batch logging.

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn kill_emits_one_audit_event_and_warn() {
    let (cluster, audit) = start_test_cluster_with_audit_capture(64*1024*1024).await;
    let _ = cluster.run("SELECT * FROM big_t ORDER BY x").await; // killed
    let events = audit.captured();
    let kills: Vec<_> = events.iter().filter(|e| e.kind == "memory_kill").collect();
    assert_eq!(kills.len(), 1, "exactly one audit event per kill");
    assert!(kills[0].fields.contains_key("bytes_spilled"));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p sqe-coordinator kill_emits_one_audit_event`
Expected: FAIL, no audit emission on kill.

- [ ] **Step 3: Emit log + audit at the abort point**

```rust
// At the QueryLevel branch of the abort path, before returning the error:
tracing::warn!(
    query_id = %query_id, user = %user, node = %node, operator = %operator,
    requested, available, spilled,
    "query killed: memory exhausted"
);
audit.emit_memory_kill(MemoryKillEvent {
    query_id: query_id.clone(), user: user.clone(),
    requested, available, spilled, reason: "memory_exhausted".into(),
});
```

For rejections, an `info!` in `admission.rs` at the `Rejected` return; for first spill, an `info!` guarded by a per-query "already logged" flag so it fires once.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p sqe-coordinator kill_emits_one_audit_event`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-coordinator/src crates/sqe-coordinator/tests/
git commit -m "feat(observability): structured log + OCSF audit on memory kills and rejections"
```

---

### Task 13: Web UI panels for memory, spill, and admission

**Files:**
- Modify: `crates/sqe-coordinator/src/web_ui.rs` (and its templates/handlers).
- Test: `crates/sqe-coordinator/src/web_ui.rs` (handler test asserting JSON shape).

**Interfaces:**
- Consumes: governor gauges, `QueryRecord`, `FragmentInfo`, `WorkerState`.
- Produces: a `/api/memory` endpoint returning `{ nodes: [{ id, used_bytes, limit_bytes, utilization, pressure, spill_files_open }], admission: { concurrency, cap, queue_depth, rejected_total }, killed_recent: [...] }`; query-list rows gain `memory_used_bytes`, `spilled_bytes`, `state`.

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn memory_api_returns_node_and_admission_state() {
    let app = test_app_with_governor().await;
    let resp = app.get("/api/memory").await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await;
    assert!(body["nodes"].is_array());
    assert!(body["admission"]["cap"].is_number());
    assert!(body["killed_recent"].is_array());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p sqe-coordinator memory_api_returns_node`
Expected: FAIL, route does not exist.

- [ ] **Step 3: Implement the endpoint and panels**

Add the `/api/memory` axum handler that reads the governor and metrics into the response shape above, register it on the existing web UI router, and add the three columns to the query-list response. Render the cluster memory panel, admission view, and killed-queries view in the existing UI templates. Keep the response shapes aligned with `QueryStageSummary`/`TaskSummary` so per-query detail can render stage-level spill.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p sqe-coordinator memory_api_returns_node`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-coordinator/src/web_ui.rs
git commit -m "feat(web-ui): memory, spill, and admission panels"
```

---

## Phase 10: Integration and no-regression gates

### Task 14: Concurrency soak, small-deployment no-regression, and config tests

**Files:**
- Test: `crates/sqe-coordinator/tests/it/memory_safety_e2e.rs`

**Interfaces:**
- Consumes: the full stack from Tasks 1 to 13.
- Produces: the headline guarantee test plus the small-usage no-regression and config tests.

- [ ] **Step 1: Write the failing tests**

```rust
// Headline gate: high concurrency on a memory-starved cluster never
// OOM-kills a process; every query completes or fails with a typed error.
#[tokio::test]
async fn concurrency_soak_no_process_oom() {
    let cluster = start_test_cluster(/* worker_mem */ 128 * 1024 * 1024).await;
    let queries = (0..200).map(|i| cluster.run(query_for(i)));
    let results = futures::future::join_all(queries).await;
    for r in &results {
        if let Err(e) = r {
            assert!(
                matches!(e, SqeError::MemoryExhausted { .. } | SqeError::Rejected { .. }),
                "only typed memory errors allowed, got {e:?}"
            );
        }
    }
    assert!(cluster.all_workers_healthy());
    assert!(!cluster.any_process_was_oom_killed(), "no SIGKILL");
}

// Small usage is unaffected: default config, RAM above working set.
#[tokio::test]
async fn small_deployment_no_spill_no_queue() {
    let cluster = start_test_cluster_default_config(/* generous mem */ 2 << 30).await;
    let r = cluster.run("SELECT * FROM small_t ORDER BY x").await.unwrap();
    assert!(spill_count_for_last_query() == 0, "no spill on small usage");
    assert_eq!(cluster.admission_queue_high_watermark(), 0, "never queued");
    drop(r);
}

// Auto-tune derives sane values from a simulated cgroup limit.
#[test]
fn autotune_pool_and_floor() {
    let mut cfg = QueryConfig::default();
    cfg.max_concurrent_queries = 100;
    let gov = NodeMemoryGovernor::new(&cfg);
    assert!(gov.available_bytes() > 0);
}
```

- [ ] **Step 2: Run tests to verify they fail or pass meaningfully**

Run: `cargo test -p sqe-coordinator --test memory_safety_e2e`
Expected: the soak and no-regression tests exercise the integrated stack; fix any integration gaps they surface.

- [ ] **Step 3: Close any integration gaps**

If the soak test trips a non-typed failure, trace it to the operator or path that did not route through the governed pool or the abort classifier, and fix that wiring. No new feature code; this task is the gate that proves Tasks 1 to 13 compose.

- [ ] **Step 4: Run the full suite + clippy**

Run:
```bash
cargo test --all
cargo clippy --all-targets --all-features -- -D warnings
```
Expected: PASS, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-coordinator/tests/
git commit -m "test(memory-safety): concurrency soak, small-usage no-regression, config gates"
```

---

### Task 15: Docs and project-state updates

**Files:**
- Modify: `README.md` (roadmap), `nextsteps.md` (status), `docs/site/book/src/operations/runbook.md` (the two config profiles + spill-dir sizing).
- Test: doc voice check.

- [ ] **Step 1: Document the two config profiles**

In `docs/site/book/src/operations/runbook.md`, add a memory-safety section: the default single-node profile (mechanisms dormant) and the production cluster profile (explicit `max-concurrent-queries`, NVMe `spill-dir`, pinned `per-query-memory-floor`), with recommended values for Chameleon worker pods and the "spill disk >= N x memory" sizing note.

- [ ] **Step 2: Update roadmap and status**

Mark the OOM-safety subsystem done in `README.md` and shift the NEXT pointer in `nextsteps.md` to subsystem B (admission and fair-share scheduling).

- [ ] **Step 3: Voice check**

Run: `grep -rnE '—|–|→' README.md nextsteps.md docs/site/book/src/operations/runbook.md`
Expected: zero hits in prose.

- [ ] **Step 4: Commit**

```bash
git add README.md nextsteps.md docs/site/book/src/operations/runbook.md
git commit -m "docs: memory-safety config profiles and roadmap update"
```

---

## Self-Review

Spec coverage:
- Shared per-node pool, floor, admission cap: Tasks 4, 5.
- Spill on every pipeline breaker incl. sort-on-write fix: Tasks 6, 7.
- Spillable shuffle receiver: Task 8.
- Three-class failure taxonomy + fail-query-not-cluster: Tasks 3, 10.
- EncodedStagePlanCache: Task 9.
- Metrics, logging, web UI: Tasks 11, 12, 13.
- Configurable + zero small-impact: Tasks 1, 2; proven in Task 14.
- Coordinator parity: Task 7 builds the runtime on both worker and coordinator.
- Auto-tuned defaults from detected memory: Task 2, used in Task 4.

Open parameters to confirm with the spec author before or during execution (do not block on them; the auto defaults are safe):
- `memory_headroom_fraction` default (0.2) and the absolute floor minimum (8 MiB) values.
- Whether the coordinator governor ships in this cut (Task 7 includes it) or is deferred.

Type consistency: `NodeMemoryGovernor`, `QueryReservation`, `AdmissionGate`, `EncodedStagePlanCache`, `MemoryFailureClass`, `SqeError::{MemoryExhausted, Rejected}`, `StageKey` are defined once and referenced consistently across tasks.
