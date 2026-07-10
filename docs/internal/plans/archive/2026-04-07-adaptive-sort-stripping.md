# Adaptive Sort Stripping Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a three-mode `sort_mode` config (`strict`/`partition_only`/`adaptive`) that strips non-essential `SortExec` nodes from the physical plan to reduce memory pressure and spill.

**Architecture:** A new `AdaptiveSortRule` physical optimizer rule lives in `sqe-coordinator` (not `sqe-planner`) because it needs access to both `IcebergScanExec` (from `sqe-catalog`) and `MemoryPressure` (from `sqe-coordinator::memory`). Applied as a manual tree transform in `execute_query()` after `create_physical_plan()` but before `try_distribute()`. Also adds `partition_column_names()` to `IcebergScanExec` and a `sorts_stripped_total` metric.

**Tech Stack:** Rust, DataFusion `PhysicalOptimizerRule`, Iceberg partition spec metadata, Prometheus metrics

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `crates/sqe-core/src/config.rs` | Modify | Add `sort_mode` field to `QueryConfig` |
| `crates/sqe-catalog/src/iceberg_scan.rs` | Modify | Add `partition_column_names()` method |
| `crates/sqe-coordinator/src/adaptive_sort.rs` | Create | `AdaptiveSortRule` physical optimizer rule |
| `crates/sqe-coordinator/src/query_handler.rs` | Modify | Wire rule into `execute_query()`, improve error messages |
| `crates/sqe-coordinator/src/lib.rs` | Modify | Add `pub mod adaptive_sort;` |
| `crates/sqe-metrics/src/lib.rs` | Modify | Add `sorts_stripped_total` metric |

---

### Task 1: Add `sort_mode` config field

**Files:**
- Modify: `crates/sqe-core/src/config.rs`

- [ ] **Step 1: Add `sort_mode` field and default function to `QueryConfig`**

In `crates/sqe-core/src/config.rs`, add the field to `QueryConfig` struct (after `target_task_size`):

```rust
    /// Controls when ORDER BY clauses are preserved vs stripped to save memory.
    /// - "strict":         Always sort. Spill to disk if needed. (backwards-compatible)
    /// - "partition_only": Only sort when keys match Iceberg partition columns.
    /// - "adaptive":       Sort when memory is Green; strip non-partition sorts under pressure.
    /// Default: "adaptive"
    #[serde(default = "default_sort_mode")]
    pub sort_mode: String,
```

Add the default function (next to the other defaults around line 670):

```rust
fn default_sort_mode() -> String { "adaptive".to_string() }
```

Add `sort_mode` to the `Default` impl:

```rust
sort_mode: default_sort_mode(),
```

- [ ] **Step 2: Add `SortMode` enum to `sqe-core` for type-safe parsing**

Add a `SortMode` enum above `QueryConfig` in config.rs:

```rust
/// Controls adaptive sort stripping behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortMode {
    /// Always sort. Spill to disk if needed.
    Strict,
    /// Only sort when keys match Iceberg partition columns.
    PartitionOnly,
    /// Sort when memory allows; strip non-partition sorts under pressure.
    Adaptive,
}

impl SortMode {
    /// Parse from config string. Returns `Adaptive` for unknown values.
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "strict" => Self::Strict,
            "partition_only" | "partition-only" => Self::PartitionOnly,
            "adaptive" => Self::Adaptive,
            _ => {
                tracing::warn!(sort_mode = s, "Unknown sort_mode, defaulting to adaptive");
                Self::Adaptive
            }
        }
    }
}
```

- [ ] **Step 3: Export `SortMode` from `sqe-core`**

In `crates/sqe-core/src/lib.rs`, make sure `SortMode` is re-exported. Check how other types are exported (likely `pub use config::SortMode;` or similar).

- [ ] **Step 4: Verify build**

Run: `cargo build -p sqe-core`
Expected: PASS — no other crates reference the new field yet

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-core/src/config.rs crates/sqe-core/src/lib.rs
git commit -m "feat: add sort_mode config (strict/partition_only/adaptive, default: adaptive)"
```

---

### Task 2: Add `partition_column_names()` to `IcebergScanExec`

**Files:**
- Modify: `crates/sqe-catalog/src/iceberg_scan.rs`

- [ ] **Step 1: Write the test**

Add to the `#[cfg(test)]` section at the bottom of `iceberg_scan.rs` (or create one if not present). Since `IcebergScanExec` requires a real Iceberg `Table`, test the extraction logic as a standalone function:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_column_extraction_identity_only() {
        // Test the logic: given identity + non-identity transforms,
        // only identity columns are returned.
        // Since we can't easily construct Iceberg Table in unit tests,
        // we test the extraction logic via a helper.
        let names = filter_identity_partition_names(
            &["year", "month", "day_bucket"],
            &[true, true, false], // identity flags
        );
        assert_eq!(names, vec!["year", "month"]);
    }

    #[test]
    fn test_partition_column_extraction_empty() {
        let names = filter_identity_partition_names(&[], &[]);
        assert!(names.is_empty());
    }

    #[test]
    fn test_partition_column_extraction_all_non_identity() {
        let names = filter_identity_partition_names(
            &["hash_id", "truncated_name"],
            &[false, false],
        );
        assert!(names.is_empty());
    }
}
```

- [ ] **Step 2: Add the helper function and run test to verify it fails**

Add a helper function above the tests module that the tests can call, and that `partition_column_names()` will also use:

```rust
/// Filter partition field names, keeping only those with identity transforms.
/// Used by `IcebergScanExec::partition_column_names()` and unit tests.
fn filter_identity_partition_names(names: &[&str], is_identity: &[bool]) -> Vec<String> {
    names.iter().zip(is_identity.iter())
        .filter(|(_, &ident)| ident)
        .map(|(name, _)| name.to_string())
        .collect()
}
```

Run: `cargo test -p sqe-catalog -- test_partition_column_extraction`
Expected: PASS (tests should pass immediately since we're testing the helper)

- [ ] **Step 3: Add `partition_column_names()` to `IcebergScanExec`**

Add this method to the `impl IcebergScanExec` block (after `pub fn projection()`):

```rust
    /// Returns the names of identity-transform partition columns from the
    /// Iceberg table's default partition spec.
    ///
    /// Bucket, truncate, date, and other derived transforms are excluded
    /// because they don't map directly to sortable column values.
    pub fn partition_column_names(&self) -> Vec<String> {
        use iceberg::spec::Transform;
        let spec = self.table.metadata().default_partition_spec();
        let schema = self.table.metadata().current_schema();
        spec.fields()
            .iter()
            .filter(|f| f.transform == Transform::Identity)
            .filter_map(|f| {
                schema
                    .field_by_id(f.source_id)
                    .map(|sf| sf.name.clone())
            })
            .collect()
    }
```

- [ ] **Step 4: Verify build**

Run: `cargo build -p sqe-catalog`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-catalog/src/iceberg_scan.rs
git commit -m "feat: add partition_column_names() to IcebergScanExec"
```

---

### Task 3: Add `sorts_stripped_total` metric

**Files:**
- Modify: `crates/sqe-metrics/src/lib.rs`

- [ ] **Step 1: Add the metric field to `MetricsRegistry` struct**

In the `MetricsRegistry` struct, after the auth metrics fields (around line 81), add:

```rust
    // Adaptive sort metrics
    pub sorts_stripped_total: IntCounterVec,
```

- [ ] **Step 2: Register the metric in `MetricsRegistry::new()`**

After the `token_refresh_total` registration block (around line 382), add:

```rust
        // Adaptive sort stripping metric
        let sorts_stripped_total = IntCounterVec::new(
            Opts::new(
                "sqe_sorts_stripped_total",
                "Total sort operations stripped by adaptive sort rule",
            ),
            &["mode", "reason"],
        )
        .unwrap();
        registry.register(Box::new(sorts_stripped_total.clone())).unwrap();
```

- [ ] **Step 3: Add to the `Self { ... }` constructor return**

Add `sorts_stripped_total,` to the struct literal in `MetricsRegistry::new()`.

- [ ] **Step 4: Update the `test_metrics_registry_creation` test**

Add to the existing test (around line 587):

```rust
        // Adaptive sort metric
        metrics.sorts_stripped_total.with_label_values(&["adaptive", "memory_pressure"]).inc_by(0);
```

Update the assertion count: increment the minimum expected metrics count by 1 (from `>= 38` to `>= 39`).

- [ ] **Step 5: Add a dedicated test for the metric**

```rust
    #[test]
    fn test_sorts_stripped_metric() {
        let metrics = MetricsRegistry::new();
        metrics.sorts_stripped_total.with_label_values(&["adaptive", "memory_pressure"]).inc();
        metrics.sorts_stripped_total.with_label_values(&["partition_only", "partition_only"]).inc();
        metrics.sorts_stripped_total.with_label_values(&["adaptive", "memory_pressure"]).inc();
        assert_eq!(
            metrics.sorts_stripped_total.with_label_values(&["adaptive", "memory_pressure"]).get(),
            2
        );
        assert_eq!(
            metrics.sorts_stripped_total.with_label_values(&["partition_only", "partition_only"]).get(),
            1
        );
    }
```

- [ ] **Step 6: Run tests**

Run: `cargo test -p sqe-metrics`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add crates/sqe-metrics/src/lib.rs
git commit -m "feat: add sorts_stripped_total metric for adaptive sort tracking"
```

---

### Task 4: Implement `AdaptiveSortRule`

**Files:**
- Create: `crates/sqe-coordinator/src/adaptive_sort.rs`
- Modify: `crates/sqe-coordinator/src/lib.rs`

- [ ] **Step 1: Write the failing tests first**

Create `crates/sqe-coordinator/src/adaptive_sort.rs` with tests at the bottom:

```rust
//! Adaptive sort stripping: removes non-essential `SortExec` nodes from the
//! physical plan based on memory pressure and Iceberg partition columns.
//!
//! **Research basis:**
//! - Adaptive Query Processing (Deshpande et al., FTDB 2007)
//! - Memory-Adaptive External Sorting (Graefe, VLDB 2006)
//! - Progressive Optimization (Markl et al., VLDB 2004)
//!
//! Key principle: graceful degradation over hard failure — return results in
//! partition order rather than spilling to disk or killing the query.

use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::ExecutionPlan;
use tracing::{debug, warn};

use sqe_catalog::IcebergScanExec;
use sqe_core::SortMode;

use crate::memory::MemoryPressure;

/// Result of adaptive sort analysis for a single `SortExec` node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SortDecision {
    /// Keep the sort — either mode is strict, sort has LIMIT, or keys are partition columns.
    Keep,
    /// Strip the sort — non-partition keys under pressure or partition_only mode.
    Strip {
        /// Column names in the ORDER BY that are being stripped.
        sort_columns: Vec<String>,
        /// Reason for stripping.
        reason: StripReason,
    },
}

/// Why a sort was stripped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StripReason {
    /// sort_mode = partition_only and sort keys are not partition columns.
    PartitionOnlyMode,
    /// sort_mode = adaptive and memory pressure is Yellow or above.
    MemoryPressure(MemoryPressure),
}

impl StripReason {
    /// Returns the metric label value for this reason.
    pub fn metric_label(&self) -> &'static str {
        match self {
            Self::PartitionOnlyMode => "partition_only",
            Self::MemoryPressure(_) => "memory_pressure",
        }
    }
}

/// Decide whether to strip a sort based on mode, pressure, and partition keys.
///
/// Public for testing. Used internally by [`apply_adaptive_sort`].
pub fn decide_sort(
    sort_mode: SortMode,
    pressure: MemoryPressure,
    sort_column_names: &[String],
    partition_column_names: &[String],
    has_limit: bool,
) -> SortDecision {
    // TopK sorts (with LIMIT) are cheap and bounded — always keep them.
    if has_limit {
        return SortDecision::Keep;
    }

    // Check if all sort columns are partition columns
    let is_partition_sort = !sort_column_names.is_empty()
        && sort_column_names.iter().all(|sc| {
            partition_column_names.iter().any(|pc| pc == sc)
        });

    match sort_mode {
        SortMode::Strict => SortDecision::Keep,
        SortMode::PartitionOnly => {
            if is_partition_sort {
                SortDecision::Keep
            } else {
                SortDecision::Strip {
                    sort_columns: sort_column_names.to_vec(),
                    reason: StripReason::PartitionOnlyMode,
                }
            }
        }
        SortMode::Adaptive => {
            if pressure == MemoryPressure::Green {
                SortDecision::Keep
            } else if is_partition_sort {
                SortDecision::Keep
            } else {
                SortDecision::Strip {
                    sort_columns: sort_column_names.to_vec(),
                    reason: StripReason::MemoryPressure(pressure),
                }
            }
        }
    }
}

/// Extract sort column names from a `SortExec`'s sort expressions.
fn extract_sort_column_names(sort: &SortExec) -> Vec<String> {
    sort.expr()
        .iter()
        .map(|expr| format!("{}", expr.expr))
        .collect()
}

/// Walk the input subtree of a plan to find an `IcebergScanExec` and extract
/// its partition column names. Returns an empty vec if no scan is found.
fn find_partition_columns(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
    if let Some(scan) = plan.as_any().downcast_ref::<IcebergScanExec>() {
        return scan.partition_column_names();
    }
    for child in plan.children() {
        let result = find_partition_columns(child);
        if !result.is_empty() {
            return result;
        }
    }
    vec![]
}

/// Apply adaptive sort stripping to a physical plan.
///
/// Walks the plan tree top-down. For each `SortExec` node, decides whether to
/// keep it or replace it with its input child (stripping the sort).
///
/// Returns the (possibly modified) plan and a list of strip decisions for
/// logging/warning purposes.
pub fn apply_adaptive_sort(
    plan: Arc<dyn ExecutionPlan>,
    sort_mode: SortMode,
    pressure: MemoryPressure,
    metrics: Option<&Arc<sqe_metrics::MetricsRegistry>>,
) -> (Arc<dyn ExecutionPlan>, Vec<SortDecision>) {
    let mut strip_log: Vec<SortDecision> = Vec::new();

    // Short-circuit: strict mode never strips anything.
    if sort_mode == SortMode::Strict {
        return (plan, strip_log);
    }

    // Short-circuit: adaptive mode at Green pressure never strips.
    if sort_mode == SortMode::Adaptive && pressure == MemoryPressure::Green {
        return (plan, strip_log);
    }

    let decisions = std::sync::Mutex::new(Vec::new());

    let transformed = plan
        .transform_down(|node| {
            if let Some(sort_exec) = node.as_any().downcast_ref::<SortExec>() {
                let sort_cols = extract_sort_column_names(sort_exec);
                let has_limit = sort_exec.fetch().is_some();

                // Walk children to find partition columns
                let partition_cols = find_partition_columns(&sort_exec.children()[0].clone());

                let decision = decide_sort(
                    sort_mode,
                    pressure,
                    &sort_cols,
                    &partition_cols,
                    has_limit,
                );

                match &decision {
                    SortDecision::Strip { sort_columns, reason } => {
                        warn!(
                            sort_columns = ?sort_columns,
                            partition_columns = ?partition_cols,
                            reason = ?reason,
                            sort_mode = ?sort_mode,
                            pressure = %pressure,
                            "Stripped ORDER BY — non-partition columns under memory pressure. \
                             Consider removing ORDER BY or adding LIMIT."
                        );

                        if let Some(m) = metrics {
                            m.sorts_stripped_total
                                .with_label_values(&[
                                    match sort_mode {
                                        SortMode::Adaptive => "adaptive",
                                        SortMode::PartitionOnly => "partition_only",
                                        SortMode::Strict => "strict",
                                    },
                                    reason.metric_label(),
                                ])
                                .inc();
                        }

                        decisions.lock().unwrap().push(decision);

                        // Replace SortExec with its input child
                        let input = Arc::clone(sort_exec.children()[0]);
                        return Ok(Transformed::yes(input));
                    }
                    SortDecision::Keep => {}
                }
            }
            Ok(Transformed::no(node))
        })
        .unwrap_or_else(|_| datafusion::common::tree_node::Transformed::no(plan.clone()));

    strip_log = decisions.into_inner().unwrap();
    (transformed.data, strip_log)
}

/// Format a warning header for Flight SQL response when sorts were stripped.
///
/// Returns `None` if no sorts were stripped.
pub fn format_sort_warning(decisions: &[SortDecision], sort_mode: SortMode) -> Option<String> {
    let stripped: Vec<&SortDecision> = decisions
        .iter()
        .filter(|d| matches!(d, SortDecision::Strip { .. }))
        .collect();

    if stripped.is_empty() {
        return None;
    }

    let all_columns: Vec<String> = stripped
        .iter()
        .flat_map(|d| match d {
            SortDecision::Strip { sort_columns, .. } => sort_columns.clone(),
            _ => vec![],
        })
        .collect();

    let reason_desc = match &stripped[0] {
        SortDecision::Strip { reason: StripReason::PartitionOnlyMode, .. } => {
            format!("sort_mode=partition_only")
        }
        SortDecision::Strip { reason: StripReason::MemoryPressure(p), .. } => {
            format!("sort_mode=adaptive, pressure={}", p)
        }
        _ => String::new(),
    };

    Some(format!(
        "ORDER BY [{}] was removed due to {} ({}). Results are returned in partition order. \
         To force sorting, set sort_mode=strict or remove ORDER BY if ordering is not required.",
        all_columns.join(", "),
        match &stripped[0] {
            SortDecision::Strip { reason: StripReason::PartitionOnlyMode, .. } => "sort_mode=partition_only",
            SortDecision::Strip { reason: StripReason::MemoryPressure(_), .. } => "memory pressure",
            _ => "unknown",
        },
        reason_desc,
    ))
}

/// Format an actionable rejection message when memory is Red and query has sorts.
///
/// Used to replace the generic "Server under memory pressure" error with guidance
/// that tells the user exactly what to change.
pub fn format_pressure_rejection(
    sort_column_names: &[String],
    pressure: MemoryPressure,
) -> String {
    if sort_column_names.is_empty() {
        return format!(
            "Query rejected: server memory is >95% utilized (pressure={}). Please retry later.",
            pressure
        );
    }

    format!(
        "Query rejected: server memory is >95% utilized (pressure={}). \
         Your query includes ORDER BY [{}] which requires sort buffers. Options:\n  \
         1. Remove ORDER BY from your query (data returns in partition order)\n  \
         2. Add LIMIT to reduce sort memory (e.g., ORDER BY {} LIMIT 1000)\n  \
         3. Retry later when memory pressure decreases\n  \
         4. Ask your administrator to set sort_mode=adaptive for automatic sort management",
        pressure,
        sort_column_names.join(", "),
        sort_column_names.first().unwrap_or(&"col".to_string()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── decide_sort unit tests (all 6 decision matrix cells) ──────────

    #[test]
    fn test_strict_mode_always_keeps() {
        let decision = decide_sort(
            SortMode::Strict,
            MemoryPressure::Red,
            &["name".into()],
            &["year".into()],
            false,
        );
        assert_eq!(decision, SortDecision::Keep);
    }

    #[test]
    fn test_partition_only_keeps_partition_sort() {
        let decision = decide_sort(
            SortMode::PartitionOnly,
            MemoryPressure::Green,
            &["year".into(), "month".into()],
            &["year".into(), "month".into()],
            false,
        );
        assert_eq!(decision, SortDecision::Keep);
    }

    #[test]
    fn test_partition_only_strips_non_partition_sort() {
        let decision = decide_sort(
            SortMode::PartitionOnly,
            MemoryPressure::Green,
            &["name".into()],
            &["year".into()],
            false,
        );
        assert!(matches!(decision, SortDecision::Strip { reason: StripReason::PartitionOnlyMode, .. }));
    }

    #[test]
    fn test_adaptive_green_keeps_everything() {
        let decision = decide_sort(
            SortMode::Adaptive,
            MemoryPressure::Green,
            &["name".into()],
            &["year".into()],
            false,
        );
        assert_eq!(decision, SortDecision::Keep);
    }

    #[test]
    fn test_adaptive_yellow_keeps_partition_sort() {
        let decision = decide_sort(
            SortMode::Adaptive,
            MemoryPressure::Yellow,
            &["year".into()],
            &["year".into()],
            false,
        );
        assert_eq!(decision, SortDecision::Keep);
    }

    #[test]
    fn test_adaptive_yellow_strips_non_partition_sort() {
        let decision = decide_sort(
            SortMode::Adaptive,
            MemoryPressure::Yellow,
            &["name".into()],
            &["year".into()],
            false,
        );
        assert!(matches!(
            decision,
            SortDecision::Strip { reason: StripReason::MemoryPressure(MemoryPressure::Yellow), .. }
        ));
    }

    #[test]
    fn test_adaptive_orange_strips_non_partition_sort() {
        let decision = decide_sort(
            SortMode::Adaptive,
            MemoryPressure::Orange,
            &["name".into(), "city".into()],
            &["year".into()],
            false,
        );
        assert!(matches!(
            decision,
            SortDecision::Strip { reason: StripReason::MemoryPressure(MemoryPressure::Orange), .. }
        ));
        if let SortDecision::Strip { sort_columns, .. } = decision {
            assert_eq!(sort_columns, vec!["name", "city"]);
        }
    }

    #[test]
    fn test_adaptive_red_strips_non_partition_sort() {
        let decision = decide_sort(
            SortMode::Adaptive,
            MemoryPressure::Red,
            &["name".into()],
            &["year".into()],
            false,
        );
        assert!(matches!(
            decision,
            SortDecision::Strip { reason: StripReason::MemoryPressure(MemoryPressure::Red), .. }
        ));
    }

    // ── LIMIT handling ────────────────────────────────────────────────

    #[test]
    fn test_limit_always_keeps_regardless_of_mode() {
        // partition_only mode with LIMIT
        let decision = decide_sort(
            SortMode::PartitionOnly,
            MemoryPressure::Green,
            &["name".into()],
            &["year".into()],
            true, // has LIMIT
        );
        assert_eq!(decision, SortDecision::Keep);

        // adaptive mode under pressure with LIMIT
        let decision = decide_sort(
            SortMode::Adaptive,
            MemoryPressure::Red,
            &["name".into()],
            &["year".into()],
            true,
        );
        assert_eq!(decision, SortDecision::Keep);
    }

    // ── Edge cases ───────────────────────────────────────────────────

    #[test]
    fn test_empty_sort_columns_strips_in_non_strict() {
        // No sort columns means "not a partition sort"
        let decision = decide_sort(
            SortMode::PartitionOnly,
            MemoryPressure::Green,
            &[],
            &["year".into()],
            false,
        );
        assert!(matches!(decision, SortDecision::Strip { .. }));
    }

    #[test]
    fn test_no_partition_spec_strips_non_strict() {
        // Table has no partition columns
        let decision = decide_sort(
            SortMode::Adaptive,
            MemoryPressure::Yellow,
            &["name".into()],
            &[], // no partition columns
            false,
        );
        assert!(matches!(decision, SortDecision::Strip { .. }));
    }

    #[test]
    fn test_partial_partition_match_strips() {
        // Only some sort columns match partition columns
        let decision = decide_sort(
            SortMode::PartitionOnly,
            MemoryPressure::Green,
            &["year".into(), "name".into()], // name is not partition
            &["year".into(), "month".into()],
            false,
        );
        assert!(matches!(decision, SortDecision::Strip { .. }));
    }

    // ── Warning formatting ───────────────────────────────────────────

    #[test]
    fn test_format_sort_warning_none_for_keeps() {
        let decisions = vec![SortDecision::Keep];
        assert!(format_sort_warning(&decisions, SortMode::Adaptive).is_none());
    }

    #[test]
    fn test_format_sort_warning_includes_columns() {
        let decisions = vec![SortDecision::Strip {
            sort_columns: vec!["name".into(), "city".into()],
            reason: StripReason::MemoryPressure(MemoryPressure::Yellow),
        }];
        let warning = format_sort_warning(&decisions, SortMode::Adaptive).unwrap();
        assert!(warning.contains("name, city"));
        assert!(warning.contains("memory pressure"));
        assert!(warning.contains("sort_mode=adaptive"));
    }

    #[test]
    fn test_format_pressure_rejection_with_sort() {
        let msg = format_pressure_rejection(
            &["col1".into(), "col2".into()],
            MemoryPressure::Red,
        );
        assert!(msg.contains("ORDER BY [col1, col2]"));
        assert!(msg.contains("Remove ORDER BY"));
        assert!(msg.contains("Add LIMIT"));
        assert!(msg.contains("sort_mode=adaptive"));
    }

    #[test]
    fn test_format_pressure_rejection_without_sort() {
        let msg = format_pressure_rejection(&[], MemoryPressure::Red);
        assert!(msg.contains("retry later"));
        assert!(!msg.contains("ORDER BY"));
    }
}
```

- [ ] **Step 2: Register the module**

In `crates/sqe-coordinator/src/lib.rs`, add:

```rust
pub mod adaptive_sort;
```

- [ ] **Step 3: Run tests to verify they compile and pass**

Run: `cargo test -p sqe-coordinator -- adaptive_sort`
Expected: PASS — all `decide_sort` tests + formatting tests should pass

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-coordinator/src/adaptive_sort.rs crates/sqe-coordinator/src/lib.rs
git commit -m "feat: implement AdaptiveSortRule with decision matrix and warning formatting"
```

---

### Task 5: Wire `AdaptiveSortRule` into `execute_query()`

**Files:**
- Modify: `crates/sqe-coordinator/src/query_handler.rs`

- [ ] **Step 1: Import the adaptive sort module**

Add to the imports at the top of `query_handler.rs`:

```rust
use crate::adaptive_sort;
use sqe_core::SortMode;
```

- [ ] **Step 2: Apply adaptive sort after physical plan creation**

In the `execute_query` method (around line 596-602), after `create_physical_plan()` and before `try_distribute()`, insert the adaptive sort step:

Find this block:
```rust
        // Get the physical plan
        let physical_plan = enforced_df
            .create_physical_plan()
            .await
            .map_err(|e| SqeError::Execution(format!("Physical plan creation failed: {e}")))?;

        // Try to distribute scan work across workers
        let final_plan = self.try_distribute(physical_plan, session, query_id).await;
```

Replace with:
```rust
        // Get the physical plan
        let physical_plan = enforced_df
            .create_physical_plan()
            .await
            .map_err(|e| SqeError::Execution(format!("Physical plan creation failed: {e}")))?;

        // Apply adaptive sort stripping based on sort_mode config and memory pressure.
        // This runs BEFORE distribution (no point distributing a sort we're about to strip).
        let sort_mode = SortMode::from_str(&self.config.query.sort_mode);
        let pressure = crate::memory::check_pressure(&self.runtime.memory_pool);
        let (physical_plan, sort_decisions) = adaptive_sort::apply_adaptive_sort(
            physical_plan,
            sort_mode,
            pressure,
            self.metrics.as_ref(),
        );

        // Log sort stripping warning if any sorts were removed
        if let Some(warning) = adaptive_sort::format_sort_warning(&sort_decisions, sort_mode) {
            debug!(warning = %warning, "Adaptive sort stripping applied");
        }

        // Try to distribute scan work across workers
        let final_plan = self.try_distribute(physical_plan, session, query_id).await;
```

- [ ] **Step 3: Improve the memory pressure rejection message**

In the same `execute_query` method (around line 161-169), find the memory pressure rejection block:

```rust
        if !pressure.admits_new_query() {
            warn!(
                pressure = %pressure,
                username = %session.user.username,
                "Rejecting query due to memory pressure"
            );
            return Err(SqeError::Execution(
                "Server under memory pressure (>95% utilized). Please retry later.".to_string(),
            ));
        }
```

Replace with:
```rust
        if !pressure.admits_new_query() {
            warn!(
                pressure = %pressure,
                username = %session.user.username,
                "Rejecting query due to memory pressure"
            );
            // Try to extract sort columns from SQL for an actionable error message.
            // This is a best-effort parse — if it fails, we fall back to generic message.
            let sort_cols = extract_order_by_columns(sql);
            return Err(SqeError::Execution(
                adaptive_sort::format_pressure_rejection(&sort_cols, pressure),
            ));
        }
```

- [ ] **Step 4: Add the `extract_order_by_columns` helper**

Add this helper function at the bottom of `query_handler.rs` (before the tests module or at module level):

```rust
/// Best-effort extraction of ORDER BY column names from SQL text.
/// Used to provide actionable error messages when rejecting queries under memory pressure.
fn extract_order_by_columns(sql: &str) -> Vec<String> {
    // Simple regex-free approach: look for ORDER BY in the SQL text.
    let upper = sql.to_uppercase();
    if let Some(idx) = upper.rfind("ORDER BY") {
        let after = &sql[idx + 8..]; // skip "ORDER BY"
        // Take until LIMIT, OFFSET, ), ;, or end of string
        let end = after
            .find(|c: char| c == ')' || c == ';')
            .or_else(|| {
                let u = after.to_uppercase();
                u.find("LIMIT").or_else(|| u.find("OFFSET")).or_else(|| u.find("FETCH"))
            })
            .unwrap_or(after.len());
        let cols_str = &after[..end];
        cols_str
            .split(',')
            .map(|s| {
                s.trim()
                    .split_whitespace()
                    .next()
                    .unwrap_or("")
                    .to_string()
            })
            .filter(|s| !s.is_empty())
            .collect()
    } else {
        vec![]
    }
}
```

- [ ] **Step 5: Run full test suite to verify nothing broke**

Run: `cargo test --all`
Expected: All tests PASS

- [ ] **Step 6: Run clippy**

Run: `cargo clippy --all-targets --all-features -- -D warnings`
Expected: PASS with zero warnings

- [ ] **Step 7: Commit**

```bash
git add crates/sqe-coordinator/src/query_handler.rs
git commit -m "feat: wire AdaptiveSortRule into execute_query with actionable error messages"
```

---

### Task 6: Integration test — verify end-to-end sort stripping

**Files:**
- Modify: `crates/sqe-coordinator/src/adaptive_sort.rs`

- [ ] **Step 1: Add a physical plan integration test using DataFusion's SortExec + LazyMemoryExec**

Add this test to the `mod tests` section of `adaptive_sort.rs`:

```rust
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::memory::LazyMemoryExec;
    use datafusion::physical_plan::sorts::sort::SortExec;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("year", DataType::Int32, true),
        ]))
    }

    fn make_sort_plan(schema: &SchemaRef, col_name: &str, limit: Option<usize>) -> Arc<dyn ExecutionPlan> {
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(LazyMemoryExec::try_new(schema.clone(), vec![]).unwrap());
        let sort_expr = PhysicalSortExpr::new(
            col(col_name, schema).unwrap(),
            datafusion::arrow::compute::SortOptions::default(),
        );
        let ordering = LexOrdering::new(vec![sort_expr]).unwrap();
        let mut sort = SortExec::new(ordering, input);
        if let Some(n) = limit {
            sort = sort.with_fetch(Some(n));
        }
        Arc::new(sort)
    }

    #[test]
    fn test_apply_strips_sort_partition_only_mode() {
        let schema = test_schema();
        let plan = make_sort_plan(&schema, "name", None);

        // No IcebergScanExec in the tree, so partition_cols = [].
        // partition_only mode: non-partition sort should be stripped.
        let (result, decisions) = apply_adaptive_sort(
            plan,
            SortMode::PartitionOnly,
            MemoryPressure::Green,
            None,
        );

        // Result should NOT be a SortExec anymore
        assert!(
            result.as_any().downcast_ref::<SortExec>().is_none(),
            "SortExec should have been stripped"
        );
        assert_eq!(decisions.len(), 1);
        assert!(matches!(decisions[0], SortDecision::Strip { .. }));
    }

    #[test]
    fn test_apply_keeps_sort_strict_mode() {
        let schema = test_schema();
        let plan = make_sort_plan(&schema, "name", None);

        let (result, decisions) = apply_adaptive_sort(
            plan,
            SortMode::Strict,
            MemoryPressure::Red,
            None,
        );

        // Strict mode: SortExec should remain
        assert!(
            result.as_any().downcast_ref::<SortExec>().is_some(),
            "SortExec should be kept in strict mode"
        );
        assert!(decisions.is_empty());
    }

    #[test]
    fn test_apply_keeps_sort_with_limit() {
        let schema = test_schema();
        let plan = make_sort_plan(&schema, "name", Some(100));

        let (result, decisions) = apply_adaptive_sort(
            plan,
            SortMode::PartitionOnly,
            MemoryPressure::Red,
            None,
        );

        // LIMIT: SortExec should remain regardless
        assert!(
            result.as_any().downcast_ref::<SortExec>().is_some(),
            "SortExec with LIMIT should always be kept"
        );
        assert!(decisions.is_empty());
    }

    #[test]
    fn test_apply_adaptive_green_keeps_everything() {
        let schema = test_schema();
        let plan = make_sort_plan(&schema, "name", None);

        let (result, decisions) = apply_adaptive_sort(
            plan,
            SortMode::Adaptive,
            MemoryPressure::Green,
            None,
        );

        // Green pressure: keep all sorts
        assert!(
            result.as_any().downcast_ref::<SortExec>().is_some(),
            "SortExec should be kept at Green pressure"
        );
        assert!(decisions.is_empty());
    }

    #[test]
    fn test_apply_adaptive_yellow_strips_non_partition() {
        let schema = test_schema();
        let plan = make_sort_plan(&schema, "name", None);

        let (result, decisions) = apply_adaptive_sort(
            plan,
            SortMode::Adaptive,
            MemoryPressure::Yellow,
            None,
        );

        // Yellow + no partition columns => strip
        assert!(
            result.as_any().downcast_ref::<SortExec>().is_none(),
            "SortExec should be stripped at Yellow pressure with non-partition columns"
        );
        assert_eq!(decisions.len(), 1);
    }
```

- [ ] **Step 2: Run the integration tests**

Run: `cargo test -p sqe-coordinator -- adaptive_sort`
Expected: PASS

- [ ] **Step 3: Run full test + clippy**

Run: `cargo test --all && cargo clippy --all-targets --all-features -- -D warnings`
Expected: All tests PASS, zero clippy warnings

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-coordinator/src/adaptive_sort.rs
git commit -m "test: add physical plan integration tests for adaptive sort stripping"
```

---

### Task 7: Update docs and nextsteps

**Files:**
- Modify: `README.md`
- Modify: `nextsteps.md`

- [ ] **Step 1: Update README.md roadmap**

Find the performance/memory section in `README.md` and add a line:
```
- [x] Adaptive sort stripping (strict/partition_only/adaptive sort_mode, default: adaptive)
```

- [ ] **Step 2: Update nextsteps.md**

Mark the adaptive sort item as done and add any next steps.

- [ ] **Step 3: Commit**

```bash
git add README.md nextsteps.md
git commit -m "docs: update roadmap for adaptive sort stripping"
```
