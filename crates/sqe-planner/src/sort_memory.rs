//! Sort memory admission and merge fan-in caps (Phase 6).
//!
//! DataFusion's `ExternalSorterMerge` consumer is created with `can_spill=false`
//! (DF 54 `sorts/sort.rs`). Unbounded merge fan-in therefore hard-OOMs instead
//! of spilling. SQE admits sort work only when protected merge headroom fits
//! under the sort grant, and caps fan-in so recursive multi-pass merge stays
//! within that headroom.
//!
//! This module is the gate + policy. Wiring into DF sort construction lands
//! when the worker governor (Phase 7) owns the FairSpillPool replacement.

use sqe_spill::{MemoryGrant, ReclaimableConsumer, split_default_read_headroom};

/// Default max runs merged in one pass. Higher fan-in needs more simultaneous
/// buffer reservations; keep this modest for laptop 64 MiB workers.
pub const DEFAULT_SORT_MERGE_FANIN: usize = 8;

/// Minimum reserved merge headroom as a fraction of the sort grant (25%).
pub const DEFAULT_MERGE_HEADROOM_NUM: usize = 1;
pub const DEFAULT_MERGE_HEADROOM_DEN: usize = 4;

/// Policy for one sort operator under a fixed grant.
#[derive(Debug, Clone)]
pub struct SortMemoryPolicy {
    pub grant: MemoryGrant,
    pub max_fan_in: usize,
}

impl SortMemoryPolicy {
    pub fn from_grant(grant: MemoryGrant) -> Self {
        Self {
            grant,
            max_fan_in: DEFAULT_SORT_MERGE_FANIN,
        }
    }

    /// Bytes reserved exclusively for the merge phase (cannot be used by run
    /// creation). Mirrors spill-read headroom: writers (run creation) get the
    /// remainder.
    pub fn merge_headroom_bytes(&self) -> usize {
        let (_, headroom) = split_default_read_headroom(self.grant.capacity_bytes());
        headroom
    }

    /// Bytes available for creating sorted runs before merge.
    pub fn run_budget_bytes(&self) -> usize {
        let (writer, _) = split_default_read_headroom(self.grant.capacity_bytes());
        writer
    }

    /// Per-run merge buffer size given current fan-in.
    pub fn per_run_merge_buffer(&self) -> usize {
        let fan = self.max_fan_in.max(1);
        (self.merge_headroom_bytes() / fan).max(1)
    }

    /// Whether admitting a sort whose estimated input is `input_bytes` can
    /// complete under this policy without unbounded merge reservations.
    ///
    /// Conservative: require that the merge headroom alone can hold
    /// `max_fan_in` buffers of at least 64 KiB each.
    pub fn can_admit(&self, _input_bytes: usize) -> Result<(), SortAdmissionError> {
        let min_buffer: usize = 64 * 1024;
        let need = min_buffer.saturating_mul(self.max_fan_in.max(1));
        if self.merge_headroom_bytes() < need {
            return Err(SortAdmissionError::InsufficientMergeHeadroom {
                headroom: self.merge_headroom_bytes(),
                need,
                fan_in: self.max_fan_in,
            });
        }
        Ok(())
    }

    /// Number of merge passes required to reduce `num_runs` with capped fan-in.
    pub fn merge_passes(&self, num_runs: usize) -> usize {
        if num_runs <= 1 {
            return 0;
        }
        let fan = self.max_fan_in.max(2);
        let mut runs = num_runs;
        let mut passes = 0;
        while runs > 1 {
            runs = runs.div_ceil(fan);
            passes += 1;
        }
        passes
    }
}

/// Typed admission failure — never escalate to OS OOM.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SortAdmissionError {
    InsufficientMergeHeadroom {
        headroom: usize,
        need: usize,
        fan_in: usize,
    },
}

impl std::fmt::Display for SortAdmissionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InsufficientMergeHeadroom {
                headroom,
                need,
                fan_in,
            } => write!(
                f,
                "sort merge headroom {headroom} bytes < {need} required for fan-in {fan_in}; \
                 raise operator_budget or lower sort concurrency"
            ),
        }
    }
}

impl std::error::Error for SortAdmissionError {}

/// Reclaimable consumer for sort operators.
pub struct SortConsumer {
    name: String,
    desired: usize,
    minimum: usize,
}

impl SortConsumer {
    pub fn new(name: impl Into<String>, desired: usize, minimum: usize) -> Self {
        Self {
            name: name.into(),
            desired: desired.max(1),
            minimum: minimum.max(1).min(desired.max(1)),
        }
    }
}

impl ReclaimableConsumer for SortConsumer {
    fn name(&self) -> &str {
        &self.name
    }
    fn desired_bytes(&self) -> usize {
        self.desired
    }
    fn minimum_bytes(&self) -> usize {
        self.minimum
    }
    fn try_reclaim(&self, _target: usize) -> usize {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqe_spill::MemoryGrant;

    #[test]
    fn headroom_split_leaves_run_budget() {
        let grant = MemoryGrant::new("sort", 4 * 1024 * 1024);
        let policy = SortMemoryPolicy::from_grant(grant);
        assert!(policy.merge_headroom_bytes() > 0);
        assert!(policy.run_budget_bytes() > policy.merge_headroom_bytes());
        assert_eq!(
            policy.run_budget_bytes() + policy.merge_headroom_bytes(),
            policy.grant.capacity_bytes()
        );
    }

    #[test]
    fn admit_rejects_tiny_grant() {
        let grant = MemoryGrant::new("tiny", 1024);
        let mut policy = SortMemoryPolicy::from_grant(grant);
        policy.max_fan_in = 8;
        let err = policy.can_admit(10_000_000).unwrap_err();
        assert!(matches!(
            err,
            SortAdmissionError::InsufficientMergeHeadroom { .. }
        ));
    }

    #[test]
    fn admit_accepts_reasonable_grant() {
        let grant = MemoryGrant::new("ok", 8 * 1024 * 1024);
        let policy = SortMemoryPolicy::from_grant(grant);
        assert!(policy.can_admit(100_000_000).is_ok());
    }

    #[test]
    fn merge_passes_caps_fan_in() {
        let grant = MemoryGrant::new("s", 8 * 1024 * 1024);
        let mut policy = SortMemoryPolicy::from_grant(grant);
        policy.max_fan_in = 4;
        // 100 runs with fan-in 4: 100→25→7→2→1 = 4 passes
        assert_eq!(policy.merge_passes(100), 4);
        assert_eq!(policy.merge_passes(1), 0);
        assert_eq!(policy.merge_passes(4), 1);
    }
}
