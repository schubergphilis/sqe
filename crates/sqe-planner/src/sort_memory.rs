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

use sqe_spill::{split_default_read_headroom, MemoryGrant};

/// Default max runs merged in one pass. Higher fan-in needs more simultaneous
/// buffer reservations; keep this modest for laptop 64 MiB workers.
pub const DEFAULT_SORT_MERGE_FANIN: usize = 8;

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

    /// Whether a sort can complete under this policy without unbounded merge
    /// reservations: the merge headroom must hold `max_fan_in` buffers of at
    /// least 64 KiB each.
    pub fn can_admit(&self) -> Result<(), SortAdmissionError> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use sqe_spill::MemoryGrant;

    #[test]
    fn admit_rejects_tiny_grant() {
        let grant = MemoryGrant::new("tiny", 1024);
        let mut policy = SortMemoryPolicy::from_grant(grant);
        policy.max_fan_in = 8;
        let err = policy.can_admit().unwrap_err();
        assert!(matches!(
            err,
            SortAdmissionError::InsufficientMergeHeadroom { .. }
        ));
    }

    #[test]
    fn admit_accepts_reasonable_grant() {
        let grant = MemoryGrant::new("ok", 8 * 1024 * 1024);
        let policy = SortMemoryPolicy::from_grant(grant);
        assert!(policy.can_admit().is_ok());
    }
}
