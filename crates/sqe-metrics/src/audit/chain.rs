//! Tamper-evident [`HashChain`] linking consecutive audit records so gaps or
//! edits are detectable.

use sha2::{Digest, Sha256};
use super::event::AuditEvent;

const GENESIS: &str = "0000000000000000000000000000000000000000000000000000000000000000";

pub struct HashChain {
    next_seq: u64,
    prev_hash: String,
}

impl HashChain {
    pub fn new() -> Self {
        Self { next_seq: 0, prev_hash: GENESIS.to_string() }
    }

    /// Return the sequence number that will be assigned to the next record.
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// Return the hash of the last committed record (or the genesis sentinel).
    pub fn current_prev_hash(&self) -> &str {
        &self.prev_hash
    }

    /// Advance the chain: record `hash` as the new tip and increment the sequence.
    /// Used by callers that compute the hash externally (e.g., for legacy records
    /// that are not `AuditEvent`-shaped).
    pub fn advance(&mut self, hash: String) {
        self.prev_hash = hash;
        self.next_seq += 1;
    }

    pub fn stamp(&mut self, event: &mut AuditEvent) {
        event.integrity.seq = self.next_seq;
        event.integrity.prev_hash = self.prev_hash.clone();
        event.integrity.hash = compute_hash(event);
        self.prev_hash = event.integrity.hash.clone();
        self.next_seq += 1;
    }
}

impl Default for HashChain {
    fn default() -> Self {
        Self::new()
    }
}

/// Hash of the event with its own `hash` field blanked, chained on `prev_hash`.
/// Formula: sha256(prev_hash || canonical_json_with_blank_hash)
fn compute_hash(event: &AuditEvent) -> String {
    let mut clone = event.clone();
    clone.integrity.hash = String::new();
    let body = serde_json::to_string(&clone).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(clone.integrity.prev_hash.as_bytes());
    hasher.update(body.as_bytes());
    format!("{:x}", hasher.finalize())
}

#[derive(Debug)]
pub enum ChainError {
    SeqGap { expected: u64, found: u64 },
    BrokenLink { seq: u64 },
    BadHash { seq: u64 },
}

/// Verify a contiguous chain of audit events starting at seq=0.
/// Returns an error if any record has an unexpected sequence number,
/// a broken prev_hash link, or a hash that no longer matches the event body.
pub fn verify_chain(events: &[AuditEvent]) -> Result<(), ChainError> {
    let mut prev = GENESIS.to_string();
    for (i, ev) in events.iter().enumerate() {
        let expected_seq = i as u64;
        if ev.integrity.seq != expected_seq {
            return Err(ChainError::SeqGap { expected: expected_seq, found: ev.integrity.seq });
        }
        if ev.integrity.prev_hash != prev {
            return Err(ChainError::BrokenLink { seq: ev.integrity.seq });
        }
        if compute_hash(ev) != ev.integrity.hash {
            return Err(ChainError::BadHash { seq: ev.integrity.seq });
        }
        prev = ev.integrity.hash.clone();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::sample_query_event;

    #[test]
    fn chain_is_well_ordered_and_verifies() {
        let mut chain = HashChain::new();
        let mut a = sample_query_event();
        let mut b = sample_query_event();
        chain.stamp(&mut a);
        chain.stamp(&mut b);
        assert_eq!(a.integrity.seq, 0);
        assert_eq!(b.integrity.seq, 1);
        assert_eq!(b.integrity.prev_hash, a.integrity.hash);
        verify_chain(&[a, b]).unwrap();
    }

    #[test]
    fn tampered_record_fails_verification() {
        let mut chain = HashChain::new();
        let mut a = sample_query_event();
        let mut b = sample_query_event();
        chain.stamp(&mut a);
        chain.stamp(&mut b);
        a.actor.username = "mallory".into(); // tamper after stamping
        assert!(verify_chain(&[a, b]).is_err());
    }

    #[test]
    fn truncated_tail_is_detectable_via_seq_gap() {
        let mut chain = HashChain::new();
        let mut a = sample_query_event();
        let mut b = sample_query_event();
        let mut c = sample_query_event();
        chain.stamp(&mut a); chain.stamp(&mut b); chain.stamp(&mut c);
        // Dropping b leaves a seq gap and a broken prev_hash link.
        assert!(verify_chain(&[a, c]).is_err());
    }
}
