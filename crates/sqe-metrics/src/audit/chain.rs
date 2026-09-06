//! Tamper-evident [`HashChain`] linking consecutive audit records so gaps or
//! edits are detectable.

use super::event::AuditEvent;
use sha2::{Digest, Sha256};

const GENESIS: &str = "0000000000000000000000000000000000000000000000000000000000000000";

pub struct HashChain {
    next_seq: u64,
    prev_hash: String,
}

impl HashChain {
    pub fn new() -> Self {
        Self {
            next_seq: 0,
            prev_hash: GENESIS.to_string(),
        }
    }

    /// Continue a chain from the last durable record.
    ///
    /// `last_seq` / `last_hash` are `integrity.seq` and `integrity.hash` of
    /// the last parseable line in the sink file. The next stamped event is
    /// `last_seq + 1` with `prev_hash = last_hash`, so a file that spans
    /// process restarts remains one verifiable chain.
    pub fn resume(last_seq: u64, last_hash: impl Into<String>) -> Self {
        Self {
            next_seq: last_seq.saturating_add(1),
            prev_hash: last_hash.into(),
        }
    }

    /// Start a new segment after an unparseable tail.
    ///
    /// `prev_hash` is `BROKEN:<reason>` so a verifier can distinguish a
    /// crash/torn write from silent truncation back to a genesis boundary.
    pub fn resume_broken(reason: &str) -> Self {
        Self {
            next_seq: 0,
            prev_hash: format!("BROKEN:{reason}"),
        }
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
    hex::encode(hasher.finalize())
}

#[derive(Debug)]
pub enum ChainError {
    SeqGap { expected: u64, found: u64 },
    BrokenLink { seq: u64 },
    BadHash { seq: u64 },
}

/// Verify a contiguous chain of audit events.
///
/// The first event may start at genesis (`seq = 0`, `prev_hash = GENESIS`)
/// or continue a resumed segment. Subsequent events must have `seq` one
/// greater than the previous and `prev_hash` equal to the previous hash.
/// A `BROKEN:` prev_hash on the first event is accepted as an explicit
/// restart marker; a `BROKEN:` link in the middle is a [`ChainError::BrokenLink`].
pub fn verify_chain(events: &[AuditEvent]) -> Result<(), ChainError> {
    let Some(first) = events.first() else {
        return Ok(());
    };
    let mut prev = first.integrity.prev_hash.clone();
    let mut expected_seq = first.integrity.seq;
    // A chain that claims to start at seq 0 must be rooted at genesis or an
    // explicit BROKEN marker. Anything else is a broken link at seq 0.
    if expected_seq == 0 && prev != GENESIS && !prev.starts_with("BROKEN:") {
        return Err(ChainError::BrokenLink { seq: 0 });
    }
    for ev in events {
        if ev.integrity.seq != expected_seq {
            return Err(ChainError::SeqGap {
                expected: expected_seq,
                found: ev.integrity.seq,
            });
        }
        if ev.integrity.prev_hash != prev {
            return Err(ChainError::BrokenLink {
                seq: ev.integrity.seq,
            });
        }
        if compute_hash(ev) != ev.integrity.hash {
            return Err(ChainError::BadHash {
                seq: ev.integrity.seq,
            });
        }
        prev = ev.integrity.hash.clone();
        expected_seq = expected_seq.saturating_add(1);
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

    /// The chain hash must stay a 64-character, fully zero-padded lowercase hex
    /// string.
    ///
    /// `compute_hash` used to render the digest with `format!("{:x}", ..)`,
    /// which sha2 0.11 no longer supports (it returns `Array<u8, N>` instead of
    /// `GenericArray`). The replacement is `hex::encode`. The digest bytes are
    /// unchanged either way, so previously written chains still verify -- but a
    /// hand-rolled per-byte `{:x}` would silently drop leading zeros (0x0a ->
    /// "a" instead of "0a"), shortening roughly one byte in sixteen and
    /// breaking verification only for some records. Assert the invariant that
    /// rules that out.
    #[test]
    fn chain_hash_is_padded_lowercase_hex() {
        let mut chain = HashChain::new();
        let mut event = sample_query_event();
        chain.stamp(&mut event);
        let hash = &event.integrity.hash;

        assert_eq!(
            hash.len(),
            64,
            "sha256 must render as exactly 64 hex chars (32 bytes x 2), got {}: {hash}",
            hash.len()
        );
        assert!(
            hash.chars()
                .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c)),
            "hash must be lowercase hex, got {hash}"
        );
    }

    /// Guards the zero-padding property directly, independent of whatever the
    /// sample event happens to hash to: every byte value must occupy two hex
    /// characters, including those below 0x10.
    #[test]
    fn hex_encoding_zero_pads_every_byte() {
        assert_eq!(hex::encode([0x0a_u8]), "0a");
        assert_eq!(hex::encode([0x00_u8, 0x01, 0x0f]), "00010f");
        assert_eq!(hex::encode([0u8; 32]).len(), 64);
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
        chain.stamp(&mut a);
        chain.stamp(&mut b);
        chain.stamp(&mut c);
        // Dropping b leaves a seq gap and a broken prev_hash link.
        assert!(verify_chain(&[a, c]).is_err());
    }

    #[test]
    fn resume_continues_seq_and_hash() {
        let mut chain = HashChain::new();
        let mut a = sample_query_event();
        chain.stamp(&mut a);
        let mut resumed = HashChain::resume(a.integrity.seq, a.integrity.hash.clone());
        let mut b = sample_query_event();
        resumed.stamp(&mut b);
        assert_eq!(b.integrity.seq, 1);
        assert_eq!(b.integrity.prev_hash, a.integrity.hash);
        verify_chain(&[a, b]).unwrap();
    }

    #[test]
    fn resume_broken_is_explicit_not_genesis() {
        let mut chain = HashChain::resume_broken("unparseable tail");
        let mut ev = sample_query_event();
        chain.stamp(&mut ev);
        assert!(ev.integrity.prev_hash.starts_with("BROKEN:"));
        assert_ne!(ev.integrity.prev_hash, GENESIS);
        verify_chain(&[ev]).unwrap();
    }
}
