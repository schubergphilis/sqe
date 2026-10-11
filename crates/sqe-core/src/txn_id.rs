//! Explicit transaction ids (#824).
//!
//! An open transaction lives in one coordinator process's memory. Its id
//! carries that process's incarnation, a random 32-bit value drawn once at
//! start, in the first eight hex digits of a v4 UUID. A coordinator handed
//! an id with another incarnation (the client was routed to another
//! coordinator, or this one restarted) refuses it instead of starting a
//! fresh transaction that silently lacks the earlier writes.
//!
//! The id stays a valid UUID: it becomes the branch name `sqe-txn-<id>`.

use std::sync::OnceLock;

use uuid::Uuid;

/// This process's incarnation, drawn on first use.
pub fn incarnation() -> u32 {
    static INCARNATION: OnceLock<u32> = OnceLock::new();
    *INCARNATION.get_or_init(|| (Uuid::new_v4().as_u128() >> 96) as u32)
}

/// A new transaction id owned by this process.
pub fn new_txn_id() -> String {
    with_incarnation(Uuid::new_v4(), incarnation()).to_string()
}

/// Whether `id` was minted by this process. False for anything that is
/// not a UUID.
pub fn is_local_txn_id(id: &str) -> bool {
    Uuid::parse_str(id).is_ok_and(|u| incarnation_of(u) == incarnation())
}

fn with_incarnation(id: Uuid, incarnation: u32) -> Uuid {
    // The top 32 bits hold no version or variant bits.
    Uuid::from_u128((id.as_u128() & (u128::MAX >> 32)) | (u128::from(incarnation) << 96))
}

fn incarnation_of(id: Uuid) -> u32 {
    (id.as_u128() >> 96) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_v4_uuids_carrying_the_incarnation() {
        let id = new_txn_id();
        let parsed = Uuid::parse_str(&id).unwrap();
        assert_eq!(parsed.get_version_num(), 4);
        assert_eq!(parsed.get_variant(), uuid::Variant::RFC4122);
        assert_eq!(&id[..8], format!("{:08x}", incarnation()));
        assert!(is_local_txn_id(&id));
        assert_ne!(new_txn_id(), id);
    }

    #[test]
    fn another_incarnation_is_not_local() {
        let foreign = with_incarnation(Uuid::new_v4(), incarnation().wrapping_add(1));
        assert!(!is_local_txn_id(&foreign.to_string()));
        assert!(!is_local_txn_id("../main"));
    }
}
