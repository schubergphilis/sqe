//! The worker's pinned copy of the coordinator's scan-ticket public key
//! (review finding 601).
//!
//! Workers never hold a signing key. They learn the public half either from
//! `worker.ticket_public_key` (static pin, never re-pinned) or from the
//! heartbeat response, which travels over the `worker_secret`-authenticated
//! channel to the coordinator the worker was configured to trust. A
//! coordinator restart rotates the ephemeral key; the next heartbeat re-pins.

use std::sync::RwLock;

use sqe_core::config::WorkerConfig;
use sqe_core::ticket_signing::{TicketKeyError, TicketVerifier};
use tracing::{info, warn};

#[derive(Debug)]
pub struct TicketKeyPin {
    inner: RwLock<Option<TicketVerifier>>,
    /// Set when the key came from config: heartbeat-delivered keys are then
    /// ignored (and logged when they differ).
    locked: bool,
}

impl TicketKeyPin {
    /// Empty, re-pinnable pin.
    pub fn floating() -> Self {
        Self {
            inner: RwLock::new(None),
            locked: false,
        }
    }

    /// Static pin from `worker.ticket_public_key` when set, otherwise floating.
    pub fn from_config(config: &WorkerConfig) -> Result<Self, TicketKeyError> {
        let key = config.ticket_public_key.trim();
        if key.is_empty() {
            return Ok(Self::floating());
        }
        let verifier = TicketVerifier::from_b64(key)?;
        info!(
            public_key = %verifier.public_key_b64(),
            "Scan-ticket public key pinned from worker.ticket_public_key"
        );
        Ok(Self {
            inner: RwLock::new(Some(verifier)),
            locked: true,
        })
    }

    /// Record the public key a heartbeat response delivered. Returns `true`
    /// when the pin changed. A locked (config) pin never changes.
    pub fn pin_b64(&self, public_key_b64: &str) -> Result<bool, TicketKeyError> {
        let verifier = TicketVerifier::from_b64(public_key_b64)?;
        let mut guard = self.inner.write().unwrap_or_else(|p| p.into_inner());
        if self.locked {
            if guard.as_ref() != Some(&verifier) {
                warn!(
                    "coordinator heartbeat offered a scan-ticket key that differs from \
                     worker.ticket_public_key; keeping the configured pin"
                );
            }
            return Ok(false);
        }
        if guard.as_ref() == Some(&verifier) {
            return Ok(false);
        }
        info!(
            public_key = %verifier.public_key_b64(),
            rotated = guard.is_some(),
            "Pinned coordinator scan-ticket public key from heartbeat"
        );
        *guard = Some(verifier);
        Ok(true)
    }

    pub fn verifier(&self) -> Option<TicketVerifier> {
        self.inner.read().unwrap_or_else(|p| p.into_inner()).clone()
    }

    pub fn is_locked(&self) -> bool {
        self.locked
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqe_core::ticket_signing::TicketSigner;

    #[test]
    fn floating_pin_takes_and_rotates_heartbeat_keys() {
        let pin = TicketKeyPin::floating();
        assert!(pin.verifier().is_none());
        let a = TicketSigner::generate().unwrap();
        let b = TicketSigner::generate().unwrap();
        assert!(pin.pin_b64(a.public_key_b64()).unwrap());
        assert!(
            !pin.pin_b64(a.public_key_b64()).unwrap(),
            "same key is a no-op"
        );
        assert!(pin.verifier().unwrap().verify(b"t", &a.sign(b"t")));
        assert!(pin.pin_b64(b.public_key_b64()).unwrap(), "restart rotates");
        assert!(!pin.verifier().unwrap().verify(b"t", &a.sign(b"t")));
    }

    /// 601: a configured pin is authoritative; a heartbeat cannot swap it.
    #[test]
    fn configured_pin_ignores_heartbeat_keys() {
        let a = TicketSigner::generate().unwrap();
        let b = TicketSigner::generate().unwrap();
        let cfg = WorkerConfig {
            ticket_public_key: a.public_key_b64().to_string(),
            ..Default::default()
        };
        let pin = TicketKeyPin::from_config(&cfg).unwrap();
        assert!(pin.is_locked());
        assert!(!pin.pin_b64(b.public_key_b64()).unwrap());
        assert!(pin.verifier().unwrap().verify(b"t", &a.sign(b"t")));
        assert!(TicketKeyPin::from_config(&WorkerConfig {
            ticket_public_key: "nope".to_string(),
            ..Default::default()
        })
        .is_err());
    }
}
