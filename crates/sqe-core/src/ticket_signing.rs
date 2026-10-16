//! Asymmetric signing of coordinator-to-worker scan tickets.
//!
//! Review finding 601: the scan-ticket HMAC used the same shared secret that
//! travels in the `x-sqe-worker-secret` header of every request, so anyone
//! holding the header value (every worker pod, every operator with read
//! access to a worker's config) could also mint tickets, and a compromised
//! worker could harvest every user's storage credentials by heartbeating an
//! attacker URL and receiving tickets. With Ed25519 only the coordinator
//! holds the signing key; workers hold the public half and can verify but not
//! forge. The public key reaches workers over the secret-authenticated
//! heartbeat response (or is pinned statically via `worker.ticket_public_key`).
//!
//! Signatures are carried as `ed25519:<hex>` so a verifier can tell them apart
//! from the legacy `<hex>` HMAC tag during migration.

use std::fmt;

use base64::Engine as _;
use ring::rand::SystemRandom;
use ring::signature::{Ed25519KeyPair, KeyPair, UnparsedPublicKey, ED25519};

const SIGNATURE_PREFIX: &str = "ed25519:";
const B64: base64::engine::general_purpose::GeneralPurpose =
    base64::engine::general_purpose::STANDARD;

#[derive(Debug, thiserror::Error)]
pub enum TicketKeyError {
    #[error("ticket signing key: could not generate an Ed25519 key pair")]
    Generate,
    #[error("ticket signing key: not a valid PKCS#8 v2 Ed25519 key ({0})")]
    InvalidPkcs8(String),
    #[error("ticket public key: not valid base64 ({0})")]
    InvalidBase64(String),
    #[error("ticket public key: expected 32 bytes, got {0}")]
    InvalidLength(usize),
}

/// Coordinator-side signer. Holds the private key; never leaves the process.
pub struct TicketSigner {
    key: Ed25519KeyPair,
    public_b64: String,
}

impl fmt::Debug for TicketSigner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TicketSigner")
            .field("public_key", &self.public_b64)
            .finish_non_exhaustive()
    }
}

impl TicketSigner {
    /// Fresh ephemeral key pair. Workers learn the public half from the
    /// heartbeat response, so a coordinator restart simply re-pins them.
    pub fn generate() -> Result<Self, TicketKeyError> {
        let rng = SystemRandom::new();
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).map_err(|_| TicketKeyError::Generate)?;
        Self::from_pkcs8(pkcs8.as_ref())
    }

    /// Load a PKCS#8 v2 Ed25519 key (DER bytes). Use this when several
    /// coordinators must sign tickets the same workers accept.
    pub fn from_pkcs8(der: &[u8]) -> Result<Self, TicketKeyError> {
        let key = Ed25519KeyPair::from_pkcs8_maybe_unchecked(der)
            .map_err(|e| TicketKeyError::InvalidPkcs8(e.to_string()))?;
        let public_b64 = B64.encode(key.public_key().as_ref());
        Ok(Self { key, public_b64 })
    }

    /// Generate a PKCS#8 v2 DER blob suitable for `from_pkcs8`, for operators
    /// provisioning a shared key file.
    pub fn generate_pkcs8_der() -> Result<Vec<u8>, TicketKeyError> {
        let rng = SystemRandom::new();
        Ed25519KeyPair::generate_pkcs8(&rng)
            .map(|d| d.as_ref().to_vec())
            .map_err(|_| TicketKeyError::Generate)
    }

    /// Base64 (standard, padded) of the 32-byte public key.
    pub fn public_key_b64(&self) -> &str {
        &self.public_b64
    }

    /// Verifier for this signer's public key.
    pub fn verifier(&self) -> TicketVerifier {
        TicketVerifier {
            public: self.key.public_key().as_ref().to_vec(),
        }
    }

    /// Sign `bytes`; the result goes in the scan-signature header.
    pub fn sign(&self, bytes: &[u8]) -> String {
        let sig = self.key.sign(bytes);
        format!("{SIGNATURE_PREFIX}{}", hex_encode(sig.as_ref()))
    }
}

/// Worker-side verifier. Holds only the public key.
#[derive(Clone, PartialEq, Eq)]
pub struct TicketVerifier {
    public: Vec<u8>,
}

impl fmt::Debug for TicketVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TicketVerifier")
            .field("public_key", &B64.encode(&self.public))
            .finish()
    }
}

impl TicketVerifier {
    pub fn from_b64(public_b64: &str) -> Result<Self, TicketKeyError> {
        let public = B64
            .decode(public_b64.trim())
            .map_err(|e| TicketKeyError::InvalidBase64(e.to_string()))?;
        if public.len() != 32 {
            return Err(TicketKeyError::InvalidLength(public.len()));
        }
        Ok(Self { public })
    }

    pub fn public_key_b64(&self) -> String {
        B64.encode(&self.public)
    }

    /// True when `signature` is an `ed25519:` tag (as opposed to a legacy
    /// HMAC hex tag).
    pub fn is_asymmetric_signature(signature: &str) -> bool {
        signature.starts_with(SIGNATURE_PREFIX)
    }

    /// Verify an `ed25519:<hex>` signature over `bytes`.
    pub fn verify(&self, bytes: &[u8], signature: &str) -> bool {
        let Some(hex) = signature.strip_prefix(SIGNATURE_PREFIX) else {
            return false;
        };
        let Some(sig) = hex_decode(hex) else {
            return false;
        };
        UnparsedPublicKey::new(&ED25519, &self.public)
            .verify(bytes, &sig)
            .is_ok()
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn hex_decode(hex: &str) -> Option<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_and_verify_round_trip() {
        let signer = TicketSigner::generate().unwrap();
        let verifier = TicketVerifier::from_b64(signer.public_key_b64()).unwrap();
        let sig = signer.sign(b"ticket bytes");
        assert!(TicketVerifier::is_asymmetric_signature(&sig));
        assert!(verifier.verify(b"ticket bytes", &sig));
        assert!(!verifier.verify(b"ticket bytes tampered", &sig));
        assert_eq!(verifier, signer.verifier());
    }

    /// 601: a party holding only the public key (every worker) cannot mint a
    /// ticket another verifier accepts, and a second coordinator's key does
    /// not verify tickets signed by the first.
    #[test]
    fn other_keys_and_legacy_tags_do_not_verify() {
        let signer = TicketSigner::generate().unwrap();
        let other = TicketSigner::generate().unwrap();
        let verifier = signer.verifier();
        assert!(!verifier.verify(b"x", &other.sign(b"x")));
        // Legacy HMAC-shaped tag (no prefix) is never accepted here.
        assert!(!verifier.verify(b"x", &hex_encode(&[0u8; 32])));
        assert!(!verifier.verify(b"x", "ed25519:zz"));
    }

    #[test]
    fn pkcs8_round_trip_gives_same_public_key() {
        let der = TicketSigner::generate_pkcs8_der().unwrap();
        let a = TicketSigner::from_pkcs8(&der).unwrap();
        let b = TicketSigner::from_pkcs8(&der).unwrap();
        assert_eq!(a.public_key_b64(), b.public_key_b64());
        assert!(b.verifier().verify(b"t", &a.sign(b"t")));
        assert!(TicketSigner::from_pkcs8(b"garbage").is_err());
    }

    #[test]
    fn verifier_rejects_malformed_public_keys() {
        assert!(matches!(
            TicketVerifier::from_b64("not base64!"),
            Err(TicketKeyError::InvalidBase64(_))
        ));
        assert!(matches!(
            TicketVerifier::from_b64(&B64.encode([1u8; 16])),
            Err(TicketKeyError::InvalidLength(16))
        ));
    }
}
