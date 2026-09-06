//! Destination and result-size policy for outbound Quack calls.
//!
//! `quack_query()` turns SQL into an outbound HTTP POST from the coordinator,
//! which holds the most privileged network position in a deployment
//! (catalog, object store, policy backend, IdP, cloud metadata). Without a
//! gate it is a server-side request forgery primitive for every
//! authenticated user (review finding 604). The policy here is the gate:
//! an exact `host:port` allow-list checked before any socket is opened, and
//! a byte cap enforced while the reply is decoded.

use std::net::IpAddr;

use url::Url;

use crate::ClientError;

/// Default cap on the decoded Arrow bytes one `quack_query()` call may hold.
pub const DEFAULT_MAX_RESULT_BYTES: usize = 64 * 1024 * 1024;

/// What a Quack client is allowed to reach and how much it may buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuackTargetPolicy {
    /// Exact `host:port` entries (host compared case-insensitively). An empty
    /// list allows nothing.
    pub allowed_hosts: Vec<String>,
    /// Maximum decoded result size across all batches of one call.
    pub max_result_bytes: usize,
}

impl Default for QuackTargetPolicy {
    /// Deny every destination. The TVF must be handed an explicit policy.
    fn default() -> Self {
        Self {
            allowed_hosts: Vec::new(),
            max_result_bytes: DEFAULT_MAX_RESULT_BYTES,
        }
    }
}

impl QuackTargetPolicy {
    pub fn new(allowed_hosts: Vec<String>, max_result_bytes: usize) -> Self {
        Self {
            allowed_hosts: allowed_hosts
                .into_iter()
                .map(|h| h.trim().to_ascii_lowercase())
                .filter(|h| !h.is_empty())
                .collect(),
            max_result_bytes,
        }
    }

    /// Allow any destination. For direct Rust callers that construct their
    /// own [`crate::QuackClient`] and are not driven by user SQL. Never hand
    /// this to the TVF.
    pub fn permissive() -> Self {
        Self {
            allowed_hosts: vec![ANY_HOST.to_string()],
            max_result_bytes: usize::MAX,
        }
    }

    /// Reject `url` unless its `host:port` is listed. Literal loopback,
    /// link-local (the cloud metadata range), private and unspecified
    /// addresses are refused unless that exact `host:port` is listed, and
    /// an empty allow-list refuses everything. Runs before any connection.
    pub fn check_destination(&self, url: &Url) -> Result<(), ClientError> {
        if self.allowed_hosts.iter().any(|h| h == ANY_HOST) {
            return Ok(());
        }
        let host = url
            .host_str()
            .ok_or_else(|| ClientError::DestinationDenied(format!("{url} has no host")))?
            .trim_matches(['[', ']'])
            .to_ascii_lowercase();
        let port = url
            .port_or_known_default()
            .ok_or_else(|| ClientError::DestinationDenied(format!("{url} has no port")))?;
        let key = format!("{host}:{port}");
        if self.allowed_hosts.is_empty() {
            return Err(ClientError::DestinationDenied(format!(
                "{key}: no quack_client.allowed_hosts configured"
            )));
        }
        if self.allowed_hosts.contains(&key) {
            return Ok(());
        }
        let reason = match host.parse::<IpAddr>() {
            Ok(ip) if is_internal(ip) => "internal address not on the allow-list",
            _ => "not on quack_client.allowed_hosts",
        };
        Err(ClientError::DestinationDenied(format!("{key}: {reason}")))
    }

    /// Fold `bytes` into `seen`, failing once the cap is exceeded.
    pub fn charge(&self, seen: &mut usize, bytes: usize) -> Result<(), ClientError> {
        *seen = seen.saturating_add(bytes);
        if *seen > self.max_result_bytes {
            return Err(ClientError::ResultTooLarge {
                cap: self.max_result_bytes,
                seen: *seen,
            });
        }
        Ok(())
    }
}

const ANY_HOST: &str = "*";

fn is_internal(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback()
                || v4.is_link_local()
                || v4.is_private()
                || v4.is_unspecified()
                || v4.is_broadcast()
                || v4.octets()[0] == 100 && (64..128).contains(&v4.octets()[1])
        }
        IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unspecified()
                || (v6.segments()[0] & 0xffc0) == 0xfe80
                || (v6.segments()[0] & 0xfe00) == 0xfc00
                || v6
                    .to_ipv4_mapped()
                    .is_some_and(|v4| is_internal(IpAddr::V4(v4)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(hosts: &[&str]) -> QuackTargetPolicy {
        QuackTargetPolicy::new(hosts.iter().map(|s| s.to_string()).collect(), 1024)
    }

    fn url(s: &str) -> Url {
        Url::parse(s).unwrap()
    }

    #[test]
    fn empty_allow_list_denies_everything() {
        let p = QuackTargetPolicy::default();
        let err = p
            .check_destination(&url("http://quack-peer:9495/quack"))
            .unwrap_err();
        assert!(
            err.to_string().contains("no quack_client.allowed_hosts"),
            "{err}"
        );
    }

    #[test]
    fn exact_host_port_is_required() {
        let p = policy(&["Quack-Peer:9495"]);
        p.check_destination(&url("http://quack-peer:9495/quack"))
            .unwrap();
        assert!(p
            .check_destination(&url("http://quack-peer:9494/quack"))
            .is_err());
        assert!(p
            .check_destination(&url("http://quack-peer.evil:9495/quack"))
            .is_err());
    }

    /// 604: the cloud metadata service must be refused before any socket is
    /// opened, even when other hosts are allowed.
    #[test]
    fn metadata_loopback_and_private_ranges_are_refused_unless_listed() {
        let p = policy(&["quack-peer:9495"]);
        for target in [
            "http://169.254.169.254/latest/meta-data",
            "http://127.0.0.1:9495/quack",
            "http://10.9.9.9:9495/quack",
            "http://192.168.1.1:9495/quack",
            "http://100.64.0.1:9495/quack",
            "http://[::1]:9495/quack",
            "http://[fe80::1]:9495/quack",
            "http://[::ffff:169.254.169.254]:80/quack",
        ] {
            let err = p.check_destination(&url(target)).unwrap_err();
            assert!(
                err.to_string().contains("internal address"),
                "{target}: {err}"
            );
        }
        // Listing the exact entry is the operator's explicit choice.
        let local = policy(&["127.0.0.1:9495"]);
        local
            .check_destination(&url("http://127.0.0.1:9495/quack"))
            .unwrap();
    }

    #[test]
    fn permissive_allows_anything_and_is_never_the_default() {
        assert_ne!(
            QuackTargetPolicy::permissive(),
            QuackTargetPolicy::default()
        );
        QuackTargetPolicy::permissive()
            .check_destination(&url("http://169.254.169.254/"))
            .unwrap();
    }

    #[test]
    fn charge_fails_once_cap_is_exceeded() {
        let p = policy(&["a:1"]);
        let mut seen = 0;
        p.charge(&mut seen, 1000).unwrap();
        let err = p.charge(&mut seen, 100).unwrap_err();
        assert!(matches!(
            err,
            ClientError::ResultTooLarge {
                cap: 1024,
                seen: 1100
            }
        ));
    }
}
