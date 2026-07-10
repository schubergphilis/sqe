//! Derive the URL a worker advertises to the coordinator in its heartbeat.
//!
//! A worker binds `0.0.0.0:{port}` but must tell the coordinator a *routable*
//! address. Sending `http://0.0.0.0:{port}` (the old behavior) made every
//! worker collide on one bogus loopback registry entry that the scheduler then
//! targeted, causing flapping (issue #220).
//!
//! Derivation order:
//! 1. `worker.advertise_url` if set (operator override; wins outright).
//! 2. `POD_IP` env var (Kubernetes downward API: the routable pod address).
//! 3. `HOSTNAME` env var, only when it parses as an IP or resolves under
//!    cluster DNS. On a bare Deployment `HOSTNAME` is the pod name, which is
//!    not resolvable, so we only trust it when it is an IP literal.
//! 4. First non-loopback, non-link-local local interface address (IPv4
//!    preferred). Covers docker-compose / bare-metal where `POD_IP` is unset.
//!
//! The scheme is `https` when the worker's TLS (the coordinator TLS block,
//! reused by workers) is enabled, otherwise `http`.

use std::net::IpAddr;

use sqe_core::SqeConfig;

/// Build the advertised URL from an already-chosen candidate.
///
/// Pure policy, unit-tested with injected inputs:
/// - an explicit `advertise_url` wins and is returned verbatim (trimmed);
/// - otherwise a `candidate_ip` is required and must not be loopback,
///   unspecified (`0.0.0.0` / `::`), or link-local;
/// - the scheme is `https` iff `tls_enabled`.
///
/// Returns `Err` with an operator-facing message when no usable address is
/// available so the worker fails visibly instead of advertising garbage.
pub fn build_advertise_url(
    explicit: &str,
    candidate_ip: Option<IpAddr>,
    port: u16,
    tls_enabled: bool,
) -> Result<String, String> {
    let explicit = explicit.trim();
    if !explicit.is_empty() {
        if explicit_is_unspecified(explicit) {
            return Err(format!(
                "worker.advertise_url = {explicit:?} resolves to an unspecified \
                 address (0.0.0.0 / ::); set it to a routable host the coordinator \
                 can reach"
            ));
        }
        return Ok(explicit.to_string());
    }

    let scheme = if tls_enabled { "https" } else { "http" };

    let ip = candidate_ip.ok_or_else(|| {
        "could not derive a routable advertise address for this worker. Set \
         worker.advertise_url explicitly, or expose POD_IP via the Kubernetes \
         downward API. The worker refuses to advertise 0.0.0.0 because it would \
         poison the coordinator's worker registry."
            .to_string()
    })?;

    if !is_routable(&ip) {
        return Err(format!(
            "derived advertise address {ip} is not routable (loopback, \
             unspecified, or link-local). Set worker.advertise_url explicitly."
        ));
    }

    // IPv6 literals need brackets in a URL authority.
    let host = match ip {
        IpAddr::V4(v4) => v4.to_string(),
        IpAddr::V6(v6) => format!("[{v6}]"),
    };
    Ok(format!("{scheme}://{host}:{port}"))
}

/// Returns `true` when an explicit URL-or-host string points at the
/// unspecified address (`0.0.0.0` / `::`). Catches the most common
/// misconfiguration (advertising the bind address).
fn explicit_is_unspecified(explicit: &str) -> bool {
    // Try as a full URL first.
    if let Ok(parsed) = url::Url::parse(explicit) {
        if let Some(host) = parsed.host_str() {
            return host_is_unspecified(host);
        }
    }
    // Fall back to treating it as a bare host[:port].
    let host = explicit
        .rsplit_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(explicit);
    let host = host.split('/').next().unwrap_or(host);
    let host = host.rsplit_once(':').map(|(h, _)| h).unwrap_or(host);
    host_is_unspecified(host)
}

fn host_is_unspecified(host: &str) -> bool {
    let bare = host
        .strip_prefix('[')
        .and_then(|r| r.strip_suffix(']'))
        .unwrap_or(host);
    matches!(bare.parse::<IpAddr>(), Ok(ip) if ip.is_unspecified())
}

/// An address is routable for advertising when it is not loopback,
/// unspecified, or link-local.
fn is_routable(ip: &IpAddr) -> bool {
    if ip.is_loopback() || ip.is_unspecified() {
        return false;
    }
    match ip {
        IpAddr::V4(v4) => !v4.is_link_local(),
        // IPv6 link-local is fe80::/10. `is_unicast_link_local` is unstable on
        // std, so test the prefix directly.
        IpAddr::V6(v6) => {
            let seg = v6.segments()[0];
            (seg & 0xffc0) != 0xfe80
        }
    }
}

/// Gather a candidate IP from the environment, then local interfaces.
///
/// Returns the first usable address. `None` when nothing routable is found
/// (the caller turns that into a hard error via [`build_advertise_url`]).
fn gather_candidate_ip() -> Option<IpAddr> {
    // Kubernetes downward API: status.podIP -> POD_IP. The routable address.
    if let Some(ip) = env_ip("POD_IP") {
        return Some(ip);
    }
    // HOSTNAME is only trustworthy when it is an IP literal; on a Deployment
    // it is the pod name and does not resolve, so we do not advertise it as a
    // hostname.
    if let Some(ip) = env_ip("HOSTNAME") {
        return Some(ip);
    }
    first_routable_interface_ip()
}

fn env_ip(var: &str) -> Option<IpAddr> {
    std::env::var(var)
        .ok()
        .and_then(|v| v.trim().parse::<IpAddr>().ok())
        .filter(is_routable)
}

/// First non-loopback, non-link-local local interface address, IPv4 preferred.
fn first_routable_interface_ip() -> Option<IpAddr> {
    let addrs = if_addrs::get_if_addrs().ok()?;
    // Prefer IPv4: simpler URL authority, fewer dual-stack surprises.
    addrs
        .iter()
        .map(|i| i.ip())
        .find(|ip| ip.is_ipv4() && is_routable(ip))
        .or_else(|| addrs.iter().map(|i| i.ip()).find(is_routable))
}

/// Derive the URL this worker advertises to the coordinator, reading the
/// explicit override, the environment, and local interfaces as needed.
///
/// The TLS scheme follows the coordinator TLS block (workers reuse it).
pub fn derive_advertise_url(config: &SqeConfig) -> Result<String, String> {
    build_advertise_url(
        &config.worker.advertise_url,
        gather_candidate_ip(),
        config.worker.flight_port,
        config.coordinator.tls.is_enabled(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    fn explicit_url_wins_verbatim() {
        let url = build_advertise_url(
            "https://worker-3.svc.cluster.local:50052",
            Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5))),
            50052,
            false,
        )
        .unwrap();
        assert_eq!(url, "https://worker-3.svc.cluster.local:50052");
    }

    #[test]
    fn explicit_unspecified_is_rejected() {
        let err = build_advertise_url("http://0.0.0.0:50052", None, 50052, false).unwrap_err();
        assert!(err.contains("unspecified"), "got: {err}");
        // Bare host form too.
        let err = build_advertise_url("0.0.0.0:50052", None, 50052, false).unwrap_err();
        assert!(err.contains("unspecified"), "got: {err}");
    }

    #[test]
    fn derives_http_from_candidate_ip() {
        let url = build_advertise_url(
            "",
            Some(IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3))),
            50052,
            false,
        )
        .unwrap();
        assert_eq!(url, "http://10.1.2.3:50052");
    }

    #[test]
    fn derives_https_when_tls_enabled() {
        let url = build_advertise_url(
            "",
            Some(IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3))),
            50052,
            true,
        )
        .unwrap();
        assert_eq!(url, "https://10.1.2.3:50052");
    }

    #[test]
    fn derives_bracketed_ipv6() {
        let ip = IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1));
        let url = build_advertise_url("", Some(ip), 50052, false).unwrap();
        assert_eq!(url, "http://[2001:db8::1]:50052");
    }

    #[test]
    fn rejects_loopback_candidate() {
        let err = build_advertise_url(
            "",
            Some(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
            50052,
            false,
        )
        .unwrap_err();
        assert!(err.contains("not routable"), "got: {err}");
    }

    #[test]
    fn rejects_unspecified_candidate() {
        let err = build_advertise_url(
            "",
            Some(IpAddr::V4(Ipv4Addr::UNSPECIFIED)),
            50052,
            false,
        )
        .unwrap_err();
        assert!(err.contains("not routable"), "got: {err}");
    }

    #[test]
    fn rejects_link_local_candidate() {
        let err = build_advertise_url(
            "",
            Some(IpAddr::V4(Ipv4Addr::new(169, 254, 1, 1))),
            50052,
            false,
        )
        .unwrap_err();
        assert!(err.contains("not routable"), "got: {err}");
    }

    #[test]
    fn no_candidate_errors_loudly() {
        let err = build_advertise_url("", None, 50052, false).unwrap_err();
        assert!(err.contains("routable advertise address"), "got: {err}");
    }

    #[test]
    fn is_routable_classifies_addresses() {
        assert!(is_routable(&IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))));
        assert!(is_routable(&IpAddr::V4(Ipv4Addr::new(192, 168, 1, 5))));
        assert!(!is_routable(&IpAddr::V4(Ipv4Addr::LOCALHOST)));
        assert!(!is_routable(&IpAddr::V4(Ipv4Addr::UNSPECIFIED)));
        assert!(!is_routable(&IpAddr::V4(Ipv4Addr::new(169, 254, 0, 1))));
        assert!(!is_routable(&IpAddr::V6(Ipv6Addr::LOCALHOST)));
        assert!(!is_routable(&IpAddr::V6(Ipv6Addr::new(
            0xfe80, 0, 0, 0, 0, 0, 0, 1
        ))));
        assert!(is_routable(&IpAddr::V6(Ipv6Addr::new(
            0x2001, 0xdb8, 0, 0, 0, 0, 0, 1
        ))));
    }
}
