use sqe_core::config::CoordinatorConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Coordinator,
    Worker,
}

/// Whether the coordinator should warn that it accepts UNAUTHENTICATED workers.
///
/// Distributed discovery is live whenever `distributed_enabled` is true: when
/// `worker_urls` is non-empty, OR `worker_secret` is set, OR
/// `allow_unauthenticated_workers` is set. Heartbeat discovery seeds the
/// registry on any of these, so an empty `worker_urls` does NOT mean discovery
/// is off. On that live path the secret is enforced whenever it is non-empty,
/// so a configured secret is genuinely authenticated even with the
/// `allow_unauthenticated_workers` waiver set.
///
/// The dangerous combination is therefore discovery live, no secret to check
/// (`worker_secret` empty), and the operator has waived the empty-secret
/// refusal (`allow_unauthenticated_workers`). Because the waiver alone makes
/// discovery live, this fires even for a heartbeat-only deployment with an
/// empty `worker_urls`. In that state any TCP-reachable client can register as
/// a worker and receive user bearer tokens.
///
/// Lives in the library rather than in `crates/sqe-server/src/main.rs` so
/// the warning text has one home that tests can reach.
#[must_use]
pub fn warns_unauthenticated_workers(coordinator: &CoordinatorConfig) -> bool {
    // Mirror `distributed_enabled` in bin/sqe_server.rs: discovery is live on
    // static worker_urls, a configured secret, OR the unauthenticated waiver.
    let discovery_live = !coordinator.worker_urls.is_empty()
        || !coordinator.worker_secret.is_empty()
        || coordinator.allow_unauthenticated_workers;
    discovery_live
        && coordinator.worker_secret.is_empty()
        && coordinator.allow_unauthenticated_workers
}

#[cfg(test)]
mod tests {
    use super::*;

    fn coord_from_toml(toml: &str) -> CoordinatorConfig {
        toml::from_str(toml).expect("valid coordinator config")
    }

    #[test]
    fn warn_fires_on_heartbeat_only_unauthenticated() {
        // Regression for the empty-worker_urls case: after distributed_enabled
        // (bin/sqe_server.rs) made heartbeat discovery live on the waiver alone,
        // an empty worker_urls + empty secret + waiver still accepts
        // unauthenticated workers. The warning must fire here.
        let coord = coord_from_toml("allow_unauthenticated_workers = true");
        assert!(coord.worker_urls.is_empty());
        assert!(warns_unauthenticated_workers(&coord));
    }

    #[test]
    fn warn_silent_for_single_node_default() {
        // No worker_urls, no secret, no waiver: single-node default. No warning.
        let coord = coord_from_toml("");
        assert!(!warns_unauthenticated_workers(&coord));
    }

    #[test]
    fn warn_silent_when_secret_set_with_waiver() {
        // A configured secret is enforced by the heartbeat handler even with the
        // waiver, so the workers are authenticated. No false positive.
        let coord =
            coord_from_toml("allow_unauthenticated_workers = true\nworker_secret = \"s3cr3t\"");
        assert!(!warns_unauthenticated_workers(&coord));
    }
}
