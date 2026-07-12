use sqe_core::config::CoordinatorConfig;
use sqe_core::SqeConfig;

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
/// Shared between `main.rs` and `bin/sqe_server.rs` so the warning cannot
/// drift between the two coordinator binaries.
#[must_use]
pub fn warns_unauthenticated_workers(coordinator: &CoordinatorConfig) -> bool {
    // Mirror `distributed_enabled` in bin/sqe_server.rs: discovery is live on
    // static worker_urls, a configured secret, OR the unauthenticated waiver.
    let discovery_live = !coordinator.worker_urls.is_empty()
        || !coordinator.worker_secret.is_empty()
        || coordinator.allow_unauthenticated_workers;
    warns_unauthenticated_workers_inner(
        discovery_live,
        coordinator.worker_secret.is_empty(),
        coordinator.allow_unauthenticated_workers,
    )
}

/// Pure predicate behind [`warns_unauthenticated_workers`], split out so the
/// condition is unit-testable without constructing a full `CoordinatorConfig`.
#[must_use]
fn warns_unauthenticated_workers_inner(
    discovery_live: bool,
    secret_empty: bool,
    allow_unauthenticated: bool,
) -> bool {
    discovery_live && secret_empty && allow_unauthenticated
}

/// Resolve the server mode from config and environment.
///
/// Priority: config file `mode` field → `SQE_MODE` env var → legacy modes default to coordinator.
pub fn resolve_mode(config: &SqeConfig) -> Result<Mode, String> {
    let env_mode = std::env::var("SQE_MODE").ok();
    resolve_mode_inner(&config.coordinator.mode, env_mode.as_deref())
}

fn resolve_mode_inner(config_mode: &str, env_mode: Option<&str>) -> Result<Mode, String> {
    let config_lower = config_mode.to_lowercase();

    if config_lower == "coordinator" {
        return Ok(Mode::Coordinator);
    }
    if config_lower == "worker" {
        return Ok(Mode::Worker);
    }

    // Fall back to SQE_MODE env var
    if let Some(val) = env_mode {
        return match val.to_lowercase().as_str() {
            "coordinator" => Ok(Mode::Coordinator),
            "worker" => Ok(Mode::Worker),
            other => Err(format!(
                "Invalid SQE_MODE={other:?}. Valid values: coordinator, worker"
            )),
        };
    }

    // Legacy modes default to coordinator
    if config_lower == "hybrid" || config_lower == "local" || config_lower == "distributed" {
        return Ok(Mode::Coordinator);
    }

    Err(format!(
        "SQE_MODE is not set and config mode={config_mode:?} is not recognized.\n\
         Set SQE_MODE=coordinator or SQE_MODE=worker, or set mode in config file."
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_coordinator() {
        assert_eq!(
            resolve_mode_inner("coordinator", None).unwrap(),
            Mode::Coordinator
        );
    }

    #[test]
    fn config_worker() {
        assert_eq!(resolve_mode_inner("worker", None).unwrap(), Mode::Worker);
    }

    #[test]
    fn config_case_insensitive() {
        assert_eq!(
            resolve_mode_inner("Coordinator", None).unwrap(),
            Mode::Coordinator
        );
        assert_eq!(resolve_mode_inner("WORKER", None).unwrap(), Mode::Worker);
    }

    #[test]
    fn legacy_hybrid_defaults_to_coordinator() {
        assert_eq!(
            resolve_mode_inner("hybrid", None).unwrap(),
            Mode::Coordinator
        );
    }

    #[test]
    fn legacy_local_defaults_to_coordinator() {
        assert_eq!(
            resolve_mode_inner("local", None).unwrap(),
            Mode::Coordinator
        );
    }

    #[test]
    fn env_var_overrides_unknown_config() {
        assert_eq!(
            resolve_mode_inner("something", Some("worker")).unwrap(),
            Mode::Worker
        );
        assert_eq!(
            resolve_mode_inner("something", Some("COORDINATOR")).unwrap(),
            Mode::Coordinator
        );
    }

    #[test]
    fn env_var_does_not_override_explicit_config() {
        // Config says coordinator explicitly — env var is ignored
        assert_eq!(
            resolve_mode_inner("coordinator", Some("worker")).unwrap(),
            Mode::Coordinator
        );
    }

    #[test]
    fn invalid_env_var() {
        let result = resolve_mode_inner("something", Some("invalid"));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid SQE_MODE"));
    }

    #[test]
    fn missing_env_and_unknown_config() {
        let result = resolve_mode_inner("unknown", None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("SQE_MODE is not set"));
    }

    #[test]
    fn env_var_with_legacy_config_prefers_legacy() {
        // Legacy modes (hybrid/local/distributed) prefer env var since it's more specific
        // Actually env var is checked first for non-coordinator/worker config modes
        assert_eq!(
            resolve_mode_inner("hybrid", Some("worker")).unwrap(),
            Mode::Worker
        );
    }

    #[test]
    fn warn_fires_on_live_unauthenticated_discovery() {
        // worker_urls set + empty secret + waiver = unauthenticated workers
        // can register and receive bearer tokens. Warn.
        assert!(warns_unauthenticated_workers_inner(true, true, true));
    }

    #[test]
    fn warn_silent_when_discovery_not_live() {
        // Pure predicate: when discovery is genuinely not live, nothing to warn
        // about. (Note the outer fn treats the waiver itself as making discovery
        // live, so this `false` only arises without any of urls/secret/waiver.)
        assert!(!warns_unauthenticated_workers_inner(false, true, true));
    }

    #[test]
    fn warn_silent_when_secret_present() {
        // A non-empty secret is enforced by the heartbeat handler even with
        // the waiver set, so the workers are authenticated. No false positive.
        assert!(!warns_unauthenticated_workers_inner(true, false, true));
    }

    #[test]
    fn warn_silent_without_waiver() {
        // Without the waiver, SqeConfig::validate refuses to boot the
        // empty-secret distributed case, so the warning is moot.
        assert!(!warns_unauthenticated_workers_inner(true, true, false));
    }

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
