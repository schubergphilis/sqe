use sqe_core::SqeConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Coordinator,
    Worker,
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
        assert_eq!(
            resolve_mode_inner("worker", None).unwrap(),
            Mode::Worker
        );
    }

    #[test]
    fn config_case_insensitive() {
        assert_eq!(
            resolve_mode_inner("Coordinator", None).unwrap(),
            Mode::Coordinator
        );
        assert_eq!(
            resolve_mode_inner("WORKER", None).unwrap(),
            Mode::Worker
        );
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
}
