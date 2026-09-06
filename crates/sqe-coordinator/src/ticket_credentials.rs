//! Scan-ticket trust material: the coordinator's signing key (finding 601)
//! and the choice of storage credentials a ticket carries (finding 401).

use iceberg::table::Table;
use sqe_core::config::{CoordinatorConfig, DistributedCredentialMode, StorageConfig};
use sqe_core::ticket_signing::TicketSigner;
use sqe_planner::ScanCredentials;
use tracing::{info, warn};

/// Iceberg REST vended-credential property names (table `config`).
const VENDED_ACCESS_KEY: &str = "s3.access-key-id";
const VENDED_SECRET_KEY: &str = "s3.secret-access-key";
const VENDED_SESSION_TOKEN: &str = "s3.session-token";
const VENDED_EXPIRES_AT_MS: &str = "s3.session-token-expires-at-ms";

/// Build the coordinator's ticket signer: from
/// `coordinator.ticket_signing_key_file` when set (PKCS#8 v2 Ed25519 DER),
/// otherwise a fresh ephemeral key pair.
///
/// A configured file that does not load is a boot failure; `SqeConfig::
/// validate` checks it first so the panic here is a programming error, not
/// an operator-facing path.
pub fn load_ticket_signer(config: &CoordinatorConfig) -> TicketSigner {
    let path = config.ticket_signing_key_file.trim();
    if path.is_empty() {
        let signer = TicketSigner::generate().expect("Ed25519 key generation cannot fail");
        info!(
            public_key = signer.public_key_b64(),
            "Generated ephemeral scan-ticket signing key; workers pin it via heartbeat"
        );
        return signer;
    }
    let der = std::fs::read(path).unwrap_or_else(|e| {
        panic!("coordinator.ticket_signing_key_file {path}: {e} (SqeConfig::validate should have refused this)")
    });
    let signer = TicketSigner::from_pkcs8(&der).unwrap_or_else(|e| {
        panic!("coordinator.ticket_signing_key_file {path}: {e} (SqeConfig::validate should have refused this)")
    });
    info!(
        path,
        public_key = signer.public_key_b64(),
        "Loaded scan-ticket signing key"
    );
    signer
}

/// Choose the credentials a distributed scan of `table` ships, per
/// `coordinator.distributed_credentials`. `None` means the mode cannot be
/// satisfied and the caller must not distribute.
pub fn select_scan_credentials(
    mode: DistributedCredentialMode,
    storage: &StorageConfig,
    table: &Table,
) -> Option<ScanCredentials> {
    let vended = vended_credentials(table);
    select_with(mode, storage, vended)
}

fn select_with(
    mode: DistributedCredentialMode,
    storage: &StorageConfig,
    vended: Option<ScanCredentials>,
) -> Option<ScanCredentials> {
    let static_keys = || {
        (!storage.s3_access_key.is_empty()).then(|| {
            ScanCredentials::static_keys(
                storage.s3_access_key.clone(),
                storage.s3_secret_key.expose().to_string(),
                storage.s3_session_token.expose().to_string(),
            )
        })
    };
    match mode {
        DistributedCredentialMode::WorkerDefault => Some(ScanCredentials::WorkerDefault),
        DistributedCredentialMode::Vended => vended,
        DistributedCredentialMode::Static => static_keys(),
        DistributedCredentialMode::Auto => match vended {
            Some(v) => Some(v),
            None => {
                let s = static_keys();
                if s.is_some() {
                    warn_static_once();
                }
                s
            }
        },
    }
}

fn warn_static_once() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        warn!(
            "distributed_credentials = auto: shipping the engine's static [storage] key pair \
             to workers because the table has no catalog-vended credentials. Set \
             coordinator.distributed_credentials explicitly (finding 401)."
        );
    });
}

/// Vended S3 credentials from the table's FileIO properties, when the
/// catalog supplied them at LOAD_TABLE time.
fn vended_credentials(table: &Table) -> Option<ScanCredentials> {
    let (_, props, _) = table.file_io().clone().into_builder().into_parts();
    vended_from_props(&props)
}

fn vended_from_props(props: &std::collections::HashMap<String, String>) -> Option<ScanCredentials> {
    let access = props.get(VENDED_ACCESS_KEY).filter(|v| !v.is_empty())?;
    let secret = props.get(VENDED_SECRET_KEY).filter(|v| !v.is_empty())?;
    let token = props.get(VENDED_SESSION_TOKEN).cloned().unwrap_or_default();
    let expires_at = props
        .get(VENDED_EXPIRES_AT_MS)
        .and_then(|ms| ms.parse::<i64>().ok())
        .map(|ms| ms / 1000);
    Some(ScanCredentials::vended(
        access.clone(),
        secret.clone(),
        token,
        expires_at,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn storage(with_keys: bool) -> StorageConfig {
        let mut s = StorageConfig::default();
        if with_keys {
            s.s3_access_key = "AKIASTATIC".to_string();
            s.s3_secret_key = sqe_core::SecretString::new("static-secret".to_string());
        }
        s
    }

    fn vended() -> Option<ScanCredentials> {
        let mut props = HashMap::new();
        props.insert(VENDED_ACCESS_KEY.to_string(), "ASIAVENDED".to_string());
        props.insert(VENDED_SECRET_KEY.to_string(), "vended-secret".to_string());
        props.insert(VENDED_SESSION_TOKEN.to_string(), "sts-token".to_string());
        props.insert(
            VENDED_EXPIRES_AT_MS.to_string(),
            "1893456000000".to_string(),
        );
        vended_from_props(&props)
    }

    #[test]
    fn vended_props_become_vended_credentials_with_expiry() {
        let c = vended().unwrap();
        assert_eq!(c.source(), "vended");
        assert_eq!(c.access_key(), "ASIAVENDED");
        assert_eq!(c.session_token(), "sts-token");
        assert_eq!(c.expires_at(), Some(1_893_456_000));
        assert!(vended_from_props(&HashMap::new()).is_none());
    }

    /// 401: `vended` never falls back to the static pair; `static` never
    /// picks up vended; `worker_default` ships nothing; `auto` prefers vended.
    #[test]
    fn modes_select_the_documented_source() {
        let s = storage(true);
        assert_eq!(
            select_with(DistributedCredentialMode::Vended, &s, None),
            None
        );
        assert_eq!(
            select_with(DistributedCredentialMode::Vended, &s, vended()).map(|c| c.source()),
            Some("vended")
        );
        assert_eq!(
            select_with(DistributedCredentialMode::Static, &s, vended()).map(|c| c.source()),
            Some("static")
        );
        assert_eq!(
            select_with(DistributedCredentialMode::Static, &storage(false), None),
            None
        );
        assert_eq!(
            select_with(DistributedCredentialMode::WorkerDefault, &s, vended()),
            Some(ScanCredentials::WorkerDefault)
        );
        assert_eq!(
            select_with(DistributedCredentialMode::Auto, &s, vended()).map(|c| c.source()),
            Some("vended")
        );
        assert_eq!(
            select_with(DistributedCredentialMode::Auto, &s, None).map(|c| c.source()),
            Some("static")
        );
        assert_eq!(
            select_with(DistributedCredentialMode::Auto, &storage(false), None),
            None
        );
    }

    /// 401: neither `Debug` of a ticket nor of its credentials prints key
    /// material. (`Serialize` must, the ticket is the transport; that hop is
    /// protected by the signature and TLS.)
    #[test]
    fn debug_never_prints_key_material() {
        let c = vended().unwrap();
        let dbg = format!("{c:?}");
        assert!(
            !dbg.contains("vended-secret") && !dbg.contains("sts-token"),
            "{dbg}"
        );
        assert!(dbg.contains("[REDACTED]"), "{dbg}");
        let task = sqe_planner::ScanTask {
            hive: None,
            version: sqe_planner::SCAN_TASK_VERSION_V1,
            morsel_id: None,
            row_group_start: None,
            row_group_end: None,
            start_byte: None,
            end_byte: None,
            fragment_id: "f".to_string(),
            data_file_paths: vec!["s3://b/k".to_string()],
            file_sizes_bytes: vec![1],
            projected_columns: vec![],
            projected_field_ids: vec![],
            s3_endpoint: String::new(),
            s3_region: String::new(),
            credentials: c,
            audience: "http://w1:50052".to_string(),
            expires_at: 1,
            s3_path_style: true,
            s3_allow_http: true,
            predicate_proto: None,
            limit: None,
        };
        let dbg = format!("{task:?}");
        assert!(!dbg.contains("vended-secret"), "{dbg}");
        let json = String::from_utf8(task.to_bytes().unwrap()).unwrap();
        assert!(json.contains("\"kind\":\"vended\""), "{json}");
    }

    #[test]
    fn ephemeral_signer_is_generated_when_no_key_file() {
        let cfg: CoordinatorConfig = toml::from_str("mode = \"local\"").unwrap();
        let a = load_ticket_signer(&cfg);
        let b = load_ticket_signer(&cfg);
        assert_ne!(a.public_key_b64(), b.public_key_b64());
    }

    #[test]
    fn key_file_is_loaded_and_stable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ticket.der");
        std::fs::write(&path, TicketSigner::generate_pkcs8_der().unwrap()).unwrap();
        let cfg: CoordinatorConfig = toml::from_str(&format!(
            "mode = \"local\"\nticket_signing_key_file = {:?}",
            path.to_string_lossy()
        ))
        .unwrap();
        let a = load_ticket_signer(&cfg);
        let b = load_ticket_signer(&cfg);
        assert_eq!(a.public_key_b64(), b.public_key_b64());
    }
}
