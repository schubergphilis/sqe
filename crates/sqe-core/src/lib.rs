pub mod circuit_breaker;
pub mod config;
pub mod error;
pub mod process_memory;
pub mod runtime;
pub mod secret;
pub mod secret_string;
pub mod session;
pub mod sql_params;
pub mod supervised_task;
pub mod table_properties;
pub mod ticket_signing;
pub mod tls;
pub mod txn_id;
pub mod variant;

pub use process_memory::{
    check_process_headroom, enforced_memory_limit_bytes, enforced_memory_limit_source,
    is_auto_memory_limit, process_rss_bytes, resolve_memory_limit, runtime_memory_info,
    spawn_runtime_memory_watch, CgroupMemoryInfo, CgroupVersion, ContainerKind,
    EnforcedLimitPressure, HostMemoryInfo, OsKind, RuntimeMemoryChange, RuntimeMemoryInfo,
    DEFAULT_RUNTIME_MEMORY_WATCH_INTERVAL,
};

pub use config::{
    parse_memory_limit, AuthProviderConfig, ExternalAuthConfig,
    FlightCompression, MemoryPoolKind, ProfileMode, QueryCacheConfig, QueryConfig,
    QueryHistoryConfig, ResolvedWorkerMemory, ServerMode, SortMode, SqeConfig,
    WorkerMemoryConfig, WorkerSpillConfig,
};
pub use runtime::{build_server_runtime, DEFAULT_RUNTIME_STACK_MIB};
pub use circuit_breaker::CircuitBreaker;
pub use error::{classified_by_text_total, CatalogOp, Result, SqeError, SqeErrorCode};
pub use secret::{Secret, SecretStore, SecretStoreError};
pub use secret_string::SecretString;
pub use session::{Credentials, Session, SessionUser};
pub use sql_params::{number_placeholders, substitute_placeholders};
pub use supervised_task::{spawn_supervised, TaskGuard};
pub use table_properties::{
    resolve_delete_mode, resolve_merge_mode, resolve_mode, resolve_update_mode, WriteMode,
    WRITE_DELETE_MODE, WRITE_MERGE_MODE, WRITE_UPDATE_MODE,
};
pub use tls::{tonic_client, ServerTlsMaterial};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
