pub mod config;
pub mod error;
pub mod secret;
pub mod secret_string;
pub mod session;
pub mod sql_params;
pub mod supervised_task;
pub mod table_properties;

pub use config::{
    parse_memory_limit, AuthProviderConfig, DeviceAuthConfig, ExternalAuthConfig,
    FlightCompression, ProfileMode, QueryCacheConfig, QueryConfig, QueryHistoryConfig, SortMode,
    SqeConfig,
};
pub use error::{CatalogOp, Result, SqeError, SqeErrorCode};
pub use secret::{Secret, SecretStore, SecretStoreError};
pub use secret_string::SecretString;
pub use session::{Credentials, Session, SessionUser};
pub use sql_params::{number_placeholders, substitute_placeholders};
pub use supervised_task::{spawn_supervised, TaskGuard};
pub use table_properties::{
    resolve_delete_mode, resolve_merge_mode, resolve_mode, resolve_update_mode, WriteMode,
    WRITE_DELETE_MODE, WRITE_MERGE_MODE, WRITE_UPDATE_MODE,
};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
