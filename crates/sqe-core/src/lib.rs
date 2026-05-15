pub mod config;
pub mod error;
pub mod secret;
pub mod secret_string;
pub mod session;
pub mod supervised_task;
pub mod table_properties;

pub use config::{AuthProviderConfig, DeviceAuthConfig, ExternalAuthConfig, FlightCompression, QueryCacheConfig, QueryConfig, QueryHistoryConfig, SortMode, SqeConfig, parse_memory_limit};
pub use error::{CatalogOp, Result, SqeError, SqeErrorCode};
pub use secret::{Secret, SecretStore, SecretStoreError};
pub use secret_string::SecretString;
pub use session::{Credentials, Session, SessionUser};
pub use supervised_task::{spawn_supervised, TaskGuard};
pub use table_properties::{
    WriteMode, WRITE_DELETE_MODE, WRITE_MERGE_MODE, WRITE_UPDATE_MODE, resolve_delete_mode,
    resolve_merge_mode, resolve_mode, resolve_update_mode,
};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
