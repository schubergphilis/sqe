pub mod config;
pub mod error;
pub mod session;
pub mod table_properties;

pub use config::{AuthProviderConfig, DeviceAuthConfig, ExternalAuthConfig, FlightCompression, QueryCacheConfig, QueryConfig, QueryHistoryConfig, SortMode, SqeConfig, parse_memory_limit};
pub use error::{Result, SqeError, SqeErrorCode};
pub use session::{Session, SessionUser};
pub use table_properties::{
    WriteMode, WRITE_DELETE_MODE, WRITE_MERGE_MODE, WRITE_UPDATE_MODE, resolve_delete_mode,
    resolve_merge_mode, resolve_mode, resolve_update_mode,
};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
