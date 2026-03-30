pub mod config;
pub mod error;
pub mod session;

pub use config::{AuthProviderConfig, DeviceAuthConfig, ExternalAuthConfig, QueryCacheConfig, QueryConfig, QueryHistoryConfig, SqeConfig, parse_memory_limit};
pub use error::{Result, SqeError, SqeErrorCode};
pub use session::{Session, SessionUser};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
