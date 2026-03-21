pub mod config;
pub mod error;
pub mod session;

pub use config::{SqeConfig, parse_memory_limit};
pub use error::{Result, SqeError};
pub use session::{Session, SessionUser};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
