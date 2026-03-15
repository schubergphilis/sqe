use thiserror::Error;

#[derive(Error, Debug)]
pub enum SqeError {
    #[error("Authentication failed: {0}")]
    Auth(String),

    #[error("Catalog error: {0}")]
    Catalog(String),

    #[error("Query execution error: {0}")]
    Execution(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Not implemented: {0}")]
    NotImplemented(String),

    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, SqeError>;
