//! RSDB Error types

use thiserror::Error;

#[derive(Error, Debug)]
pub enum RsdbError {
    #[error("Catalog error: {0}")]
    Catalog(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("SQL parsing error: {0}")]
    SqlParse(String),

    #[error("Planning error: {0}")]
    Planner(String),

    #[error("Execution error: {0}")]
    Execution(String),

    #[error("Network error: {0}")]
    Network(String),

    #[error("Coordinator error: {0}")]
    Coordinator(String),

    #[error("Worker error: {0}")]
    Worker(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Already exists: {0}")]
    AlreadyExists(String),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error("TOML error: {0}")]
    Toml(#[from] toml::ser::Error),

    #[error("TOML parse error: {0}")]
    TomlParse(#[from] toml::de::Error),
}

pub type Result<T> = std::result::Result<T, RsdbError>;
