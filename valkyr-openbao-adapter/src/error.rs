use thiserror::Error;

#[derive(Debug, Error)]
pub enum AdapterError {
    #[error("invalid adapter configuration: {0}")]
    Configuration(String),
    #[error("configuration file failed: {0}")]
    ConfigurationFile(#[from] std::io::Error),
    #[error("OpenBao request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("OpenBao returned status {status}")]
    OpenBao { status: reqwest::StatusCode },
    #[error("OpenBao response was invalid: {0}")]
    Json(#[from] serde_json::Error),
    #[error("route is invalid: {0}")]
    Route(#[from] valkyr_core::Error),
    #[error("Valkyr callback failed: {0}")]
    Valkyr(#[from] valkyr_client::ClientError),
    #[error("operation is not supported: {0}")]
    Unsupported(&'static str),
    #[error("context destination already exists")]
    NamespaceExists,
}

pub type Result<T> = std::result::Result<T, AdapterError>;
