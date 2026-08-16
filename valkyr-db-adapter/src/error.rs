use thiserror::Error;
use tokio::task;

#[derive(Debug, Error)]
pub enum AdapterError {
    #[error("SQL database operation failed: {0}")]
    Sql(#[from] sqlx::Error),
    #[error("database task failed: {0}")]
    Task(#[from] task::JoinError),
    #[error("row has an invalid route: {0}")]
    Route(#[from] valkyr_core::Error),
    #[error("row contains invalid JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("publish failed: {0}")]
    Publish(#[from] valkyr_client::ClientError),
    #[error("replication failed for endpoint(s): {0}")]
    Replication(String),
    #[error("invalid adapter configuration: {0}")]
    Configuration(String),
    #[error("configuration file failed: {0}")]
    ConfigurationFile(#[from] std::io::Error),
}
pub type Result<T> = std::result::Result<T, AdapterError>;
