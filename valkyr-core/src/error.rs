use crate::Route;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{field} must not be empty")]
    EmptyIdentifier { field: &'static str },
    #[error("route not found: {0}")]
    NotFound(Route),
    #[error("destination namespace already exists: {0}")]
    NamespaceExists(String),
    #[error("invalid protocol message: {0}")]
    Protocol(String),
    #[error("connection error: {0}")]
    Connection(String),
    #[error("authentication failed")]
    AuthenticationFailed,
    #[error("permission denied for namespace: {0}")]
    PermissionDenied(String),
    #[error("invalid authorization record: {0}")]
    InvalidAuthorization(String),
    #[error("encryption failed: {0}")]
    Encryption(String),
}

pub type Result<T> = std::result::Result<T, Error>;
