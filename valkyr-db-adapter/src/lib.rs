//! Pull database rows and publish them to Valkyr.

mod bridge;
mod config;
mod error;
mod publisher;
mod sqlx_impl;
mod traits;

pub use bridge::CallbackBridge;
pub use config::{
    AdapterConfig, DatabaseConfig, InitConfig, LogFormat, LoggingConfig, ProviderConfig,
    QueryConfig, StoreConfig, ValkyrConfig, ValkyrEndpoint,
};
pub use error::{AdapterError, Result};
pub use publisher::{Adapter, ReconnectingPublisher};
pub use sqlx_impl::{DatabaseManager, DatabaseQueryProvider, DatabaseSource, DatabaseStoreWriter};
pub use traits::{DatabaseValue, QueryProvider, StorageWriter, ValuePublisher, ValueSource};
