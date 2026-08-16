//! OpenBao KV v2 query and write-through adapter for Valkyr.
mod bridge;
mod client;
mod config;
mod error;
mod mapping;
mod traits;

pub use bridge::{CallbackBridge, OpenBaoQueryProvider, OpenBaoStoreWriter, fetch_provider_values};
pub use client::{AppRole, OpenBaoClient, Versioned};
pub use config::{
    AdapterConfig, AppRoleConfig, LogFormat, LoggingConfig, OnMissing, OpenBaoConfig,
    ProviderConfig, QueryConfig, StoreConfig, ValkyrConfig, ValkyrEndpoint,
};
pub use error::{AdapterError, Result};
pub use mapping::{OpenBaoMapping, SecretLocation, decode, encode};
pub use traits::{OpenBaoValue, QueryProvider, StorageWriter};
