use async_trait::async_trait;
use serde_json::Value;
use std::time::Duration;
use valkyr_core::{Key, KeyPattern, NamespaceContext};

use crate::Result;

/// One JSON value ready to be published.
#[derive(Clone, Debug)]
pub struct DatabaseValue {
    pub namespace: NamespaceContext,
    pub key: Key,
    pub value: Value,
    pub ttl: Option<Duration>,
}
/// Reads the current database values. Implementations should return a complete,
/// self-contained batch; the adapter makes no assumptions about cursors.
#[async_trait]
pub trait ValueSource: Send + Sync {
    async fn fetch_values(&self) -> Result<Vec<DatabaseValue>>;
}
/// Receives values selected by a [`ValueSource`].
#[async_trait]
pub trait ValuePublisher: Send + Sync {
    async fn publish(&self, value: DatabaseValue) -> Result<()>;
}
/// Handles a cache-miss query using the adapter's database. Returning `None`
/// deliberately leaves the Valkyr cache unchanged.
#[async_trait]
pub trait QueryProvider: Send + Sync {
    fn matches(&self, namespace: &NamespaceContext, key: &Key) -> bool;
    async fn query(&self, namespace: NamespaceContext, key: Key) -> Result<Option<DatabaseValue>>;
}
/// Handles mutations forwarded by Valkyr before its local cache is changed.
#[async_trait]
pub trait StorageWriter: Send + Sync {
    fn matches(&self, namespace: &NamespaceContext, key_pattern: Option<&KeyPattern>) -> bool;
    async fn set(&self, value: DatabaseValue) -> Result<()>;
    async fn set_batch(
        &self,
        namespace: NamespaceContext,
        entries: Vec<(Key, Value)>,
        ttl: Option<Duration>,
    ) -> Result<()>;
    async fn delete(
        &self,
        namespace: NamespaceContext,
        key_pattern: Option<KeyPattern>,
    ) -> Result<()>;
    async fn move_namespace(
        &self,
        source: NamespaceContext,
        destination: NamespaceContext,
    ) -> Result<()>;
}
