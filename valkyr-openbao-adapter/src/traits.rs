use crate::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::time::Duration;
use valkyr_core::{Key, KeyPattern, NamespaceContext};

#[derive(Clone, Debug)]
pub struct OpenBaoValue {
    pub namespace: NamespaceContext,
    pub key: Key,
    pub value: Value,
    pub ttl: Option<Duration>,
}
#[async_trait]
pub trait QueryProvider: Send + Sync {
    fn matches(&self, namespace: &NamespaceContext, key: &Key) -> bool;
    async fn query(&self, namespace: NamespaceContext, key: Key) -> Result<Option<OpenBaoValue>>;
}
#[async_trait]
pub trait StorageWriter: Send + Sync {
    fn matches(&self, namespace: &NamespaceContext, key_pattern: Option<&KeyPattern>) -> bool;
    async fn set(&self, value: OpenBaoValue) -> Result<()>;
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
