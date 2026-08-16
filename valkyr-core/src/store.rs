use crate::{Key, KeyPattern, NamespaceContext, Result};
use async_trait::async_trait;
use serde_json::Value;
use std::{sync::Arc, time::Duration};

#[derive(Clone, Debug)]
pub struct StoredValue {
    pub value: Value,
    pub remaining_ttl: Option<Duration>,
}

/// The storage boundary for the broker. Implementations must make namespace
/// moves atomic with respect to their own reads and writes.
#[async_trait]
pub trait Store: Send + Sync + 'static {
    async fn get(&self, namespace: &NamespaceContext, key: &Key) -> Result<Option<StoredValue>>;
    async fn set(
        &self,
        namespace: NamespaceContext,
        key: Key,
        value: Value,
        ttl: Option<Duration>,
    ) -> Result<()>;
    async fn set_batch(
        &self,
        namespace: NamespaceContext,
        entries: Vec<(Key, Value)>,
        ttl: Option<Duration>,
    ) -> Result<()> {
        for (key, value) in entries {
            self.set(namespace.clone(), key, value, ttl).await?;
        }
        Ok(())
    }
    async fn delete(
        &self,
        namespace: &NamespaceContext,
        pattern: Option<&KeyPattern>,
    ) -> Result<u64>;
    async fn scan(
        &self,
        namespace: &NamespaceContext,
        pattern: &KeyPattern,
    ) -> Result<Vec<(Key, StoredValue)>>;
    async fn move_namespace(
        &self,
        source: &NamespaceContext,
        destination: NamespaceContext,
    ) -> Result<()>;
    async fn len(&self) -> Result<u64>;

    async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }
}

/// An ordered, in-process composition of stores. Reads return the first hit;
/// writes fan out in order and stop at the first failure.
pub struct CompositeStore {
    stores: Vec<Arc<dyn Store>>,
}

impl CompositeStore {
    pub fn new(stores: Vec<Arc<dyn Store>>) -> Self {
        Self { stores }
    }
}

#[async_trait]
impl Store for CompositeStore {
    async fn get(&self, namespace: &NamespaceContext, key: &Key) -> Result<Option<StoredValue>> {
        for store in &self.stores {
            if let Some(value) = store.get(namespace, key).await? {
                return Ok(Some(value));
            }
        }
        Ok(None)
    }
    async fn set(
        &self,
        namespace: NamespaceContext,
        key: Key,
        value: Value,
        ttl: Option<Duration>,
    ) -> Result<()> {
        for store in &self.stores {
            store
                .set(namespace.clone(), key.clone(), value.clone(), ttl)
                .await?;
        }
        Ok(())
    }
    async fn delete(
        &self,
        namespace: &NamespaceContext,
        pattern: Option<&KeyPattern>,
    ) -> Result<u64> {
        let mut deleted = 0;
        for store in &self.stores {
            deleted += store.delete(namespace, pattern).await?;
        }
        Ok(deleted)
    }
    async fn scan(
        &self,
        namespace: &NamespaceContext,
        pattern: &KeyPattern,
    ) -> Result<Vec<(Key, StoredValue)>> {
        for store in &self.stores {
            let values = store.scan(namespace, pattern).await?;
            if !values.is_empty() {
                return Ok(values);
            }
        }
        Ok(Vec::new())
    }
    async fn move_namespace(
        &self,
        source: &NamespaceContext,
        destination: NamespaceContext,
    ) -> Result<()> {
        for store in &self.stores {
            store.move_namespace(source, destination.clone()).await?;
        }
        Ok(())
    }
    async fn len(&self) -> Result<u64> {
        let mut count = 0;
        for store in &self.stores {
            count += store.len().await?;
        }
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryStore;
    use serde_json::json;

    #[tokio::test]
    async fn composite_store_fans_out_mutations_and_reads_first_hit() {
        let first = Arc::new(MemoryStore::new());
        let second = Arc::new(MemoryStore::new());
        let namespace = NamespaceContext::new("/values").unwrap();
        let key = Key::new("entry").unwrap();
        first
            .set(namespace.clone(), key.clone(), json!("first"), None)
            .await
            .unwrap();
        second
            .set(namespace.clone(), key.clone(), json!("second"), None)
            .await
            .unwrap();
        let composite = CompositeStore::new(vec![first.clone(), second.clone()]);

        assert_eq!(
            composite
                .get(&namespace, &key)
                .await
                .unwrap()
                .unwrap()
                .value,
            json!("first")
        );
        assert_eq!(composite.delete(&namespace, None).await.unwrap(), 2);
        composite
            .set(namespace.clone(), key.clone(), json!("moved"), None)
            .await
            .unwrap();
        let destination = NamespaceContext::new("/archive").unwrap();
        composite
            .move_namespace(&namespace, destination.clone())
            .await
            .unwrap();

        assert!(first.get(&namespace, &key).await.unwrap().is_none());
        assert!(second.get(&namespace, &key).await.unwrap().is_none());
        assert_eq!(
            first.get(&destination, &key).await.unwrap().unwrap().value,
            json!("moved")
        );
        assert_eq!(
            second.get(&destination, &key).await.unwrap().unwrap().value,
            json!("moved")
        );
    }
}
