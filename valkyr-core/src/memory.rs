use crate::{Error, Key, KeyPattern, NamespaceContext, Result, Store, StoredValue};
use async_trait::async_trait;
use moka::{Expiry, future::Cache};
use serde_json::Value;
use std::{
    hash::Hash,
    time::{Duration, Instant},
};
use tokio::sync::RwLock;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct CacheKey {
    namespace: NamespaceContext,
    key: Key,
}

#[derive(Clone, Debug)]
struct CacheEntry {
    value: Value,
    expires_at: Option<Instant>,
}

#[derive(Debug, Default)]
struct EntryExpiry;

impl Expiry<CacheKey, CacheEntry> for EntryExpiry {
    fn expire_after_create(
        &self,
        _key: &CacheKey,
        value: &CacheEntry,
        created_at: Instant,
    ) -> Option<Duration> {
        value
            .expires_at
            .map(|expires_at| expires_at.saturating_duration_since(created_at))
    }

    fn expire_after_read(
        &self,
        _key: &CacheKey,
        _value: &CacheEntry,
        _read_at: Instant,
        duration_until_expiry: Option<Duration>,
        _last_modified_at: Instant,
    ) -> Option<Duration> {
        duration_until_expiry
    }

    fn expire_after_update(
        &self,
        _key: &CacheKey,
        value: &CacheEntry,
        updated_at: Instant,
        _duration_until_expiry: Option<Duration>,
    ) -> Option<Duration> {
        value
            .expires_at
            .map(|expires_at| expires_at.saturating_duration_since(updated_at))
    }
}

/// Runtime policy for the in-memory store.
#[derive(Clone, Debug, Default)]
pub struct MemoryStoreConfig {
    /// Maximum number of entries retained by Moka. `None` means unbounded.
    pub max_capacity: Option<u64>,
    /// Optional Moka time-to-idle policy. Command TTLs remain independent.
    pub time_to_idle: Option<Duration>,
}

impl MemoryStoreConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_max_capacity(mut self, max_capacity: u64) -> Self {
        self.max_capacity = Some(max_capacity);
        self
    }

    pub fn with_time_to_idle(mut self, time_to_idle: Duration) -> Self {
        self.time_to_idle = Some(time_to_idle);
        self
    }
}

/// In-memory implementation of [`Store`].
///
/// A single Moka cache stores composite namespace/key entries, providing
/// concurrent access, command TTL expiry, and optional capacity or idle
/// eviction. A store-level async lock coordinates namespace-wide operations so
/// callers cannot observe a partially completed move or whole-namespace delete.
/// Batch insertion remains incrementally visible as permitted by [`Store`].
#[derive(Debug)]
pub struct MemoryStore {
    cache: Cache<CacheKey, CacheEntry>,
    operations: RwLock<()>,
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::with_config(MemoryStoreConfig::default())
    }
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_config(config: MemoryStoreConfig) -> Self {
        let mut builder = Cache::builder().expire_after(EntryExpiry);
        if let Some(max_capacity) = config.max_capacity {
            builder = builder.max_capacity(max_capacity);
        }
        if let Some(time_to_idle) = config.time_to_idle {
            builder = builder.time_to_idle(time_to_idle);
        }
        Self {
            cache: builder.build(),
            operations: RwLock::new(()),
        }
    }

    fn cache_key(namespace: NamespaceContext, key: Key) -> CacheKey {
        CacheKey { namespace, key }
    }

    fn expires_at(ttl: Option<Duration>) -> Option<Instant> {
        ttl.and_then(|duration| Instant::now().checked_add(duration))
    }

    fn stored_value(entry: &CacheEntry, now: Instant) -> StoredValue {
        StoredValue {
            value: entry.value.clone(),
            remaining_ttl: entry
                .expires_at
                .map(|expires_at| expires_at.saturating_duration_since(now)),
        }
    }

    fn matching_entries(
        &self,
        namespace: &NamespaceContext,
        pattern: &KeyPattern,
    ) -> Vec<(CacheKey, CacheEntry)> {
        self.cache
            .iter()
            .filter(|(cache_key, _)| {
                &cache_key.namespace == namespace && pattern.matches(&cache_key.key)
            })
            .map(|(cache_key, entry)| ((*cache_key).clone(), entry))
            .collect()
    }
}

#[async_trait]
impl Store for MemoryStore {
    async fn get(&self, namespace: &NamespaceContext, key: &Key) -> Result<Option<StoredValue>> {
        let _operation = self.operations.read().await;
        let cache_key = Self::cache_key(namespace.clone(), key.clone());
        let Some(entry) = self.cache.get(&cache_key).await else {
            self.cache.invalidate(&cache_key).await;
            return Ok(None);
        };
        Ok(Some(Self::stored_value(&entry, Instant::now())))
    }

    async fn set(
        &self,
        namespace: NamespaceContext,
        key: Key,
        value: Value,
        ttl: Option<Duration>,
    ) -> Result<()> {
        let _operation = self.operations.read().await;
        self.cache
            .insert(
                Self::cache_key(namespace, key),
                CacheEntry {
                    value,
                    expires_at: Self::expires_at(ttl),
                },
            )
            .await;
        Ok(())
    }

    async fn set_batch(
        &self,
        namespace: NamespaceContext,
        entries: Vec<(Key, Value)>,
        ttl: Option<Duration>,
    ) -> Result<()> {
        let _operation = self.operations.read().await;
        let expires_at = Self::expires_at(ttl);
        for (key, value) in entries {
            self.cache
                .insert(
                    Self::cache_key(namespace.clone(), key),
                    CacheEntry { value, expires_at },
                )
                .await;
        }
        Ok(())
    }

    async fn delete(
        &self,
        namespace: &NamespaceContext,
        pattern: Option<&KeyPattern>,
    ) -> Result<u64> {
        match pattern {
            None => {
                let _operation = self.operations.write().await;
                let keys: Vec<_> = self
                    .cache
                    .iter()
                    .filter(|(cache_key, _)| &cache_key.namespace == namespace)
                    .map(|(cache_key, _)| (*cache_key).clone())
                    .collect();
                for key in &keys {
                    self.cache.invalidate(key).await;
                }
                Ok(keys.len() as u64)
            }
            Some(pattern) => {
                let _operation = self.operations.read().await;
                let entries = self.matching_entries(namespace, pattern);
                for (key, _) in &entries {
                    self.cache.invalidate(key).await;
                }
                Ok(entries.len() as u64)
            }
        }
    }

    async fn scan(
        &self,
        namespace: &NamespaceContext,
        pattern: &KeyPattern,
    ) -> Result<Vec<(Key, StoredValue)>> {
        let _operation = self.operations.read().await;
        let now = Instant::now();
        Ok(self
            .matching_entries(namespace, pattern)
            .into_iter()
            .map(|(cache_key, entry)| (cache_key.key, Self::stored_value(&entry, now)))
            .collect())
    }

    async fn move_namespace(
        &self,
        source: &NamespaceContext,
        destination: NamespaceContext,
    ) -> Result<()> {
        if source == &destination {
            return Ok(());
        }
        let _operation = self.operations.write().await;
        let destination_exists = self
            .cache
            .iter()
            .any(|(cache_key, _)| cache_key.namespace == destination);
        if destination_exists {
            return Err(Error::NamespaceExists(destination.to_string()));
        }
        let entries: Vec<_> = self
            .cache
            .iter()
            .filter(|(cache_key, _)| &cache_key.namespace == source)
            .map(|(cache_key, entry)| ((*cache_key).clone(), entry))
            .collect();
        for (cache_key, entry) in &entries {
            self.cache
                .insert(
                    Self::cache_key(destination.clone(), cache_key.key.clone()),
                    entry.clone(),
                )
                .await;
        }
        for (cache_key, _) in &entries {
            self.cache.invalidate(cache_key).await;
        }
        Ok(())
    }

    async fn len(&self) -> Result<u64> {
        let _operation = self.operations.read().await;
        self.cache.run_pending_tasks().await;
        Ok(self.cache.iter().count() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::sync::Barrier;

    fn namespace(name: &str) -> NamespaceContext {
        NamespaceContext::new(name).unwrap()
    }

    fn key(name: &str) -> Key {
        Key::new(name).unwrap()
    }

    #[test]
    fn expiry_policy_covers_ttl_transitions_and_reads() {
        let policy = EntryExpiry;
        let cache_key = CacheKey {
            namespace: namespace("/values"),
            key: key("k"),
        };
        let created_at = Instant::now();
        let no_ttl = CacheEntry {
            value: json!("no ttl"),
            expires_at: None,
        };
        assert_eq!(
            policy.expire_after_create(&cache_key, &no_ttl, created_at),
            None
        );

        let zero_ttl = CacheEntry {
            value: json!("zero ttl"),
            expires_at: Some(created_at),
        };
        assert_eq!(
            policy.expire_after_create(&cache_key, &zero_ttl, created_at),
            Some(Duration::ZERO)
        );

        let finite_ttl = CacheEntry {
            value: json!("finite ttl"),
            expires_at: Some(created_at + Duration::from_secs(30)),
        };
        assert_eq!(
            policy.expire_after_create(&cache_key, &finite_ttl, created_at),
            Some(Duration::from_secs(30))
        );
        assert_eq!(
            policy.expire_after_update(
                &cache_key,
                &finite_ttl,
                created_at + Duration::from_secs(5),
                None,
            ),
            Some(Duration::from_secs(25))
        );

        let reset_ttl = CacheEntry {
            value: json!("reset ttl"),
            expires_at: Some(created_at + Duration::from_secs(90)),
        };
        assert_eq!(
            policy.expire_after_update(&cache_key, &reset_ttl, created_at, Some(Duration::ZERO)),
            Some(Duration::from_secs(90))
        );
        assert_eq!(
            policy.expire_after_update(
                &cache_key,
                &no_ttl,
                created_at,
                Some(Duration::from_secs(1))
            ),
            None
        );
        assert_eq!(
            policy.expire_after_read(
                &cache_key,
                &finite_ttl,
                created_at,
                Some(Duration::from_secs(20)),
                created_at,
            ),
            Some(Duration::from_secs(20))
        );
        assert_eq!(
            policy.expire_after_read(&cache_key, &no_ttl, created_at, None, created_at),
            None
        );
    }

    #[tokio::test]
    async fn batch_write_is_visible_as_one_store_operation() {
        let store = MemoryStore::new();
        let namespace = namespace("/values");
        store
            .set_batch(
                namespace.clone(),
                vec![(key("one"), json!(1)), (key("two"), json!(2))],
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            store
                .scan(&namespace, &KeyPattern::new("*").unwrap())
                .await
                .unwrap()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn get_removes_expired_entry() {
        let store = MemoryStore::new();
        let namespace = namespace("/values");
        let entry_key = key("k");
        store
            .set(
                namespace.clone(),
                entry_key.clone(),
                json!("v"),
                Some(Duration::from_millis(10)),
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(store.get(&namespace, &entry_key).await.unwrap().is_none());
        assert_eq!(store.len().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn scan_removes_expired_entries() {
        let store = MemoryStore::new();
        let namespace = namespace("/values");
        store
            .set(
                namespace.clone(),
                key("a"),
                json!(1),
                Some(Duration::from_millis(10)),
            )
            .await
            .unwrap();
        store
            .set(namespace.clone(), key("b"), json!(2), None)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let entries = store
            .scan(&namespace, &KeyPattern::new("*").unwrap())
            .await
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, key("b"));
    }

    #[tokio::test]
    async fn len_excludes_expired_entries_after_maintenance() {
        let store = MemoryStore::new();
        let namespace = namespace("/values");
        store
            .set(
                namespace.clone(),
                key("a"),
                json!(1),
                Some(Duration::from_millis(10)),
            )
            .await
            .unwrap();
        store
            .set(namespace, key("b"), json!(2), None)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(store.len().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn ttl_is_reset_on_overwrite_and_can_be_cleared() {
        let store = MemoryStore::new();
        let namespace = namespace("/values");
        let entry_key = key("k");
        store
            .set(
                namespace.clone(),
                entry_key.clone(),
                json!(1),
                Some(Duration::from_millis(10)),
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        store
            .set(
                namespace.clone(),
                entry_key.clone(),
                json!(2),
                Some(Duration::from_secs(1)),
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(
            store
                .get(&namespace, &entry_key)
                .await
                .unwrap()
                .unwrap()
                .value,
            json!(2)
        );
        store
            .set(namespace.clone(), entry_key.clone(), json!(3), None)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            store
                .get(&namespace, &entry_key)
                .await
                .unwrap()
                .unwrap()
                .value,
            json!(3)
        );
    }

    #[tokio::test]
    async fn zero_ttl_is_immediately_unavailable() {
        let store = MemoryStore::new();
        let namespace = namespace("/values");
        let entry_key = key("k");
        store
            .set(
                namespace.clone(),
                entry_key.clone(),
                json!(1),
                Some(Duration::ZERO),
            )
            .await
            .unwrap();
        assert!(store.get(&namespace, &entry_key).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_by_pattern_returns_count() {
        let store = MemoryStore::new();
        let namespace = namespace("/values");
        store
            .set(namespace.clone(), key("a1"), json!(1), None)
            .await
            .unwrap();
        store
            .set(namespace.clone(), key("a2"), json!(2), None)
            .await
            .unwrap();
        store
            .set(namespace.clone(), key("b1"), json!(3), None)
            .await
            .unwrap();
        assert_eq!(
            store
                .delete(&namespace, Some(&KeyPattern::new("a*").unwrap()))
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            store
                .scan(&namespace, &KeyPattern::new("*").unwrap())
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn delete_namespace_reclaims_entries() {
        let store = MemoryStore::new();
        let namespace = namespace("/values");
        store
            .set(namespace.clone(), key("a"), json!(1), None)
            .await
            .unwrap();
        assert_eq!(store.delete(&namespace, None).await.unwrap(), 1);
        store
            .set(namespace.clone(), key("b"), json!(2), None)
            .await
            .unwrap();
        assert_eq!(
            store
                .scan(&namespace, &KeyPattern::new("*").unwrap())
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn move_namespace_errors_when_destination_exists() {
        let store = MemoryStore::new();
        let src = namespace("/src");
        let dst = namespace("/dst");
        store
            .set(src.clone(), key("a"), json!(1), None)
            .await
            .unwrap();
        store
            .set(dst.clone(), key("b"), json!(2), None)
            .await
            .unwrap();
        assert!(matches!(
            store.move_namespace(&src, dst).await.unwrap_err(),
            Error::NamespaceExists(_)
        ));
        assert!(store.get(&src, &key("a")).await.unwrap().is_some());
        assert_eq!(
            store
                .get(&namespace("/dst"), &key("b"))
                .await
                .unwrap()
                .unwrap()
                .value,
            json!(2)
        );
    }

    #[tokio::test]
    async fn move_namespace_is_ok_when_source_missing() {
        let store = MemoryStore::new();
        store
            .move_namespace(&namespace("/src"), namespace("/dst"))
            .await
            .unwrap();
        assert_eq!(store.len().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn move_namespace_is_ok_when_same_namespace() {
        let store = MemoryStore::new();
        let namespace = namespace("/same");
        store
            .set(namespace.clone(), key("a"), json!(1), None)
            .await
            .unwrap();
        store
            .move_namespace(&namespace, namespace.clone())
            .await
            .unwrap();
        assert!(store.get(&namespace, &key("a")).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn move_namespace_moves_all_keys_and_preserves_ttl() {
        let store = MemoryStore::new();
        let src = namespace("/src");
        let dst = namespace("/dst");
        store
            .set(
                src.clone(),
                key("a"),
                json!(1),
                Some(Duration::from_secs(1)),
            )
            .await
            .unwrap();
        store
            .set(src.clone(), key("b"), json!(2), None)
            .await
            .unwrap();
        let remaining_before_move = store
            .get(&src, &key("a"))
            .await
            .unwrap()
            .unwrap()
            .remaining_ttl
            .unwrap();
        store.move_namespace(&src, dst.clone()).await.unwrap();
        assert_eq!(store.len().await.unwrap(), 2);
        assert!(store.get(&src, &key("a")).await.unwrap().is_none());
        let moved = store.get(&dst, &key("a")).await.unwrap().unwrap();
        assert!(
            moved
                .remaining_ttl
                .is_some_and(|remaining| { remaining <= remaining_before_move })
        );
    }

    #[tokio::test]
    async fn configured_time_to_idle_refreshes_point_reads() {
        let store = MemoryStore::with_config(
            MemoryStoreConfig::new().with_time_to_idle(Duration::from_millis(250)),
        );
        let namespace = namespace("/values");
        let entry_key = key("k");
        store
            .set(namespace.clone(), entry_key.clone(), json!(1), None)
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(75)).await;
        assert!(store.get(&namespace, &entry_key).await.unwrap().is_some());
        tokio::time::sleep(Duration::from_millis(75)).await;
        assert!(store.get(&namespace, &entry_key).await.unwrap().is_some());
        tokio::time::sleep(Duration::from_millis(350)).await;
        assert!(store.get(&namespace, &entry_key).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn configured_capacity_evicts_entries() {
        let store = MemoryStore::with_config(MemoryStoreConfig::new().with_max_capacity(1));
        let namespace = namespace("/values");
        store
            .set(namespace.clone(), key("a"), json!(1), None)
            .await
            .unwrap();
        store
            .set(namespace.clone(), key("b"), json!(2), None)
            .await
            .unwrap();
        assert_eq!(store.len().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn concurrent_set_get_storm_is_race_free() {
        let store = std::sync::Arc::new(MemoryStore::new());
        let namespace = namespace("/storm");
        let mut set = tokio::task::JoinSet::new();
        for i in 0..100 {
            let store = store.clone();
            let namespace = namespace.clone();
            set.spawn(async move {
                store
                    .set(namespace, key(&format!("key{}", i % 10)), json!(i), None)
                    .await
                    .unwrap();
            });
        }
        while let Some(result) = set.join_next().await {
            result.unwrap();
        }
        for i in 0..10 {
            assert!(
                store
                    .get(&namespace, &key(&format!("key{}", i)))
                    .await
                    .unwrap()
                    .is_some()
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn namespace_delete_waits_for_queued_writer() {
        let store = std::sync::Arc::new(MemoryStore::new());
        let namespace = namespace("/values");
        store
            .set(namespace.clone(), key("before"), json!(1), None)
            .await
            .unwrap();
        let initial = store
            .scan(&namespace, &KeyPattern::new("*").unwrap())
            .await
            .unwrap();
        assert_eq!(initial.len(), 1);

        let read_gate = store.operations.read().await;
        let delete_store = store.clone();
        let delete_namespace = namespace.clone();
        let mut delete =
            tokio::spawn(async move { delete_store.delete(&delete_namespace, None).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut delete)
                .await
                .is_err()
        );

        let writer_store = store.clone();
        let writer_namespace = namespace.clone();
        let mut writer = tokio::spawn(async move {
            writer_store
                .set(writer_namespace, key("after"), json!(2), None)
                .await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut writer)
                .await
                .is_err()
        );

        drop(read_gate);
        assert_eq!(delete.await.unwrap().unwrap(), 1);
        writer.await.unwrap().unwrap();

        let after = store
            .scan(&namespace, &KeyPattern::new("*").unwrap())
            .await
            .unwrap();
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].0, key("after"));
        assert_eq!(after[0].1.value, json!(2));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn namespace_move_excludes_queued_reader_and_writer() {
        let store = std::sync::Arc::new(MemoryStore::new());
        let src = namespace("/src");
        let dst = namespace("/dst");
        for i in 0..3 {
            store
                .set(src.clone(), key(&format!("k{i}")), json!(i), None)
                .await
                .unwrap();
        }
        let initial = store
            .scan(&src, &KeyPattern::new("*").unwrap())
            .await
            .unwrap();
        assert_eq!(initial.len(), 3);
        assert!(
            store
                .scan(&dst, &KeyPattern::new("*").unwrap())
                .await
                .unwrap()
                .is_empty()
        );

        let write_gate = store.operations.write().await;
        let move_store = store.clone();
        let move_src = src.clone();
        let move_dst = dst.clone();
        let mut move_task =
            tokio::spawn(async move { move_store.move_namespace(&move_src, move_dst).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut move_task)
                .await
                .is_err()
        );

        let readers_ready = std::sync::Arc::new(Barrier::new(3));
        let reader_store = store.clone();
        let reader_src = src.clone();
        let reader_dst = dst.clone();
        let reader_ready = readers_ready.clone();
        let mut reader = tokio::spawn(async move {
            reader_ready.wait().await;
            let source = reader_store
                .scan(&reader_src, &KeyPattern::new("*").unwrap())
                .await
                .unwrap();
            let destination = reader_store
                .scan(&reader_dst, &KeyPattern::new("*").unwrap())
                .await
                .unwrap();
            (source, destination)
        });

        let writer_store = store.clone();
        let writer_src = src.clone();
        let writer_ready = readers_ready.clone();
        let mut writer = tokio::spawn(async move {
            writer_ready.wait().await;
            writer_store
                .set(writer_src, key("late"), json!(99), None)
                .await
        });
        readers_ready.wait().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut reader)
                .await
                .is_err()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut writer)
                .await
                .is_err()
        );

        drop(write_gate);
        move_task.await.unwrap().unwrap();
        let (reader_source, reader_destination) = reader.await.unwrap();
        writer.await.unwrap().unwrap();

        assert!(
            reader_source
                .iter()
                .all(|(entry_key, _)| ![key("k0"), key("k1"), key("k2")].contains(entry_key))
        );
        assert_eq!(reader_destination.len(), 3);
        let final_source = store
            .scan(&src, &KeyPattern::new("*").unwrap())
            .await
            .unwrap();
        let final_destination = store
            .scan(&dst, &KeyPattern::new("*").unwrap())
            .await
            .unwrap();
        assert_eq!(final_source.len(), 1);
        assert_eq!(final_source[0].0, key("late"));
        assert_eq!(final_source[0].1.value, json!(99));
        assert_eq!(final_destination.len(), 3);
        for i in 0..3 {
            assert_eq!(
                store
                    .get(&dst, &key(&format!("k{i}")))
                    .await
                    .unwrap()
                    .unwrap()
                    .value,
                json!(i)
            );
        }
    }
}
