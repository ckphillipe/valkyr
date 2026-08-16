use crate::{Key, KeyPattern, NamespaceContext};
use moka::{Expiry, future::Cache};
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct CacheKey {
    namespace: NamespaceContext,
    key: Key,
}

#[derive(Clone, Copy, Debug)]
struct Marker {
    ttl: Duration,
}

#[derive(Debug, Default)]
struct MarkerExpiry;

impl Expiry<CacheKey, Marker> for MarkerExpiry {
    fn expire_after_create(
        &self,
        _key: &CacheKey,
        value: &Marker,
        _created_at: Instant,
    ) -> Option<Duration> {
        // Moka performs the expiration-time addition with saturation. Keep
        // the protocol duration relative so u64::MAX seconds cannot panic
        // before Moka applies that safe arithmetic.
        Some(value.ttl)
    }

    fn expire_after_read(
        &self,
        _key: &CacheKey,
        _value: &Marker,
        _read_at: Instant,
        duration_until_expiry: Option<Duration>,
        _last_modified_at: Instant,
    ) -> Option<Duration> {
        duration_until_expiry
    }

    fn expire_after_update(
        &self,
        _key: &CacheKey,
        value: &Marker,
        _updated_at: Instant,
        _duration_until_expiry: Option<Duration>,
    ) -> Option<Duration> {
        Some(value.ttl)
    }
}

#[derive(Debug)]
pub struct MissCache {
    cache: Cache<CacheKey, Marker>,
}

impl Default for MissCache {
    fn default() -> Self {
        Self::new()
    }
}

impl MissCache {
    pub fn new() -> Self {
        Self {
            cache: Cache::builder()
                .expire_after(MarkerExpiry)
                .support_invalidation_closures()
                .build(),
        }
    }

    pub async fn contains(&self, namespace: &NamespaceContext, key: &Key) -> bool {
        self.cache
            .get(&CacheKey {
                namespace: namespace.clone(),
                key: key.clone(),
            })
            .await
            .is_some()
    }

    pub async fn insert(&self, namespace: NamespaceContext, key: Key, ttl: Duration) {
        if ttl.is_zero() {
            return;
        }
        self.cache
            .insert(CacheKey { namespace, key }, Marker { ttl })
            .await;
    }

    pub async fn invalidate_exact(&self, namespace: &NamespaceContext, key: &Key) {
        self.cache
            .invalidate(&CacheKey {
                namespace: namespace.clone(),
                key: key.clone(),
            })
            .await;
    }

    pub async fn invalidate_namespace(&self, namespace: &NamespaceContext) {
        let namespace = namespace.clone();
        let _ = self
            .cache
            .invalidate_entries_if(move |key, _| key.namespace == namespace);
    }

    pub async fn invalidate_pattern(
        &self,
        namespace: &NamespaceContext,
        pattern: Option<&KeyPattern>,
    ) {
        let namespace = namespace.clone();
        let pattern = pattern
            .cloned()
            .unwrap_or_else(|| KeyPattern::new("*").expect("wildcard is valid"));
        let _ = self.cache.invalidate_entries_if(move |key, _| {
            key.namespace == namespace && pattern.matches(&key.key)
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn entries_expire_and_namespace_invalidation_is_scoped() {
        let cache = MissCache::new();
        let first = NamespaceContext::new("/first").unwrap();
        let second = NamespaceContext::new("/second").unwrap();
        let key = Key::new("missing").unwrap();
        cache
            .insert(first.clone(), key.clone(), Duration::from_millis(10))
            .await;
        cache
            .insert(second.clone(), key.clone(), Duration::from_secs(1))
            .await;
        assert!(cache.contains(&first, &key).await);
        assert!(cache.contains(&second, &key).await);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!cache.contains(&first, &key).await);
        cache.invalidate_namespace(&second).await;
        assert!(!cache.contains(&second, &key).await);
    }

    #[tokio::test]
    async fn pattern_invalidation_removes_only_matching_keys() {
        let cache = MissCache::new();
        let namespace = NamespaceContext::new("/values").unwrap();
        let first = Key::new("one").unwrap();
        let second = Key::new("two").unwrap();
        cache
            .insert(namespace.clone(), first.clone(), Duration::from_secs(1))
            .await;
        cache
            .insert(namespace.clone(), second.clone(), Duration::from_secs(1))
            .await;
        cache
            .invalidate_pattern(&namespace, Some(&KeyPattern::new("o*").unwrap()))
            .await;
        assert!(!cache.contains(&namespace, &first).await);
        assert!(cache.contains(&namespace, &second).await);
    }

    #[tokio::test]
    async fn maximum_protocol_ttl_does_not_overflow() {
        let cache = MissCache::new();
        let namespace = NamespaceContext::new("/values").unwrap();
        let key = Key::new("missing").unwrap();
        cache
            .insert(
                namespace.clone(),
                key.clone(),
                Duration::from_secs(u64::MAX),
            )
            .await;
        assert!(cache.contains(&namespace, &key).await);
    }
}
