use crate::{Key, KeyPattern, NamespaceContext, Pattern, ProvideOptions};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Instant,
};
use tokio::sync::RwLock;
use uuid::Uuid;

pub type ConnectionId = Uuid;

#[derive(Clone, Debug)]
pub struct ProviderRegistration {
    pub id: Uuid,
    pub owner: ConnectionId,
    pub namespace_pattern: String,
    pub key_pattern: String,
    pub(crate) namespace_matcher: Pattern,
    pub(crate) key_matcher: Pattern,
    pub options: ProvideOptions,
    pub registered_at: Instant,
}

#[derive(Clone, Debug)]
pub struct StoreRegistration {
    pub id: Uuid,
    pub owner: ConnectionId,
    /// Shared adapter identity, used to avoid reflecting mutations to their origin.
    pub adapter_instance: Option<Uuid>,
    pub namespace_pattern: String,
    pub key_pattern: String,
    pub(crate) namespace_matcher: Pattern,
    pub(crate) key_matcher: Pattern,
    pub registered_at: Instant,
}

/// Result of selecting one persistence adapter for every entry in a batch.
#[derive(Clone, Debug)]
pub enum BatchStoreMatch {
    None,
    Store(Box<StoreRegistration>),
    Mixed,
}

/// Tracks connection-scoped provider and persistence registrations. Network
/// transport remains outside core; callers use the returned owner ID to dispatch.
#[derive(Default)]
pub struct Registry {
    providers: RwLock<Vec<ProviderRegistration>>,
    stores: RwLock<Vec<StoreRegistration>>,
    next_provider: AtomicUsize,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register_provider(
        &self,
        owner: ConnectionId,
        namespace_pattern: impl Into<String>,
        key_pattern: impl Into<String>,
        options: impl Into<ProvideOptions>,
    ) -> ProviderRegistration {
        let namespace_pattern = namespace_pattern.into();
        let key_pattern = key_pattern.into();
        let registration = ProviderRegistration {
            id: Uuid::new_v4(),
            owner,
            namespace_matcher: Pattern::new(&namespace_pattern),
            key_matcher: Pattern::new(&key_pattern),
            namespace_pattern,
            key_pattern,
            options: options.into(),
            registered_at: Instant::now(),
        };
        self.providers.write().await.push(registration.clone());
        registration
    }
    pub async fn register_store(
        &self,
        owner: ConnectionId,
        adapter_instance: Option<Uuid>,
        namespace_pattern: impl Into<String>,
        key_pattern: impl Into<String>,
    ) -> StoreRegistration {
        let namespace_pattern = namespace_pattern.into();
        let key_pattern = key_pattern.into();
        let registration = StoreRegistration {
            id: Uuid::new_v4(),
            owner,
            adapter_instance,
            namespace_matcher: Pattern::new(&namespace_pattern),
            key_matcher: Pattern::new(&key_pattern),
            namespace_pattern,
            key_pattern,
            registered_at: Instant::now(),
        };
        self.stores.write().await.push(registration.clone());
        registration
    }
    pub async fn remove_owner(&self, owner: ConnectionId) {
        self.providers
            .write()
            .await
            .retain(|registration| registration.owner != owner);
        self.stores
            .write()
            .await
            .retain(|registration| registration.owner != owner);
    }
    pub async fn provider_for(
        &self,
        namespace: &NamespaceContext,
        key: &Key,
    ) -> Option<ProviderRegistration> {
        let providers = self.providers.read().await;
        let matching_indices: Vec<_> = providers
            .iter()
            .enumerate()
            .filter_map(|(index, registration)| {
                matches_route(
                    &registration.namespace_matcher,
                    &registration.key_matcher,
                    namespace,
                    key,
                )
                .then_some(index)
            })
            .collect();
        (!matching_indices.is_empty()).then(|| {
            providers[matching_indices
                [self.next_provider.fetch_add(1, Ordering::Relaxed) % matching_indices.len()]]
            .clone()
        })
    }
    /// Most recently registered matching store wins. A mutation from an adapter
    /// skips every registration sharing its stable adapter instance ID.
    pub async fn store_for(
        &self,
        namespace: &NamespaceContext,
        pattern: &KeyPattern,
        source_adapter: Option<Uuid>,
    ) -> Option<StoreRegistration> {
        self.stores
            .read()
            .await
            .iter()
            .rev()
            .find(|registration| {
                source_adapter.is_none_or(|source| registration.adapter_instance != Some(source))
                    && patterns_overlap(
                        &registration.namespace_matcher,
                        &registration.key_matcher,
                        namespace,
                        pattern,
                    )
            })
            .cloned()
    }
    /// Select the newest store that matches every key in a batch. If one or
    /// more stores match only a subset, callers must reject the batch instead
    /// of silently splitting persistence across adapters.
    pub async fn store_for_batch(
        &self,
        namespace: &NamespaceContext,
        patterns: &[KeyPattern],
        source_adapter: Option<Uuid>,
    ) -> BatchStoreMatch {
        let stores = self.stores.read().await;
        let candidates: Vec<_> = stores
            .iter()
            .rev()
            .filter(|registration| {
                source_adapter.is_none_or(|source| registration.adapter_instance != Some(source))
            })
            .collect();
        let any_matches = candidates.iter().any(|registration| {
            patterns.iter().any(|pattern| {
                patterns_overlap(
                    &registration.namespace_matcher,
                    &registration.key_matcher,
                    namespace,
                    pattern,
                )
            })
        });
        match candidates.into_iter().find(|registration| {
            patterns.iter().all(|pattern| {
                patterns_overlap(
                    &registration.namespace_matcher,
                    &registration.key_matcher,
                    namespace,
                    pattern,
                )
            })
        }) {
            Some(registration) => BatchStoreMatch::Store(Box::new(registration.clone())),
            None if any_matches => BatchStoreMatch::Mixed,
            None => BatchStoreMatch::None,
        }
    }
    pub async fn provider_count(&self) -> usize {
        self.providers.read().await.len()
    }
}

fn matches_route(
    namespace_pattern: &Pattern,
    key_pattern: &Pattern,
    namespace: &NamespaceContext,
    key: &Key,
) -> bool {
    matches_namespace(namespace_pattern, namespace.as_str())
        && key_pattern.matches(key.as_str()).is_some()
}
fn patterns_overlap(
    namespace_pattern: &Pattern,
    key_pattern: &Pattern,
    namespace: &NamespaceContext,
    key_pattern_to_match: &KeyPattern,
) -> bool {
    matches_namespace(namespace_pattern, namespace.as_str())
        && pattern_overlap(key_pattern, key_pattern_to_match.as_str())
}
fn matches_namespace(pattern: &Pattern, namespace: &str) -> bool {
    pattern.matches(namespace).is_some()
        || (!pattern.has_wildcard_or_capture()
            && namespace
                .strip_prefix(pattern.source())
                .is_some_and(|suffix| suffix.starts_with("::")))
}
fn pattern_overlap(left: &Pattern, right: &str) -> bool {
    left.source() == "*"
        || right == "*"
        || left.matches(right).is_some()
        || Pattern::new(right).matches(left.source()).is_some()
        || left
            .source()
            .strip_suffix('*')
            .is_some_and(|prefix| right.starts_with(prefix))
        || right
            .strip_suffix('*')
            .is_some_and(|prefix| left.source().starts_with(prefix))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn registrations_are_matched_and_cleaned_up_by_owner() {
        let registry = Registry::new();
        let owner = Uuid::new_v4();
        registry
            .register_provider(owner, "/users/*", "profile.{id}", None)
            .await;
        assert!(
            registry
                .provider_for(
                    &NamespaceContext::new("/users/42").unwrap(),
                    &Key::new("profile.42").unwrap()
                )
                .await
                .is_some()
        );
        registry.remove_owner(owner).await;
        assert_eq!(registry.provider_count().await, 0);
    }

    #[tokio::test]
    async fn matching_providers_are_selected_in_rotation() {
        let registry = Registry::new();
        let first = registry
            .register_provider(Uuid::new_v4(), "/values", "entry", None)
            .await;
        let second = registry
            .register_provider(Uuid::new_v4(), "/values", "entry", None)
            .await;
        let namespace = NamespaceContext::new("/values").unwrap();
        let key = Key::new("entry").unwrap();

        assert_eq!(
            registry.provider_for(&namespace, &key).await.unwrap().id,
            first.id
        );
        assert_eq!(
            registry.provider_for(&namespace, &key).await.unwrap().id,
            second.id
        );
        assert_eq!(
            registry.provider_for(&namespace, &key).await.unwrap().id,
            first.id
        );
    }

    #[tokio::test]
    async fn provider_selection_preserves_unit_safe_options() {
        let registry = Registry::new();
        let options = ProvideOptions::new()
            .with_timeout_ms(250)
            .with_miss_ttl_seconds(30);
        registry
            .register_provider(Uuid::new_v4(), "/values", "entry", options)
            .await;
        assert_eq!(
            registry
                .provider_for(
                    &NamespaceContext::new("/values").unwrap(),
                    &Key::new("entry").unwrap()
                )
                .await
                .unwrap()
                .options,
            options
        );
    }

    #[tokio::test]
    async fn batch_selection_rejects_split_store_routes() {
        let registry = Registry::new();
        registry
            .register_store(Uuid::new_v4(), None, "/values", "a*")
            .await;
        registry
            .register_store(Uuid::new_v4(), None, "/values", "b*")
            .await;
        let outcome = registry
            .store_for_batch(
                &NamespaceContext::new("/values").unwrap(),
                &[
                    KeyPattern::new("alpha").unwrap(),
                    KeyPattern::new("beta").unwrap(),
                ],
                None,
            )
            .await;
        assert!(matches!(outcome, BatchStoreMatch::Mixed));
    }

    #[tokio::test]
    async fn newest_matching_store_wins_and_source_adapter_is_excluded() {
        let registry = Registry::new();
        let namespace = NamespaceContext::new("/values").unwrap();
        let pattern = KeyPattern::new("entry").unwrap();
        let first_adapter = Uuid::new_v4();
        let first = registry
            .register_store(Uuid::new_v4(), Some(first_adapter), "/values", "entry")
            .await;
        let newest = registry
            .register_store(Uuid::new_v4(), Some(Uuid::new_v4()), "/values", "entry")
            .await;

        assert_eq!(
            registry
                .store_for(&namespace, &pattern, None)
                .await
                .unwrap()
                .id,
            newest.id
        );
        assert_eq!(
            registry
                .store_for(&namespace, &pattern, Some(newest.adapter_instance.unwrap()))
                .await
                .unwrap()
                .id,
            first.id
        );
    }
}
