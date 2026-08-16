use async_trait::async_trait;
use std::{collections::BTreeMap, time::Duration};
use tracing::warn;
use valkyr_client::{ClientBuilder, ServerCommandHandler};
use valkyr_core::{
    Key, KeyPattern, NamespaceContext, Pattern, ServerCommand, ServerResult, SetEntry,
};

use crate::{AdapterError, DatabaseValue, QueryProvider, Result, StorageWriter};

/// Routes streaming Valkyr callbacks to configured database query and storage
/// handlers. This keeps callback protocol mechanics independent of SQL drivers.
pub struct CallbackBridge {
    queries: Vec<std::sync::Arc<dyn QueryProvider>>,
    stores: Vec<std::sync::Arc<dyn StorageWriter>>,
}
impl CallbackBridge {
    pub fn new(
        queries: Vec<std::sync::Arc<dyn QueryProvider>>,
        stores: Vec<std::sync::Arc<dyn StorageWriter>>,
    ) -> Self {
        Self { queries, stores }
    }
    pub fn with_forwarding(
        mut self,
        endpoints: Vec<ClientBuilder>,
        source_endpoint: usize,
    ) -> Self {
        self.stores = self
            .stores
            .into_iter()
            .map(|store| {
                std::sync::Arc::new(ForwardingStorageWriter {
                    store,
                    endpoints: endpoints.clone(),
                    source_endpoint,
                }) as std::sync::Arc<dyn StorageWriter>
            })
            .collect();
        self
    }
    fn store(
        &self,
        namespace: &NamespaceContext,
        pattern: Option<&KeyPattern>,
    ) -> Option<&std::sync::Arc<dyn StorageWriter>> {
        self.stores
            .iter()
            .rev()
            .find(|store| store.matches(namespace, pattern))
    }
}

struct ForwardingStorageWriter {
    store: std::sync::Arc<dyn StorageWriter>,
    endpoints: Vec<ClientBuilder>,
    source_endpoint: usize,
}
impl ForwardingStorageWriter {
    fn forward(&self, mutation: ForwardedMutation) {
        for (index, builder) in self.endpoints.iter().cloned().enumerate() {
            if index == self.source_endpoint {
                continue;
            }
            let mutation = mutation.clone();
            let mutation_name = mutation.name();
            tokio::spawn(async move {
                let result = async {
                    let client = builder.connect().await?;
                    mutation.send(&client).await
                }
                .await;
                if let Err(error) = result {
                    warn!(destination_endpoint = index, mutation = mutation_name, %error, "best-effort store forwarding failed");
                }
            });
        }
    }
}
#[derive(Clone)]
enum ForwardedMutation {
    Set(DatabaseValue),
    SetBatch(NamespaceContext, Vec<SetEntry>, Option<Duration>),
    Delete(NamespaceContext, Option<KeyPattern>),
    Move(NamespaceContext, NamespaceContext),
}
impl ForwardedMutation {
    async fn send(
        self,
        client: &valkyr_client::Client,
    ) -> std::result::Result<(), valkyr_client::ClientError> {
        match self {
            Self::Set(value) => {
                client
                    .set(value.namespace, value.key, value.value, value.ttl)
                    .await
            }
            Self::SetBatch(namespace, entries, ttl) => {
                client.set_batch(namespace, entries, ttl).await
            }
            Self::Delete(namespace, pattern) => client.delete(namespace, pattern).await,
            Self::Move(source, destination) => client.move_namespace(source, destination).await,
        }
    }
    fn name(&self) -> &'static str {
        match self {
            Self::Set(_) => "set",
            Self::SetBatch(..) => "set_batch",
            Self::Delete(..) => "delete",
            Self::Move(..) => "move",
        }
    }
}
#[async_trait]
impl StorageWriter for ForwardingStorageWriter {
    fn matches(&self, namespace: &NamespaceContext, pattern: Option<&KeyPattern>) -> bool {
        self.store.matches(namespace, pattern)
    }
    async fn set(&self, value: DatabaseValue) -> Result<()> {
        self.store.set(value.clone()).await?;
        self.forward(ForwardedMutation::Set(value));
        Ok(())
    }
    async fn set_batch(
        &self,
        namespace: NamespaceContext,
        entries: Vec<(Key, serde_json::Value)>,
        ttl: Option<Duration>,
    ) -> Result<()> {
        self.store
            .set_batch(namespace.clone(), entries.clone(), ttl)
            .await?;
        self.forward(ForwardedMutation::SetBatch(
            namespace,
            entries
                .into_iter()
                .map(|(key, value)| SetEntry { key, value })
                .collect(),
            ttl,
        ));
        Ok(())
    }
    async fn delete(&self, namespace: NamespaceContext, pattern: Option<KeyPattern>) -> Result<()> {
        self.store
            .delete(namespace.clone(), pattern.clone())
            .await?;
        self.forward(ForwardedMutation::Delete(namespace, pattern));
        Ok(())
    }
    async fn move_namespace(
        &self,
        source: NamespaceContext,
        destination: NamespaceContext,
    ) -> Result<()> {
        self.store
            .move_namespace(source.clone(), destination.clone())
            .await?;
        self.forward(ForwardedMutation::Move(source, destination));
        Ok(())
    }
}
#[async_trait]
impl ServerCommandHandler for CallbackBridge {
    async fn handle(&self, command: ServerCommand) -> ServerResult {
        match command {
            ServerCommand::Query {
                request_id,
                namespace,
                key,
            } => {
                let result = match self
                    .queries
                    .iter()
                    .find(|query| query.matches(&namespace, &key))
                {
                    Some(query) => query.query(namespace, key).await,
                    None => Ok(None),
                };
                match result {
                    Ok(Some(value)) => ServerResult::Query {
                        request_id,
                        value: Some(value.value),
                        ttl_seconds: value.ttl.map(|ttl| ttl.as_secs()),
                        error: None,
                    },
                    Ok(None) => ServerResult::Query {
                        request_id,
                        value: None,
                        ttl_seconds: None,
                        error: None,
                    },
                    Err(error) => ServerResult::Query {
                        request_id,
                        value: None,
                        ttl_seconds: None,
                        error: Some(error.to_string()),
                    },
                }
            }
            ServerCommand::PersistSet {
                request_id,
                namespace,
                key,
                value,
                ttl_seconds,
            } => operation(
                request_id,
                match self.store(
                    &namespace,
                    Some(&KeyPattern::new(key.as_str()).expect("validated key")),
                ) {
                    Some(store) => {
                        store
                            .set(DatabaseValue {
                                namespace,
                                key,
                                value,
                                ttl: ttl_seconds.map(Duration::from_secs),
                            })
                            .await
                    }
                    None => Err(AdapterError::Publish(valkyr_client::ClientError::Server(
                        "no matching store".into(),
                    ))),
                },
            ),
            ServerCommand::PersistSetBatch {
                request_id,
                namespace,
                entries,
                ttl_seconds,
            } => operation(
                request_id,
                match entries
                    .first()
                    .and_then(|entry| KeyPattern::new(entry.key.as_str()).ok())
                    .and_then(|pattern| self.store(&namespace, Some(&pattern)))
                {
                    Some(store) => {
                        store
                            .set_batch(
                                namespace,
                                entries
                                    .into_iter()
                                    .map(|entry| (entry.key, entry.value))
                                    .collect(),
                                ttl_seconds.map(Duration::from_secs),
                            )
                            .await
                    }
                    None => Err(AdapterError::Publish(valkyr_client::ClientError::Server(
                        "no matching store".into(),
                    ))),
                },
            ),
            ServerCommand::PersistDelete {
                request_id,
                namespace,
                key_pattern,
            } => operation(
                request_id,
                match self.store(&namespace, key_pattern.as_ref()) {
                    Some(store) => store.delete(namespace, key_pattern).await,
                    None => Err(AdapterError::Publish(valkyr_client::ClientError::Server(
                        "no matching store".into(),
                    ))),
                },
            ),
            ServerCommand::PersistMove {
                request_id,
                source,
                destination,
            } => operation(
                request_id,
                match self.store(&source, None) {
                    Some(store) => store.move_namespace(source, destination).await,
                    None => Err(AdapterError::Publish(valkyr_client::ClientError::Server(
                        "no matching store".into(),
                    ))),
                },
            ),
        }
    }
}
fn operation(request_id: uuid::Uuid, result: Result<()>) -> ServerResult {
    ServerResult::Operation {
        request_id,
        error: result.err().map(|error| error.to_string()),
    }
}
pub(crate) fn route_captures(
    namespace_pattern: &str,
    key_pattern: &str,
    namespace: &NamespaceContext,
    key: &Key,
) -> Option<BTreeMap<String, String>> {
    let mut values = namespace_pattern_captures(namespace_pattern, namespace.as_str())?;
    values.extend(Pattern::new(key_pattern).matches(key.as_str())?);
    values.insert("namespace".into(), namespace.as_str().into());
    values.insert("key".into(), key.as_str().into());
    values.insert("context".into(), namespace.ctx().unwrap_or_default().into());
    Some(values)
}
pub(crate) fn namespace_pattern_matches(pattern: &str, namespace: &str) -> bool {
    namespace_pattern_captures(pattern, namespace).is_some()
}
pub(crate) fn namespace_pattern_captures(
    pattern: &str,
    namespace: &str,
) -> Option<BTreeMap<String, String>> {
    Pattern::new(pattern).matches(namespace).or_else(|| {
        (!pattern.contains('*') && !pattern.contains('{'))
            .then(|| namespace.strip_prefix(pattern))
            .flatten()
            .filter(|suffix| suffix.starts_with("::"))
            .map(|_| BTreeMap::new())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_captures_use_cached_context_semantics() {
        for (text, expected_context) in [
            ("/tenant", ""),
            ("/tenant::user", "user"),
            ("::tenant", ""),
            ("/tenant::", ""),
            ("/tenant::user::region", "user::region"),
        ] {
            let namespace = NamespaceContext::new(text).unwrap();
            let key = Key::new("value").unwrap();
            let captures = route_captures("*", "{field}", &namespace, &key).unwrap();

            assert_eq!(captures["context"], expected_context);
            assert_eq!(captures["field"], "value");
        }
    }
}
