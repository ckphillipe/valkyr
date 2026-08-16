use crate::{
    AdapterError, OpenBaoClient, OpenBaoMapping, OpenBaoValue, QueryConfig, QueryProvider, Result,
    StorageWriter, StoreConfig, decode,
};
use async_trait::async_trait;
use std::{cmp::Ordering, sync::Arc, time::Duration};
use tracing::warn;
use valkyr_client::{ClientBuilder, ServerCommandHandler};
use valkyr_core::{
    Key, KeyPattern, NamespaceContext, Pattern, ServerCommand, ServerResult, validate_context_move,
};

pub struct CallbackBridge {
    queries: Vec<Arc<dyn QueryProvider>>,
    stores: Vec<Arc<dyn StorageWriter>>,
}
impl CallbackBridge {
    pub fn new(queries: Vec<Arc<dyn QueryProvider>>, stores: Vec<Arc<dyn StorageWriter>>) -> Self {
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
                Arc::new(ForwardingStorageWriter {
                    store,
                    endpoints: endpoints.clone(),
                    source_endpoint,
                }) as Arc<dyn StorageWriter>
            })
            .collect();
        self
    }
    fn store(
        &self,
        ns: &NamespaceContext,
        key: Option<&KeyPattern>,
    ) -> Option<&Arc<dyn StorageWriter>> {
        self.stores
            .iter()
            .rev()
            .find(|store| store.matches(ns, key))
    }
}

struct ForwardingStorageWriter {
    store: Arc<dyn StorageWriter>,
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
                    warn!(destination_endpoint = index, mutation = mutation_name, %error, "best-effort OpenBao store forwarding failed");
                }
            });
        }
    }
}
#[derive(Clone)]
enum ForwardedMutation {
    Set(OpenBaoValue),
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
            Self::Delete(namespace, pattern) => client.delete(namespace, pattern).await,
            Self::Move(source, destination) => client.move_namespace(source, destination).await,
        }
    }
    fn name(&self) -> &'static str {
        match self {
            Self::Set(_) => "set",
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
    async fn set(&self, value: OpenBaoValue) -> Result<()> {
        self.store.set(value.clone()).await?;
        self.forward(ForwardedMutation::Set(value));
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
            } => match match self.queries.iter().find(|q| q.matches(&namespace, &key)) {
                Some(query) => query.query(namespace, key).await,
                None => Ok(None),
            } {
                Ok(Some(value)) => ServerResult::Query {
                    request_id,
                    value: Some(value.value),
                    ttl_seconds: value.ttl.map(|v| v.as_secs()),
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
            },
            ServerCommand::PersistSet {
                request_id,
                namespace,
                key,
                value,
                ttl_seconds,
            } => operation(
                request_id,
                match self.store(&namespace, KeyPattern::new(key.as_str()).ok().as_ref()) {
                    Some(w) => {
                        w.set(OpenBaoValue {
                            namespace,
                            key,
                            value,
                            ttl: ttl_seconds.map(Duration::from_secs),
                        })
                        .await
                    }
                    None => Err(AdapterError::Unsupported("no matching store")),
                },
            ),
            ServerCommand::PersistDelete {
                request_id,
                namespace,
                key_pattern,
            } => operation(
                request_id,
                match self.store(&namespace, key_pattern.as_ref()) {
                    Some(w) => w.delete(namespace, key_pattern).await,
                    None => Err(AdapterError::Unsupported("no matching store")),
                },
            ),
            ServerCommand::PersistMove {
                request_id,
                source,
                destination,
            } => operation(
                request_id,
                match self.store(&source, None) {
                    Some(w) => w.move_namespace(source, destination).await,
                    None => Err(AdapterError::Unsupported("no matching store")),
                },
            ),
            ServerCommand::PersistSetBatch { request_id, .. } => operation(
                request_id,
                Err(AdapterError::Unsupported("batch persistence")),
            ),
        }
    }
}
fn operation(request_id: uuid::Uuid, result: Result<()>) -> ServerResult {
    ServerResult::Operation {
        request_id,
        error: result.err().map(|e| e.to_string()),
    }
}

pub struct OpenBaoQueryProvider {
    client: OpenBaoClient,
    mapping: OpenBaoMapping,
    config: QueryConfig,
}

/// Read every persisted value for one exact logical namespace for scheduled cache sync.
pub async fn fetch_provider_values(
    client: &OpenBaoClient,
    mapping: &OpenBaoMapping,
    namespace: NamespaceContext,
) -> Result<Vec<OpenBaoValue>> {
    let collection = match namespace.ctx() {
        None => mapping.root_collection_path(&namespace),
        Some(context) => {
            let Some(index) = client.read(&mapping.index_path(namespace.ns())).await? else {
                return Ok(Vec::new());
            };
            let Some(collection) = index
                .value
                .get("contexts")
                .and_then(|value| value.get(context))
                .and_then(|value| value.as_str())
            else {
                return Ok(Vec::new());
            };
            mapping.context_collection_path(namespace.ns(), collection)
        }
    };
    if OpenBaoMapping::is_auth_namespace(&namespace) {
        return fetch_auth_provider_values(client, mapping, namespace, collection).await;
    }

    let mut values = Vec::new();
    let mut keys = client.list(&collection).await?;
    keys.sort();
    for encoded_key in keys {
        if encoded_key.ends_with('/') {
            return Err(AdapterError::Configuration(
                "OpenBao provider path must contain only keys".into(),
            ));
        }
        let key = Key::new(decode(&encoded_key)?)?;
        let Some(record) = client.read(&format!("{collection}/{encoded_key}")).await? else {
            continue;
        };
        let value = record.value.get("value").cloned().ok_or_else(|| {
            AdapterError::Configuration("OpenBao value document had no value".into())
        })?;
        let ttl = record
            .value
            .get("ttl_seconds")
            .and_then(|value| value.as_u64())
            .map(Duration::from_secs);
        values.push(OpenBaoValue {
            namespace: namespace.clone(),
            key,
            value,
            ttl,
        });
    }
    Ok(values)
}

async fn fetch_auth_provider_values(
    client: &OpenBaoClient,
    mapping: &OpenBaoMapping,
    namespace: NamespaceContext,
    collection: String,
) -> Result<Vec<OpenBaoValue>> {
    let mut paths = Vec::new();
    list_auth_paths(client, &collection, 0, &[], &mut paths).await?;
    let mut values = Vec::new();
    for (path, parts) in paths {
        let Some(record) = client.read(&path).await? else {
            continue;
        };
        let document = OpenBaoMapping::decode_auth_document(&record.value)?;
        let key = Key::new(document.key)?;
        if mapping.auth_key_parts(&key).as_slice() != parts.as_slice() {
            return Err(AdapterError::Configuration(
                "OpenBao auth path digest did not match document key".into(),
            ));
        }
        values.push(OpenBaoValue {
            namespace: namespace.clone(),
            key,
            value: document.value,
            ttl: document.ttl,
        });
    }
    values.sort_by(|left, right| left.key.as_str().cmp(right.key.as_str()));
    Ok(values)
}

async fn list_auth_paths(
    client: &OpenBaoClient,
    path: &str,
    depth: usize,
    parts: &[String],
    paths: &mut Vec<(String, Vec<String>)>,
) -> Result<()> {
    let mut pending = vec![(path.to_owned(), depth, parts.to_vec())];
    while let Some((current, depth, parts)) = pending.pop() {
        let mut children = client.list(&current).await?;
        children.sort();
        for child in children.into_iter().rev() {
            let is_collection = child.ends_with('/');
            let segment = child.strip_suffix('/').unwrap_or(&child);
            if segment.len() != 16
                || !segment
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
            {
                return Err(AdapterError::Configuration(
                    "OpenBao auth provider path had an invalid digest segment".into(),
                ));
            }
            match depth.cmp(&3) {
                Ordering::Less => {
                    if !is_collection {
                        return Err(AdapterError::Configuration(
                            "OpenBao auth provider path ended before four digest segments".into(),
                        ));
                    }
                    let mut next_parts = parts.clone();
                    next_parts.push(segment.to_owned());
                    pending.push((format!("{current}/{segment}"), depth + 1, next_parts));
                }
                Ordering::Equal => {
                    if is_collection {
                        return Err(AdapterError::Configuration(
                            "OpenBao auth provider path had more than four digest segments".into(),
                        ));
                    }
                    let mut complete = parts.clone();
                    complete.push(segment.to_owned());
                    paths.push((format!("{current}/{segment}"), complete));
                }
                Ordering::Greater => {}
            }
        }
    }
    Ok(())
}
impl OpenBaoQueryProvider {
    pub fn new(
        client: OpenBaoClient,
        mapping: OpenBaoMapping,
        config: QueryConfig,
    ) -> Result<Self> {
        Ok(Self {
            client,
            mapping,
            config,
        })
    }
}
#[async_trait]
impl QueryProvider for OpenBaoQueryProvider {
    fn matches(&self, ns: &NamespaceContext, key: &Key) -> bool {
        Pattern::new(&self.config.namespace_pattern)
            .matches(ns.as_str())
            .is_some()
            && Pattern::new(&self.config.key_pattern)
                .matches(key.as_str())
                .is_some()
    }
    async fn query(&self, namespace: NamespaceContext, key: Key) -> Result<Option<OpenBaoValue>> {
        let path = resolve_path(&self.client, &self.mapping, &namespace, &key).await?;
        let data = match self.client.read(&path).await? {
            Some(data) => data,
            None if self.config.on_missing.generate_xchacha20poly1305_key => {
                let mut record = serde_json::json!({"key": format!("{}{}", uuid::Uuid::new_v4().simple(), uuid::Uuid::new_v4().simple())});
                if self.config.on_missing.record_created_unix_seconds {
                    record["created"] = serde_json::json!(
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map_err(|e| AdapterError::Configuration(e.to_string()))?
                            .as_secs()
                    );
                }
                let document = if OpenBaoMapping::is_auth_namespace(&namespace) {
                    OpenBaoMapping::encode_auth_document(&key, record, None)
                } else {
                    serde_json::json!({"value": record})
                };
                if self.client.write(&path, document, Some(0)).await? {
                    self.client
                        .read(&path)
                        .await?
                        .expect("created OpenBao document")
                } else {
                    self.client.read(&path).await?.ok_or_else(|| {
                        AdapterError::Configuration(
                            "OpenBao security-key creation raced with deletion".into(),
                        )
                    })?
                }
            }
            None => return Ok(None),
        };
        let (value, ttl) = if OpenBaoMapping::is_auth_namespace(&namespace) {
            let document = OpenBaoMapping::decode_auth_document(&data.value)?;
            if document.key != key.as_str() {
                return Err(AdapterError::Configuration(
                    "OpenBao auth document key did not match requested key".into(),
                ));
            }
            (document.value, document.ttl)
        } else {
            let value = data.value.get("value").cloned().ok_or_else(|| {
                AdapterError::Configuration("OpenBao value document had no value".into())
            })?;
            let ttl = data
                .value
                .get("ttl_seconds")
                .and_then(|v| v.as_u64())
                .map(Duration::from_secs);
            (value, ttl)
        };
        Ok(Some(OpenBaoValue {
            namespace,
            key,
            value,
            ttl,
        }))
    }
}

pub struct OpenBaoStoreWriter {
    client: OpenBaoClient,
    mapping: OpenBaoMapping,
    config: StoreConfig,
}
impl OpenBaoStoreWriter {
    pub fn new(
        client: OpenBaoClient,
        mapping: OpenBaoMapping,
        config: StoreConfig,
    ) -> Result<Self> {
        Ok(Self {
            client,
            mapping,
            config,
        })
    }
}
#[async_trait]
impl StorageWriter for OpenBaoStoreWriter {
    fn matches(&self, ns: &NamespaceContext, key: Option<&KeyPattern>) -> bool {
        Pattern::new(&self.config.namespace_pattern)
            .matches(ns.as_str())
            .is_some()
            && key.is_none_or(|key| {
                Pattern::new(&self.config.key_pattern)
                    .matches(key.as_str())
                    .is_some()
            })
    }
    async fn set(&self, value: OpenBaoValue) -> Result<()> {
        let path = ensure_path(&self.client, &self.mapping, &value.namespace, &value.key).await?;
        let document = if OpenBaoMapping::is_auth_namespace(&value.namespace) {
            OpenBaoMapping::encode_auth_document(&value.key, value.value, value.ttl)
        } else {
            serde_json::json!({"value": value.value, "ttl_seconds": value.ttl.map(|v| v.as_secs())})
        };
        self.client.write(&path, document, None).await?;
        Ok(())
    }
    async fn delete(
        &self,
        namespace: NamespaceContext,
        key_pattern: Option<KeyPattern>,
    ) -> Result<()> {
        let key = key_pattern.ok_or(AdapterError::Unsupported("namespace delete"))?;
        if key.as_str().ends_with('*') {
            return Err(AdapterError::Unsupported("wildcard delete"));
        }
        let key = Key::new(key.as_str())?;
        let Some(path) =
            resolve_path_optional(&self.client, &self.mapping, &namespace, &key).await?
        else {
            return Ok(());
        };
        self.client.delete(&path).await
    }
    async fn move_namespace(
        &self,
        source: NamespaceContext,
        destination: NamespaceContext,
    ) -> Result<()> {
        if !self.config.allow_context_move {
            return Err(AdapterError::Unsupported("context move"));
        }
        validate_context_move(&source, &destination)?;
        move_context(&self.client, &self.mapping, &source, &destination).await
    }
}
async fn resolve_path(
    client: &OpenBaoClient,
    mapping: &OpenBaoMapping,
    namespace: &NamespaceContext,
    key: &Key,
) -> Result<String> {
    resolve_path_optional(client, mapping, namespace, key)
        .await?
        .ok_or_else(|| AdapterError::Configuration("missing context collection".into()))
}
async fn ensure_path(
    client: &OpenBaoClient,
    mapping: &OpenBaoMapping,
    namespace: &NamespaceContext,
    key: &Key,
) -> Result<String> {
    if namespace.ctx().is_none() {
        return Ok(mapping.locate(namespace, key, None)?.path().into());
    }
    if let Some(path) = resolve_path_optional(client, mapping, namespace, key).await? {
        return Ok(path);
    }
    let context = namespace.ctx().expect("context checked");
    let index_path = mapping.index_path(namespace.ns());
    for _ in 0..8 {
        let current = client.read(&index_path).await?;
        let (mut data, version) = match current {
            Some(index) => (index.value, index.version),
            None => (serde_json::json!({"contexts": {}}), 0),
        };
        let contexts = data
            .get_mut("contexts")
            .and_then(|v| v.as_object_mut())
            .ok_or_else(|| AdapterError::Configuration("OpenBao index was malformed".into()))?;
        let collection = contexts
            .entry(context)
            .or_insert_with(|| serde_json::Value::String(uuid::Uuid::new_v4().to_string()))
            .as_str()
            .ok_or_else(|| {
                AdapterError::Configuration(
                    "OpenBao index contained a non-string collection id".into(),
                )
            })?
            .to_owned();
        if client.write(&index_path, data, Some(version)).await? {
            return Ok(mapping
                .locate(namespace, key, Some(&collection))?
                .path()
                .into());
        }
    }
    Err(AdapterError::Configuration(
        "OpenBao context index update conflicted repeatedly".into(),
    ))
}
async fn resolve_path_optional(
    client: &OpenBaoClient,
    mapping: &OpenBaoMapping,
    namespace: &NamespaceContext,
    key: &Key,
) -> Result<Option<String>> {
    match namespace.ctx() {
        None => Ok(Some(mapping.locate(namespace, key, None)?.path().into())),
        Some(context) => {
            let Some(index) = client.read(&mapping.index_path(namespace.ns())).await? else {
                return Ok(None);
            };
            let collection = index
                .value
                .get("contexts")
                .and_then(|v| v.get(context))
                .and_then(|v| v.as_str());
            Ok(collection
                .map(|id| {
                    mapping
                        .locate(namespace, key, Some(id))
                        .map(|p| p.path().into())
                })
                .transpose()?)
        }
    }
}
async fn move_context(
    client: &OpenBaoClient,
    mapping: &OpenBaoMapping,
    source: &NamespaceContext,
    destination: &NamespaceContext,
) -> Result<()> {
    let source_context = source.ctx().expect("validated");
    let destination_context = destination.ctx().expect("validated");
    let index_path = mapping.index_path(source.ns());
    for _ in 0..8 {
        let Some(index) = client.read(&index_path).await? else {
            return Ok(());
        };
        let mut data = index.value;
        let contexts = data
            .get_mut("contexts")
            .and_then(|v| v.as_object_mut())
            .ok_or_else(|| AdapterError::Configuration("OpenBao index was malformed".into()))?;
        let Some(collection) = contexts.remove(source_context) else {
            return Ok(());
        };
        if contexts.contains_key(destination_context) {
            return Err(AdapterError::NamespaceExists);
        }
        contexts.insert(destination_context.into(), collection);
        if client.write(&index_path, data, Some(index.version)).await? {
            return Ok(());
        }
    }
    Err(AdapterError::Configuration(
        "OpenBao index update conflicted repeatedly".into(),
    ))
}

#[cfg(test)]
#[path = "bridge_tests.rs"]
mod bridge_tests;
