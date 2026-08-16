use crate::miss_cache::MissCache;
use crate::{
    AuthInfo, AuthLookup, AuthManager, BatchStoreMatch, Command, ConnectionId, Error, Key,
    KeyPattern, NamespaceContext, Operation, Registry, Response, Result, ServerCommand, SetEntry,
    Stats, Store, ValueCipher,
};
use serde_json::Value;
use std::{
    collections::{BTreeMap, HashMap},
    hash::{Hash, Hasher},
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::Mutex;
#[cfg(test)]
use tokio::sync::Notify;
use tracing::debug;
use uuid::Uuid;

pub const AUTH_NAMESPACE: &str = "/__auth";
pub const SECURITY_NAMESPACE: &str = "/__secrets";
pub const LEASE_NAMESPACE: &str = "/__lease";
const LEASE_DURATION: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SecurityKeyRecord {
    pub key: String,
    pub created: u64,
}

#[derive(Clone, Debug)]
pub struct RequestContext {
    pub owner: ConnectionId,
    pub adapter_instance: Option<Uuid>,
    pub auth: Option<AuthInfo>,
    /// Optional caller-supplied variable values used by `${name}` GET keys.
    pub variables: BTreeMap<String, String>,
}

impl RequestContext {
    pub fn anonymous(owner: ConnectionId) -> Self {
        Self {
            owner,
            adapter_instance: None,
            auth: None,
            variables: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum PendingMutation {
    Set {
        namespace: NamespaceContext,
        key: Key,
        value: Value,
        ttl: Option<Duration>,
    },
    SetBatch {
        namespace: NamespaceContext,
        entries: Vec<SetEntry>,
        ttl: Option<Duration>,
    },
    Delete {
        namespace: NamespaceContext,
        key_pattern: Option<KeyPattern>,
    },
    Move {
        source: NamespaceContext,
        destination: NamespaceContext,
    },
}

#[derive(Clone, Debug)]
pub struct Dispatch {
    pub owner: ConnectionId,
    pub provider_id: Option<Uuid>,
    pub provider_refresh_id: Option<Uuid>,
    pub mutation_generation: u64,
    pub command: ServerCommand,
    pub authentication: bool,
    pub provider_options: Option<ProvideOptions>,
    pub encrypted: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ProvideOptions {
    pub max_rate: Option<u32>,
    pub timeout_ms: u64,
    pub miss_ttl_seconds: u64,
}

impl ProvideOptions {
    pub const fn new() -> Self {
        Self {
            max_rate: None,
            timeout_ms: 0,
            miss_ttl_seconds: 0,
        }
    }

    pub const fn with_max_rate(mut self, max_rate: Option<u32>) -> Self {
        self.max_rate = max_rate;
        self
    }

    pub const fn with_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    pub const fn with_miss_ttl_seconds(mut self, miss_ttl_seconds: u64) -> Self {
        self.miss_ttl_seconds = miss_ttl_seconds;
        self
    }
}

impl From<Option<u32>> for ProvideOptions {
    fn from(max_rate: Option<u32>) -> Self {
        Self {
            max_rate,
            ..Self::default()
        }
    }
}

#[derive(Clone, Debug)]
pub struct BrokerOutcome {
    pub response: Response,
    pub dispatch: Option<Dispatch>,
    pub pending_mutation: Option<PendingMutation>,
    pub authenticated: Option<AuthInfo>,
}

impl BrokerOutcome {
    fn response(response: Response) -> Self {
        Self {
            response,
            dispatch: None,
            pending_mutation: None,
            authenticated: None,
        }
    }
}

/// Stateful command executor shared by all Valkyr transports. Network code
/// performs connection ownership and callback delivery; this type owns the
/// authorization, cache, registration, and mutation ordering rules.
pub struct Broker {
    store: Arc<dyn Store>,
    auth: Option<Arc<AuthManager>>,
    registry: Arc<Registry>,
    requests: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
    provider_limits: Mutex<HashMap<Uuid, ProviderRateLimit>>,
    leases: Mutex<HashMap<String, Lease>>,
    miss_cache: MissCache,
    mutation_shards: [MutationShard; MUTATION_SHARD_COUNT],
    mutation_clock: AtomicU64,
    #[cfg(test)]
    test_miss_insertion_pause: Option<Arc<MissInsertionPause>>,
}

const MUTATION_SHARD_COUNT: usize = 32;
type MutationRoute = (NamespaceContext, Key);

struct MutationShard {
    routes: StdMutex<HashMap<MutationRoute, Arc<MutationRouteState>>>,
}

struct MutationRouteState {
    gate: Mutex<()>,
    generation: AtomicU64,
    provider_refresh_id: Uuid,
}

#[cfg(test)]
struct MissInsertionPause {
    checked: Notify,
    continue_to_insert: Notify,
    inserted: Notify,
    continue_after_insert: Notify,
    mutation_ready: Notify,
    continue_to_mutation: Notify,
    mutation_waiting: Notify,
}

#[derive(Clone, Copy, Debug)]
struct ProviderRateLimit {
    window_started: std::time::Instant,
    requests: u32,
}

#[derive(Clone, Copy, Debug)]
struct Lease {
    owner: Uuid,
    expires_at: std::time::Instant,
}

impl Broker {
    pub fn new(store: Arc<dyn Store>, auth: Option<Arc<AuthManager>>) -> Self {
        Self {
            store,
            auth,
            registry: Arc::new(Registry::new()),
            requests: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            provider_limits: Mutex::new(HashMap::new()),
            leases: Mutex::new(HashMap::new()),
            miss_cache: MissCache::new(),
            mutation_shards: std::array::from_fn(|_| MutationShard {
                routes: StdMutex::new(HashMap::new()),
            }),
            mutation_clock: AtomicU64::new(0),
            #[cfg(test)]
            test_miss_insertion_pause: None,
        }
    }

    #[cfg(test)]
    fn with_miss_insertion_pause(mut self, pause: Arc<MissInsertionPause>) -> Self {
        self.test_miss_insertion_pause = Some(pause);
        self
    }
    pub fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }
    pub fn store(&self) -> &Arc<dyn Store> {
        &self.store
    }

    pub fn schedule_auth_refresh(&self, api_key: &str) -> bool {
        self.auth
            .as_ref()
            .is_some_and(|auth| auth.schedule_refresh(api_key))
    }

    pub async fn authenticate_request(&self, api_key: &str) -> Result<AuthInfo> {
        match self.auth.as_ref().map(|auth| auth.authenticate(api_key)) {
            Some(AuthLookup::Authenticated(info)) => Ok(info),
            _ => Err(Error::AuthenticationFailed),
        }
    }

    pub async fn auth_provider_dispatch(&self, api_key: &str) -> Option<Dispatch> {
        let auth = self.auth.as_ref()?;
        if !auth.take_scheduled_load(api_key) {
            return None;
        }
        let namespace = NamespaceContext::new(AUTH_NAMESPACE).expect("valid auth namespace");
        let key = match Key::new(api_key) {
            Ok(key) => key,
            Err(_) => {
                auth.fail_provider_load(api_key);
                return None;
            }
        };
        let Some(provider) = self.registry.provider_for(&namespace, &key).await else {
            auth.fail_provider_load(api_key);
            return None;
        };
        Some(Dispatch {
            owner: provider.owner,
            provider_id: Some(provider.id),
            provider_refresh_id: None,
            mutation_generation: 0,
            command: ServerCommand::Query {
                request_id: Uuid::new_v4(),
                namespace,
                key,
            },
            authentication: true,
            provider_options: None,
            encrypted: false,
        })
    }

    pub fn complete_auth_provider_load(
        &self,
        api_key: String,
        value: Option<Value>,
    ) -> Option<Duration> {
        self.auth.as_ref()?.complete_provider_load(api_key, value)
    }

    pub fn fail_auth_provider_load(&self, api_key: &str) {
        if let Some(auth) = &self.auth {
            auth.fail_provider_load(api_key);
        }
    }

    pub async fn execute(
        &self,
        command: Command,
        context: RequestContext,
    ) -> Result<BrokerOutcome> {
        self.requests.fetch_add(1, Ordering::Relaxed);
        match command {
            Command::Auth {
                api_key,
                adapter_instance,
            } => self.authenticate(api_key, adapter_instance, context).await,
            Command::Get { namespace, key } => self.get(namespace, key, context).await,
            Command::Set {
                namespace,
                key,
                value,
                ttl_seconds,
            } => {
                self.set(
                    namespace,
                    key,
                    value,
                    ttl_seconds.map(Duration::from_secs),
                    context,
                )
                .await
            }
            Command::SetBatch {
                namespace,
                entries,
                ttl_seconds,
            } => {
                self.set_batch(
                    namespace,
                    entries,
                    ttl_seconds.map(Duration::from_secs),
                    context,
                )
                .await
            }
            Command::Delete {
                namespace,
                key_pattern,
            } => self.delete(namespace, key_pattern, context).await,
            Command::Move {
                source,
                destination,
            } => self.move_namespace(source, destination, context).await,
            Command::Provide {
                namespace_pattern,
                key_pattern,
                max_rate,
                timeout,
                miss_ttl,
            } => {
                if max_rate == Some(0) {
                    return Err(Error::Protocol(
                        "PROVIDE max_rate must be greater than zero".into(),
                    ));
                }
                let namespace = NamespaceContext::new(namespace_pattern.as_str())?;
                if matches!(namespace.as_str(), AUTH_NAMESPACE | SECURITY_NAMESPACE) {
                    self.require_bootstrap(&context)?;
                } else if namespace.as_str() == LEASE_NAMESPACE {
                    return Err(Error::PermissionDenied(namespace.to_string()));
                } else {
                    self.authorize(&context, &namespace, Operation::Provide)?;
                }
                if context.adapter_instance.is_none() {
                    return Err(Error::Protocol(
                        "PROVIDE requires an adapter instance".into(),
                    ));
                }
                self.registry
                    .register_provider(
                        context.owner,
                        namespace_pattern.as_str(),
                        key_pattern.as_str(),
                        ProvideOptions {
                            max_rate,
                            timeout_ms: timeout.unwrap_or_default(),
                            miss_ttl_seconds: miss_ttl.unwrap_or_default(),
                        },
                    )
                    .await;
                Ok(BrokerOutcome::response(Response::Ok))
            }
            Command::Store {
                namespace_pattern,
                key_pattern,
            } => {
                let namespace = NamespaceContext::new(namespace_pattern.as_str())?;
                if matches!(namespace.as_str(), AUTH_NAMESPACE | SECURITY_NAMESPACE) {
                    self.require_bootstrap(&context)?;
                } else if namespace.as_str() == LEASE_NAMESPACE {
                    return Err(Error::PermissionDenied(namespace.to_string()));
                } else {
                    self.authorize(&context, &namespace, Operation::Store)?;
                }
                if context.adapter_instance.is_none() {
                    return Err(Error::Protocol("STORE requires an adapter instance".into()));
                }
                self.registry
                    .register_store(
                        context.owner,
                        context.adapter_instance,
                        namespace_pattern.as_str(),
                        key_pattern.as_str(),
                    )
                    .await;
                Ok(BrokerOutcome::response(Response::Ok))
            }
            Command::Ping => Ok(BrokerOutcome::response(Response::Pong)),
            Command::Stats => Ok(BrokerOutcome::response(Response::Stats(
                self.stats().await?,
            ))),
        }
    }

    pub async fn commit(&self, mutation: PendingMutation) -> Result<()> {
        match mutation {
            PendingMutation::Set {
                namespace,
                key,
                value,
                ttl,
            } => {
                self.store
                    .set(namespace.clone(), key.clone(), value.clone(), ttl)
                    .await?;
                if namespace.as_str() == AUTH_NAMESPACE {
                    if let Some(auth) = &self.auth {
                        auth.replace_auth_record(key.to_string(), value);
                    }
                }
                self.record_exact_mutation(&namespace, &key).await;
                Ok(())
            }
            PendingMutation::SetBatch {
                namespace,
                entries,
                ttl,
            } => {
                let keys: Vec<_> = entries.iter().map(|entry| entry.key.clone()).collect();
                self.store
                    .set_batch(
                        namespace.clone(),
                        entries
                            .into_iter()
                            .map(|entry| (entry.key, entry.value))
                            .collect(),
                        ttl,
                    )
                    .await?;
                for key in keys {
                    self.record_exact_mutation(&namespace, &key).await;
                }
                Ok(())
            }
            PendingMutation::Delete {
                namespace,
                key_pattern,
            } => {
                self.store.delete(&namespace, key_pattern.as_ref()).await?;
                self.record_broad_mutation(&namespace, key_pattern.as_ref())
                    .await;
                Ok(())
            }
            PendingMutation::Move {
                source,
                destination,
            } => {
                self.store
                    .move_namespace(&source, destination.clone())
                    .await?;
                self.record_broad_mutation(&source, None).await;
                self.record_broad_mutation(&destination, None).await;
                Ok(())
            }
        }
    }

    /// Applies a value returned by a provider. Callers must run the resulting
    /// dispatch through a storage adapter, if present, before calling `commit`.
    pub async fn accept_provider_value(
        &self,
        namespace: NamespaceContext,
        key: Key,
        value: Value,
        ttl: Option<Duration>,
        source_adapter: Option<Uuid>,
        encrypted: bool,
    ) -> Result<BrokerOutcome> {
        let value = if encrypted {
            ValueCipher::encrypt(
                &self.security_key(&namespace).await?,
                &namespace,
                &key,
                &value,
            )?
        } else {
            value
        };
        self.prepare_mutation(
            PendingMutation::Set {
                namespace,
                key,
                value,
                ttl,
            },
            source_adapter,
        )
        .await
    }

    pub async fn confirm_provider_miss(
        &self,
        namespace: NamespaceContext,
        key: Key,
        miss_ttl_seconds: u64,
        refresh_generation: u64,
        provider_refresh_id: Uuid,
    ) {
        if miss_ttl_seconds == 0 {
            return;
        }
        let Some(state) = self.active_route_state(&namespace, &key) else {
            return;
        };
        if state.provider_refresh_id != provider_refresh_id
            || state.generation.load(Ordering::Acquire) != refresh_generation
        {
            self.release_provider_refresh(&namespace, &key, provider_refresh_id)
                .await;
            return;
        }
        let Ok(current_value) = self.store.get(&namespace, &key).await else {
            self.release_provider_refresh(&namespace, &key, provider_refresh_id)
                .await;
            return;
        };
        if current_value.is_some() {
            self.release_provider_refresh(&namespace, &key, provider_refresh_id)
                .await;
            return;
        }
        let _coordination = state.gate.lock().await;
        if state.provider_refresh_id != provider_refresh_id
            || state.generation.load(Ordering::Acquire) != refresh_generation
        {
            self.release_provider_refresh(&namespace, &key, provider_refresh_id)
                .await;
            return;
        }
        #[cfg(test)]
        if let Some(pause) = &self.test_miss_insertion_pause {
            pause.checked.notify_one();
            pause.continue_to_insert.notified().await;
        }
        self.miss_cache
            .insert(
                namespace.clone(),
                key.clone(),
                Duration::from_secs(miss_ttl_seconds),
            )
            .await;
        #[cfg(test)]
        if let Some(pause) = &self.test_miss_insertion_pause {
            pause.inserted.notify_one();
            pause.continue_after_insert.notified().await;
        }
        self.release_provider_refresh(&namespace, &key, provider_refresh_id)
            .await;
    }

    pub async fn mutation_generation(&self) -> u64 {
        self.mutation_clock.load(Ordering::Acquire)
    }

    async fn route_mutation_generation(
        &self,
        namespace: &NamespaceContext,
        key: &Key,
    ) -> (u64, Uuid) {
        let state = self.get_or_create_route_state(namespace, key);
        let _coordination = state.gate.lock().await;
        (
            state.generation.load(Ordering::Acquire),
            state.provider_refresh_id,
        )
    }

    async fn record_exact_mutation(&self, namespace: &NamespaceContext, key: &Key) {
        if let Some(state) = self.active_route_state(namespace, key) {
            #[cfg(test)]
            if let Some(pause) = &self.test_miss_insertion_pause {
                pause.mutation_ready.notify_one();
                pause.continue_to_mutation.notified().await;
                pause.mutation_waiting.notify_one();
            }
            let _coordination = state.gate.lock().await;
            state.generation.fetch_add(1, Ordering::AcqRel);
            self.miss_cache.invalidate_exact(namespace, key).await;
        }
        self.mutation_clock.fetch_add(1, Ordering::Release);
    }

    async fn record_broad_mutation(
        &self,
        namespace: &NamespaceContext,
        pattern: Option<&KeyPattern>,
    ) {
        let pattern_text = pattern.map_or("*", KeyPattern::as_str);
        let affected = self.active_routes_in_namespace(namespace);
        for ((route_namespace, route_key), state) in affected {
            if !pattern_matches(pattern_text, route_key.as_str()) {
                continue;
            }
            let _coordination = state.gate.lock().await;
            state.generation.fetch_add(1, Ordering::AcqRel);
            self.miss_cache
                .invalidate_exact(&route_namespace, &route_key)
                .await;
        }
        match pattern {
            Some(pattern) => {
                self.miss_cache
                    .invalidate_pattern(namespace, Some(pattern))
                    .await
            }
            None => self.miss_cache.invalidate_namespace(namespace).await,
        }
        self.mutation_clock.fetch_add(1, Ordering::Release);
    }

    pub async fn release_provider_refresh(
        &self,
        namespace: &NamespaceContext,
        key: &Key,
        provider_refresh_id: Uuid,
    ) {
        let shard = self.mutation_shard(namespace, key);
        let mut routes = self.mutation_shards[shard]
            .routes
            .lock()
            .expect("mutation shard mutex poisoned");
        let route = (namespace.clone(), key.clone());
        if routes
            .get(&route)
            .is_some_and(|state| state.provider_refresh_id == provider_refresh_id)
        {
            routes.remove(&route);
        }
    }

    pub async fn active_route_count(&self) -> usize {
        self.mutation_shards
            .iter()
            .map(|shard| {
                shard
                    .routes
                    .lock()
                    .expect("mutation shard mutex poisoned")
                    .len()
            })
            .sum()
    }

    #[cfg(test)]
    async fn mutation_coordination_size(&self) -> usize {
        self.active_route_count().await
    }

    fn mutation_shard(&self, namespace: &NamespaceContext, key: &Key) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        namespace.hash(&mut hasher);
        key.hash(&mut hasher);
        (hasher.finish() as usize) % MUTATION_SHARD_COUNT
    }

    fn get_or_create_route_state(
        &self,
        namespace: &NamespaceContext,
        key: &Key,
    ) -> Arc<MutationRouteState> {
        let shard = self.mutation_shard(namespace, key);
        let mut routes = self.mutation_shards[shard]
            .routes
            .lock()
            .expect("mutation shard mutex poisoned");
        routes
            .entry((namespace.clone(), key.clone()))
            .or_insert_with(|| {
                Arc::new(MutationRouteState {
                    gate: Mutex::new(()),
                    generation: AtomicU64::new(0),
                    provider_refresh_id: Uuid::new_v4(),
                })
            })
            .clone()
    }

    fn active_route_state(
        &self,
        namespace: &NamespaceContext,
        key: &Key,
    ) -> Option<Arc<MutationRouteState>> {
        let shard = self.mutation_shard(namespace, key);
        self.mutation_shards[shard]
            .routes
            .lock()
            .expect("mutation shard mutex poisoned")
            .get(&(namespace.clone(), key.clone()))
            .cloned()
    }

    fn active_routes_in_namespace(
        &self,
        namespace: &NamespaceContext,
    ) -> Vec<(MutationRoute, Arc<MutationRouteState>)> {
        self.mutation_shards
            .iter()
            .flat_map(|shard| {
                shard
                    .routes
                    .lock()
                    .expect("mutation shard mutex poisoned")
                    .iter()
                    .filter(|((route_namespace, _), _)| route_namespace == namespace)
                    .map(|(route, state)| (route.clone(), state.clone()))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// Charges a provider rate limit for a refresh generation that has already
    /// been admitted by the server. Joiners must not call this method.
    pub async fn provider_retry_after(
        &self,
        provider_id: Option<Uuid>,
        max_rate: Option<u32>,
    ) -> Option<u64> {
        let (Some(provider_id), Some(limit)) = (provider_id, max_rate) else {
            return None;
        };
        self.provider_retry_after_values(provider_id, limit).await
    }

    pub async fn cached_value_response(
        &self,
        namespace: &NamespaceContext,
        key: &Key,
        encrypted: bool,
    ) -> Result<Option<Response>> {
        let Some(stored) = self.store.get(namespace, key).await? else {
            return Ok(None);
        };
        let value = if encrypted {
            ValueCipher::decrypt(
                &self.security_key(namespace).await?,
                namespace,
                key,
                &stored.value,
            )?
        } else {
            stored.value
        };
        Ok(Some(Response::Value {
            value,
            ttl_seconds: stored.remaining_ttl.map(duration_to_seconds),
        }))
    }

    /// Return the provider callback needed to obtain the encryption key for a
    /// namespace, if that key is not cached. Encryption keys are supplied by a
    /// registered `/__secrets` provider; Valkyr never invents a replacement
    /// key because doing so would make previously encrypted data unreadable.
    pub async fn security_key_provider_dispatch(
        &self,
        namespace: &NamespaceContext,
    ) -> Result<Option<Dispatch>> {
        let (security_namespace, scope_key) = security_route(namespace)?;
        if let Some(value) = self.store.get(&security_namespace, &scope_key).await? {
            parse_security_key(&value.value)?;
            return Ok(None);
        }
        let provider = self
            .registry
            .provider_for(&security_namespace, &scope_key)
            .await
            .ok_or_else(|| {
                Error::Encryption(format!(
                    "no /__secrets provider is registered for scope '{}'",
                    scope_key.as_str()
                ))
            })?;
        Ok(Some(Dispatch {
            owner: provider.owner,
            provider_id: Some(provider.id),
            provider_refresh_id: None,
            mutation_generation: 0,
            command: ServerCommand::Query {
                request_id: Uuid::new_v4(),
                namespace: security_namespace,
                key: scope_key,
            },
            authentication: false,
            provider_options: None,
            encrypted: false,
        }))
    }

    /// Perform the authorization portion of an encrypted command before a
    /// transport asks the external security-key provider to do work. The
    /// command is authorized again during normal execution; this guard keeps
    /// unauthorized callers from probing or loading security material.
    pub fn authorize_encrypted_command(
        &self,
        command: &Command,
        context: &RequestContext,
    ) -> Result<()> {
        let authorize = |namespace: &NamespaceContext, operation| {
            if matches!(namespace.as_str(), AUTH_NAMESPACE | SECURITY_NAMESPACE) {
                self.require_bootstrap(context)
            } else {
                self.authorize(context, namespace, operation)
            }
        };
        match command {
            Command::Get { namespace, key } if marked_key(key.as_str()).is_some() => {
                authorize(namespace, Operation::ReadEncrypted)
            }
            Command::Set { namespace, key, .. } if marked_key(key.as_str()).is_some() => {
                authorize(namespace, Operation::WriteEncrypted)
            }
            Command::SetBatch {
                namespace, entries, ..
            } => {
                for entry in entries {
                    if marked_key(entry.key.as_str()).is_some() {
                        authorize(namespace, Operation::WriteEncrypted)?;
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    pub async fn stats(&self) -> Result<Stats> {
        Ok(Stats {
            requests: self.requests.load(Ordering::Relaxed),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            values: self.store.len().await?,
        })
    }

    async fn authenticate(
        &self,
        api_key: String,
        _adapter_instance: Option<Uuid>,
        _context: RequestContext,
    ) -> Result<BrokerOutcome> {
        let Some(auth) = &self.auth else {
            return Ok(BrokerOutcome::response(Response::AuthFailure {
                message: "authentication is not configured".into(),
            }));
        };
        let lookup = auth.authenticate(&api_key);
        match lookup {
            AuthLookup::Authenticated(info) => Ok(BrokerOutcome {
                response: Response::AuthSuccess {
                    client_id: info.client_id.clone(),
                    session_ttl_seconds: auth.session_timeout().as_secs(),
                },
                dispatch: self.auth_provider_dispatch(&api_key).await,
                pending_mutation: None,
                authenticated: Some(info),
            }),
            AuthLookup::Pending if auth.take_store_load(&api_key) => {
                let namespace =
                    NamespaceContext::new(AUTH_NAMESPACE).expect("valid auth namespace");
                let key = Key::new(&api_key)?;
                let value = match self.store.get(&namespace, &key).await {
                    Ok(value) => value,
                    Err(error) => {
                        auth.fail_provider_load(&api_key);
                        return Err(error);
                    }
                };
                let Some(value) = value else {
                    return Ok(BrokerOutcome {
                        response: Response::AuthPending { retry_after_ms: 10 },
                        dispatch: self.auth_provider_dispatch(&api_key).await,
                        pending_mutation: None,
                        authenticated: None,
                    });
                };
                match auth.complete_store_load(api_key.clone(), value.value) {
                    AuthLookup::Authenticated(info) => Ok(BrokerOutcome {
                        response: Response::AuthSuccess {
                            client_id: info.client_id.clone(),
                            session_ttl_seconds: auth.session_timeout().as_secs(),
                        },
                        dispatch: None,
                        pending_mutation: None,
                        authenticated: Some(info),
                    }),
                    AuthLookup::Pending => Ok(BrokerOutcome {
                        response: Response::AuthPending { retry_after_ms: 10 },
                        dispatch: self.auth_provider_dispatch(&api_key).await,
                        pending_mutation: None,
                        authenticated: None,
                    }),
                    AuthLookup::Rejected => Ok(BrokerOutcome::response(Response::AuthFailure {
                        message: "invalid API key".into(),
                    })),
                }
            }
            AuthLookup::Pending => Ok(BrokerOutcome::response(Response::AuthPending {
                retry_after_ms: 10,
            })),
            AuthLookup::Rejected => Ok(BrokerOutcome::response(Response::AuthFailure {
                message: "invalid API key".into(),
            })),
        }
    }

    async fn get(
        &self,
        namespace: NamespaceContext,
        key: Key,
        context: RequestContext,
    ) -> Result<BrokerOutcome> {
        if namespace.as_str() == LEASE_NAMESPACE {
            return self.acquire_lease(key, &context).await;
        }
        let raw_key_is_encrypted = marked_key(key.as_str()).is_some();
        if matches!(namespace.as_str(), AUTH_NAMESPACE | SECURITY_NAMESPACE) {
            self.require_bootstrap(&context)?;
        } else {
            self.authorize(
                &context,
                &namespace,
                if raw_key_is_encrypted {
                    Operation::ReadEncrypted
                } else {
                    Operation::Read
                },
            )?;
        }
        let key = self.resolve_key(&namespace, &key, &context).await?;
        let (key, encrypted) = normalize_marked_key(key)?;
        if !matches!(namespace.as_str(), AUTH_NAMESPACE | SECURITY_NAMESPACE)
            && encrypted != raw_key_is_encrypted
        {
            self.authorize(
                &context,
                &namespace,
                if encrypted {
                    Operation::ReadEncrypted
                } else {
                    Operation::Read
                },
            )?;
        }
        let Some(stored) = self.store.get(&namespace, &key).await? else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            debug!(namespace = %namespace, key = %key, "cache miss");
            if self.miss_cache.contains(&namespace, &key).await {
                return Ok(BrokerOutcome::response(Response::Miss {
                    retry_after_ms: 0,
                }));
            }
            return Ok(match self.registry.provider_for(&namespace, &key).await {
                Some(provider) => {
                    let (mutation_generation, provider_refresh_id) =
                        self.route_mutation_generation(&namespace, &key).await;
                    debug!(namespace = %namespace, key = %key, provider = %provider.owner, "selecting provider query");
                    BrokerOutcome {
                        response: Response::Miss { retry_after_ms: 10 },
                        dispatch: Some(Dispatch {
                            owner: provider.owner,
                            provider_id: Some(provider.id),
                            provider_refresh_id: Some(provider_refresh_id),
                            command: ServerCommand::Query {
                                request_id: Uuid::new_v4(),
                                namespace,
                                key,
                            },
                            authentication: false,
                            provider_options: Some(provider.options),
                            encrypted,
                            mutation_generation,
                        }),
                        pending_mutation: None,
                        authenticated: None,
                    }
                }
                None => {
                    debug!(namespace = %namespace, key = %key, "no provider is registered for cache miss");
                    BrokerOutcome::response(Response::Unknown)
                }
            });
        };
        self.hits.fetch_add(1, Ordering::Relaxed);
        debug!(namespace = %namespace, key = %key, "cache hit");
        let value = if encrypted {
            ValueCipher::decrypt(
                &self.security_key(&namespace).await?,
                &namespace,
                &key,
                &stored.value,
            )?
        } else {
            stored.value
        };
        Ok(BrokerOutcome::response(Response::Value {
            value,
            ttl_seconds: stored.remaining_ttl.map(duration_to_seconds),
        }))
    }

    async fn set(
        &self,
        namespace: NamespaceContext,
        key: Key,
        value: Value,
        ttl: Option<Duration>,
        context: RequestContext,
    ) -> Result<BrokerOutcome> {
        if namespace.as_str() == LEASE_NAMESPACE {
            return self.release_lease(key, value, &context).await;
        }
        if namespace.as_str() == AUTH_NAMESPACE {
            self.require_bootstrap(&context)?;
            serde_json::from_value::<crate::AuthRecord>(value.clone())
                .map_err(|error| Error::InvalidAuthorization(error.to_string()))?;
        } else if namespace.as_str() == SECURITY_NAMESPACE {
            self.require_bootstrap(&context)?;
            if marked_key(key.as_str()).is_some() {
                return Err(Error::Protocol(
                    "security key records cannot use encrypted keys".into(),
                ));
            }
            parse_security_key(&value)?;
            self.store.set(namespace, key, value, ttl).await?;
            return Ok(BrokerOutcome::response(Response::Ok));
        }
        let (key, encrypted) = normalize_marked_key(key)?;
        if namespace.as_str() != AUTH_NAMESPACE {
            self.authorize(
                &context,
                &namespace,
                if encrypted {
                    Operation::WriteEncrypted
                } else {
                    Operation::Write
                },
            )?;
        }
        let value = if encrypted {
            ValueCipher::encrypt(
                &self.security_key(&namespace).await?,
                &namespace,
                &key,
                &value,
            )?
        } else {
            value
        };
        self.prepare_mutation(
            PendingMutation::Set {
                namespace,
                key,
                value,
                ttl,
            },
            context.adapter_instance,
        )
        .await
    }

    async fn set_batch(
        &self,
        namespace: NamespaceContext,
        entries: Vec<SetEntry>,
        ttl: Option<Duration>,
        context: RequestContext,
    ) -> Result<BrokerOutcome> {
        if entries.is_empty() {
            return Err(Error::Protocol("SET batch cannot be empty".into()));
        }
        let mut converted = Vec::with_capacity(entries.len());
        for entry in entries {
            let (key, encrypted) = normalize_marked_key(entry.key)?;
            self.authorize(
                &context,
                &namespace,
                if encrypted {
                    Operation::WriteEncrypted
                } else {
                    Operation::Write
                },
            )?;
            let value = if encrypted {
                ValueCipher::encrypt(
                    &self.security_key(&namespace).await?,
                    &namespace,
                    &key,
                    &entry.value,
                )?
            } else {
                entry.value
            };
            converted.push(SetEntry { key, value });
        }
        self.prepare_mutation(
            PendingMutation::SetBatch {
                namespace,
                entries: converted,
                ttl,
            },
            context.adapter_instance,
        )
        .await
    }

    async fn delete(
        &self,
        namespace: NamespaceContext,
        key_pattern: Option<KeyPattern>,
        context: RequestContext,
    ) -> Result<BrokerOutcome> {
        if matches!(
            namespace.as_str(),
            AUTH_NAMESPACE | SECURITY_NAMESPACE | LEASE_NAMESPACE
        ) {
            return Err(Error::PermissionDenied(namespace.to_string()));
        }
        self.authorize(&context, &namespace, Operation::Delete)?;
        if key_pattern
            .as_ref()
            .is_some_and(|pattern| marked_key(pattern.as_str()).is_some())
        {
            return Err(Error::Protocol(
                "encrypted key markers are not supported for delete".into(),
            ));
        }
        self.prepare_mutation(
            PendingMutation::Delete {
                namespace,
                key_pattern,
            },
            context.adapter_instance,
        )
        .await
    }

    async fn move_namespace(
        &self,
        source: NamespaceContext,
        destination: NamespaceContext,
        context: RequestContext,
    ) -> Result<BrokerOutcome> {
        if matches!(
            source.as_str(),
            AUTH_NAMESPACE | SECURITY_NAMESPACE | LEASE_NAMESPACE
        ) || matches!(
            destination.as_str(),
            AUTH_NAMESPACE | SECURITY_NAMESPACE | LEASE_NAMESPACE
        ) {
            return Err(Error::PermissionDenied(AUTH_NAMESPACE.into()));
        }
        crate::validate_context_move(&source, &destination)?;
        self.authorize(&context, &source, Operation::Delete)?;
        self.authorize(&context, &destination, Operation::Write)?;
        self.prepare_mutation(
            PendingMutation::Move {
                source,
                destination,
            },
            context.adapter_instance,
        )
        .await
    }

    async fn prepare_mutation(
        &self,
        mutation: PendingMutation,
        source_adapter: Option<Uuid>,
    ) -> Result<BrokerOutcome> {
        if let PendingMutation::SetBatch {
            namespace,
            entries,
            ttl,
        } = &mutation
        {
            let patterns = entries
                .iter()
                .map(|entry| KeyPattern::new(entry.key.as_str()))
                .collect::<Result<Vec<_>>>()?;
            return match self
                .registry
                .store_for_batch(namespace, &patterns, source_adapter)
                .await
            {
                BatchStoreMatch::Store(store) => Ok(BrokerOutcome {
                    response: Response::Ok,
                    dispatch: Some(Dispatch {
                        owner: store.owner,
                        provider_id: None,
                        provider_refresh_id: None,
                        mutation_generation: 0,
                        command: ServerCommand::PersistSetBatch {
                            request_id: Uuid::new_v4(),
                            namespace: namespace.clone(),
                            entries: entries.clone(),
                            ttl_seconds: ttl.map(|ttl| ttl.as_secs()),
                        },
                        authentication: false,
                        provider_options: None,
                        encrypted: false,
                    }),
                    pending_mutation: Some(mutation),
                    authenticated: None,
                }),
                BatchStoreMatch::Mixed => Err(Error::Protocol(
                    "SET batch keys require different storage adapters".into(),
                )),
                BatchStoreMatch::None => {
                    self.commit(mutation).await?;
                    Ok(BrokerOutcome::response(Response::Ok))
                }
            };
        }
        let (namespace, pattern, command) = match &mutation {
            PendingMutation::Set {
                namespace,
                key,
                value,
                ttl,
            } => (
                namespace.clone(),
                KeyPattern::new(key.as_str())?,
                ServerCommand::PersistSet {
                    request_id: Uuid::new_v4(),
                    namespace: namespace.clone(),
                    key: key.clone(),
                    value: value.clone(),
                    ttl_seconds: ttl.map(|ttl| ttl.as_secs()),
                },
            ),
            PendingMutation::SetBatch { .. } => unreachable!("batch returned above"),
            PendingMutation::Delete {
                namespace,
                key_pattern,
            } => (
                namespace.clone(),
                key_pattern.clone().unwrap_or(KeyPattern::new("*")?),
                ServerCommand::PersistDelete {
                    request_id: Uuid::new_v4(),
                    namespace: namespace.clone(),
                    key_pattern: key_pattern.clone(),
                },
            ),
            PendingMutation::Move {
                source,
                destination,
            } => (
                source.clone(),
                KeyPattern::new("*")?,
                ServerCommand::PersistMove {
                    request_id: Uuid::new_v4(),
                    source: source.clone(),
                    destination: destination.clone(),
                },
            ),
        };
        match self
            .registry
            .store_for(&namespace, &pattern, source_adapter)
            .await
        {
            Some(store) => Ok(BrokerOutcome {
                response: Response::Ok,
                dispatch: Some(Dispatch {
                    owner: store.owner,
                    provider_id: None,
                    provider_refresh_id: None,
                    mutation_generation: 0,
                    command,
                    authentication: false,
                    provider_options: None,
                    encrypted: false,
                }),
                pending_mutation: Some(mutation),
                authenticated: None,
            }),
            None => {
                self.commit(mutation).await?;
                Ok(BrokerOutcome::response(Response::Ok))
            }
        }
    }

    fn authorize(
        &self,
        context: &RequestContext,
        namespace: &NamespaceContext,
        operation: Operation,
    ) -> Result<()> {
        let Some(manager) = &self.auth else {
            return Ok(());
        };
        let auth = context.auth.as_ref().ok_or(Error::AuthenticationFailed)?;
        manager.authorize(auth, namespace, operation)
    }

    async fn acquire_lease(&self, key: Key, context: &RequestContext) -> Result<BrokerOutcome> {
        let owner = self.require_adapter(context)?;
        let now = std::time::Instant::now();
        let mut leases = self.leases.lock().await;
        let granted = match leases.get(&key.to_string()) {
            Some(lease) if lease.expires_at > now && lease.owner != owner => false,
            _ => {
                leases.insert(
                    key.to_string(),
                    Lease {
                        owner,
                        expires_at: now + LEASE_DURATION,
                    },
                );
                true
            }
        };
        Ok(BrokerOutcome::response(Response::Value {
            value: Value::Bool(granted),
            ttl_seconds: granted.then_some(LEASE_DURATION.as_secs()),
        }))
    }

    async fn release_lease(
        &self,
        key: Key,
        value: Value,
        context: &RequestContext,
    ) -> Result<BrokerOutcome> {
        let owner = self.require_adapter(context)?;
        if value != 0 {
            return Err(Error::Protocol("lease release requires value 0".into()));
        }
        let mut leases = self.leases.lock().await;
        match leases.get(&key.to_string()) {
            Some(lease) if lease.owner == owner => {
                leases.remove(&key.to_string());
                Ok(BrokerOutcome::response(Response::Ok))
            }
            _ => Err(Error::PermissionDenied(LEASE_NAMESPACE.into())),
        }
    }

    fn require_adapter(&self, context: &RequestContext) -> Result<Uuid> {
        context
            .adapter_instance
            .filter(|_| context.auth.is_some())
            .ok_or_else(|| Error::PermissionDenied(LEASE_NAMESPACE.into()))
    }

    async fn provider_retry_after_values(&self, provider_id: Uuid, limit: u32) -> Option<u64> {
        let mut limits = self.provider_limits.lock().await;
        let now = std::time::Instant::now();
        let state = limits.entry(provider_id).or_insert(ProviderRateLimit {
            window_started: now,
            requests: 0,
        });
        let elapsed = now.duration_since(state.window_started);
        if elapsed >= Duration::from_secs(1) {
            state.window_started = now;
            state.requests = 0;
        }
        if state.requests < limit {
            state.requests += 1;
            None
        } else {
            Some(
                Duration::from_secs(1)
                    .saturating_sub(elapsed)
                    .as_millis()
                    .max(1) as u64,
            )
        }
    }
    async fn resolve_key(
        &self,
        namespace: &NamespaceContext,
        key: &Key,
        context: &RequestContext,
    ) -> Result<Key> {
        let mut resolved = key.as_str().to_owned();
        let mut cursor = 0;
        while let Some(start) = resolved[cursor..].find("${") {
            let start = cursor + start;
            let Some(end_offset) = resolved[start + 2..].find('}') else {
                break;
            };
            let end = start + 2 + end_offset;
            let variable = &resolved[start + 2..end];
            if variable.is_empty() {
                cursor = end + 1;
                continue;
            }
            let replacement = if let Some(value) = context.variables.get(variable) {
                Some(value.clone())
            } else {
                self.store
                    .get(namespace, &Key::new(variable)?)
                    .await?
                    .map(|stored| match stored.value {
                        Value::String(value) => value,
                        value => value.to_string(),
                    })
            };
            match replacement {
                Some(value) => {
                    resolved.replace_range(start..=end, &value);
                    cursor = start + value.len();
                }
                None => cursor = end + 1,
            }
        }
        Key::new(resolved)
    }
    fn require_bootstrap(&self, context: &RequestContext) -> Result<()> {
        context
            .auth
            .as_ref()
            .filter(|auth| auth.is_bootstrap_admin())
            .map(|_| ())
            .ok_or(Error::PermissionDenied(AUTH_NAMESPACE.into()))
    }
    async fn security_key(&self, namespace: &NamespaceContext) -> Result<String> {
        let (security_namespace, scope_key) = security_route(namespace)?;
        if let Some(value) = self.store.get(&security_namespace, &scope_key).await? {
            return parse_security_key(&value.value);
        }
        Err(Error::Encryption(format!(
            "encryption key for scope '{}' is not loaded",
            scope_key.as_str()
        )))
    }
}

fn security_route(namespace: &NamespaceContext) -> Result<(NamespaceContext, Key)> {
    // A context refines an encryption scope, but ordinary namespaces are also
    // valid encrypted routes. Context-qualified values deliberately share the
    // key of their base namespace to keep one durable key per logical route.
    let scope = namespace.ns();
    Ok((NamespaceContext::new(SECURITY_NAMESPACE)?, Key::new(scope)?))
}

fn parse_security_key(value: &Value) -> Result<String> {
    serde_json::from_value::<SecurityKeyRecord>(value.clone())
        .map(|record| record.key)
        .map_err(|_| Error::Encryption("security key record is malformed".into()))
}

fn marked_key(value: &str) -> Option<&str> {
    value
        .strip_prefix('~')
        .and_then(|value| value.strip_suffix('~'))
        .filter(|value| !value.is_empty())
}
fn normalize_marked_key(key: Key) -> Result<(Key, bool)> {
    match marked_key(key.as_str()) {
        Some(value) => Ok((Key::new(value)?, true)),
        None => Ok((key, false)),
    }
}
fn duration_to_seconds(duration: Duration) -> u64 {
    duration
        .as_secs()
        .saturating_add(u64::from(duration.subsec_nanos() != 0))
}

fn pattern_matches(pattern: &str, key: &str) -> bool {
    pattern
        .strip_suffix('*')
        .map_or(pattern == key, |prefix| key.starts_with(prefix))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AuthRecord, MemoryStore, SimpleAuthenticator, StoredValue};
    use async_trait::async_trait;

    struct GetFailingStore;

    #[async_trait]
    impl Store for GetFailingStore {
        async fn get(&self, _: &NamespaceContext, _: &Key) -> Result<Option<StoredValue>> {
            Err(Error::Protocol("store read should not occur".into()))
        }

        async fn set(
            &self,
            _: NamespaceContext,
            _: Key,
            _: Value,
            _: Option<Duration>,
        ) -> Result<()> {
            Ok(())
        }

        async fn delete(&self, _: &NamespaceContext, _: Option<&KeyPattern>) -> Result<u64> {
            Ok(0)
        }

        async fn scan(
            &self,
            _: &NamespaceContext,
            _: &KeyPattern,
        ) -> Result<Vec<(Key, StoredValue)>> {
            Ok(Vec::new())
        }

        async fn move_namespace(&self, _: &NamespaceContext, _: NamespaceContext) -> Result<()> {
            Ok(())
        }

        async fn len(&self) -> Result<u64> {
            Ok(0)
        }
    }

    #[tokio::test]
    async fn broker_without_an_auth_manager_rejects_authentication() {
        let broker = Broker::new(Arc::new(MemoryStore::new()), None);

        let outcome = broker
            .execute(
                Command::Auth {
                    api_key: "any-key".into(),
                    adapter_instance: None,
                },
                RequestContext::anonymous(Uuid::new_v4()),
            )
            .await
            .unwrap();
        assert!(matches!(outcome.response, Response::AuthFailure { .. }));
        assert!(outcome.authenticated.is_none());
    }

    #[tokio::test]
    async fn cached_auth_record_authenticates_without_a_provider_dispatch() {
        let store = Arc::new(MemoryStore::new());
        store
            .set(
                NamespaceContext::new(AUTH_NAMESPACE).unwrap(),
                Key::new("reader").unwrap(),
                serde_json::json!({
                    "client_id": "reader",
                    "name": "Reader"
                }),
                None,
            )
            .await
            .unwrap();
        let broker = Broker::new(
            store,
            Some(Arc::new(AuthManager::new(Arc::new(
                SimpleAuthenticator::new(HashMap::new()),
            )))),
        );

        let outcome = broker
            .execute(
                Command::Auth {
                    api_key: "reader".into(),
                    adapter_instance: None,
                },
                RequestContext::anonymous(Uuid::new_v4()),
            )
            .await
            .unwrap();
        assert!(matches!(outcome.response, Response::AuthSuccess { .. }));
        assert!(outcome.authenticated.is_some());
        assert!(outcome.dispatch.is_none());
    }

    #[tokio::test]
    async fn committed_auth_record_replaces_an_active_session_principal() {
        let store = Arc::new(MemoryStore::new());
        let auth_namespace = NamespaceContext::new(AUTH_NAMESPACE).unwrap();
        let consumer_key = Key::new("consumer").unwrap();
        store
            .set(
                auth_namespace.clone(),
                consumer_key.clone(),
                serde_json::json!({
                    "client_id": "consumer",
                    "name": "Consumer",
                    "permissions": [{"namespace": "/old", "operations": ["read"]}]
                }),
                None,
            )
            .await
            .unwrap();
        let auth = Arc::new(AuthManager::with_bootstrap_admin(
            Arc::new(SimpleAuthenticator::new(HashMap::new())),
            Some("bootstrap".into()),
            Duration::from_secs(60),
        ));
        let broker = Broker::new(store, Some(auth.clone()));
        let consumer = match broker
            .execute(
                Command::Auth {
                    api_key: "consumer".into(),
                    adapter_instance: None,
                },
                RequestContext::anonymous(Uuid::new_v4()),
            )
            .await
            .unwrap()
            .authenticated
        {
            Some(auth) => auth,
            None => panic!("consumer authentication must succeed"),
        };
        let AuthLookup::Authenticated(bootstrap) = auth.authenticate("bootstrap") else {
            panic!("bootstrap authentication must succeed");
        };
        broker
            .execute(
                Command::Set {
                    namespace: auth_namespace,
                    key: consumer_key.clone(),
                    value: serde_json::json!({
                        "client_id": "consumer",
                        "name": "Consumer",
                        "permissions": [{"namespace": "/new", "operations": ["read"]}]
                    }),
                    ttl_seconds: None,
                },
                RequestContext {
                    owner: Uuid::new_v4(),
                    adapter_instance: None,
                    auth: Some(bootstrap.clone()),
                    variables: BTreeMap::new(),
                },
            )
            .await
            .unwrap();

        let active_session = RequestContext {
            owner: Uuid::new_v4(),
            adapter_instance: None,
            auth: Some(consumer),
            variables: BTreeMap::new(),
        };
        assert!(matches!(
            broker
                .execute(
                    Command::Get {
                        namespace: NamespaceContext::new("/old").unwrap(),
                        key: Key::new("entry").unwrap(),
                    },
                    active_session.clone(),
                )
                .await,
            Err(Error::PermissionDenied(_))
        ));
        assert!(matches!(
            broker
                .execute(
                    Command::Get {
                        namespace: NamespaceContext::new("/new").unwrap(),
                        key: Key::new("entry").unwrap(),
                    },
                    active_session.clone(),
                )
                .await
                .unwrap()
                .response,
            Response::Unknown
        ));
        broker
            .execute(
                Command::Set {
                    namespace: NamespaceContext::new(AUTH_NAMESPACE).unwrap(),
                    key: consumer_key,
                    value: serde_json::json!({
                        "client_id": "consumer",
                        "name": "Consumer",
                        "enabled": false
                    }),
                    ttl_seconds: None,
                },
                RequestContext {
                    owner: Uuid::new_v4(),
                    adapter_instance: None,
                    auth: Some(bootstrap),
                    variables: BTreeMap::new(),
                },
            )
            .await
            .unwrap();
        assert!(matches!(
            broker
                .execute(
                    Command::Get {
                        namespace: NamespaceContext::new("/new").unwrap(),
                        key: Key::new("entry").unwrap(),
                    },
                    active_session,
                )
                .await,
            Err(Error::PermissionDenied(_))
        ));
    }

    #[tokio::test]
    async fn missing_auth_record_remains_pending_with_one_provider_dispatch() {
        let broker = Broker::new(
            Arc::new(MemoryStore::new()),
            Some(Arc::new(AuthManager::new(Arc::new(
                SimpleAuthenticator::new(HashMap::new()),
            )))),
        );
        let owner = Uuid::new_v4();
        broker
            .registry()
            .register_provider(owner, AUTH_NAMESPACE, "*", None)
            .await;

        let outcome = broker
            .execute(
                Command::Auth {
                    api_key: "reader".into(),
                    adapter_instance: None,
                },
                RequestContext::anonymous(Uuid::new_v4()),
            )
            .await
            .unwrap();
        assert!(matches!(outcome.response, Response::AuthPending { .. }));
        assert!(matches!(
            outcome.dispatch,
            Some(Dispatch {
                authentication: true,
                ..
            })
        ));

        let repeated = broker
            .execute(
                Command::Auth {
                    api_key: "reader".into(),
                    adapter_instance: None,
                },
                RequestContext::anonymous(Uuid::new_v4()),
            )
            .await
            .unwrap();
        assert!(repeated.dispatch.is_none());
    }

    #[tokio::test]
    async fn bootstrap_can_warm_security_keys_without_store_dispatch() {
        let store = Arc::new(MemoryStore::new());
        let auth = Arc::new(AuthManager::with_bootstrap_admin(
            Arc::new(SimpleAuthenticator::new(HashMap::new())),
            Some("bootstrap".into()),
            Duration::from_secs(60),
        ));
        let broker = Broker::new(store.clone(), Some(auth.clone()));
        broker
            .registry()
            .register_store(
                Uuid::new_v4(),
                Some(Uuid::new_v4()),
                SECURITY_NAMESPACE,
                "*",
            )
            .await;
        let AuthLookup::Authenticated(auth) = auth.authenticate("bootstrap") else {
            panic!("bootstrap key must authenticate");
        };
        let key = Key::new("/example").unwrap();
        let value = serde_json::json!({
            "key": "11".repeat(32),
            "created": 1,
        });

        let outcome = broker
            .execute(
                Command::Set {
                    namespace: NamespaceContext::new(SECURITY_NAMESPACE).unwrap(),
                    key: key.clone(),
                    value: value.clone(),
                    ttl_seconds: Some(60),
                },
                RequestContext {
                    owner: Uuid::new_v4(),
                    adapter_instance: None,
                    auth: Some(auth),
                    variables: BTreeMap::new(),
                },
            )
            .await
            .unwrap();

        assert!(matches!(outcome.response, Response::Ok));
        assert!(outcome.dispatch.is_none());
        assert!(outcome.pending_mutation.is_none());
        assert_eq!(
            store
                .get(&NamespaceContext::new(SECURITY_NAMESPACE).unwrap(), &key)
                .await
                .unwrap()
                .map(|stored| stored.value),
            Some(value)
        );
    }

    #[tokio::test]
    async fn security_key_warmup_requires_a_valid_bootstrap_write() {
        let auth = Arc::new(AuthManager::with_bootstrap_admin(
            Arc::new(SimpleAuthenticator::new(HashMap::new())),
            Some("bootstrap".into()),
            Duration::from_secs(60),
        ));
        let broker = Broker::new(Arc::new(MemoryStore::new()), Some(auth.clone()));
        let namespace = NamespaceContext::new(SECURITY_NAMESPACE).unwrap();
        let record = serde_json::json!({"key": "11".repeat(32), "created": 1});

        let denied = broker
            .execute(
                Command::Set {
                    namespace: namespace.clone(),
                    key: Key::new("/example").unwrap(),
                    value: record.clone(),
                    ttl_seconds: None,
                },
                RequestContext::anonymous(Uuid::new_v4()),
            )
            .await;
        assert!(matches!(denied, Err(Error::PermissionDenied(_))));

        let AuthLookup::Authenticated(bootstrap) = auth.authenticate("bootstrap") else {
            panic!("bootstrap key must authenticate");
        };
        let context = RequestContext {
            owner: Uuid::new_v4(),
            adapter_instance: None,
            auth: Some(bootstrap),
            variables: BTreeMap::new(),
        };
        let malformed = broker
            .execute(
                Command::Set {
                    namespace: namespace.clone(),
                    key: Key::new("/example").unwrap(),
                    value: serde_json::json!({"key": "11".repeat(32)}),
                    ttl_seconds: None,
                },
                context.clone(),
            )
            .await;
        assert!(matches!(malformed, Err(Error::Encryption(_))));
        let encrypted_name = broker
            .execute(
                Command::Set {
                    namespace,
                    key: Key::new("~/example~").unwrap(),
                    value: record,
                    ttl_seconds: None,
                },
                context,
            )
            .await;
        assert!(matches!(encrypted_name, Err(Error::Protocol(_))));
    }

    #[tokio::test]
    async fn provider_max_rate_defers_excess_cache_misses() {
        let broker = Broker::new(Arc::new(MemoryStore::new()), None);
        let owner = Uuid::new_v4();
        broker
            .registry()
            .register_provider(owner, "/values", "*", Some(1))
            .await;
        let context = RequestContext::anonymous(Uuid::new_v4());
        let first = broker
            .execute(
                Command::Get {
                    namespace: NamespaceContext::new("/values").unwrap(),
                    key: Key::new("one").unwrap(),
                },
                context.clone(),
            )
            .await
            .unwrap();
        assert!(first.dispatch.is_some());
        let provider_id = first
            .dispatch
            .as_ref()
            .and_then(|dispatch| dispatch.provider_id);
        assert_eq!(
            broker.provider_retry_after(provider_id, Some(1)).await,
            None
        );
        let second = broker
            .execute(
                Command::Get {
                    namespace: NamespaceContext::new("/values").unwrap(),
                    key: Key::new("two").unwrap(),
                },
                context,
            )
            .await
            .unwrap();
        assert!(second.dispatch.is_some());
        assert!(
            broker
                .provider_retry_after(provider_id, Some(1))
                .await
                .is_some()
        );
    }

    #[tokio::test]
    async fn confirmed_provider_misses_skip_rate_limits_until_their_ttl_expires() {
        let broker = Broker::new(Arc::new(MemoryStore::new()), None);
        let owner = Uuid::new_v4();
        broker
            .registry()
            .register_provider(
                owner,
                "/values",
                "*",
                ProvideOptions::new()
                    .with_max_rate(Some(1))
                    .with_miss_ttl_seconds(1),
            )
            .await;
        let namespace = NamespaceContext::new("/values").unwrap();
        let key = Key::new("missing").unwrap();
        let context = RequestContext::anonymous(Uuid::new_v4());
        let first = broker
            .execute(
                Command::Get {
                    namespace: namespace.clone(),
                    key: key.clone(),
                },
                context.clone(),
            )
            .await
            .unwrap();
        assert!(first.dispatch.is_some());
        let first_dispatch = first.dispatch.unwrap();
        broker
            .confirm_provider_miss(
                namespace.clone(),
                key.clone(),
                1,
                first_dispatch.mutation_generation,
                first_dispatch.provider_refresh_id.unwrap(),
            )
            .await;
        let cached = broker
            .execute(
                Command::Get {
                    namespace: namespace.clone(),
                    key: key.clone(),
                },
                context,
            )
            .await
            .unwrap();
        assert!(cached.dispatch.is_none());
        assert!(matches!(
            cached.response,
            Response::Miss { retry_after_ms: 0 }
        ));
        broker
            .store
            .set(
                namespace.clone(),
                key.clone(),
                Value::String("live".into()),
                None,
            )
            .await
            .unwrap();
        let value = broker
            .execute(
                Command::Get { namespace, key },
                RequestContext::anonymous(Uuid::new_v4()),
            )
            .await
            .unwrap();
        assert!(matches!(value.response, Response::Value { .. }));
    }

    #[tokio::test]
    async fn provider_misses_started_before_each_mutation_cannot_become_stale_markers() {
        let broker = Broker::new(Arc::new(MemoryStore::new()), None);
        let owner = Uuid::new_v4();
        broker
            .registry()
            .register_provider(owner, "/values", "*", None)
            .await;
        let namespace = NamespaceContext::new("/values").unwrap();
        let refresh_generation = broker.mutation_generation().await;

        broker
            .commit(PendingMutation::Set {
                namespace: namespace.clone(),
                key: Key::new("exact").unwrap(),
                value: Value::String("temporary".into()),
                ttl: Some(Duration::from_millis(1)),
            })
            .await
            .unwrap();
        broker
            .commit(PendingMutation::SetBatch {
                namespace: namespace.clone(),
                entries: vec![SetEntry {
                    key: Key::new("batch").unwrap(),
                    value: Value::String("temporary".into()),
                }],
                ttl: Some(Duration::from_millis(1)),
            })
            .await
            .unwrap();
        broker
            .commit(PendingMutation::Delete {
                namespace: namespace.clone(),
                key_pattern: Some(KeyPattern::new("pattern*").unwrap()),
            })
            .await
            .unwrap();
        broker
            .commit(PendingMutation::Move {
                source: NamespaceContext::new("/values::old").unwrap(),
                destination: NamespaceContext::new("/values::new").unwrap(),
            })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(10)).await;
        for key in ["exact", "batch", "pattern-key", "moved"] {
            let key = Key::new(key).unwrap();
            broker
                .confirm_provider_miss(
                    namespace.clone(),
                    key.clone(),
                    60,
                    refresh_generation,
                    Uuid::nil(),
                )
                .await;
            let outcome = broker
                .execute(
                    Command::Get {
                        namespace: namespace.clone(),
                        key,
                    },
                    RequestContext::anonymous(Uuid::new_v4()),
                )
                .await
                .unwrap();
            assert!(outcome.dispatch.is_some(), "stale marker was inserted");
        }
    }

    #[tokio::test]
    async fn mutation_generations_are_scoped_to_affected_routes() {
        for shape in ["exact", "batch", "pattern", "namespace", "move"] {
            let broker = Broker::new(Arc::new(MemoryStore::new()), None);
            let owner = Uuid::new_v4();
            broker
                .registry()
                .register_provider(owner, "/values", "*", None)
                .await;
            let namespace = if shape == "move" {
                NamespaceContext::new("/values::old").unwrap()
            } else {
                NamespaceContext::new("/values").unwrap()
            };
            let key = Key::new(format!("{shape}-key")).unwrap();
            let dispatch = broker
                .execute(
                    Command::Get {
                        namespace: namespace.clone(),
                        key: key.clone(),
                    },
                    RequestContext::anonymous(Uuid::new_v4()),
                )
                .await
                .unwrap()
                .dispatch
                .unwrap();

            match shape {
                "exact" => {
                    broker
                        .commit(PendingMutation::Set {
                            namespace: namespace.clone(),
                            key: key.clone(),
                            value: Value::String("temporary".into()),
                            ttl: Some(Duration::from_millis(1)),
                        })
                        .await
                        .unwrap();
                    broker
                        .commit(PendingMutation::Delete {
                            namespace: namespace.clone(),
                            key_pattern: Some(KeyPattern::new(key.as_str()).unwrap()),
                        })
                        .await
                        .unwrap();
                }
                "batch" => {
                    broker
                        .commit(PendingMutation::SetBatch {
                            namespace: namespace.clone(),
                            entries: vec![SetEntry {
                                key: key.clone(),
                                value: Value::String("temporary".into()),
                            }],
                            ttl: Some(Duration::from_millis(1)),
                        })
                        .await
                        .unwrap();
                    broker
                        .commit(PendingMutation::Delete {
                            namespace: namespace.clone(),
                            key_pattern: Some(KeyPattern::new(key.as_str()).unwrap()),
                        })
                        .await
                        .unwrap();
                }
                "pattern" => {
                    broker
                        .commit(PendingMutation::Delete {
                            namespace: namespace.clone(),
                            key_pattern: Some(KeyPattern::new(format!("{shape}-*")).unwrap()),
                        })
                        .await
                        .unwrap();
                }
                "namespace" => {
                    broker
                        .commit(PendingMutation::Delete {
                            namespace: namespace.clone(),
                            key_pattern: None,
                        })
                        .await
                        .unwrap();
                }
                "move" => {
                    broker
                        .commit(PendingMutation::Move {
                            source: namespace.clone(),
                            destination: NamespaceContext::new("/values::new").unwrap(),
                        })
                        .await
                        .unwrap();
                }
                _ => unreachable!(),
            }

            broker
                .confirm_provider_miss(
                    namespace.clone(),
                    key.clone(),
                    60,
                    dispatch.mutation_generation,
                    dispatch.provider_refresh_id.unwrap(),
                )
                .await;
            let outcome = broker
                .execute(
                    Command::Get { namespace, key },
                    RequestContext::anonymous(Uuid::new_v4()),
                )
                .await
                .unwrap();
            assert!(
                outcome.dispatch.is_some(),
                "stale marker survived {shape} mutation"
            );
        }

        let broker = Broker::new(Arc::new(MemoryStore::new()), None);
        broker
            .registry()
            .register_provider(Uuid::new_v4(), "/values", "*", None)
            .await;
        let target_namespace = NamespaceContext::new("/values").unwrap();
        let target_key = Key::new("target").unwrap();
        let dispatch = broker
            .execute(
                Command::Get {
                    namespace: target_namespace.clone(),
                    key: target_key.clone(),
                },
                RequestContext::anonymous(Uuid::new_v4()),
            )
            .await
            .unwrap()
            .dispatch
            .unwrap();
        broker
            .commit(PendingMutation::Set {
                namespace: target_namespace.clone(),
                key: Key::new("unrelated").unwrap(),
                value: Value::String("live".into()),
                ttl: None,
            })
            .await
            .unwrap();
        broker
            .confirm_provider_miss(
                target_namespace.clone(),
                target_key.clone(),
                60,
                dispatch.mutation_generation,
                dispatch.provider_refresh_id.unwrap(),
            )
            .await;
        let outcome = broker
            .execute(
                Command::Get {
                    namespace: target_namespace,
                    key: target_key,
                },
                RequestContext::anonymous(Uuid::new_v4()),
            )
            .await
            .unwrap();
        assert!(
            outcome.dispatch.is_none(),
            "unrelated mutation suppressed miss caching"
        );
        assert!(matches!(
            outcome.response,
            Response::Miss { retry_after_ms: 0 }
        ));
    }

    #[tokio::test]
    async fn mutation_coordination_is_reclaimed_after_refreshes_and_writes() {
        let broker = Broker::new(Arc::new(MemoryStore::new()), None);
        broker
            .registry()
            .register_provider(Uuid::new_v4(), "/values", "*", None)
            .await;

        for index in 0..128 {
            let namespace = NamespaceContext::new("/values").unwrap();
            let key = Key::new(format!("missing-{index}")).unwrap();
            let dispatch = broker
                .execute(
                    Command::Get {
                        namespace: namespace.clone(),
                        key: key.clone(),
                    },
                    RequestContext::anonymous(Uuid::new_v4()),
                )
                .await
                .unwrap();
            let dispatch = dispatch.dispatch.unwrap();
            broker
                .release_provider_refresh(&namespace, &key, dispatch.provider_refresh_id.unwrap())
                .await;
        }
        assert_eq!(broker.mutation_coordination_size().await, 0);

        for index in 0..128 {
            broker
                .commit(PendingMutation::Set {
                    namespace: NamespaceContext::new("/values").unwrap(),
                    key: Key::new(format!("written-{index}")).unwrap(),
                    value: Value::String("value".into()),
                    ttl: None,
                })
                .await
                .unwrap();
        }
        assert_eq!(broker.mutation_coordination_size().await, 0);
    }

    #[tokio::test]
    async fn unrelated_route_generation_does_not_wait_on_target_coordination() {
        let broker = Broker::new(Arc::new(MemoryStore::new()), None);
        broker
            .registry()
            .register_provider(Uuid::new_v4(), "/values", "*", None)
            .await;
        let target_namespace = NamespaceContext::new("/values").unwrap();
        let target_key = Key::new("target").unwrap();
        broker
            .execute(
                Command::Get {
                    namespace: target_namespace.clone(),
                    key: target_key.clone(),
                },
                RequestContext::anonymous(Uuid::new_v4()),
            )
            .await
            .unwrap();
        let target_state = broker
            .active_route_state(&target_namespace, &target_key)
            .expect("provider admission creates route state");
        let target_guard = target_state.gate.lock().await;

        let unrelated = tokio::time::timeout(
            Duration::from_millis(100),
            broker.route_mutation_generation(&target_namespace, &Key::new("unrelated").unwrap()),
        )
        .await
        .expect("unrelated route was blocked by target coordination");
        assert_eq!(unrelated.0, 0);
        drop(target_guard);
        broker
            .release_provider_refresh(
                &target_namespace,
                &target_key,
                target_state.provider_refresh_id,
            )
            .await;
        broker
            .release_provider_refresh(
                &target_namespace,
                &Key::new("unrelated").unwrap(),
                unrelated.1,
            )
            .await;
    }

    #[tokio::test]
    async fn final_miss_check_and_insertion_are_serialized_with_exact_mutation() {
        let pause = Arc::new(MissInsertionPause {
            checked: Notify::new(),
            continue_to_insert: Notify::new(),
            inserted: Notify::new(),
            continue_after_insert: Notify::new(),
            mutation_ready: Notify::new(),
            continue_to_mutation: Notify::new(),
            mutation_waiting: Notify::new(),
        });
        let broker = Arc::new(
            Broker::new(Arc::new(MemoryStore::new()), None)
                .with_miss_insertion_pause(pause.clone()),
        );
        broker
            .registry()
            .register_provider(Uuid::new_v4(), "/values", "*", None)
            .await;
        let namespace = NamespaceContext::new("/values").unwrap();
        let key = Key::new("raced").unwrap();
        let dispatch = broker
            .execute(
                Command::Get {
                    namespace: namespace.clone(),
                    key: key.clone(),
                },
                RequestContext::anonymous(Uuid::new_v4()),
            )
            .await
            .unwrap()
            .dispatch
            .unwrap();

        let confirm = tokio::spawn({
            let broker = broker.clone();
            let namespace = namespace.clone();
            let key = key.clone();
            async move {
                broker
                    .confirm_provider_miss(
                        namespace,
                        key,
                        60,
                        dispatch.mutation_generation,
                        dispatch.provider_refresh_id.unwrap(),
                    )
                    .await;
            }
        });
        pause.checked.notified().await;

        let commit = tokio::spawn({
            let broker = broker.clone();
            let namespace = namespace.clone();
            let key = key.clone();
            async move {
                broker
                    .commit(PendingMutation::Set {
                        namespace,
                        key,
                        value: Value::String("committed".into()),
                        ttl: Some(Duration::from_millis(1)),
                    })
                    .await
                    .unwrap();
            }
        });
        pause.mutation_ready.notified().await;
        pause.continue_to_mutation.notify_one();
        pause.mutation_waiting.notified().await;
        pause.continue_to_insert.notify_one();
        pause.inserted.notified().await;
        pause.continue_after_insert.notify_one();
        confirm.await.unwrap();
        commit.await.unwrap();

        assert!(!broker.miss_cache.contains(&namespace, &key).await);
    }

    #[tokio::test]
    async fn move_invalidates_active_source_and_destination_routes() {
        let broker = Broker::new(Arc::new(MemoryStore::new()), None);
        broker
            .registry()
            .register_provider(Uuid::new_v4(), "/values", "*", None)
            .await;
        let source = NamespaceContext::new("/values::source").unwrap();
        let destination = NamespaceContext::new("/values::destination").unwrap();
        let source_key = Key::new("source-key").unwrap();
        let destination_key = Key::new("destination-key").unwrap();
        let source_dispatch = broker
            .execute(
                Command::Get {
                    namespace: source.clone(),
                    key: source_key.clone(),
                },
                RequestContext::anonymous(Uuid::new_v4()),
            )
            .await
            .unwrap()
            .dispatch
            .unwrap();
        let destination_dispatch = broker
            .execute(
                Command::Get {
                    namespace: destination.clone(),
                    key: destination_key.clone(),
                },
                RequestContext::anonymous(Uuid::new_v4()),
            )
            .await
            .unwrap()
            .dispatch
            .unwrap();

        broker
            .commit(PendingMutation::Move {
                source: source.clone(),
                destination: destination.clone(),
            })
            .await
            .unwrap();
        broker
            .confirm_provider_miss(
                source.clone(),
                source_key.clone(),
                60,
                source_dispatch.mutation_generation,
                source_dispatch.provider_refresh_id.unwrap(),
            )
            .await;
        broker
            .confirm_provider_miss(
                destination.clone(),
                destination_key.clone(),
                60,
                destination_dispatch.mutation_generation,
                destination_dispatch.provider_refresh_id.unwrap(),
            )
            .await;
        assert!(
            broker
                .execute(
                    Command::Get {
                        namespace: source,
                        key: source_key,
                    },
                    RequestContext::anonymous(Uuid::new_v4()),
                )
                .await
                .unwrap()
                .dispatch
                .is_some()
        );
        assert!(
            broker
                .execute(
                    Command::Get {
                        namespace: destination,
                        key: destination_key,
                    },
                    RequestContext::anonymous(Uuid::new_v4()),
                )
                .await
                .unwrap()
                .dispatch
                .is_some()
        );
    }

    #[tokio::test]
    async fn provider_rejects_a_zero_rate_limit() {
        let broker = Broker::new(Arc::new(MemoryStore::new()), None);
        let result = broker
            .execute(
                Command::Provide {
                    namespace_pattern: crate::NamespacePattern::new("/values").unwrap(),
                    key_pattern: KeyPattern::new("*").unwrap(),
                    max_rate: Some(0),
                    timeout: None,
                    miss_ttl: None,
                },
                RequestContext {
                    owner: Uuid::new_v4(),
                    adapter_instance: Some(Uuid::new_v4()),
                    auth: None,
                    variables: BTreeMap::new(),
                },
            )
            .await;
        assert!(matches!(result, Err(Error::Protocol(_))));
    }

    #[tokio::test]
    async fn reserved_registry_namespaces_cannot_be_deleted_or_moved() {
        let broker = Broker::new(Arc::new(MemoryStore::new()), None);
        let context = RequestContext::anonymous(Uuid::new_v4());
        let delete = broker
            .execute(
                Command::Delete {
                    namespace: NamespaceContext::new(AUTH_NAMESPACE).unwrap(),
                    key_pattern: None,
                },
                context.clone(),
            )
            .await;
        assert!(matches!(delete, Err(Error::PermissionDenied(_))));
        let move_result = broker
            .execute(
                Command::Move {
                    source: NamespaceContext::new(AUTH_NAMESPACE).unwrap(),
                    destination: NamespaceContext::new("/archive").unwrap(),
                },
                context,
            )
            .await;
        assert!(matches!(move_result, Err(Error::PermissionDenied(_))));
    }

    #[tokio::test]
    async fn server_local_leases_are_owned_and_released_by_one_adapter() {
        let broker = Broker::new(Arc::new(MemoryStore::new()), None);
        let auth = AuthRecord {
            client_id: "adapter".into(),
            name: "Adapter".into(),
            roles: Vec::new(),
            permissions: Vec::new(),
            enabled: true,
        }
        .into_info("adapter-key".into())
        .unwrap();
        let context = |adapter_instance| RequestContext {
            owner: Uuid::new_v4(),
            adapter_instance: Some(adapter_instance),
            auth: Some(auth.clone()),
            variables: BTreeMap::new(),
        };
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        let lease = || Command::Get {
            namespace: NamespaceContext::new(LEASE_NAMESPACE).unwrap(),
            key: Key::new("/orders").unwrap(),
        };

        assert!(matches!(
            broker
                .execute(lease(), context(first))
                .await
                .unwrap()
                .response,
            Response::Value {
                value: Value::Bool(true),
                ttl_seconds: Some(30)
            }
        ));
        assert!(matches!(
            broker
                .execute(lease(), context(second))
                .await
                .unwrap()
                .response,
            Response::Value {
                value: Value::Bool(false),
                ttl_seconds: None
            }
        ));
        assert!(matches!(
            broker
                .execute(
                    Command::Set {
                        namespace: NamespaceContext::new(LEASE_NAMESPACE).unwrap(),
                        key: Key::new("/orders").unwrap(),
                        value: Value::from(0),
                        ttl_seconds: None,
                    },
                    context(second),
                )
                .await,
            Err(Error::PermissionDenied(_))
        ));
        assert!(matches!(
            broker
                .execute(
                    Command::Set {
                        namespace: NamespaceContext::new(LEASE_NAMESPACE).unwrap(),
                        key: Key::new("/orders").unwrap(),
                        value: Value::from(0),
                        ttl_seconds: None,
                    },
                    context(first),
                )
                .await
                .unwrap()
                .response,
            Response::Ok
        ));
    }

    #[tokio::test]
    async fn denied_get_does_not_resolve_key_variables() {
        let authenticator = SimpleAuthenticator::new(HashMap::from([(
            "denied".into(),
            AuthRecord {
                client_id: "denied".into(),
                name: "Denied".into(),
                roles: Vec::new(),
                permissions: Vec::new(),
                enabled: true,
            },
        )]));
        let broker = Broker::new(
            Arc::new(GetFailingStore),
            Some(Arc::new(AuthManager::new(Arc::new(authenticator)))),
        );
        let auth = AuthRecord {
            client_id: "denied".into(),
            name: "Denied".into(),
            roles: Vec::new(),
            permissions: Vec::new(),
            enabled: true,
        }
        .into_info("denied".into())
        .unwrap();

        let result = broker
            .execute(
                Command::Get {
                    namespace: NamespaceContext::new("/profiles").unwrap(),
                    key: Key::new("profile.${selected}").unwrap(),
                },
                RequestContext {
                    owner: Uuid::new_v4(),
                    adapter_instance: None,
                    auth: Some(auth),
                    variables: BTreeMap::new(),
                },
            )
            .await;

        assert!(matches!(result, Err(Error::PermissionDenied(_))));
    }

    #[tokio::test]
    async fn role_limited_principal_is_denied_outside_its_namespace() {
        let auth = AuthRecord {
            client_id: "limited".into(),
            name: "Limited".into(),
            roles: vec![
                crate::Role::Read("/allowed".into()),
                crate::Role::Write("/allowed".into()),
            ],
            permissions: Vec::new(),
            enabled: true,
        }
        .into_info("limited-key".into())
        .unwrap();
        let broker = Broker::new(
            Arc::new(MemoryStore::new()),
            Some(Arc::new(AuthManager::new(Arc::new(
                SimpleAuthenticator::new(HashMap::new()),
            )))),
        );
        let context = RequestContext {
            owner: Uuid::new_v4(),
            adapter_instance: Some(Uuid::new_v4()),
            auth: Some(auth),
            variables: BTreeMap::new(),
        };
        let namespace = NamespaceContext::new("/denied").unwrap();

        for command in [
            Command::Get {
                namespace: namespace.clone(),
                key: Key::new("entry").unwrap(),
            },
            Command::Set {
                namespace: namespace.clone(),
                key: Key::new("entry").unwrap(),
                value: Value::Null,
                ttl_seconds: None,
            },
            Command::Delete {
                namespace: namespace.clone(),
                key_pattern: None,
            },
            Command::Provide {
                namespace_pattern: crate::NamespacePattern::new("/denied").unwrap(),
                key_pattern: KeyPattern::new("*").unwrap(),
                max_rate: None,
                timeout: None,
                miss_ttl: None,
            },
            Command::Store {
                namespace_pattern: crate::NamespacePattern::new("/denied").unwrap(),
                key_pattern: KeyPattern::new("*").unwrap(),
            },
        ] {
            assert!(matches!(
                broker.execute(command, context.clone()).await,
                Err(Error::PermissionDenied(_))
            ));
        }
    }

    #[test]
    fn auth_sessions_refresh_after_half_their_ttl_and_fail_closed_when_revoked() {
        let manager = AuthManager::with_bootstrap_admin(
            Arc::new(SimpleAuthenticator::new(HashMap::new())),
            None,
            Duration::from_millis(20),
        );
        let record = serde_json::json!({
            "client_id": "reader",
            "name": "Reader",
            "permissions": [{"namespace": "/values", "operations": ["read"]}]
        });

        assert!(matches!(
            manager.authenticate("reader"),
            AuthLookup::Pending
        ));
        assert_eq!(
            manager.complete_provider_load("reader".into(), Some(record)),
            Some(Duration::from_millis(10))
        );
        assert!(matches!(
            manager.authenticate("reader"),
            AuthLookup::Authenticated(_)
        ));
        assert!(!manager.take_scheduled_load("reader"));

        std::thread::sleep(Duration::from_millis(12));
        assert!(matches!(
            manager.authenticate("reader"),
            AuthLookup::Authenticated(_)
        ));
        assert!(manager.take_scheduled_load("reader"));

        let revoked = AuthManager::new(Arc::new(SimpleAuthenticator::new(HashMap::new())));
        assert!(matches!(
            revoked.authenticate("revoked"),
            AuthLookup::Pending
        ));
        assert_eq!(revoked.complete_provider_load("revoked".into(), None), None);
        assert!(matches!(
            revoked.authenticate("revoked"),
            AuthLookup::Rejected
        ));
    }

    #[test]
    fn security_key_records_require_a_created_timestamp() {
        assert!(
            parse_security_key(&serde_json::json!({
                "key": "11".repeat(32),
                "created": 1
            }))
            .is_ok()
        );
        assert!(parse_security_key(&serde_json::json!({"key": "11".repeat(32)})).is_err());
    }

    #[test]
    fn encrypted_key_routes_allow_plain_and_contextual_namespaces() {
        assert_eq!(
            security_route(&NamespaceContext::new("/people").unwrap())
                .unwrap()
                .1
                .as_str(),
            "/people"
        );
        assert_eq!(
            security_route(&NamespaceContext::new("/people::42").unwrap())
                .unwrap()
                .1
                .as_str(),
            "/people"
        );
    }
}
