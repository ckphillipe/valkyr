use crate::{Error, NamespaceContext, Result, Store};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD_NO_PAD};
use chacha20poly1305::{
    AeadCore, KeyInit, XChaCha20Poly1305, XNonce,
    aead::{Aead, OsRng},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use subtle::ConstantTimeEq;

const MAX_AUTH_PERMISSIONS: usize = 256;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "pattern", rename_all = "snake_case")]
pub enum Role {
    Read(String),
    Write(String),
    ReadEncrypted(String),
    WriteEncrypted(String),
    Admin,
}

/// A concise, data-oriented permission record for `/__auth` registry values.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionOperation {
    Read,
    Write,
    ReadEncrypted,
    WriteEncrypted,
    Admin,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Permission {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    pub operations: Vec<PermissionOperation>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Operation {
    Read,
    Write,
    Delete,
    Provide,
    Store,
    ReadEncrypted,
    WriteEncrypted,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthSource {
    Registry,
    BootstrapAdmin,
}

#[derive(Clone, Debug)]
pub struct AuthInfo {
    pub api_key: String,
    pub client_id: String,
    pub roles: Vec<Role>,
    pub source: AuthSource,
    authenticated_at: Instant,
}

impl AuthInfo {
    pub fn is_bootstrap_admin(&self) -> bool {
        self.source == AuthSource::BootstrapAdmin
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuthRecord {
    pub client_id: String,
    pub name: String,
    #[serde(default)]
    pub roles: Vec<Role>,
    #[serde(default)]
    pub permissions: Vec<Permission>,
    #[serde(default = "enabled_by_default")]
    pub enabled: bool,
}

fn enabled_by_default() -> bool {
    true
}

impl AuthRecord {
    pub fn into_info(self, api_key: String) -> Result<AuthInfo> {
        if self.client_id.trim().is_empty() {
            return Err(Error::InvalidAuthorization("client_id is required".into()));
        }
        if self.name.trim().is_empty() {
            return Err(Error::InvalidAuthorization("name is required".into()));
        }
        if !self.enabled {
            return Err(Error::AuthenticationFailed);
        }
        if self.permissions.len() > MAX_AUTH_PERMISSIONS {
            return Err(Error::InvalidAuthorization("too many permissions".into()));
        }
        let permission_count = self.roles.len()
            + self
                .permissions
                .iter()
                .map(|permission| permission.operations.len())
                .sum::<usize>();
        if permission_count > MAX_AUTH_PERMISSIONS {
            return Err(Error::InvalidAuthorization("too many permissions".into()));
        }
        let mut roles = self.roles;
        let permission_roles = self
            .permissions
            .into_iter()
            .map(Permission::into_roles)
            .collect::<Result<Vec<_>>>()?;
        roles.extend(permission_roles.into_iter().flatten());
        Ok(AuthInfo {
            api_key,
            client_id: self.client_id,
            roles,
            source: AuthSource::Registry,
            authenticated_at: Instant::now(),
        })
    }
}

impl Permission {
    fn into_roles(self) -> Result<Vec<Role>> {
        if self.operations.is_empty() {
            return Err(Error::InvalidAuthorization(
                "permission requires at least one operation".into(),
            ));
        }
        if self
            .operations
            .iter()
            .enumerate()
            .any(|(index, operation)| self.operations[..index].contains(operation))
        {
            return Err(Error::InvalidAuthorization(
                "permission operations must not contain duplicates".into(),
            ));
        }

        if self.operations.contains(&PermissionOperation::Admin) {
            if self.operations.len() != 1 {
                return Err(Error::InvalidAuthorization(
                    "admin permission must not be combined with other operations".into(),
                ));
            }
            if self.namespace.is_some() {
                return Err(Error::InvalidAuthorization(
                    "admin permission must not include a namespace".into(),
                ));
            }
            return Ok(vec![Role::Admin]);
        }

        let namespace = self
            .namespace
            .filter(|namespace| !namespace.trim().is_empty())
            .ok_or_else(|| {
                Error::InvalidAuthorization("permission operations require a namespace".into())
            })?;
        Ok(self
            .operations
            .into_iter()
            .map(|operation| match operation {
                PermissionOperation::Read => Role::Read(namespace.clone()),
                PermissionOperation::Write => Role::Write(namespace.clone()),
                PermissionOperation::ReadEncrypted => Role::ReadEncrypted(namespace.clone()),
                PermissionOperation::WriteEncrypted => Role::WriteEncrypted(namespace.clone()),
                PermissionOperation::Admin => unreachable!("admin is handled above"),
            })
            .collect())
    }
}

#[async_trait]
pub trait ApiKeyAuthenticator: Send + Sync {
    async fn authenticate(&self, api_key: &str) -> Result<AuthInfo>;
}

pub struct StoreAuthenticator<S> {
    store: Arc<S>,
}
impl<S> StoreAuthenticator<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl<S: Store> ApiKeyAuthenticator for StoreAuthenticator<S> {
    async fn authenticate(&self, api_key: &str) -> Result<AuthInfo> {
        let namespace = NamespaceContext::new("/__auth")?;
        let key = crate::Key::new(api_key)?;
        let value = self
            .store
            .get(&namespace, &key)
            .await?
            .ok_or(Error::AuthenticationFailed)?
            .value;
        serde_json::from_value::<AuthRecord>(value)
            .map_err(|error| Error::InvalidAuthorization(error.to_string()))?
            .into_info(api_key.to_owned())
    }
}

pub struct SimpleAuthenticator {
    records: HashMap<String, AuthRecord>,
}
impl SimpleAuthenticator {
    pub fn new(records: HashMap<String, AuthRecord>) -> Self {
        Self { records }
    }
}
#[async_trait]
impl ApiKeyAuthenticator for SimpleAuthenticator {
    async fn authenticate(&self, api_key: &str) -> Result<AuthInfo> {
        self.records
            .get(api_key)
            .cloned()
            .ok_or(Error::AuthenticationFailed)?
            .into_info(api_key.to_owned())
    }
}

pub struct AuthManager {
    bootstrap_key: Option<String>,
    timeout: Duration,
    cache: Mutex<HashMap<String, AuthCacheEntry>>,
}

#[derive(Clone, Debug)]
pub enum AuthLookup {
    Authenticated(AuthInfo),
    Pending,
    Rejected,
}

#[derive(Debug)]
enum AuthCacheEntry {
    Loading {
        store_loading: bool,
        dispatched: bool,
    },
    Ready {
        info: AuthInfo,
        expires_at: Instant,
        scheduled: bool,
        loading: bool,
    },
    Rejected {
        expires_at: Instant,
    },
}

impl AuthManager {
    pub fn new(authenticator: Arc<dyn ApiKeyAuthenticator>) -> Self {
        Self::with_bootstrap_admin(authenticator, None, Duration::from_secs(3600))
    }
    pub fn with_bootstrap_admin(
        _authenticator: Arc<dyn ApiKeyAuthenticator>,
        bootstrap_key: Option<String>,
        timeout: Duration,
    ) -> Self {
        Self {
            bootstrap_key,
            timeout,
            cache: Mutex::new(HashMap::new()),
        }
    }
    pub fn authenticate(&self, api_key: &str) -> AuthLookup {
        if self.bootstrap_key.as_deref().is_some_and(|key| {
            api_key.len() == key.len() && api_key.as_bytes().ct_eq(key.as_bytes()).into()
        }) {
            return AuthLookup::Authenticated(AuthInfo {
                api_key: api_key.into(),
                client_id: "bootstrap-admin".into(),
                roles: vec![Role::Admin],
                source: AuthSource::BootstrapAdmin,
                authenticated_at: Instant::now(),
            });
        }
        let now = Instant::now();
        let mut cache = self.cache.lock().expect("auth cache lock");
        match cache.get_mut(api_key) {
            Some(AuthCacheEntry::Ready {
                info,
                expires_at,
                scheduled,
                loading,
            }) if *expires_at > now => {
                if !*loading && now.duration_since(info.authenticated_at) >= self.timeout / 2 {
                    *scheduled = true;
                }
                AuthLookup::Authenticated(info.clone())
            }
            Some(AuthCacheEntry::Loading { .. }) => AuthLookup::Pending,
            Some(AuthCacheEntry::Rejected { expires_at }) if *expires_at > now => {
                AuthLookup::Rejected
            }
            _ => {
                cache.insert(
                    api_key.into(),
                    AuthCacheEntry::Loading {
                        store_loading: false,
                        dispatched: false,
                    },
                );
                AuthLookup::Pending
            }
        }
    }
    pub fn take_store_load(&self, api_key: &str) -> bool {
        let mut cache = self.cache.lock().expect("auth cache lock");
        match cache.get_mut(api_key) {
            Some(AuthCacheEntry::Loading { store_loading, .. }) if !*store_loading => {
                *store_loading = true;
                true
            }
            _ => false,
        }
    }
    pub fn take_scheduled_load(&self, api_key: &str) -> bool {
        let mut cache = self.cache.lock().expect("auth cache lock");
        match cache.get_mut(api_key) {
            Some(AuthCacheEntry::Loading { dispatched, .. }) if !*dispatched => {
                *dispatched = true;
                true
            }
            Some(AuthCacheEntry::Ready {
                scheduled, loading, ..
            }) if *scheduled && !*loading => {
                *scheduled = false;
                *loading = true;
                true
            }
            _ => false,
        }
    }
    pub fn schedule_refresh(&self, api_key: &str) -> bool {
        let now = Instant::now();
        let mut cache = self.cache.lock().expect("auth cache lock");
        let Some(AuthCacheEntry::Ready {
            expires_at,
            scheduled,
            loading,
            ..
        }) = cache.get_mut(api_key)
        else {
            return false;
        };
        if *expires_at <= now || *loading || *scheduled {
            return false;
        }
        *scheduled = true;
        true
    }
    pub fn complete_provider_load(
        &self,
        api_key: String,
        value: Option<Value>,
    ) -> Option<Duration> {
        self.complete_load(api_key, value)
            .then_some(self.timeout / 2)
    }
    pub fn complete_store_load(&self, api_key: String, value: Value) -> AuthLookup {
        self.complete_load(api_key.clone(), Some(value));
        self.authenticate(&api_key)
    }
    /// Replace the cached principal after its `/__auth` record has committed.
    pub fn replace_auth_record(&self, api_key: String, value: Value) {
        self.complete_load(api_key, Some(value));
    }
    fn complete_load(&self, api_key: String, value: Option<Value>) -> bool {
        let now = Instant::now();
        let mut cache = self.cache.lock().expect("auth cache lock");
        let was_refresh = matches!(cache.get(&api_key), Some(AuthCacheEntry::Ready { .. }));
        let info = value
            .ok_or(Error::AuthenticationFailed)
            .and_then(|value| {
                serde_json::from_value::<AuthRecord>(value)
                    .map_err(|error| Error::InvalidAuthorization(error.to_string()))
            })
            .and_then(|record| record.into_info(api_key.clone()));
        match info {
            Ok(info) => {
                cache.insert(
                    api_key,
                    AuthCacheEntry::Ready {
                        info,
                        expires_at: now + self.timeout,
                        scheduled: false,
                        loading: false,
                    },
                );
                true
            }
            Err(_) if was_refresh => {
                cache.remove(&api_key);
                false
            }
            Err(_) => {
                cache.insert(
                    api_key,
                    AuthCacheEntry::Rejected {
                        expires_at: now + self.timeout,
                    },
                );
                false
            }
        }
    }
    pub fn fail_provider_load(&self, api_key: &str) {
        let mut cache = self.cache.lock().expect("auth cache lock");
        if matches!(cache.get(api_key), Some(AuthCacheEntry::Ready { .. })) {
            cache.remove(api_key);
        } else {
            cache.insert(
                api_key.into(),
                AuthCacheEntry::Rejected {
                    expires_at: Instant::now() + self.timeout,
                },
            );
        }
    }
    pub fn authorize(
        &self,
        auth: &AuthInfo,
        namespace: &NamespaceContext,
        operation: Operation,
    ) -> Result<()> {
        if auth.is_bootstrap_admin() {
            return Ok(());
        }
        let now = Instant::now();
        let cache = self.cache.lock().expect("auth cache lock");
        let Some(AuthCacheEntry::Ready {
            info, expires_at, ..
        }) = cache.get(&auth.api_key)
        else {
            return Err(Error::PermissionDenied(namespace.to_string()));
        };
        if *expires_at <= now {
            return Err(Error::PermissionDenied(namespace.to_string()));
        }
        if info
            .roles
            .iter()
            .any(|role| role_allows(role, namespace.as_str(), operation))
        {
            Ok(())
        } else {
            Err(Error::PermissionDenied(namespace.to_string()))
        }
    }
    pub fn disconnect(&self, api_key: &str) {
        self.cache.lock().expect("auth cache lock").remove(api_key);
    }

    pub fn session_timeout(&self) -> Duration {
        self.timeout
    }
}

fn role_allows(role: &Role, namespace: &str, operation: Operation) -> bool {
    match role {
        Role::Admin => true,
        Role::Read(pattern) => {
            operation == Operation::Read && namespace_permission_matches(pattern, namespace)
        }
        Role::Write(pattern) => {
            matches!(
                operation,
                Operation::Write | Operation::Delete | Operation::Provide | Operation::Store
            ) && namespace_permission_matches(pattern, namespace)
        }
        Role::ReadEncrypted(pattern) => {
            operation == Operation::ReadEncrypted
                && namespace_permission_matches(pattern, namespace)
        }
        Role::WriteEncrypted(pattern) => {
            operation == Operation::WriteEncrypted
                && namespace_permission_matches(pattern, namespace)
        }
    }
}

pub fn namespace_permission_matches(pattern: &str, namespace: &str) -> bool {
    if let Some(prefix) = pattern.strip_suffix('*') {
        return namespace.starts_with(prefix);
    }
    namespace == pattern
        || namespace
            .strip_prefix(pattern)
            .is_some_and(|suffix| suffix.starts_with('/') || suffix.starts_with("::"))
}

/// Versioned XChaCha20-Poly1305 JSON envelopes. The base namespace and key are
/// authenticated additional data, preventing ciphertext replay across
/// namespaces while allowing context moves within one namespace.
pub struct ValueCipher;
impl ValueCipher {
    pub fn encrypt(
        key_hex: &str,
        namespace: &NamespaceContext,
        key: &crate::Key,
        value: &Value,
    ) -> Result<Value> {
        let cipher = cipher(key_hex)?;
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
        let plaintext =
            serde_json::to_vec(value).map_err(|error| Error::Encryption(error.to_string()))?;
        let aad = aad(namespace, key);
        let ciphertext = cipher
            .encrypt(
                &nonce,
                chacha20poly1305::aead::Payload {
                    msg: &plaintext,
                    aad: aad.as_bytes(),
                },
            )
            .map_err(|_| Error::Encryption("encryption rejected the input".into()))?;
        Ok(
            json!({"version": 1, "nonce": STANDARD_NO_PAD.encode(nonce), "ciphertext": STANDARD_NO_PAD.encode(ciphertext)}),
        )
    }
    pub fn decrypt(
        key_hex: &str,
        namespace: &NamespaceContext,
        key: &crate::Key,
        envelope: &Value,
    ) -> Result<Value> {
        let cipher = cipher(key_hex)?;
        let nonce_text = envelope
            .get("nonce")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::Encryption("missing nonce".into()))?;
        let ciphertext_text = envelope
            .get("ciphertext")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::Encryption("missing ciphertext".into()))?;
        if envelope.get("version").and_then(Value::as_u64) != Some(1) {
            return Err(Error::Encryption("unsupported envelope version".into()));
        }
        let nonce = STANDARD_NO_PAD
            .decode(nonce_text)
            .map_err(|error| Error::Encryption(error.to_string()))?;
        let ciphertext = STANDARD_NO_PAD
            .decode(ciphertext_text)
            .map_err(|error| Error::Encryption(error.to_string()))?;
        let aad = aad(namespace, key);
        let plaintext = cipher
            .decrypt(
                XNonce::from_slice(&nonce),
                chacha20poly1305::aead::Payload {
                    msg: &ciphertext,
                    aad: aad.as_bytes(),
                },
            )
            .map_err(|_| Error::Encryption("decryption failed".into()))?;
        serde_json::from_slice(&plaintext).map_err(|error| Error::Encryption(error.to_string()))
    }
}

fn aad(namespace: &NamespaceContext, key: &crate::Key) -> String {
    format!("{}\n{}", namespace.ns(), key.as_str())
}

fn cipher(key_hex: &str) -> Result<XChaCha20Poly1305> {
    let bytes = hex_decode(key_hex)?;
    XChaCha20Poly1305::new_from_slice(&bytes).map_err(|_| {
        Error::Encryption("key must be exactly 32 bytes encoded as hexadecimal".into())
    })
}
fn hex_decode(value: &str) -> Result<Vec<u8>> {
    if value.len() % 2 != 0 {
        return Err(Error::Encryption("key is not hexadecimal".into()));
    }
    (0..value.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&value[index..index + 2], 16)
                .map_err(|_| Error::Encryption("key is not hexadecimal".into()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn permissions_respect_namespace_boundaries() {
        assert!(namespace_permission_matches("/users", "/users::42"));
        assert!(namespace_permission_matches("/users", "/users/42"));
        assert!(!namespace_permission_matches("/users", "/users2"));
        assert!(namespace_permission_matches("/users/*", "/users/42"));
        assert!(!namespace_permission_matches("/users/*", "/users"));
        assert!(!namespace_permission_matches("/users/*", "/users::42"));
    }

    #[test]
    fn grouped_permissions_become_roles() {
        let record: AuthRecord = serde_json::from_value(json!({
            "client_id": "reader",
            "name": "Reader",
            "permissions": [{
                "namespace": "/people",
                "operations": ["read", "write", "read_encrypted", "write_encrypted"]
            }]
        }))
        .unwrap();
        let auth = record.into_info("api-key".into()).unwrap();
        assert_eq!(
            auth.roles,
            vec![
                Role::Read("/people".into()),
                Role::Write("/people".into()),
                Role::ReadEncrypted("/people".into()),
                Role::WriteEncrypted("/people".into()),
            ]
        );
    }

    #[test]
    fn permission_records_reject_legacy_and_invalid_groups() {
        let invalid_records = [
            json!({
                "client_id": "reader",
                "name": "Reader",
                "kind": "consumer",
                "permissions": [{"namespace": "/people", "operations": ["read"]}]
            }),
            json!({
                "client_id": "reader",
                "name": "Reader",
                "permissions": [{"operation": "read", "namespace": "/people"}]
            }),
            json!({
                "client_id": "reader",
                "name": "Reader",
                "permissions": [{"namespace": "/people", "operations": []}]
            }),
            json!({
                "client_id": "reader",
                "name": "Reader",
                "permissions": [{"namespace": "/people", "operations": ["read", "read"]}]
            }),
            json!({
                "client_id": "reader",
                "name": "Reader",
                "permissions": [{"operations": ["read"]}]
            }),
            json!({
                "client_id": "reader",
                "name": "Reader",
                "permissions": [{"namespace": "/people", "operations": ["admin"]}]
            }),
            json!({
                "client_id": "reader",
                "name": "Reader",
                "permissions": [{"operations": ["admin", "read"]}]
            }),
        ];

        for record in invalid_records {
            match serde_json::from_value::<AuthRecord>(record) {
                Err(_) => {}
                Ok(record) => assert!(record.into_info("api-key".into()).is_err()),
            }
        }
    }

    #[test]
    fn admin_permission_and_expanded_limit_are_validated() {
        let admin: AuthRecord = serde_json::from_value(json!({
            "client_id": "admin",
            "name": "Admin",
            "permissions": [{"operations": ["admin"]}]
        }))
        .unwrap();
        assert_eq!(
            admin.into_info("api-key".into()).unwrap().roles,
            vec![Role::Admin]
        );

        let permissions = (0..257)
            .map(|_| json!({"namespace": "/people", "operations": ["read"]}))
            .collect::<Vec<_>>();
        let record: AuthRecord = serde_json::from_value(json!({
            "client_id": "reader",
            "name": "Reader",
            "permissions": permissions
        }))
        .unwrap();
        assert!(record.into_info("api-key".into()).is_err());
    }

    #[test]
    fn permission_entry_limit_rejects_empty_groups() {
        let permissions = (0..257)
            .map(|_| json!({"namespace": "/people", "operations": []}))
            .collect::<Vec<_>>();
        let record: AuthRecord = serde_json::from_value(json!({
            "client_id": "reader",
            "name": "Reader",
            "permissions": permissions
        }))
        .unwrap();

        assert!(matches!(
            record.into_info("api-key".into()),
            Err(Error::InvalidAuthorization(message)) if message == "too many permissions"
        ));
    }

    #[test]
    fn cipher_binds_a_value_to_its_route() {
        let namespace = NamespaceContext::new("/users").unwrap();
        let key = crate::Key::new("42").unwrap();
        let encrypted =
            ValueCipher::encrypt(&"11".repeat(32), &namespace, &key, &json!({"name":"Ada"}))
                .unwrap();
        assert_eq!(
            ValueCipher::decrypt(&"11".repeat(32), &namespace, &key, &encrypted).unwrap(),
            json!({"name":"Ada"})
        );
        assert!(
            ValueCipher::decrypt(
                &"11".repeat(32),
                &namespace,
                &crate::Key::new("43").unwrap(),
                &encrypted
            )
            .is_err()
        );
    }

    #[test]
    fn cipher_allows_context_moves_within_a_namespace() {
        let draft = NamespaceContext::new("/users::draft").unwrap();
        let published = NamespaceContext::new("/users::published").unwrap();
        let key = crate::Key::new("42").unwrap();
        let encrypted =
            ValueCipher::encrypt(&"11".repeat(32), &draft, &key, &json!({"name":"Ada"})).unwrap();
        assert_eq!(
            ValueCipher::decrypt(&"11".repeat(32), &published, &key, &encrypted).unwrap(),
            json!({"name":"Ada"})
        );
    }

    #[test]
    fn auth_cache_deduplicates_loads_and_serves_completed_entries() {
        let manager = AuthManager::new(Arc::new(SimpleAuthenticator::new(HashMap::new())));
        assert!(matches!(
            manager.authenticate("reader"),
            AuthLookup::Pending
        ));
        assert!(manager.take_scheduled_load("reader"));
        assert!(matches!(
            manager.authenticate("reader"),
            AuthLookup::Pending
        ));
        assert!(!manager.take_scheduled_load("reader"));

        let refresh_delay = manager.complete_provider_load(
            "reader".into(),
            Some(json!({
                "client_id": "reader",
                "name": "Reader"
            })),
        );
        assert_eq!(refresh_delay, Some(Duration::from_secs(1800)));
        assert!(matches!(
            manager.authenticate("reader"),
            AuthLookup::Authenticated(info) if info.client_id == "reader"
        ));
    }

    #[test]
    fn store_loads_create_sessions_and_reject_invalid_records() {
        let manager = AuthManager::new(Arc::new(SimpleAuthenticator::new(HashMap::new())));
        assert!(matches!(
            manager.authenticate("reader"),
            AuthLookup::Pending
        ));
        assert!(manager.take_store_load("reader"));
        assert!(matches!(
            manager.complete_store_load(
                "reader".into(),
                json!({
                    "client_id": "reader",
                    "name": "Reader"
                })
            ),
            AuthLookup::Authenticated(info) if info.client_id == "reader"
        ));

        assert!(matches!(
            manager.authenticate("disabled"),
            AuthLookup::Pending
        ));
        assert!(manager.take_store_load("disabled"));
        assert!(matches!(
            manager.complete_store_load(
                "disabled".into(),
                json!({
                    "client_id": "disabled",
                    "name": "Disabled",
                    "enabled": false
                })
            ),
            AuthLookup::Rejected
        ));
    }
}
