use crate::{
    AdapterError, AppRole, CallbackBridge, OpenBaoClient, OpenBaoMapping, OpenBaoQueryProvider,
    OpenBaoStoreWriter, QueryProvider, Result, StorageWriter,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    time::Duration,
};
use valkyr_client::{ClientBuilder, TlsClientConfig, verified_tls_config};
use valkyr_core::{ProvideOptions, duration};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AdapterConfig {
    pub openbao: OpenBaoConfig,
    pub valkyr: ValkyrConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
    #[serde(default)]
    pub queries: BTreeMap<String, QueryConfig>,
    #[serde(default)]
    pub stores: BTreeMap<String, StoreConfig>,
    #[serde(default)]
    pub providers: BTreeMap<String, ProviderConfig>,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OpenBaoConfig {
    pub address: String,
    pub kv_mount: String,
    pub prefix: String,
    #[serde(default = "timeout")]
    pub request_timeout_seconds: u64,
    pub ca_certificate_file: Option<PathBuf>,
    pub auth: AppRoleConfig,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AppRoleConfig {
    #[serde(rename = "type")]
    pub kind: String,
    pub role_id: String,
    pub secret_id_file: PathBuf,
    #[serde(default = "renew_before")]
    pub renew_before_seconds: u64,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ValkyrConfig {
    pub endpoints: Vec<ValkyrEndpoint>,
    #[serde(default, with = "duration::option")]
    pub provider_wait_timeout: Option<Duration>,
    #[serde(default, with = "duration::option")]
    pub miss_cache_ttl: Option<Duration>,
    #[serde(default = "request_timeout", with = "duration")]
    pub request_timeout: Duration,
    #[serde(default = "retries")]
    pub max_retries: u32,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ValkyrEndpoint {
    pub url: String,
    pub api_key_file: PathBuf,
    pub ca_certificate_file: Option<PathBuf>,
    #[serde(skip)]
    pub api_key: String,
    #[serde(skip)]
    pub tls_config: Option<TlsClientConfig>,
}
impl ValkyrEndpoint {
    pub fn address(&self) -> &str {
        self.url
            .strip_prefix("tcp://")
            .or_else(|| self.url.strip_prefix("tls://"))
            .unwrap_or(&self.url)
    }
    pub fn uses_tls(&self) -> bool {
        self.url.starts_with("tls://")
    }
    pub fn client_builder(&self, adapter_instance: uuid::Uuid, timeout: Duration) -> ClientBuilder {
        let builder = ClientBuilder::new()
            .api_key(self.api_key.clone())
            .adapter_instance(adapter_instance)
            .connection_timeout(timeout)
            .request_timeout(timeout);
        if self.uses_tls() {
            match &self.tls_config {
                Some(config) => builder.tls_server_with_config(self.address(), config.clone()),
                None => builder.tls_server(self.address()),
            }
        } else {
            builder.server(self.address())
        }
    }
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct LoggingConfig {
    pub level: String,
    pub format: LogFormat,
}
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    #[default]
    Pretty,
    Json,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QueryConfig {
    pub namespace_pattern: String,
    pub key_pattern: String,
    #[serde(default)]
    pub on_missing: OnMissing,
    #[serde(default, with = "duration::option")]
    pub provider_wait_timeout: Option<Duration>,
    #[serde(default, with = "duration::option")]
    pub miss_cache_ttl: Option<Duration>,
}
impl ValkyrConfig {
    pub fn provider_options(&self, query: &QueryConfig) -> Result<ProvideOptions> {
        let timeout = query
            .provider_wait_timeout
            .or(self.provider_wait_timeout)
            .unwrap_or_default();
        let miss_ttl = query
            .miss_cache_ttl
            .or(self.miss_cache_ttl)
            .unwrap_or_default();
        Ok(ProvideOptions {
            max_rate: None,
            timeout_ms: duration::to_millis(timeout).map_err(|error| {
                AdapterError::Configuration(format!("provider_wait_timeout: {error}"))
            })?,
            miss_ttl_seconds: duration::to_seconds(miss_ttl)
                .map_err(|error| AdapterError::Configuration(format!("miss_cache_ttl: {error}")))?,
        })
    }
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct OnMissing {
    #[serde(default)]
    pub generate_xchacha20poly1305_key: bool,
    #[serde(default)]
    pub record_created_unix_seconds: bool,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StoreConfig {
    pub namespace_pattern: String,
    pub key_pattern: String,
    #[serde(default)]
    pub allow_context_move: bool,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ProviderConfig {
    pub namespace: String,
    pub frequency: String,
    #[serde(default)]
    pub run_on_startup: bool,
}
const fn timeout() -> u64 {
    5
}
const fn renew_before() -> u64 {
    60
}
const fn request_timeout() -> Duration {
    Duration::from_secs(5)
}
const fn retries() -> u32 {
    9
}
impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".into(),
            format: LogFormat::Pretty,
        }
    }
}

impl AdapterConfig {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let content = fs::read_to_string(path)?;
        let mut config: Self = serde_yaml::from_str(&content)
            .map_err(|e| AdapterError::Configuration(e.to_string()))?;
        let base = path.parent().unwrap_or(Path::new("."));
        config.resolve_paths(base);
        for endpoint in &mut config.valkyr.endpoints {
            endpoint.api_key_file = relative(base, endpoint.api_key_file.clone());
            endpoint.ca_certificate_file = endpoint
                .ca_certificate_file
                .take()
                .map(|path| relative(base, path));
            endpoint.api_key = secret(&endpoint.api_key_file)?;
            endpoint.tls_config = endpoint
                .ca_certificate_file
                .as_ref()
                .map(fs::read)
                .transpose()?
                .as_deref()
                .map(|certificate| verified_tls_config(Some(certificate)))
                .transpose()
                .map_err(|error| AdapterError::Configuration(error.to_string()))?;
        }
        config.validate()?;
        Ok(config)
    }
    fn resolve_paths(&mut self, base: &Path) {
        self.openbao.ca_certificate_file = self
            .openbao
            .ca_certificate_file
            .take()
            .map(|p| relative(base, p));
        self.openbao.auth.secret_id_file = relative(base, self.openbao.auth.secret_id_file.clone());
    }
    pub fn validate(&self) -> Result<()> {
        if self
            .openbao
            .address
            .strip_prefix("http://")
            .or_else(|| self.openbao.address.strip_prefix("https://"))
            .is_none()
        {
            return Err(AdapterError::Configuration(
                "openbao.address must be an HTTP(S) URL".into(),
            ));
        }
        if self.openbao.kv_mount.trim().is_empty() || self.openbao.request_timeout_seconds == 0 {
            return Err(AdapterError::Configuration(
                "openbao.kv_mount and request_timeout_seconds are required".into(),
            ));
        }
        OpenBaoMapping::new(&self.openbao.prefix)?;
        if self.openbao.auth.kind != "approle"
            || self.openbao.auth.role_id.trim().is_empty()
            || self.openbao.auth.renew_before_seconds == 0
        {
            return Err(AdapterError::Configuration(
                "only AppRole auth with role_id and a positive renewal window is supported".into(),
            ));
        }
        if !self.openbao.auth.secret_id_file.is_file() {
            return Err(AdapterError::Configuration(format!(
                "credential file does not exist: {}",
                self.openbao.auth.secret_id_file.display()
            )));
        }
        for (index, endpoint) in self.valkyr.endpoints.iter().enumerate() {
            let (scheme, address) = endpoint
                .url
                .split_once("://")
                .map_or(("", endpoint.url.as_str()), |(scheme, address)| {
                    (scheme, address)
                });
            if !scheme.is_empty() && scheme != "tcp" && scheme != "tls" {
                return Err(AdapterError::Configuration(format!(
                    "Valkyr endpoint {index} uses unsupported URL scheme '{scheme}'"
                )));
            }
            if address.trim().is_empty() {
                return Err(AdapterError::Configuration(format!(
                    "Valkyr endpoint {index} URL must not be empty"
                )));
            }
            if !endpoint.api_key_file.is_file() {
                return Err(AdapterError::Configuration(format!(
                    "Valkyr API key file does not exist: {}",
                    endpoint.api_key_file.display()
                )));
            }
            if endpoint.ca_certificate_file.is_some() && !endpoint.uses_tls() {
                return Err(AdapterError::Configuration(format!(
                    "Valkyr endpoint {index} has a CA certificate but is not TLS"
                )));
            }
            if let Some(path) = &endpoint.ca_certificate_file {
                if !path.is_file() {
                    return Err(AdapterError::Configuration(format!(
                        "Valkyr CA certificate file does not exist: {}",
                        path.display()
                    )));
                }
            }
        }
        if let Some(path) = &self.openbao.ca_certificate_file {
            if !path.is_file() {
                return Err(AdapterError::Configuration(format!(
                    "CA certificate file does not exist: {}",
                    path.display()
                )));
            }
        }
        if self.valkyr.endpoints.is_empty()
            || self.valkyr.request_timeout.is_zero()
            || self.valkyr.max_retries == 0
        {
            return Err(AdapterError::Configuration(
                "Valkyr endpoints, request_timeout, and max_retries must be set".into(),
            ));
        }
        validate_wire_duration(
            "provider_wait_timeout",
            self.valkyr.provider_wait_timeout,
            duration::to_millis,
        )?;
        validate_wire_duration(
            "miss_cache_ttl",
            self.valkyr.miss_cache_ttl,
            duration::to_seconds,
        )?;
        for (name, query) in &self.queries {
            validate_route(name, &query.namespace_pattern, &query.key_pattern)?;
            validate_wire_duration(
                &format!("query '{name}' provider_wait_timeout"),
                query.provider_wait_timeout,
                duration::to_millis,
            )?;
            validate_wire_duration(
                &format!("query '{name}' miss_cache_ttl"),
                query.miss_cache_ttl,
                duration::to_seconds,
            )?;
        }
        for (name, store) in &self.stores {
            validate_route(name, &store.namespace_pattern, &store.key_pattern)?;
        }
        for (name, provider) in &self.providers {
            if !provider.namespace.starts_with('/') || provider.namespace.trim().is_empty() {
                return Err(AdapterError::Configuration(format!(
                    "provider '{name}' has an invalid namespace"
                )));
            }
            provider.schedule()?;
        }
        Ok(())
    }
    pub fn openbao_client(&self) -> Result<OpenBaoClient> {
        let secret_id = secret(&self.openbao.auth.secret_id_file)?;
        let ca = self
            .openbao
            .ca_certificate_file
            .as_ref()
            .map(fs::read)
            .transpose()?;
        OpenBaoClient::new(
            &self.openbao.address,
            self.openbao.kv_mount.clone(),
            Duration::from_secs(self.openbao.request_timeout_seconds),
            AppRole {
                role_id: self.openbao.auth.role_id.clone(),
                secret_id,
            },
            ca.as_deref(),
        )
    }
    pub fn callback_bridge(&self, client: OpenBaoClient) -> Result<CallbackBridge> {
        let mapping = OpenBaoMapping::new(&self.openbao.prefix)?;
        let queries = self
            .queries
            .values()
            .cloned()
            .map(|c| {
                Ok(std::sync::Arc::new(OpenBaoQueryProvider::new(
                    client.clone(),
                    mapping.clone(),
                    c,
                )?) as std::sync::Arc<dyn QueryProvider>)
            })
            .collect::<Result<Vec<_>>>()?;
        let stores =
            self.stores
                .values()
                .cloned()
                .map(|c| {
                    Ok(std::sync::Arc::new(OpenBaoStoreWriter::new(
                        client.clone(),
                        mapping.clone(),
                        c,
                    )?) as std::sync::Arc<dyn StorageWriter>)
                })
                .collect::<Result<Vec<_>>>()?;
        Ok(CallbackBridge::new(queries, stores))
    }
}

fn validate_wire_duration(
    name: &str,
    value: Option<Duration>,
    converter: fn(Duration) -> std::result::Result<u64, String>,
) -> Result<()> {
    if let Some(value) = value {
        converter(value)
            .map_err(|error| AdapterError::Configuration(format!("{name}: {error}")))?;
    }
    Ok(())
}
impl ProviderConfig {
    pub fn schedule(&self) -> Result<cron::Schedule> {
        use std::str::FromStr;
        cron::Schedule::from_str(&self.frequency).map_err(|error| {
            AdapterError::Configuration(format!(
                "invalid provider cron '{}': {error}",
                self.frequency
            ))
        })
    }
}
fn relative(base: &Path, path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        base.join(path)
    }
}
fn secret(path: &Path) -> Result<String> {
    let value = fs::read_to_string(path)?;
    let value = value.trim().to_owned();
    if value.is_empty() {
        Err(AdapterError::Configuration(format!(
            "credential file is empty: {}",
            path.display()
        )))
    } else {
        Ok(value)
    }
}
fn validate_route(name: &str, namespace: &str, key: &str) -> Result<()> {
    if namespace.trim().is_empty() || key.trim().is_empty() || !namespace.starts_with('/') {
        return Err(AdapterError::Configuration(format!(
            "route '{name}' has malformed patterns"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn root() -> PathBuf {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = std::env::temp_dir().join(format!(
            "valkyr-openbao-adapter-config-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&root).unwrap();
        root
    }

    fn yaml(endpoint: &str) -> String {
        format!(
            "openbao:\n  address: https://openbao.test:8200\n  kv_mount: kv\n  prefix: cache\n  auth:\n    type: approle\n    role_id: role\n    secret_id_file: secret-id\nvalkyr:\n  endpoints:\n    - url: {endpoint}\n      api_key_file: api-key\n      ca_certificate_file: ca.crt\n",
        )
    }

    #[test]
    fn resolves_per_endpoint_credentials_and_ca_from_config_directory() {
        let root = root();
        fs::write(root.join("secret-id"), "secret\n").unwrap();
        fs::write(root.join("api-key"), "endpoint-key\n").unwrap();
        fs::copy(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../example/tls/localhost.crt"),
            root.join("ca.crt"),
        )
        .unwrap();
        fs::write(root.join("adapter.yml"), yaml("tls://valkyr.test:8443")).unwrap();

        let config = AdapterConfig::from_file(root.join("adapter.yml")).unwrap();
        let endpoint = &config.valkyr.endpoints[0];
        assert_eq!(endpoint.api_key, "endpoint-key");
        assert_eq!(endpoint.api_key_file, root.join("api-key"));
        assert_eq!(endpoint.ca_certificate_file, Some(root.join("ca.crt")));
        assert!(endpoint.tls_config.is_some());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_legacy_shared_api_key_endpoint_shape() {
        let error = serde_yaml::from_str::<AdapterConfig>(
            "openbao: { address: https://openbao.test:8200, kv_mount: kv, prefix: cache, auth: { type: approle, role_id: role, secret_id_file: secret-id } }\nvalkyr: { endpoints: [tls://valkyr.test:8443], api_key_file: api-key }",
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("invalid type") || error.to_string().contains("endpoints")
        );
    }

    #[test]
    fn provider_options_inherit_each_field_independently() {
        let defaults: ValkyrConfig = serde_yaml::from_str(
            "endpoints: []\nprovider_wait_timeout: 11ms\nmiss_cache_ttl: 22s\n",
        )
        .unwrap();
        let query = |provider_wait_timeout, miss_cache_ttl| QueryConfig {
            namespace_pattern: "/values".into(),
            key_pattern: "*".into(),
            on_missing: OnMissing::default(),
            provider_wait_timeout,
            miss_cache_ttl,
        };
        assert_eq!(
            defaults.provider_options(&query(None, None)).unwrap(),
            ProvideOptions {
                max_rate: None,
                timeout_ms: 11,
                miss_ttl_seconds: 22,
            }
        );
        assert_eq!(
            defaults
                .provider_options(&query(Some(Duration::ZERO), Some(Duration::from_secs(7))))
                .unwrap(),
            ProvideOptions {
                max_rate: None,
                timeout_ms: 0,
                miss_ttl_seconds: 7,
            }
        );

        let converted: ValkyrConfig = serde_yaml::from_str(
            "endpoints: []\nprovider_wait_timeout: 1.5s\nmiss_cache_ttl: 1m\n",
        )
        .unwrap();
        assert_eq!(
            converted.provider_options(&query(None, None)).unwrap(),
            ProvideOptions {
                max_rate: None,
                timeout_ms: 1_500,
                miss_ttl_seconds: 60,
            }
        );
    }

    #[test]
    fn rejects_legacy_names_and_numeric_duration_values() {
        for name in ["timeout_ms", "timeout", "miss_ttl"] {
            let legacy =
                serde_yaml::from_str::<ValkyrConfig>(&format!("endpoints: []\n{name}: 5000\n"));
            assert!(legacy.unwrap_err().to_string().contains("unknown field"));
        }
        for field in ["request_timeout", "provider_wait_timeout", "miss_cache_ttl"] {
            let numeric =
                serde_yaml::from_str::<ValkyrConfig>(&format!("endpoints: []\n{field}: 5000\n"));
            assert!(numeric.is_err(), "{field} should require a string");
        }
    }

    #[test]
    fn rejects_lossy_and_overflowing_wire_durations() {
        let query = QueryConfig {
            namespace_pattern: "/values".into(),
            key_pattern: "*".into(),
            on_missing: OnMissing::default(),
            provider_wait_timeout: Some(Duration::from_nanos(1)),
            miss_cache_ttl: Some(Duration::from_millis(1)),
        };
        let defaults: ValkyrConfig = serde_yaml::from_str("endpoints: []\n").unwrap();
        assert!(defaults.provider_options(&query).is_err());

        let overflowing: ValkyrConfig =
            serde_yaml::from_str("endpoints: []\nprovider_wait_timeout: 18446744073709551616ms\n")
                .unwrap();
        assert!(
            overflowing
                .provider_options(&QueryConfig {
                    provider_wait_timeout: None,
                    ..query
                })
                .is_err()
        );
    }

    #[test]
    fn rejects_malformed_endpoint_ca_before_startup() {
        let root = root();
        fs::write(root.join("secret-id"), "secret").unwrap();
        fs::write(root.join("api-key"), "key").unwrap();
        fs::write(root.join("ca.crt"), "not pem").unwrap();
        fs::write(root.join("adapter.yml"), yaml("tls://valkyr.test:8443")).unwrap();
        let error = AdapterConfig::from_file(root.join("adapter.yml")).unwrap_err();
        assert!(error.to_string().contains("CA certificate"));
        fs::remove_dir_all(root).unwrap();
    }
}
