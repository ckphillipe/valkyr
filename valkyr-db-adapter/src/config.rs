use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};
use valkyr_client::{ClientBuilder, TlsClientConfig, verified_tls_config};
use valkyr_core::{Pattern, ProvideOptions, duration};

use crate::{
    AdapterError, CallbackBridge, DatabaseManager, DatabaseQueryProvider, DatabaseSource,
    DatabaseStoreWriter, QueryProvider, Result, StorageWriter,
};

/// YAML configuration for the standalone adapter.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AdapterConfig {
    pub database: DatabaseConfig,
    pub valkyr: ValkyrConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
    #[serde(default)]
    pub providers: BTreeMap<String, ProviderConfig>,
    #[serde(default)]
    pub queries: BTreeMap<String, QueryConfig>,
    #[serde(default)]
    pub stores: BTreeMap<String, StoreConfig>,
    #[serde(default)]
    pub init: Vec<InitConfig>,
}
/// Structured log output for the standalone database adapter.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct LoggingConfig {
    /// A tracing filter such as `info`, `debug`, or
    /// `valkyr_db_adapter=debug,valkyr_server=info`.
    pub level: String,
    /// Human-readable terminal logs or newline-delimited JSON records.
    pub format: LogFormat,
    /// Include Rust module targets in text output.
    pub target: bool,
    /// Include thread names in output.
    pub thread_names: bool,
    /// Enable ANSI colour codes for text output.
    pub ansi: bool,
}
impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".into(),
            format: LogFormat::Pretty,
            target: false,
            thread_names: false,
            ansi: true,
        }
    }
}
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    #[default]
    Pretty,
    Json,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DatabaseConfig {
    /// A SQLite, MySQL, or PostgreSQL connection URL.
    pub url: String,
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    #[serde(default = "default_connection_timeout_seconds")]
    pub connection_timeout_seconds: u64,
    /// Bound database reads and writes.
    #[serde(default = "default_query_timeout_seconds")]
    pub query_timeout_seconds: u64,
}
const fn default_max_connections() -> u32 {
    5
}
const fn default_connection_timeout_seconds() -> u64 {
    30
}
const fn default_query_timeout_seconds() -> u64 {
    30
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ValkyrConfig {
    pub endpoints: Vec<ValkyrEndpoint>,
    #[serde(default, with = "duration::option")]
    pub provider_wait_timeout: Option<Duration>,
    #[serde(default, with = "duration::option")]
    pub miss_cache_ttl: Option<Duration>,
    #[serde(default = "default_request_timeout", with = "duration")]
    pub request_timeout: Duration,
    #[serde(default = "default_max_retries")]
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
const fn default_request_timeout() -> Duration {
    Duration::from_secs(5)
}
const fn default_max_retries() -> u32 {
    9
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ProviderConfig {
    /// Base namespace when provider rows do not return a `namespace` or `ns`
    /// column. A row `context` column is appended as `::context`.
    pub namespace_pattern: Option<String>,
    /// Documents the concrete key contract returned by the query.
    pub key_pattern: Option<String>,
    pub query: String,
    #[serde(default)]
    pub parameters: BTreeMap<String, serde_yaml::Value>,
    /// Six-field cron schedule, for example `0 */5 * * * *`.
    pub frequency: String,
    #[serde(default)]
    pub run_on_startup: bool,
    pub ttl_seconds: Option<u64>,
    pub description: Option<String>,
}
impl ProviderConfig {
    pub fn schedule(&self) -> Result<cron::Schedule> {
        cron::Schedule::from_str(&self.frequency).map_err(|error| {
            AdapterError::Configuration(format!(
                "invalid provider cron '{}': {error}",
                self.frequency
            ))
        })
    }
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QueryConfig {
    pub namespace_pattern: String,
    pub key_pattern: String,
    pub query: String,
    #[serde(default)]
    pub parameters: Vec<String>,
    pub description: Option<String>,
    pub timeout_seconds: Option<u64>,
    pub ttl_seconds: Option<u64>,
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
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StoreConfig {
    pub namespace_pattern: String,
    pub key_pattern: String,
    pub set_query: String,
    #[serde(default)]
    pub set_parameters: Vec<String>,
    pub delete_query: String,
    #[serde(default)]
    pub delete_parameters: Vec<String>,
    pub move_ns_query: Option<String>,
    pub move_ns_pattern: Option<String>,
    #[serde(default)]
    pub move_ns_parameters: Vec<String>,
    pub delete_ns_query: Option<String>,
    #[serde(default)]
    pub delete_ns_parameters: Vec<String>,
    #[serde(default = "default_query_timeout_seconds")]
    pub timeout_seconds: u64,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct InitConfig {
    pub name: String,
    pub sql: String,
    #[serde(default = "default_init_timeout_seconds")]
    pub timeout_seconds: u64,
}
const fn default_init_timeout_seconds() -> u64 {
    30
}
impl AdapterConfig {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let content = fs::read_to_string(path)?;
        let mut config = serde_yaml::from_str::<Self>(&content)
            .map_err(|error| AdapterError::Configuration(error.to_string()))?;
        let base = path.parent().unwrap_or(Path::new("."));
        for endpoint in &mut config.valkyr.endpoints {
            endpoint.api_key_file = resolve_path(base, endpoint.api_key_file.clone());
            endpoint.ca_certificate_file = endpoint
                .ca_certificate_file
                .take()
                .map(|path| resolve_path(base, path));
            endpoint.api_key = fs::read_to_string(&endpoint.api_key_file)
                .map_err(AdapterError::from)?
                .trim()
                .to_owned();
            if endpoint.api_key.is_empty() {
                return Err(AdapterError::Configuration(format!(
                    "Valkyr API key file must not be empty: {}",
                    endpoint.api_key_file.display()
                )));
            }
            endpoint.tls_config = endpoint
                .ca_certificate_file
                .as_ref()
                .map(fs::read)
                .transpose()
                .map_err(AdapterError::from)?
                .as_deref()
                .map(|certificate| verified_tls_config(Some(certificate)))
                .transpose()
                .map_err(|error| AdapterError::Configuration(error.to_string()))?;
        }
        config.validate()?;
        Ok(config)
    }
    pub fn validate(&self) -> Result<()> {
        if self.database.url.trim().is_empty() {
            return Err(AdapterError::Configuration(
                "database.url is required".into(),
            ));
        }
        if self.database.max_connections == 0
            || self.database.connection_timeout_seconds == 0
            || self.database.query_timeout_seconds == 0
        {
            return Err(AdapterError::Configuration(
                "database connection limits must be greater than zero".into(),
            ));
        }
        if self.valkyr.endpoints.is_empty() {
            return Err(AdapterError::Configuration(
                "at least one Valkyr endpoint is required".into(),
            ));
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
            if !endpoint.api_key.is_empty() && endpoint.api_key.trim().is_empty() {
                return Err(AdapterError::Configuration(format!(
                    "Valkyr API key file must not be empty: {}",
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
        if self.valkyr.request_timeout.is_zero() {
            return Err(AdapterError::Configuration(
                "request_timeout must be greater than zero".into(),
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
        if self.valkyr.max_retries == 0 {
            return Err(AdapterError::Configuration(
                "max_retries must be greater than zero".into(),
            ));
        }
        if self.logging.level.trim().is_empty() {
            return Err(AdapterError::Configuration(
                "logging.level must not be empty".into(),
            ));
        }
        for (name, provider) in &self.providers {
            if provider.query.trim().is_empty() {
                return Err(AdapterError::Configuration(format!(
                    "provider '{name}' has no query"
                )));
            }
            provider.schedule()?;
        }
        for (name, query) in &self.queries {
            validate_callback_config(
                name,
                &query.namespace_pattern,
                &query.key_pattern,
                &query.query,
            )?;
            if query.timeout_seconds == Some(0) {
                return Err(AdapterError::Configuration(format!(
                    "query '{name}' has a zero timeout"
                )));
            }
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
            validate_query_parameters(name, query)?;
        }
        for (name, store) in &self.stores {
            validate_callback_config(
                name,
                &store.namespace_pattern,
                &store.key_pattern,
                &store.set_query,
            )?;
            if store.delete_query.trim().is_empty() {
                return Err(AdapterError::Configuration(format!(
                    "store '{name}' has no delete_query"
                )));
            }
            if store.timeout_seconds == 0 {
                return Err(AdapterError::Configuration(format!(
                    "store '{name}' has a zero timeout"
                )));
            }
            validate_store_operations(name, store)?;
            validate_store_parameters(name, store)?;
        }
        let mut names = std::collections::BTreeSet::new();
        for statement in &self.init {
            if statement.name.trim().is_empty()
                || statement.sql.trim().is_empty()
                || statement.timeout_seconds == 0
            {
                return Err(AdapterError::Configuration(
                    "init statements require name, SQL, and a non-zero timeout".into(),
                ));
            }
            if !names.insert(&statement.name) {
                return Err(AdapterError::Configuration(format!(
                    "duplicate init statement '{}'",
                    statement.name
                )));
            }
        }
        Ok(())
    }
    /// Open the configured SQLx Any database (SQLite, MySQL, or PostgreSQL).
    pub async fn database_manager(&self) -> Result<DatabaseManager> {
        DatabaseManager::connect(&self.database).await
    }
    pub fn database_sources(
        &self,
        database: DatabaseManager,
    ) -> Vec<(String, DatabaseSource, ProviderConfig)> {
        self.providers
            .iter()
            .map(|(name, provider)| {
                (
                    name.clone(),
                    DatabaseSource::new(database.clone(), provider.clone()),
                    provider.clone(),
                )
            })
            .collect()
    }
    pub fn database_callback_bridge(&self, database: DatabaseManager) -> Result<CallbackBridge> {
        let queries = self
            .queries
            .values()
            .cloned()
            .map(|config| {
                Ok(
                    std::sync::Arc::new(DatabaseQueryProvider::new(database.clone(), config)?)
                        as std::sync::Arc<dyn QueryProvider>,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let stores = self
            .stores
            .values()
            .cloned()
            .map(|config| {
                Ok(
                    std::sync::Arc::new(DatabaseStoreWriter::new(database.clone(), config)?)
                        as std::sync::Arc<dyn StorageWriter>,
                )
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

fn resolve_path(base: &Path, path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        base.join(path)
    }
}
pub(crate) fn validate_callback_config(
    name: &str,
    namespace: &str,
    key: &str,
    query: &str,
) -> Result<()> {
    if namespace.trim().is_empty() || key.trim().is_empty() || query.trim().is_empty() {
        return Err(AdapterError::Configuration(format!(
            "callback '{name}' needs namespace_pattern, key_pattern, and query"
        )));
    }
    Ok(())
}
fn declared_parameters(namespace_pattern: &str, key_pattern: &str) -> BTreeSet<String> {
    let mut names = Pattern::new(namespace_pattern).capture_names();
    names.extend(Pattern::new(key_pattern).capture_names());
    names.insert("context".into());
    names
}
fn validate_parameter_names(
    callback: &str,
    operation: &str,
    names: &[String],
    allowed: BTreeSet<String>,
) -> Result<()> {
    for name in names {
        if !allowed.contains(name) {
            return Err(AdapterError::Configuration(format!(
                "{callback} {operation} parameter '{name}' is not declared by its route"
            )));
        }
    }
    Ok(())
}
pub(crate) fn validate_query_parameters(name: &str, query: &QueryConfig) -> Result<()> {
    let mut allowed = declared_parameters(&query.namespace_pattern, &query.key_pattern);
    allowed.extend(["namespace".into(), "key".into()]);
    validate_parameter_names(name, "query", &query.parameters, allowed)
}
pub(crate) fn validate_store_parameters(name: &str, store: &StoreConfig) -> Result<()> {
    let captures = declared_parameters(&store.namespace_pattern, &store.key_pattern);
    let mut set_allowed = captures.clone();
    set_allowed.extend(
        ["namespace", "key", "value", "ttl_seconds"]
            .into_iter()
            .map(str::to_owned),
    );
    validate_parameter_names(name, "set", &store.set_parameters, set_allowed)?;
    let mut delete_allowed = captures.clone();
    delete_allowed.extend(["namespace", "key_pattern"].into_iter().map(str::to_owned));
    validate_parameter_names(name, "delete", &store.delete_parameters, delete_allowed)?;
    let mut move_allowed = captures.clone();
    if let Some(destination_pattern) = &store.move_ns_pattern {
        move_allowed.extend(Pattern::new(destination_pattern).capture_names());
    }
    move_allowed.extend(
        [
            "namespace",
            "source_namespace",
            "destination_namespace",
            "source_context",
            "destination_context",
        ]
        .into_iter()
        .map(str::to_owned),
    );
    validate_parameter_names(name, "move", &store.move_ns_parameters, move_allowed)?;
    let mut delete_namespace_allowed = captures;
    delete_namespace_allowed.extend(["namespace"].into_iter().map(str::to_owned));
    validate_parameter_names(
        name,
        "namespace delete",
        &store.delete_ns_parameters,
        delete_namespace_allowed,
    )
}
pub(crate) fn validate_store_operations(name: &str, store: &StoreConfig) -> Result<()> {
    match (&store.move_ns_query, &store.move_ns_pattern) {
        (None, Some(_)) => {
            return Err(AdapterError::Configuration(format!(
                "store '{name}' has move_ns_pattern without move_ns_query"
            )));
        }
        (Some(query), _) if query.trim().is_empty() => {
            return Err(AdapterError::Configuration(format!(
                "store '{name}' has an empty move_ns_query"
            )));
        }
        _ => {}
    }
    if store.move_ns_query.is_none() && !store.move_ns_parameters.is_empty() {
        return Err(AdapterError::Configuration(format!(
            "store '{name}' has move_ns_parameters without move_ns_query"
        )));
    }
    if store
        .delete_ns_query
        .as_deref()
        .is_some_and(|query| query.trim().is_empty())
    {
        return Err(AdapterError::Configuration(format!(
            "store '{name}' has an empty delete_ns_query"
        )));
    }
    if store.delete_ns_query.is_none() && !store.delete_ns_parameters.is_empty() {
        return Err(AdapterError::Configuration(format!(
            "store '{name}' has delete_ns_parameters without delete_ns_query"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn credential_file(contents: &str) -> PathBuf {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "valkyr-db-adapter-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(&path, contents).unwrap();
        path
    }

    fn config_yaml(api_key_file: &Path, extra: &str) -> String {
        let api_key_file = api_key_file.display().to_string().replace('\'', "''");
        format!(
            "database: {{ url: sqlite://./state.db }}\nvalkyr:\n  endpoints:\n    - url: tcp://127.0.0.1:8081\n      api_key_file: '{api_key_file}'\n{extra}",
        )
    }

    #[test]
    fn accepts_valkyr_configuration_and_dollar_brace_captures() {
        let key_path = credential_file("test-key");
        let config: AdapterConfig = serde_yaml::from_str(&config_yaml(
            &key_path,
            r#"queries:
  service:
    namespace_pattern: /services/${service}
    key_pattern: "{id}"
    query: SELECT value FROM values_table WHERE id = ?
    parameters: [service]
"#,
        ))
        .unwrap();
        config.validate().unwrap();
    }

    #[test]
    fn accepts_structured_json_logging_configuration() {
        let key_path = credential_file("test-key");
        let config: AdapterConfig = serde_yaml::from_str(&config_yaml(
            &key_path,
            r#"logging:
  level: valkyr_db_adapter=debug,valkyr_client=info
  format: json
  target: true
  thread_names: true
  ansi: false
"#,
        ))
        .unwrap();
        config.validate().unwrap();
        assert_eq!(config.logging.format, LogFormat::Json);
        assert!(config.logging.target);
        assert!(config.logging.thread_names);
        assert!(!config.logging.ansi);
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
            query: "SELECT value".into(),
            parameters: Vec::new(),
            description: None,
            timeout_seconds: None,
            ttl_seconds: None,
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
        assert_eq!(
            defaults
                .provider_options(&query(Some(Duration::from_millis(5)), None))
                .unwrap(),
            ProvideOptions {
                max_rate: None,
                timeout_ms: 5,
                miss_ttl_seconds: 22,
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
            query: "SELECT value".into(),
            parameters: Vec::new(),
            description: None,
            timeout_seconds: None,
            ttl_seconds: None,
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
    fn rejects_undeclared_callback_parameters_during_validation() {
        let key_path = credential_file("test-key");
        let config: AdapterConfig = serde_yaml::from_str(&config_yaml(
            &key_path,
            r#"queries:
  service:
    namespace_pattern: /services/{service}
    key_pattern: "{id}"
    query: SELECT value FROM values_table WHERE id = ?
    parameters: [missing]
"#,
        ))
        .unwrap();
        assert!(matches!(
            config.validate(),
            Err(AdapterError::Configuration(message)) if message.contains("missing")
        ));
    }

    #[test]
    fn resolves_relative_credential_paths_from_config_file() {
        let root = std::env::temp_dir().join(format!(
            "valkyr-db-adapter-config-test-{}",
            std::process::id()
        ));
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("api-key"), "  relative-key\n").unwrap();
        fs::write(
            root.join("adapter.yml"),
            "database: { url: sqlite://./state.db }\nvalkyr:\n  endpoints:\n    - url: tcp://127.0.0.1:8081\n      api_key_file: api-key\n",
        )
        .unwrap();
        let config = AdapterConfig::from_file(root.join("adapter.yml")).unwrap();
        assert_eq!(
            config.valkyr.endpoints[0].api_key_file,
            root.join("api-key")
        );
        assert_eq!(config.valkyr.endpoints[0].api_key, "relative-key");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_missing_credential_file() {
        let path = std::env::temp_dir().join(format!(
            "valkyr-db-adapter-missing-key-{}",
            std::process::id()
        ));
        let error = serde_yaml::from_str::<AdapterConfig>(&config_yaml(&path, ""))
            .unwrap()
            .validate()
            .unwrap_err();
        assert!(
            matches!(error, AdapterError::Configuration(message) if message.contains("does not exist"))
        );
    }

    #[test]
    fn rejects_empty_credential_file_and_trims_loaded_key() {
        let whitespace_path = credential_file(" \n\t");
        let root = std::env::temp_dir().join(format!(
            "valkyr-db-adapter-empty-key-test-{}",
            std::process::id()
        ));
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("adapter.yml"), config_yaml(&whitespace_path, "")).unwrap();
        assert!(matches!(
            AdapterConfig::from_file(root.join("adapter.yml")),
            Err(AdapterError::Configuration(message)) if message.contains("must not be empty")
        ));
        fs::remove_dir_all(root).unwrap();

        let key_path = credential_file("  trimmed-key  \n");
        let config: AdapterConfig = serde_yaml::from_str(&config_yaml(&key_path, "")).unwrap();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn loads_endpoint_key_and_private_ca_relative_to_config() {
        let root = std::env::temp_dir().join(format!(
            "valkyr-db-adapter-endpoint-config-test-{}",
            std::process::id()
        ));
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("api-key"), "endpoint-key\n").unwrap();
        fs::write(
            root.join("adapter.yml"),
            format!(
                "database: {{ url: sqlite://./state.db }}\nvalkyr:\n  endpoints:\n    - url: tls://valkyr.test:8443\n      api_key_file: api-key\n      ca_certificate_file: {}\n",
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../example/tls/localhost.crt")
                    .display()
            ),
        )
        .unwrap();
        let config = AdapterConfig::from_file(root.join("adapter.yml")).unwrap();
        let endpoint = &config.valkyr.endpoints[0];
        assert_eq!(endpoint.api_key, "endpoint-key");
        assert!(endpoint.tls_config.is_some());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_ca_configuration_on_plain_tcp_endpoint() {
        let key_path = credential_file("test-key");
        let config: AdapterConfig = serde_yaml::from_str(&format!(
            "database: {{ url: sqlite://./state.db }}\nvalkyr:\n  endpoints:\n    - url: tcp://127.0.0.1:8081\n      api_key_file: {}\n      ca_certificate_file: {}\n",
            key_path.display(),
            key_path.display()
        ))
        .unwrap();
        assert!(matches!(
            config.validate(),
            Err(AdapterError::Configuration(message)) if message.contains("not TLS")
        ));
    }

    #[test]
    fn rejects_plaintext_api_key_configuration() {
        let error = serde_yaml::from_str::<AdapterConfig>(
            r#"
database: { url: sqlite://./state.db }
valkyr:
  endpoints: ["127.0.0.1:8081"]
  api_key: test-key
"#,
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("invalid type") || error.to_string().contains("endpoints")
        );
    }
}
