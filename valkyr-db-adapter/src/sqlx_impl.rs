use async_trait::async_trait;
use serde_json::Value;
use sqlx::{Any, AnyPool, Column, Row, any::AnyPoolOptions};
use std::{collections::BTreeMap, time::Duration};
use valkyr_core::{Key, KeyPattern, NamespaceContext};

use crate::{
    AdapterError, DatabaseConfig, DatabaseValue, InitConfig, ProviderConfig, QueryConfig,
    QueryProvider, Result, StorageWriter, StoreConfig, ValueSource,
    bridge::{namespace_pattern_captures, namespace_pattern_matches, route_captures},
    config::{
        validate_callback_config, validate_query_parameters, validate_store_operations,
        validate_store_parameters,
    },
};

struct SqlOperation {
    query: String,
    parameters: Vec<String>,
    namespace: NamespaceContext,
    key: Option<Key>,
    key_pattern: Option<KeyPattern>,
    value: Option<Value>,
    ttl: Option<Duration>,
    captures: BTreeMap<String, String>,
}
/// Shared SQLx pool for SQLite, MySQL, and PostgreSQL. SQLx selects the
/// driver from the URL; no URL is silently redirected to another driver.
#[derive(Clone, Debug)]
pub struct DatabaseManager {
    pool: AnyPool,
    query_timeout: Duration,
}
impl DatabaseManager {
    pub async fn connect(config: &DatabaseConfig) -> Result<Self> {
        if config.url.trim().is_empty() {
            return Err(AdapterError::Configuration(
                "database.url is required".into(),
            ));
        }
        sqlx::any::install_default_drivers();
        let pool = AnyPoolOptions::new()
            .max_connections(config.max_connections)
            .acquire_timeout(Duration::from_secs(config.connection_timeout_seconds))
            .connect(&config.url)
            .await?;
        Ok(Self {
            pool,
            query_timeout: Duration::from_secs(config.query_timeout_seconds),
        })
    }
    async fn provider_values(&self, provider: &ProviderConfig) -> Result<Vec<DatabaseValue>> {
        let parameters = provider
            .parameters
            .values()
            .map(yaml_parameter)
            .collect::<Result<Vec<_>>>()?;
        let rows = tokio::time::timeout(
            self.query_timeout,
            bind_any(sqlx::query(&provider.query), parameters).fetch_all(&self.pool),
        )
        .await
        .map_err(|_| AdapterError::Configuration("provider query timed out".into()))??;
        rows.into_iter()
            .map(|row| {
                let namespace = row
                    .try_get::<Option<String>, _>("namespace")
                    .ok()
                    .flatten()
                    .or_else(|| row.try_get::<Option<String>, _>("ns").ok().flatten())
                    .or_else(|| provider.namespace_pattern.clone())
                    .ok_or_else(|| {
                        AdapterError::Configuration(
                            "provider row needs namespace/ns or provider.namespace_pattern".into(),
                        )
                    })?;
                let context = row.try_get::<Option<String>, _>("context").ok().flatten();
                let namespace = match context {
                    Some(context) if !context.is_empty() => {
                        if namespace.contains("::") {
                            return Err(AdapterError::Configuration(
                                "provider row cannot combine a decorated namespace with context"
                                    .into(),
                            ));
                        }
                        format!("{namespace}::{context}")
                    }
                    _ => namespace,
                };
                let key: String = row.try_get("key")?;
                let value = any_json(&row, column_index(&row, "value")?)?;
                let ttl = row
                    .try_get::<Option<i64>, _>("ttl_seconds")
                    .ok()
                    .flatten()
                    .map(|ttl| Duration::from_secs(ttl.max(0) as u64))
                    .or_else(|| provider.ttl_seconds.map(Duration::from_secs));
                Ok(DatabaseValue {
                    namespace: NamespaceContext::new(namespace)?,
                    key: Key::new(key)?,
                    value,
                    ttl,
                })
            })
            .collect()
    }
    /// Execute a configured one-time initialization statement with a bounded
    /// timeout. The service invokes these only at process startup.
    pub async fn execute_init(&self, statement: &InitConfig) -> Result<()> {
        tokio::time::timeout(
            Duration::from_secs(statement.timeout_seconds),
            sqlx::query(&statement.sql).execute(&self.pool),
        )
        .await
        .map_err(|_| {
            AdapterError::Configuration(format!("init statement '{}' timed out", statement.name))
        })??;
        Ok(())
    }
    async fn query_value(
        &self,
        query: &str,
        parameters: Vec<AnyParameter>,
        timeout: Option<u64>,
    ) -> Result<Option<Value>> {
        let row = tokio::time::timeout(
            timeout
                .map(Duration::from_secs)
                .unwrap_or(self.query_timeout),
            bind_any(sqlx::query(query), parameters).fetch_optional(&self.pool),
        )
        .await
        .map_err(|_| AdapterError::Configuration("query callback timed out".into()))??;
        row.map(|row| any_json(&row, 0)).transpose()
    }
    async fn execute(
        &self,
        query: &str,
        parameters: Vec<AnyParameter>,
        timeout: Duration,
    ) -> Result<()> {
        tokio::time::timeout(
            timeout,
            bind_any(sqlx::query(query), parameters).execute(&self.pool),
        )
        .await
        .map_err(|_| AdapterError::Configuration("storage query timed out".into()))??;
        Ok(())
    }
    async fn execute_batch(
        &self,
        operations: Vec<(String, Vec<AnyParameter>)>,
        timeout: Duration,
    ) -> Result<()> {
        tokio::time::timeout(timeout, async {
            let mut transaction = self.pool.begin().await?;
            for (query, parameters) in operations {
                bind_any(sqlx::query(&query), parameters)
                    .execute(&mut *transaction)
                    .await?;
            }
            transaction.commit().await?;
            Ok::<(), sqlx::Error>(())
        })
        .await
        .map_err(|_| AdapterError::Configuration("storage batch timed out".into()))??;
        Ok(())
    }
}
/// A scheduled source backed by any database supported by SQLx Any.
#[derive(Clone, Debug)]
pub struct DatabaseSource {
    database: DatabaseManager,
    provider: ProviderConfig,
}
impl DatabaseSource {
    pub fn new(database: DatabaseManager, provider: ProviderConfig) -> Self {
        Self { database, provider }
    }
}
#[async_trait]
impl ValueSource for DatabaseSource {
    async fn fetch_values(&self) -> Result<Vec<DatabaseValue>> {
        self.database.provider_values(&self.provider).await
    }
}
#[derive(Clone, Debug)]
enum AnyParameter {
    Text(String),
    Integer(i64),
    Float(f64),
    Bool(bool),
    Null,
}
fn bind_any<'q>(
    mut query: sqlx::query::Query<'q, Any, sqlx::any::AnyArguments<'q>>,
    parameters: Vec<AnyParameter>,
) -> sqlx::query::Query<'q, Any, sqlx::any::AnyArguments<'q>> {
    for parameter in parameters {
        query = match parameter {
            AnyParameter::Text(value) => query.bind(value),
            AnyParameter::Integer(value) => query.bind(value),
            AnyParameter::Float(value) => query.bind(value),
            AnyParameter::Bool(value) => query.bind(value),
            AnyParameter::Null => query.bind(Option::<String>::None),
        };
    }
    query
}
fn yaml_parameter(value: &serde_yaml::Value) -> Result<AnyParameter> {
    match value {
        serde_yaml::Value::Null => Ok(AnyParameter::Null),
        serde_yaml::Value::Bool(value) => Ok(AnyParameter::Bool(*value)),
        serde_yaml::Value::Number(value) => value
            .as_i64()
            .map(AnyParameter::Integer)
            .or_else(|| value.as_f64().map(AnyParameter::Float))
            .ok_or_else(|| {
                AdapterError::Configuration("unsupported YAML numeric parameter".into())
            }),
        serde_yaml::Value::String(value) => Ok(AnyParameter::Text(value.clone())),
        _ => Err(AdapterError::Configuration(
            "provider parameters must be scalar YAML values".into(),
        )),
    }
}
fn any_json(row: &sqlx::any::AnyRow, index: usize) -> Result<Value> {
    if let Ok(value) = row.try_get::<Option<String>, _>(index) {
        return Ok(value
            .map(|value| serde_json::from_str(&value).unwrap_or(Value::String(value)))
            .unwrap_or(Value::Null));
    }
    if let Ok(value) = row.try_get::<Option<i64>, _>(index) {
        return Ok(value.map(Value::from).unwrap_or(Value::Null));
    }
    if let Ok(value) = row.try_get::<Option<f64>, _>(index) {
        return value
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number)
            .ok_or_else(|| {
                AdapterError::Configuration("database returned a non-finite number".into())
            });
    }
    if let Ok(value) = row.try_get::<Option<bool>, _>(index) {
        return Ok(value.map(Value::Bool).unwrap_or(Value::Null));
    }
    Err(AdapterError::Configuration(
        "database value cannot be represented as JSON".into(),
    ))
}
fn column_index(row: &sqlx::any::AnyRow, name: &str) -> Result<usize> {
    row.columns()
        .iter()
        .position(|column| column.name().eq_ignore_ascii_case(name))
        .ok_or_else(|| AdapterError::Configuration(format!("database row has no '{name}' column")))
}
fn any_parameters(
    names: &[String],
    namespace: &NamespaceContext,
    key: Option<&Key>,
    key_pattern: Option<&KeyPattern>,
    value: Option<&Value>,
    ttl: Option<Duration>,
    captures: &BTreeMap<String, String>,
) -> Result<Vec<AnyParameter>> {
    names
        .iter()
        .map(|name| match name.as_str() {
            "namespace" => Ok(AnyParameter::Text(namespace.as_str().into())),
            "key" => key
                .map(|key| AnyParameter::Text(key.as_str().into()))
                .ok_or_else(|| {
                    AdapterError::Configuration("parameter 'key' is unavailable".into())
                }),
            "key_pattern" => key_pattern
                .map(|key| AnyParameter::Text(key.as_str().into()))
                .ok_or_else(|| {
                    AdapterError::Configuration("parameter 'key_pattern' is unavailable".into())
                }),
            "value" => value
                .map(|value| AnyParameter::Text(value.to_string()))
                .ok_or_else(|| {
                    AdapterError::Configuration("parameter 'value' is unavailable".into())
                }),
            "ttl_seconds" => Ok(ttl.map_or(AnyParameter::Null, |ttl| {
                AnyParameter::Integer(ttl.as_secs() as i64)
            })),
            "source_namespace" | "destination_namespace" => captures
                .get(name)
                .cloned()
                .map(AnyParameter::Text)
                .ok_or_else(|| {
                    AdapterError::Configuration(format!("parameter '{name}' is unavailable"))
                }),
            "source_context" | "destination_context" | "context" => Ok(captures
                .get(name)
                .filter(|value| !value.is_empty())
                .cloned()
                .map(AnyParameter::Text)
                .unwrap_or_else(|| AnyParameter::Text(String::new()))),
            variable => captures
                .get(variable)
                .cloned()
                .map(AnyParameter::Text)
                .ok_or_else(|| {
                    AdapterError::Configuration(format!("unknown SQL parameter '{variable}'"))
                }),
        })
        .collect()
}
/// On-demand query provider backed by a SQLx Any pool.
#[derive(Clone, Debug)]
pub struct DatabaseQueryProvider {
    database: DatabaseManager,
    config: QueryConfig,
}
impl DatabaseQueryProvider {
    pub fn new(database: DatabaseManager, config: QueryConfig) -> Result<Self> {
        validate_callback_config(
            "query",
            &config.namespace_pattern,
            &config.key_pattern,
            &config.query,
        )?;
        validate_query_parameters("query", &config)?;
        Ok(Self { database, config })
    }
    fn captures(
        &self,
        namespace: &NamespaceContext,
        key: &Key,
    ) -> Option<BTreeMap<String, String>> {
        route_captures(
            &self.config.namespace_pattern,
            &self.config.key_pattern,
            namespace,
            key,
        )
    }
}
#[async_trait]
impl QueryProvider for DatabaseQueryProvider {
    fn matches(&self, namespace: &NamespaceContext, key: &Key) -> bool {
        self.captures(namespace, key).is_some()
    }
    async fn query(&self, namespace: NamespaceContext, key: Key) -> Result<Option<DatabaseValue>> {
        let Some(captures) = self.captures(&namespace, &key) else {
            return Ok(None);
        };
        let parameters = any_parameters(
            &self.config.parameters,
            &namespace,
            Some(&key),
            None,
            None,
            None,
            &captures,
        )?;
        let value = self
            .database
            .query_value(&self.config.query, parameters, self.config.timeout_seconds)
            .await?;
        Ok(value.map(|value| DatabaseValue {
            namespace,
            key,
            value,
            ttl: self.config.ttl_seconds.map(Duration::from_secs),
        }))
    }
}
/// Durable SQLx Any persistence callback for SQLite, MySQL, and PostgreSQL.
#[derive(Clone, Debug)]
pub struct DatabaseStoreWriter {
    database: DatabaseManager,
    config: StoreConfig,
}
impl DatabaseStoreWriter {
    pub fn new(database: DatabaseManager, config: StoreConfig) -> Result<Self> {
        validate_callback_config(
            "store",
            &config.namespace_pattern,
            &config.key_pattern,
            &config.set_query,
        )?;
        if config.delete_query.trim().is_empty() {
            return Err(AdapterError::Configuration(
                "store has no delete_query".into(),
            ));
        }
        validate_store_operations("store", &config)?;
        validate_store_parameters("store", &config)?;
        Ok(Self { database, config })
    }
    fn captures(
        &self,
        namespace: &NamespaceContext,
        key: &Key,
    ) -> Option<BTreeMap<String, String>> {
        route_captures(
            &self.config.namespace_pattern,
            &self.config.key_pattern,
            namespace,
            key,
        )
    }
    async fn execute(&self, operation: SqlOperation) -> Result<()> {
        let parameters = any_parameters(
            &operation.parameters,
            &operation.namespace,
            operation.key.as_ref(),
            operation.key_pattern.as_ref(),
            operation.value.as_ref(),
            operation.ttl,
            &operation.captures,
        )?;
        self.database
            .execute(
                &operation.query,
                parameters,
                Duration::from_secs(self.config.timeout_seconds),
            )
            .await
    }
    fn set_operation(&self, value: DatabaseValue) -> Result<SqlOperation> {
        let captures = self
            .captures(&value.namespace, &value.key)
            .ok_or_else(|| AdapterError::Configuration("set route does not match store".into()))?;
        Ok(SqlOperation {
            query: self.config.set_query.clone(),
            parameters: self.config.set_parameters.clone(),
            namespace: value.namespace,
            key: Some(value.key),
            key_pattern: None,
            value: Some(value.value),
            ttl: value.ttl,
            captures,
        })
    }
}
#[async_trait]
impl StorageWriter for DatabaseStoreWriter {
    fn matches(&self, namespace: &NamespaceContext, key_pattern: Option<&KeyPattern>) -> bool {
        match key_pattern {
            Some(pattern) => Key::new(pattern.as_str())
                .ok()
                .and_then(|key| self.captures(namespace, &key))
                .is_some(),
            None => namespace_pattern_matches(&self.config.namespace_pattern, namespace.as_str()),
        }
    }
    async fn set(&self, value: DatabaseValue) -> Result<()> {
        self.execute(self.set_operation(value)?).await
    }
    async fn set_batch(
        &self,
        namespace: NamespaceContext,
        entries: Vec<(Key, Value)>,
        ttl: Option<Duration>,
    ) -> Result<()> {
        let operations = entries
            .into_iter()
            .map(|(key, value)| {
                self.set_operation(DatabaseValue {
                    namespace: namespace.clone(),
                    key,
                    value,
                    ttl,
                })
            })
            .map(|operation| {
                let operation = operation?;
                let parameters = any_parameters(
                    &operation.parameters,
                    &operation.namespace,
                    operation.key.as_ref(),
                    operation.key_pattern.as_ref(),
                    operation.value.as_ref(),
                    operation.ttl,
                    &operation.captures,
                )?;
                Ok((operation.query, parameters))
            })
            .collect::<Result<Vec<_>>>()?;
        self.database
            .execute_batch(operations, Duration::from_secs(self.config.timeout_seconds))
            .await
    }
    async fn delete(
        &self,
        namespace: NamespaceContext,
        key_pattern: Option<KeyPattern>,
    ) -> Result<()> {
        let (query, parameters) = match key_pattern.as_ref() {
            Some(_) => (
                self.config.delete_query.clone(),
                self.config.delete_parameters.clone(),
            ),
            None => (
                self.config.delete_ns_query.clone().ok_or_else(|| {
                    AdapterError::Configuration("store does not support namespace deletes".into())
                })?,
                self.config.delete_ns_parameters.clone(),
            ),
        };
        let captures = key_pattern
            .as_ref()
            .and_then(|pattern| Key::new(pattern.as_str()).ok())
            .and_then(|key| self.captures(&namespace, &key))
            .unwrap_or_default();
        self.execute(SqlOperation {
            query,
            parameters,
            namespace,
            key: None,
            key_pattern,
            value: None,
            ttl: None,
            captures,
        })
        .await
    }
    async fn move_namespace(
        &self,
        source: NamespaceContext,
        destination: NamespaceContext,
    ) -> Result<()> {
        if let Some(pattern) = &self.config.move_ns_pattern {
            if namespace_pattern_captures(pattern, destination.as_str()).is_none() {
                return Err(AdapterError::Configuration(
                    "store does not support moves to this namespace".into(),
                ));
            }
        }
        let query = self.config.move_ns_query.clone().ok_or_else(|| {
            AdapterError::Configuration("store does not support namespace moves".into())
        })?;
        let mut captures =
            namespace_pattern_captures(&self.config.namespace_pattern, source.as_str())
                .unwrap_or_default();
        if let Some(pattern) = &self.config.move_ns_pattern {
            captures.extend(
                namespace_pattern_captures(pattern, destination.as_str()).unwrap_or_default(),
            );
        }
        captures.insert("source_namespace".into(), source.as_str().into());
        captures.insert("destination_namespace".into(), destination.as_str().into());
        captures.insert(
            "source_context".into(),
            source.ctx().unwrap_or_default().into(),
        );
        captures.insert(
            "destination_context".into(),
            destination.ctx().unwrap_or_default().into(),
        );
        self.execute(SqlOperation {
            query,
            parameters: self.config.move_ns_parameters.clone(),
            namespace: source,
            key: None,
            key_pattern: None,
            value: None,
            ttl: None,
            captures,
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn generic_database_manager_runs_init_and_reads_provider_rows() {
        let database = DatabaseManager::connect(&DatabaseConfig {
            url: "sqlite::memory:".into(),
            max_connections: 1,
            connection_timeout_seconds: 5,
            query_timeout_seconds: 5,
        })
        .await
        .unwrap();
        database
            .execute_init(&InitConfig {
                name: "values".into(),
                sql: "CREATE TABLE values_table (namespace TEXT, key TEXT, value TEXT, ttl_seconds INTEGER); INSERT INTO values_table VALUES ('/people', 'ada', '{\"name\":\"Ada\"}', 42);".into(),
                timeout_seconds: 5,
            })
            .await
            .unwrap();
        let values = DatabaseSource::new(
            database,
            ProviderConfig {
                namespace_pattern: None,
                key_pattern: None,
                query: "SELECT namespace, key, value, ttl_seconds FROM values_table".into(),
                parameters: BTreeMap::new(),
                frequency: "0 * * * * *".into(),
                run_on_startup: false,
                ttl_seconds: None,
                description: None,
            },
        )
        .fetch_values()
        .await
        .unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].value, serde_json::json!({"name": "Ada"}));
        assert_eq!(values[0].ttl, Some(Duration::from_secs(42)));
    }
    #[tokio::test]
    async fn database_store_batch_rolls_back_on_a_failed_write() {
        let database = DatabaseManager::connect(&DatabaseConfig {
            url: "sqlite::memory:".into(),
            max_connections: 1,
            connection_timeout_seconds: 5,
            query_timeout_seconds: 5,
        })
        .await
        .unwrap();
        database
            .execute_init(&InitConfig {
                name: "values".into(),
                sql: "CREATE TABLE values_table (namespace TEXT, state_key TEXT UNIQUE, value TEXT); INSERT INTO values_table VALUES ('/people', 'duplicate', 'null');".into(),
                timeout_seconds: 5,
            })
            .await
            .unwrap();
        let store = DatabaseStoreWriter::new(
            database.clone(),
            StoreConfig {
                namespace_pattern: "/people".into(),
                key_pattern: "*".into(),
                set_query: "INSERT INTO values_table(namespace, state_key, value) VALUES (?, ?, ?)"
                    .into(),
                set_parameters: vec!["namespace".into(), "key".into(), "value".into()],
                delete_query: "DELETE FROM values_table WHERE namespace = ?".into(),
                delete_parameters: vec!["namespace".into()],
                move_ns_query: None,
                move_ns_pattern: None,
                move_ns_parameters: Vec::new(),
                delete_ns_query: None,
                delete_ns_parameters: Vec::new(),
                timeout_seconds: 5,
            },
        )
        .unwrap();
        assert!(
            store
                .set_batch(
                    NamespaceContext::new("/people").unwrap(),
                    vec![
                        (Key::new("first").unwrap(), serde_json::json!(1)),
                        (Key::new("duplicate").unwrap(), serde_json::json!(2)),
                    ],
                    None,
                )
                .await
                .is_err()
        );
        assert_eq!(
            database
                .query_value(
                    "SELECT value FROM values_table WHERE state_key = 'first'",
                    Vec::new(),
                    None,
                )
                .await
                .unwrap(),
            None
        );
    }
    #[tokio::test]
    async fn provider_uses_configured_namespace_and_row_context() {
        let database = DatabaseManager::connect(&DatabaseConfig {
            url: "sqlite::memory:".into(),
            max_connections: 1,
            connection_timeout_seconds: 5,
            query_timeout_seconds: 5,
        })
        .await
        .unwrap();
        database
            .execute_init(&InitConfig {
                name: "rows".into(),
                sql: "CREATE TABLE rows_table (key TEXT, value TEXT, context TEXT); INSERT INTO rows_table VALUES ('voice', '\"hello\"', '20818');".into(),
                timeout_seconds: 5,
            })
            .await
            .unwrap();
        let values = DatabaseSource::new(
            database,
            ProviderConfig {
                namespace_pattern: Some("/svcx".into()),
                key_pattern: Some("{field}".into()),
                query: "SELECT key, value, context FROM rows_table".into(),
                parameters: BTreeMap::new(),
                frequency: "0 * * * * *".into(),
                run_on_startup: false,
                ttl_seconds: Some(60),
                description: None,
            },
        )
        .fetch_values()
        .await
        .unwrap();
        assert_eq!(values[0].namespace.as_str(), "/svcx::20818");
        assert_eq!(values[0].key.as_str(), "voice");
        assert_eq!(values[0].value, serde_json::json!("hello"));
        assert_eq!(values[0].ttl, Some(Duration::from_secs(60)));
    }
    #[tokio::test]
    async fn provider_uses_base_namespace_for_an_empty_row_context() {
        let database = DatabaseManager::connect(&DatabaseConfig {
            url: "sqlite::memory:".into(),
            max_connections: 1,
            connection_timeout_seconds: 5,
            query_timeout_seconds: 5,
        })
        .await
        .unwrap();
        database
            .execute_init(&InitConfig {
                name: "rows".into(),
                sql: "CREATE TABLE rows_table (key TEXT, value TEXT, context TEXT); INSERT INTO rows_table VALUES ('voice', '\"hello\"', '');".into(),
                timeout_seconds: 5,
            })
            .await
            .unwrap();
        let values = DatabaseSource::new(
            database,
            ProviderConfig {
                namespace_pattern: Some("/svcx".into()),
                key_pattern: Some("{field}".into()),
                query: "SELECT key, value, context FROM rows_table".into(),
                parameters: BTreeMap::new(),
                frequency: "0 * * * * *".into(),
                run_on_startup: false,
                ttl_seconds: None,
                description: None,
            },
        )
        .fetch_values()
        .await
        .unwrap();
        assert_eq!(values[0].namespace.as_str(), "/svcx");
    }
    #[tokio::test]
    async fn provider_rejects_decorated_namespace_with_a_context_column() {
        let database = DatabaseManager::connect(&DatabaseConfig {
            url: "sqlite::memory:".into(),
            max_connections: 1,
            connection_timeout_seconds: 5,
            query_timeout_seconds: 5,
        })
        .await
        .unwrap();
        let source = DatabaseSource::new(
            database,
            ProviderConfig {
                namespace_pattern: None,
                key_pattern: None,
                query: "SELECT '/svcx::existing' AS namespace, 'voice' AS key, '\"hello\"' AS value, 'next' AS context".into(),
                parameters: BTreeMap::new(),
                frequency: "0 * * * * *".into(),
                run_on_startup: false,
                ttl_seconds: None,
                description: None,
            },
        );
        assert!(matches!(
            source.fetch_values().await,
            Err(AdapterError::Configuration(message)) if message.contains("decorated namespace")
        ));
    }
    #[tokio::test]
    async fn move_uses_destination_pattern_captures() {
        let database = DatabaseManager::connect(&DatabaseConfig {
            url: "sqlite::memory:".into(),
            max_connections: 1,
            connection_timeout_seconds: 5,
            query_timeout_seconds: 5,
        })
        .await
        .unwrap();
        database
            .execute_init(&InitConfig {
                name: "values".into(),
                sql: "CREATE TABLE values_table (namespace TEXT, state_key TEXT); INSERT INTO values_table VALUES ('/people/ada', 'name');".into(),
                timeout_seconds: 5,
            })
            .await
            .unwrap();
        let store = DatabaseStoreWriter::new(
            database.clone(),
            StoreConfig {
                namespace_pattern: "/people/{old}".into(),
                key_pattern: "*".into(),
                set_query: "INSERT INTO values_table(namespace, state_key) VALUES (?, ?)".into(),
                set_parameters: vec!["namespace".into(), "key".into()],
                delete_query: "DELETE FROM values_table WHERE namespace = ?".into(),
                delete_parameters: vec!["namespace".into()],
                move_ns_query: Some(
                    "UPDATE values_table SET namespace = '/people/' || ? WHERE namespace = '/people/' || ?".into(),
                ),
                move_ns_pattern: Some("/people/{new}".into()),
                move_ns_parameters: vec!["new".into(), "old".into()],
                delete_ns_query: None,
                delete_ns_parameters: Vec::new(),
                timeout_seconds: 5,
            },
        )
        .unwrap();
        let mut captures = namespace_pattern_captures("/people/{old}", "/people/ada").unwrap();
        captures.extend(namespace_pattern_captures("/people/{new}", "/people/grace").unwrap());
        assert_eq!(captures["old"], "ada");
        assert_eq!(captures["new"], "grace");
        assert!(matches!(
            any_parameters(
                &["new".into(), "old".into()],
                &NamespaceContext::new("/people/ada").unwrap(),
                None,
                None,
                None,
                None,
                &captures,
            )
            .unwrap()
            .as_slice(),
            [AnyParameter::Text(new), AnyParameter::Text(old)] if new == "grace" && old == "ada"
        ));
        store
            .move_namespace(
                NamespaceContext::new("/people/ada").unwrap(),
                NamespaceContext::new("/people/grace").unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            database
                .query_value("SELECT namespace FROM values_table", Vec::new(), None)
                .await
                .unwrap(),
            Some(serde_json::json!("/people/grace"))
        );
    }
    #[test]
    fn literal_adapter_namespace_pattern_accepts_a_context_suffix() {
        let captures = route_captures(
            "/svcx",
            "{field}",
            &NamespaceContext::new("/svcx::20818").unwrap(),
            &Key::new("voice").unwrap(),
        )
        .unwrap();
        assert_eq!(captures["context"], "20818");
        assert_eq!(captures["field"], "voice");
    }
    #[test]
    fn context_parameter_is_empty_without_a_context_route() {
        let captures = route_captures(
            "/people",
            "{id}",
            &NamespaceContext::new("/people").unwrap(),
            &Key::new("ada").unwrap(),
        )
        .unwrap();
        let namespace = NamespaceContext::new("/people").unwrap();
        let key = Key::new("ada").unwrap();
        assert!(matches!(
            any_parameters(
                &["context".into()],
                &namespace,
                Some(&key),
                None,
                None,
                None,
                &captures,
            )
            .unwrap()
            .as_slice(),
            [AnyParameter::Text(context)] if context.is_empty()
        ));
    }

    #[test]
    fn context_parameters_use_cached_route_context_for_query_and_store() {
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
            let parameters = any_parameters(
                &["context".into()],
                &namespace,
                Some(&key),
                None,
                None,
                None,
                &captures,
            )
            .unwrap();

            assert!(matches!(
                parameters.as_slice(),
                [AnyParameter::Text(context)] if context == expected_context
            ));
        }
    }

    #[tokio::test]
    async fn move_binds_cached_source_and_destination_contexts() {
        let database = DatabaseManager::connect(&DatabaseConfig {
            url: "sqlite::memory:".into(),
            max_connections: 1,
            connection_timeout_seconds: 5,
            query_timeout_seconds: 5,
        })
        .await
        .unwrap();
        database
            .execute_init(&InitConfig {
                name: "moves".into(),
                sql: "CREATE TABLE moves (id INTEGER PRIMARY KEY AUTOINCREMENT, source_context TEXT, destination_context TEXT);".into(),
                timeout_seconds: 5,
            })
            .await
            .unwrap();
        let store = DatabaseStoreWriter::new(
            database.clone(),
            StoreConfig {
                namespace_pattern: "*".into(),
                key_pattern: "*".into(),
                set_query: "SELECT 1".into(),
                set_parameters: Vec::new(),
                delete_query: "SELECT 1".into(),
                delete_parameters: Vec::new(),
                move_ns_query: Some(
                    "INSERT INTO moves(source_context, destination_context) VALUES (?, ?)".into(),
                ),
                move_ns_pattern: Some("*".into()),
                move_ns_parameters: vec!["source_context".into(), "destination_context".into()],
                delete_ns_query: None,
                delete_ns_parameters: Vec::new(),
                timeout_seconds: 5,
            },
        )
        .unwrap();

        for (source, destination, expected) in [
            ("/tenant", "/other", "|"),
            ("/tenant::user", "/other::admin", "user|admin"),
            ("::tenant", "/other::", "|"),
            (
                "/tenant::user::region",
                "/other::admin::west",
                "user::region|admin::west",
            ),
        ] {
            store
                .move_namespace(
                    NamespaceContext::new(source).unwrap(),
                    NamespaceContext::new(destination).unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                database
                    .query_value(
                        "SELECT source_context || '|' || destination_context FROM moves ORDER BY id DESC LIMIT 1",
                        Vec::new(),
                        None,
                    )
                    .await
                    .unwrap(),
                Some(serde_json::json!(expected))
            );
        }
    }
}
