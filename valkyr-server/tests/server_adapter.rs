use std::{
    fs,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::watch,
    time::{self, Instant},
};
use uuid::Uuid;
use valkyr_client::{Client, ClientBuilder, ClientError, StreamingClient};
use valkyr_core::{
    AuthManager, Broker, Command, Key, KeyPattern, MemoryStore, NamespaceContext, NamespacePattern,
    Response, StoreAuthenticator,
};
use valkyr_db_adapter::{
    AdapterConfig, DatabaseManager, DatabaseQueryProvider, InitConfig, QueryConfig, QueryProvider,
};
use valkyr_server::Server;

const BOOTSTRAP_KEY: &str = "integration-bootstrap-key";
const CONSUMER_KEY: &str = "integration-consumer-key";
const READ_ONLY_KEY: &str = "integration-read-only-key";
const DEADLINE: Duration = Duration::from_secs(5);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let name = format!(
            "valkyr-server-adapter-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after Unix epoch")
                .as_nanos()
        );
        let path = std::env::temp_dir().join(name);
        fs::create_dir(&path).expect("create test directory");
        Self(path)
    }

    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Fixture {
    native: SocketAddr,
    replica_native: SocketAddr,
    http: SocketAddr,
    metrics: SocketAddr,
    _adapters: Vec<StreamingClient>,
    native_shutdown: watch::Sender<()>,
    http_shutdown: watch::Sender<()>,
    metrics_shutdown: watch::Sender<()>,
    native_task: tokio::task::JoinHandle<std::io::Result<()>>,
    replica_native_shutdown: watch::Sender<()>,
    replica_native_task: tokio::task::JoinHandle<std::io::Result<()>>,
    http_task: tokio::task::JoinHandle<std::io::Result<()>>,
    metrics_task: tokio::task::JoinHandle<std::io::Result<()>>,
    database: DatabaseManager,
    _directory: TestDirectory,
}

impl Fixture {
    async fn start() -> Self {
        let directory = TestDirectory::new();
        let server = new_server();

        let (native_shutdown, native_receiver) = watch::channel(());
        let running = server
            .clone()
            .bind_shared("127.0.0.1:0")
            .await
            .expect("bind native listener");
        let native = running.local_addr().expect("read native listener address");
        let native_task = tokio::spawn(running.run(native_receiver));

        let replica_server = new_server();
        let (replica_native_shutdown, replica_native_receiver) = watch::channel(());
        let replica_running = replica_server
            .bind_shared("127.0.0.1:0")
            .await
            .expect("bind replica native listener");
        let replica_native = replica_running
            .local_addr()
            .expect("read replica native listener address");
        let replica_native_task = tokio::spawn(replica_running.run(replica_native_receiver));

        let http = unused_loopback_address();
        let (http_shutdown, http_receiver) = watch::channel(());
        let http_task = tokio::spawn(server.clone().serve_http(http, http_receiver));

        let metrics = unused_loopback_address();
        let (metrics_shutdown, metrics_receiver) = watch::channel(());
        let metrics_task = tokio::spawn(server.clone().serve_metrics(metrics, metrics_receiver));

        wait_for_tcp(http).await;
        wait_for_tcp(metrics).await;

        let config = write_adapter_config(&directory, &[native, replica_native]);
        let database = config
            .database_manager()
            .await
            .expect("open SQLite database");
        for statement in &config.init {
            database
                .execute_init(statement)
                .await
                .expect("initialize SQLite database");
        }

        let adapter_id = Uuid::new_v4();
        let endpoint_builders = [native, replica_native]
            .into_iter()
            .map(|endpoint| {
                ClientBuilder::new()
                    .server(endpoint.to_string())
                    .api_key(BOOTSTRAP_KEY)
                    .adapter_instance(adapter_id)
                    .connection_timeout(Duration::from_secs(1))
                    .request_timeout(Duration::from_secs(1))
            })
            .collect::<Vec<_>>();
        let mut adapters = Vec::new();
        for (source_endpoint, endpoint) in [native, replica_native].into_iter().enumerate() {
            let bridge = Arc::new(
                config
                    .database_callback_bridge(database.clone())
                    .expect("create database callback bridge")
                    .with_forwarding(endpoint_builders.clone(), source_endpoint),
            );
            let adapter = StreamingClient::connect(endpoint, BOOTSTRAP_KEY, adapter_id, bridge)
                .await
                .expect("connect adapter callback channel");
            register_routes(&adapter, &config).await;
            adapters.push(adapter);
        }

        Self {
            native,
            replica_native,
            http,
            metrics,
            _adapters: adapters,
            native_shutdown,
            http_shutdown,
            metrics_shutdown,
            native_task,
            replica_native_shutdown,
            replica_native_task,
            http_task,
            metrics_task,
            database,
            _directory: directory,
        }
    }

    async fn shutdown(self) {
        let _ = self.native_shutdown.send(());
        let _ = self.replica_native_shutdown.send(());
        let _ = self.http_shutdown.send(());
        let _ = self.metrics_shutdown.send(());
        self.native_task
            .await
            .expect("join native listener")
            .expect("stop native listener");
        self.replica_native_task
            .await
            .expect("join replica native listener")
            .expect("stop replica native listener");
        self.http_task
            .await
            .expect("join HTTP listener")
            .expect("stop HTTP listener");
        self.metrics_task
            .await
            .expect("join metrics listener")
            .expect("stop metrics listener");
    }

    async fn stored_value(&self, context: &str, key: &str) -> Option<Value> {
        DatabaseQueryProvider::new(
            self.database.clone(),
            QueryConfig {
                namespace_pattern: format!("/example::{context}"),
                key_pattern: key.into(),
                query: "SELECT state_value AS value FROM example_state WHERE context = ? AND state_key = ?".into(),
                parameters: vec!["context".into(), "key".into()],
                description: None,
                timeout_seconds: Some(5),
                ttl_seconds: None,
                provider_wait_timeout: None,
                miss_cache_ttl: None,
            },
        )
        .expect("build durable-state query")
        .query(
            NamespaceContext::new(format!("/example::{context}")).expect("valid state namespace"),
            Key::new(key).expect("valid state key"),
        )
        .await
        .expect("query durable state")
        .map(|value| value.value)
    }
}

fn new_server() -> Arc<Server> {
    let store = Arc::new(MemoryStore::new());
    let auth = AuthManager::with_bootstrap_admin(
        Arc::new(StoreAuthenticator::new(store.clone())),
        Some(BOOTSTRAP_KEY.into()),
        Duration::from_secs(60),
    );
    Arc::new(Server::with_broker(Broker::new(
        store,
        Some(Arc::new(auth)),
    )))
}

async fn register_routes(adapter: &StreamingClient, config: &AdapterConfig) {
    for query in config.queries.values() {
        adapter
            .provide_with_options(
                NamespacePattern::new(&query.namespace_pattern)
                    .expect("valid query namespace pattern"),
                KeyPattern::new(&query.key_pattern).expect("valid query key pattern"),
                config
                    .valkyr
                    .provider_options(query)
                    .expect("valid provider duration configuration"),
            )
            .await
            .expect("register query provider");
    }
    for store in config.stores.values() {
        adapter
            .store(
                NamespacePattern::new(&store.namespace_pattern)
                    .expect("valid store namespace pattern"),
                KeyPattern::new(&store.key_pattern).expect("valid store key pattern"),
            )
            .await
            .expect("register storage writer");
    }
}

fn write_adapter_config(directory: &TestDirectory, endpoints: &[SocketAddr]) -> AdapterConfig {
    let bootstrap_file = directory.path("bootstrap-api-key");
    fs::write(&bootstrap_file, BOOTSTRAP_KEY).expect("write bootstrap key");
    let database_file = directory.path("state.db");
    let config_file = directory.path("adapter.yml");
    let database_url = if cfg!(windows) {
        format!(
            "sqlite:///{}?mode=rwc",
            database_file.display().to_string().replace('\\', "/")
        )
    } else {
        format!("sqlite://{}?mode=rwc", database_file.display())
    }
    .replace('\'', "''");
    let config = format!(
        r#"
database:
  url: '{database_url}'
  max_connections: 1
  connection_timeout_seconds: 5
  query_timeout_seconds: 5
valkyr:
  endpoints: [{}]
  request_timeout: "5s"
  max_retries: 1
logging:
  level: warn
  format: pretty
  target: false
  thread_names: false
  ansi: false
init:
  - name: create_auth_registry
    sql: |
      CREATE TABLE auth_registry (api_key TEXT PRIMARY KEY, auth_value TEXT NOT NULL);
      INSERT INTO auth_registry VALUES ('{CONSUMER_KEY}', '{{"client_id":"consumer","name":"integration consumer","permissions":[{{"namespace":"/example","operations":["read","write","read_encrypted","write_encrypted"]}}]}}');
      INSERT INTO auth_registry VALUES ('{READ_ONLY_KEY}', '{{"client_id":"reader","name":"integration reader","permissions":[{{"namespace":"/example","operations":["read"]}}]}}');
  - name: create_security_keys
    sql: |
      CREATE TABLE security_keys (base_scope TEXT PRIMARY KEY, key_bytes BLOB NOT NULL, created INTEGER NOT NULL);
  - name: create_example_state
    sql: |
      CREATE TABLE example_state (context TEXT NOT NULL, state_key TEXT NOT NULL, state_value TEXT NOT NULL, PRIMARY KEY (context, state_key));
      INSERT INTO example_state VALUES ('source', 'db-only', '{{"name":"Ada"}}');
queries:
  auth_lookup:
    namespace_pattern: /__auth
    key_pattern: "{{api_key}}"
    query: SELECT auth_value AS value FROM auth_registry WHERE api_key = ?
    parameters: [key]
    timeout_seconds: 5
  security_lookup:
    namespace_pattern: /__secrets
    key_pattern: "{{base_scope}}"
    query: |
      INSERT INTO security_keys (base_scope, key_bytes, created)
      VALUES (?, randomblob(32), CAST(strftime('%s', 'now') AS INTEGER))
      ON CONFLICT(base_scope) DO UPDATE SET base_scope = excluded.base_scope
      RETURNING '{{"key":"' || lower(hex(key_bytes)) || '","created":' || created || '}}' AS value
    parameters: [key]
    timeout_seconds: 5
  state_lookup:
    namespace_pattern: /example
    key_pattern: "{{key}}"
    query: SELECT state_value AS value FROM example_state WHERE context = ? AND state_key = ?
    parameters: [context, key]
    timeout_seconds: 5
    provider_wait_timeout: "500ms"
    miss_cache_ttl: "30s"
stores:
  state_writer:
    namespace_pattern: /example
    key_pattern: "{{key}}"
    set_query: |
      INSERT INTO example_state (context, state_key, state_value) VALUES (?, ?, ?)
      ON CONFLICT(context, state_key) DO UPDATE SET state_value = excluded.state_value
    set_parameters: [context, key, value]
    delete_query: DELETE FROM example_state WHERE context = ? AND state_key = ?
    delete_parameters: [context, key_pattern]
    move_ns_query: UPDATE example_state SET context = ? WHERE context = ?
    move_ns_parameters: [destination_context, source_context]
    timeout_seconds: 5
"#,
        endpoints
            .iter()
            .map(|endpoint| {
                format!("{{ url: tcp://{endpoint}, api_key_file: bootstrap-api-key }}")
            })
            .collect::<Vec<_>>()
            .join(", ")
    );
    fs::write(&config_file, config).expect("write adapter configuration");
    AdapterConfig::from_file(config_file).expect("load adapter configuration")
}

fn unused_loopback_address() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve loopback port");
    listener.local_addr().expect("read reserved address")
}

async fn wait_for_tcp(address: SocketAddr) {
    let deadline = Instant::now() + DEADLINE;
    loop {
        if TcpStream::connect(address).await.is_ok() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "listener {address} did not become ready"
        );
        time::sleep(Duration::from_millis(10)).await;
    }
}

async fn authenticate_after_warmup(client: &Client, key: &str) {
    let deadline = Instant::now() + DEADLINE;
    loop {
        match client.authenticate(key, None).await {
            Ok(()) => return,
            Err(ClientError::AuthenticationPending { .. }) if Instant::now() < deadline => {
                time::sleep(Duration::from_millis(10)).await;
            }
            result => panic!("authentication did not warm up: {result:?}"),
        }
    }
}

async fn get_after_refresh(client: &Client, namespace: NamespaceContext, key: Key) -> Value {
    let deadline = Instant::now() + DEADLINE;
    loop {
        match client
            .request(Command::Get {
                namespace: namespace.clone(),
                key: key.clone(),
            })
            .await
        {
            Ok(Response::Value { value, .. }) => return value,
            Ok(Response::Miss { .. }) if Instant::now() < deadline => {
                time::sleep(Duration::from_millis(10)).await;
            }
            result => panic!("provider refresh did not return a value: {result:?}"),
        }
    }
}

async fn get_after_replication(client: &Client, namespace: NamespaceContext, key: Key) -> Value {
    let deadline = Instant::now() + DEADLINE;
    loop {
        match client
            .request(Command::Get {
                namespace: namespace.clone(),
                key: key.clone(),
            })
            .await
        {
            Ok(Response::Value { value, .. }) => return value,
            Ok(Response::Unknown) if Instant::now() < deadline => {
                time::sleep(Duration::from_millis(10)).await;
            }
            result => panic!("replication did not reach the replica cache: {result:?}"),
        }
    }
}

async fn http_get(address: SocketAddr, path: &str, key: &str) -> String {
    let mut connection = TcpStream::connect(address)
        .await
        .expect("connect HTTP listener");
    connection
        .write_all(
            format!(
                "GET {path} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {key}\r\nConnection: close\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .expect("write HTTP request");
    let mut response = String::new();
    connection
        .read_to_string(&mut response)
        .await
        .expect("read HTTP response");
    response
}

pub async fn run_benchmark(iterations: u64) {
    let fixture = Fixture::start().await;
    let client = Client::connect(fixture.native)
        .await
        .expect("connect benchmark client");
    authenticate_after_warmup(&client, CONSUMER_KEY).await;

    let write_namespace = NamespaceContext::new("/example::benchmark-write")
        .expect("valid benchmark write namespace");
    let write_started = std::time::Instant::now();
    for index in 0..iterations {
        client
            .set(
                write_namespace.clone(),
                benchmark_key(index),
                json!(index),
                None,
            )
            .await
            .expect("persist benchmark value");
    }
    report_benchmark("write-through writes", iterations, write_started.elapsed());

    let cached_read_started = std::time::Instant::now();
    for index in 0..iterations {
        assert_eq!(
            client
                .get(write_namespace.clone(), benchmark_key(index))
                .await
                .expect("read cached benchmark value"),
            json!(index)
        );
    }
    report_benchmark("cached reads", iterations, cached_read_started.elapsed());

    let miss_namespace =
        NamespaceContext::new("/example::benchmark-miss").expect("valid benchmark miss namespace");
    seed_cache_misses(&fixture, iterations).await;
    let miss_started = std::time::Instant::now();
    for index in 0..iterations {
        assert!(matches!(
            client
                .request(Command::Get {
                    namespace: miss_namespace.clone(),
                    key: benchmark_key(index),
                })
                .await,
            Ok(Response::Miss { .. }) | Ok(Response::Value { .. })
        ));
    }
    report_benchmark("cold read misses", iterations, miss_started.elapsed());

    for index in 0..iterations {
        assert_eq!(
            get_after_refresh(&client, miss_namespace.clone(), benchmark_key(index)).await,
            json!(index)
        );
    }
    fixture.shutdown().await;
}

fn benchmark_key(index: u64) -> Key {
    Key::new(format!("entry-{index}")).expect("valid benchmark key")
}

async fn seed_cache_misses(fixture: &Fixture, iterations: u64) {
    let values = (0..iterations)
        .map(|index| format!("('benchmark-miss', 'entry-{index}', '{index}')"))
        .collect::<Vec<_>>()
        .join(", ");
    fixture
        .database
        .execute_init(&InitConfig {
            name: "seed benchmark cache misses".into(),
            sql: format!(
                "INSERT INTO example_state (context, state_key, state_value) VALUES {values}"
            ),
            timeout_seconds: 5,
        })
        .await
        .expect("seed benchmark cache misses");
}

fn report_benchmark(name: &str, operations: u64, elapsed: Duration) {
    println!(
        "server_adapter {name}: {operations} operations in {:.3}s ({:.1} ops/s, {:.3} ms/op)",
        elapsed.as_secs_f64(),
        operations as f64 / elapsed.as_secs_f64(),
        elapsed.as_secs_f64() * 1_000.0 / operations as f64
    );
}

#[tokio::test]
async fn configured_database_adapter_serves_auth_storage_encryption_and_http() {
    let fixture = Fixture::start().await;
    let consumer = Client::connect(fixture.native)
        .await
        .expect("connect consumer");

    assert!(matches!(
        consumer.authenticate(CONSUMER_KEY, None).await,
        Err(ClientError::AuthenticationPending { .. })
    ));
    authenticate_after_warmup(&consumer, CONSUMER_KEY).await;

    let source = NamespaceContext::new("/example::source").unwrap();
    let rest_value = http_get(fixture.http, "/example::source?db-only", CONSUMER_KEY).await;
    assert!(
        rest_value.starts_with("HTTP/1.1 200") && rest_value.contains("Ada"),
        "REST provider wait did not return the value: {rest_value}"
    );
    assert_eq!(
        get_after_refresh(&consumer, source.clone(), Key::new("db-only").unwrap()).await,
        json!({"name": "Ada"})
    );

    consumer
        .set(
            source.clone(),
            Key::new("persist").unwrap(),
            json!({"version": 1}),
            None,
        )
        .await
        .expect("persist initial value");
    consumer
        .set(
            source.clone(),
            Key::new("persist").unwrap(),
            json!({"version": 2}),
            None,
        )
        .await
        .expect("overwrite durable value");
    assert_eq!(
        fixture.stored_value("source", "persist").await,
        Some(json!({"version": 2}))
    );

    let replica = Client::connect(fixture.replica_native)
        .await
        .expect("connect replica client");
    replica
        .authenticate(BOOTSTRAP_KEY, None)
        .await
        .expect("authenticate replica admin");
    assert_eq!(
        get_after_replication(&replica, source.clone(), Key::new("persist").unwrap()).await,
        json!({"version": 2})
    );

    let destination = NamespaceContext::new("/example::destination").unwrap();
    consumer
        .move_namespace(source.clone(), destination.clone())
        .await
        .expect("move context state");
    assert_eq!(fixture.stored_value("source", "persist").await, None);
    assert_eq!(
        fixture.stored_value("destination", "persist").await,
        Some(json!({"version": 2}))
    );
    consumer
        .delete(
            destination.clone(),
            Some(KeyPattern::new("persist").unwrap()),
        )
        .await
        .expect("delete durable value");
    assert_eq!(fixture.stored_value("destination", "persist").await, None);

    let encrypted_key = Key::new("~secret~").unwrap();
    consumer
        .set(
            destination.clone(),
            encrypted_key.clone(),
            json!("do not persist plaintext"),
            None,
        )
        .await
        .expect("write encrypted value");
    assert_eq!(
        consumer
            .get(destination.clone(), encrypted_key.clone())
            .await
            .expect("read encrypted value"),
        json!("do not persist plaintext")
    );
    let ciphertext = fixture
        .stored_value("destination", "secret")
        .await
        .expect("encrypted value is durable");
    assert_ne!(ciphertext, json!("do not persist plaintext"));

    let final_namespace = NamespaceContext::new("/example::final").unwrap();
    consumer
        .move_namespace(destination, final_namespace.clone())
        .await
        .expect("move encrypted context state");
    assert_eq!(
        consumer
            .get(final_namespace, encrypted_key)
            .await
            .expect("read moved encrypted value"),
        json!("do not persist plaintext")
    );

    let read_only = Client::connect(fixture.native)
        .await
        .expect("connect read-only client");
    authenticate_after_warmup(&read_only, READ_ONLY_KEY).await;
    assert!(
        read_only
            .set(
                NamespaceContext::new("/example::final").unwrap(),
                Key::new("denied").unwrap(),
                json!(true),
                None
            )
            .await
            .is_err()
    );

    let http_response = http_get(fixture.http, "/example::final?%7Esecret%7E", CONSUMER_KEY).await;
    assert!(
        http_response.starts_with("HTTP/1.1 200"),
        "unexpected HTTP response: {http_response}"
    );
    assert!(http_response.contains("do not persist plaintext"));

    let metrics = http_get(fixture.metrics, "/metrics", "ignored").await;
    assert!(
        metrics.starts_with("HTTP/1.1 200"),
        "unexpected metrics response: {metrics}"
    );
    assert!(metrics.contains("valkyr_requests_total"));
    assert!(metrics.contains("valkyr_cache_misses_total"));

    fixture.shutdown().await;
}
