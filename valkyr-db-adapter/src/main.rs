use chrono::Utc;
use std::{collections::BTreeMap, env, process, sync::Arc, time::Duration};
use tokio::time;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;
use valkyr_client::StreamingClient;
use valkyr_core::{KeyPattern, NamespaceContext, NamespacePattern};
use valkyr_db_adapter::{
    AdapterConfig, CallbackBridge, DatabaseSource, LogFormat, LoggingConfig, ReconnectingPublisher,
    ValkyrEndpoint, ValueSource,
};

fn usage() -> &'static str {
    "usage: valkyr-db-adapter --config <adapter.yml>"
}

fn config_path() -> Result<String, String> {
    let mut arguments = env::args().skip(1);
    match (arguments.next().as_deref(), arguments.next()) {
        (Some("--config"), Some(path)) => Ok(path),
        (Some("--help" | "-h"), _) => Err(usage().into()),
        _ => Err(usage().into()),
    }
}

fn pattern(value: &str, kind: &str) -> Result<NamespacePattern, String> {
    NamespacePattern::new(value).map_err(|error| format!("invalid {kind}: {error}"))
}

fn key_pattern(value: &str, kind: &str) -> Result<KeyPattern, String> {
    KeyPattern::new(value).map_err(|error| format!("invalid {kind}: {error}"))
}

fn init_logging(config: &LoggingConfig) -> Result<(), String> {
    let filter = match EnvFilter::try_from_default_env() {
        Ok(filter) => filter,
        Err(_) => EnvFilter::try_new(&config.level)
            .map_err(|error| format!("invalid logging.level '{}': {error}", config.level))?,
    };
    let result = match config.format {
        LogFormat::Pretty => tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(config.target)
            .with_thread_names(config.thread_names)
            .with_ansi(config.ansi)
            .try_init(),
        LogFormat::Json => tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .with_target(config.target)
            .with_thread_names(config.thread_names)
            .with_ansi(false)
            .try_init(),
    };
    result.map_err(|error| format!("could not initialize structured logging: {error}"))
}

async fn connect_and_register(
    config: &AdapterConfig,
    adapter_id: Uuid,
    bridge: Arc<CallbackBridge>,
    endpoint: &ValkyrEndpoint,
) -> Result<StreamingClient, String> {
    let client = if endpoint.uses_tls() {
        match &endpoint.tls_config {
            Some(config) => {
                StreamingClient::connect_tls_with_config(
                    endpoint.address(),
                    config.clone(),
                    endpoint.api_key.clone(),
                    adapter_id,
                    bridge,
                )
                .await
            }
            None => {
                StreamingClient::connect_tls(
                    endpoint.address(),
                    endpoint.api_key.clone(),
                    adapter_id,
                    bridge,
                )
                .await
            }
        }
    } else {
        StreamingClient::connect(
            endpoint.address(),
            endpoint.api_key.clone(),
            adapter_id,
            bridge,
        )
        .await
    }
    .map_err(|error| error.to_string())?;
    for (name, query) in &config.queries {
        client
            .provide_with_options(
                pattern(&query.namespace_pattern, "query namespace pattern")?,
                key_pattern(&query.key_pattern, "query key pattern")?,
                config
                    .valkyr
                    .provider_options(query)
                    .map_err(|error| error.to_string())?,
            )
            .await
            .map_err(|error| format!("could not register query '{name}': {error}"))?;
    }
    for (name, store) in &config.stores {
        client
            .store(
                pattern(&store.namespace_pattern, "store namespace pattern")?,
                key_pattern(&store.key_pattern, "store key pattern")?,
            )
            .await
            .map_err(|error| format!("could not register store '{name}': {error}"))?;
    }
    Ok(client)
}

async fn connect_with_backoff(
    config: &AdapterConfig,
    adapter_id: Uuid,
    bridge: Arc<CallbackBridge>,
    endpoint: &ValkyrEndpoint,
) -> Result<StreamingClient, String> {
    let mut last_error = String::new();
    for attempt in 1..=config.valkyr.max_retries {
        match connect_and_register(config, adapter_id, bridge.clone(), endpoint).await {
            Ok(client) => return Ok(client),
            Err(error) => last_error = error,
        }
        if attempt < config.valkyr.max_retries {
            let delay = reconnect_delay(attempt);
            warn!(attempt, error = %last_error, ?delay, "callback connection attempt failed; retrying");
            time::sleep(delay).await;
        }
    }
    Err(format!(
        "callback reconnection failed after {} attempts: {last_error}",
        config.valkyr.max_retries
    ))
}

async fn reconnect_until_connected(
    config: &AdapterConfig,
    adapter_id: Uuid,
    bridge: Arc<CallbackBridge>,
    endpoint: &ValkyrEndpoint,
) -> StreamingClient {
    loop {
        match connect_with_backoff(config, adapter_id, bridge.clone(), endpoint).await {
            Ok(client) => return client,
            Err(error) => {
                warn!(%error, "callback reconnection cycle failed; retrying");
                time::sleep(reconnect_delay(config.valkyr.max_retries)).await;
            }
        }
    }
}

fn reconnect_delay(attempt: u32) -> Duration {
    Duration::from_millis(
        250_u64
            .saturating_mul(1_u64 << attempt.saturating_sub(1).min(16))
            .min(30_000),
    )
}

async fn sync_scheduled_provider(
    source: &DatabaseSource,
    publisher: &ReconnectingPublisher,
) -> valkyr_db_adapter::Result<usize> {
    let values = source.fetch_values().await?;
    let count = values.len();
    let mut namespaces = BTreeMap::new();
    for value in values {
        namespaces
            .entry(value.namespace.as_str().to_owned())
            .or_insert_with(Vec::new)
            .push(value);
    }
    for (namespace, values) in namespaces {
        let namespace = NamespaceContext::new(namespace)?;
        let owners = publisher.acquire_schedule_leases(&namespace).await;
        let result: valkyr_db_adapter::Result<()> = async {
            for value in values {
                publisher
                    .publish_to_leased_endpoints(&owners, value)
                    .await?;
            }
            Ok(())
        }
        .await;
        publisher.release_schedule_leases(&namespace, &owners).await;
        result?;
    }
    Ok(count)
}

#[tokio::main]
async fn main() {
    let path = match config_path() {
        Ok(path) => path,
        Err(error) => {
            eprintln!("{error}");
            process::exit(2);
        }
    };
    let config = match AdapterConfig::from_file(path) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("could not load adapter configuration: {error}");
            process::exit(2);
        }
    };
    if let Err(error) = init_logging(&config.logging) {
        eprintln!("{error}");
        process::exit(2);
    }
    info!(
        database_url = %config.database.url,
        endpoints = ?config.valkyr.endpoints.iter().map(|endpoint| &endpoint.url).collect::<Vec<_>>(),
        log_format = ?config.logging.format,
        "starting Valkyr database adapter"
    );
    let database = match config.database_manager().await {
        Ok(database) => database,
        Err(error) => {
            error!(%error, "could not connect to configured database");
            process::exit(1);
        }
    };
    for statement in &config.init {
        if let Err(error) = database.execute_init(statement).await {
            error!(statement = %statement.name, %error, "could not run init statement");
            process::exit(1);
        }
    }
    let sources = config.database_sources(database.clone());
    let callback_builders = config
        .valkyr
        .endpoints
        .iter()
        .map(|endpoint| endpoint.client_builder(Uuid::nil(), config.valkyr.request_timeout))
        .collect::<Vec<_>>();
    let adapter_id = Uuid::new_v4();
    let callback_bridges = match (0..config.valkyr.endpoints.len())
        .map(|source_endpoint| {
            config
                .database_callback_bridge(database.clone())
                .map(|bridge| {
                    Arc::new(
                        bridge.with_forwarding(
                            callback_builders
                                .iter()
                                .cloned()
                                .map(|builder| builder.adapter_instance(adapter_id))
                                .collect(),
                            source_endpoint,
                        ),
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(bridges) => bridges,
        Err(error) => {
            error!(%error, "could not create callback handlers");
            process::exit(2);
        }
    };
    let mut callback_clients = Vec::new();
    for (index, endpoint) in config.valkyr.endpoints.iter().enumerate() {
        match connect_with_backoff(
            &config,
            adapter_id,
            callback_bridges[index].clone(),
            endpoint,
        )
        .await
        {
            Ok(client) => callback_clients.push(client),
            Err(error) => {
                error!(endpoint = %endpoint.url, %error, "could not establish callback channel to Valkyr");
                process::exit(1);
            }
        }
    }
    let publisher = ReconnectingPublisher::from_builders(
        callback_builders
            .into_iter()
            .map(|builder| builder.adapter_instance(adapter_id))
            .collect(),
    );
    for (name, source, provider) in sources {
        let publisher = publisher.clone();
        tokio::spawn(async move {
            let schedule = match provider.schedule() {
                Ok(schedule) => schedule,
                Err(error) => {
                    error!(provider = %name, %error, "provider has invalid schedule");
                    return;
                }
            };
            if provider.run_on_startup {
                if let Err(error) = sync_scheduled_provider(&source, &publisher).await {
                    warn!(provider = %name, %error, "provider startup sync failed");
                }
            }
            loop {
                let Some(next) = schedule.upcoming(Utc).next() else {
                    error!(provider = %name, "provider has no next scheduled run");
                    return;
                };
                let delay = (next - Utc::now()).to_std().unwrap_or(Duration::ZERO);
                time::sleep(delay).await;
                match sync_scheduled_provider(&source, &publisher).await {
                    Ok(count) => info!(provider = %name, count, "provider sync completed"),
                    Err(error) => warn!(provider = %name, %error, "provider sync failed"),
                }
            }
        });
    }
    info!(adapter_id = %adapter_id, "Valkyr database adapter is connected; press Ctrl-C to stop");
    let mut monitor = time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                if let Err(error) = result { error!(%error, "could not wait for shutdown"); process::exit(1); }
                info!("database adapter shutting down");
                return;
            }
            _ = monitor.tick() => {
                for (index, callback_client) in callback_clients.iter_mut().enumerate() {
                    if callback_client.is_closed() {
                        let endpoint = &config.valkyr.endpoints[index];
                        warn!(endpoint = %endpoint.url, "callback connection closed; restoring registrations");
                        *callback_client = reconnect_until_connected(&config, adapter_id, callback_bridges[index].clone(), endpoint).await;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::TcpListener,
    };
    use valkyr_core::{
        Command, Response,
        line_protocol::{decode_command, encode_response},
    };
    use valkyr_db_adapter::QueryConfig;

    #[test]
    fn caps_delay_between_failed_reconnect_cycles() {
        assert_eq!(reconnect_delay(1), Duration::from_millis(250));
        assert_eq!(reconnect_delay(2), Duration::from_millis(500));
        assert_eq!(reconnect_delay(u32::MAX), Duration::from_secs(30));
    }

    #[tokio::test]
    async fn callback_connections_use_their_endpoint_api_keys() {
        async fn serve(listener: TcpListener, connections: usize) -> Vec<Command> {
            let mut commands = Vec::new();
            for _ in 0..connections {
                let (stream, _) = listener.accept().await.unwrap();
                let (reader, mut writer) = stream.into_split();
                let mut reader = BufReader::new(reader);
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                assert!(matches!(
                    decode_command(line.trim_end()).unwrap(),
                    Command::Auth { .. }
                ));
                writer
                    .write_all(format!("{}\n", "OK callback TTL 60").as_bytes())
                    .await
                    .unwrap();
                line.clear();
                reader.read_line(&mut line).await.unwrap();
                let command = decode_command(line.trim_end()).unwrap();
                commands.push(command.clone());
                writer
                    .write_all(
                        format!("{}\n", encode_response(&command, &Response::Ok).unwrap())
                            .as_bytes(),
                    )
                    .await
                    .unwrap();
            }
            commands
        }

        let first_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let first_address = first_listener.local_addr().unwrap();
        let second_address = second_listener.local_addr().unwrap();
        let first_server = tokio::spawn(serve(first_listener, 2));
        let second_server = tokio::spawn(serve(second_listener, 1));
        let mut config: AdapterConfig = serde_yaml::from_str(&format!(
            "database: {{ url: sqlite://./state.db }}\nvalkyr:\n  endpoints:\n    - url: {}\n      api_key_file: unused-first\n    - url: {}\n      api_key_file: unused-second\n",
            first_address, second_address
        ))
        .unwrap();
        config.valkyr.endpoints[0].api_key = "first-callback-key".into();
        config.valkyr.endpoints[1].api_key = "second-callback-key".into();
        config.valkyr.provider_wait_timeout = Some(Duration::from_millis(700));
        config.valkyr.miss_cache_ttl = Some(Duration::from_secs(40));
        config.queries.insert(
            "people".into(),
            QueryConfig {
                namespace_pattern: "/people".into(),
                key_pattern: "*".into(),
                query: "SELECT 1".into(),
                parameters: Vec::new(),
                description: None,
                timeout_seconds: None,
                ttl_seconds: None,
                provider_wait_timeout: Some(Duration::ZERO),
                miss_cache_ttl: None,
            },
        );
        let bridge = Arc::new(CallbackBridge::new(vec![], vec![]));

        let first = connect_and_register(
            &config,
            Uuid::new_v4(),
            bridge.clone(),
            &config.valkyr.endpoints[0],
        )
        .await
        .unwrap();
        drop(first);
        let restored = connect_and_register(
            &config,
            Uuid::new_v4(),
            bridge.clone(),
            &config.valkyr.endpoints[0],
        )
        .await
        .unwrap();
        drop(restored);
        connect_and_register(&config, Uuid::new_v4(), bridge, &config.valkyr.endpoints[1])
            .await
            .unwrap();

        let first_commands = first_server.await.unwrap();
        let second_commands = second_server.await.unwrap();
        for commands in [first_commands, second_commands] {
            assert!(commands.iter().all(|command| matches!(
                command,
                Command::Provide {
                    timeout: Some(0),
                    miss_ttl: Some(40),
                    ..
                }
            )));
        }
    }
}
