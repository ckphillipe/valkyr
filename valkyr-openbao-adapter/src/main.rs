use chrono::Utc;
use std::{env, process, sync::Arc, time::Duration};
use tokio::{sync::watch, time};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;
use valkyr_client::{Client, ClientBuilder, ScheduleLease, StreamingClient};
use valkyr_core::{KeyPattern, NamespaceContext, NamespacePattern};
use valkyr_openbao_adapter::{
    AdapterConfig, CallbackBridge, LogFormat, LoggingConfig, OpenBaoClient, OpenBaoMapping,
    ProviderConfig, ValkyrEndpoint, fetch_provider_values,
};

fn usage() -> &'static str {
    "usage: valkyr-openbao-adapter --config <adapter.yml>"
}
fn config_path() -> Result<String, String> {
    let mut args = env::args().skip(1);
    match (args.next().as_deref(), args.next()) {
        (Some("--config"), Some(path)) => Ok(path),
        _ => Err(usage().into()),
    }
}
fn init_logging(config: &LoggingConfig) -> Result<(), String> {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(&config.level))
        .map_err(|e| e.to_string())?;
    match config.format {
        LogFormat::Pretty => tracing_subscriber::fmt().with_env_filter(filter).try_init(),
        LogFormat::Json => tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .try_init(),
    }
    .map_err(|e| e.to_string())
}
async fn connect(
    config: &AdapterConfig,
    id: Uuid,
    bridge: Arc<CallbackBridge>,
    configured_endpoint: &ValkyrEndpoint,
) -> Result<StreamingClient, String> {
    let client = if configured_endpoint.uses_tls() {
        match &configured_endpoint.tls_config {
            Some(config) => {
                StreamingClient::connect_tls_with_config(
                    configured_endpoint.address(),
                    config.clone(),
                    configured_endpoint.api_key.clone(),
                    id,
                    bridge,
                )
                .await
            }
            None => {
                StreamingClient::connect_tls(
                    configured_endpoint.address(),
                    configured_endpoint.api_key.clone(),
                    id,
                    bridge,
                )
                .await
            }
        }
    } else {
        StreamingClient::connect(
            configured_endpoint.address(),
            configured_endpoint.api_key.clone(),
            id,
            bridge,
        )
        .await
    }
    .map_err(|e| e.to_string())?
    .with_request_timeout(config.valkyr.request_timeout);
    for query in config.queries.values() {
        client
            .provide_with_options(
                NamespacePattern::new(&query.namespace_pattern).map_err(|e| e.to_string())?,
                KeyPattern::new(&query.key_pattern).map_err(|e| e.to_string())?,
                config
                    .valkyr
                    .provider_options(query)
                    .map_err(|error| error.to_string())?,
            )
            .await
            .map_err(|e| e.to_string())?;
    }
    for store in config.stores.values() {
        client
            .store(
                NamespacePattern::new(&store.namespace_pattern).map_err(|e| e.to_string())?,
                KeyPattern::new(&store.key_pattern).map_err(|e| e.to_string())?,
            )
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(client)
}
async fn reconnect(
    config: &AdapterConfig,
    id: Uuid,
    bridge: Arc<CallbackBridge>,
    configured_endpoint: &ValkyrEndpoint,
    mut shutdown: watch::Receiver<bool>,
) -> Reconnect {
    loop {
        if *shutdown.borrow() {
            return Reconnect::Shutdown;
        }
        for attempt in 1..=config.valkyr.max_retries {
            let result = tokio::select! {
                _ = shutdown.changed() => return Reconnect::Shutdown,
                result = connect(config, id, bridge.clone(), configured_endpoint) => result,
            };
            match result {
                Ok(client) if !*shutdown.borrow() => {
                    info!(endpoint = %configured_endpoint.url, "callback connection established");
                    return Reconnect::Connected(client);
                }
                Ok(_) => return Reconnect::Shutdown,
                Err(error) => {
                    warn!(attempt, %error, "callback connection attempt failed");
                    if !wait_for_retry(delay(attempt), &mut shutdown).await {
                        return Reconnect::Shutdown;
                    }
                }
            }
        }
    }
}

enum Reconnect {
    Connected(StreamingClient),
    Shutdown,
}

async fn wait_for_retry(delay: Duration, shutdown: &mut watch::Receiver<bool>) -> bool {
    if *shutdown.borrow() {
        return false;
    }
    tokio::select! {
        _ = shutdown.changed() => false,
        _ = time::sleep(delay) => true,
    }
}

fn delay(attempt: u32) -> Duration {
    Duration::from_millis(
        250_u64
            .saturating_mul(1_u64 << attempt.saturating_sub(1).min(16))
            .min(30_000),
    )
}

async fn sync_provider(
    client: &OpenBaoClient,
    mapping: &OpenBaoMapping,
    namespace: NamespaceContext,
    endpoints: &[ClientBuilder],
) -> Result<usize, String> {
    let mut owners = Vec::<Client>::new();
    for endpoint in endpoints {
        if let Ok(client) = endpoint.clone().connect().await {
            if matches!(
                client.acquire_schedule_lease(&namespace).await,
                Ok(ScheduleLease::Acquired { .. })
            ) {
                owners.push(client);
            }
        }
    }
    if owners.is_empty() {
        return Ok(0);
    }
    let result = async {
        let values = fetch_provider_values(client, mapping, namespace.clone())
            .await
            .map_err(|error| error.to_string())?;
        for value in &values {
            for owner in &owners {
                owner
                    .set(
                        value.namespace.clone(),
                        value.key.clone(),
                        value.value.clone(),
                        value.ttl,
                    )
                    .await
                    .map_err(|error| error.to_string())?;
            }
        }
        Ok(values.len())
    }
    .await;
    for owner in owners {
        let _ = owner.release_schedule_lease(&namespace).await;
    }
    result
}

async fn sync_configured_provider(
    name: &str,
    provider: &ProviderConfig,
    client: &OpenBaoClient,
    mapping: &OpenBaoMapping,
    endpoints: &[ClientBuilder],
) {
    let namespace = match NamespaceContext::new(&provider.namespace) {
        Ok(namespace) => namespace,
        Err(error) => {
            error!(provider = %name, %error, "provider namespace is invalid");
            return;
        }
    };
    match sync_provider(client, mapping, namespace, endpoints).await {
        Ok(count) => info!(provider = %name, count, "provider sync completed"),
        Err(error) => warn!(provider = %name, %error, "provider sync failed"),
    }
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
        eprintln!("could not initialize logging: {error}");
        process::exit(2);
    }
    let client = match config.openbao_client() {
        Ok(client) => client,
        Err(error) => {
            error!(%error, "could not create OpenBao client");
            process::exit(2);
        }
    };
    if let Err(error) = client.login().await {
        error!(%error, "could not authenticate to OpenBao");
        process::exit(1);
    }
    let (shutdown_sender, mut shutdown) = watch::channel(false);
    let signal_sender = shutdown_sender.clone();
    tokio::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                signal_sender.send_replace(true);
            }
            Err(error) => error!(%error, "shutdown signal failed"),
        }
    });
    let _shutdown_sender = shutdown_sender;
    let id = Uuid::new_v4();
    let forwarding_builders = config
        .valkyr
        .endpoints
        .iter()
        .map(|endpoint| endpoint.client_builder(id, config.valkyr.request_timeout))
        .collect::<Vec<_>>();
    let provider_mapping = match OpenBaoMapping::new(&config.openbao.prefix) {
        Ok(mapping) => mapping,
        Err(error) => {
            error!(%error, "could not configure OpenBao provider mapping");
            process::exit(2);
        }
    };
    let bridges = match (0..config.valkyr.endpoints.len())
        .map(|source_endpoint| {
            config.callback_bridge(client.clone()).map(|bridge| {
                Arc::new(bridge.with_forwarding(forwarding_builders.clone(), source_endpoint))
            })
        })
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(bridges) => bridges,
        Err(error) => {
            error!(%error, "could not create callback bridge");
            process::exit(2);
        }
    };
    let mut callbacks = Vec::new();
    for (index, configured_endpoint) in config.valkyr.endpoints.iter().enumerate() {
        match reconnect(
            &config,
            id,
            bridges[index].clone(),
            configured_endpoint,
            shutdown.clone(),
        )
        .await
        {
            Reconnect::Connected(client) => callbacks.push(client),
            Reconnect::Shutdown => {
                info!("OpenBao adapter shutting down");
                return;
            }
        }
    }
    for (name, provider) in config.providers.clone() {
        let client = client.clone();
        let mapping = provider_mapping.clone();
        let endpoints = forwarding_builders.clone();
        tokio::spawn(async move {
            let schedule = match provider.schedule() {
                Ok(schedule) => schedule,
                Err(error) => {
                    error!(provider = %name, %error, "provider schedule is invalid");
                    return;
                }
            };
            if provider.run_on_startup {
                sync_configured_provider(&name, &provider, &client, &mapping, &endpoints).await;
            }
            loop {
                let Some(next) = schedule.upcoming(Utc).next() else {
                    return;
                };
                time::sleep((next - Utc::now()).to_std().unwrap_or(Duration::ZERO)).await;
                sync_configured_provider(&name, &provider, &client, &mapping, &endpoints).await;
            }
        });
    }
    info!(adapter_id = %id, "Valkyr OpenBao adapter is connected");
    let mut monitor = time::interval(Duration::from_secs(1));
    let mut renew = time::interval(Duration::from_secs(
        config.openbao.auth.renew_before_seconds,
    ));
    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                info!("OpenBao adapter shutting down");
                return;
            }
            _ = monitor.tick() => {
                let mut callback_restored = false;
                for (index, callback) in callbacks.iter_mut().enumerate() {
                    if callback.is_closed() {
                        let configured_endpoint = &config.valkyr.endpoints[index];
                        warn!(endpoint = %configured_endpoint.url, "callback connection closed; restoring registrations");
                        match reconnect(&config, id, bridges[index].clone(), configured_endpoint, shutdown.clone()).await {
                            Reconnect::Connected(client) => {
                                *callback = client;
                                callback_restored = true;
                            }
                            Reconnect::Shutdown => {
                                info!("OpenBao adapter shutting down");
                                return;
                            }
                        }
                    }
                }
                if callback_restored {
                    info!("callback connection restored; syncing configured providers");
                    for (name, provider) in config.providers.clone() {
                        let client = client.clone();
                        let mapping = provider_mapping.clone();
                        let endpoints = forwarding_builders.clone();
                        tokio::spawn(async move {
                            sync_configured_provider(&name, &provider, &client, &mapping, &endpoints).await;
                        });
                    }
                }
            }
            _ = renew.tick() => if let Err(error) = client.renew().await {
                warn!(%error, "OpenBao token renewal failed; login will be retried");
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
    use valkyr_openbao_adapter::{OnMissing, QueryConfig};

    #[test]
    fn caps_reconnect_delay() {
        assert_eq!(delay(1), Duration::from_millis(250));
        assert_eq!(delay(2), Duration::from_millis(500));
        assert_eq!(delay(u32::MAX), Duration::from_secs(30));
    }

    #[tokio::test]
    async fn retry_wait_stops_when_shutdown_is_requested() {
        let (sender, shutdown) = watch::channel(false);
        let wait = tokio::spawn(async move {
            let mut shutdown = shutdown;
            wait_for_retry(Duration::from_secs(30), &mut shutdown).await
        });

        tokio::task::yield_now().await;
        sender.send_replace(true);

        assert!(!wait.await.unwrap());
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
            "openbao:\n  address: https://openbao.test:8200\n  kv_mount: kv\n  prefix: cache\n  auth:\n    type: approle\n    role_id: role\n    secret_id_file: unused-secret\nvalkyr:\n  endpoints:\n    - url: {}\n      api_key_file: unused-first\n    - url: {}\n      api_key_file: unused-second\n",
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
                provider_wait_timeout: Some(Duration::ZERO),
                miss_cache_ttl: None,
                on_missing: OnMissing::default(),
            },
        );
        let bridge = Arc::new(CallbackBridge::new(vec![], vec![]));

        let first = connect(
            &config,
            Uuid::new_v4(),
            bridge.clone(),
            &config.valkyr.endpoints[0],
        )
        .await
        .unwrap();
        drop(first);
        let restored = connect(
            &config,
            Uuid::new_v4(),
            bridge.clone(),
            &config.valkyr.endpoints[0],
        )
        .await
        .unwrap();
        drop(restored);
        connect(&config, Uuid::new_v4(), bridge, &config.valkyr.endpoints[1])
            .await
            .unwrap();

        for commands in [first_server.await.unwrap(), second_server.await.unwrap()] {
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
