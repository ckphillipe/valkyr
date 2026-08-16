mod config;

use std::{path::PathBuf, sync::Arc, time::Duration};

use tokio::sync::watch;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use valkyr_core::{AuthManager, Broker, MemoryStore, MemoryStoreConfig, StoreAuthenticator};
use valkyr_server::{Server, tls_config};

use crate::config::{AuthConfig, CacheConfig};

fn init_logging(filter: &str) -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_new(filter)?)
        .with_target(false)
        .try_init()
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    Ok(())
}

fn configured_server(
    config: &AuthConfig,
    cache_config: &CacheConfig,
) -> Result<Arc<Server>, Box<dyn std::error::Error>> {
    let bootstrap_key = config::bootstrap_api_key(config)?;
    let store = Arc::new(MemoryStore::with_config(MemoryStoreConfig {
        max_capacity: cache_config.max_capacity,
        time_to_idle: cache_config.time_to_idle_seconds.map(Duration::from_secs),
    }));
    let authenticator = Arc::new(StoreAuthenticator::new(store.clone()));
    let auth = AuthManager::with_bootstrap_admin(
        authenticator,
        Some(bootstrap_key),
        Duration::from_secs(config.session_ttl_seconds),
    );
    Ok(Arc::new(Server::with_broker(Broker::new(
        store,
        Some(Arc::new(auth)),
    ))))
}

fn config_path_from_args() -> Result<Option<PathBuf>, Box<dyn std::error::Error>> {
    let mut arguments = std::env::args_os().skip(1);
    let Some(argument) = arguments.next() else {
        return Ok(None);
    };
    if argument == "--help" {
        println!("Usage: valkyr-server [--config <path>]");
        std::process::exit(0);
    }
    if argument != "--config" {
        return Err(format!("unknown argument: {}", argument.to_string_lossy()).into());
    }
    let path = arguments.next().ok_or("--config requires a path")?;
    if arguments.next().is_some() {
        return Err("only one --config <path> argument is supported".into());
    }
    Ok(Some(PathBuf::from(path)))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config_path = config_path_from_args()?;
    let config = config::load_config(config_path.as_deref())?;
    init_logging(&config.log_filter)?;
    let server = configured_server(
        config
            .auth
            .as_ref()
            .expect("configuration validation requires authentication"),
        &config.cache,
    )?;
    let tls = config
        .tls
        .as_ref()
        .map(|settings| tls_config(&settings.certificate_file, &settings.private_key_file))
        .transpose()?;
    let running = server.clone().bind_shared(config.native_listen).await?;
    info!(address = %running.local_addr()?, "native TCP listener bound");
    let (shutdown_sender, shutdown_receiver) = watch::channel(());
    let http_shutdown = shutdown_receiver.clone();
    let http_server = server.clone();
    let http_address = config.http_listen;
    tokio::spawn(async move {
        if let Err(error) = http_server.serve_http(http_address, http_shutdown).await {
            error!(%error, address = %http_address, "HTTP listener stopped");
        }
    });
    let metrics_shutdown = shutdown_receiver.clone();
    let metrics_server = server.clone();
    let metrics_address = config.metrics_listen;
    tokio::spawn(async move {
        if let Err(error) = metrics_server
            .serve_metrics(metrics_address, metrics_shutdown)
            .await
        {
            error!(%error, address = %metrics_address, "metrics listener stopped");
        }
    });
    if let (Some(tls_settings), Some(tls)) = (config.tls, tls) {
        let running_tls = server.clone().bind_tls(tls_settings.listen, tls).await?;
        info!(address = %running_tls.local_addr()?, "TLS listener bound");
        let tls_shutdown = shutdown_receiver.clone();
        tokio::spawn(async move {
            if let Err(error) = running_tls.run(tls_shutdown).await {
                error!(%error, "TLS listener stopped");
            }
        });
    }
    tokio::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                info!("shutdown signal received");
                let _ = shutdown_sender.send(());
            }
            Err(error) => warn!(%error, "could not wait for shutdown signal"),
        }
    });
    running.run(shutdown_receiver).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::{
        fs,
        sync::atomic::{AtomicUsize, Ordering},
    };
    use valkyr_core::{Key, NamespaceContext};

    fn temporary_path(prefix: &str) -> PathBuf {
        static NEXT_PATH: AtomicUsize = AtomicUsize::new(0);
        std::env::temp_dir().join(format!(
            "valkyr-{prefix}-{}-{}",
            std::process::id(),
            NEXT_PATH.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[tokio::test]
    async fn configured_server_applies_parsed_idle_policy_to_shared_store() {
        let bootstrap_path = temporary_path("bootstrap");
        let config_path = temporary_path("config");
        fs::write(&bootstrap_path, "bootstrap-key\n").unwrap();
        fs::write(
            &config_path,
            format!(
                "auth:\n  bootstrap_api_key_file: {}\ncache:\n  time_to_idle_seconds: 1\n",
                bootstrap_path.display()
            ),
        )
        .unwrap();

        let config = config::load_config(Some(&config_path)).unwrap();
        let server = configured_server(config.auth.as_ref().unwrap(), &config.cache).unwrap();
        let store = server.broker().store().clone();
        let namespace = NamespaceContext::new("/configured").unwrap();
        let entry_key = Key::new("key").unwrap();
        store
            .set(namespace.clone(), entry_key.clone(), json!("value"), None)
            .await
            .unwrap();

        assert!(store.get(&namespace, &entry_key).await.unwrap().is_some());
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        assert!(store.get(&namespace, &entry_key).await.unwrap().is_none());

        fs::remove_file(config_path).unwrap();
        fs::remove_file(bootstrap_path).unwrap();
    }
}
