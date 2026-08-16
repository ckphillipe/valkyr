use async_trait::async_trait;
use valkyr_client::{Client, ClientBuilder, ScheduleLease};
use valkyr_core::NamespaceContext;

use crate::{DatabaseValue, Result, ValuePublisher, ValueSource};

#[async_trait]
impl ValuePublisher for Client {
    async fn publish(&self, value: DatabaseValue) -> Result<()> {
        self.set(value.namespace, value.key, value.value, value.ttl)
            .await?;
        Ok(())
    }
}
/// A scheduled-provider publisher that reconnects on a failed write. It
/// serializes publish attempts to preserve source ordering across reconnects.
#[derive(Clone)]
pub struct ReconnectingPublisher {
    endpoints: std::sync::Arc<Vec<EndpointClient>>,
    serialization: std::sync::Arc<tokio::sync::Mutex<()>>,
}
struct EndpointClient {
    builder: ClientBuilder,
    client: tokio::sync::Mutex<Option<Client>>,
}
impl ReconnectingPublisher {
    pub fn new(builder: ClientBuilder) -> Self {
        Self::from_builders(vec![builder])
    }
    pub fn from_builders(builders: Vec<ClientBuilder>) -> Self {
        Self {
            endpoints: std::sync::Arc::new(
                builders
                    .into_iter()
                    .map(|builder| EndpointClient {
                        builder,
                        client: tokio::sync::Mutex::new(None),
                    })
                    .collect(),
            ),
            serialization: std::sync::Arc::new(tokio::sync::Mutex::new(())),
        }
    }
    async fn connection(endpoint: &EndpointClient) -> Result<Client> {
        let mut connection = endpoint.client.lock().await;
        if connection.is_none() {
            *connection = Some(endpoint.builder.clone().connect().await?);
        }
        Ok(connection.clone().expect("connection was initialized"))
    }
    async fn publish_endpoint(endpoint: &EndpointClient, value: &DatabaseValue) -> Result<()> {
        let send = |client: Client| async move {
            client
                .set(
                    value.namespace.clone(),
                    value.key.clone(),
                    value.value.clone(),
                    value.ttl,
                )
                .await
        };
        let first = send(Self::connection(endpoint).await?).await;
        match first {
            Ok(()) => Ok(()),
            Err(error) if !error.is_connection_failure() => Err(error.into()),
            Err(_) => {
                *endpoint.client.lock().await = None;
                send(Self::connection(endpoint).await?)
                    .await
                    .map_err(Into::into)
            }
        }
    }
    /// Forget the current connection so the next publish establishes a new
    /// authenticated channel. Useful to coordinate an external lifecycle.
    pub async fn invalidate(&self) {
        for endpoint in self.endpoints.iter() {
            *endpoint.client.lock().await = None;
        }
    }

    /// Acquire this adapter's server-local schedule lease at every reachable
    /// endpoint. Callers publish only through the returned clients.
    pub async fn acquire_schedule_leases(&self, namespace: &NamespaceContext) -> Vec<Client> {
        let mut owners = Vec::new();
        for endpoint in self.endpoints.iter() {
            let Ok(client) = Self::connection(endpoint).await else {
                continue;
            };
            match client.acquire_schedule_lease(namespace).await {
                Ok(ScheduleLease::Acquired { .. }) => owners.push(client),
                Ok(ScheduleLease::Unavailable) => {}
                Err(_) => *endpoint.client.lock().await = None,
            }
        }
        owners
    }

    pub async fn release_schedule_leases(&self, namespace: &NamespaceContext, owners: &[Client]) {
        for owner in owners {
            let _ = owner.release_schedule_lease(namespace).await;
        }
    }

    pub async fn publish_to_leased_endpoints(
        &self,
        owners: &[Client],
        value: DatabaseValue,
    ) -> Result<()> {
        let _serial = self.serialization.lock().await;
        for owner in owners {
            owner.publish(value.clone()).await?;
        }
        Ok(())
    }
}
#[async_trait]
impl ValuePublisher for ReconnectingPublisher {
    async fn publish(&self, value: DatabaseValue) -> Result<()> {
        let _serial = self.serialization.lock().await;
        let mut failures = Vec::new();
        for (index, endpoint) in self.endpoints.iter().enumerate() {
            if let Err(error) = Self::publish_endpoint(endpoint, &value).await {
                failures.push(format!("{index}: {error}"));
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(crate::AdapterError::Replication(failures.join(", ")))
        }
    }
}
/// Coordinates a complete database-to-Valkyr synchronization.
pub struct Adapter<S, P> {
    source: S,
    publisher: P,
}
impl<S, P> Adapter<S, P> {
    pub fn new(source: S, publisher: P) -> Self {
        Self { source, publisher }
    }
}
impl<S: ValueSource, P: ValuePublisher> Adapter<S, P> {
    /// Fetch and publish in source order. Sequential publishing preserves a
    /// deterministic final value when a query contains duplicate routes.
    pub async fn sync_once(&self) -> Result<usize> {
        let values = self.source.fetch_values().await?;
        let count = values.len();
        for value in values {
            self.publisher.publish(value).await?;
        }
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ValkyrEndpoint;
    use serde_json::json;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::TcpListener,
    };
    use valkyr_client::ClientBuilder;
    use valkyr_core::{
        Command, Key, NamespaceContext, Response,
        line_protocol::{decode_command, encode_response},
    };

    #[tokio::test]
    async fn invalidate_establishes_a_fresh_authenticated_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let authentications = Arc::new(AtomicUsize::new(0));
        let task = tokio::spawn({
            let authentications = authentications.clone();
            async move {
                for _ in 0..2 {
                    let (stream, _) = listener.accept().await.unwrap();
                    let (reader, mut writer) = stream.into_split();
                    let mut reader = BufReader::new(reader);
                    let mut line = String::new();
                    reader.read_line(&mut line).await.unwrap();
                    assert!(matches!(
                        decode_command(line.trim_end()).unwrap(),
                        Command::Auth { .. }
                    ));
                    authentications.fetch_add(1, Ordering::SeqCst);
                    writer
                        .write_all(format!("{}\n", "OK publisher TTL 60").as_bytes())
                        .await
                        .unwrap();
                    line.clear();
                    reader.read_line(&mut line).await.unwrap();
                    let command = decode_command(line.trim_end()).unwrap();
                    assert!(matches!(&command, Command::Set { .. }));
                    writer
                        .write_all(
                            format!("{}\n", encode_response(&command, &Response::Ok).unwrap())
                                .as_bytes(),
                        )
                        .await
                        .unwrap();
                }
            }
        });
        let publisher = ReconnectingPublisher::new(
            ClientBuilder::new()
                .server(address.to_string())
                .api_key("bootstrap"),
        );
        let value = |key| DatabaseValue {
            namespace: NamespaceContext::new("/people").unwrap(),
            key: Key::new(key).unwrap(),
            value: json!(key),
            ttl: None,
        };

        publisher.publish(value("first")).await.unwrap();
        publisher.invalidate().await;
        publisher.publish(value("second")).await.unwrap();
        task.await.unwrap();
        assert_eq!(authentications.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn publishes_to_each_endpoint_with_its_configured_api_key() {
        async fn serve(listener: TcpListener) -> String {
            let (stream, _) = listener.accept().await.unwrap();
            let (reader, mut writer) = stream.into_split();
            let mut reader = BufReader::new(reader);
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            let Command::Auth { api_key, .. } = decode_command(line.trim_end()).unwrap() else {
                panic!("publisher did not authenticate first");
            };
            writer
                .write_all(format!("{}\n", "OK publisher TTL 60").as_bytes())
                .await
                .unwrap();
            line.clear();
            reader.read_line(&mut line).await.unwrap();
            let command = decode_command(line.trim_end()).unwrap();
            assert!(matches!(&command, Command::Set { .. }));
            writer
                .write_all(
                    format!("{}\n", encode_response(&command, &Response::Ok).unwrap()).as_bytes(),
                )
                .await
                .unwrap();
            api_key
        }

        let first_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let first_address = first_listener.local_addr().unwrap();
        let second_address = second_listener.local_addr().unwrap();
        let first_server = tokio::spawn(serve(first_listener));
        let second_server = tokio::spawn(serve(second_listener));
        let endpoint = |address: std::net::SocketAddr, api_key: &str| ValkyrEndpoint {
            url: address.to_string(),
            api_key_file: std::path::PathBuf::new(),
            ca_certificate_file: None,
            api_key: api_key.into(),
            tls_config: None,
        };
        let publisher = ReconnectingPublisher::from_builders(vec![
            endpoint(first_address, "first-endpoint-key")
                .client_builder(uuid::Uuid::nil(), std::time::Duration::from_secs(1)),
            endpoint(second_address, "second-endpoint-key")
                .client_builder(uuid::Uuid::nil(), std::time::Duration::from_secs(1)),
        ]);

        publisher
            .publish(DatabaseValue {
                namespace: NamespaceContext::new("/people").unwrap(),
                key: Key::new("alice").unwrap(),
                value: json!("value"),
                ttl: None,
            })
            .await
            .unwrap();

        assert_eq!(first_server.await.unwrap(), "first-endpoint-key");
        assert_eq!(second_server.await.unwrap(), "second-endpoint-key");
    }
}
