//! Async client for Valkyr's native human-readable text protocol.

use async_trait::async_trait;
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use serde_json::Value;
use std::{collections::VecDeque, io::Cursor, sync::Arc, time::Duration};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    sync::{Mutex, mpsc},
};
use tokio_rustls::{
    TlsConnector,
    rustls::{ClientConfig as RustlsClientConfig, RootCertStore, pki_types::ServerName},
};
use tokio_util::codec::{Framed, LinesCodec};
use tracing::{debug, warn};
use uuid::Uuid;
use valkyr_core::{
    Command, Key, KeyPattern, LEASE_NAMESPACE, NamespaceContext, NamespacePattern, ProvideOptions,
    Response, ServerCommand, ServerResult, SetEntry, Stats,
    line_protocol::{decode_response, decode_server_command, encode_command, encode_server_result},
};

#[cfg(feature = "capi")]
mod capi;

trait ConnectionIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> ConnectionIo for T {}
type Connection = Framed<Box<dyn ConnectionIo>, LinesCodec>;
type Sink = SplitSink<Connection, String>;

pub type TlsClientConfig = Arc<RustlsClientConfig>;

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("connection failed: {0}")]
    Connection(#[from] std::io::Error),
    #[error("TLS handshake failed: {0}")]
    Tls(String),
    #[error("protocol frame failed: {0}")]
    Frame(String),
    #[error("invalid server response: {0}")]
    Protocol(String),
    #[error("server closed the connection")]
    Closed,
    /// The request may be retried only after establishing a fresh connection.
    #[error("server did not respond before the request timeout")]
    RequestTimeout,
    #[error("server error: {0}")]
    Server(String),
    #[error("unexpected server response: {0}")]
    UnexpectedResponse(&'static str),
    #[error("invalid client configuration: {0}")]
    Configuration(String),
    #[error("all configured endpoints failed")]
    NoHealthyEndpoints,
    #[error("server authentication failed: {0}")]
    Authentication(String),
    #[error("authentication is warming; retry after {retry_after_ms}ms")]
    AuthenticationPending { retry_after_ms: u64 },
}

impl ClientError {
    /// Whether retrying on a new connection can plausibly resolve this error.
    pub fn is_connection_failure(&self) -> bool {
        matches!(
            self,
            Self::Connection(_) | Self::Frame(_) | Self::Closed | Self::RequestTimeout
        )
    }
    pub fn is_retryable(&self) -> bool {
        self.is_connection_failure() || matches!(self, Self::AuthenticationPending { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use tokio::{net::TcpListener, sync::mpsc, time::sleep};
    use tokio_rustls::{
        TlsAcceptor,
        rustls::{ServerConfig, pki_types::PrivateKeyDer},
    };
    use tokio_util::codec::{Framed, LinesCodec};
    use valkyr_core::line_protocol::{decode_command, encode_response};

    #[test]
    fn pending_authentication_is_retryable() {
        assert!(ClientError::AuthenticationPending { retry_after_ms: 10 }.is_retryable());
    }

    #[test]
    fn verified_tls_config_accepts_pem_ca_certificates() {
        let certificate = include_bytes!("../../example/tls/localhost.crt");
        assert!(verified_tls_config(Some(certificate)).is_ok());
    }

    #[test]
    fn verified_tls_config_rejects_malformed_pem() {
        assert!(matches!(
            verified_tls_config(Some(b"not a certificate")),
            Err(ClientError::Configuration(message)) if message.contains("CA certificate")
        ));
    }

    #[test]
    fn provider_options_encode_for_both_client_surfaces() {
        let command = Command::Provide {
            namespace_pattern: NamespacePattern::new("/values").unwrap(),
            key_pattern: KeyPattern::new("*").unwrap(),
            max_rate: Some(2),
            timeout: Some(250),
            miss_ttl: Some(30),
        };
        assert_eq!(
            serde_json::to_value(command).unwrap(),
            serde_json::json!({
                "type": "provide",
                "namespace_pattern": "/values",
                "key_pattern": "*",
                "max_rate": 2,
                "timeout": 250,
                "miss_ttl": 30,
            })
        );
    }

    #[tokio::test]
    async fn client_timeout_poisoning_is_propagated_by_builder() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut framed = Framed::new(stream, LinesCodec::new());
            framed.next().await.unwrap().unwrap();
            framed.send("OK test TTL 60".to_owned()).await.unwrap();
            sleep(Duration::from_secs(1)).await;
        });

        let client = ClientBuilder::new()
            .server(address.to_string())
            .api_key("test")
            .request_timeout(Duration::from_millis(20))
            .connect()
            .await
            .unwrap();

        assert!(matches!(
            client.ping().await,
            Err(ClientError::RequestTimeout)
        ));
        assert!(matches!(client.ping().await, Err(ClientError::Closed)));
        server.abort();
    }

    #[tokio::test]
    async fn invalid_ordered_answer_poisons_ordinary_client() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut framed = Framed::new(stream, LinesCodec::new());
            assert_eq!(framed.next().await.unwrap().unwrap(), "PING");
            framed.send("OK".to_owned()).await.unwrap();
        });

        let client = Client::connect(address).await.unwrap();
        assert!(matches!(
            client.request(Command::Ping).await,
            Err(ClientError::Protocol(_))
        ));
        assert!(matches!(
            client.request(Command::Ping).await,
            Err(ClientError::Closed)
        ));

        server.await.unwrap();
    }

    #[tokio::test]
    async fn public_registration_calls_emit_options_on_ordinary_and_streaming_clients() {
        async fn serve(listener: TcpListener, sender: mpsc::UnboundedSender<Command>) {
            let (stream, _) = listener.accept().await.unwrap();
            let mut framed = Framed::new(stream, LinesCodec::new());
            while let Some(Ok(line)) = framed.next().await {
                let command = decode_command(&line).unwrap();
                let response = match &command {
                    Command::Auth { .. } => Response::AuthSuccess {
                        client_id: "test".into(),
                        session_ttl_seconds: 60,
                    },
                    _ => Response::Ok,
                };
                sender.send(command.clone()).unwrap();
                framed
                    .send(encode_response(&command, &response).unwrap())
                    .await
                    .unwrap();
            }
        }

        async fn exercise_ordinary(address: std::net::SocketAddr) {
            let client = Client::connect(address).await.unwrap();
            client
                .provide(
                    NamespacePattern::new("/values").unwrap(),
                    KeyPattern::new("legacy").unwrap(),
                    None,
                )
                .await
                .unwrap();
            client
                .provide_with_options(
                    NamespacePattern::new("/values").unwrap(),
                    KeyPattern::new("zero").unwrap(),
                    ProvideOptions::new(),
                )
                .await
                .unwrap();
            client
                .provide_with_options(
                    NamespacePattern::new("/values").unwrap(),
                    KeyPattern::new("positive").unwrap(),
                    ProvideOptions::new()
                        .with_timeout_ms(250)
                        .with_miss_ttl_seconds(30),
                )
                .await
                .unwrap();
        }

        async fn exercise_streaming(address: std::net::SocketAddr) {
            let client =
                StreamingClient::connect(address, "test", Uuid::new_v4(), Arc::new(NoopHandler))
                    .await
                    .unwrap();
            client
                .provide(
                    NamespacePattern::new("/values").unwrap(),
                    KeyPattern::new("legacy").unwrap(),
                    None,
                )
                .await
                .unwrap();
            client
                .provide_with_options(
                    NamespacePattern::new("/values").unwrap(),
                    KeyPattern::new("zero").unwrap(),
                    ProvideOptions::new(),
                )
                .await
                .unwrap();
            client
                .provide_with_options(
                    NamespacePattern::new("/values").unwrap(),
                    KeyPattern::new("positive").unwrap(),
                    ProvideOptions::new()
                        .with_timeout_ms(250)
                        .with_miss_ttl_seconds(30),
                )
                .await
                .unwrap();
            client._reader.abort();
            client.outbound.lock().await.take();
        }

        async fn assert_registration(streaming: bool) {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (sender, mut received) = mpsc::unbounded_channel();
            let server = tokio::spawn(serve(listener, sender));
            if streaming {
                exercise_streaming(address).await;
            } else {
                exercise_ordinary(address).await;
            }
            server.await.unwrap();
            let mut provides = Vec::new();
            while let Ok(command) = received.try_recv() {
                if let Command::Provide {
                    max_rate,
                    timeout,
                    miss_ttl,
                    ..
                } = command
                {
                    provides.push((max_rate, timeout, miss_ttl));
                }
            }
            assert_eq!(
                provides,
                vec![
                    (None, None, None),
                    (None, Some(0), Some(0)),
                    (None, Some(250), Some(30))
                ]
            );
        }

        assert_registration(false).await;
        assert_registration(true).await;
    }

    struct NoopHandler;

    #[async_trait]
    impl ServerCommandHandler for NoopHandler {
        async fn handle(&self, _: ServerCommand) -> ServerResult {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn streaming_client_timeout_closes_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut framed = Framed::new(stream, LinesCodec::new());
            framed.next().await.unwrap().unwrap();
            framed.send("OK test TTL 60".to_owned()).await.unwrap();
            sleep(Duration::from_secs(1)).await;
        });

        let client =
            StreamingClient::connect(address, "test", Uuid::new_v4(), Arc::new(NoopHandler))
                .await
                .unwrap()
                .with_request_timeout(Duration::from_millis(20));

        assert!(matches!(
            client.request(Command::Ping).await,
            Err(ClientError::RequestTimeout)
        ));
        assert!(client.is_closed());
        assert!(matches!(
            client.request(Command::Ping).await,
            Err(ClientError::Closed)
        ));
        server.abort();
    }

    fn private_tls_server_config() -> (Arc<ServerConfig>, String) {
        let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let certificate_pem = certificate.cert.pem();
        let private_key = PrivateKeyDer::Pkcs8(certificate.key_pair.serialize_der().into());
        let server_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![certificate.cert.der().clone()], private_key)
            .unwrap();
        (Arc::new(server_config), certificate_pem)
    }

    fn spawn_auth_server(
        listener: TcpListener,
        server_config: Arc<ServerConfig>,
    ) -> tokio::task::JoinHandle<std::result::Result<String, String>> {
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.map_err(|error| error.to_string())?;
            let stream = TlsAcceptor::from(server_config)
                .accept(stream)
                .await
                .map_err(|error| error.to_string())?;
            let mut framed = Framed::new(stream, LinesCodec::new());
            let line = framed
                .next()
                .await
                .ok_or_else(|| "client closed before authentication".to_owned())?
                .map_err(|error| error.to_string())?;
            let command = decode_command(&line).map_err(|error| error.to_string())?;
            let Command::Auth { api_key, .. } = command else {
                return Err("first command was not authentication".into());
            };
            framed
                .send("OK tls-test TTL 60".to_owned())
                .await
                .map_err(|error| error.to_string())?;
            Ok(api_key)
        })
    }

    #[tokio::test]
    async fn builder_tls_connection_verifies_private_ca_and_authenticates_key() {
        let (server_config, certificate_pem) = private_tls_server_config();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = spawn_auth_server(listener, server_config);
        let client_config = verified_tls_config(Some(certificate_pem.as_bytes())).unwrap();

        ClientBuilder::new()
            .tls_server_with_config(format!("localhost:{port}"), client_config)
            .api_key("ordinary-client-key")
            .connect()
            .await
            .unwrap();

        assert_eq!(server.await.unwrap().unwrap(), "ordinary-client-key");
    }

    #[tokio::test]
    async fn streaming_tls_connection_verifies_private_ca_and_authenticates_key() {
        let (server_config, certificate_pem) = private_tls_server_config();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = spawn_auth_server(listener, server_config);
        let client_config = verified_tls_config(Some(certificate_pem.as_bytes())).unwrap();

        StreamingClient::connect_tls_with_config(
            &format!("localhost:{port}"),
            client_config,
            "streaming-client-key",
            Uuid::new_v4(),
            Arc::new(NoopHandler),
        )
        .await
        .unwrap();

        assert_eq!(server.await.unwrap().unwrap(), "streaming-client-key");
    }

    #[tokio::test]
    async fn builder_tls_connection_rejects_untrusted_private_ca() {
        let (server_config, _) = private_tls_server_config();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = spawn_auth_server(listener, server_config);

        let error = match ClientBuilder::new()
            .tls_server(format!("localhost:{port}"))
            .api_key("untrusted-key")
            .connect()
            .await
        {
            Ok(_) => panic!("untrusted private CA was accepted"),
            Err(error) => error,
        };

        assert!(matches!(error, ClientError::NoHealthyEndpoints));
        assert!(server.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn streaming_tls_connection_rejects_hostname_mismatch() {
        let (server_config, certificate_pem) = private_tls_server_config();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = spawn_auth_server(listener, server_config);
        let client_config = verified_tls_config(Some(certificate_pem.as_bytes())).unwrap();

        let error = match StreamingClient::connect_tls_with_config(
            &address.to_string(),
            client_config,
            "mismatched-host-key",
            Uuid::new_v4(),
            Arc::new(NoopHandler),
        )
        .await
        {
            Ok(_) => panic!("hostname mismatch was accepted"),
            Err(error) => error,
        };

        assert!(matches!(error, ClientError::Tls(_)));
        assert!(server.await.unwrap().is_err());
    }
}

pub type Result<T> = std::result::Result<T, ClientError>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScheduleLease {
    Acquired { ttl_seconds: u64 },
    Unavailable,
}

/// A shared, request-ordered native Valkyr connection.
#[derive(Clone)]
pub struct Client {
    connection: Arc<Mutex<Connection>>,
    request_timeout: Duration,
    poisoned: Arc<std::sync::atomic::AtomicBool>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServerEndpoint {
    pub address: String,
    pub use_tls: bool,
}
impl ServerEndpoint {
    pub fn tcp(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
            use_tls: false,
        }
    }
    pub fn tls(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
            use_tls: true,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ClientBuilder {
    endpoints: Vec<ServerEndpoint>,
    tls_configs: Vec<Option<TlsClientConfig>>,
    api_key: Option<String>,
    adapter_instance: Option<Uuid>,
    connection_timeout: Duration,
    request_timeout: Duration,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}
impl ClientBuilder {
    pub fn new() -> Self {
        Self {
            endpoints: Vec::new(),
            tls_configs: Vec::new(),
            api_key: None,
            adapter_instance: None,
            connection_timeout: Duration::from_secs(10),
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
        }
    }
    pub fn server(mut self, address: impl Into<String>) -> Self {
        self.endpoints.push(ServerEndpoint::tcp(address));
        self.tls_configs.push(None);
        self
    }
    pub fn tls_server(mut self, address: impl Into<String>) -> Self {
        self.endpoints.push(ServerEndpoint::tls(address));
        self.tls_configs.push(None);
        self
    }
    /// Add a TLS endpoint using an already-built verified client configuration.
    pub fn tls_server_with_config(
        mut self,
        address: impl Into<String>,
        config: TlsClientConfig,
    ) -> Self {
        self.endpoints.push(ServerEndpoint::tls(address));
        self.tls_configs.push(Some(config));
        self
    }
    pub fn endpoints(mut self, endpoints: Vec<ServerEndpoint>) -> Self {
        self.tls_configs = vec![None; endpoints.len()];
        self.endpoints = endpoints;
        self
    }
    pub fn api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = Some(api_key.into());
        self
    }
    pub fn adapter_instance(mut self, id: Uuid) -> Self {
        self.adapter_instance = Some(id);
        self
    }
    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.connection_timeout = timeout;
        self
    }
    /// Set the maximum time a request may wait for a server response.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }
    pub async fn connect(self) -> Result<Client> {
        if self.endpoints.is_empty() {
            return Err(ClientError::Configuration(
                "at least one endpoint is required".into(),
            ));
        }
        let api_key = self
            .api_key
            .ok_or_else(|| ClientError::Configuration("an API key is required".into()))?;
        for (endpoint, tls_config) in self.endpoints.into_iter().zip(self.tls_configs) {
            debug!(address = %endpoint.address, tls = endpoint.use_tls, "connecting to Valkyr endpoint");
            let connection = tokio::time::timeout(self.connection_timeout, async {
                if endpoint.use_tls {
                    match tls_config {
                        Some(config) => {
                            Client::connect_tls_with_config(&endpoint.address, config).await
                        }
                        None => Client::connect_tls(&endpoint.address).await,
                    }
                } else {
                    Client::connect(&endpoint.address).await
                }
            })
            .await;
            let client = match connection {
                Ok(Ok(client)) => client,
                Ok(Err(error)) => {
                    warn!(address = %endpoint.address, tls = endpoint.use_tls, %error, "Valkyr endpoint connection failed");
                    continue;
                }
                Err(_) => {
                    warn!(address = %endpoint.address, tls = endpoint.use_tls, timeout = ?self.connection_timeout, "Valkyr endpoint connection timed out");
                    continue;
                }
            };
            let client = client.with_request_timeout(self.request_timeout);
            match client.authenticate(&api_key, self.adapter_instance).await {
                Ok(()) => return Ok(client),
                Err(error) => {
                    warn!(address = %endpoint.address, %error, "Valkyr endpoint authentication failed");
                }
            }
        }
        Err(ClientError::NoHealthyEndpoints)
    }
}

impl Client {
    pub async fn connect(address: impl tokio::net::ToSocketAddrs) -> Result<Self> {
        let stream = TcpStream::connect(address).await?;
        Ok(Self::from_io(stream))
    }

    /// Establish a TLS-protected native connection using the platform's
    /// WebPKI roots. The server name is derived from `host:port`.
    pub async fn connect_tls(address: &str) -> Result<Self> {
        Self::connect_tls_with_config(address, verified_tls_config(None)?).await
    }

    /// Establish TLS with an explicit Rustls client configuration. This is
    /// useful for private PKI deployments and test certificates.
    pub async fn connect_tls_with_config(address: &str, config: TlsClientConfig) -> Result<Self> {
        let server_name = tls_server_name(address)?;
        Self::connect_tls_with_server_name(address, server_name, config).await
    }

    /// Establish TLS with an explicit configuration and SNI/certificate name.
    /// This permits an IP listener address with a DNS certificate name.
    pub async fn connect_tls_with_server_name(
        address: &str,
        server_name: ServerName<'static>,
        config: TlsClientConfig,
    ) -> Result<Self> {
        install_default_crypto_provider();
        let stream = TcpStream::connect(address).await?;
        let stream = TlsConnector::from(config)
            .connect(server_name, stream)
            .await
            .map_err(|error| ClientError::Tls(error.to_string()))?;
        Ok(Self::from_io(stream))
    }

    fn from_io(stream: impl ConnectionIo + 'static) -> Self {
        let stream: Box<dyn ConnectionIo> = Box::new(stream);
        Self {
            connection: Arc::new(Mutex::new(Framed::new(
                stream,
                LinesCodec::new_with_max_length(1024 * 1024),
            ))),
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            poisoned: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    async fn poison_connection(&self, connection: &mut Connection) {
        self.poisoned
            .store(true, std::sync::atomic::Ordering::Release);
        let _ = connection.get_mut().shutdown().await;
    }

    pub async fn request(&self, command: Command) -> Result<Response> {
        if self.poisoned.load(std::sync::atomic::Ordering::Acquire) {
            return Err(ClientError::Closed);
        }
        let encoded =
            encode_command(&command).map_err(|error| ClientError::Protocol(error.to_string()))?;
        let mut connection = self.connection.lock().await;
        if self.poisoned.load(std::sync::atomic::Ordering::Acquire) {
            return Err(ClientError::Closed);
        }
        let line = match tokio::time::timeout(self.request_timeout, async {
            connection
                .send(encoded)
                .await
                .map_err(|error| ClientError::Frame(error.to_string()))?;
            connection
                .next()
                .await
                .ok_or(ClientError::Closed)?
                .map_err(|error| ClientError::Frame(error.to_string()))
        })
        .await
        {
            Ok(Ok(line)) => line,
            Ok(Err(error)) => {
                self.poison_connection(&mut connection).await;
                return Err(error);
            }
            Err(_) => {
                self.poison_connection(&mut connection).await;
                return Err(ClientError::RequestTimeout);
            }
        };
        let response = match decode_response(&command, &line) {
            Ok(response) => response,
            Err(error) => {
                self.poison_connection(&mut connection).await;
                return Err(ClientError::Protocol(error.to_string()));
            }
        };
        match response {
            Response::Error { message } => Err(ClientError::Server(message)),
            Response::AuthFailure { message } => Err(ClientError::Authentication(message)),
            response => Ok(response),
        }
    }

    pub async fn authenticate(
        &self,
        api_key: impl Into<String>,
        adapter_instance: Option<Uuid>,
    ) -> Result<()> {
        match self
            .request(Command::Auth {
                api_key: api_key.into(),
                adapter_instance,
            })
            .await?
        {
            Response::AuthSuccess { .. } => Ok(()),
            Response::AuthPending { retry_after_ms } => {
                Err(ClientError::AuthenticationPending { retry_after_ms })
            }
            _ => Err(ClientError::UnexpectedResponse("authentication success")),
        }
    }

    pub async fn provide(
        &self,
        namespace_pattern: NamespacePattern,
        key_pattern: KeyPattern,
        max_rate: Option<u32>,
    ) -> Result<()> {
        match self
            .request(Command::Provide {
                namespace_pattern,
                key_pattern,
                max_rate,
                timeout: None,
                miss_ttl: None,
            })
            .await?
        {
            Response::Ok => Ok(()),
            _ => Err(ClientError::UnexpectedResponse("ok")),
        }
    }

    pub async fn provide_with_options(
        &self,
        namespace_pattern: NamespacePattern,
        key_pattern: KeyPattern,
        options: ProvideOptions,
    ) -> Result<()> {
        match self
            .request(Command::Provide {
                namespace_pattern,
                key_pattern,
                max_rate: options.max_rate,
                timeout: Some(options.timeout_ms),
                miss_ttl: Some(options.miss_ttl_seconds),
            })
            .await?
        {
            Response::Ok => Ok(()),
            _ => Err(ClientError::UnexpectedResponse("ok")),
        }
    }

    pub async fn store(
        &self,
        namespace_pattern: NamespacePattern,
        key_pattern: KeyPattern,
    ) -> Result<()> {
        match self
            .request(Command::Store {
                namespace_pattern,
                key_pattern,
            })
            .await?
        {
            Response::Ok => Ok(()),
            _ => Err(ClientError::UnexpectedResponse("ok")),
        }
    }

    pub async fn get(&self, namespace: NamespaceContext, key: Key) -> Result<Value> {
        match self.request(Command::Get { namespace, key }).await? {
            Response::Value { value, .. } => Ok(value),
            _ => Err(ClientError::UnexpectedResponse("value")),
        }
    }

    /// Acquire or renew this adapter's 30-second, server-local schedule lease.
    pub async fn acquire_schedule_lease(
        &self,
        namespace: &NamespaceContext,
    ) -> Result<ScheduleLease> {
        let lease_namespace =
            NamespaceContext::new(LEASE_NAMESPACE).expect("the built-in lease namespace is valid");
        let key = Key::new(namespace.as_str())
            .map_err(|error| ClientError::Configuration(error.to_string()))?;
        match self
            .request(Command::Get {
                namespace: lease_namespace,
                key,
            })
            .await?
        {
            Response::Value {
                value: Value::Bool(true),
                ttl_seconds: Some(ttl_seconds),
            } if ttl_seconds > 0 => Ok(ScheduleLease::Acquired { ttl_seconds }),
            Response::Value {
                value: Value::Bool(false),
                ..
            } => Ok(ScheduleLease::Unavailable),
            _ => Err(ClientError::UnexpectedResponse("schedule lease response")),
        }
    }

    /// Release a schedule lease owned by this authenticated adapter connection.
    pub async fn release_schedule_lease(&self, namespace: &NamespaceContext) -> Result<()> {
        let lease_namespace =
            NamespaceContext::new(LEASE_NAMESPACE).expect("the built-in lease namespace is valid");
        let key = Key::new(namespace.as_str())
            .map_err(|error| ClientError::Configuration(error.to_string()))?;
        self.set(lease_namespace, key, Value::from(0), None).await
    }

    pub async fn set(
        &self,
        namespace: NamespaceContext,
        key: Key,
        value: Value,
        ttl: Option<Duration>,
    ) -> Result<()> {
        match self
            .request(Command::Set {
                namespace,
                key,
                value,
                ttl_seconds: ttl.map(|duration| duration.as_secs()),
            })
            .await?
        {
            Response::Ok => Ok(()),
            _ => Err(ClientError::UnexpectedResponse("ok")),
        }
    }

    pub async fn set_batch(
        &self,
        namespace: NamespaceContext,
        entries: Vec<SetEntry>,
        ttl: Option<Duration>,
    ) -> Result<()> {
        match self
            .request(Command::SetBatch {
                namespace,
                entries,
                ttl_seconds: ttl.map(|duration| duration.as_secs()),
            })
            .await?
        {
            Response::Ok => Ok(()),
            _ => Err(ClientError::UnexpectedResponse("ok")),
        }
    }

    pub async fn delete(
        &self,
        namespace: NamespaceContext,
        pattern: Option<KeyPattern>,
    ) -> Result<()> {
        match self
            .request(Command::Delete {
                namespace,
                key_pattern: pattern,
            })
            .await?
        {
            Response::Ok => Ok(()),
            _ => Err(ClientError::UnexpectedResponse("ok")),
        }
    }

    pub async fn move_namespace(
        &self,
        source: NamespaceContext,
        destination: NamespaceContext,
    ) -> Result<()> {
        match self
            .request(Command::Move {
                source,
                destination,
            })
            .await?
        {
            Response::Ok => Ok(()),
            _ => Err(ClientError::UnexpectedResponse("ok")),
        }
    }

    pub async fn ping(&self) -> Result<()> {
        match self.request(Command::Ping).await? {
            Response::Pong => Ok(()),
            _ => Err(ClientError::UnexpectedResponse("pong")),
        }
    }

    pub async fn stats(&self) -> Result<Stats> {
        match self.request(Command::Stats).await? {
            Response::Stats(stats) => Ok(stats),
            _ => Err(ClientError::UnexpectedResponse("stats")),
        }
    }
}

fn install_default_crypto_provider() {
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
}

#[async_trait]
pub trait ServerCommandHandler: Send + Sync + 'static {
    async fn handle(&self, command: ServerCommand) -> ServerResult;
}

/// A long-lived adapter connection. Its reader remains active while idle, so
/// provider and storage callbacks are delivered immediately rather than only
/// while an ordinary request is awaiting a response.
pub struct StreamingClient {
    outbound: Arc<Mutex<Option<Sink>>>,
    responses: Mutex<mpsc::UnboundedReceiver<Response>>,
    pending_commands: Arc<Mutex<VecDeque<Command>>>,
    _reader: tokio::task::JoinHandle<()>,
    request_timeout: Duration,
    poisoned: std::sync::atomic::AtomicBool,
}

impl Drop for StreamingClient {
    fn drop(&mut self) {
        self._reader.abort();
    }
}

impl StreamingClient {
    pub async fn connect(
        address: impl tokio::net::ToSocketAddrs,
        api_key: impl Into<String>,
        adapter_instance: Uuid,
        handler: Arc<dyn ServerCommandHandler>,
    ) -> Result<Self> {
        let stream = TcpStream::connect(address).await?;
        Self::connect_with_io(stream, api_key, adapter_instance, handler).await
    }

    /// Establish a TLS-protected streaming connection. Callback traffic uses
    /// the same authenticated connection as normal adapter registration.
    pub async fn connect_tls(
        address: &str,
        api_key: impl Into<String>,
        adapter_instance: Uuid,
        handler: Arc<dyn ServerCommandHandler>,
    ) -> Result<Self> {
        Self::connect_tls_with_config(
            address,
            verified_tls_config(None)?,
            api_key,
            adapter_instance,
            handler,
        )
        .await
    }

    /// Establish a TLS-protected streaming connection with an explicit
    /// verified Rustls client configuration.
    pub async fn connect_tls_with_config(
        address: &str,
        config: Arc<RustlsClientConfig>,
        api_key: impl Into<String>,
        adapter_instance: Uuid,
        handler: Arc<dyn ServerCommandHandler>,
    ) -> Result<Self> {
        let stream = TcpStream::connect(address).await?;
        let server_name = tls_server_name(address)?;
        let stream = TlsConnector::from(config)
            .connect(server_name, stream)
            .await
            .map_err(|error| ClientError::Tls(error.to_string()))?;
        Self::connect_with_io(stream, api_key, adapter_instance, handler).await
    }

    async fn connect_with_io(
        stream: impl ConnectionIo + 'static,
        api_key: impl Into<String>,
        adapter_instance: Uuid,
        handler: Arc<dyn ServerCommandHandler>,
    ) -> Result<Self> {
        let stream: Box<dyn ConnectionIo> = Box::new(stream);
        let framed = Framed::new(stream, LinesCodec::new_with_max_length(1024 * 1024));
        let (sink, source) = framed.split();
        let outbound = Arc::new(Mutex::new(Some(sink)));
        let (response_sender, response_receiver) = mpsc::unbounded_channel();
        let pending_commands = Arc::new(Mutex::new(VecDeque::new()));
        let reader = tokio::spawn(read_stream(
            source,
            outbound.clone(),
            handler,
            response_sender,
            pending_commands.clone(),
        ));
        let client = Self {
            outbound,
            responses: Mutex::new(response_receiver),
            pending_commands,
            _reader: reader,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            poisoned: std::sync::atomic::AtomicBool::new(false),
        };
        client
            .request(Command::Auth {
                api_key: api_key.into(),
                adapter_instance: Some(adapter_instance),
            })
            .await?;
        Ok(client)
    }

    pub async fn request(&self, command: Command) -> Result<Response> {
        if self.poisoned.load(std::sync::atomic::Ordering::Acquire) {
            return Err(ClientError::Closed);
        }
        let encoded =
            encode_command(&command).map_err(|error| ClientError::Protocol(error.to_string()))?;
        let mut responses = self.responses.lock().await;
        if self.poisoned.load(std::sync::atomic::Ordering::Acquire) {
            return Err(ClientError::Closed);
        }
        let response = match tokio::time::timeout(self.request_timeout, async {
            let mut outbound = self.outbound.lock().await;
            self.pending_commands
                .lock()
                .await
                .push_back(command.clone());
            outbound
                .as_mut()
                .ok_or(ClientError::Closed)?
                .send(encoded)
                .await
                .map_err(|error| ClientError::Frame(error.to_string()))?;
            responses.recv().await.ok_or(ClientError::Closed)
        })
        .await
        {
            Ok(result) => result?,
            Err(_) => {
                self.poisoned
                    .store(true, std::sync::atomic::Ordering::Release);
                self.outbound.lock().await.take();
                self._reader.abort();
                return Err(ClientError::RequestTimeout);
            }
        };
        match response {
            Response::Error { message } => Err(ClientError::Server(message)),
            Response::AuthFailure { message } => Err(ClientError::Authentication(message)),
            response => Ok(response),
        }
    }

    /// True once the background reader has observed a closed or invalid
    /// connection. Callers can use this to restore callback registrations.
    pub fn is_closed(&self) -> bool {
        self.poisoned.load(std::sync::atomic::Ordering::Acquire) || self._reader.is_finished()
    }

    /// Set the maximum time a request may wait for a server response.
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    pub async fn provide(
        &self,
        namespace_pattern: NamespacePattern,
        key_pattern: KeyPattern,
        max_rate: Option<u32>,
    ) -> Result<()> {
        match self
            .request(Command::Provide {
                namespace_pattern,
                key_pattern,
                max_rate,
                timeout: None,
                miss_ttl: None,
            })
            .await?
        {
            Response::Ok => Ok(()),
            _ => Err(ClientError::UnexpectedResponse("ok")),
        }
    }

    pub async fn provide_with_options(
        &self,
        namespace_pattern: NamespacePattern,
        key_pattern: KeyPattern,
        options: ProvideOptions,
    ) -> Result<()> {
        match self
            .request(Command::Provide {
                namespace_pattern,
                key_pattern,
                max_rate: options.max_rate,
                timeout: Some(options.timeout_ms),
                miss_ttl: Some(options.miss_ttl_seconds),
            })
            .await?
        {
            Response::Ok => Ok(()),
            _ => Err(ClientError::UnexpectedResponse("ok")),
        }
    }

    pub async fn store(
        &self,
        namespace_pattern: NamespacePattern,
        key_pattern: KeyPattern,
    ) -> Result<()> {
        match self
            .request(Command::Store {
                namespace_pattern,
                key_pattern,
            })
            .await?
        {
            Response::Ok => Ok(()),
            _ => Err(ClientError::UnexpectedResponse("ok")),
        }
    }
}

fn tls_server_name(address: &str) -> Result<ServerName<'static>> {
    let host = address
        .strip_prefix('[')
        .and_then(|value| value.split_once(']').map(|(host, _)| host))
        .or_else(|| address.rsplit_once(':').map(|(host, _)| host))
        .unwrap_or(address);
    ServerName::try_from(host.to_owned()).map_err(|error| {
        ClientError::Configuration(format!("invalid TLS server name '{host}': {error}"))
    })
}

/// Build a verified client configuration from WebPKI roots plus optional PEM
/// CA certificates. Custom roots augment, rather than replace, public roots.
pub fn verified_tls_config(ca_certificate: Option<&[u8]>) -> Result<TlsClientConfig> {
    install_default_crypto_provider();
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    if let Some(ca_certificate) = ca_certificate {
        let mut reader = Cursor::new(ca_certificate);
        let certificates = rustls_pemfile::certs(&mut reader)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| {
                ClientError::Configuration(format!("invalid CA certificate PEM: {error}"))
            })?;
        if certificates.is_empty() {
            return Err(ClientError::Configuration(
                "CA certificate PEM contains no certificates".into(),
            ));
        }
        for certificate in certificates {
            roots.add(certificate).map_err(|error| {
                ClientError::Configuration(format!("invalid CA certificate: {error}"))
            })?;
        }
    }
    Ok(Arc::new(
        RustlsClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    ))
}

async fn read_stream(
    mut source: SplitStream<Connection>,
    outbound: Arc<Mutex<Option<Sink>>>,
    handler: Arc<dyn ServerCommandHandler>,
    responses: mpsc::UnboundedSender<Response>,
    pending_commands: Arc<Mutex<VecDeque<Command>>>,
) {
    while let Some(frame) = source.next().await {
        let Ok(line) = frame else {
            break;
        };
        if let Ok(command) = decode_server_command(&line) {
            let result = handler.handle(command.clone()).await;
            let Ok(encoded) = encode_server_result(&command, &result) else {
                break;
            };
            let mut outbound = outbound.lock().await;
            let Some(outbound) = outbound.as_mut() else {
                break;
            };
            if outbound.send(encoded).await.is_err() {
                break;
            }
        } else if let Some(command) = pending_commands.lock().await.pop_front() {
            let Ok(response) = decode_response(&command, &line) else {
                break;
            };
            if responses.send(response).is_err() {
                break;
            }
        } else {
            break;
        }
    }
}
