//! Native TCP server for Valkyr's human-readable text protocol.

mod http_api;
use futures_util::{SinkExt, StreamExt};
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    future::Future,
    io::{self, BufReader},
    path::Path,
    pin::Pin,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};
#[cfg(test)]
use tokio::sync::Notify;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpListener,
    sync::{Mutex, mpsc, oneshot, watch},
    time,
};
use tokio_rustls::TlsAcceptor;
use tokio_util::codec::{Framed, LinesCodec};
use tracing::{debug, info, warn};
use uuid::Uuid;
use valkyr_core::{
    Broker, BrokerOutcome, Command, ConnectionId, Dispatch, MemoryStore, RequestContext, Response,
    ServerCommand, ServerResult,
    line_protocol::{
        callback_request_id, decode_command, decode_server_result, encode_response,
        encode_server_command,
    },
};

const DEFAULT_OUTBOUND_CAPACITY: usize = 256;
const DEFAULT_COMMAND_CAPACITY: usize = 64;

trait SessionIo: Send + 'static {
    type Sink: Send + 'static;
    type Source: Send + 'static;

    fn split(self) -> (Self::Sink, Self::Source);
    fn send(
        sink: &mut Self::Sink,
        message: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), ()>> + Send + '_>>;
    fn next(source: &mut Self::Source)
    -> Pin<Box<dyn Future<Output = Option<String>> + Send + '_>>;
}

struct TcpSessionIo<S>(Framed<S, LinesCodec>);

impl<S> SessionIo for TcpSessionIo<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Sink = futures_util::stream::SplitSink<Framed<S, LinesCodec>, String>;
    type Source = futures_util::stream::SplitStream<Framed<S, LinesCodec>>;

    fn split(self) -> (Self::Sink, Self::Source) {
        self.0.split()
    }

    fn send(
        sink: &mut Self::Sink,
        message: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), ()>> + Send + '_>> {
        Box::pin(async move { sink.send(message).await.map_err(|_| ()) })
    }

    fn next(
        source: &mut Self::Source,
    ) -> Pin<Box<dyn Future<Output = Option<String>> + Send + '_>> {
        Box::pin(async move { source.next().await.and_then(Result::ok) })
    }
}

struct WebSocketSessionIo(axum::extract::ws::WebSocket);

impl SessionIo for WebSocketSessionIo {
    type Sink =
        futures_util::stream::SplitSink<axum::extract::ws::WebSocket, axum::extract::ws::Message>;
    type Source = futures_util::stream::SplitStream<axum::extract::ws::WebSocket>;

    fn split(self) -> (Self::Sink, Self::Source) {
        self.0.split()
    }

    fn send(
        sink: &mut Self::Sink,
        message: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), ()>> + Send + '_>> {
        Box::pin(async move {
            sink.send(axum::extract::ws::Message::Text(message.into()))
                .await
                .map_err(|_| ())
        })
    }

    fn next(
        source: &mut Self::Source,
    ) -> Pin<Box<dyn Future<Output = Option<String>> + Send + '_>> {
        Box::pin(async move {
            match source.next().await {
                Some(Ok(axum::extract::ws::Message::Text(message))) => Some(message.to_string()),
                _ => None,
            }
        })
    }
}

struct ConnectionHandle {
    outbound: mpsc::Sender<String>,
    adapter_instance: Mutex<Option<Uuid>>,
    pending: Mutex<PendingRequests>,
    shutdown: watch::Sender<()>,
}

#[derive(Default)]
struct PendingRequests {
    ids: HashSet<Uuid>,
    closed: bool,
}

struct PendingResult {
    owner: ConnectionId,
    sender: oneshot::Sender<ServerResult>,
    command: ServerCommand,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RefreshCompletion {
    Pending,
    Value,
    Miss,
    RateLimited(u64),
    Failed,
}

struct RefreshState {
    completion: watch::Sender<RefreshCompletion>,
    broker_refresh_id: Option<Uuid>,
}

#[cfg(test)]
struct RateLimitedReleasePause {
    released: Notify,
    continue_publication: Notify,
}

type RefreshRoute = (valkyr_core::NamespaceContext, valkyr_core::Key);
type Refreshes = HashMap<RefreshRoute, Arc<RefreshState>>;

impl ConnectionHandle {
    fn close(&self) {
        let _ = self.shutdown.send(());
    }
}

fn is_client_keyword(keyword: &str) -> bool {
    matches!(
        keyword,
        "AUTH"
            | "GET"
            | "SET"
            | "SET_BATCH"
            | "DELETE"
            | "MOVE"
            | "PROVIDE"
            | "STORE"
            | "PING"
            | "STATS"
    )
}

pub struct Server {
    broker: Arc<Broker>,
    max_frame_length: usize,
    callback_timeout: Duration,
    outbound_capacity: usize,
    command_capacity: usize,
    connections: Mutex<HashMap<ConnectionId, Arc<ConnectionHandle>>>,
    pending_results: Mutex<HashMap<Uuid, PendingResult>>,
    inflight_refreshes: Arc<StdMutex<Refreshes>>,
    connection_shutdown: watch::Sender<()>,
    #[cfg(test)]
    rate_limited_release_pause: Option<Arc<RateLimitedReleasePause>>,
}

pub enum AuthenticationResult {
    Authenticated(valkyr_core::AuthInfo),
    Pending(u64),
    Rejected,
}

impl Server {
    pub fn in_memory() -> Self {
        Self::with_broker(Broker::new(Arc::new(MemoryStore::new()), None))
    }

    pub fn with_broker(broker: Broker) -> Self {
        let (connection_shutdown, _) = watch::channel(());
        Self {
            broker: Arc::new(broker),
            max_frame_length: 1024 * 1024,
            callback_timeout: Duration::from_secs(30),
            outbound_capacity: DEFAULT_OUTBOUND_CAPACITY,
            command_capacity: DEFAULT_COMMAND_CAPACITY,
            connections: Mutex::new(HashMap::new()),
            pending_results: Mutex::new(HashMap::new()),
            inflight_refreshes: Arc::new(StdMutex::new(HashMap::new())),
            connection_shutdown,
            #[cfg(test)]
            rate_limited_release_pause: None,
        }
    }
    pub fn with_max_frame_length(mut self, bytes: usize) -> Self {
        self.max_frame_length = bytes;
        self
    }
    pub fn with_callback_timeout(mut self, timeout: Duration) -> Self {
        self.callback_timeout = timeout;
        self
    }
    #[cfg(test)]
    fn with_rate_limited_release_pause(mut self, pause: Arc<RateLimitedReleasePause>) -> Self {
        self.rate_limited_release_pause = Some(pause);
        self
    }
    /// Set the maximum queued callbacks and responses per connection.
    /// Connections that exceed this capacity are closed.
    pub fn with_outbound_capacity(mut self, capacity: usize) -> Self {
        assert!(capacity > 0, "outbound capacity must be positive");
        self.outbound_capacity = capacity;
        self
    }
    /// Set the maximum queued commands per connection.
    /// Reading pauses when this capacity is reached.
    pub fn with_command_capacity(mut self, capacity: usize) -> Self {
        assert!(capacity > 0, "command capacity must be positive");
        self.command_capacity = capacity;
        self
    }
    pub fn broker(&self) -> &Arc<Broker> {
        &self.broker
    }

    /// Execute a request from a stateless transport such as REST. The caller
    /// supplies its authenticated identity; storage callbacks still use their
    /// connection-scoped registrations.
    pub async fn execute(
        self: &Arc<Self>,
        command: Command,
        auth: Option<valkyr_core::AuthInfo>,
    ) -> Result<Response, valkyr_core::Error> {
        self.finish_outcome(
            self.execute_broker_command(
                command,
                RequestContext {
                    owner: Uuid::new_v4(),
                    adapter_instance: None,
                    auth,
                    variables: Default::default(),
                },
            )
            .await?,
        )
        .await
    }

    /// Execute through the canonical text codec. HTTP uses this boundary so
    /// its external JSON contract cannot bypass protocol validation.
    pub async fn execute_text(
        self: &Arc<Self>,
        command: Command,
        auth: Option<valkyr_core::AuthInfo>,
    ) -> Result<Response, valkyr_core::Error> {
        let encoded = valkyr_core::line_protocol::encode_command(&command)?;
        let decoded = valkyr_core::line_protocol::decode_command(&encoded)?;
        let response = self.execute(decoded, auth).await?;
        let encoded_response = valkyr_core::line_protocol::encode_response(&command, &response)?;
        valkyr_core::line_protocol::decode_response(&command, &encoded_response)
    }

    async fn execute_broker_command(
        self: &Arc<Self>,
        command: Command,
        context: RequestContext,
    ) -> Result<BrokerOutcome, valkyr_core::Error> {
        self.broker
            .authorize_encrypted_command(&command, &context)?;
        self.ensure_command_security_key(&command).await?;
        self.broker.execute(command, context).await
    }

    async fn ensure_command_security_key(
        self: &Arc<Self>,
        command: &Command,
    ) -> Result<(), valkyr_core::Error> {
        let namespace = match command {
            Command::Get { namespace, key } if encrypted_key(key.as_str()) => Some(namespace),
            Command::Set { namespace, key, .. } if encrypted_key(key.as_str()) => Some(namespace),
            Command::SetBatch {
                namespace, entries, ..
            } if entries
                .iter()
                .any(|entry| encrypted_key(entry.key.as_str())) =>
            {
                Some(namespace)
            }
            _ => None,
        };
        let Some(namespace) = namespace else {
            return Ok(());
        };
        let Some(dispatch) = self
            .broker
            .security_key_provider_dispatch(namespace)
            .await?
        else {
            return Ok(());
        };
        let ServerCommand::Query { namespace, key, .. } = dispatch.command.clone() else {
            unreachable!("security key lookup always dispatches a query");
        };
        let ServerResult::Query {
            value: Some(value),
            error: None,
            ttl_seconds,
            ..
        } = self.invoke(dispatch.clone()).await?
        else {
            return Err(valkyr_core::Error::Encryption(
                "security key provider did not return a key".into(),
            ));
        };
        let from_adapter = match self.connections.lock().await.get(&dispatch.owner).cloned() {
            Some(handle) => handle.adapter_instance.lock().await.is_some(),
            None => false,
        };
        let outcome = self
            .broker
            .accept_provider_value(
                namespace,
                key,
                value,
                ttl_seconds.map(Duration::from_secs),
                from_adapter,
                false,
            )
            .await?;
        self.finish_outcome(outcome).await?;
        Ok(())
    }

    pub async fn authenticate(
        self: &Arc<Self>,
        api_key: &str,
    ) -> Result<AuthenticationResult, valkyr_core::Error> {
        let command = Command::Auth {
            api_key: api_key.into(),
            adapter_instance: None,
        };
        let encoded = valkyr_core::line_protocol::encode_command(&command)?;
        let command = valkyr_core::line_protocol::decode_command(&encoded)?;
        let outcome = self
            .execute_broker_command(command.clone(), RequestContext::anonymous(Uuid::new_v4()))
            .await?;
        let authenticated = outcome.authenticated.clone();
        let response = self.finish_outcome(outcome).await?;
        let encoded_response = valkyr_core::line_protocol::encode_response(&command, &response)?;
        let response = valkyr_core::line_protocol::decode_response(&command, &encoded_response)?;
        Ok(match response {
            Response::AuthSuccess { .. } => AuthenticationResult::Authenticated(
                authenticated.expect("auth success includes a principal"),
            ),
            Response::AuthPending { retry_after_ms } => {
                AuthenticationResult::Pending(retry_after_ms)
            }
            Response::AuthFailure { .. } => AuthenticationResult::Rejected,
            _ => AuthenticationResult::Rejected,
        })
    }

    pub async fn bind(self, address: impl tokio::net::ToSocketAddrs) -> io::Result<RunningServer> {
        Arc::new(self).bind_shared(address).await
    }

    pub async fn bind_shared(
        self: Arc<Self>,
        address: impl tokio::net::ToSocketAddrs,
    ) -> io::Result<RunningServer> {
        Ok(RunningServer {
            listener: TcpListener::bind(address).await?,
            server: self,
        })
    }

    /// Bind a TLS listener for the native text protocol. HTTP/WebSocket
    /// remains a separately configured listener.
    pub async fn bind_tls(
        self: Arc<Self>,
        address: impl tokio::net::ToSocketAddrs,
        tls: Arc<rustls::ServerConfig>,
    ) -> io::Result<RunningTlsServer> {
        Ok(RunningTlsServer {
            listener: TcpListener::bind(address).await?,
            server: self,
            acceptor: TlsAcceptor::from(tls),
        })
    }

    pub fn http_router(self: Arc<Self>) -> axum::Router {
        http_api::router(self)
    }

    /// Render broker and connection counters in Prometheus' text exposition
    /// format. This deliberately contains no backend-specific labels.
    pub async fn metrics_text(&self) -> Result<String, valkyr_core::Error> {
        let stats = self.broker.stats().await?;
        let active_connections = self.connections.lock().await.len();
        Ok(format!(
            "# HELP valkyr_requests_total Total commands handled by Valkyr\n\
             # TYPE valkyr_requests_total counter\n\
             valkyr_requests_total {}\n\
             # HELP valkyr_cache_hits_total Cache hits\n\
             # TYPE valkyr_cache_hits_total counter\n\
             valkyr_cache_hits_total {}\n\
             # HELP valkyr_cache_misses_total Cache misses\n\
             # TYPE valkyr_cache_misses_total counter\n\
             valkyr_cache_misses_total {}\n\
             # HELP valkyr_values Current cached values\n\
             # TYPE valkyr_values gauge\n\
             valkyr_values {}\n\
             # HELP valkyr_active_connections Open native and WebSocket connections\n\
             # TYPE valkyr_active_connections gauge\n\
             valkyr_active_connections {active_connections}\n",
            stats.requests, stats.hits, stats.misses, stats.values
        ))
    }

    pub async fn serve_http(
        self: Arc<Self>,
        address: impl tokio::net::ToSocketAddrs,
        shutdown: watch::Receiver<()>,
    ) -> io::Result<()> {
        let listener = TcpListener::bind(address).await?;
        info!(address = %listener.local_addr()?, "HTTP/WebSocket listener bound");
        let router = self.clone().http_router();
        let shutdown_server = self;
        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let mut shutdown = shutdown;
                let _ = shutdown.changed().await;
                let _ = shutdown_server.connection_shutdown.send(());
            })
            .await
    }

    /// Serve unauthenticated Prometheus metrics on an independent listener.
    pub async fn serve_metrics(
        self: Arc<Self>,
        address: impl tokio::net::ToSocketAddrs,
        mut shutdown: watch::Receiver<()>,
    ) -> io::Result<()> {
        let listener = TcpListener::bind(address).await?;
        info!(address = %listener.local_addr()?, "Prometheus metrics listener bound");
        axum::serve(listener, http_api::metrics_router(self))
            .with_graceful_shutdown(async move {
                let _ = shutdown.changed().await;
            })
            .await
    }

    async fn handle_connection<S>(self: Arc<Self>, stream: S)
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let framed = Framed::new(
            stream,
            LinesCodec::new_with_max_length(self.max_frame_length),
        );
        self.serve_session(TcpSessionIo(framed), "native").await;
    }

    pub(crate) async fn handle_websocket(self: Arc<Self>, socket: axum::extract::ws::WebSocket) {
        self.serve_session(WebSocketSessionIo(socket), "WebSocket")
            .await;
    }

    async fn serve_session<I>(self: Arc<Self>, io: I, transport: &'static str)
    where
        I: SessionIo,
    {
        let mut shutdown = self.connection_shutdown.subscribe();
        let id = Uuid::new_v4();
        debug!(connection_id = %id, %transport, "connection opened");
        let (mut sink, mut source) = io.split();
        let (outbound, mut outbound_receiver) = mpsc::channel(self.outbound_capacity);
        let (commands, mut command_receiver) = mpsc::channel(self.command_capacity);
        let (connection_shutdown, mut connection_shutdown_receiver) = watch::channel(());
        let handle = Arc::new(ConnectionHandle {
            outbound: outbound.clone(),
            adapter_instance: Mutex::new(None),
            pending: Mutex::new(PendingRequests::default()),
            shutdown: connection_shutdown,
        });
        self.connections.lock().await.insert(id, handle.clone());
        let writer = tokio::spawn(async move {
            while let Some(message) = outbound_receiver.recv().await {
                if I::send(&mut sink, message).await.is_err() {
                    break;
                }
            }
        });
        let mut worker = {
            let server = self.clone();
            let handle = handle.clone();
            tokio::spawn(async move {
                let mut context = RequestContext::anonymous(id);
                while let Some(command) = command_receiver.recv().await {
                    debug!(connection_id = %id, %transport, command = command_name(&command), "processing command");
                    let response_context = command.clone();
                    if let Command::Auth {
                        adapter_instance, ..
                    } = &command
                    {
                        context.adapter_instance = *adapter_instance;
                    }
                    let response = match server
                        .execute_broker_command(command, context.clone())
                        .await
                    {
                        Ok(outcome) => {
                            if let Some(authenticated) = outcome.authenticated.clone() {
                                context.auth = Some(authenticated);
                                let connection =
                                    { server.connections.lock().await.get(&id).cloned() };
                                if let Some(handle) = connection {
                                    *handle.adapter_instance.lock().await =
                                        context.adapter_instance;
                                }
                            }
                            server
                                .finish_outcome(outcome)
                                .await
                                .unwrap_or_else(Response::error)
                        }
                        Err(error) => Response::error(error),
                    };
                    let encoded = encode_response(&response_context, &response)
                        .unwrap_or_else(|_| "KO response encoding failed".into());
                    if handle.outbound.try_send(encoded).is_err() {
                        handle.close();
                        break;
                    }
                }
            })
        };
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    let _ = changed;
                    break;
                }
                changed = connection_shutdown_receiver.changed() => {
                    let _ = changed;
                    break;
                }
                line = I::next(&mut source) => {
                    let Some(line) = line else { break };
                    let keyword = line.split_ascii_whitespace().next();
                    let adapter_connection = handle.adapter_instance.lock().await.is_some();
                    let callback_frame = matches!(keyword, Some("OPERATION" | "QUERY_RESULT"))
                        || (adapter_connection
                            && keyword.is_some_and(|value| !is_client_keyword(value)));
                    if callback_frame {
                        let Ok(request_id) = callback_request_id(&line) else {
                            handle.close();
                            break;
                        };
                        let expected = self.pending_results.lock().await.get(&request_id).map(|pending| pending.command.clone());
                        let Some(expected) = expected else { handle.close(); break; };
                        match decode_server_result(&expected, &line) {
                            Ok(result) => self.complete_result(result).await,
                            Err(_) => { handle.close(); break; }
                        }
                    } else {
                        match decode_command(&line) {
                            Ok(command) => if commands.send(command).await.is_err() { break; },
                            Err(error) => {
                                let encoded = encode_response(&Command::Ping, &Response::error(error));
                                if outbound.try_send(encoded.unwrap_or_else(|_| "KO invalid protocol message".into())).is_err() { handle.close(); break; }
                            }
                        }
                    }
                }
            }
        }
        self.remove_connection(id, &handle).await;
        self.broker.registry().remove_owner(id).await;
        debug!(connection_id = %id, %transport, "connection closed");
        drop(commands);
        drop(outbound);
        if time::timeout(Duration::from_secs(5), &mut worker)
            .await
            .is_err()
        {
            worker.abort();
        }
        writer.abort();
    }

    async fn finish_outcome(
        self: &Arc<Self>,
        outcome: BrokerOutcome,
    ) -> Result<Response, valkyr_core::Error> {
        let Some(dispatch) = outcome.dispatch else {
            return Ok(outcome.response);
        };
        if matches!(dispatch.command, ServerCommand::Query { .. }) {
            let server = self.clone();
            if dispatch.authentication {
                tokio::spawn(async move {
                    server.refresh_auth_from_provider(dispatch).await;
                });
            } else {
                let ServerCommand::Query { namespace, key, .. } = &dispatch.command else {
                    unreachable!("query dispatch always carries a query command");
                };
                let route = (namespace.clone(), key.clone());
                let refreshes = self.inflight_refreshes.clone();
                let (state, created) = {
                    let mut refreshes = refreshes
                        .lock()
                        .expect("in-flight refreshes mutex poisoned");
                    match refreshes.get(&route) {
                        Some(state) => (state.clone(), false),
                        None => {
                            let (completion, _) = watch::channel(RefreshCompletion::Pending);
                            let state = Arc::new(RefreshState {
                                completion,
                                broker_refresh_id: dispatch.provider_refresh_id,
                            });
                            refreshes.insert(route.clone(), state.clone());
                            (state, true)
                        }
                    }
                };
                if !created && state.broker_refresh_id != dispatch.provider_refresh_id {
                    if let Some(provider_refresh_id) = dispatch.provider_refresh_id {
                        self.broker
                            .release_provider_refresh(&route.0, &route.1, provider_refresh_id)
                            .await;
                    }
                }
                if created {
                    if let Some(retry_after_ms) = self
                        .broker
                        .provider_retry_after(
                            dispatch.provider_id,
                            dispatch
                                .provider_options
                                .and_then(|options| options.max_rate),
                        )
                        .await
                    {
                        if let Some(provider_refresh_id) = dispatch.provider_refresh_id {
                            self.broker
                                .release_provider_refresh(&route.0, &route.1, provider_refresh_id)
                                .await;
                        }
                        #[cfg(test)]
                        if let Some(pause) = &self.rate_limited_release_pause {
                            pause.released.notify_one();
                            pause.continue_publication.notified().await;
                        }
                        self.publish_refresh_completion(
                            &route,
                            &state,
                            RefreshCompletion::RateLimited(retry_after_ms),
                        );
                    } else {
                        let state_for_task = state.clone();
                        let route_for_task = route.clone();
                        let dispatch_for_task = dispatch.clone();
                        tokio::spawn(async move {
                            server
                                .refresh_from_provider(
                                    dispatch_for_task,
                                    state_for_task,
                                    route_for_task,
                                )
                                .await;
                        });
                    }
                }
                let timeout_ms = dispatch
                    .provider_options
                    .map_or(0, |options| options.timeout_ms);
                if timeout_ms == 0 {
                    if let RefreshCompletion::RateLimited(retry_after_ms) =
                        state.completion.borrow().clone()
                    {
                        return Ok(Response::Miss { retry_after_ms });
                    }
                    return Ok(outcome.response);
                }
                let mut completion = state.completion.subscribe();
                let terminal = time::timeout(
                    Duration::from_millis(timeout_ms),
                    wait_for_refresh(&mut completion),
                )
                .await
                .ok()
                .unwrap_or(RefreshCompletion::Failed);
                return match terminal {
                    RefreshCompletion::Value => self
                        .broker
                        .cached_value_response(&route.0, &route.1, dispatch.encrypted)
                        .await?
                        .map_or(Ok(outcome.response), Ok),
                    RefreshCompletion::Miss => self
                        .broker
                        .cached_value_response(&route.0, &route.1, dispatch.encrypted)
                        .await?
                        .map_or(Ok(Response::Miss { retry_after_ms: 0 }), Ok),
                    RefreshCompletion::RateLimited(retry_after_ms) => {
                        Ok(Response::Miss { retry_after_ms })
                    }
                    RefreshCompletion::Pending | RefreshCompletion::Failed => Ok(outcome.response),
                };
            }
            return Ok(outcome.response);
        }
        match (self.invoke(dispatch).await?, outcome.pending_mutation) {
            (
                ServerResult::Operation {
                    error: Some(error), ..
                },
                _,
            ) => Err(valkyr_core::Error::Protocol(error)),
            (ServerResult::Operation { error: None, .. }, Some(mutation)) => {
                self.broker.commit(mutation).await?;
                Ok(outcome.response)
            }
            _ => Err(valkyr_core::Error::Protocol(
                "callback result does not match request".into(),
            )),
        }
    }

    async fn refresh_from_provider(
        self: Arc<Self>,
        dispatch: Dispatch,
        state: Arc<RefreshState>,
        route: (valkyr_core::NamespaceContext, valkyr_core::Key),
    ) {
        let ServerCommand::Query { namespace, key, .. } = dispatch.command.clone() else {
            return;
        };
        let completion = match self.invoke(dispatch.clone()).await {
            Ok(ServerResult::Query {
                value: Some(value),
                error: None,
                ttl_seconds,
                ..
            }) => {
                let handle = self.connections.lock().await.get(&dispatch.owner).cloned();
                let from_adapter = match handle {
                    Some(handle) => handle.adapter_instance.lock().await.is_some(),
                    None => false,
                };
                let result = self
                    .broker
                    .accept_provider_value(
                        namespace,
                        key,
                        value,
                        ttl_seconds.map(Duration::from_secs),
                        from_adapter,
                        dispatch.encrypted,
                    )
                    .await;
                match result {
                    Ok(outcome) => match (outcome.dispatch, outcome.pending_mutation) {
                        (Some(dispatch), Some(mutation)) => match self.invoke(dispatch).await {
                            Ok(ServerResult::Operation { error: None, .. }) => {
                                self.broker.commit(mutation).await.map_or_else(
                                    |error| {
                                        warn!(%error, "provider value could not be committed");
                                        RefreshCompletion::Failed
                                    },
                                    |_| RefreshCompletion::Value,
                                )
                            }
                            _ => RefreshCompletion::Failed,
                        },
                        (None, None) => RefreshCompletion::Value,
                        _ => RefreshCompletion::Failed,
                    },
                    Err(error) => {
                        warn!(%error, "provider value could not be accepted");
                        RefreshCompletion::Failed
                    }
                }
            }
            Ok(ServerResult::Query {
                value: None,
                error: None,
                ..
            }) => {
                if let Some(provider_refresh_id) = dispatch.provider_refresh_id {
                    self.broker
                        .confirm_provider_miss(
                            namespace,
                            key,
                            dispatch
                                .provider_options
                                .map_or(0, |options| options.miss_ttl_seconds),
                            dispatch.mutation_generation,
                            provider_refresh_id,
                        )
                        .await;
                }
                RefreshCompletion::Miss
            }
            Ok(ServerResult::Query {
                error: Some(error), ..
            }) => {
                warn!(owner = %dispatch.owner, %error, "provider query failed");
                RefreshCompletion::Failed
            }
            _ => {
                warn!(owner = %dispatch.owner, "provider query did not return a valid result");
                RefreshCompletion::Failed
            }
        };
        if let Some(provider_refresh_id) = dispatch.provider_refresh_id {
            self.broker
                .release_provider_refresh(&route.0, &route.1, provider_refresh_id)
                .await;
        }
        self.publish_refresh_completion(&route, &state, completion);
    }

    fn publish_refresh_completion(
        &self,
        route: &RefreshRoute,
        state: &Arc<RefreshState>,
        completion: RefreshCompletion,
    ) {
        state.completion.send_replace(completion);
        self.remove_refresh_if_current(route, state);
    }

    fn remove_refresh_if_current(&self, route: &RefreshRoute, state: &Arc<RefreshState>) {
        let mut refreshes = self
            .inflight_refreshes
            .lock()
            .expect("in-flight refreshes mutex poisoned");
        if refreshes
            .get(route)
            .is_some_and(|current| Arc::ptr_eq(current, state))
        {
            refreshes.remove(route);
        }
    }

    async fn refresh_auth_from_provider(self: Arc<Self>, dispatch: Dispatch) {
        let ServerCommand::Query { key, .. } = dispatch.command.clone() else {
            return;
        };
        let api_key = key.as_str().to_owned();
        let value = match self.invoke(dispatch).await {
            Ok(ServerResult::Query {
                value, error: None, ..
            }) => value,
            _ => {
                self.broker.fail_auth_provider_load(&api_key);
                return;
            }
        };
        if let Some(delay) = self
            .broker
            .complete_auth_provider_load(api_key.clone(), value)
        {
            let server = self.clone();
            tokio::spawn(async move { server.auth_refresh_loop(api_key, delay).await });
        }
    }

    async fn auth_refresh_loop(self: Arc<Self>, api_key: String, mut delay: Duration) {
        loop {
            time::sleep(delay).await;
            if !self.broker.schedule_auth_refresh(&api_key) {
                return;
            }
            let Some(dispatch) = self.broker.auth_provider_dispatch(&api_key).await else {
                return;
            };
            let value = match self.invoke(dispatch).await {
                Ok(ServerResult::Query {
                    value, error: None, ..
                }) => value,
                _ => {
                    self.broker.fail_auth_provider_load(&api_key);
                    return;
                }
            };
            let Some(next_delay) = self
                .broker
                .complete_auth_provider_load(api_key.clone(), value)
            else {
                return;
            };
            delay = next_delay;
        }
    }

    async fn invoke(&self, dispatch: Dispatch) -> Result<ServerResult, valkyr_core::Error> {
        let request_id = request_id(&dispatch.command);
        let handle = self
            .connections
            .lock()
            .await
            .get(&dispatch.owner)
            .cloned()
            .ok_or_else(|| {
                valkyr_core::Error::Connection("registered connection has closed".into())
            })?;
        let (sender, receiver) = oneshot::channel();
        if !self
            .register_pending_result(
                &handle,
                dispatch.owner,
                request_id,
                dispatch.command.clone(),
                sender,
            )
            .await
        {
            return Err(valkyr_core::Error::Connection(
                "registered connection has closed".into(),
            ));
        }
        let encoded = encode_server_command(&dispatch.command);
        let encoded = match encoded {
            Ok(encoded) => encoded,
            Err(error) => {
                self.remove_pending_result(&handle, request_id).await;
                return Err(error);
            }
        };
        if handle.outbound.try_send(encoded).is_err() {
            self.remove_pending_result(&handle, request_id).await;
            handle.close();
            return Err(valkyr_core::Error::Connection(
                "registered connection cannot receive callbacks".into(),
            ));
        }
        match time::timeout(self.callback_timeout, receiver).await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(_)) => {
                self.remove_pending_result(&handle, request_id).await;
                Err(valkyr_core::Error::Connection(
                    "callback was cancelled".into(),
                ))
            }
            Err(_) => {
                self.remove_pending_result(&handle, request_id).await;
                Err(valkyr_core::Error::Connection("callback timed out".into()))
            }
        }
    }

    async fn complete_result(&self, result: ServerResult) {
        let request_id = match &result {
            ServerResult::Operation { request_id, .. } | ServerResult::Query { request_id, .. } => {
                *request_id
            }
        };
        let pending = { self.pending_results.lock().await.remove(&request_id) };
        if let Some(pending) = pending {
            let connection = { self.connections.lock().await.get(&pending.owner).cloned() };
            if let Some(handle) = connection {
                handle.pending.lock().await.ids.remove(&request_id);
            }
            let _ = pending.sender.send(result);
        } else {
            debug!(%request_id, "received an uncorrelated callback result");
        }
    }

    async fn register_pending_result(
        &self,
        handle: &ConnectionHandle,
        owner: ConnectionId,
        request_id: Uuid,
        command: ServerCommand,
        sender: oneshot::Sender<ServerResult>,
    ) -> bool {
        let mut pending = handle.pending.lock().await;
        if pending.closed {
            return false;
        }
        self.pending_results.lock().await.insert(
            request_id,
            PendingResult {
                owner,
                sender,
                command,
            },
        );
        pending.ids.insert(request_id);
        true
    }

    async fn remove_pending_result(&self, handle: &ConnectionHandle, request_id: Uuid) {
        self.pending_results.lock().await.remove(&request_id);
        handle.pending.lock().await.ids.remove(&request_id);
    }

    async fn remove_connection(&self, id: ConnectionId, handle: &ConnectionHandle) {
        let ids = {
            let mut pending = handle.pending.lock().await;
            pending.closed = true;
            std::mem::take(&mut pending.ids)
        };
        self.connections.lock().await.remove(&id);
        let mut pending_results = self.pending_results.lock().await;
        for request_id in ids {
            pending_results.remove(&request_id);
        }
    }

    #[cfg(test)]
    async fn pending_results_len(&self) -> usize {
        self.pending_results.lock().await.len()
    }
}

/// Load a standard PEM certificate chain and private key for [`Server::bind_tls`].
pub fn tls_config(
    certificate_path: impl AsRef<Path>,
    private_key_path: impl AsRef<Path>,
) -> io::Result<Arc<rustls::ServerConfig>> {
    install_default_crypto_provider();
    let certificate_path = certificate_path.as_ref();
    let private_key_path = private_key_path.as_ref();
    let mut certificate_reader = BufReader::new(open_tls_file(certificate_path, "certificate")?);
    let certificates =
        rustls_pemfile::certs(&mut certificate_reader).collect::<Result<Vec<_>, _>>()?;
    let mut key_reader = BufReader::new(open_tls_file(private_key_path, "private key")?);
    let private_key = rustls_pemfile::private_key(&mut key_reader)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "PEM file does not contain a private key",
        )
    })?;
    rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certificates, private_key)
        .map(Arc::new)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn open_tls_file(path: &Path, description: &str) -> io::Result<File> {
    File::open(path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "failed to read TLS {description} file {}: {error}",
                path.display()
            ),
        )
    })
}

fn install_default_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

fn encrypted_key(key: &str) -> bool {
    key.strip_prefix('~')
        .and_then(|key| key.strip_suffix('~'))
        .is_some_and(|key| !key.is_empty())
}

async fn wait_for_refresh(receiver: &mut watch::Receiver<RefreshCompletion>) -> RefreshCompletion {
    loop {
        let current = receiver.borrow().clone();
        if current != RefreshCompletion::Pending {
            return current;
        }
        if receiver.changed().await.is_err() {
            return RefreshCompletion::Failed;
        }
    }
}

fn request_id(command: &ServerCommand) -> Uuid {
    match command {
        ServerCommand::Query { request_id, .. }
        | ServerCommand::PersistSet { request_id, .. }
        | ServerCommand::PersistSetBatch { request_id, .. }
        | ServerCommand::PersistDelete { request_id, .. }
        | ServerCommand::PersistMove { request_id, .. } => *request_id,
    }
}

fn command_name(command: &Command) -> &'static str {
    match command {
        Command::Auth { .. } => "auth",
        Command::Get { .. } => "get",
        Command::Set { .. } => "set",
        Command::SetBatch { .. } => "set_batch",
        Command::Delete { .. } => "delete",
        Command::Move { .. } => "move",
        Command::Provide { .. } => "provide",
        Command::Store { .. } => "store",
        Command::Ping => "ping",
        Command::Stats => "stats",
    }
}

pub struct RunningServer {
    listener: TcpListener,
    server: Arc<Server>,
}

pub struct RunningTlsServer {
    listener: TcpListener,
    server: Arc<Server>,
    acceptor: TlsAcceptor,
}
impl RunningTlsServer {
    pub fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }
    pub async fn run(self, mut shutdown: watch::Receiver<()>) -> io::Result<()> {
        let server = self.server;
        loop {
            tokio::select! {
                changed = shutdown.changed() => match changed { Ok(()) | Err(_) => { let _ = server.connection_shutdown.send(()); return Ok(()) } },
                accepted = self.listener.accept() => {
                    let (stream, _) = accepted?;
                    let acceptor = self.acceptor.clone();
                    let server = server.clone();
                    tokio::spawn(async move {
                        if let Ok(stream) = acceptor.accept(stream).await {
                            server.handle_connection(stream).await;
                        }
                    });
                }
            }
        }
    }
}
impl RunningServer {
    pub fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }
    pub async fn run(self, mut shutdown: watch::Receiver<()>) -> io::Result<()> {
        let server = self.server;
        loop {
            tokio::select! { changed = shutdown.changed() => match changed { Ok(()) | Err(_) => { let _ = server.connection_shutdown.send(()); return Ok(()) } }, accepted = self.listener.accept() => { let (stream, _) = accepted?; tokio::spawn(server.clone().handle_connection(stream)); } }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use rustls::{
        ClientConfig, RootCertStore,
        pki_types::{PrivateKeyDer, ServerName},
    };
    use serde_json::{Value, json};
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };
    use tokio::{
        io::duplex,
        net::TcpStream,
        sync::{Notify, Semaphore},
    };
    use valkyr_client::{Client, ServerCommandHandler, StreamingClient};
    use valkyr_core::line_protocol::{decode_response, encode_command};
    use valkyr_core::{
        AuthManager, Broker, Key, KeyPattern, MemoryStore, NamespaceContext, NamespacePattern,
        ProvideOptions, StoreAuthenticator,
    };
    use valkyr_db_adapter::{DatabaseValue, ReconnectingPublisher, ValuePublisher};

    struct Provider;
    #[async_trait]
    impl ServerCommandHandler for Provider {
        async fn handle(&self, command: ServerCommand) -> ServerResult {
            match command {
                ServerCommand::Query { request_id, .. } => ServerResult::Query {
                    request_id,
                    value: Some(json!({"name":"Ada"})),
                    error: None,
                    ttl_seconds: Some(60),
                },
                ServerCommand::PersistSet { request_id, .. }
                | ServerCommand::PersistSetBatch { request_id, .. }
                | ServerCommand::PersistDelete { request_id, .. }
                | ServerCommand::PersistMove { request_id, .. } => ServerResult::Operation {
                    request_id,
                    error: None,
                },
            }
        }
    }

    struct MissProvider;
    #[async_trait]
    impl ServerCommandHandler for MissProvider {
        async fn handle(&self, command: ServerCommand) -> ServerResult {
            match command {
                ServerCommand::Query { request_id, .. } => ServerResult::Query {
                    request_id,
                    value: None,
                    error: None,
                    ttl_seconds: None,
                },
                ServerCommand::PersistSet { request_id, .. }
                | ServerCommand::PersistSetBatch { request_id, .. }
                | ServerCommand::PersistDelete { request_id, .. }
                | ServerCommand::PersistMove { request_id, .. } => ServerResult::Operation {
                    request_id,
                    error: None,
                },
            }
        }
    }

    struct BlockingCountingProvider {
        queries: AtomicUsize,
        release: Semaphore,
    }

    #[async_trait]
    impl ServerCommandHandler for BlockingCountingProvider {
        async fn handle(&self, command: ServerCommand) -> ServerResult {
            match command {
                ServerCommand::Query { request_id, .. } => {
                    self.queries.fetch_add(1, Ordering::SeqCst);
                    self.release.acquire().await.unwrap().forget();
                    ServerResult::Query {
                        request_id,
                        value: Some(json!({"name":"Ada"})),
                        error: None,
                        ttl_seconds: Some(60),
                    }
                }
                ServerCommand::PersistSet { request_id, .. }
                | ServerCommand::PersistSetBatch { request_id, .. }
                | ServerCommand::PersistDelete { request_id, .. }
                | ServerCommand::PersistMove { request_id, .. } => ServerResult::Operation {
                    request_id,
                    error: None,
                },
            }
        }
    }

    struct BlockingStorageProvider {
        started: Notify,
        release: Notify,
    }

    #[async_trait]
    impl ServerCommandHandler for BlockingStorageProvider {
        async fn handle(&self, command: ServerCommand) -> ServerResult {
            match command {
                ServerCommand::PersistSet { request_id, .. } => {
                    self.started.notify_one();
                    self.release.notified().await;
                    ServerResult::Operation {
                        request_id,
                        error: None,
                    }
                }
                ServerCommand::PersistSetBatch { request_id, .. }
                | ServerCommand::PersistDelete { request_id, .. }
                | ServerCommand::PersistMove { request_id, .. } => ServerResult::Operation {
                    request_id,
                    error: None,
                },
                ServerCommand::Query { request_id, .. } => ServerResult::Query {
                    request_id,
                    value: None,
                    error: None,
                    ttl_seconds: None,
                },
            }
        }
    }

    struct BlockingMissProvider {
        queries: AtomicUsize,
        started: Notify,
        release: Notify,
    }

    #[async_trait]
    impl ServerCommandHandler for BlockingMissProvider {
        async fn handle(&self, command: ServerCommand) -> ServerResult {
            match command {
                ServerCommand::Query { request_id, .. } => {
                    self.queries.fetch_add(1, Ordering::SeqCst);
                    self.started.notify_one();
                    self.release.notified().await;
                    ServerResult::Query {
                        request_id,
                        value: None,
                        error: None,
                        ttl_seconds: None,
                    }
                }
                ServerCommand::PersistSet { request_id, .. }
                | ServerCommand::PersistSetBatch { request_id, .. }
                | ServerCommand::PersistDelete { request_id, .. }
                | ServerCommand::PersistMove { request_id, .. } => ServerResult::Operation {
                    request_id,
                    error: None,
                },
            }
        }
    }

    struct BlockingValueStorageProvider {
        persist_calls: AtomicUsize,
    }

    #[async_trait]
    impl ServerCommandHandler for BlockingValueStorageProvider {
        async fn handle(&self, command: ServerCommand) -> ServerResult {
            match command {
                ServerCommand::Query { request_id, .. } => ServerResult::Query {
                    request_id,
                    value: Some(json!({"name": "Ada"})),
                    error: None,
                    ttl_seconds: None,
                },
                ServerCommand::PersistSet { request_id, .. } => {
                    self.persist_calls.fetch_add(1, Ordering::SeqCst);
                    ServerResult::Operation {
                        request_id,
                        error: None,
                    }
                }
                ServerCommand::PersistSetBatch { request_id, .. }
                | ServerCommand::PersistDelete { request_id, .. }
                | ServerCommand::PersistMove { request_id, .. } => ServerResult::Operation {
                    request_id,
                    error: None,
                },
            }
        }
    }

    struct FailingDurableProvider {
        persist_calls: AtomicUsize,
    }

    #[async_trait]
    impl ServerCommandHandler for FailingDurableProvider {
        async fn handle(&self, command: ServerCommand) -> ServerResult {
            match command {
                ServerCommand::Query { request_id, .. } => ServerResult::Query {
                    request_id,
                    value: Some(json!({"name": "Ada"})),
                    error: None,
                    ttl_seconds: None,
                },
                ServerCommand::PersistSet { request_id, .. }
                | ServerCommand::PersistSetBatch { request_id, .. }
                | ServerCommand::PersistDelete { request_id, .. }
                | ServerCommand::PersistMove { request_id, .. } => {
                    self.persist_calls.fetch_add(1, Ordering::SeqCst);
                    ServerResult::Operation {
                        request_id,
                        error: Some("durable write failed".into()),
                    }
                }
            }
        }
    }

    struct RetryingProvider {
        queries: AtomicUsize,
    }

    #[async_trait]
    impl ServerCommandHandler for RetryingProvider {
        async fn handle(&self, command: ServerCommand) -> ServerResult {
            match command {
                ServerCommand::Query { request_id, .. }
                    if self.queries.fetch_add(1, Ordering::SeqCst) == 0 =>
                {
                    ServerResult::Query {
                        request_id,
                        value: None,
                        error: Some("temporary provider failure".into()),
                        ttl_seconds: None,
                    }
                }
                ServerCommand::Query { request_id, .. } => ServerResult::Query {
                    request_id,
                    value: Some(json!({"name":"Ada"})),
                    error: None,
                    ttl_seconds: Some(60),
                },
                ServerCommand::PersistSet { request_id, .. }
                | ServerCommand::PersistSetBatch { request_id, .. }
                | ServerCommand::PersistDelete { request_id, .. }
                | ServerCommand::PersistMove { request_id, .. } => ServerResult::Operation {
                    request_id,
                    error: None,
                },
            }
        }
    }

    struct AuthProvider;
    #[async_trait]
    impl ServerCommandHandler for AuthProvider {
        async fn handle(&self, command: ServerCommand) -> ServerResult {
            match command {
                ServerCommand::Query { request_id, .. } => ServerResult::Query {
                    request_id,
                    value: Some(json!({
                        "client_id": "reader",
                        "name": "Reader",
                        "permissions": [{"namespace": "/people", "operations": ["read"]}]
                    })),
                    error: None,
                    ttl_seconds: None,
                },
                ServerCommand::PersistSet { request_id, .. }
                | ServerCommand::PersistSetBatch { request_id, .. }
                | ServerCommand::PersistDelete { request_id, .. }
                | ServerCommand::PersistMove { request_id, .. } => ServerResult::Operation {
                    request_id,
                    error: None,
                },
            }
        }
    }

    struct SecurityProvider;
    #[async_trait]
    impl ServerCommandHandler for SecurityProvider {
        async fn handle(&self, command: ServerCommand) -> ServerResult {
            match command {
                ServerCommand::Query { request_id, .. } => ServerResult::Query {
                    request_id,
                    value: Some(json!({
                        "key": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                        "created": 1
                    })),
                    error: None,
                    ttl_seconds: None,
                },
                ServerCommand::PersistSet { request_id, .. }
                | ServerCommand::PersistSetBatch { request_id, .. }
                | ServerCommand::PersistDelete { request_id, .. }
                | ServerCommand::PersistMove { request_id, .. } => ServerResult::Operation {
                    request_id,
                    error: None,
                },
            }
        }
    }

    fn authenticated_server() -> Server {
        let store = Arc::new(MemoryStore::new());
        let auth = AuthManager::with_bootstrap_admin(
            Arc::new(StoreAuthenticator::new(store.clone())),
            Some("bootstrap".into()),
            Duration::from_secs(60),
        );
        Server::with_broker(Broker::new(store, Some(Arc::new(auth))))
    }

    async fn authenticate_after_provider_refresh(client: &Client, api_key: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        loop {
            match client.authenticate(api_key, None).await {
                Ok(()) => return,
                Err(valkyr_client::ClientError::AuthenticationPending { .. })
                    if tokio::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                result => panic!("authentication did not refresh: {result:?}"),
            }
        }
    }

    async fn get_after_provider_refresh(
        client: &Client,
        namespace: NamespaceContext,
        key: Key,
    ) -> Value {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        loop {
            match client
                .request(Command::Get {
                    namespace: namespace.clone(),
                    key: key.clone(),
                })
                .await
            {
                Ok(Response::Value { value, .. }) => return value,
                Ok(Response::Miss { .. }) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                result => panic!("provider refresh did not return a value: {result:?}"),
            }
        }
    }

    #[test]
    fn reports_missing_tls_certificate_path() {
        let path = std::env::temp_dir().join(format!(
            "valkyr-server-missing-tls-certificate-{}",
            std::process::id()
        ));

        let error = tls_config(&path, &path).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::NotFound);
        assert!(
            error
                .to_string()
                .contains("failed to read TLS certificate file")
        );
        assert!(error.to_string().contains(path.to_string_lossy().as_ref()));
    }

    #[test]
    fn terminal_refresh_completion_is_retained_without_receivers() {
        let (sender, _) = watch::channel(RefreshCompletion::Pending);
        sender.send_replace(RefreshCompletion::Value);
        assert_eq!(*sender.borrow(), RefreshCompletion::Value);
        let receiver = sender.subscribe();
        assert_eq!(*receiver.borrow(), RefreshCompletion::Value);
    }

    #[test]
    fn server_publishes_value_completion_before_refresh_removal() {
        let server = Arc::new(Server::in_memory());
        let route = (
            NamespaceContext::new("/values").unwrap(),
            Key::new("value").unwrap(),
        );
        let (completion, _) = watch::channel(RefreshCompletion::Pending);
        let state = Arc::new(RefreshState {
            completion,
            broker_refresh_id: None,
        });
        server
            .inflight_refreshes
            .lock()
            .unwrap()
            .insert(route.clone(), state.clone());

        server.publish_refresh_completion(&route, &state, RefreshCompletion::Value);

        let receiver = state.completion.subscribe();
        assert_eq!(*receiver.borrow(), RefreshCompletion::Value);
        assert!(server.inflight_refreshes.lock().unwrap().is_empty());
    }

    #[test]
    fn server_publishes_miss_completion_before_refresh_removal() {
        let server = Arc::new(Server::in_memory());
        let route = (
            NamespaceContext::new("/values").unwrap(),
            Key::new("missing").unwrap(),
        );
        let (completion, _) = watch::channel(RefreshCompletion::Pending);
        let state = Arc::new(RefreshState {
            completion,
            broker_refresh_id: None,
        });
        server
            .inflight_refreshes
            .lock()
            .unwrap()
            .insert(route.clone(), state.clone());

        server.publish_refresh_completion(&route, &state, RefreshCompletion::Miss);

        let receiver = state.completion.subscribe();
        assert_eq!(*receiver.borrow(), RefreshCompletion::Miss);
        assert!(server.inflight_refreshes.lock().unwrap().is_empty());
    }

    #[test]
    fn stale_refresh_completion_cannot_remove_a_replacement_generation() {
        let server = Arc::new(Server::in_memory());
        let route = (
            NamespaceContext::new("/values").unwrap(),
            Key::new("replacement").unwrap(),
        );
        let (old_completion, _) = watch::channel(RefreshCompletion::Pending);
        let old_state = Arc::new(RefreshState {
            completion: old_completion,
            broker_refresh_id: None,
        });
        let (new_completion, _) = watch::channel(RefreshCompletion::Pending);
        let new_state = Arc::new(RefreshState {
            completion: new_completion,
            broker_refresh_id: None,
        });
        server
            .inflight_refreshes
            .lock()
            .unwrap()
            .insert(route.clone(), new_state.clone());

        server.publish_refresh_completion(&route, &old_state, RefreshCompletion::Value);

        let refreshes = server.inflight_refreshes.lock().unwrap();
        assert!(
            refreshes
                .get(&route)
                .is_some_and(|state| Arc::ptr_eq(state, &new_state))
        );
    }

    #[test]
    fn connection_capacities_default_and_can_be_overridden() {
        let defaults = Server::in_memory();
        assert_eq!(defaults.outbound_capacity, DEFAULT_OUTBOUND_CAPACITY);
        assert_eq!(defaults.command_capacity, DEFAULT_COMMAND_CAPACITY);

        let configured = Server::in_memory()
            .with_outbound_capacity(3)
            .with_command_capacity(2);
        assert_eq!(configured.outbound_capacity, 3);
        assert_eq!(configured.command_capacity, 2);
    }

    #[tokio::test]
    async fn bounded_command_channel_processes_a_burst_in_order() {
        let (shutdown_sender, shutdown_receiver) = watch::channel(());
        let running = Server::in_memory()
            .with_command_capacity(1)
            .bind("127.0.0.1:0")
            .await
            .unwrap();
        let address = running.local_addr().unwrap();
        let task = tokio::spawn(running.run(shutdown_receiver));
        let mut client = Framed::new(
            TcpStream::connect(address).await.unwrap(),
            LinesCodec::new(),
        );
        let command = encode_command(&Command::Ping).unwrap();

        for _ in 0..65 {
            client.send(command.clone()).await.unwrap();
        }
        for _ in 0..65 {
            let line = client.next().await.unwrap().unwrap();
            assert!(matches!(
                decode_response(&Command::Ping, &line).unwrap(),
                Response::Pong
            ));
        }

        shutdown_sender.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn full_outbound_channel_closes_the_connection() {
        let server = Arc::new(Server::in_memory().with_outbound_capacity(1));
        let id = Uuid::new_v4();
        let (outbound, _receiver) = mpsc::channel(1);
        outbound.try_send("queued".into()).unwrap();
        let (shutdown, mut shutdown_receiver) = watch::channel(());
        server.connections.lock().await.insert(
            id,
            Arc::new(ConnectionHandle {
                outbound,
                adapter_instance: Mutex::new(None),
                pending: Mutex::new(PendingRequests::default()),
                shutdown,
            }),
        );
        let dispatch = Dispatch {
            owner: id,
            provider_id: None,
            provider_refresh_id: None,
            mutation_generation: 0,
            command: ServerCommand::Query {
                request_id: Uuid::new_v4(),
                namespace: NamespaceContext::new("/people").unwrap(),
                key: Key::new("ada").unwrap(),
            },
            authentication: false,
            provider_options: None,
            encrypted: false,
        };

        assert!(server.invoke(dispatch).await.is_err());
        tokio::time::timeout(Duration::from_millis(100), shutdown_receiver.changed())
            .await
            .unwrap()
            .unwrap();
        assert!(server.pending_results.lock().await.is_empty());
    }

    #[tokio::test]
    async fn timed_out_callback_removes_its_pending_result() {
        let server = Arc::new(Server::in_memory().with_callback_timeout(Duration::from_millis(10)));
        let id = Uuid::new_v4();
        let (outbound, _receiver) = mpsc::channel(1);
        let (shutdown, _) = watch::channel(());
        server.connections.lock().await.insert(
            id,
            Arc::new(ConnectionHandle {
                outbound,
                adapter_instance: Mutex::new(None),
                pending: Mutex::new(PendingRequests::default()),
                shutdown,
            }),
        );
        let dispatch = Dispatch {
            owner: id,
            provider_id: None,
            provider_refresh_id: None,
            mutation_generation: 0,
            command: ServerCommand::Query {
                request_id: Uuid::new_v4(),
                namespace: NamespaceContext::new("/people").unwrap(),
                key: Key::new("ada").unwrap(),
            },
            authentication: false,
            provider_options: None,
            encrypted: false,
        };

        assert!(server.invoke(dispatch).await.is_err());
        assert_eq!(server.pending_results_len().await, 0);
    }

    #[tokio::test]
    async fn callback_completion_releases_global_locks_before_connection_lock() {
        let server = Arc::new(Server::in_memory());
        let owner = Uuid::new_v4();
        let request_id = Uuid::new_v4();
        let (outbound, _receiver) = mpsc::channel(1);
        let (shutdown, _) = watch::channel(());
        let handle = Arc::new(ConnectionHandle {
            outbound,
            adapter_instance: Mutex::new(None),
            pending: Mutex::new(PendingRequests {
                ids: HashSet::from([request_id]),
                closed: false,
            }),
            shutdown,
        });
        server
            .connections
            .lock()
            .await
            .insert(owner, handle.clone());
        let (sender, _receiver) = oneshot::channel();
        server.pending_results.lock().await.insert(
            request_id,
            PendingResult {
                owner,
                sender,
                command: ServerCommand::Query {
                    request_id,
                    namespace: NamespaceContext::new("/people").unwrap(),
                    key: Key::new("ada").unwrap(),
                },
            },
        );

        let connection_pending = handle.pending.lock().await;
        let completion = tokio::spawn({
            let server = server.clone();
            async move {
                server
                    .complete_result(ServerResult::Query {
                        request_id,
                        value: None,
                        error: None,
                        ttl_seconds: None,
                    })
                    .await;
            }
        });
        time::timeout(Duration::from_secs(1), async {
            loop {
                if let Ok(pending_results) = server.pending_results.try_lock() {
                    if !pending_results.contains_key(&request_id) {
                        break;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("callback completion retained the pending-results lock");
        let connections = time::timeout(Duration::from_millis(100), server.connections.lock())
            .await
            .expect("callback completion retained the connections lock");
        drop(connections);

        drop(connection_pending);
        completion.await.unwrap();
    }

    #[tokio::test]
    async fn closing_connection_purges_its_pending_results() {
        let server = Arc::new(Server::in_memory().with_callback_timeout(Duration::from_secs(1)));
        let (stream, client_stream) = duplex(1024);
        let session = tokio::spawn(server.clone().handle_connection(stream));
        let (id, handle) = loop {
            if let Some((id, handle)) = server
                .connections
                .lock()
                .await
                .iter()
                .next()
                .map(|(id, handle)| (*id, handle.clone()))
            {
                break (id, handle);
            }
            tokio::task::yield_now().await;
        };
        let callback = tokio::spawn({
            let server = server.clone();
            async move {
                server
                    .invoke(Dispatch {
                        owner: id,
                        provider_id: None,
                        provider_refresh_id: None,
                        mutation_generation: 0,
                        command: ServerCommand::Query {
                            request_id: Uuid::new_v4(),
                            namespace: NamespaceContext::new("/people").unwrap(),
                            key: Key::new("ada").unwrap(),
                        },
                        authentication: false,
                        provider_options: None,
                        encrypted: false,
                    })
                    .await
            }
        });
        let mut client = Framed::new(client_stream, LinesCodec::new());
        assert!(client.next().await.is_some());

        drop(client);
        session.await.unwrap();
        assert!(callback.await.unwrap().is_err());
        assert_eq!(server.pending_results_len().await, 0);
        assert!(handle.pending.lock().await.ids.is_empty());
    }

    #[tokio::test]
    async fn malformed_callback_result_closes_and_cleans_pending() {
        let server = Arc::new(Server::in_memory().with_callback_timeout(Duration::from_secs(1)));
        let (stream, client_stream) = duplex(1024);
        let session = tokio::spawn(server.clone().handle_connection(stream));
        let id = loop {
            if let Some(id) = server.connections.lock().await.keys().next().copied() {
                break id;
            }
            tokio::task::yield_now().await;
        };
        let callback = tokio::spawn({
            let server = server.clone();
            async move {
                server
                    .invoke(Dispatch {
                        owner: id,
                        provider_id: None,
                        provider_refresh_id: None,
                        mutation_generation: 0,
                        command: ServerCommand::Query {
                            request_id: Uuid::new_v4(),
                            namespace: NamespaceContext::new("/people").unwrap(),
                            key: Key::new("ada").unwrap(),
                        },
                        authentication: false,
                        provider_options: None,
                        encrypted: false,
                    })
                    .await
            }
        });
        let mut client = Framed::new(client_stream, LinesCodec::new());
        assert!(client.next().await.is_some());
        client.send("OPERATION not-a-uuid OK").await.unwrap();
        session.await.unwrap();
        assert!(callback.await.unwrap().is_err());
        assert_eq!(server.pending_results_len().await, 0);
    }

    #[tokio::test]
    async fn unknown_adapter_callback_keyword_closes_session() {
        let server = Arc::new(Server::in_memory());
        let (stream, client_stream) = duplex(1024);
        let session = tokio::spawn(server.clone().handle_connection(stream));
        let handle = loop {
            if let Some(handle) = server.connections.lock().await.values().next().cloned() {
                break handle;
            }
            tokio::task::yield_now().await;
        };
        *handle.adapter_instance.lock().await = Some(Uuid::new_v4());
        let mut client = Framed::new(client_stream, LinesCodec::new());
        client
            .send("FUTURE_RESULT 00000000-0000-0000-0000-000000000001 OK")
            .await
            .unwrap();
        session.await.unwrap();
        assert!(server.connections.lock().await.is_empty());
    }

    #[tokio::test]
    async fn connection_close_signal_removes_connection() {
        let server = Arc::new(Server::in_memory());
        let (stream, _client) = duplex(64);
        let task = tokio::spawn(server.clone().handle_connection(stream));
        let handle = loop {
            if let Some(handle) = server.connections.lock().await.values().next().cloned() {
                break handle;
            }
            tokio::task::yield_now().await;
        };

        handle.close();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        assert!(server.connections.lock().await.is_empty());
    }

    #[tokio::test]
    async fn disconnect_waits_for_an_inflight_storage_commit() {
        let (shutdown_sender, shutdown_receiver) = watch::channel(());
        let server = Arc::new(authenticated_server());
        let running = server.clone().bind_shared("127.0.0.1:0").await.unwrap();
        let address = running.local_addr().unwrap();
        let task = tokio::spawn(running.run(shutdown_receiver));
        let storage = Arc::new(BlockingStorageProvider {
            started: Notify::new(),
            release: Notify::new(),
        });
        let adapter =
            StreamingClient::connect(address, "bootstrap", Uuid::new_v4(), storage.clone())
                .await
                .unwrap();
        adapter
            .store(
                NamespacePattern::new("/people").unwrap(),
                KeyPattern::new("*").unwrap(),
            )
            .await
            .unwrap();

        let mut client = Framed::new(
            TcpStream::connect(address).await.unwrap(),
            LinesCodec::new(),
        );
        client
            .send(
                encode_command(&Command::Auth {
                    api_key: "bootstrap".into(),
                    adapter_instance: None,
                })
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(
            decode_response(
                &Command::Auth {
                    api_key: "bootstrap".into(),
                    adapter_instance: None
                },
                &client.next().await.unwrap().unwrap()
            )
            .unwrap(),
            Response::AuthSuccess { .. }
        ));
        client
            .send(
                encode_command(&Command::Set {
                    namespace: NamespaceContext::new("/people").unwrap(),
                    key: Key::new("ada").unwrap(),
                    value: json!({"name": "Ada"}),
                    ttl_seconds: None,
                })
                .unwrap(),
            )
            .await
            .unwrap();
        time::timeout(Duration::from_secs(1), storage.started.notified())
            .await
            .unwrap();
        drop(client);
        time::timeout(Duration::from_secs(1), async {
            while server.connections.lock().await.len() != 1 {
                time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        storage.release.notify_one();

        let auth = match server.authenticate("bootstrap").await.unwrap() {
            AuthenticationResult::Authenticated(auth) => auth,
            _ => panic!("bootstrap authentication should succeed"),
        };
        let response = time::timeout(Duration::from_secs(1), async {
            loop {
                if let Response::Value { value, .. } = server
                    .execute(
                        Command::Get {
                            namespace: NamespaceContext::new("/people").unwrap(),
                            key: Key::new("ada").unwrap(),
                        },
                        Some(auth.clone()),
                    )
                    .await
                    .unwrap()
                {
                    break value;
                }
                time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(response, json!({"name": "Ada"}));

        shutdown_sender.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn accepts_a_client_round_trip() {
        let (shutdown_sender, shutdown_receiver) = watch::channel(());
        let running = authenticated_server().bind("127.0.0.1:0").await.unwrap();
        let address = running.local_addr().unwrap();
        let task = tokio::spawn(running.run(shutdown_receiver));
        let unauthenticated = Client::connect(address).await.unwrap();
        assert!(
            unauthenticated
                .set(
                    NamespaceContext::new("/people").unwrap(),
                    Key::new("unauthenticated").unwrap(),
                    json!("denied"),
                    None,
                )
                .await
                .is_err()
        );
        let client = Client::connect(address).await.unwrap();
        client.authenticate("bootstrap", None).await.unwrap();
        client
            .set(
                NamespaceContext::new("/people").unwrap(),
                Key::new("ada").unwrap(),
                json!({"name":"Ada"}),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            client
                .get(
                    NamespaceContext::new("/people").unwrap(),
                    Key::new("ada").unwrap()
                )
                .await
                .unwrap(),
            json!({"name":"Ada"})
        );
        client
            .set(
                NamespaceContext::new("/people").unwrap(),
                Key::new("selected").unwrap(),
                json!("ada"),
                None,
            )
            .await
            .unwrap();
        client
            .set(
                NamespaceContext::new("/people").unwrap(),
                Key::new("profile.ada").unwrap(),
                json!({"id":"ada"}),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            client
                .get(
                    NamespaceContext::new("/people").unwrap(),
                    Key::new("profile.${selected}").unwrap(),
                )
                .await
                .unwrap(),
            json!({"id":"ada"})
        );
        shutdown_sender.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn registry_authentication_works_over_the_native_listener() {
        let store = Arc::new(MemoryStore::new());
        let auth = AuthManager::with_bootstrap_admin(
            Arc::new(StoreAuthenticator::new(store.clone())),
            Some("bootstrap".into()),
            Duration::from_secs(60),
        );
        let server = Arc::new(Server::with_broker(Broker::new(
            store,
            Some(Arc::new(auth)),
        )));
        let (shutdown_sender, shutdown_receiver) = watch::channel(());
        let running = server.bind_shared("127.0.0.1:0").await.unwrap();
        let address = running.local_addr().unwrap();
        let task = tokio::spawn(running.run(shutdown_receiver));
        let bootstrap = Client::connect(address).await.unwrap();
        bootstrap.authenticate("bootstrap", None).await.unwrap();
        let provider =
            StreamingClient::connect(address, "bootstrap", Uuid::new_v4(), Arc::new(AuthProvider))
                .await
                .unwrap();
        provider
            .provide(
                NamespacePattern::new("/__auth").unwrap(),
                KeyPattern::new("reader-key").unwrap(),
                None,
            )
            .await
            .unwrap();
        let reader = Client::connect(address).await.unwrap();
        assert!(matches!(
            reader.authenticate("reader-key", None).await,
            Err(valkyr_client::ClientError::AuthenticationPending { .. })
        ));
        authenticate_after_provider_refresh(&reader, "reader-key").await;
        shutdown_sender.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn auth_record_update_applies_to_an_open_native_session() {
        let (shutdown_sender, shutdown_receiver) = watch::channel(());
        let running = authenticated_server().bind("127.0.0.1:0").await.unwrap();
        let address = running.local_addr().unwrap();
        let task = tokio::spawn(running.run(shutdown_receiver));
        let bootstrap = Client::connect(address).await.unwrap();
        bootstrap.authenticate("bootstrap", None).await.unwrap();
        let auth_namespace = NamespaceContext::new("/__auth").unwrap();
        let consumer_key = Key::new("consumer-key").unwrap();
        bootstrap
            .set(
                auth_namespace.clone(),
                consumer_key.clone(),
                json!({
                    "client_id": "consumer",
                    "name": "Consumer",
                    "permissions": [{"namespace": "/old", "operations": ["read"]}]
                }),
                None,
            )
            .await
            .unwrap();
        let consumer = Client::connect(address).await.unwrap();
        consumer.authenticate("consumer-key", None).await.unwrap();
        assert!(matches!(
            consumer
                .request(Command::Get {
                    namespace: NamespaceContext::new("/old").unwrap(),
                    key: Key::new("entry").unwrap(),
                })
                .await,
            Ok(Response::Unknown)
        ));

        bootstrap
            .set(
                auth_namespace,
                consumer_key,
                json!({
                    "client_id": "consumer",
                    "name": "Consumer",
                    "permissions": [{"namespace": "/new", "operations": ["read"]}]
                }),
                None,
            )
            .await
            .unwrap();

        assert!(matches!(
            consumer
                .request(Command::Get {
                    namespace: NamespaceContext::new("/old").unwrap(),
                    key: Key::new("entry").unwrap(),
                })
                .await,
            Err(valkyr_client::ClientError::Server(_))
        ));
        assert!(matches!(
            consumer
                .request(Command::Get {
                    namespace: NamespaceContext::new("/new").unwrap(),
                    key: Key::new("entry").unwrap(),
                })
                .await,
            Ok(Response::Unknown)
        ));
        shutdown_sender.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn provider_callback_refreshes_a_cache_miss() {
        let (shutdown_sender, shutdown_receiver) = watch::channel(());
        let running = authenticated_server().bind("127.0.0.1:0").await.unwrap();
        let address = running.local_addr().unwrap();
        let task = tokio::spawn(running.run(shutdown_receiver));
        let provider =
            StreamingClient::connect(address, "bootstrap", Uuid::new_v4(), Arc::new(Provider))
                .await
                .unwrap();
        provider
            .provide(
                NamespacePattern::new("/people").unwrap(),
                KeyPattern::new("ada").unwrap(),
                None,
            )
            .await
            .unwrap();
        let client = Client::connect(address).await.unwrap();
        client.authenticate("bootstrap", None).await.unwrap();
        assert!(matches!(
            client
                .request(Command::Get {
                    namespace: NamespaceContext::new("/people").unwrap(),
                    key: Key::new("ada").unwrap()
                })
                .await
                .unwrap(),
            Response::Miss { .. }
        ));
        assert_eq!(
            get_after_provider_refresh(
                &client,
                NamespaceContext::new("/people").unwrap(),
                Key::new("ada").unwrap(),
            )
            .await,
            json!({"name":"Ada"})
        );
        shutdown_sender.send(()).unwrap();
        task.await.unwrap().unwrap();
        for _ in 0..10 {
            if provider.is_closed() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(provider.is_closed());
    }

    #[tokio::test]
    async fn provider_wait_returns_value_after_shared_refresh_acceptance() {
        let (shutdown_sender, shutdown_receiver) = watch::channel(());
        let running = authenticated_server().bind("127.0.0.1:0").await.unwrap();
        let address = running.local_addr().unwrap();
        let task = tokio::spawn(running.run(shutdown_receiver));
        let provider =
            StreamingClient::connect(address, "bootstrap", Uuid::new_v4(), Arc::new(Provider))
                .await
                .unwrap();
        provider
            .provide_with_options(
                NamespacePattern::new("/people").unwrap(),
                KeyPattern::new("ada").unwrap(),
                ProvideOptions::new().with_timeout_ms(500),
            )
            .await
            .unwrap();
        let client = Client::connect(address).await.unwrap();
        client.authenticate("bootstrap", None).await.unwrap();
        assert_eq!(
            client
                .get(
                    NamespaceContext::new("/people").unwrap(),
                    Key::new("ada").unwrap()
                )
                .await
                .unwrap(),
            json!({"name":"Ada"})
        );
        shutdown_sender.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn provider_wait_returns_a_clean_miss_and_cleans_up_for_maximum_ttl() {
        let server = Arc::new(authenticated_server());
        let (shutdown_sender, shutdown_receiver) = watch::channel(());
        let running = server.clone().bind_shared("127.0.0.1:0").await.unwrap();
        let address = running.local_addr().unwrap();
        let task = tokio::spawn(running.run(shutdown_receiver));
        let provider =
            StreamingClient::connect(address, "bootstrap", Uuid::new_v4(), Arc::new(MissProvider))
                .await
                .unwrap();
        provider
            .provide_with_options(
                NamespacePattern::new("/people").unwrap(),
                KeyPattern::new("missing").unwrap(),
                ProvideOptions::new()
                    .with_timeout_ms(500)
                    .with_miss_ttl_seconds(u64::MAX),
            )
            .await
            .unwrap();
        let client = Client::connect(address).await.unwrap();
        client.authenticate("bootstrap", None).await.unwrap();
        assert!(matches!(
            client
                .request(Command::Get {
                    namespace: NamespaceContext::new("/people").unwrap(),
                    key: Key::new("missing").unwrap(),
                })
                .await
                .unwrap(),
            Response::Miss { retry_after_ms: 0 }
        ));
        assert!(server.inflight_refreshes.lock().unwrap().is_empty());
        shutdown_sender.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn concurrent_cache_misses_share_one_provider_refresh() {
        let server = Arc::new(authenticated_server());
        let (shutdown_sender, shutdown_receiver) = watch::channel(());
        let running = server.clone().bind_shared("127.0.0.1:0").await.unwrap();
        let address = running.local_addr().unwrap();
        let task = tokio::spawn(running.run(shutdown_receiver));
        let handler = Arc::new(BlockingCountingProvider {
            queries: AtomicUsize::new(0),
            release: Semaphore::new(0),
        });
        let provider =
            StreamingClient::connect(address, "bootstrap", Uuid::new_v4(), handler.clone())
                .await
                .unwrap();
        provider
            .provide(
                NamespacePattern::new("/people").unwrap(),
                KeyPattern::new("ada").unwrap(),
                None,
            )
            .await
            .unwrap();
        let mut clients = Vec::new();
        for _ in 0..8 {
            let client = Client::connect(address).await.unwrap();
            client.authenticate("bootstrap", None).await.unwrap();
            clients.push(client);
        }

        let responses = futures_util::future::join_all(clients.iter().map(|client| {
            client.request(Command::Get {
                namespace: NamespaceContext::new("/people").unwrap(),
                key: Key::new("ada").unwrap(),
            })
        }))
        .await;
        assert!(
            responses
                .iter()
                .all(|response| matches!(response, Ok(Response::Miss { .. })))
        );
        for _ in 0..20 {
            if handler.queries.load(Ordering::SeqCst) == 1 {
                break;
            }
            time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(handler.queries.load(Ordering::SeqCst), 1);

        handler.release.add_permits(1);
        let value = time::timeout(Duration::from_secs(1), async {
            loop {
                if let Ok(value) = clients[0]
                    .get(
                        NamespaceContext::new("/people").unwrap(),
                        Key::new("ada").unwrap(),
                    )
                    .await
                {
                    break value;
                }
                time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(value, json!({"name":"Ada"}));
        assert_eq!(handler.queries.load(Ordering::SeqCst), 1);

        shutdown_sender.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn joining_waiters_do_not_consume_rate_limit_capacity() {
        let server = Arc::new(authenticated_server());
        let (shutdown_sender, shutdown_receiver) = watch::channel(());
        let running = server.clone().bind_shared("127.0.0.1:0").await.unwrap();
        let address = running.local_addr().unwrap();
        let task = tokio::spawn(running.run(shutdown_receiver));
        let handler = Arc::new(BlockingCountingProvider {
            queries: AtomicUsize::new(0),
            release: Semaphore::new(0),
        });
        let first_provider =
            StreamingClient::connect(address, "bootstrap", Uuid::new_v4(), handler.clone())
                .await
                .unwrap();
        first_provider
            .provide_with_options(
                NamespacePattern::new("/people").unwrap(),
                KeyPattern::new("*").unwrap(),
                ProvideOptions::new()
                    .with_max_rate(Some(1))
                    .with_timeout_ms(0),
            )
            .await
            .unwrap();
        let second_provider =
            StreamingClient::connect(address, "bootstrap", Uuid::new_v4(), handler.clone())
                .await
                .unwrap();
        second_provider
            .provide_with_options(
                NamespacePattern::new("/people").unwrap(),
                KeyPattern::new("*").unwrap(),
                ProvideOptions::new()
                    .with_max_rate(Some(1))
                    .with_timeout_ms(500),
            )
            .await
            .unwrap();
        let client = Client::connect(address).await.unwrap();
        client.authenticate("bootstrap", None).await.unwrap();

        assert!(matches!(
            client
                .request(Command::Get {
                    namespace: NamespaceContext::new("/people").unwrap(),
                    key: Key::new("ada").unwrap(),
                })
                .await
                .unwrap(),
            Response::Miss { .. }
        ));
        time::timeout(Duration::from_secs(1), async {
            while handler.queries.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let waiting = client.request(Command::Get {
            namespace: NamespaceContext::new("/people").unwrap(),
            key: Key::new("ada").unwrap(),
        });
        handler.release.add_permits(1);
        assert!(matches!(waiting.await.unwrap(), Response::Value { .. }));

        assert!(matches!(
            client
                .request(Command::Get {
                    namespace: NamespaceContext::new("/people").unwrap(),
                    key: Key::new("other").unwrap(),
                })
                .await
                .unwrap(),
            Response::Miss { retry_after_ms } if retry_after_ms > 0
        ));
        assert!(matches!(
            client
                .request(Command::Get {
                    namespace: NamespaceContext::new("/people").unwrap(),
                    key: Key::new("another").unwrap(),
                })
                .await
                .unwrap(),
            Response::Miss { .. }
        ));
        assert_eq!(handler.queries.load(Ordering::SeqCst), 2);

        shutdown_sender.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn rate_limited_refreshes_reclaim_server_and_broker_state() {
        let broker = Broker::new(Arc::new(MemoryStore::new()), None);
        let provider_id = Uuid::new_v4();
        broker
            .registry()
            .register_provider(
                provider_id,
                "/values",
                "*",
                ProvideOptions::new().with_max_rate(Some(1)),
            )
            .await;
        let server = Arc::new(Server::with_broker(broker));

        assert_eq!(
            server
                .broker
                .provider_retry_after(Some(provider_id), Some(1))
                .await,
            None
        );

        for index in 0..128 {
            let namespace = NamespaceContext::new("/values").unwrap();
            let key = Key::new(format!("missing-{index}")).unwrap();
            let outcome = server
                .broker
                .execute(
                    Command::Get { namespace, key },
                    valkyr_core::RequestContext::anonymous(Uuid::new_v4()),
                )
                .await
                .unwrap();
            assert!(matches!(
                server.finish_outcome(outcome).await.unwrap(),
                Response::Miss { retry_after_ms } if retry_after_ms > 0
            ));
        }

        assert!(server.inflight_refreshes.lock().unwrap().is_empty());
        assert_eq!(server.broker.active_route_count().await, 0);
    }

    #[tokio::test]
    async fn rate_limited_joiners_reclaim_replaced_broker_state_before_publication() {
        let broker = Broker::new(Arc::new(MemoryStore::new()), None);
        let provider_id = Uuid::new_v4();
        let provider = broker
            .registry()
            .register_provider(
                provider_id,
                "/values",
                "*",
                ProvideOptions::new().with_max_rate(Some(1)),
            )
            .await;
        let pause = Arc::new(RateLimitedReleasePause {
            released: Notify::new(),
            continue_publication: Notify::new(),
        });
        let server =
            Arc::new(Server::with_broker(broker).with_rate_limited_release_pause(pause.clone()));

        assert!(
            server
                .broker
                .provider_retry_after(Some(provider.id), Some(1))
                .await
                .is_none()
        );

        for index in 0..128 {
            let namespace = NamespaceContext::new("/values").unwrap();
            let key = Key::new(format!("interleaved-{index}")).unwrap();
            let outcome = server
                .broker
                .execute(
                    Command::Get {
                        namespace: namespace.clone(),
                        key: key.clone(),
                    },
                    valkyr_core::RequestContext::anonymous(Uuid::new_v4()),
                )
                .await
                .unwrap();
            let first_refresh_id = outcome
                .dispatch
                .as_ref()
                .and_then(|dispatch| dispatch.provider_refresh_id)
                .expect("provider admission creates a refresh identity");
            let completion = tokio::spawn({
                let server = server.clone();
                async move { server.finish_outcome(outcome).await.unwrap() }
            });

            pause.released.notified().await;
            assert_eq!(server.broker.active_route_count().await, 0);
            assert_eq!(server.inflight_refreshes.lock().unwrap().len(), 1);

            let joiner = server
                .broker
                .execute(
                    Command::Get {
                        namespace: namespace.clone(),
                        key: key.clone(),
                    },
                    valkyr_core::RequestContext::anonymous(Uuid::new_v4()),
                )
                .await
                .unwrap();
            let replacement_refresh_id = joiner
                .dispatch
                .as_ref()
                .and_then(|dispatch| dispatch.provider_refresh_id)
                .expect("joiner creates replacement route state");
            assert_ne!(replacement_refresh_id, first_refresh_id);
            assert!(matches!(
                server.finish_outcome(joiner).await.unwrap(),
                Response::Miss { .. }
            ));
            assert_eq!(server.broker.active_route_count().await, 0);
            assert_eq!(server.inflight_refreshes.lock().unwrap().len(), 1);

            pause.continue_publication.notify_one();
            assert!(matches!(completion.await.unwrap(), Response::Miss { .. }));
            assert!(server.inflight_refreshes.lock().unwrap().is_empty());
            assert_eq!(server.broker.active_route_count().await, 0);
        }
    }

    #[tokio::test]
    async fn zero_timeout_rate_limited_reads_keep_the_window_retry_hint() {
        let server = Arc::new(authenticated_server());
        let (shutdown_sender, shutdown_receiver) = watch::channel(());
        let running = server.clone().bind_shared("127.0.0.1:0").await.unwrap();
        let address = running.local_addr().unwrap();
        let task = tokio::spawn(running.run(shutdown_receiver));
        let handler = Arc::new(BlockingCountingProvider {
            queries: AtomicUsize::new(0),
            release: Semaphore::new(0),
        });
        let storage =
            StreamingClient::connect(address, "bootstrap", Uuid::new_v4(), handler.clone())
                .await
                .unwrap();
        storage
            .provide_with_options(
                NamespacePattern::new("/people").unwrap(),
                KeyPattern::new("*").unwrap(),
                ProvideOptions::new()
                    .with_max_rate(Some(1))
                    .with_timeout_ms(0),
            )
            .await
            .unwrap();
        let client = Client::connect(address).await.unwrap();
        client.authenticate("bootstrap", None).await.unwrap();
        assert!(matches!(
            client
                .request(Command::Get {
                    namespace: NamespaceContext::new("/people").unwrap(),
                    key: Key::new("first").unwrap(),
                })
                .await
                .unwrap(),
            Response::Miss { .. }
        ));
        time::timeout(Duration::from_secs(1), async {
            while handler.queries.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        handler.release.add_permits(1);
        time::timeout(Duration::from_secs(1), async {
            while !server.inflight_refreshes.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let response = client
            .request(Command::Get {
                namespace: NamespaceContext::new("/people").unwrap(),
                key: Key::new("second").unwrap(),
            })
            .await
            .unwrap();
        assert!(matches!(response, Response::Miss { retry_after_ms } if retry_after_ms > 10));

        shutdown_sender.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn slow_clean_misses_finish_after_a_bounded_wait_and_cache_afterward() {
        let server = Arc::new(authenticated_server());
        let (shutdown_sender, shutdown_receiver) = watch::channel(());
        let running = server.clone().bind_shared("127.0.0.1:0").await.unwrap();
        let address = running.local_addr().unwrap();
        let task = tokio::spawn(running.run(shutdown_receiver));
        let handler = Arc::new(BlockingMissProvider {
            queries: AtomicUsize::new(0),
            started: Notify::new(),
            release: Notify::new(),
        });
        let provider =
            StreamingClient::connect(address, "bootstrap", Uuid::new_v4(), handler.clone())
                .await
                .unwrap();
        provider
            .provide_with_options(
                NamespacePattern::new("/people").unwrap(),
                KeyPattern::new("missing").unwrap(),
                ProvideOptions::new()
                    .with_timeout_ms(40)
                    .with_miss_ttl_seconds(60),
            )
            .await
            .unwrap();
        let client = Client::connect(address).await.unwrap();
        client.authenticate("bootstrap", None).await.unwrap();
        let started = time::Instant::now();
        let response = client
            .request(Command::Get {
                namespace: NamespaceContext::new("/people").unwrap(),
                key: Key::new("missing").unwrap(),
            })
            .await
            .unwrap();
        assert!(matches!(response, Response::Miss { .. }));
        assert!(started.elapsed() >= Duration::from_millis(25));
        handler.release.notify_one();
        time::timeout(Duration::from_secs(1), async {
            while !server.inflight_refreshes.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(matches!(
            client
                .request(Command::Get {
                    namespace: NamespaceContext::new("/people").unwrap(),
                    key: Key::new("missing").unwrap(),
                })
                .await
                .unwrap(),
            Response::Miss { retry_after_ms: 0 }
        ));
        assert_eq!(handler.queries.load(Ordering::SeqCst), 1);
        shutdown_sender.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn waiting_refresh_handles_callback_timeout_and_disconnect() {
        let server =
            Arc::new(authenticated_server().with_callback_timeout(Duration::from_millis(20)));
        let (shutdown_sender, shutdown_receiver) = watch::channel(());
        let running = server.clone().bind_shared("127.0.0.1:0").await.unwrap();
        let address = running.local_addr().unwrap();
        let task = tokio::spawn(running.run(shutdown_receiver));
        let timeout_handler = Arc::new(BlockingMissProvider {
            queries: AtomicUsize::new(0),
            started: Notify::new(),
            release: Notify::new(),
        });
        let timeout_provider = StreamingClient::connect(
            address,
            "bootstrap",
            Uuid::new_v4(),
            timeout_handler.clone(),
        )
        .await
        .unwrap();
        timeout_provider
            .provide_with_options(
                NamespacePattern::new("/timeout").unwrap(),
                KeyPattern::new("value").unwrap(),
                ProvideOptions::new().with_timeout_ms(200),
            )
            .await
            .unwrap();
        let client = Client::connect(address).await.unwrap();
        client.authenticate("bootstrap", None).await.unwrap();
        assert!(matches!(
            client
                .request(Command::Get {
                    namespace: NamespaceContext::new("/timeout").unwrap(),
                    key: Key::new("value").unwrap(),
                })
                .await
                .unwrap(),
            Response::Miss { .. }
        ));
        time::timeout(Duration::from_secs(1), async {
            while !server.inflight_refreshes.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        timeout_handler.release.notify_one();
        drop(timeout_provider);

        let disconnect_handler = Arc::new(BlockingMissProvider {
            queries: AtomicUsize::new(0),
            started: Notify::new(),
            release: Notify::new(),
        });
        let disconnect_provider = StreamingClient::connect(
            address,
            "bootstrap",
            Uuid::new_v4(),
            disconnect_handler.clone(),
        )
        .await
        .unwrap();
        disconnect_provider
            .provide_with_options(
                NamespacePattern::new("/disconnect").unwrap(),
                KeyPattern::new("value").unwrap(),
                ProvideOptions::new().with_timeout_ms(200),
            )
            .await
            .unwrap();
        let response = tokio::spawn({
            async move {
                client
                    .request(Command::Get {
                        namespace: NamespaceContext::new("/disconnect").unwrap(),
                        key: Key::new("value").unwrap(),
                    })
                    .await
            }
        });
        time::timeout(
            Duration::from_secs(1),
            disconnect_handler.started.notified(),
        )
        .await
        .unwrap();
        drop(disconnect_provider);
        assert!(matches!(
            response.await.unwrap().unwrap(),
            Response::Miss { .. }
        ));

        shutdown_sender.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn adapter_provider_value_is_published_without_durable_acceptance() {
        let server = Arc::new(authenticated_server());
        let (shutdown_sender, shutdown_receiver) = watch::channel(());
        let running = server.clone().bind_shared("127.0.0.1:0").await.unwrap();
        let address = running.local_addr().unwrap();
        let task = tokio::spawn(running.run(shutdown_receiver));
        let handler = Arc::new(BlockingValueStorageProvider {
            persist_calls: AtomicUsize::new(0),
        });
        let storage =
            StreamingClient::connect(address, "bootstrap", Uuid::new_v4(), handler.clone())
                .await
                .unwrap();
        storage
            .store(
                NamespacePattern::new("/people").unwrap(),
                KeyPattern::new("ada").unwrap(),
            )
            .await
            .unwrap();
        let provider =
            StreamingClient::connect(address, "bootstrap", Uuid::new_v4(), Arc::new(Provider))
                .await
                .unwrap();
        provider
            .provide_with_options(
                NamespacePattern::new("/people").unwrap(),
                KeyPattern::new("ada").unwrap(),
                ProvideOptions::new().with_timeout_ms(500),
            )
            .await
            .unwrap();
        let client = Client::connect(address).await.unwrap();
        client.authenticate("bootstrap", None).await.unwrap();
        let request = client
            .request(Command::Get {
                namespace: NamespaceContext::new("/people").unwrap(),
                key: Key::new("ada").unwrap(),
            })
            .await
            .unwrap();
        assert!(
            matches!(request, Response::Value { value, .. } if value == json!({"name": "Ada"}))
        );
        assert_eq!(handler.persist_calls.load(Ordering::SeqCst), 0);

        shutdown_sender.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn adapter_provider_value_is_cached_even_when_durability_would_fail() {
        let server = Arc::new(authenticated_server());
        let (shutdown_sender, shutdown_receiver) = watch::channel(());
        let running = server.clone().bind_shared("127.0.0.1:0").await.unwrap();
        let address = running.local_addr().unwrap();
        let task = tokio::spawn(running.run(shutdown_receiver));
        let failing_provider = Arc::new(FailingDurableProvider {
            persist_calls: AtomicUsize::new(0),
        });
        let provider = StreamingClient::connect(
            address,
            "bootstrap",
            Uuid::new_v4(),
            failing_provider.clone(),
        )
        .await
        .unwrap();
        provider
            .store(
                NamespacePattern::new("/people").unwrap(),
                KeyPattern::new("ada").unwrap(),
            )
            .await
            .unwrap();
        let provider =
            StreamingClient::connect(address, "bootstrap", Uuid::new_v4(), Arc::new(Provider))
                .await
                .unwrap();
        provider
            .provide_with_options(
                NamespacePattern::new("/people").unwrap(),
                KeyPattern::new("ada").unwrap(),
                ProvideOptions::new().with_timeout_ms(100),
            )
            .await
            .unwrap();
        let client = Client::connect(address).await.unwrap();
        client.authenticate("bootstrap", None).await.unwrap();
        assert!(matches!(
            client
                .request(Command::Get {
                    namespace: NamespaceContext::new("/people").unwrap(),
                    key: Key::new("ada").unwrap(),
                })
                .await
                .unwrap(),
            Response::Value { value, .. } if value == json!({"name": "Ada"})
        ));
        assert!(
            server
                .broker
                .store()
                .get(
                    &NamespaceContext::new("/people").unwrap(),
                    &Key::new("ada").unwrap()
                )
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(failing_provider.persist_calls.load(Ordering::SeqCst), 0);
        assert!(server.inflight_refreshes.lock().unwrap().is_empty());

        shutdown_sender.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn encrypted_waiting_provider_values_are_decrypted_after_commit() {
        let (shutdown_sender, shutdown_receiver) = watch::channel(());
        let running = authenticated_server().bind("127.0.0.1:0").await.unwrap();
        let address = running.local_addr().unwrap();
        let task = tokio::spawn(running.run(shutdown_receiver));
        let security = StreamingClient::connect(
            address,
            "bootstrap",
            Uuid::new_v4(),
            Arc::new(SecurityProvider),
        )
        .await
        .unwrap();
        security
            .provide(
                NamespacePattern::new("/__secrets").unwrap(),
                KeyPattern::new("*").unwrap(),
                None,
            )
            .await
            .unwrap();
        let provider =
            StreamingClient::connect(address, "bootstrap", Uuid::new_v4(), Arc::new(Provider))
                .await
                .unwrap();
        provider
            .provide_with_options(
                NamespacePattern::new("/people").unwrap(),
                KeyPattern::new("voice").unwrap(),
                ProvideOptions::new().with_timeout_ms(500),
            )
            .await
            .unwrap();
        let client = Client::connect(address).await.unwrap();
        client.authenticate("bootstrap", None).await.unwrap();
        assert_eq!(
            client
                .get(
                    NamespaceContext::new("/people").unwrap(),
                    Key::new("~voice~").unwrap()
                )
                .await
                .unwrap(),
            json!({"name": "Ada"})
        );
        shutdown_sender.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn committed_value_wins_over_a_terminal_provider_miss() {
        let server = Arc::new(authenticated_server());
        let (shutdown_sender, shutdown_receiver) = watch::channel(());
        let running = server.clone().bind_shared("127.0.0.1:0").await.unwrap();
        let address = running.local_addr().unwrap();
        let task = tokio::spawn(running.run(shutdown_receiver));
        let handler = Arc::new(BlockingMissProvider {
            queries: AtomicUsize::new(0),
            started: Notify::new(),
            release: Notify::new(),
        });
        let provider =
            StreamingClient::connect(address, "bootstrap", Uuid::new_v4(), handler.clone())
                .await
                .unwrap();
        provider
            .provide_with_options(
                NamespacePattern::new("/people").unwrap(),
                KeyPattern::new("ada").unwrap(),
                ProvideOptions::new()
                    .with_timeout_ms(500)
                    .with_miss_ttl_seconds(60),
            )
            .await
            .unwrap();
        let waiter = Client::connect(address).await.unwrap();
        waiter.authenticate("bootstrap", None).await.unwrap();
        let request = tokio::spawn(async move {
            waiter
                .request(Command::Get {
                    namespace: NamespaceContext::new("/people").unwrap(),
                    key: Key::new("ada").unwrap(),
                })
                .await
        });
        time::timeout(Duration::from_secs(1), handler.started.notified())
            .await
            .unwrap();
        let setter = Client::connect(address).await.unwrap();
        setter.authenticate("bootstrap", None).await.unwrap();
        setter
            .set(
                NamespaceContext::new("/people").unwrap(),
                Key::new("ada").unwrap(),
                json!({"name": "committed"}),
                None,
            )
            .await
            .unwrap();
        handler.release.notify_one();
        assert!(matches!(
            request.await.unwrap().unwrap(),
            Response::Value { value, .. } if value == json!({"name": "committed"})
        ));
        setter
            .delete(
                NamespaceContext::new("/people").unwrap(),
                Some(KeyPattern::new("ada").unwrap()),
            )
            .await
            .unwrap();
        let second = tokio::spawn(async move {
            setter
                .request(Command::Get {
                    namespace: NamespaceContext::new("/people").unwrap(),
                    key: Key::new("ada").unwrap(),
                })
                .await
        });
        time::timeout(Duration::from_secs(1), handler.started.notified())
            .await
            .unwrap();
        handler.release.notify_one();
        assert!(matches!(
            second.await.unwrap().unwrap(),
            Response::Miss { .. }
        ));
        assert_eq!(handler.queries.load(Ordering::SeqCst), 2);

        shutdown_sender.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn failed_provider_refresh_allows_a_later_retry() {
        let server = Arc::new(authenticated_server());
        let (shutdown_sender, shutdown_receiver) = watch::channel(());
        let running = server.clone().bind_shared("127.0.0.1:0").await.unwrap();
        let address = running.local_addr().unwrap();
        let task = tokio::spawn(running.run(shutdown_receiver));
        let handler = Arc::new(RetryingProvider {
            queries: AtomicUsize::new(0),
        });
        let provider =
            StreamingClient::connect(address, "bootstrap", Uuid::new_v4(), handler.clone())
                .await
                .unwrap();
        provider
            .provide(
                NamespacePattern::new("/people").unwrap(),
                KeyPattern::new("ada").unwrap(),
                None,
            )
            .await
            .unwrap();
        let client = Client::connect(address).await.unwrap();
        client.authenticate("bootstrap", None).await.unwrap();

        assert!(matches!(
            client
                .request(Command::Get {
                    namespace: NamespaceContext::new("/people").unwrap(),
                    key: Key::new("ada").unwrap(),
                })
                .await
                .unwrap(),
            Response::Miss { .. }
        ));
        for _ in 0..20 {
            if handler.queries.load(Ordering::SeqCst) == 1
                && server.inflight_refreshes.lock().unwrap().is_empty()
            {
                break;
            }
            time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(handler.queries.load(Ordering::SeqCst), 1);
        assert!(server.inflight_refreshes.lock().unwrap().is_empty());

        assert!(matches!(
            client
                .request(Command::Get {
                    namespace: NamespaceContext::new("/people").unwrap(),
                    key: Key::new("ada").unwrap(),
                })
                .await
                .unwrap(),
            Response::Miss { .. }
        ));
        let value = time::timeout(Duration::from_secs(1), async {
            loop {
                if let Ok(value) = client
                    .get(
                        NamespaceContext::new("/people").unwrap(),
                        Key::new("ada").unwrap(),
                    )
                    .await
                {
                    break value;
                }
                time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(value, json!({"name":"Ada"}));
        assert_eq!(handler.queries.load(Ordering::SeqCst), 2);

        shutdown_sender.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn encrypted_values_use_the_registered_security_provider() {
        let (shutdown_sender, shutdown_receiver) = watch::channel(());
        let running = authenticated_server().bind("127.0.0.1:0").await.unwrap();
        let address = running.local_addr().unwrap();
        let task = tokio::spawn(running.run(shutdown_receiver));
        let provider = StreamingClient::connect(
            address,
            "bootstrap",
            Uuid::new_v4(),
            Arc::new(SecurityProvider),
        )
        .await
        .unwrap();
        provider
            .provide(
                NamespacePattern::new("/__secrets").unwrap(),
                KeyPattern::new("*").unwrap(),
                None,
            )
            .await
            .unwrap();
        let client = Client::connect(address).await.unwrap();
        client.authenticate("bootstrap", None).await.unwrap();
        let namespace = NamespaceContext::new("/people").unwrap();
        client
            .set(
                namespace.clone(),
                Key::new("~voice~").unwrap(),
                json!("hello"),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            client
                .get(namespace, Key::new("~voice~").unwrap())
                .await
                .unwrap(),
            json!("hello")
        );
        shutdown_sender.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn bootstrap_can_warm_security_keys_for_encrypted_values() {
        let (shutdown_sender, shutdown_receiver) = watch::channel(());
        let running = authenticated_server().bind("127.0.0.1:0").await.unwrap();
        let address = running.local_addr().unwrap();
        let task = tokio::spawn(running.run(shutdown_receiver));
        let client = Client::connect(address).await.unwrap();
        client.authenticate("bootstrap", None).await.unwrap();
        client
            .set(
                NamespaceContext::new("/__secrets").unwrap(),
                Key::new("/people").unwrap(),
                json!({"key": "11".repeat(32), "created": 1}),
                None,
            )
            .await
            .unwrap();
        let namespace = NamespaceContext::new("/people").unwrap();
        client
            .set(
                namespace.clone(),
                Key::new("~voice~").unwrap(),
                json!("hello"),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            client
                .get(namespace, Key::new("~voice~").unwrap())
                .await
                .unwrap(),
            json!("hello")
        );
        shutdown_sender.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn tls_listener_serves_native_protocol() {
        install_default_crypto_provider();
        let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let certificate_der = certificate.cert.der().clone();
        let private_key = PrivateKeyDer::Pkcs8(certificate.key_pair.serialize_der().into());
        let server_tls = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![certificate_der.clone()], private_key)
            .unwrap();
        let (shutdown_sender, shutdown_receiver) = watch::channel(());
        let running = Arc::new(Server::in_memory())
            .bind_tls("127.0.0.1:0", Arc::new(server_tls))
            .await
            .unwrap();
        let address = running.local_addr().unwrap();
        let task = tokio::spawn(running.run(shutdown_receiver));

        let mut roots = RootCertStore::empty();
        roots.add(certificate_der).unwrap();
        let client_tls = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let connection = Client::connect_tls_with_server_name(
            &address.to_string(),
            ServerName::try_from("localhost").unwrap(),
            Arc::new(client_tls),
        )
        .await
        .unwrap();
        connection.ping().await.unwrap();
        shutdown_sender.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn websocket_session_authenticates_and_round_trips_values() {
        use tokio_tungstenite::{connect_async, tungstenite::Message};

        let server = Arc::new(authenticated_server());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown_sender, mut shutdown_receiver) = watch::channel(());
        let task = tokio::spawn(async move {
            axum::serve(listener, server.http_router())
                .with_graceful_shutdown(async move {
                    let _ = shutdown_receiver.changed().await;
                })
                .await
        });
        let (mut socket, _) = connect_async(format!("ws://{address}/ws")).await.unwrap();

        async fn send_command(
            socket: &mut tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<TcpStream>,
            >,
            command: Command,
        ) -> Response {
            socket
                .send(Message::Text(encode_command(&command).unwrap().into()))
                .await
                .unwrap();
            let message = socket.next().await.unwrap().unwrap();
            decode_response(&command, &message.into_text().unwrap()).unwrap()
        }

        assert!(matches!(
            send_command(
                &mut socket,
                Command::Auth {
                    api_key: "bootstrap".into(),
                    adapter_instance: None,
                },
            )
            .await,
            Response::AuthSuccess { .. }
        ));
        assert!(matches!(
            send_command(
                &mut socket,
                Command::Set {
                    namespace: NamespaceContext::new("/people").unwrap(),
                    key: Key::new("ada").unwrap(),
                    value: json!({"name": "Ada"}),
                    ttl_seconds: None,
                },
            )
            .await,
            Response::Ok
        ));
        assert!(matches!(
            send_command(
                &mut socket,
                Command::Get {
                    namespace: NamespaceContext::new("/people").unwrap(),
                    key: Key::new("ada").unwrap(),
                },
            )
            .await,
            Response::Value { value, .. } if value == json!({"name": "Ada"})
        ));

        shutdown_sender.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn reconnecting_publisher_recovers_after_server_restart() {
        let (shutdown_sender, shutdown_receiver) = watch::channel(());
        let running = authenticated_server().bind("127.0.0.1:0").await.unwrap();
        let address = running.local_addr().unwrap();
        let task = tokio::spawn(running.run(shutdown_receiver));
        let publisher = ReconnectingPublisher::new(
            valkyr_client::ClientBuilder::new()
                .server(address.to_string())
                .api_key("bootstrap"),
        );
        publisher
            .publish(DatabaseValue {
                namespace: NamespaceContext::new("/people").unwrap(),
                key: Key::new("ada").unwrap(),
                value: json!({"name": "Ada"}),
                ttl: None,
            })
            .await
            .unwrap();
        shutdown_sender.send(()).unwrap();
        task.await.unwrap().unwrap();

        let (shutdown_sender, shutdown_receiver) = watch::channel(());
        let running = authenticated_server().bind(address).await.unwrap();
        let task = tokio::spawn(running.run(shutdown_receiver));
        publisher
            .publish(DatabaseValue {
                namespace: NamespaceContext::new("/people").unwrap(),
                key: Key::new("grace").unwrap(),
                value: json!({"name": "Grace"}),
                ttl: None,
            })
            .await
            .unwrap();
        let client = Client::connect(address).await.unwrap();
        client.authenticate("bootstrap", None).await.unwrap();
        assert_eq!(
            client
                .get(
                    NamespaceContext::new("/people").unwrap(),
                    Key::new("grace").unwrap()
                )
                .await
                .unwrap(),
            json!({"name": "Grace"})
        );
        shutdown_sender.send(()).unwrap();
        task.await.unwrap().unwrap();
    }
}
