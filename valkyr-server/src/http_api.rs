use crate::{AuthenticationResult, Server};
use axum::{
    Router,
    body::to_bytes,
    extract::{Request, State, ws::WebSocketUpgrade},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{any, get},
};
use serde_json::Value;
use std::sync::Arc;
use tower_http::trace::TraceLayer;
use valkyr_core::{Command, Key, KeyPattern, NamespaceContext, Response as CoreResponse};

pub fn router(server: Arc<Server>) -> Router {
    Router::new()
        .route("/ws", get(websocket))
        .fallback(any(rest))
        .layer(TraceLayer::new_for_http())
        .with_state(server)
}

pub fn metrics_router(server: Arc<Server>) -> Router {
    Router::new()
        .route("/metrics", get(metrics))
        .route("/health", get(health))
        .layer(TraceLayer::new_for_http())
        .with_state(server)
}

async fn health() -> StatusCode {
    StatusCode::OK
}

async fn metrics(State(server): State<Arc<Server>>) -> Response {
    match server.metrics_text().await {
        Ok(metrics) => (
            StatusCode::OK,
            [(
                header::CONTENT_TYPE,
                "text/plain; version=0.0.4; charset=utf-8",
            )],
            metrics,
        )
            .into_response(),
        Err(metrics_error) => error(StatusCode::INTERNAL_SERVER_ERROR, metrics_error.to_string()),
    }
}

async fn rest(State(server): State<Arc<Server>>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let Some(api_key) = bearer(&parts.headers) else {
        return error(StatusCode::UNAUTHORIZED, "missing bearer token");
    };
    let auth = match server.authenticate(api_key).await {
        Ok(AuthenticationResult::Authenticated(auth)) => auth,
        Ok(AuthenticationResult::Pending(retry_after_ms)) => return auth_pending(retry_after_ms),
        Ok(AuthenticationResult::Rejected) | Err(_) => {
            return error(StatusCode::UNAUTHORIZED, "invalid bearer token");
        }
    };
    let namespace = match percent_decode(parts.uri.path())
        .and_then(|path| {
            path.starts_with('/')
                .then_some(path)
                .ok_or("namespace must be an absolute path")
        })
        .and_then(|path| NamespaceContext::new(path).map_err(|_| "invalid namespace"))
    {
        Ok(namespace) => namespace,
        Err(message) => return error(StatusCode::BAD_REQUEST, message),
    };
    let query = match key_query(parts.uri.query()) {
        Ok(value) => value,
        Err(message) => return error(StatusCode::BAD_REQUEST, message),
    };
    let command = match parts.method.as_str() {
        "GET" => {
            let Some(key) = query.and_then(|key| Key::new(key).ok()) else {
                return error(
                    StatusCode::BAD_REQUEST,
                    "GET requires a key query parameter",
                );
            };
            Command::Get { namespace, key }
        }
        "PUT" if parts.headers.contains_key("destination") => {
            if query.is_some() {
                return error(
                    StatusCode::BAD_REQUEST,
                    "PUT with Destination does not accept a key query parameter",
                );
            }
            let body = match to_bytes(body, 1024 * 1024).await {
                Ok(body) if body.is_empty() => body,
                Ok(_) => {
                    return error(
                        StatusCode::BAD_REQUEST,
                        "PUT with Destination does not accept a body",
                    );
                }
                Err(_) => return error(StatusCode::PAYLOAD_TOO_LARGE, "request body is too large"),
            };
            drop(body);
            let destination = match destination_namespace(&parts.headers) {
                Ok(destination) if destination != namespace => destination,
                Ok(_) => {
                    return error(
                        StatusCode::BAD_REQUEST,
                        "source and destination namespaces must differ",
                    );
                }
                Err(message) => return error(StatusCode::BAD_REQUEST, message),
            };
            Command::Move {
                source: namespace,
                destination,
            }
        }
        "PUT" => {
            let Some(key) = query.and_then(|key| Key::new(key).ok()) else {
                return error(
                    StatusCode::BAD_REQUEST,
                    "PUT requires a key query parameter",
                );
            };
            let content_type = parts
                .headers
                .get(header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.split(';').next())
                .map(str::trim);
            if !content_type.is_some_and(|value| value.eq_ignore_ascii_case("application/json")) {
                return error(
                    StatusCode::BAD_REQUEST,
                    "Content-Type must be application/json",
                );
            }
            let body = match to_bytes(body, 1024 * 1024).await {
                Ok(body) => body,
                Err(_) => return error(StatusCode::PAYLOAD_TOO_LARGE, "request body is too large"),
            };
            let value: Value = match serde_json::from_slice(&body) {
                Ok(value) => value,
                Err(_) => return error(StatusCode::BAD_REQUEST, "PUT body must be JSON"),
            };
            let ttl_seconds = match parts.headers.get("valkyr-ttl") {
                Some(value) => match value.to_str().ok().and_then(|value| value.parse().ok()) {
                    Some(value) if value > 0 => Some(value),
                    _ => {
                        return error(
                            StatusCode::BAD_REQUEST,
                            "Valkyr-Ttl must be a positive integer",
                        );
                    }
                },
                None => None,
            };
            Command::Set {
                namespace,
                key,
                value,
                ttl_seconds,
            }
        }
        "DELETE" => Command::Delete {
            namespace,
            key_pattern: query.map(KeyPattern::new).transpose().unwrap_or(None),
        },
        _ => return error(StatusCode::METHOD_NOT_ALLOWED, "unsupported method"),
    };
    match server.execute_text(command, Some(auth)).await {
        Ok(response) => rest_response(response),
        Err(error) => core_error(error),
    }
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    if headers.get_all(header::AUTHORIZATION).iter().count() != 1 {
        return None;
    }
    let mut parts = headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .split_ascii_whitespace();
    let scheme = parts.next()?;
    let token = parts.next()?;
    (scheme.eq_ignore_ascii_case("bearer") && !token.is_empty() && parts.next().is_none())
        .then_some(token)
}

fn key_query(raw_query: Option<&str>) -> Result<Option<String>, &'static str> {
    let Some(raw_query) = raw_query else {
        return Ok(None);
    };
    if raw_query.is_empty() {
        return Err("key query must not be empty");
    }
    if raw_query.contains(['&', '=']) {
        return Err("key query delimiters must be percent-encoded");
    }
    let key = percent_decode(raw_query)?;
    if key.is_empty() {
        return Err("key query must not be empty");
    }
    Ok(Some(key))
}

fn destination_namespace(headers: &HeaderMap) -> Result<NamespaceContext, &'static str> {
    let values = headers.get_all("destination");
    if values.iter().count() != 1 {
        return Err("Destination must appear exactly once");
    }
    let raw = values
        .iter()
        .next()
        .and_then(|value| value.to_str().ok())
        .ok_or("Destination must be a valid header")?;
    if !raw.starts_with('/') || raw.contains(['?', '#']) {
        return Err("Destination must be an absolute namespace path");
    }
    NamespaceContext::new(percent_decode(raw)?).map_err(|_| "invalid Destination header")
}

fn percent_decode(input: &str) -> Result<String, &'static str> {
    let bytes = input.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'%' {
            decoded.push(bytes[index]);
            index += 1;
            continue;
        }
        let Some((&high, &low)) = bytes.get(index + 1).zip(bytes.get(index + 2)) else {
            return Err("invalid percent encoding");
        };
        let Some(high) = hex_value(high) else {
            return Err("invalid percent encoding");
        };
        let Some(low) = hex_value(low) else {
            return Err("invalid percent encoding");
        };
        decoded.push((high << 4) | low);
        index += 3;
    }
    String::from_utf8(decoded).map_err(|_| "invalid percent encoding")
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn rest_response(response: CoreResponse) -> Response {
    match response {
        CoreResponse::Ok => StatusCode::NO_CONTENT.into_response(),
        CoreResponse::Value { value, .. } => axum::Json(value).into_response(),
        CoreResponse::Pong | CoreResponse::Stats(_) | CoreResponse::AuthSuccess { .. } => {
            axum::Json(response).into_response()
        }
        CoreResponse::Miss { .. } | CoreResponse::Unknown => {
            error(StatusCode::NOT_FOUND, "value not found")
        }
        CoreResponse::AuthFailure { message } => error(StatusCode::UNAUTHORIZED, message),
        CoreResponse::AuthPending { retry_after_ms } => auth_pending(retry_after_ms),
        CoreResponse::Error { message } => error(StatusCode::BAD_REQUEST, message),
    }
}
fn auth_pending(retry_after_ms: u64) -> Response {
    let mut response = error(StatusCode::SERVICE_UNAVAILABLE, "authentication is warming");
    let retry_after_seconds = retry_after_ms.div_ceil(1000).max(1);
    response.headers_mut().insert(
        header::RETRY_AFTER,
        retry_after_seconds
            .to_string()
            .parse()
            .expect("valid retry header"),
    );
    response
}
fn core_error(err: valkyr_core::Error) -> Response {
    let status = match err {
        valkyr_core::Error::AuthenticationFailed => StatusCode::UNAUTHORIZED,
        valkyr_core::Error::PermissionDenied(_) => StatusCode::FORBIDDEN,
        valkyr_core::Error::NotFound(_) => StatusCode::NOT_FOUND,
        _ => StatusCode::BAD_REQUEST,
    };
    error(status, err.to_string())
}
fn error(status: StatusCode, message: impl Into<String>) -> Response {
    let mut response = (
        status,
        axum::Json(serde_json::json!({"error": {
            "code": error_code(status),
            "message": message.into()
        }})),
    )
        .into_response();
    if status == StatusCode::UNAUTHORIZED {
        response.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            "Bearer".parse().expect("valid header"),
        );
    }
    if status == StatusCode::METHOD_NOT_ALLOWED {
        response.headers_mut().insert(
            header::ALLOW,
            "GET, PUT, DELETE".parse().expect("valid header"),
        );
    }
    response
}

fn error_code(status: StatusCode) -> &'static str {
    match status {
        StatusCode::UNAUTHORIZED => "unauthorized",
        StatusCode::FORBIDDEN => "forbidden",
        StatusCode::NOT_FOUND => "not_found",
        StatusCode::METHOD_NOT_ALLOWED => "method_not_allowed",
        StatusCode::PAYLOAD_TOO_LARGE => "payload_too_large",
        StatusCode::INTERNAL_SERVER_ERROR => "internal_error",
        _ => "invalid_request",
    }
}

async fn websocket(
    State(server): State<Arc<Server>>,
    upgrade: WebSocketUpgrade,
) -> impl IntoResponse {
    upgrade.on_upgrade(move |socket| server.handle_websocket(socket))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Method, Request},
    };
    use std::time::Duration;
    use valkyr_core::{AuthManager, Broker, MemoryStore, StoreAuthenticator};

    fn authenticated_server() -> Arc<Server> {
        let store = Arc::new(MemoryStore::new());
        let auth = AuthManager::with_bootstrap_admin(
            Arc::new(StoreAuthenticator::new(store.clone())),
            Some("development-key".into()),
            Duration::from_secs(60),
        );
        Arc::new(Server::with_broker(Broker::new(
            store,
            Some(Arc::new(auth)),
        )))
    }

    fn request(method: Method, uri: &str, body: Body) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header(header::AUTHORIZATION, "Bearer development-key")
            .header(header::CONTENT_TYPE, "application/json")
            .body(body)
            .unwrap()
    }

    #[tokio::test]
    async fn rest_round_trip_move_delete_and_ttl_validation() {
        let server = authenticated_server();
        let put = rest(
            State(server.clone()),
            request(
                Method::PUT,
                "/people::active?ada",
                Body::from(r#"{"name":"Ada"}"#),
            ),
        )
        .await;
        assert_eq!(put.status(), StatusCode::NO_CONTENT);
        let get = rest(
            State(server.clone()),
            request(Method::GET, "/people::active?ada", Body::empty()),
        )
        .await;
        assert_eq!(get.status(), StatusCode::OK);
        assert_eq!(
            to_bytes(get.into_body(), 1024).await.unwrap(),
            r#"{"name":"Ada"}"#
        );

        let invalid_ttl = Request::builder()
            .method(Method::PUT)
            .uri("/people?ada")
            .header(header::AUTHORIZATION, "Bearer development-key")
            .header(header::CONTENT_TYPE, "application/json")
            .header("valkyr-ttl", "forever")
            .body(Body::from("null"))
            .unwrap();
        assert_eq!(
            rest(State(server.clone()), invalid_ttl).await.status(),
            StatusCode::BAD_REQUEST
        );

        let move_request = Request::builder()
            .method(Method::PUT)
            .uri("/people::active")
            .header(header::AUTHORIZATION, "Bearer development-key")
            .header("destination", "/people::archived")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            rest(State(server.clone()), move_request).await.status(),
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            rest(
                State(server.clone()),
                request(Method::GET, "/people::archived?ada", Body::empty())
            )
            .await
            .status(),
            StatusCode::OK
        );
        assert_eq!(
            rest(
                State(server.clone()),
                request(Method::DELETE, "/people::archived?ada", Body::empty())
            )
            .await
            .status(),
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            rest(
                State(server),
                request(Method::GET, "/people::archived?ada", Body::empty())
            )
            .await
            .status(),
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn rest_requires_a_bearer_token() {
        let request = Request::builder()
            .uri("/people?ada")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            rest(State(Arc::new(Server::in_memory())), request)
                .await
                .status(),
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn cached_auth_record_authenticates_the_first_consumer_request() {
        let server = authenticated_server();
        let auth_record = rest(
            State(server.clone()),
            request(
                Method::PUT,
                "/__auth?consumer-key",
                Body::from(
                    r#"{"client_id":"consumer","name":"Consumer","permissions":[{"namespace":"/people","operations":["read"]}]}"#,
                ),
            ),
        )
        .await;
        assert_eq!(auth_record.status(), StatusCode::NO_CONTENT);
        let value = rest(
            State(server.clone()),
            request(Method::PUT, "/people?ada", Body::from(r#"{"name":"Ada"}"#)),
        )
        .await;
        assert_eq!(value.status(), StatusCode::NO_CONTENT);

        let consumer_request = Request::builder()
            .method(Method::GET)
            .uri("/people?ada")
            .header(header::AUTHORIZATION, "Bearer consumer-key")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            rest(State(server), consumer_request).await.status(),
            StatusCode::OK
        );
    }

    #[test]
    fn pending_authentication_is_a_retryable_service_unavailable_response() {
        let response = rest_response(CoreResponse::AuthPending { retry_after_ms: 10 });
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()[header::RETRY_AFTER], "1");
    }

    #[test]
    fn percent_decoding_keeps_plus_as_a_literal_character() {
        assert_eq!(percent_decode("key%2E%E2%98%83+").unwrap(), "key.☃+");
        assert!(percent_decode("%XZ").is_err());
        assert!(key_query(Some("key&other")).is_err());
    }

    #[tokio::test]
    async fn metrics_use_prometheus_text_without_authentication() {
        let server = Arc::new(Server::in_memory());
        server
            .execute(Command::Ping, None)
            .await
            .expect("ping succeeds");
        let response = metrics(State(server)).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        let body = std::str::from_utf8(&body).unwrap();
        assert!(body.contains("valkyr_requests_total 1"));
        assert!(body.contains("valkyr_active_connections"));
    }

    #[tokio::test]
    async fn health_returns_ok_without_authentication() {
        assert_eq!(health().await, StatusCode::OK);
    }
}
