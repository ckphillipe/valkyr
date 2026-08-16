use super::*;
use crate::encode;
use serde_json::{Value, json};
use std::{collections::HashMap, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
    task::JoinHandle,
};
use valkyr_core::{KeyPattern, NamespaceContext};

#[derive(Clone, Debug)]
struct StubResponse {
    status: u16,
    body: String,
}

impl StubResponse {
    fn json(status: u16, body: Value) -> Self {
        Self {
            status,
            body: body.to_string(),
        }
    }

    fn ok() -> Self {
        Self::json(200, json!({}))
    }

    fn not_found() -> Self {
        Self::json(404, json!({"errors": []}))
    }
}

#[derive(Clone, Debug)]
struct CapturedRequest {
    method: String,
    path: String,
    body: String,
}

struct HttpFixture {
    address: String,
    requests: Arc<Mutex<Vec<CapturedRequest>>>,
    task: JoinHandle<()>,
}

impl HttpFixture {
    async fn start(mut routes: HashMap<(String, String), StubResponse>) -> Self {
        routes.insert(
            ("POST".into(), "/v1/auth/approle/login".into()),
            StubResponse::json(200, json!({"auth": {"client_token": "fixture-token"}})),
        );
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let routes = Arc::new(routes);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let task_routes = Arc::clone(&routes);
        let task_requests = Arc::clone(&requests);
        let task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let routes = Arc::clone(&task_routes);
                let requests = Arc::clone(&task_requests);
                tokio::spawn(async move {
                    serve_request(stream, routes, requests).await;
                });
            }
        });
        Self {
            address,
            requests,
            task,
        }
    }

    async fn requests(&self) -> Vec<CapturedRequest> {
        self.requests.lock().await.clone()
    }
}

impl Drop for HttpFixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve_request(
    mut stream: TcpStream,
    routes: Arc<HashMap<(String, String), StubResponse>>,
    requests: Arc<Mutex<Vec<CapturedRequest>>>,
) {
    let mut bytes = Vec::new();
    let header_end = loop {
        if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break index;
        }
        if stream.read_buf(&mut bytes).await.unwrap_or(0) == 0 {
            return;
        }
    };
    let headers = String::from_utf8_lossy(&bytes[..header_end]);
    let mut lines = headers.lines();
    let Some(request_line) = lines.next() else {
        return;
    };
    let mut request_parts = request_line.split_whitespace();
    let (Some(method), Some(path)) = (request_parts.next(), request_parts.next()) else {
        return;
    };
    let method = method.to_owned();
    let path = path.to_owned();
    let content_length = lines
        .find_map(|line| {
            line.strip_prefix("Content-Length:")
                .or_else(|| line.strip_prefix("content-length:"))
                .and_then(|value| value.trim().parse::<usize>().ok())
        })
        .unwrap_or(0);
    let body_start = header_end + 4;
    while bytes.len() < body_start + content_length {
        if stream.read_buf(&mut bytes).await.unwrap_or(0) == 0 {
            return;
        }
    }
    let body = String::from_utf8_lossy(&bytes[body_start..body_start + content_length]).into();
    let request = CapturedRequest { method, path, body };
    requests.lock().await.push(request.clone());
    let response = routes
        .get(&(request.method, request.path))
        .cloned()
        .unwrap_or_else(StubResponse::not_found);
    let reason = match response.status {
        200 => "OK",
        204 => "No Content",
        404 => "Not Found",
        _ => "Error",
    };
    let output = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        response.status,
        reason,
        response.body.len(),
        response.body
    );
    let _ = stream.write_all(output.as_bytes()).await;
}

fn client(fixture: &HttpFixture) -> OpenBaoClient {
    OpenBaoClient::new(
        &fixture.address,
        "kv".into(),
        Duration::from_secs(2),
        crate::AppRole {
            role_id: "role".into(),
            secret_id: "secret".into(),
        },
        None,
    )
    .unwrap()
}

fn wire(kind: &str, path: &str) -> String {
    format!("/v1/kv/{kind}/{}", path.replace('%', "%25"))
}

fn auth_read(path: &str, document: Value) -> ((String, String), StubResponse) {
    (
        ("GET".into(), wire("data", path)),
        StubResponse::json(
            200,
            json!({"data": {"data": document, "metadata": {"version": 1}}}),
        ),
    )
}

fn list_response(path: &str, keys: Vec<String>) -> ((String, String), StubResponse) {
    (
        ("LIST".into(), wire("metadata", path)),
        StubResponse::json(200, json!({"data": {"keys": keys}})),
    )
}

fn auth_namespace() -> NamespaceContext {
    NamespaceContext::new("/__auth").unwrap()
}

fn query_config(namespace_pattern: &str) -> QueryConfig {
    QueryConfig {
        namespace_pattern: namespace_pattern.into(),
        key_pattern: "*".into(),
        on_missing: Default::default(),
        provider_wait_timeout: None,
        miss_cache_ttl: None,
    }
}

fn store_config(namespace_pattern: &str) -> StoreConfig {
    StoreConfig {
        namespace_pattern: namespace_pattern.into(),
        key_pattern: "*".into(),
        allow_context_move: false,
    }
}

#[tokio::test]
async fn auth_exact_operations_capture_digest_paths_and_strict_documents() {
    let mapping = OpenBaoMapping::new("cache").unwrap();
    let namespace = auth_namespace();
    let key = Key::new("raw/api-key?with/slashes").unwrap();
    let malformed_key = Key::new("malformed").unwrap();
    let mismatch_key = Key::new("mismatch").unwrap();
    let mut routes = HashMap::new();
    let (route, response) = auth_read(
        &mapping.auth_path(&key),
        OpenBaoMapping::encode_auth_document(
            &key,
            json!({"role": "reader"}),
            Some(Duration::from_secs(31)),
        ),
    );
    routes.insert(route, response);
    let (route, response) = auth_read(
        &mapping.auth_path(&malformed_key),
        json!({"value": {"role": "reader"}}),
    );
    routes.insert(route, response);
    let (route, response) = auth_read(
        &mapping.auth_path(&mismatch_key),
        OpenBaoMapping::encode_auth_document(&key, json!({"role": "reader"}), None),
    );
    routes.insert(route, response);
    let ordinary_namespace = NamespaceContext::new("/orders").unwrap();
    let ordinary_key = Key::new("a/b").unwrap();
    let ordinary_path = mapping
        .locate(&ordinary_namespace, &ordinary_key, None)
        .unwrap()
        .path()
        .to_owned();
    let (route, response) = auth_read(
        &ordinary_path,
        json!({"value": {"role": "reader"}, "ttl_seconds": 7}),
    );
    routes.insert(route, response);
    routes.insert(
        ("POST".into(), wire("data", &mapping.auth_path(&key))),
        StubResponse::ok(),
    );
    routes.insert(
        ("POST".into(), wire("data", &ordinary_path)),
        StubResponse::ok(),
    );
    routes.insert(
        ("DELETE".into(), wire("data", &mapping.auth_path(&key))),
        StubResponse::ok(),
    );
    let fixture = HttpFixture::start(routes).await;
    let bao = client(&fixture);
    let provider =
        OpenBaoQueryProvider::new(bao.clone(), mapping.clone(), query_config("/__auth")).unwrap();

    let result = provider
        .query(namespace.clone(), key.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.value, json!({"role": "reader"}));
    assert_eq!(result.ttl, Some(Duration::from_secs(31)));
    assert!(
        provider
            .query(namespace.clone(), Key::new("missing").unwrap())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        provider
            .query(namespace.clone(), malformed_key)
            .await
            .is_err()
    );
    assert!(
        provider
            .query(namespace.clone(), mismatch_key)
            .await
            .is_err()
    );

    let writer =
        OpenBaoStoreWriter::new(bao.clone(), mapping.clone(), store_config("/__auth")).unwrap();
    writer
        .set(OpenBaoValue {
            namespace: namespace.clone(),
            key: key.clone(),
            value: json!({"role": "writer"}),
            ttl: Some(Duration::from_secs(19)),
        })
        .await
        .unwrap();
    writer
        .delete(namespace, Some(KeyPattern::new(key.as_str()).unwrap()))
        .await
        .unwrap();

    let ordinary_writer =
        OpenBaoStoreWriter::new(bao.clone(), mapping.clone(), store_config("/orders")).unwrap();
    ordinary_writer
        .set(OpenBaoValue {
            namespace: ordinary_namespace.clone(),
            key: ordinary_key.clone(),
            value: json!({"role": "writer"}),
            ttl: Some(Duration::from_secs(7)),
        })
        .await
        .unwrap();

    let ordinary_provider =
        OpenBaoQueryProvider::new(bao, mapping.clone(), query_config("/orders")).unwrap();
    assert_eq!(
        ordinary_provider
            .query(ordinary_namespace, ordinary_key)
            .await
            .unwrap()
            .unwrap()
            .value,
        json!({"role": "reader"})
    );
    let requests = fixture.requests().await;
    let auth_requests = requests
        .iter()
        .filter(|request| request.path.contains("%252F__auth"))
        .collect::<Vec<_>>();
    assert!(
        auth_requests
            .iter()
            .all(|request| !request.path.contains(key.as_str()))
    );
    let set = auth_requests
        .iter()
        .find(|request| request.method == "POST")
        .unwrap();
    let body: Value = serde_json::from_str(&set.body).unwrap();
    assert_eq!(body["data"]["key"], key.as_str());
    assert_eq!(body["data"]["value"], json!({"role": "writer"}));
    assert_eq!(body["data"]["ttl_seconds"], 19);
    assert_eq!(body.get("options"), None);
    let ordinary_set = requests
        .iter()
        .find(|request| request.method == "POST" && request.path == wire("data", &ordinary_path))
        .unwrap();
    let ordinary_body: Value = serde_json::from_str(&ordinary_set.body).unwrap();
    assert_eq!(ordinary_body["data"]["value"], json!({"role": "writer"}));
    assert_eq!(ordinary_body["data"]["ttl_seconds"], 7);
    assert!(ordinary_body["data"].get("key").is_none());
    assert!(
        requests
            .iter()
            .any(|request| request.path == wire("data", &ordinary_path) && request.method == "GET")
    );
}

#[tokio::test]
async fn auth_provider_recursively_lists_valid_records_in_key_order() {
    let mapping = OpenBaoMapping::new("cache").unwrap();
    let namespace = auth_namespace();
    let records = [
        ("z-key", json!({"role": "z"})),
        ("a-key", json!({"role": "a"})),
    ];
    let collection = mapping.auth_collection_path();
    let mut lists: HashMap<String, Vec<String>> = HashMap::new();
    let mut routes = HashMap::new();
    for (key, value) in records {
        let key = Key::new(key).unwrap();
        let parts = mapping.auth_key_parts(&key);
        for depth in 0..4 {
            let parent = if depth == 0 {
                collection.clone()
            } else {
                format!("{collection}/{}", parts[..depth].join("/"))
            };
            let child = if depth < 3 {
                format!("{}/", parts[depth])
            } else {
                parts[depth].clone()
            };
            lists.entry(parent).or_default().push(child);
        }
        let path = mapping.auth_path(&key);
        let (route, response) = auth_read(
            &path,
            OpenBaoMapping::encode_auth_document(&key, value, None),
        );
        routes.insert(route, response);
    }
    for (path, mut children) in lists {
        children.sort();
        let (route, response) = list_response(&path, children);
        routes.insert(route, response);
    }
    let fixture = HttpFixture::start(routes).await;
    let values = fetch_provider_values(&client(&fixture), &mapping, namespace)
        .await
        .unwrap();
    assert_eq!(
        values
            .iter()
            .map(|value| value.key.as_str())
            .collect::<Vec<_>>(),
        vec!["a-key", "z-key"]
    );
}

fn single_record_list_routes(
    mapping: &OpenBaoMapping,
    key: &Key,
) -> HashMap<(String, String), StubResponse> {
    let collection = mapping.auth_collection_path();
    let parts = mapping.auth_key_parts(key);
    let mut routes = HashMap::new();
    for depth in 0..4 {
        let parent = if depth == 0 {
            collection.clone()
        } else {
            format!("{collection}/{}", parts[..depth].join("/"))
        };
        let child = if depth < 3 {
            format!("{}/", parts[depth])
        } else {
            parts[depth].clone()
        };
        let (route, response) = list_response(&parent, vec![child]);
        routes.insert(route, response);
    }
    routes
}

#[tokio::test]
async fn auth_provider_rejects_malformed_paths_documents_and_digest_mismatches() {
    let mapping = OpenBaoMapping::new("cache").unwrap();
    let collection = mapping.auth_collection_path();
    let malformed_cases = [
        vec![(collection.clone(), vec!["invalid/".into()])],
        vec![(collection.clone(), vec!["aaaaaaaaaaaaaaaa".into()])],
        vec![
            (collection.clone(), vec!["aaaaaaaaaaaaaaaa/".into()]),
            (
                format!("{collection}/aaaaaaaaaaaaaaaa"),
                vec!["bbbbbbbbbbbbbbbb/".into()],
            ),
            (
                format!("{collection}/aaaaaaaaaaaaaaaa/bbbbbbbbbbbbbbbb"),
                vec!["cccccccccccccccc/".into()],
            ),
            (
                format!("{collection}/aaaaaaaaaaaaaaaa/bbbbbbbbbbbbbbbb/cccccccccccccccc"),
                vec!["dddddddddddddddd/".into()],
            ),
        ],
    ];
    for lists in malformed_cases {
        let mut routes = HashMap::new();
        for (path, children) in lists {
            let (route, response) = list_response(&path, children);
            routes.insert(route, response);
        }
        let fixture = HttpFixture::start(routes).await;
        assert!(
            fetch_provider_values(&client(&fixture), &mapping, auth_namespace())
                .await
                .is_err()
        );
    }

    let key = Key::new("valid-key").unwrap();
    let path = mapping.auth_path(&key);
    let mut routes = single_record_list_routes(&mapping, &key);
    let (route, response) = auth_read(&path, json!({"value": {"role": "reader"}}));
    routes.insert(route, response);
    let fixture = HttpFixture::start(routes).await;
    assert!(
        fetch_provider_values(&client(&fixture), &mapping, auth_namespace())
            .await
            .is_err()
    );

    let mut routes = single_record_list_routes(&mapping, &key);
    let (route, response) = auth_read(
        &path,
        OpenBaoMapping::encode_auth_document(
            &Key::new("other-key").unwrap(),
            json!({"role": "reader"}),
            None,
        ),
    );
    routes.insert(route, response);
    let fixture = HttpFixture::start(routes).await;
    assert!(
        fetch_provider_values(&client(&fixture), &mapping, auth_namespace())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn auth_provider_skips_deleted_records_and_ordinary_listing_stays_reversible() {
    let mapping = OpenBaoMapping::new("cache").unwrap();
    let key = Key::new("gone").unwrap();
    let routes = single_record_list_routes(&mapping, &key);
    let fixture = HttpFixture::start(routes).await;
    assert!(
        fetch_provider_values(&client(&fixture), &mapping, auth_namespace())
            .await
            .unwrap()
            .is_empty()
    );

    let namespace = NamespaceContext::new("/orders").unwrap();
    let collection = mapping.root_collection_path(&namespace);
    let encoded_key = encode("a/b");
    let path = format!("{collection}/{encoded_key}");
    let mut routes = HashMap::new();
    let (route, response) = list_response(&collection, vec![encoded_key]);
    routes.insert(route, response);
    let (route, response) = auth_read(
        &path,
        json!({"value": {"role": "reader"}, "ttl_seconds": 7}),
    );
    routes.insert(route, response);
    let fixture = HttpFixture::start(routes).await;
    let values = fetch_provider_values(&client(&fixture), &mapping, namespace.clone())
        .await
        .unwrap();
    assert_eq!(values[0].key.as_str(), "a/b");
    assert_eq!(values[0].namespace, namespace);
}
