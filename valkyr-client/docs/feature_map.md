# valkyr-client — Feature Map

Async client for the native human-readable text protocol, plus a streaming adapter
connection and an optional C ABI.

## Surface

| Item | Responsibility |
| --- | --- |
| `Client` | Shared, request-ordered connection (mutex-serialized `request`). Typed helpers: `get`, `set`, `set_batch`, `delete`, context-only `move_namespace`, `provide`, `store`, `ping`, `stats`, `authenticate`. |
| `ClientBuilder` | Multi-endpoint connect with per-endpoint TCP/TLS, API key, `adapter_instance`, connect timeout, and request timeout. First healthy endpoint wins. |
| `ServerEndpoint` | `tcp` / `tls` address with optional verified per-endpoint Rustls configuration. |
| `StreamingClient` | Long-lived adapter connection with a background reader task; dispatches `ServerCommand` callbacks to a `ServerCommandHandler` while idle. `is_closed` lets owners restore registrations. |
| `ServerCommandHandler` | Trait implemented by providers/storage adapters; maps `ServerCommand` → `ServerResult`. |
| `capi` (feature) | `extern "C"` handle API: `valkyr_client_new/free`, `get/set/delete/move`, `valkyr_client_last_error`, `valkyr_string_free`. One tokio runtime + serializing mutex per handle. |

## TLS

`connect_tls` uses WebPKI roots and derives SNI from `host:port`;
`verified_tls_config` augments WebPKI roots with PEM CAs, while
`connect_tls_with_config` / `connect_tls_with_server_name` support private PKI
and IP-listener/DNS-certificate splits. `ClientBuilder` and `StreamingClient`
share the same verified configuration path.

## Error model

`ClientError` distinguishes connection failures (`is_connection_failure` →
retryable by e.g. `ReconnectingPublisher`), server-reported errors, confirmed
auth failures, retryable authentication warm-up (`is_retryable`), and
unexpected-response contract violations.

## Notes

- Requests time out after 30 seconds by default; `ClientBuilder::request_timeout`
  configures the ordinary client and `StreamingClient::with_request_timeout`
  configures a streaming client. A timeout poisons the connection because
  responses are ordered; reconnect before retrying. Keep this request timeout
  above the largest provider wait timeout.
- `Client` responses are matched by order, not ID — safe because the server
  answers each connection's commands sequentially.

## Testing

Focused tests call both public ordinary and streaming registration APIs and
capture omitted, zero, and positive provider-option frames; server integration
tests cover TCP + TLS round trips, streaming provider callbacks, and reconnect
scenarios.

`provide_with_options` forwards optional millisecond wait and second miss-TTL
provider policy fields while the legacy zero-option helper remains available.
