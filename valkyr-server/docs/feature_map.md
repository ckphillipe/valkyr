# valkyr-server — Feature Map

Native TCP/TLS server, REST + WebSocket API, and Prometheus metrics, all
driving one shared `Broker`.

## Modules

| Module | Responsibility |
| --- | --- |
| `lib` | `Server`: shared native TCP/WebSocket session loop, connection registry, callback correlation (`pending_results`), dispatch invoke with `callback_timeout`, provider-miss background refresh, security-key prefetch, metrics text. `RunningServer` / `RunningTlsServer` accept loops. `tls_config` PEM loader. |
| `http_api` | Axum router: bearer-auth REST fallback (path = namespace, raw query = key), `Destination`-header context move, `GET /ws` upgrade, unauthenticated `GET /metrics` router, percent decoding, error envelope mapping. |
| `config` | YAML schema, loopback listener defaults, config-relative TLS/secret path resolution, optional Moka cache capacity/idle policy, startup validation, and bootstrap-secret file loading. |
| `main` | `--config <path>` parsing, configuration-driven listener/auth/logging wiring, and graceful shutdown via `watch`. |

## Transports

- **Native TCP** (default `127.0.0.1:8081`), **REST/WebSocket** (default
  `127.0.0.1:8080`), and **metrics** (default `127.0.0.1:8090`) listeners
  start automatically; TLS remains optional. Native TCP accepts one text
  command per line in, one text answer per line out. Per-connection command
  queues are bounded (default 64) and apply read backpressure; outbound
  callback/response queues are bounded (default 256) and close slow clients.
- **REST**: `GET/PUT/DELETE /<namespace>?<key>`, `PUT` + `Destination` header
  moves `namespace::context` to another context of the same namespace,
  `Valkyr-Ttl` header sets TTL.
- **WebSocket** (`GET /ws`): one text protocol message per WebSocket frame,
  using the same codec as TCP.
- **Metrics**: separate unauthenticated listener; `GET /metrics` serves
  Prometheus text and `GET /health` returns a liveness `200 OK`.

## Callback model

- Provider queries (`ServerCommand::Query`) run in the background; the
  client gets `Miss { retry_after_ms }` immediately and the cache is warmed
  asynchronously (persisted first when a store adapter matches; values returned
  by an adapter are cache-only). Concurrent
  misses for the same namespace/key share one in-flight provider refresh.
  Admission owns the provider rate-limit charge, so joiners wait independently
  without consuming capacity. Each broker route state has a stable refresh
  identity carried by its dispatch and server refresh state. Every terminal
  path releases the matching core active-route identity before publishing
  completion; a joiner that recreates state while an older server refresh is
  still mapped releases that replacement instead.
  Completion uses a retained terminal watch value before identity-checked
  generation removal; the actual in-flight map preserves value and miss results
  for caller-owned late subscribers.
- Mutations with a matching store registration are synchronously invoked to
  the adapter for normal clients; adapter-originated mutations commit only to
  cache and never invoke another storage callback.
- Encrypted commands prefetch their scope key from the `/__secrets`
  provider before execution (`ensure_command_security_key`).
- Connection teardown removes the connection's registry entries and drains its
  pending callback correlations; callback timeouts and cancellations also
  remove their correlation entries.

## Authentication

`auth.bootstrap_api_key_file` is mandatory for every configured server. Its
bootstrap key is the control-plane administrator; other API keys warm from
`/__auth` provider callbacks. Cold native and REST authentication return a
retryable pending outcome (REST: 503 plus `Retry-After`); cached sessions
refresh in the background at half TTL and fail closed on refresh failure.

## Testing

Integration tests in `lib.rs`: native round trip incl. `${var}` keys,
registry auth, shared value/miss refresh, mixed waiter rate admission,
zero-timeout retry hints, slow/failing/timed-out/disconnected callbacks,
durable ordering/failure, encrypted waiting, actual-map late value/miss
terminal publication and replacement identity protection, committed value
precedence, scoped mutation races, maximum-TTL and rate-limited-route cleanup,
the deterministic same-route rate-limited joiner interleaving across saturated
routes, TLS listener,
publisher reconnect, and callback cleanup. REST/metrics tests in
`http_api.rs`. `tests/server_adapter.rs` starts two loopback servers and a
configured SQLite-backed adapter callback channel to cover auth warm-up,
provider refresh, write-through set/overwrite/delete/context move, cross-server
store replication, encrypted durability, REST, authorization, and metrics in the normal workspace test
suite. `cargo bench -p valkyr-server --bench server_adapter` separately reports
write-through writes, cached reads, and initial cold cache misses (override
iterations with `VALKYR_BENCH_ITERATIONS`).

Normal provider refreshes publish value, confirmed miss, rate-limited, or
failure through a shared per-route state; each caller applies its own provider
wait timeout. Confirmed misses carry the affected-route generation captured at
admission and are inserted only when the route remains empty; a terminal miss
rechecks the committed value before reaching a waiter.

## Container image

`Dockerfile` builds the server from the workspace root into a Google Distroless
`cc-debian13` runtime image. It exposes the native TCP, HTTP/WebSocket, and
metrics ports and defaults to `/etc/valkyr/server.yml`. GitHub Actions CI builds
this image without publishing it.
