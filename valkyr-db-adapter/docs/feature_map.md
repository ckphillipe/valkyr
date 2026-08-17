# valkyr-db-adapter — Feature Map

Bridges SQL databases (SQLite, MySQL, PostgreSQL) and Valkyr over the shared
human-readable text callback protocol: scheduled
publishing into the cache, on-demand cache-miss queries, and durable
write-through persistence.

## Concepts

| Item | Responsibility |
| --- | --- |
| `ValueSource` / `ValuePublisher` | Pull side: fetch a complete batch, publish in order. `Adapter::sync_once` coordinates one pass. |
| `QueryProvider` | Answer `ServerCommand::Query` (cache miss) from the database; `None` leaves the cache unchanged. |
| `StorageWriter` | Handle `Persist*` mutations before the server commits to its cache. |
| `CallbackBridge` | Routes streaming callbacks to the first matching provider / newest matching writer. |
| `ReconnectingPublisher` | Serialized multicast publish with one reconnecting client per endpoint. |
| `DatabaseManager` | SQLx `Any` pool + query timeouts; init statements; provider row mapping (`namespace`/`ns` + optional `context` → `ns::context`); JSON-or-string decoding for SQL text and UTF-8 BLOB values. |

## Implementations

- **SQLx family (used by the binary and public API):** `DatabaseSource`,
  `DatabaseQueryProvider`, `DatabaseStoreWriter` — pooled, timeout-bounded,
  batched writes in a transaction.

## Module layout

`lib.rs` is the public re-export facade. `error.rs` defines adapter errors;
`traits.rs` defines the core value and callback traits; `bridge.rs` routes
streaming callbacks; `publisher.rs` coordinates publishing and scheduled
syncs; `config.rs` owns YAML configuration and validation; and `sqlx_impl.rs`
contains the SQLx-backed implementations.

## Configuration (`--config adapter.yml`)

`AdapterConfig` sections: `database` (URL + pool/timeouts), `valkyr`
(`endpoints` as structured URL/key/optional-CA entries, provider
`provider_wait_timeout` and `miss_cache_ttl` defaults, transport
`request_timeout`,
max_retries), `logging`
(pretty/JSON),
`providers` (cron-scheduled pulls), `queries` (PROVIDE registrations),
`stores` (STORE registrations), `init` (startup SQL). Each endpoint key and
optional CA path is resolved relative to the adapter YAML file, must point to
a regular file, and is read/parsed once during startup; empty keys and
malformed CAs are rejected. CAs are valid only for TLS URLs and augment the
normal WebPKI roots.
`validate()` rejects
empty required fields, zero timeouts, undeclared SQL parameters, and
orphaned move/delete-namespace settings.

## SQL parameter binding

Route patterns declare `{captures}`; statements bind by name from a fixed
vocabulary: `namespace`, `key`, `key_pattern`, `value`, `ttl_seconds`,
`context`, `source_namespace`, `destination_namespace`, `source_context`,
`destination_context`, plus any pattern capture. Context parameters bind the
context from a `namespace::context` route, or an empty string when no context
exists.

## Runtime (binary)

Connect → run `init` → build one endpoint-aware bridge and `StreamingClient`
per endpoint → register queries/stores everywhere → spawn one cron task per
provider → monitor and independently re-register a dropped callback
connection. Scheduled providers acquire a 30-second local `/__lease` for each
value namespace and publish only to endpoints they own. After a local
store write succeeds, the bridge best-effort forwards the original set, batch,
delete, or move to every endpoint except its source. The shared adapter UUID
suppresses callback loops; forwarding has no retry or durable outbox.

## Testing

In-crate tests cover independent provider-option inheritance including explicit
zero overrides, multi-endpoint registration and restoration capture, parameter
validation, capture binding, SQLx init/provider reads, scheduled and on-demand
UTF-8 BLOB decoding (including nullable and invalid binary values), batch
rollback, context decoration rules, and context moves. `valkyr-server/tests/server_adapter.rs` adds a configured
SQLite end-to-end path: it registers the callback bridge with a live native
server and verifies auth lookup, security-key lookup, provider refresh, and
durable storage mutations. See `example/sqlite-security-config.yml` for a full
auth + encryption-key setup.

## Container image

`Dockerfile` builds the adapter from the workspace root into a Google
Distroless `cc-debian13` runtime image. It starts with
`--config /etc/valkyr/adapter.yml` and exposes no ports because it connects to
Valkyr and databases as a client. GitHub Actions CI builds the image without
publishing it.

Optional `valkyr.provider_wait_timeout` and `valkyr.miss_cache_ttl` defaults
resolve independently against each query override, then zero, and are restored
by every endpoint registration/reconnect path. `valkyr.request_timeout` defaults
to five seconds and is used directly for transport setup. YAML durations require
explicit units; provider values convert losslessly to integer v1 milliseconds or
seconds. The v1 wire fields retain their `timeout` and `miss_ttl` names.
