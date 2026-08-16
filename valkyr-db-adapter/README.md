# valkyr-db-adapter

`valkyr-db-adapter` is Valkyr's SQL adapter. It supports SQLite, MySQL, and
PostgreSQL through SQLx, runs scheduled providers, answers cache-miss
callbacks, and persists server-routed writes before Valkyr updates its local
cache. The library separates those protocol concerns from database interfaces.

Read the [workspace README](../README.md) for the service overview and the
[operations guide](../docs/operations.md) before deploying an adapter.

Under `valkyr:`, `request_timeout` is the adapter's own connection and request
deadline and defaults to `"5s"`. Optional `provider_wait_timeout` and
`miss_cache_ttl` provide global defaults for query registrations. A query may
override either policy independently; precedence is query value, global value,
then zero. Values must be quoted duration strings with an explicit unit such as
`"15ms"`, `"5s"`, or `"1m"`; legacy names and bare numbers are rejected.
Provider wait values are converted exactly to the v1 wire field's integer
milliseconds, and miss-cache values exactly to integer seconds. Zero wait keeps
immediate-MISS behavior and zero miss-cache TTL disables negative caching.
The v1 wire fields remain named `timeout` and `miss_ttl`; those protocol names
are not the adapter YAML names. Every application connection that issues a
waiting GET must set its own request timeout above that route's provider wait
timeout.

Scheduled SQLx providers expect a query returning these aliases:

| Column | Type | Meaning |
| --- | --- | --- |
| `namespace` | text | Valkyr namespace |
| `key` | text | Valkyr key |
| `value` | text | Valid JSON value |
| `ttl_seconds` | integer, nullable | Optional value lifetime |

## Docker

Build from the workspace root. Mount the adapter configuration and any
database-specific credentials read-only; neither belongs in the image.

```sh
docker build -f valkyr-db-adapter/Dockerfile -t valkyr-db-adapter .
docker run --rm \
  --mount type=bind,src=./valkyr-adapter.yml,dst=/etc/valkyr/adapter.yml,readonly \
  --mount type=bind,src=./adapter-api-key,dst=/etc/valkyr/adapter-api-key,readonly \
  valkyr-db-adapter
```

The image uses Google Distroless `cc-debian13`, so it has no shell or package
manager for interactive debugging. Supply connection settings through the YAML
file and mount certificates or secret files at the absolute paths referenced
therein.

Run the service with a YAML file:

```sh
cargo run -p valkyr-db-adapter -- --config ./valkyr-adapter.yml
```

```yaml
database:
  url: sqlite://./state.db
  max_connections: 5
  connection_timeout_seconds: 30
valkyr:
  endpoints:
    - url: tcp://127.0.0.1:8081
      api_key_file: ./adapter-api-key
    - url: tls://replica.internal:8081
      api_key_file: ./replica-api-key
      ca_certificate_file: ./tls/replica.crt
  # Endpoint key and CA paths resolve from this adapter YAML file.
  request_timeout: "5s"
  provider_wait_timeout: "1s"
  miss_cache_ttl: "30s"
  max_retries: 9
logging:
  level: info
  format: pretty # pretty or json
  target: false
  thread_names: false
  ansi: true
providers:
  all_values:
    namespace_pattern: /values # optional when rows return namespace or ns
    key_pattern: "{id}"
    query: SELECT namespace, key, value, ttl_seconds FROM valkyr_values
    parameters: { active: true }
    frequency: "0 */5 * * * *"
    run_on_startup: true
init:
  - name: values_table
    sql: CREATE TABLE IF NOT EXISTS valkyr_values (namespace TEXT, key TEXT, value TEXT, ttl_seconds INTEGER)
    timeout_seconds: 30
queries:
  person:
    namespace_pattern: /people
    key_pattern: "{id}"
    query: SELECT value FROM valkyr_values WHERE namespace = ? AND key = ?
    parameters: [namespace, id]
    timeout_seconds: 5
    ttl_seconds: 60
stores:
  person:
    namespace_pattern: /people
    key_pattern: "{id}"
    set_query: INSERT OR REPLACE INTO valkyr_values(namespace, key, value) VALUES (?, ?, ?)
    set_parameters: [namespace, id, value]
    delete_query: DELETE FROM valkyr_values WHERE namespace = ? AND key = ?
    delete_parameters: [namespace, key_pattern]
    timeout_seconds: 5
```

Provider queries must return `key` and `value`. They may also return `namespace`
or `ns`; otherwise `namespace_pattern` is used. An optional row `context`
becomes a `namespace::context` route. Query callbacks return the first column
of their first row (JSON text is decoded; other SQL types become their
equivalent JSON values). SQL parameters may use `namespace`,
`context`, `key`, `key_pattern`, `value`, `ttl_seconds`, or captures from
`{name}` / `${name}` patterns. Parameter names are checked against those
declared route captures at startup, so spelling mistakes fail configuration
loading instead of the first callback.

Provider `parameters` is a map of scalar YAML constants, bound in stable key
order. `database.query_timeout_seconds` defaults to 30 seconds; callback
queries may override it with
`timeout_seconds`, and stores have a 30-second default. Stores may additionally
set `delete_ns_query`/`delete_ns_parameters` for a namespace delete and
`move_ns_query` (optionally with `move_ns_pattern`) for context moves. For a move, captures from
`namespace_pattern` come from the source and captures from `move_ns_pattern`
come from the destination. A multi-key `set` is committed as one database
transaction: if any item fails, none of the batch is persisted.

`valkyr.endpoints` is a replication set. Provider values are published to every
endpoint. Each endpoint receives its own `PROVIDE` and `STORE` registration;
after a successful local database write, a store mutation is forwarded to all
other endpoints using the same adapter identity, so the receiving cache commits
it without calling this adapter again. Forwarding is asynchronous, best effort,
and has no retry or durable outbox: an unavailable replica can miss an update
without delaying or failing the originating store request.

Use a database URL appropriate to the driver: `sqlite:...`, `mysql://...`, or
`postgres://...`. Each endpoint API-key file must contain one non-empty key; it
is trimmed and loaded once at startup, so rotating a file requires a restart.
Optional endpoint CA files are PEM certificates that augment the normal public
roots and are accepted only for `tls://` endpoints. Keep credential and CA
files outside source control, restrict them to the adapter service account, and
mount them read-only. `init` statements run once at process startup, before Valkyr
connections and callback registrations are created; they are not repeated on a
Valkyr reconnect. A closed callback connection reconnects independently with
bounded exponential backoff; healthy endpoint connections remain active.
Scheduled publishers reconnect and retry a failed endpoint write once;
in-flight callback operations are not replayed.

## Logging

The adapter emits structured `tracing` events for startup, database
initialization, callback registration/reconnection, and every scheduled
provider sync. Configure terminal output with the YAML `logging` section:
`format: pretty` is readable during development, while `format: json` emits
newline-delimited JSON for log collectors. `level` accepts normal tracing
filters, such as `debug` or `valkyr_db_adapter=debug,valkyr_client=info`.
`RUST_LOG`, when set, overrides `logging.level` for a process invocation.
