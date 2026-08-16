# Valkyr workspace

| Package | Responsibility |
| --- | --- |
| `valkyr-core` | Routes, protocol, in-memory/composite storage, broker, authorization, encryption, and registrations. |
| `valkyr-client` | Async native client, endpoint builder, and streaming callback client. |
| `valkyr-server` | Native TCP, REST, and WebSocket server with connection-scoped callback dispatch. |
| `valkyr-db-adapter` | SQLite, MySQL, and PostgreSQL providers plus durable-storage callback bridge. |
| `valkyr-openbao-adapter` | OpenBao KV v2 providers and durable write-through callback bridge. |
| `valkyr-python` | Python 3.11+ SDK (outside the Cargo workspace): fluent client and streaming adapter client. |
| `valkyr-go` | Go 1.25 SDK (outside the Cargo workspace): fluent client and supervised provider/store adapter client. |

The native protocol is one human-readable UTF-8 command per line. Commands include `AUTH`,
`get`, `set`, batched `set`, `delete`, `move`, `provide`, `store`, `ping`, and
`stats`. Providers receive cache-miss queries over their existing connection;
storage adapters receive mutations and must confirm them before the memory
store changes.

`PROVIDE` registrations may set `max_rate`; Valkyr enforces that limit per
provider over one-second windows and returns a cache miss with `retry_after_ms`
when the provider is temporarily saturated.

Provider routes may also set `timeout` in milliseconds and `miss_ttl` in
seconds. A route timeout bounds how long an ordinary GET waits for its shared
refresh; `miss_ttl` caches only a clean provider miss. Omitted or null values
default to zero, and `valkyr.timeout_ms` remains the transport request timeout.
Because native and streaming responses are ordered, an application request
timeout must exceed the largest provider wait timeout it uses; otherwise the
client can poison its connection while the server is still waiting.

Encrypted keys use the `~key~` form. Before the first encrypted operation for
a namespace scope, the server synchronously queries a registered provider for
`/__secrets` and caches its returned key record. The record is
`{ "key": "<64 hexadecimal characters>", "created": <unix-seconds> }`;
configure that provider against durable storage so key rotation and backup
remain under your control.
