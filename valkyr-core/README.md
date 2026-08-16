# valkyr-core

The dependency-light domain layer for Valkyr. It exposes validated route
identifiers, the human-readable text command/response model, `Store`, `MemoryStore`, and
the `Broker` command executor. It performs no network I/O.

`MemoryStore` is suitable for a single process and test environments. It uses
Moka for concurrent access, command-provided per-entry TTL expiry, and optional
entry-count capacity or time-to-idle eviction. The default constructor remains
unbounded with no idle expiry; use `MemoryStore::with_config` for those policies.

The [architecture guide](../docs/architecture.md) describes how this domain
layer is used by the server, client, and SQL adapter.
