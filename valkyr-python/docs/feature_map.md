# valkyr-python — Feature Map

Standalone Python 3.11+ SDK for the Valkyr native human-readable text protocol.

## Modules

| Module | Responsibility |
| --- | --- |
| `src/valkyr/__init__.py` | Public exports: `Client`, `AdapterClient`, `Adapter`, `Provider`, `ProviderValue`, `Store`, typed result/error classes. |
| `src/valkyr/_wire.py` | Protocol v1 wire models: `Command`, `Response`, `ServerCommand`, `ServerResult`, `SetEntry`, `Stats`. Decoders ignore additive fields and reject unknown tagged variants. |
| `src/valkyr/_errors.py` | Typed exception hierarchy: `ValkyrError`, `ProtocolError`, `ConnectionError`, `TimeoutError`, `AuthError`, `AuthPending`, `OverloadError`, `ServerError`, `RouteError`. |
| `src/valkyr/_transport.py` | Async TCP/TLS `Transport` with ordered request/response framing, atomic writes, and text codec decode. |
| `src/valkyr/_auth.py` | `authenticate_once` and bounded `authenticate` retry for `AuthPending`. |
| `src/valkyr/_client.py` | Fluent application `Client`, `Namespace`, `Route`, and typed read results `Value`, `Miss`, `Unknown`. |
| `src/valkyr/_handlers.py` | `ProviderValue` result wrapper plus `Provider` and `Store` abstract base classes. |
| `src/valkyr/_registration.py` | `Adapter` registration builder and `adapter_instance` UUID generation. |
| `src/valkyr/adapter.py` | Streaming `AdapterClient` with supervised reconnect, callback dispatch, bounded concurrency, and fail-closed overload handling. |
| `tests/unit/` | Wire serialization, transport, auth, fluent client, and adapter unit tests. |
| `tests/integration/` | Live server-backed conformance tests for auth, get/set, batch, move, TTL, provider refresh, store write-through, and reconnect. |
| `examples/` | Runnable `client_basic.py` and `provider_store.py` examples. |

## Key flows

- **Connect:** parse host/port, open TCP/TLS transport, authenticate with bounded
  `AuthPending` retry.
- **Fluent read:** `get()` returns `Value`, `Miss`, or `Unknown`; `get_with_retry()`
  sleeps for a positive `retry_after_ms` and retries once; `get_value()` returns
  the decoded value or `None`.
- **Fluent write:** `set()`, `set_many()`, `delete()`, and `move()` expect `Ok`.
- **Adapter lifecycle:** connect → authenticate → register provide/store routes →
  read callbacks → dispatch to handlers through a bounded callback limiter →
  write correlated results. Unknown callback variants or connection drops close
  the connection and trigger supervised reconnect with restored registrations;
  exponential delay resets only after a valid callback frame.
- **Provider result:** provider callbacks may return a raw value, `None` for a
  miss, or `ProviderValue(value, ttl_seconds)` for a successful value with an
  optional whole-second cache TTL. Invalid TTLs become correlated provider
  errors; misses, errors, timeouts, cancellation, and overloads have no value
  TTL.
- **TLS:** system roots, custom CA path or PEM bytes, and explicit server-name
  override; no client certificates or insecure bypass.

## Invariants

- Ordered request/response connections are poisoned on timeout so callers cannot
  misalign responses.
- Adapter callbacks never queue when concurrency is exhausted; overload
  immediately returns a correlated error.
- Unhandled durable callbacks (set, batch, delete, move) are never acknowledged.
- Reconnects share the same `adapter_instance` UUID and restore all registrations.

At `AdapterClient` construction, provider and store route configuration is
captured in immutable tuples while the registered provider/store handler
objects remain shared. Callback routing and reconnect registration use only
that snapshot, preserving validated `max_rate`, `timeout`, and `miss_ttl`.
Registration `max_rate` is a non-zero protocol `u32`; timeout is whole
milliseconds and miss TTL is whole seconds.
Unit and live tests cover raw values, provider TTLs and expiry refresh, misses,
errors, store TTL forwarding, callback limits, wire round trips, and restored
registration frames.
