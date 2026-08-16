# valkyr-go — Feature Map

Standalone Go 1.25 SDK for Valkyr native protocol v1. The package root owns
the public application client and supervised provider/store adapter client;
wire representation remains private.

## Modules

| File | Responsibility |
| --- | --- |
| `client.go`, `route.go`, `result.go` | Context-aware fluent application operations, typed `Value`/`Miss`/`Unknown` outcomes, JSON decoding, one bounded read retry, ping, and stats. |
| `transport.go`, `tls.go`, `options.go` | Ordered TCP/TLS connection, bounded text framing, deadline derivation, verified roots, address parsing, and connection poisoning. |
| `auth.go` | One-shot authentication outcome and bounded `auth_pending` retry. |
| `wire.go`, `errors.go` | Private v1 frame codecs, additive-field tolerance, strict tagged dispatch, duration/UUID validation, and typed error categories. |
| `adapter_registration.go`, `pattern.go`, `handlers.go` | Immutable serving snapshots, generated/stable adapter identity, pre-tokenized route patterns, provider/store contracts, `ProviderValue` results, and local route selection. |
| `adapter_client.go` | Per-endpoint authentication and registration restoration, callback dispatch, timeout/panic containment, overload rejection, reconnect backoff, and shutdown. |
| `examples/` | Small application and provider/store programs compiled as part of CI. |
| `tests/integration/` | Tagged live conformance tests against isolated Rust server processes, including plain TCP, TLS, callbacks, failure, and reconnect. |

## Key flows

- **Application connect:** parse address → open TCP or verified TLS → authenticate
  with bounded pending retry → serialize each command/write and matching response
  on the connection mutex.
- **Read:** `Get` maps `value`, `miss`, and `unknown` to closed public result
  types. `GetWithRetry` sleeps for the server delay and performs exactly one
  second read.
- **Adapter connect:** open endpoint → authenticate with the stable adapter UUID
  → restore provider/store registrations → read callbacks continuously.
- **Callback:** validate the tagged frame → acquire a bounded slot or return a
  correlated overload error → execute with a callback context → acknowledge a
  durable operation only after the handler succeeds.
- **Provider result:** `Provider.Get` may return a raw value, `nil` for a miss,
  or `ProviderValue{Value, TTL}` for a successful value with an optional
  whole-second cache TTL. Invalid durations become correlated provider errors;
  misses, errors, timeouts, cancellation, and overloads have no value TTL.
- **Reconnect:** close the poisoned connection, apply capped jittered backoff,
  reconnect independently per endpoint, and restore the same registrations and
  adapter identity.

## Invariants

- Ordinary request responses are correlated by order, not request ID; ambiguous
  framing, I/O, timeout, cancellation, or response-contract failures close the
  connection.
- Protocol values stay as `json.RawMessage` until callers explicitly decode
  them. A provider `nil` is a miss and is not a stored JSON-null value.
- Unknown tagged callbacks close the adapter connection without an
  acknowledgement. Durable callback failure never commits the server cache.
- Registration snapshots are copied for serving; provider overlaps are rejected
  at construction time and newest matching store registrations win locally.
- TLS always verifies server certificates using system or explicitly appended
  roots; insecure verification bypass and client certificates are unsupported.

## Testing

Unit tests cover canonical fixtures, negative wire cases, framing limits,
ordered concurrency, context deadlines, authentication, routes, patterns,
registration, callback errors, and panic containment. Build-tagged integration
tests cover live auth warm-up, CRUD/batch/delete/move, TTL/result outcomes,
ping/stats, provider refresh, durable callbacks and failure, TLS server-name
verification, and registration restoration after disconnect. CI runs unit/race
checks on every change and the live suite with `-tags=integration`.

At `NewAdapterClient` time, `AdapterClient` captures provider and store routes
plus adapter identity in an immutable client-owned snapshot; provider/store
handler objects remain shared. Every endpoint reconnect restores that snapshot,
including the copied
`max_rate` value and unit-safe timeout and miss-TTL options; omitted fields
retain zero defaults. Provider `max_rate` is validated as a non-zero protocol
`u32`. Unit and live tests cover duration validation, raw and TTL-bearing
provider results, expiry refresh, misses, errors, store TTL forwarding,
immutable snapshots, wire registration capture, and restoration on a second
registration cycle.
