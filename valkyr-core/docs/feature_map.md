# valkyr-core — Feature Map

Typed command model and execution rules shared by every Valkyr transport and
client. No network code lives here; transports deliver `Command`s and carry
out `Dispatch` callbacks.

## Modules

| Module | Responsibility |
| --- | --- |
| `duration` | Strict unit-bearing duration parsing/serialization and exact integer wire-unit conversion shared by adapter configuration. |
| `protocol` | Transport-independent typed command, answer, callback, and value models. |
| `line_protocol` | Canonical bounded UTF-8 lexer, parser, formatter, context-aware answers, and callback correlation for text protocol v1. |
| `route` | Owned `NamespaceContext`, `Key`, `KeyPattern`, `NamespacePattern`, and `Route` identifiers; `NamespaceContext` preserves the full namespace text while exposing allocation-free `ns()`/`ctx()` accessors for valid `namespace::context` routes. |
| `pattern` | Deterministic matcher for registration patterns: `*` wildcard, `{name}` / `${name}` captures, tokenized at construction. |
| `store` | `Store` trait (async CRUD + scan + atomic context move) and `CompositeStore` fan-out. |
| `memory` | `MemoryStore`: Moka-backed in-memory `Store` with composite namespace/key entries, command TTL expiry, optional entry-count capacity and time-to-idle eviction, and async coordination for namespace lifecycle operations. |
| `registry` | Connection-scoped provider/store registrations, round-robin provider pick, newest-store-wins, and compact boxed batch-store matches with `Mixed` detection. |
| `security` | Auth model (`AuthRecord` → `AuthInfo`, namespace-grouped permissions expanded into roles), TTL-bounded refresh-ahead `AuthManager` sessions that resolve the current principal on each protected command, `StoreAuthenticator`/`SimpleAuthenticator`, `ValueCipher` (XChaCha20-Poly1305, base-namespace-and-key AAD). |
| `broker` | `Broker`: the single command executor — authorization, cache hit/miss, provider selection and route-scoped mutation-generation capture, write-through persistence ordering, `~key~` encryption, `${var}` key resolution, `/__secrets` key loading. |
| `error` | `Error` / `Result` for the whole workspace. |

## Key flows

- **Read:** `Broker::get` → authorize → resolve `${var}`s → memory lookup →
  hit: decrypt if `~key~` → miss: provider dispatch (`Response::Miss` +
  `retry_after_ms`), else `Unknown`.
- **Write:** authorize → encrypt if marked → `prepare_mutation`: if a store
  registration matches, return `PendingMutation` + `Persist*` dispatch
  (transport persists first, then `commit`); otherwise commit immediately.
- **Encrypted read/write:** transport calls
  `security_key_provider_dispatch` first; keys come from a registered
  `/__secrets` provider as `{key: <64 hex>, created: <unix>}` and are cached
  in the store. Context namespaces (`/a::b`) share the key of `/a`.
- **Registration:** `Provide`/`Store` require an `adapter_instance` and are
  scoped to the owning connection; `/__auth` and `/__secrets` registrations
  require the bootstrap admin.
- **Authentication:** a non-bootstrap API key first loads a valid cached
  `/__auth` record, when present; otherwise a cold key emits `AuthPending`
  while the broker queries its `/__auth` provider. It remains valid for the
  session TTL and refreshes after half that interval. A successful committed
  `PUT /__auth?<api-key>` replaces the cached principal, so active sessions
  observe permission changes on their next protected command.

## Invariants

- Durable write before cache write (broker returns `PendingMutation`;
  transport commits only after adapter success).
- Batches never split across storage adapters (`BatchStoreMatch::Mixed` is
  an error).
- `delete`/`move` are denied on `/__auth` and `/__secrets`. Bootstrap-only
  `set` on `/__secrets` validates and warms the local cache directly, without
  invoking durable stores or adapters; cold keys still enter through providers.
- `Move` accepts only `namespace::context` routes with the same base namespace.
  Ciphertext is bound to that base namespace and key via AAD, so a context move
  does not require re-encryption.

## Testing

Unit tests per module plus broker-level tests for admission-owned rate limits,
route-scoped exact/batch/pattern/namespace/move miss races, final-check versus
mutation synchronization, bounded coordination cleanup, unrelated-route
coordination, maximum TTL handling, zero-rate rejection, and reserved-namespace
protection. Protocol tests round-trip every v1 fixture and reject unknown
tagged variants.

Provider registrations carry `ProvideOptions`; the server charges a selected
provider only when it admits a new refresh generation. A separate Moka cache
stores only confirmed misses, with relative per-entry expiry. Mutation
coordination uses fixed-shard, active-route state bounded by currently active
refreshes: each route state has a stable refresh identity carried through the
dispatch and terminal lifecycle, cleanup removes only the completing identity,
and a joiner that recreates state during publication is reclaimed. Value, miss,
failure, and rate-limited terminal paths reclaim completed routes, broad
mutations touch only active matching routes, and route-gated generation
validation plus marker insertion is atomic with committed invalidation while
durable store I/O remains outside the gates.
