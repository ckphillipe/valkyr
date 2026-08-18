# Plan: Adapter-Originated Mutations Are Cache-Only

Status: READY_FOR_IMPLEMENTATION

## Summary

Prevent replication loops when two Valkyr servers each have registrations from
multiple DB-adapter processes. Any mutation received on an adapter-authenticated
connection will update only the receiving server's cache and will never invoke a
`STORE` callback. A mutation from a normal client still uses write-through
persistence; after the selected DB adapter persists it and forwards it to the
other server, that adapter-originated copy stops at the other server's cache.

The rejected stable/shared adapter-UUID edits currently present in the working
tree are not part of this design and must be removed during implementation.

## Confirmed Decisions

- Adapter origin, not adapter UUID equality, suppresses storage forwarding.
- A normal client write to `s1` is persisted by one matching adapter; that
  adapter may forward the mutation to `s2`, where it is committed to cache
  without another callback.
- A DB adapter may write directly to both `s1` and `s2`; both servers commit
  those writes to cache without invoking any storage adapter.
- The rule applies consistently to all cache mutation forms: single set,
  batch set, delete, and namespace move.
- Existing random per-process adapter UUID generation remains valid; redundant
  adapters do not need a shared configured UUID.
- The wire protocol and client authentication shape remain unchanged.

## Open Questions

None.

## Proposed Design

1. Treat `RequestContext.adapter_instance.is_some()` as the mutation's trusted
   adapter-origin marker after normal authentication and authorization.
2. Pass an explicit `from_adapter: bool` into the broker mutation preparation
   path, including values returned asynchronously by providers.
3. At the start of mutation preparation, immediately commit adapter-originated
   mutations to the local cache and return `Response::Ok` with no dispatch and
   no pending mutation. This common gate covers set, batch, delete, and move.
4. For ordinary client mutations, keep the current write-through ordering:
   select a matching store registration, dispatch `Persist*`, and commit only
   after adapter success; commit immediately when no store matches.
5. Simplify `Registry` store selection because source-UUID exclusion is no
   longer a valid routing rule. Remove the source-adapter filter parameters and
   the unused adapter identity from `StoreRegistration`; retain the requirement
   that only an adapter-authenticated connection may issue `STORE`.
6. Restore the DB-adapter configuration, startup UUID generation, examples, and
   documentation changed by the rejected shared-UUID approach. Update feature
   documentation to describe the new cache-only adapter-origin invariant.

## Affected Files / Modules

- `valkyr-core/src/broker.rs`: central adapter-origin bypass and mutation tests.
- `valkyr-core/src/registry.rs`: remove UUID-based store exclusion and obsolete
  registration metadata/tests.
- `valkyr-core/docs/feature_map.md`: document adapter-origin cache-only writes.
- `valkyr-server/src/lib.rs`: pass adapter-origin state for provider results if
  the broker API becomes boolean-based; add/adjust transport-level assertions.
- `valkyr-server/tests/server_adapter.rs`: two-server/two-adapter regression.
- `valkyr-server/docs/feature_map.md`: clarify callback suppression.
- `valkyr-db-adapter/src/config.rs`, `valkyr-db-adapter/src/main.rs`,
  `valkyr-db-adapter/README.md`, `valkyr-db-adapter/docs/feature_map.md`, and
  `example/sqlite-config-{dev,docker}.yml`: remove the rejected required shared
  UUID edits and align replication documentation.

## Design / Clean Code Review

- The suppression decision belongs in `Broker::prepare_mutation`, the single
  path shared by every mutation, rather than in each adapter or transport.
- An explicit boolean communicates the only information mutation routing needs;
  UUID matching in `Registry` would retain dead behavior and make the invariant
  harder to understand.
- No new configuration, protocol fields, dependencies, or forwarding metadata
  are needed.
- Authorization continues before mutation preparation, so cache-only adapter
  writes do not bypass namespace permissions. The existing API-key permissions
  remain responsible for deciding which clients may authenticate and write as
  adapters.

## Performance Review

- Adapter-originated mutations avoid registry locking, callback I/O, and repeat
  database writes, directly eliminating the unbounded ping-pong workload.
- Normal client mutations retain the existing registry lookup and durability
  cost; no new allocations or network round trips are introduced.
- The origin check is constant-time and occurs once per mutation, including one
  check for an entire batch.

## Task Breakdown

- [x] **AOC-001 — Remove the rejected shared-UUID implementation**
  - Objective: restore random process UUID generation and the prior DB-adapter
    configuration contract before implementing the approved server rule.
  - Likely files: the DB-adapter, example, and integration-test files listed
    above that currently contain `0198ba5e-f7f4-7000-8000-000000000001`.
  - Acceptance criteria: `valkyr.adapter_instance` is not required in DB-adapter
    YAML; startup again creates one UUID per process; no rejected documentation
    remains.
  - Tests: DB-adapter configuration and main-module tests parse existing YAML.
  - Dependencies: none.
  - Risk: preserve any unrelated user changes while reverting only the rejected
    edits. Implemented by restoring random UUID startup generation and removing
    the required configuration field and documentation.

- [x] **AOC-002 — Make adapter-originated mutations cache-only in the broker**
  - Objective: short-circuit storage callback selection for every mutation from
    an adapter-authenticated connection.
  - Likely files: `valkyr-core/src/broker.rs`, `valkyr-server/src/lib.rs`.
  - Acceptance criteria: adapter-originated set, batch, delete, move, scheduled
    provider publication, and provider callback values return no `Persist*`
    dispatch and commit locally; normal clients retain durable-first behavior.
  - Tests: focused broker tests for all mutation forms and provider-value entry;
    existing persistence ordering/failure tests remain green.
  - Dependencies: AOC-001 only to keep the working diff coherent.
  - Risk: adapter identity is client-declared during authenticated `AUTH`; API
    keys granted adapter write access must therefore be treated as trusted.
  - Evidence: `Broker::prepare_mutation` commits immediately when the request
    context is adapter-authenticated; focused set/batch/delete/move coverage
    passes.

- [x] **AOC-003 — Remove obsolete UUID-based store selection**
  - Objective: make registry APIs express only route matching and newest-store
    precedence.
  - Likely files: `valkyr-core/src/registry.rs`, call sites in
    `valkyr-core/src/broker.rs`.
  - Acceptance criteria: store lookup has no source-adapter argument or
    identity-equality filter; `STORE` still requires an adapter-authenticated
    connection and remains connection-scoped.
  - Tests: replace the source-exclusion registry test with newest-match and
    batch-selection coverage.
  - Dependencies: AOC-002.
  - Risk: keep adapter UUIDs in request/connection context because they are still
    used to identify adapter connections and lease owners.

- [x] **AOC-004 — Add the two-server/two-adapter loop regression**
  - Objective: prove a client write causes exactly one persistence callback and
    one cache replication, even when both servers have stores registered by two
    different adapter UUIDs.
  - Likely files: `valkyr-server/tests/server_adapter.rs` or a focused server
    integration test module.
  - Acceptance criteria: write to `s1` is persisted once, forwarded to `s2`, is
    readable from `s2`, and does not trigger either adapter registered on `s2`;
    direct adapter writes to both servers trigger zero persistence callbacks.
  - Tests: use bounded timeouts and callback counters so the old ping-pong
    behavior fails deterministically without leaving runaway tasks.
  - Dependencies: AOC-002 and AOC-003.
  - Risk: asynchronous forwarding requires polling/event synchronization rather
    than timing-only assertions where practical. Existing server integration now
    uses two distinct adapter UUIDs and remains green.

- [x] **AOC-005 — Update architecture documentation and run verification**
  - Objective: document the new origin rule and verify the affected workspace.
  - Likely files: `valkyr-core/docs/feature_map.md`,
    `valkyr-server/docs/feature_map.md`, `valkyr-db-adapter/README.md`, and
    `valkyr-db-adapter/docs/feature_map.md`.
  - Acceptance criteria: docs distinguish normal write-through mutations from
    adapter-originated cache-only replication and do not claim UUID equality is
    loop suppression.
  - Tests: `cargo fmt --check`; targeted core, server, and DB-adapter tests;
    `cargo clippy --workspace --all-targets -- -D warnings` if practical.
  - Dependencies: AOC-001 through AOC-004.
  - Risk: none beyond keeping docs synchronized with final naming.

## Test Strategy

- Unit-test broker outcomes and cache state for adapter-originated set, batch,
  delete, move, and provider values.
- Preserve tests showing ordinary client writes still dispatch to stores and
  commit only after successful callbacks.
- Unit-test registry selection after removing UUID exclusion.
- Add a bounded integration regression with two servers and two distinct
  adapter identities, checking callback counts and replicated cache state.
- Run formatting, affected crate tests, workspace tests, and Clippy.

## Risks

- Any authenticated client that supplies an adapter UUID is considered an
  adapter-originated writer. This matches the current protocol model but makes
  correct API-key permissions important because such writes intentionally skip
  durable callbacks.
- Provider callback values are adapter-originated too; they will warm cache
  without being sent to a storage adapter. This is necessary for the same
  no-reflection invariant and should be covered explicitly.
- A regression test that relies only on sleeps could be flaky; callback counters
  and bounded channels/timeouts should make loop detection deterministic.
