# Code Review

## Executive Summary

The central routing change is simple and correctly gates adapter-originated
mutations in `Broker::prepare_mutation`. Provider publication tests and the
two-server/two-adapter regression now match the cache-only contract. The
benchmark-compatible imports also resolve the prior all-targets Clippy issue;
focused and full workspace verification pass with no blocking findings.

## Top Recommendations

- Keep provider-refresh assertions aligned with the cache-only invariant.
- Keep callback counters and direct adapter mutation coverage in the
  two-server/two-adapter regression.

## Detailed Findings

### [High] Existing provider durability tests fail under the new contract

**Finding:** `CR-F001`

**Location:** `valkyr-server/src/lib.rs`,
`waiting_value_is_published_only_after_durable_acceptance` and
`durable_failure_does_not_publish_or_cache_a_provider_value`

**Disposition:** Blocking

**Problem:** The implementation deliberately treats values returned by an
adapter provider as cache-only, but these tests still require the provider
value to wait for a storage callback and expect a failing storage callback to
produce a miss. `cargo test --workspace --all-targets` currently fails both
tests: the first times out waiting for the old durable ordering, and the
second receives a cached value instead of `Response::Miss`.

**Why it matters:** A required workspace verification command is red, and the
repository's tests contradict the confirmed invariant that all adapter-origin
mutations, including provider publications, must avoid `Persist*` dispatch.
Leaving the old tests in place makes the intended behavior appear broken and
blocks safe finalization.

**Suggested fix:** Rewrite the tests around the new invariant. For a provider
publication, assert the waiting request completes without waiting for a store
callback, the local cache contains the value, and a storage handler receives
no callback. For the failing-storage fixture, assert the provider value is
still readable from cache and no durable callback was attempted (or rename the
test to reflect cache-only behavior). Keep separate ordinary-client tests for
durable-first failure semantics.

**Resolution:** Resolved by `CR-T001`; the provider tests now assert immediate
cache visibility and zero persistence callbacks.

### [Medium] The integration regression does not verify callback suppression

**Finding:** `CR-F002`

**Location:** `valkyr-server/tests/server_adapter.rs`,
`configured_database_adapter_serves_auth_storage_encryption_and_http` and
`Fixture`

**Disposition:** Blocking

**Problem:** The fixture now creates two adapters with distinct UUIDs and
checks that a normal client write appears in the replica cache, but it never
counts storage callbacks. It therefore does not prove that the write is
persisted exactly once, that the replica adapter is not called, or that a
direct adapter write to each server produces zero callbacks. It also does not
exercise the direct adapter-to-both-server case required by the plan.

**Why it matters:** The test can pass even if a replicated write still
invokes one of the adapters, or if direct adapter writes continue to ping-pong
through storage. Those are the precise regressions this plan is meant to
prevent.

**Suggested fix:** Add bounded callback counters (or an equivalent instrumented
storage bridge) for both adapters. Assert a normal client write causes one
durable callback and one replica-cache update with no additional callback;
then issue set/batch/delete/move commands through each adapter connection to
both servers and assert zero persistence callbacks while checking the target
cache state. Use polling/timeouts rather than unbounded sleeps.

**Resolution:** Resolved by `CR-T002`; the regression now has deterministic
counters and direct set, batch, delete, and move cache assertions across both
servers.

### [High] Shared benchmark module has unused conditional imports

**Finding:** `CR-F003`

**Location:** `valkyr-server/tests/server_adapter.rs`, imports at the top of
the shared integration-test/benchmark module

**Disposition:** Blocking

**Problem:** `Ordering` and `SetEntry` are guarded with `#[cfg(test)]`, but
`valkyr-server/benches/server_adapter.rs` includes this file directly in a
non-test benchmark target. The imports are therefore unused in that target,
and `cargo clippy --workspace --all-targets -- -D warnings` fails with two
`unused_imports` errors.

**Why it matters:** The plan requires all-targets verification where practical,
and the changed regression test must remain compatible with its existing
benchmark reuse. A clean test run alone does not establish a clean workspace.

**Suggested fix:** Remove the `#[cfg(test)]` attributes from these imports (or
otherwise make the shared module's imports valid in both test and benchmark
compilations), then rerun all-targets Clippy.

**Resolution:** Resolved by `CR-T003`; the imports are scoped so both the
integration test and benchmark include compile without warnings.

## Simplicity Review

The boolean origin marker and one common early return in
`Broker::prepare_mutation` are appropriately narrow. Removing UUID-based
store filtering also removes dead routing state without adding new protocol
surface.

## Performance Review

The adapter-originated path commits directly and avoids registry lookup and
callback I/O, which removes the loop's unbounded work. No new allocations or
network round trips are introduced on ordinary client writes.

## Design Review

Responsibilities remain well placed: authentication establishes adapter
origin, the broker owns mutation policy, and the registry only selects stores.
The provider refresh path correctly passes the originating connection's
adapter status into the same broker gate. The tests are aligned with that
design.

## Testing Review

`git diff --check`, `cargo fmt --all -- --check`,
`cargo clippy --workspace --all-targets -- -D warnings`,
`cargo test --workspace --all-targets`, `cargo test -p valkyr-core`,
`cargo test -p valkyr-server --lib`, and
`cargo test -p valkyr-server --test server_adapter` pass.
The provider tests now assert immediate cache visibility and zero callbacks;
the integration test has deterministic counters and direct set, batch, delete,
and move cache assertions on both servers.

## Implementation Tasks

- [x] **CR-T001 — Update provider publication tests for cache-only adapter origin**
  - **Objective:** Make provider-refresh tests assert the approved cache-only
    behavior and restore a green workspace test suite.
  - **Addresses:** `CR-F001`
  - **Likely files/modules:** `valkyr-server/src/lib.rs`
  - **Acceptance criteria:** Provider values returned on adapter connections
    complete without a storage callback; values are visible in the local
    cache; ordinary client write-through failure tests retain their old
    durable-first assertions.
  - **Tests:** `cargo test -p valkyr-server --lib`; `cargo test --workspace --all-targets`.
  - **Dependencies:** None
  - **Risks:** Avoid weakening ordinary-client durability and failure coverage
    while changing only adapter-origin provider expectations.
  - **Evidence:** Provider publication tests now complete without waiting for
    persistence, assert the value is cached, and verify zero persistence
    callbacks. `cargo test -p valkyr-server --lib` passes.

- [x] **CR-T002 — Strengthen the two-server adapter loop regression**
  - **Objective:** Verify callback counts and direct adapter writes for the
    two-server/two-adapter topology.
  - **Addresses:** `CR-F002`
  - **Likely files/modules:** `valkyr-server/tests/server_adapter.rs`,
    adapter test bridge/callback instrumentation
  - **Acceptance criteria:** A normal client write causes exactly one storage
    callback and reaches the other server's cache; replicated and direct
    adapter-originated writes cause zero additional storage callbacks; direct
    set, batch, delete, and move cache effects are asserted with bounded
    synchronization.
  - **Tests:** `cargo test -p valkyr-server --test server_adapter`.
  - **Dependencies:** None
  - **Risks:** Keep callback counters deterministic and prevent the regression
    test from creating runaway forwarding tasks on failure.
  - **Evidence:** The two-server fixture counts persistence callbacks, asserts
    one callback for a normal client write and replica-cache visibility, then
    covers direct adapter set, batch, delete, and move on both servers with no
    additional callbacks and local cache assertions. `cargo test -p
    valkyr-server --test server_adapter` passes.

- [x] **CR-T003 — Make shared regression imports benchmark-compatible**
  - **Objective:** Restore a warning-free all-targets build for the shared
    server adapter integration test and benchmark module.
  - **Addresses:** `CR-F003`
  - **Likely files/modules:** `valkyr-server/tests/server_adapter.rs`
  - **Acceptance criteria:** `Ordering` and `SetEntry` compile as used by both
    the integration test and benchmark include; no unused-import warning
    remains.
  - **Tests:** `cargo clippy --workspace --all-targets -- -D warnings`;
    `cargo test --workspace --all-targets`.
  - **Dependencies:** None
  - **Risks:** Preserve benchmark reuse of the integration fixture while
    changing only import configuration.
  - **Evidence:** Moved test-only `Ordering` and `SetEntry` imports into the
    integration test function so the benchmark include has no unused imports;
    `cargo clippy --workspace --all-targets -- -D warnings` and
    `cargo test --workspace --all-targets` pass.

## Final Verdict

`PASS`

Blocking findings: None. `CR-F001`, `CR-F002`, and `CR-F003` are resolved by
checked tasks `CR-T001`, `CR-T002`, and `CR-T003` respectively. No remaining
implementation tasks or findings.
