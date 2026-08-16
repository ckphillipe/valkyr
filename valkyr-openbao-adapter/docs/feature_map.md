# valkyr-openbao-adapter — Feature Map

Native Valkyr callbacks use the shared human-readable text protocol; JSON is
limited to OpenBao HTTP and typed value payloads.

Bridges Valkyr cache-miss and write-through callbacks to OpenBao KV v2.

`client.rs` owns AppRole authentication, token renewal and KV v2 HTTP calls.
OpenBao HTTPS uses Reqwest's native TLS backend so the host OpenSSL trust store
and explicitly configured `openbao.ca_certificate_file` roots are both
available. That OpenBao HTTP CA is separate from per-endpoint Valkyr CAs.
`mapping.rs` maps ordinary logical Valkyr routes to human-readable, RFC
3986-style percent-encoded OpenBao path segments and decodes provider keys.
The exact reserved `/__auth` root uses a separate SHA-256 locator codec: four
fixed 16-character digest segments identify a record, while the protected KV
document carries the original API key for provider synchronization.
`bridge.rs` selects configured providers/stores, handles supported callbacks,
and composes endpoint-aware best-effort forwarding after successful OpenBao
writes. `main.rs` maintains one streaming callback connection per configured
Valkyr endpoint; all use one adapter UUID so forwarded mutations commit at a
replica without re-entering this adapter.
`config.rs` parses and validates YAML plus file-backed credentials. Valkyr
endpoints use structured URL/key/optional-CA entries; paths are relative to the
adapter YAML and native Valkyr CAs augment WebPKI roots. `main.rs`
registers routes and restores the streaming callback connection after drops;
Ctrl-C interrupts both initial connection and restoration retries. A restored
callback connection triggers one immediate, lease-coordinated provider sync;
cron schedules continue independently.

V1 supports PROVIDE, set, exact-key soft delete, and opt-in same-base context
move. It rejects batches, wildcard deletes, namespace deletes, and moves that
are not explicitly enabled. Context values use a CAS-protected index. Forwarded
set, exact delete, and enabled context move requests are asynchronous and have
no retry or durable outbox, so a temporarily unavailable endpoint can miss an
update without affecting the local OpenBao write.

Configured `providers` list every KV v2 value in one exact namespace and sync
only servers where this adapter owns the local 30-second `/__lease`. Provider
policies require OpenBao `list` as well as `read` capability for those paths.
The `/__auth` provider recursively lists its four digest levels and validates
each key/digest pair before publishing; malformed or mismatched records are
rejected without partial publication. Other namespaces retain one-level
percent-decoded listing.

## Container image

`Dockerfile` builds the adapter from the workspace root into a Google
Distroless `cc-debian13` runtime image. It starts with
`--config /etc/valkyr/adapter.yml` and exposes no ports because it connects to
Valkyr and OpenBao as a client. GitHub Actions CI builds the image without
publishing it.

Optional `valkyr.provider_wait_timeout` and `valkyr.miss_cache_ttl` defaults
resolve independently against each query override, then zero, and are restored
by every endpoint registration/reconnect path; config and callback tests cover
explicit-zero overrides, partial inheritance, multiple endpoints, and
restoration capture. `valkyr.request_timeout` defaults to five seconds and is
used directly for Valkyr transport setup. YAML durations require explicit units
and convert losslessly to the existing integer v1 `timeout` and `miss_ttl`
fields.
