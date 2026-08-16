# Valkyr OpenBao Adapter

`valkyr-openbao-adapter --config adapter.yml` serves configured Valkyr
providers and write-through stores from OpenBao KV v2. It authenticates with
AppRole using a file-backed SecretID; the issued client token stays in memory.

Use an OpenBao policy limited to `read` and `list` for provider paths and
`create`, `update`, and `delete` for enabled store paths. KV v2 exact-key
deletion is a soft delete of the latest version; use mount `max_versions` or
metadata deletion outside the adapter when full erasure is required.

Under `valkyr:`, `request_timeout` is the adapter's own Valkyr connection and
request deadline and defaults to `"5s"`. Optional `provider_wait_timeout` and
`miss_cache_ttl` provide global defaults for query registrations. A query may
override either policy independently; precedence is query value, global value,
then zero. Values must be quoted duration strings with an explicit unit such as
`"15ms"`, `"5s"`, or `"1m"`; legacy names and bare numbers are rejected.
Provider wait values are converted exactly to the v1 wire field's integer
milliseconds, and miss-cache values exactly to integer seconds. Zero wait keeps
immediate-MISS behavior and zero miss-cache TTL disables negative caching.
The v1 wire fields remain named `timeout` and `miss_ttl`; those protocol names
are not the adapter YAML names. Applications issuing waiting GETs must set
their own request deadline above the route's provider wait timeout.

## Authentication and security-key storage

The adapter does not write its OpenBao login material to KV. It reads the
AppRole `SecretID` from `openbao.auth.secret_id_file`, sends it with the
configured `role_id` to `auth/approle/login`, and keeps the resulting client
token only in memory (including when it is renewed). Keep the SecretID file
outside the KV mount and mount it read-only into the adapter container.

All adapter values, including generated security keys, are KV v2 secrets below
`openbao.prefix`. Namespace and key segments use RFC 3986-style percent
encoding, so arbitrary Valkyr names cannot change the OpenBao path while
remaining readable. For a root namespace and key, the logical KV path is:

```text
<prefix>/values/<percent-encoded-namespace>/root/<percent-encoded-key>
```

For example, `/orders` and `signing-key` with `prefix: cache` are stored at
`cache/values/%2Forders/root/signing-key`. A key named `a/b` is stored as
`a%2Fb`. Store writes use this document
shape, where `value` is the original Valkyr JSON value and `ttl_seconds` is
optional:

```json
{"value": {"key": "..."}, "ttl_seconds": 3600}
```

The reserved `/__auth` namespace is intentionally different: raw API keys are
never used as OpenBao path segments. Each record is stored below
`<prefix>/values/%2F__auth/root/` with the SHA-256 digest of the API key split
into four 16-character lowercase hexadecimal segments:

```text
<prefix>/values/%2F__auth/root/<digest-1>/<digest-2>/<digest-3>/<digest-4>
```

The protected KV value contains the key needed to reconstruct provider values:

```json
{"key": "<api-key>", "value": {"role": "reader"}, "ttl_seconds": null}
```

The digest is only a deterministic locator, not password storage. It conceals
high-entropy keys from path and ordinary audit metadata while still revealing
equality; weak API keys remain guessable. OpenBao ACLs, TLS, storage
encryption, and audit redaction are still required. Scheduled `/__auth`
enumeration validates that each payload key hashes to its listed path before
publishing it. Missing, malformed, or mismatched records fail the sync.
Existing plaintext-path `/__auth` records are deliberately unsupported after
this early-stage breaking format change and are not migrated or read.

Enable `on_missing.generate_xchacha20poly1305_key` on a matching query to
create a missing key atomically. The adapter writes `value.key` as a newly
generated 32-byte XChaCha20-Poly1305 key encoded as 64 lowercase hexadecimal
characters; setting `record_created_unix_seconds` also writes
`value.created` as Unix seconds. Existing keys are returned unchanged.

Context namespaces use a separate collection under
`<prefix>/values/<percent-encoded-base-namespace>/contexts/<uuid>/...`. The adapter
maintains `<prefix>/indexes/<percent-encoded-base-namespace>`, a CAS-protected
`{"contexts":{"<context>":"<uuid>"}}` index, to resolve those paths.
Renaming a context changes only this index; it does not copy the key secrets.

This is a breaking storage-format change. The adapter does not read or migrate
values and context indexes stored at the previous base64url paths. Migrate or
remove those records before upgrading.

## Docker

Build from the workspace root. Mount the adapter configuration and the AppRole
SecretID/API-key files read-only; the image deliberately contains no
configuration or credentials.

```sh
docker build -f valkyr-openbao-adapter/Dockerfile -t valkyr-openbao-adapter .
docker run --rm \
  --mount type=bind,src=./openbao-adapter.yml,dst=/etc/valkyr/adapter.yml,readonly \
  --mount type=bind,src=./openbao-secret-id,dst=/run/secrets/openbao-secret-id,readonly \
  --mount type=bind,src=./valkyr-api-key,dst=/run/secrets/valkyr-api-key,readonly \
  valkyr-openbao-adapter
```

The image uses Google Distroless `cc-debian13`, so it has no shell or package
manager for interactive debugging. Mount certificates and secrets at the paths
referenced by the configuration file.

Context moves update a CAS-protected index rather than copying secrets. They
are enabled per store and fail if the destination context exists. The durable
move happens before Valkyr's cache commit; if the cache commit later fails,
move the context back or reconcile the cache before retrying.

```yaml
openbao:
  address: https://openbao.internal:8200
  kv_mount: valkyr
  prefix: cache
  auth:
    type: approle
    role_id: role-id
    secret_id_file: /run/secrets/openbao-secret-id
valkyr:
  endpoints:
    - url: tls://valkyr.internal:8081
      api_key_file: /run/secrets/valkyr-primary-key
      ca_certificate_file: /run/certs/valkyr.crt
    - url: tls://valkyr-replica.internal:8081
      api_key_file: /run/secrets/valkyr-replica-key
queries: {}
stores: {}
```

`valkyr.endpoints` is a replication set. Each entry has a URL, its own API-key
file, and an optional PEM CA file. Relative paths resolve from the adapter YAML
file. Valkyr CAs augment normal WebPKI verification and do not disable TLS
verification. The separate `openbao.ca_certificate_file` configures only the
OpenBao HTTP client. The adapter registers every configured
provider and store route with every endpoint. After OpenBao accepts a supported
store mutation, the adapter asynchronously forwards it to every other endpoint
with the same adapter identity, preventing a callback loop or duplicate
OpenBao write. Forwarding is best effort, has no retry or durable outbox, and
does not delay or alter the originating callback result.

## Scheduled providers

```yaml
providers:
  orders:
    namespace: /orders
    frequency: "0 */5 * * * *"
    run_on_startup: true
```

Providers list and read all KV v2 keys in the exact namespace, then write only
to servers where the adapter holds the local 30-second `/__lease`. Grant the
AppRole `list` and `read` access to the relevant metadata and data paths.
