# Security model

## Credentials

Every configured server needs a bootstrap API key file. That key is the
control-plane administrator: it can manage `/__auth` and reserved adapters.
Generate it with a cryptographically secure source, keep it out of Git and
environment variables, mount it read-only, and rotate it under a documented
operational procedure.

Provision separate, least-privilege credentials for applications and adapters.
Non-bootstrap credentials are loaded from a `/__auth` provider on first use;
clients should retry the documented pending response rather than treating it as
an authorization grant or a permanent failure.

The OpenBao adapter stores `/__auth` records under four fixed SHA-256 digest
path segments, not raw API-key paths. The protected KV payload also stores the
original key so scheduled synchronization can recover it and validate that the
payload matches its digest locator. This conceals high-entropy keys from path
metadata but reveals equality and does not make weak keys safe. Existing
plaintext-path `/__auth` records are intentionally unsupported after the
format cutover; plan a fresh provider dataset rather than relying on fallback
reads or migration.

## Encryption

`~key~` addresses request encrypted values. Valkyr obtains key records from a
registered `/__secrets` provider, caches them, and binds ciphertext to the
base namespace and key. Use durable, access-controlled storage for those
records and test backup/restore before enabling encrypted data in production.
When upgrading from an earlier release, migrate existing records to
`/__secrets` before encrypted requests are served; new keys cannot decrypt
existing ciphertext.

Encryption protects values handled by Valkyr; it does not replace TLS, database
access controls, secret management, authorization design, or operational
monitoring. Enable TLS on untrusted networks and restrict access to native,
HTTP, WebSocket, metrics, database, and secret-mount endpoints.

## Deployment checklist

- Generate unique bootstrap and application keys; do not use checked-in sample values.
- Grant only the namespaces and operations each client needs.
- Store bootstrap/TLS material in read-only secret mounts.
- Use TLS outside trusted local networks.
- Persist and back up `/__auth` and `/__secrets` provider data.
- Restrict or isolate the unauthenticated metrics listener.
- Monitor authentication failures, provider callback failures, and storage errors.

To report a vulnerability, use GitHub's private vulnerability reporting for
this repository. See [SECURITY.md](../SECURITY.md).
