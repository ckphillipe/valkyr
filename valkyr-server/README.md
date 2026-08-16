# valkyr-server

The native Valkyr TCP server. It starts native TCP, REST/WebSocket, and
Prometheus metrics automatically. Defaults bind safely to loopback:

- Native TCP: `127.0.0.1:8081`
- REST/WebSocket: `127.0.0.1:8080`
- Metrics and health: `127.0.0.1:8090` (`GET /metrics`, `GET /health`)

Configure it with a YAML file passed explicitly to `--config`; authentication
is mandatory, so the file must include `auth.bootstrap_api_key_file`. The
binary does not read `VALKYR_*` or `RUST_LOG` environment variables.

```sh
cargo run -p valkyr-server -- --config ./valkyr-server.yml
```

```yaml
native_listen: 0.0.0.0:8081
http_listen: 0.0.0.0:8080
metrics_listen: 0.0.0.0:8090
log_filter: info
tls:
  listen: 0.0.0.0:8443
  certificate_file: /run/valkyr/tls/tls.crt
  private_key_file: /run/valkyr/tls/tls.key
auth:
  bootstrap_api_key_file: /run/valkyr/secrets/bootstrap-api-key
  session_ttl_seconds: 3600
cache:
  max_capacity: 100000
  time_to_idle_seconds: 3600
```

All top-level fields except `auth` are optional. The
`auth.bootstrap_api_key_file` is required; `auth.session_ttl_seconds` defaults
to 3600 and must be at least 2. Unknown fields fail startup. Optional
`cache.max_capacity` limits the number of in-memory entries and
`cache.time_to_idle_seconds` enables Moka-managed idle eviction; both are unset
by default. Command TTLs remain per-entry and an entry expires at the earliest
applicable command or idle deadline. Capacity eviction is Moka-managed (TinyLFU
admission with LRU eviction), not strict LRU. TLS and bootstrap secrets are
file paths, allowing read-only mounts without placing secret values
in configuration or environment variables. Relative secret and TLS paths are
resolved from the configuration file's directory.
The `tls.listen` listener is for the native TCP text protocol only. The
REST and WebSocket endpoints on `http_listen` remain HTTP and WS; use an HTTPS
reverse proxy to provide HTTPS/WSS for those endpoints.

The bootstrap key is the control-plane administrator. Use it to provision
`/__auth` records, then clients authenticate with the API keys represented by
those records. Keep the bootstrap key in a read-only, access-restricted secret
mount; losing it prevents control-plane changes until it is restored.

For an end-to-end local setup and the authentication warm-up behavior, start at
the [workspace quick start](../README.md#quick-start). Deployment and secret
handling guidance is in the [operations guide](../docs/operations.md).

## Docker and Kubernetes

Build the image from the workspace root, mount the configuration and secret
files read-only, and pass the same argument in both environments:

```sh
docker build -f valkyr-server/Dockerfile -t valkyr-server:latest .
docker run --rm -p 8081:8081 -p 8080:8080 -p 8090:8090 \
  --mount type=bind,src=./example,dst=/etc/valkyr,readonly \
  valkyr-server:latest --config /etc/valkyr/valkyr-server.yml
```

The Kubernetes example at `deploy/kubernetes.yaml` mounts YAML from a
ConfigMap and a required bootstrap-key Secret. Create that Secret outside the
manifest; mount TLS certificates as a separate Secret when enabling TLS.

## Logging

The binary emits structured `tracing` events for listener binding, native
connections, callback failures, provider refreshes, and shutdown. Set
`log_filter` in YAML, for example `valkyr_server=debug,valkyr_core=debug`.

For embedding, use `Server::in_memory()` or construct a `Broker` and pass it
to `Server::with_broker`. The library API has no process exit behavior, making
it suitable for tests and supervised runtimes.
