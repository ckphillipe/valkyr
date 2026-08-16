# valkyr-client

An async, cloneable TCP client for Valkyr. A cloned client serializes requests
on its shared connection, preserving the request/response order of the native
human-readable text protocol.

Streaming adapters can use `provide_with_options` with core
`ProvideOptions.timeout_ms` and `ProvideOptions.miss_ttl_seconds`; both
default to zero.
Keep the ordered client's request timeout greater than the largest provider
wait timeout, or a late response can poison the connection and require a
reconnect. This request deadline is separate from `timeout_ms` transport
configuration in bundled adapters.

For server setup, authentication, and transport behavior, see the
[workspace README](../README.md) and [architecture guide](../docs/architecture.md).

```no_run
use valkyr_client::Client;
use valkyr_core::{Key, Namespace};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let client = Client::connect("127.0.0.1:8081").await?;
client.ping().await?;
let value = client.get(Namespace::new("/example")?, Key::new("answer")?).await?;
# Ok(())
# }
```
# C ABI

Enable the optional `capi` feature to build a `cdylib` with `valkyr_client_new`,
`valkyr_client_get`, `valkyr_client_set`, `valkyr_client_delete`,
`valkyr_client_move`, and matching
free functions. Returned strings are owned by the caller and must be released
with `valkyr_string_free`.

```sh
cargo build -p valkyr-client --release --features capi
```
