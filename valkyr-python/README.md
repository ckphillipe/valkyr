# valkyr-python

Python 3.11+ SDK for the Valkyr native human-readable text protocol (protocol v1).

This package is a standalone sibling of the Rust workspace. It provides an
async-first fluent client and a streaming adapter client for providers and
durable stores.

## Installation

```sh
pip install valkyr
```

Requires Python 3.11 or newer.

## Compatibility

- Python SDK `0.1.x` targets Valkyr server `0.1.x` and protocol v1.
- Go and Java SDKs are deferred and not part of this release.

## Fluent application client

```python
import asyncio
from valkyr import Client, Miss, Value


async def main():
    async with Client.connect("127.0.0.1:8081", api_key="app-key") as client:
        user = client.namespace("/users").key("42")

        await user.set({"name": "Ada"}, ttl_seconds=300)
        result = await user.get_with_retry()  # waits once if a provider is warming
        if isinstance(result, Value):
            print(result.value)
        elif isinstance(result, Miss):
            print(f"provider warming: retry after {result.retry_after_ms}ms")
        else:
            print("value is absent")

        await client.namespace("/sessions").set_many(
            {
                "session-1": {"user_id": "42"},
                "session-2": {"user_id": "43"},
            }
        )


asyncio.run(main())
```

## Provider and store adapter

```python
import asyncio
import contextlib
from valkyr import Adapter, AdapterClient, Provider, ProviderValue, Store


class UsersProvider(Provider):
    async def get(self, namespace: str, key: str):
        if key == "42":
            return ProviderValue({"name": "Ada"}, ttl_seconds=300)
        return None  # a provider miss; raw values also remain supported


class UsersStore(Store):
    async def set(self, namespace, key, value, ttl_seconds=None): ...

    async def set_many(self, namespace, entries, ttl_seconds=None): ...

    async def delete(self, namespace, key_pattern): ...

    async def move(self, source, destination): ...


adapter = (
    Adapter()
    .provide("/users", "*", UsersProvider(), max_rate=100, timeout=250, miss_ttl=30)
    .store("/users", "*", UsersStore())
)


async def main():
    client = await AdapterClient.connect(
        "127.0.0.1:8081",
        api_key="adapter-key",
        adapter=adapter,
    )
    with contextlib.suppress(asyncio.CancelledError):
        await client.serve_forever()


asyncio.run(main())
```

`ProviderValue` controls how long Valkyr caches a successful provider result;
it does not refresh the upstream source. Its TTL is optional, non-negative,
and expressed in whole seconds. Provider exceptions, callback timeouts,
cancellation, and overloads return errors without a value TTL. Store mutation
TTLs are forwarded separately, while `miss_ttl` controls provider misses and
`timeout` controls callback execution.

## TLS

Use `tls=True` for system trust roots, or pass a custom CA and server name:

```python
async def main():
    async with Client.connect(
        "127.0.0.1:8443",
        api_key="app-key",
        tls={"ca": "/path/to/ca.pem", "server_hostname": "localhost"},
    ) as client:
        await client.ping()


asyncio.run(main())
```

Client certificates and insecure verification bypass are not supported.

## Reconnect

`AdapterClient` automatically reconnects with capped exponential backoff and
restores registrations after authentication. Supply an explicit
`adapter_instance` UUID to preserve identity across process restarts.

## Development

```sh
pip install -e "valkyr-python/[dev]"
python3 -m pytest valkyr-python/tests/unit/ -v
```

Integration tests require a local Rust build of `valkyr-server`:

```sh
cargo build -p valkyr-server
python3 -m pytest valkyr-python/tests/integration/ -v
```

The integration fixture builds `valkyr-server` automatically when the debug
binary is not already available.
