"""Example provider and store adapter for Valkyr."""

import asyncio
import contextlib
from typing import Any

from valkyr import Adapter, AdapterClient, Provider, ProviderValue, Store


class UsersProvider(Provider):
    async def get(self, namespace: str, key: str) -> dict | ProviderValue | None:
        # In a real application, fetch from a database or upstream service.
        if key == "42":
            return ProviderValue({"name": "Ada"}, ttl_seconds=300)
        return None  # A miss; raise an exception to report a provider error.


class UsersStore(Store):
    async def set(
        self, namespace: str, key: str, value: Any, ttl_seconds: int | None = None
    ) -> None:
        print(f"persist set {namespace}/{key}: {value}")

    async def set_many(
        self,
        namespace: str,
        entries: list[dict],
        ttl_seconds: int | None = None,
    ) -> None:
        print(f"persist batch {namespace} (ttl={ttl_seconds}): {entries}")

    async def delete(self, namespace: str, key_pattern: str | None) -> None:
        print(f"persist delete {namespace}/{key_pattern}")

    async def move(self, source: str, destination: str) -> None:
        print(f"persist move {source} -> {destination}")


async def main() -> None:
    adapter = (
        Adapter()
        .provide("/users", "*", UsersProvider(), max_rate=100)
        .store("/users", "*", UsersStore())
    )

    client = await AdapterClient.connect(
        "127.0.0.1:8081",
        api_key="adapter-key",
        adapter=adapter,
    )
    with contextlib.suppress(asyncio.CancelledError):
        await client.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
