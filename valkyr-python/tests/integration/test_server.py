"""Live integration tests against valkyr-server."""

import asyncio
from typing import Any

import pytest

from valkyr import (
    Adapter,
    AdapterClient,
    Client,
    Miss,
    Provider,
    ProviderValue,
    Store,
    Unknown,
    Value,
)
from valkyr._errors import ServerError

pytestmark = pytest.mark.integration


class InMemoryProvider(Provider):
    def __init__(self, values: dict[str, Any]):
        self.values = values

    async def get(self, namespace: str, key: str):
        return self.values.get(key)


class ExpiringProvider(Provider):
    def __init__(self):
        self.calls = 0

    async def get(self, namespace: str, key: str):
        self.calls += 1
        return ProviderValue({"calls": self.calls}, ttl_seconds=1)


class AuthProvider(Provider):
    async def get(self, namespace: str, key: str):
        if namespace != "/__auth" or key != "cold-reader-key":
            return None
        return {
            "client_id": "cold-reader",
            "name": "Cold reader",
            "permissions": [{"namespace": "/", "operations": ["read"]}],
        }


class InMemoryStore(Store):
    def __init__(self):
        self.data: dict[str, Any] = {}
        self.batches: list[tuple[str, list[dict], int | None]] = []
        self.deletes: list[tuple[str, str | None]] = []
        self.moves: list[tuple[str, str]] = []

    async def set(self, namespace: str, key: str, value: Any, ttl_seconds: int | None = None):
        self.data[key] = value

    async def set_many(
        self,
        namespace: str,
        entries: list[dict],
        ttl_seconds: int | None = None,
    ):
        self.batches.append((namespace, entries, ttl_seconds))
        for entry in entries:
            self.data[entry["key"]] = entry["value"]

    async def delete(self, namespace: str, key_pattern: str | None):
        self.deletes.append((namespace, key_pattern))
        if key_pattern is None or key_pattern == "*":
            self.data.clear()
        elif key_pattern in self.data:
            del self.data[key_pattern]

    async def move(self, source: str, destination: str):
        self.moves.append((source, destination))


class FailingStore(InMemoryStore):
    async def set(
        self, namespace: str, key: str, value: Any, ttl_seconds: int | None = None
    ) -> None:
        raise RuntimeError("durable store rejected the value")


class TestFluentClient:
    @pytest.mark.asyncio
    async def test_auth_warmup(self, valkyr_server):
        async with Client.connect(
            valkyr_server["host"],
            valkyr_server["native_port"],
            api_key=valkyr_server["bootstrap_key"],
            auth_timeout_secs=10.0,
        ) as client:
            await client.ping()

    @pytest.mark.asyncio
    async def test_cold_key_auth_warmup(self, valkyr_server):
        adapter = Adapter().provide("/__auth", "*", AuthProvider())
        adapter_client = await AdapterClient.connect(
            valkyr_server["host"],
            valkyr_server["native_port"],
            api_key=valkyr_server["bootstrap_key"],
            adapter=adapter,
            auth_timeout_secs=10.0,
        )
        serve_task = asyncio.create_task(adapter_client.serve_forever())
        try:
            await asyncio.sleep(0)
            async with Client.connect(
                valkyr_server["host"],
                valkyr_server["native_port"],
                api_key="cold-reader-key",
                auth_timeout_secs=10.0,
            ) as client:
                await client.ping()
        finally:
            await adapter_client.close()
            await asyncio.wait_for(serve_task, timeout=2.0)

    @pytest.mark.asyncio
    async def test_get_miss_set_get_delete(self, valkyr_server):
        async with Client.connect(
            valkyr_server["host"],
            valkyr_server["native_port"],
            api_key=valkyr_server["bootstrap_key"],
            auth_timeout_secs=10.0,
        ) as client:
            route = client.namespace("/test").key("hello")

            first = await route.get()
            assert first is Unknown

            await route.set({"name": "Ada"}, ttl_seconds=300)
            value = await route.get()
            assert isinstance(value, Value)
            assert value.value == {"name": "Ada"}

            await route.delete()
            after = await route.get()
            assert after is Unknown

    @pytest.mark.asyncio
    async def test_set_many(self, valkyr_server):
        async with Client.connect(
            valkyr_server["host"],
            valkyr_server["native_port"],
            api_key=valkyr_server["bootstrap_key"],
            auth_timeout_secs=10.0,
        ) as client:
            await client.namespace("/batch").set_many({"a": 1, "b": 2})
            assert (await client.namespace("/batch").key("a").get()).value == 1
            assert (await client.namespace("/batch").key("b").get()).value == 2

    @pytest.mark.asyncio
    async def test_context_move(self, valkyr_server):
        async with Client.connect(
            valkyr_server["host"],
            valkyr_server["native_port"],
            api_key=valkyr_server["bootstrap_key"],
            auth_timeout_secs=10.0,
        ) as client:
            src = client.namespace("/move::source").key("k")
            await src.set({"v": 1})
            await src.move("dest")
            dest = await client.namespace("/move::dest").key("k").get_value()
            assert dest == {"v": 1}

    @pytest.mark.asyncio
    async def test_ping_and_stats(self, valkyr_server):
        async with Client.connect(
            valkyr_server["host"],
            valkyr_server["native_port"],
            api_key=valkyr_server["bootstrap_key"],
            auth_timeout_secs=10.0,
        ) as client:
            await client.ping()
            stats = await client.stats()
            assert stats.stats.requests >= 0


class TestAdapter:
    @pytest.mark.asyncio
    async def test_provider_refresh(self, valkyr_server):
        provider = InMemoryProvider({"ada": {"name": "Ada"}})
        adapter = Adapter().provide("/warm", "*", provider)

        async with Client.connect(
            valkyr_server["host"],
            valkyr_server["native_port"],
            api_key=valkyr_server["bootstrap_key"],
            auth_timeout_secs=10.0,
        ) as client:
            adapter_client = await AdapterClient.connect(
                valkyr_server["host"],
                valkyr_server["native_port"],
                api_key=valkyr_server["bootstrap_key"],
                adapter=adapter,
                auth_timeout_secs=10.0,
            )
            serve_task = asyncio.create_task(adapter_client.serve_forever())
            try:
                first = await client.namespace("/warm").key("ada").get()
                assert isinstance(first, Miss)

                result: Value | Miss = Miss(0)
                for _ in range(50):
                    result = await client.namespace("/warm").key("ada").get()
                    if isinstance(result, Value):
                        break
                    await asyncio.sleep(0.05)
                assert isinstance(result, Value)
                assert result.value == {"name": "Ada"}
            finally:
                await adapter_client.close()
                await asyncio.wait_for(serve_task, timeout=2.0)

    @pytest.mark.asyncio
    async def test_provider_value_ttl_expires_and_refreshes(self, valkyr_server):
        provider = ExpiringProvider()
        adapter = Adapter().provide("/expiring", "*", provider)

        async with Client.connect(
            valkyr_server["host"],
            valkyr_server["native_port"],
            api_key=valkyr_server["bootstrap_key"],
            auth_timeout_secs=10.0,
        ) as client:
            adapter_client = await AdapterClient.connect(
                valkyr_server["host"],
                valkyr_server["native_port"],
                api_key=valkyr_server["bootstrap_key"],
                adapter=adapter,
                auth_timeout_secs=10.0,
            )
            serve_task = asyncio.create_task(adapter_client.serve_forever())
            try:
                route = client.namespace("/expiring").key("temperature")
                assert isinstance(await route.get(), Miss)
                first: Value | Miss = Miss(0)
                for _ in range(50):
                    first = await route.get()
                    if isinstance(first, Value):
                        break
                    await asyncio.sleep(0.05)
                assert isinstance(first, Value)
                assert first.ttl_seconds == 1
                assert first.value == {"calls": 1}

                await asyncio.sleep(1.1)
                refreshed: Value | Miss = Miss(0)
                for _ in range(50):
                    refreshed = await route.get()
                    if isinstance(refreshed, Value) and refreshed.value == {"calls": 2}:
                        break
                    await asyncio.sleep(0.05)
                assert isinstance(refreshed, Value)
                assert refreshed.value == {"calls": 2}
            finally:
                await adapter_client.close()
                await asyncio.wait_for(serve_task, timeout=2.0)

    @pytest.mark.asyncio
    async def test_store_write_through(self, valkyr_server):
        store = InMemoryStore()
        adapter = Adapter().store("/durable", "*", store)

        async with Client.connect(
            valkyr_server["host"],
            valkyr_server["native_port"],
            api_key=valkyr_server["bootstrap_key"],
            auth_timeout_secs=10.0,
        ) as client:
            adapter_client = await AdapterClient.connect(
                valkyr_server["host"],
                valkyr_server["native_port"],
                api_key=valkyr_server["bootstrap_key"],
                adapter=adapter,
                auth_timeout_secs=10.0,
            )
            serve_task = asyncio.create_task(adapter_client.serve_forever())
            try:
                await client.namespace("/durable").key("x").set({"v": 1})
                assert store.data["x"] == {"v": 1}
            finally:
                await adapter_client.close()
                await asyncio.wait_for(serve_task, timeout=2.0)

    @pytest.mark.asyncio
    async def test_store_batch_delete_move(self, valkyr_server):
        store = InMemoryStore()
        adapter = Adapter().store("/durable", "*", store)

        async with Client.connect(
            valkyr_server["host"],
            valkyr_server["native_port"],
            api_key=valkyr_server["bootstrap_key"],
            auth_timeout_secs=10.0,
        ) as client:
            adapter_client = await AdapterClient.connect(
                valkyr_server["host"],
                valkyr_server["native_port"],
                api_key=valkyr_server["bootstrap_key"],
                adapter=adapter,
                auth_timeout_secs=10.0,
            )
            serve_task = asyncio.create_task(adapter_client.serve_forever())
            try:
                namespace = client.namespace("/durable")
                await namespace.set_many({"a": 1, "b": 2})
                assert store.batches == [
                    ("/durable", [{"key": "a", "value": 1}, {"key": "b", "value": 2}], None)
                ]

                await namespace.delete("a")
                assert store.deletes == [("/durable", "a")]

                source = client.namespace("/durable::draft").key("moved")
                await source.set({"v": 1})
                await source.move("published")
                assert store.moves == [("/durable::draft", "/durable::published")]
            finally:
                await adapter_client.close()
                await asyncio.wait_for(serve_task, timeout=2.0)

    @pytest.mark.asyncio
    async def test_store_handler_failure_does_not_commit(self, valkyr_server):
        store = FailingStore()
        adapter = Adapter().store("/failure", "*", store)

        async with Client.connect(
            valkyr_server["host"],
            valkyr_server["native_port"],
            api_key=valkyr_server["bootstrap_key"],
            auth_timeout_secs=10.0,
        ) as client:
            adapter_client = await AdapterClient.connect(
                valkyr_server["host"],
                valkyr_server["native_port"],
                api_key=valkyr_server["bootstrap_key"],
                adapter=adapter,
                auth_timeout_secs=10.0,
            )
            serve_task = asyncio.create_task(adapter_client.serve_forever())
            try:
                with pytest.raises(ServerError):
                    await client.namespace("/failure").key("x").set({"v": 1})
                assert await client.namespace("/failure").key("x").get() is Unknown
            finally:
                await adapter_client.close()
                await asyncio.wait_for(serve_task, timeout=2.0)

    @pytest.mark.asyncio
    async def test_disconnect_re_registers_and_continues(self, valkyr_server):
        store = InMemoryStore()
        adapter = Adapter().store("/reconn", "*", store)

        async with Client.connect(
            valkyr_server["host"],
            valkyr_server["native_port"],
            api_key=valkyr_server["bootstrap_key"],
            auth_timeout_secs=10.0,
        ) as client:
            adapter_client = await AdapterClient.connect(
                valkyr_server["host"],
                valkyr_server["native_port"],
                api_key=valkyr_server["bootstrap_key"],
                adapter=adapter,
                auth_timeout_secs=10.0,
                reconnect_delay_min=0.1,
                reconnect_delay_max=1.0,
            )
            serve_task = asyncio.create_task(adapter_client.serve_forever())
            try:
                await client.namespace("/reconn").key("x").set({"v": 1})
                assert store.data["x"] == {"v": 1}

                old_transport = adapter_client._transport
                assert old_transport is not None
                await old_transport.close()
                for _ in range(50):
                    current_transport = adapter_client._transport
                    if (
                        current_transport is not None
                        and current_transport is not old_transport
                        and not current_transport.closed
                    ):
                        break
                    await asyncio.sleep(0.1)
                else:
                    pytest.fail("adapter did not reconnect and re-register within 5 seconds")

                await client.namespace("/reconn").key("y").set({"v": 2})
                assert store.data["y"] == {"v": 2}
            finally:
                await adapter_client.close()
                await asyncio.wait_for(serve_task, timeout=2.0)
