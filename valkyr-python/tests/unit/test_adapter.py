"""Unit tests for the streaming adapter client."""

import asyncio
from typing import Any

import pytest

from valkyr import Adapter, AdapterClient, Provider, ProviderValue, Store
from valkyr._errors import ConnectionError
from valkyr._transport import Transport
from valkyr._wire import (
    Ok,
    OperationResult,
    PersistDelete,
    PersistMove,
    PersistSet,
    PersistSetBatch,
    Query,
    QueryResult,
    ServerCommand,
    ServerResult,
    SetEntry,
    server_command_from_line,
    server_command_to_line,
    server_result_to_line,
)


class FakeTransport:
    """Test double with the same public surface as Transport."""

    def __init__(self, read_lines: list[str | Exception]):
        self.read_lines = list(read_lines)
        self.write_results: list[tuple[ServerCommand, ServerResult]] = []
        self.closed_state = False
        self.requests: list[Any] = []

    async def read_command(self) -> ServerCommand:
        if not self.read_lines:
            raise ConnectionError("no more frames")
        line = self.read_lines.pop(0)
        if isinstance(line, Exception):
            raise line
        return server_command_from_line(line)

    async def request(self, cmd: Any) -> Any:
        self.requests.append(cmd)
        from valkyr._wire import Auth as WireAuth

        if isinstance(cmd, WireAuth):
            from valkyr._wire import AuthSuccess

            return AuthSuccess(client_id="test", session_ttl_seconds=3600)
        return Ok()

    async def write_result(self, command: ServerCommand, result: ServerResult) -> None:
        self.write_results.append((command, result))

    async def close(self) -> None:
        self.closed_state = True

    @property
    def closed(self) -> bool:
        return self.closed_state


def _written_result_line(transport: FakeTransport, index: int = 0) -> str:
    command, result = transport.write_results[index]
    return server_result_to_line(command, result)


class MemoryProvider(Provider):
    def __init__(self, value: dict | None = None):
        self.value = value

    async def get(self, namespace: str, key: str) -> dict | None:
        return self.value


class MemoryStore(Store):
    def __init__(self):
        self.sets: list[tuple] = []
        self.batches: list[tuple] = []
        self.deletes: list[tuple] = []
        self.moves: list[tuple] = []

    async def set(
        self, namespace: str, key: str, value: Any, ttl_seconds: int | None = None
    ) -> None:
        self.sets.append((namespace, key, value, ttl_seconds))

    async def set_many(
        self,
        namespace: str,
        entries: list[dict],
        ttl_seconds: int | None = None,
    ) -> None:
        self.batches.append((namespace, entries, ttl_seconds))

    async def delete(self, namespace: str, key_pattern: str | None) -> None:
        self.deletes.append((namespace, key_pattern))

    async def move(self, source: str, destination: str) -> None:
        self.moves.append((source, destination))


class TestAdapterRegistration:
    def test_provider_options_match_protocol_ranges(self):
        for max_rate in (0, 2**32):
            with pytest.raises(ValueError):
                Adapter().provide("/users", "*", MemoryProvider(), max_rate=max_rate)
        for timeout, miss_ttl in ((-1, None), (None, -1)):
            with pytest.raises(ValueError):
                Adapter().provide(
                    "/users",
                    "*",
                    MemoryProvider(),
                    timeout=timeout,
                    miss_ttl=miss_ttl,
                )

    @pytest.mark.asyncio
    async def test_endpoint_failover(self, monkeypatch):
        transport = FakeTransport([])
        attempts: list[tuple[str, int]] = []

        async def fake_open(host, port, **kwargs):
            attempts.append((host, port))
            if host == "bad":
                raise ConnectionError("unavailable")
            return transport

        monkeypatch.setattr(Transport, "open", fake_open)

        client = await AdapterClient.connect(
            ["bad:1001", "good:1002"],
            adapter=Adapter(),
            api_key="key",
        )
        await client.close()

        assert attempts == [("bad", 1001), ("good", 1002)]

    @pytest.mark.asyncio
    async def test_registers_provider_and_store(self, monkeypatch):
        transport = FakeTransport([])

        async def fake_open(*args, **kwargs):
            return transport

        monkeypatch.setattr(Transport, "open", fake_open)

        adapter = (
            Adapter()
            .provide("/users", "*", MemoryProvider(), max_rate=100)
            .store("/users", "*", MemoryStore())
        )
        client = await AdapterClient.connect("127.0.0.1", adapter=adapter, api_key="key")
        task = asyncio.create_task(client.serve_forever())
        await asyncio.sleep(0.05)
        await client.close()
        await asyncio.wait_for(task, timeout=1.0)

        requests = [cmd.__class__.__name__ for cmd in transport.requests]
        assert "Provide" in requests
        assert "Store" in requests

    @pytest.mark.asyncio
    async def test_provider_options_are_preserved_during_registration(self, monkeypatch):
        transport = FakeTransport([])

        async def fake_open(*args, **kwargs):
            return transport

        monkeypatch.setattr(Transport, "open", fake_open)
        adapter = Adapter().provide("/users", "*", MemoryProvider(), timeout=250, miss_ttl=30)
        client = await AdapterClient.connect("127.0.0.1", adapter=adapter, api_key="key")
        await client.close()
        provide = next(
            command for command in transport.requests if command.__class__.__name__ == "Provide"
        )
        assert provide.timeout == 250
        assert provide.miss_ttl == 30

    @pytest.mark.asyncio
    async def test_registration_snapshot_ignores_source_mutation(self, monkeypatch):
        transport = FakeTransport([])

        async def fake_open(*args, **kwargs):
            return transport

        monkeypatch.setattr(Transport, "open", fake_open)
        provider = MemoryProvider({"source": "original"})
        store = MemoryStore()
        adapter = (
            Adapter()
            .provide("/users", "*", provider, max_rate=7, timeout=250, miss_ttl=30)
            .store("/durable", "*", store)
        )
        client = await AdapterClient.connect("127.0.0.1", adapter=adapter, api_key="key")
        assert client._provide_routes[0].provider is provider
        assert client._store_routes[0].store is store

        adapter.provide("/new", "*", MemoryProvider(), max_rate=9)
        adapter.store("/other", "*", MemoryStore())
        transport.requests.clear()
        await client._register(transport)

        assert client._find_provider("/new", "key") is None
        assert (
            client._find_store(
                "/other",
                "key",
                PersistSet(
                    request_id="00000000-0000-0000-0000-000000000001",
                    namespace="/other",
                    key="key",
                    value={"v": 1},
                    ttl_seconds=None,
                ),
            )
            is None
        )
        provide = next(
            command for command in transport.requests if command.__class__.__name__ == "Provide"
        )
        assert (provide.namespace_pattern, provide.key_pattern) == ("/users", "*")
        assert (provide.max_rate, provide.timeout, provide.miss_ttl) == (7, 250, 30)
        assert [
            (command.namespace_pattern, command.key_pattern)
            for command in transport.requests
            if command.__class__.__name__ == "Store"
        ] == [("/durable", "*")]
        await client.close()

    @pytest.mark.asyncio
    async def test_adapter_instance_persists(self, monkeypatch):
        transport = FakeTransport([])

        async def fake_open(*args, **kwargs):
            return transport

        monkeypatch.setattr(Transport, "open", fake_open)

        adapter = Adapter().provide("/users", "*", MemoryProvider(), timeout=250, miss_ttl=30)
        client = await AdapterClient.connect("127.0.0.1", adapter=adapter, api_key="key")
        assert client._adapter_instance
        instance = client._adapter_instance
        await client.close()
        assert client._adapter_instance == instance


class TestDispatch:
    def test_provider_registration_rejects_capture_literal_overlap(self):
        adapter = Adapter().provide("/users/{id}", "*", MemoryProvider())
        with pytest.raises(ValueError):
            adapter.provide("/users/42", "*", MemoryProvider())

    def test_embedded_dollar_capture_matches_server_pattern(self):
        from valkyr.adapter import _match_pattern

        assert _match_pattern("/services/${service}/config", "/services/api/config")

    @pytest.mark.asyncio
    async def test_provider_value_ttl_and_miss_are_distinct(self, monkeypatch):
        class ResultProvider(Provider):
            def __init__(self):
                self.result: Any = ProviderValue({"name": "Ada"}, ttl_seconds=300)

            async def get(self, namespace: str, key: str) -> Any:
                return self.result

        async def fake_open(*args, **kwargs):
            return FakeTransport([])

        monkeypatch.setattr(Transport, "open", fake_open)
        provider = ResultProvider()
        client = await AdapterClient.connect(
            "127.0.0.1",
            adapter=Adapter().provide("/users", "*", provider),
            api_key="key",
        )
        query = Query(
            request_id="00000000-0000-0000-0000-000000000001",
            namespace="/users",
            key="42",
        )
        result = await client._handle(query)
        assert result.value == {"name": "Ada"}
        assert result.error is None
        assert result.ttl_seconds == 300

        provider.result = None
        result = await client._handle(query)
        assert result.value is None
        assert result.error is None
        assert result.ttl_seconds is None

        provider.result = ProviderValue({"name": "Ada"}, ttl_seconds=-1)
        result = await client._handle(query)
        assert result.value is None
        assert result.error == "provider value TTL must be a non-negative integer or None"
        assert result.ttl_seconds is None

        provider.result = ProviderValue({"name": "Ada"}, ttl_seconds=2**64 - 1)
        result = await client._handle(query)
        assert result.value == {"name": "Ada"}
        assert result.error is None
        assert result.ttl_seconds == 2**64 - 1

        for invalid_ttl in (2**64, True, 1.5):
            provider.result = ProviderValue({"name": "Ada"}, ttl_seconds=invalid_ttl)
            result = await client._handle(query)
            assert result.value is None
            assert result.error == "provider value TTL must be a non-negative integer or None"
            assert result.ttl_seconds is None
        await client.close()

    @pytest.mark.asyncio
    async def test_all_configured_endpoints_register(self, monkeypatch):
        opened: list[tuple[str, int]] = []

        async def fake_open(host, port, **kwargs):
            opened.append((host, port))
            return FakeTransport([])

        monkeypatch.setattr(Transport, "open", fake_open)
        client = await AdapterClient.connect(
            endpoints=["127.0.0.1:8081", "127.0.0.1:8082"],
            api_key="key",
            adapter=Adapter(),
        )
        await client.close()

        assert opened == [("127.0.0.1", 8081), ("127.0.0.1", 8082)]

    @pytest.mark.asyncio
    async def test_provider_matches_namespace_and_key_captures(self, monkeypatch):
        transport = FakeTransport(
            [
                server_command_to_line(
                    Query(
                        request_id="00000000-0000-0000-0000-000000000001",
                        namespace="/users/42",
                        key="profile-main",
                    )
                )
            ]
        )

        async def fake_open(*args, **kwargs):
            return transport

        monkeypatch.setattr(Transport, "open", fake_open)
        adapter = Adapter().provide("/users/{id}", "profile-*", MemoryProvider({"ok": True}))
        client = await AdapterClient.connect("127.0.0.1", adapter=adapter, api_key="key")
        task = asyncio.create_task(client.serve_forever())
        await asyncio.sleep(0.05)
        await client.close()
        await asyncio.wait_for(task, timeout=1.0)

        _, result = transport.write_results[0]
        assert isinstance(result, QueryResult)
        assert result.value == {"ok": True}
        assert _written_result_line(transport) == (
            "QUERY_RESULT 00000000-0000-0000-0000-000000000001 SET /users/42 "
            'profile-main {"ok":true}'
        )

    @pytest.mark.asyncio
    async def test_callback_timeout_returns_safe_error(self, monkeypatch):
        transport = FakeTransport(
            [
                server_command_to_line(
                    Query(
                        request_id="00000000-0000-0000-0000-000000000001",
                        namespace="/users",
                        key="42",
                    )
                )
            ]
        )

        async def fake_open(*args, **kwargs):
            return transport

        monkeypatch.setattr(Transport, "open", fake_open)

        class BlockingProvider(Provider):
            async def get(self, namespace: str, key: str) -> dict | None:
                await asyncio.sleep(10)
                return None

        client = await AdapterClient.connect(
            "127.0.0.1",
            adapter=Adapter().provide("/users", "*", BlockingProvider()),
            api_key="key",
            callback_timeout_secs=0.01,
        )
        task = asyncio.create_task(client.serve_forever())
        await asyncio.sleep(0.05)
        await client.close()
        await asyncio.wait_for(task, timeout=1.0)

        _, result = transport.write_results[0]
        assert isinstance(result, QueryResult)
        assert result.error == "adapter callback timed out"
        assert result.ttl_seconds is None
        assert _written_result_line(transport) == (
            'QUERY_RESULT 00000000-0000-0000-0000-000000000001 KO "adapter callback timed out"'
        )

    @pytest.mark.asyncio
    async def test_callback_cancellation_returns_safe_error(self, monkeypatch):
        transport = FakeTransport([])

        async def fake_open(*args, **kwargs):
            return transport

        monkeypatch.setattr(Transport, "open", fake_open)

        class BlockingProvider(Provider):
            async def get(self, namespace: str, key: str) -> dict | None:
                await asyncio.sleep(10)
                return None

        client = await AdapterClient.connect(
            "127.0.0.1",
            adapter=Adapter().provide("/users", "*", BlockingProvider()),
            api_key="key",
        )
        callback = asyncio.create_task(
            client._dispatch(
                Query(
                    request_id="00000000-0000-0000-0000-000000000001", namespace="/users", key="42"
                ),
                transport,
            )
        )
        await asyncio.sleep(0)
        callback.cancel()
        await callback
        await client.close()

        _, result = transport.write_results[0]
        assert isinstance(result, QueryResult)
        assert result.error == "adapter callback cancelled"
        assert _written_result_line(transport) == (
            'QUERY_RESULT 00000000-0000-0000-0000-000000000001 KO "adapter callback cancelled"'
        )

    @pytest.mark.asyncio
    async def test_callbacks_can_complete_out_of_order(self, monkeypatch):
        transport = FakeTransport(
            [
                server_command_to_line(
                    Query(
                        request_id="00000000-0000-0000-0000-000000000001",
                        namespace="/users",
                        key="slow",
                    )
                ),
                server_command_to_line(
                    Query(
                        request_id="00000000-0000-0000-0000-000000000002",
                        namespace="/users",
                        key="fast",
                    )
                ),
            ]
        )

        async def fake_open(*args, **kwargs):
            return transport

        monkeypatch.setattr(Transport, "open", fake_open)

        class OrderedProvider(Provider):
            async def get(self, namespace: str, key: str) -> dict:
                await asyncio.sleep(0.03 if key == "slow" else 0)
                return {"key": key}

        client = await AdapterClient.connect(
            "127.0.0.1",
            adapter=Adapter().provide("/users", "*", OrderedProvider()),
            api_key="key",
        )
        task = asyncio.create_task(client.serve_forever())
        await asyncio.sleep(0.08)
        await client.close()
        await asyncio.wait_for(task, timeout=1.0)

        assert [command.request_id for command, _ in transport.write_results] == [
            "00000000-0000-0000-0000-000000000002",
            "00000000-0000-0000-0000-000000000001",
        ]

    @pytest.mark.asyncio
    async def test_provider_query(self, monkeypatch):
        transport = FakeTransport(
            [
                server_command_to_line(
                    Query(
                        request_id="00000000-0000-0000-0000-000000000001",
                        namespace="/users",
                        key="42",
                    )
                ),
            ]
        )

        async def fake_open(*args, **kwargs):
            return transport

        monkeypatch.setattr(Transport, "open", fake_open)

        adapter = Adapter().provide("/users", "*", MemoryProvider({"name": "Ada"}))
        client = await AdapterClient.connect("127.0.0.1", adapter=adapter, api_key="key")
        task = asyncio.create_task(client.serve_forever())
        await asyncio.sleep(0.05)
        await client.close()
        await asyncio.wait_for(task, timeout=1.0)

        assert len(transport.write_results) == 1
        _, result = transport.write_results[0]
        assert isinstance(result, QueryResult)
        assert result.value == {"name": "Ada"}
        assert result.error is None
        assert _written_result_line(transport) == (
            'QUERY_RESULT 00000000-0000-0000-0000-000000000001 SET /users 42 {"name":"Ada"}'
        )

    @pytest.mark.asyncio
    async def test_provider_query_no_handler(self, monkeypatch):
        transport = FakeTransport(
            [
                server_command_to_line(
                    Query(
                        request_id="00000000-0000-0000-0000-000000000001",
                        namespace="/other",
                        key="42",
                    )
                ),
            ]
        )

        async def fake_open(*args, **kwargs):
            return transport

        monkeypatch.setattr(Transport, "open", fake_open)

        adapter = Adapter().provide("/users", "*", MemoryProvider({"name": "Ada"}))
        client = await AdapterClient.connect("127.0.0.1", adapter=adapter, api_key="key")
        task = asyncio.create_task(client.serve_forever())
        await asyncio.sleep(0.05)
        await client.close()
        await asyncio.wait_for(task, timeout=1.0)

        assert len(transport.write_results) == 1
        _, result = transport.write_results[0]
        assert isinstance(result, QueryResult)
        assert result.value is None
        assert result.error is None
        assert _written_result_line(transport) == (
            "QUERY_RESULT 00000000-0000-0000-0000-000000000001 MISS"
        )

    @pytest.mark.asyncio
    async def test_store_set(self, monkeypatch):
        transport = FakeTransport(
            [
                server_command_to_line(
                    PersistSet(
                        request_id="00000000-0000-0000-0000-000000000002",
                        namespace="/users",
                        key="42",
                        value={"name": "Ada"},
                        ttl_seconds=300,
                    )
                ),
            ]
        )

        async def fake_open(*args, **kwargs):
            return transport

        monkeypatch.setattr(Transport, "open", fake_open)

        store = MemoryStore()
        adapter = Adapter().store("/users", "*", store)
        client = await AdapterClient.connect("127.0.0.1", adapter=adapter, api_key="key")
        task = asyncio.create_task(client.serve_forever())
        await asyncio.sleep(0.05)
        await client.close()
        await asyncio.wait_for(task, timeout=1.0)

        assert len(store.sets) == 1
        assert store.sets[0][3] == 300
        _, result = transport.write_results[0]
        assert isinstance(result, OperationResult)
        assert result.error is None
        assert _written_result_line(transport) == (
            "OPERATION 00000000-0000-0000-0000-000000000002 OK"
        )

    @pytest.mark.asyncio
    async def test_store_unhandled_fails(self, monkeypatch):
        transport = FakeTransport(
            [
                server_command_to_line(
                    PersistSet(
                        request_id="00000000-0000-0000-0000-000000000002",
                        namespace="/users",
                        key="42",
                        value={"name": "Ada"},
                        ttl_seconds=None,
                    )
                ),
            ]
        )

        async def fake_open(*args, **kwargs):
            return transport

        monkeypatch.setattr(Transport, "open", fake_open)

        adapter = Adapter()  # no store
        client = await AdapterClient.connect("127.0.0.1", adapter=adapter, api_key="key")
        task = asyncio.create_task(client.serve_forever())
        await asyncio.sleep(0.05)
        await client.close()
        await asyncio.wait_for(task, timeout=1.0)

        assert len(transport.write_results) == 1
        _, result = transport.write_results[0]
        assert isinstance(result, OperationResult)
        assert result.error == "no store handler registered for /users"
        assert _written_result_line(transport) == (
            "OPERATION 00000000-0000-0000-0000-000000000002 KO "
            '"no store handler registered for /users"'
        )

    @pytest.mark.asyncio
    async def test_store_set_many(self, monkeypatch):
        transport = FakeTransport(
            [
                server_command_to_line(
                    PersistSetBatch(
                        request_id="00000000-0000-0000-0000-000000000003",
                        namespace="/users",
                        entries=[SetEntry(key="a", value=1)],
                        ttl_seconds=None,
                    )
                ),
            ]
        )

        async def fake_open(*args, **kwargs):
            return transport

        monkeypatch.setattr(Transport, "open", fake_open)

        store = MemoryStore()
        adapter = Adapter().store("/users", "*", store)
        client = await AdapterClient.connect("127.0.0.1", adapter=adapter, api_key="key")
        task = asyncio.create_task(client.serve_forever())
        await asyncio.sleep(0.05)
        await client.close()
        await asyncio.wait_for(task, timeout=1.0)

        assert len(store.batches) == 1
        assert store.batches[0][1] == [{"key": "a", "value": 1}]

    @pytest.mark.asyncio
    async def test_store_delete_and_move(self, monkeypatch):
        transport = FakeTransport(
            [
                server_command_to_line(
                    PersistDelete(
                        request_id="00000000-0000-0000-0000-000000000001",
                        namespace="/users",
                        key_pattern="*",
                    )
                ),
                server_command_to_line(
                    PersistMove(
                        request_id="00000000-0000-0000-0000-000000000002",
                        source="/users::draft",
                        destination="/users::published",
                    )
                ),
            ]
        )

        async def fake_open(*args, **kwargs):
            return transport

        monkeypatch.setattr(Transport, "open", fake_open)

        store = MemoryStore()
        adapter = Adapter().store("/users", "*", store)
        client = await AdapterClient.connect("127.0.0.1", adapter=adapter, api_key="key")
        task = asyncio.create_task(client.serve_forever())
        await asyncio.sleep(0.2)
        await client.close()
        await asyncio.wait_for(task, timeout=1.0)

        assert len(store.deletes) == 1
        assert len(store.moves) == 1
        assert store.moves[0] == ("/users::draft", "/users::published")


class TestOverload:
    @pytest.mark.asyncio
    async def test_semaphore_overload(self, monkeypatch):
        transport = FakeTransport(
            [
                server_command_to_line(
                    Query(
                        request_id="00000000-0000-0000-0000-000000000001",
                        namespace="/users",
                        key="1",
                    )
                ),
                server_command_to_line(
                    Query(
                        request_id="00000000-0000-0000-0000-000000000002",
                        namespace="/users",
                        key="2",
                    )
                ),
            ]
        )

        async def fake_open(*args, **kwargs):
            return transport

        monkeypatch.setattr(Transport, "open", fake_open)

        # Block the provider so the semaphore stays at capacity.
        class BlockingProvider(Provider):
            async def get(self, namespace: str, key: str) -> dict | None:
                await asyncio.sleep(10)
                return None

        adapter = Adapter().provide("/users", "*", BlockingProvider())
        client = await AdapterClient.connect(
            "127.0.0.1", adapter=adapter, api_key="key", max_concurrency=1
        )
        task = asyncio.create_task(client.serve_forever())
        await asyncio.sleep(0.05)
        await client.close()
        await asyncio.wait_for(task, timeout=1.0)

        # At least one query should be rejected with an overload error.
        overloads = [
            result
            for _, result in transport.write_results
            if isinstance(result, (OperationResult, QueryResult))
            and result.error == "adapter overloaded"
        ]
        assert overloads


class TestReconnect:
    @pytest.mark.asyncio
    async def test_reconnect_restores_registrations(self, monkeypatch):
        first = FakeTransport(
            [
                ConnectionError("drop"),
            ]
        )
        second = FakeTransport([])
        calls: list[FakeTransport] = []

        async def fake_open(*args, **kwargs):
            transport = second if len(calls) else first
            calls.append(transport)
            return transport

        monkeypatch.setattr(Transport, "open", fake_open)

        adapter = Adapter().provide("/users", "*", MemoryProvider(), timeout=250, miss_ttl=30)
        client = await AdapterClient.connect(
            "127.0.0.1",
            adapter=adapter,
            api_key="key",
            reconnect_delay_min=0.01,
            reconnect_delay_max=0.05,
        )
        task = asyncio.create_task(client.serve_forever())
        await asyncio.sleep(0.1)
        await client.close()
        await asyncio.wait_for(task, timeout=1.0)

        assert len(calls) >= 2
        provide = next(cmd for cmd in second.requests if cmd.__class__.__name__ == "Provide")
        assert provide.timeout == 250
        assert provide.miss_ttl == 30

    @pytest.mark.asyncio
    async def test_reconnect_backoff_increases(self, monkeypatch):
        transport = FakeTransport([ConnectionError("drop")])
        call_times: list[float] = []

        async def fake_open(*args, **kwargs):
            call_times.append(asyncio.get_event_loop().time())
            return transport

        monkeypatch.setattr(Transport, "open", fake_open)
        # Remove jitter so backoff gaps are deterministic.
        monkeypatch.setattr("valkyr.adapter.random.uniform", lambda _a, _b: 0.0)

        adapter = Adapter().provide("/users", "*", MemoryProvider())
        client = await AdapterClient.connect(
            "127.0.0.1",
            adapter=adapter,
            api_key="key",
            reconnect_delay_min=0.01,
            reconnect_delay_max=0.1,
        )
        task = asyncio.create_task(client.serve_forever())
        await asyncio.sleep(0.15)
        await client.close()
        await asyncio.wait_for(task, timeout=1.0)

        assert len(call_times) >= 3
        gaps = [call_times[i + 1] - call_times[i] for i in range(len(call_times) - 1)]
        # Exponential backoff with jitter removed: the later half should accumulate
        # more delay than the earlier half.
        mid = len(gaps) // 2
        assert sum(gaps[mid:]) > sum(gaps[:mid])
