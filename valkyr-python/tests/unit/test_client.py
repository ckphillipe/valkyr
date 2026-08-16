"""Unit tests for the fluent application client."""

import asyncio
from typing import Any

import pytest

from valkyr import Miss, Unknown, Value
from valkyr._client import Client
from valkyr._errors import AuthenticationRejectedError, AuthError, RouteError, ServerError
from valkyr._wire import (
    Auth,
    AuthSuccess,
    Delete,
    Get,
    Move,
    Ok,
    Set,
    SetBatch,
    Stats,
    StatsResponse,
)
from valkyr._wire import (
    AuthPending as WireAuthPending,
)
from valkyr._wire import (
    Miss as WireMiss,
)
from valkyr._wire import (
    Pong as WirePong,
)
from valkyr._wire import (
    Unknown as WireUnknown,
)
from valkyr._wire import (
    Value as WireValue,
)


class FakeTransport:
    def __init__(self, responses: list[Any]):
        self.responses = list(responses)
        self.commands: list[Any] = []
        self.closed = False

    async def request(self, cmd: Any) -> Any:
        self.commands.append(cmd)
        if not self.responses:
            raise ServerError("no more responses")
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response

    async def close(self) -> None:
        self.closed = True


class TestRouteBuilder:
    def test_namespace_requires_name(self):
        client = Client(FakeTransport([]), None)
        with pytest.raises(RouteError):
            client.namespace("")

    def test_key_requires_value(self):
        client = Client(FakeTransport([]), None)
        with pytest.raises(RouteError):
            client.namespace("/users").key("")

    @pytest.mark.asyncio
    async def test_get_value(self):
        transport = FakeTransport([WireValue(value={"name": "Ada"}, ttl_seconds=300)])
        client = Client(transport, None)
        route = client.namespace("/users").key("42")
        result = await route.get()
        assert isinstance(result, Value)
        assert result.value == {"name": "Ada"}
        assert result.ttl_seconds == 300
        assert isinstance(transport.commands[0], Get)

    @pytest.mark.asyncio
    async def test_get_miss(self):
        transport = FakeTransport([WireMiss(retry_after_ms=25)])
        client = Client(transport, None)
        route = client.namespace("/users").key("42")
        result = await route.get()
        assert isinstance(result, Miss)
        assert result.retry_after_ms == 25

    @pytest.mark.asyncio
    async def test_get_unknown(self):
        transport = FakeTransport([WireUnknown()])
        client = Client(transport, None)
        route = client.namespace("/users").key("42")
        result = await route.get()
        assert result is Unknown

    @pytest.mark.asyncio
    async def test_set(self):
        transport = FakeTransport([Ok()])
        client = Client(transport, None)
        await client.namespace("/users").key("42").set({"name": "Ada"}, ttl_seconds=300)
        cmd = transport.commands[0]
        assert isinstance(cmd, Set)
        assert cmd.namespace == "/users"
        assert cmd.key == "42"
        assert cmd.value == {"name": "Ada"}
        assert cmd.ttl_seconds == 300

    @pytest.mark.asyncio
    async def test_delete(self):
        transport = FakeTransport([Ok()])
        client = Client(transport, None)
        await client.namespace("/users").key("42").delete()
        cmd = transport.commands[0]
        assert isinstance(cmd, Delete)
        assert cmd.key_pattern == "42"

    @pytest.mark.asyncio
    async def test_move(self):
        transport = FakeTransport([Ok()])
        client = Client(transport, None)
        await client.namespace("/users::draft").key("42").move("published")
        cmd = transport.commands[0]
        assert isinstance(cmd, Move)
        assert cmd.source == "/users::draft"
        assert cmd.destination == "/users::published"

    @pytest.mark.asyncio
    async def test_move_requires_context(self):
        transport = FakeTransport([Ok()])
        client = Client(transport, None)
        with pytest.raises(RouteError):
            await client.namespace("/users").key("42").move("published")


class TestRetry:
    @pytest.mark.asyncio
    async def test_get_with_retry_positive_delay(self):
        transport = FakeTransport([WireMiss(retry_after_ms=50), WireValue(value={"name": "Ada"})])
        client = Client(transport, None)
        start = asyncio.get_event_loop().time()
        result = await client.namespace("/users").key("42").get_with_retry()
        elapsed = asyncio.get_event_loop().time() - start
        assert isinstance(result, Value)
        assert elapsed >= 0.04
        assert len(transport.commands) == 2

    @pytest.mark.asyncio
    async def test_get_with_retry_zero_delay_no_sleep(self):
        transport = FakeTransport([WireMiss(retry_after_ms=0), WireValue(value={"name": "Ada"})])
        client = Client(transport, None)
        result = await client.namespace("/users").key("42").get_with_retry()
        assert isinstance(result, Value)

    @pytest.mark.asyncio
    async def test_get_with_retry_second_miss_not_retried(self):
        transport = FakeTransport([WireMiss(retry_after_ms=10), WireMiss(retry_after_ms=10)])
        client = Client(transport, None)
        result = await client.namespace("/users").key("42").get_with_retry()
        assert isinstance(result, Miss)
        assert len(transport.commands) == 2

    @pytest.mark.asyncio
    async def test_get_with_retry_unknown_not_retried(self):
        transport = FakeTransport([WireUnknown()])
        client = Client(transport, None)
        result = await client.namespace("/users").key("42").get_with_retry()
        assert result is Unknown
        assert len(transport.commands) == 1

    @pytest.mark.asyncio
    async def test_get_with_retry_error_not_retried(self):
        transport = FakeTransport([ServerError("boom")])
        client = Client(transport, None)
        with pytest.raises(ServerError):
            await client.namespace("/users").key("42").get_with_retry()

    @pytest.mark.asyncio
    async def test_get_with_retry_auth_error_not_retried(self):
        transport = FakeTransport([AuthError("denied")])
        client = Client(transport, None)
        with pytest.raises(AuthError):
            await client.namespace("/users").key("42").get_with_retry()


class TestBatchAndAdmin:
    @pytest.mark.asyncio
    async def test_set_many(self):
        transport = FakeTransport([Ok()])
        client = Client(transport, None)
        await client.namespace("/sessions").set_many({"a": 1, "b": 2})
        cmd = transport.commands[0]
        assert isinstance(cmd, SetBatch)
        assert cmd.namespace == "/sessions"
        assert len(cmd.entries) == 2

    @pytest.mark.asyncio
    async def test_ping(self):
        transport = FakeTransport([WirePong()])
        client = Client(transport, None)
        await client.namespace("/any").ping()

    @pytest.mark.asyncio
    async def test_stats(self):
        stats = Stats(requests=10, hits=5, misses=3, values=2)
        transport = FakeTransport([StatsResponse(stats=stats)])
        client = Client(transport, None)
        result = await client.namespace("/any").stats()
        assert isinstance(result, StatsResponse)
        assert result.stats == stats


class TestAuth:
    @pytest.mark.asyncio
    async def test_auth_pending_auto_retry(self):
        transport = FakeTransport(
            [
                WireAuthPending(retry_after_ms=10),
                AuthSuccess(client_id="c1", session_ttl_seconds=3600),
            ]
        )
        from valkyr._auth import authenticate

        result = await authenticate(transport, "key", timeout_secs=1.0)
        assert isinstance(result, AuthSuccess)
        assert result.client_id == "c1"
        assert len(transport.commands) == 2
        assert isinstance(transport.commands[0], Auth)
        assert isinstance(transport.commands[1], Auth)

    @pytest.mark.asyncio
    async def test_auth_failure_stops_immediately(self):
        from valkyr._wire import AuthFailure as WireAuthFailure

        transport = FakeTransport([WireAuthFailure(message="bad key")])
        from valkyr._auth import authenticate

        with pytest.raises(AuthenticationRejectedError):
            await authenticate(transport, "key", timeout_secs=1.0)
        assert len(transport.commands) == 1

    @pytest.mark.asyncio
    async def test_auth_timeout(self):
        transport = FakeTransport(
            [WireAuthPending(retry_after_ms=5000), WireAuthPending(retry_after_ms=5000)]
        )
        from valkyr._auth import authenticate

        with pytest.raises(AuthError):
            await authenticate(transport, "key", timeout_secs=0.01)
        assert len(transport.commands) >= 1
