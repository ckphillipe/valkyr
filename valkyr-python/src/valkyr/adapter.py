"""Streaming adapter client with supervised reconnect."""

import asyncio
import builtins
import contextlib
import random
import ssl
import uuid
from collections.abc import Callable, Sequence
from typing import Any

from ._auth import authenticate
from ._client import _build_ssl_context, _parse_address
from ._errors import AuthenticationRejectedError, ConnectionError
from ._errors import TimeoutError as ValkyrTimeoutError
from ._handlers import ProviderValue
from ._registration import MAX_U64, Adapter, ProvideRoute, StoreRoute
from ._transport import Transport
from ._wire import (
    Ok,
    OperationResult,
    PersistDelete,
    PersistMove,
    PersistSet,
    PersistSetBatch,
    Provide,
    Query,
    QueryResult,
    ServerCommand,
    ServerResult,
)
from ._wire import (
    Store as StoreCommand,
)


def _pattern_tokens(pattern: str) -> list[tuple[str, str | None]]:
    tokens: list[tuple[str, str | None]] = []
    index = 0
    while index < len(pattern):
        tail = pattern[index:]
        if tail.startswith("*"):
            tokens.append(("wildcard", None))
            index += 1
            continue
        if tail.startswith("${"):
            end = tail.find("}")
            if end > 2:
                tokens.append(("capture", tail[2:end]))
                index += end + 1
                continue
        if tail.startswith("{"):
            end = tail.find("}")
            if end > 1:
                tokens.append(("capture", tail[1:end]))
                index += end + 1
                continue
        next_special = [
            position
            for position in (tail.find("*"), tail.find("{"), tail.find("${"))
            if position > 0
        ]
        end = min(next_special, default=len(tail))
        tokens.append(("literal", tail[:end]))
        index += end
    return tokens


def _match_pattern(pattern: str, value: str) -> bool:
    """Match the same wildcard/capture grammar as ``valkyr-core``."""
    tokens = _pattern_tokens(pattern)
    cache: dict[tuple[int, int], bool] = {}

    def matches(token_index: int, value_index: int) -> bool:
        state = (token_index, value_index)
        if state in cache:
            return cache[state]
        if token_index == len(tokens):
            result = value_index == len(value)
        else:
            kind, text = tokens[token_index]
            if kind == "literal":
                result = value.startswith(text or "", value_index) and matches(
                    token_index + 1,
                    value_index + len(text or ""),
                )
            elif kind == "wildcard":
                result = any(
                    matches(token_index + 1, index) for index in range(value_index, len(value) + 1)
                )
            else:
                result = any(
                    matches(token_index + 1, index)
                    for index in range(value_index + 1, len(value) + 1)
                )
        cache[state] = result
        return result

    return matches(0, 0)


def _matches_namespace(pattern: str, namespace: str) -> bool:
    if _match_pattern(pattern, namespace):
        return True
    has_dynamic_tokens = any(kind != "literal" for kind, _ in _pattern_tokens(pattern))
    return not has_dynamic_tokens and namespace.startswith(f"{pattern}::")


def _patterns_overlap(left: str, right: str) -> bool:
    """Mirror the server's conservative overlap checks for mutation patterns."""
    if left == "*" or right == "*":
        return True
    if _match_pattern(left, right) or _match_pattern(right, left):
        return True
    if left.endswith("*") and right.startswith(left[:-1]):
        return True
    return right.endswith("*") and left.startswith(right[:-1])


class _CallbackLimiter:
    """Tracks in-flight callback work without a waiting backlog."""

    def __init__(self, max_concurrency: int):
        self._max = max_concurrency
        self._running = 0
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        async with self._lock:
            if self._running >= self._max:
                return False
            self._running += 1
            return True

    async def release(self) -> None:
        async with self._lock:
            self._running -= 1


class AdapterClient:
    """Long-lived adapter connection with callback dispatch and reconnect."""

    def __init__(
        self,
        host: str,
        port: int,
        api_key: str,
        adapter: Adapter,
        *,
        adapter_instance: str,
        endpoints: list[tuple[str, int]] | None = None,
        ssl_context: ssl.SSLContext | None = None,
        server_hostname: str | None = None,
        timeout_secs: float = 30.0,
        auth_timeout_secs: float = 5.0,
        callback_timeout_secs: float = 30.0,
        max_concurrency: int = 32,
        reconnect_delay_min: float = 0.5,
        reconnect_delay_max: float = 30.0,
    ):
        self._endpoints = endpoints or [(host, port)]
        self._api_key = api_key
        self._provide_routes = tuple(adapter.provide_routes)
        self._store_routes = tuple(adapter.store_routes)
        self._adapter_instance = adapter_instance
        self._ssl_context = ssl_context
        self._server_hostname = server_hostname
        self._timeout_secs = timeout_secs
        self._auth_timeout_secs = auth_timeout_secs
        self._callback_timeout_secs = callback_timeout_secs
        self._max_concurrency = max_concurrency
        self._reconnect_delay_min = reconnect_delay_min
        self._reconnect_delay_max = reconnect_delay_max
        self._callback_limit = _CallbackLimiter(max_concurrency)
        self._transport: Transport | None = None
        self._transports: dict[int, Transport] = {}
        self._closed = False
        self._supervisor_task: asyncio.Task[None] | None = None
        self._inflight: set[asyncio.Task[None]] = set()

    @classmethod
    async def connect(
        cls,
        host: str | Sequence[str] | None = None,
        port: int | None = None,
        *,
        adapter: Adapter,
        api_key: str,
        endpoints: Sequence[str] | None = None,
        tls: Any | None = None,
        adapter_instance: str | None = None,
        max_concurrency: int = 32,
        auth_timeout_secs: float = 5.0,
        callback_timeout_secs: float = 30.0,
        reconnect_delay_min: float = 0.5,
        reconnect_delay_max: float = 30.0,
        timeout_secs: float = 30.0,
    ) -> "AdapterClient":
        if callback_timeout_secs <= 0:
            raise ValueError("callback_timeout_secs must be positive")
        if endpoints is not None:
            endpoint_values = list(endpoints)
        elif isinstance(host, str):
            endpoint_values = [host]
        elif host is not None:
            endpoint_values = list(host)
        else:
            endpoint_values = []
        if not endpoint_values and host is None:
            raise ValueError("an endpoint is required")
        if port is not None and len(endpoint_values) != 1:
            raise ValueError("port can only be used with one endpoint")
        parsed_endpoints = [
            _parse_address(endpoint, port if len(endpoint_values) == 1 else None)
            for endpoint in endpoint_values
        ]
        host, port = parsed_endpoints[0]
        ssl_context, server_hostname = _build_ssl_context(tls)
        if adapter_instance is None:
            adapter_instance = str(uuid.uuid4())
        client = cls(
            host=host,
            port=port,
            api_key=api_key,
            adapter=adapter,
            adapter_instance=adapter_instance,
            endpoints=parsed_endpoints,
            ssl_context=ssl_context,
            server_hostname=server_hostname,
            timeout_secs=timeout_secs,
            auth_timeout_secs=auth_timeout_secs,
            callback_timeout_secs=callback_timeout_secs,
            max_concurrency=max_concurrency,
            reconnect_delay_min=reconnect_delay_min,
            reconnect_delay_max=reconnect_delay_max,
        )
        await client._connect_all()
        return client

    async def __aenter__(self) -> "AdapterClient":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def serve_forever(self) -> None:
        """Run the supervised connection until ``close()`` is called."""
        self._supervisor_task = asyncio.create_task(self._supervise_all())
        with contextlib.suppress(asyncio.CancelledError):
            await self._supervisor_task

    async def close(self) -> None:
        self._closed = True
        if self._supervisor_task is not None and not self._supervisor_task.done():
            self._supervisor_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._supervisor_task
        for transport in set(self._transports.values()):
            await transport.close()
        self._transports.clear()
        await self._cancel_inflight()

    async def _connect_endpoint(self, index: int) -> Transport:
        host, port = self._endpoints[index]
        transport = await Transport.open(
            host,
            port,
            ssl_context=self._ssl_context,
            server_hostname=self._server_hostname,
            timeout_secs=self._timeout_secs,
        )
        try:
            await authenticate(
                transport,
                self._api_key,
                timeout_secs=self._auth_timeout_secs,
                adapter_instance=self._adapter_instance,
            )
            await self._register(transport)
        except BaseException:
            await transport.close()
            raise
        return transport

    async def _connect_all(self) -> None:
        errors: list[BaseException] = []
        for index in range(len(self._endpoints)):
            try:
                transport = await self._connect_endpoint(index)
            except AuthenticationRejectedError:
                raise
            except BaseException as exc:
                errors.append(exc)
                continue
            self._transports[index] = transport
            if self._transport is None:
                self._transport = transport
        if not self._transports:
            raise errors[-1] if errors else ConnectionError("no endpoints configured")

    async def _supervise_all(self) -> None:
        tasks = [
            asyncio.create_task(self._supervise_endpoint(index))
            for index in range(len(self._endpoints))
        ]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _supervise_endpoint(self, index: int) -> None:
        delay = self._reconnect_delay_min
        while not self._closed:

            def reset_delay() -> None:
                nonlocal delay
                delay = self._reconnect_delay_min

            try:
                transport = self._transports.get(index)
                if transport is None or transport.closed:
                    transport = await self._connect_endpoint(index)
                    self._transports[index] = transport
                    if self._transport is None or index == 0:
                        self._transport = transport
                await self._read_loop(transport, on_frame=reset_delay)
            except asyncio.CancelledError:
                break
            except AuthenticationRejectedError:
                raise
            except Exception:  # noqa: BLE001
                await self._cancel_inflight()
                transport = self._transports.pop(index, None)
                if transport is not None:
                    await transport.close()
                if self._closed:
                    break
                jitter = random.uniform(0, delay * 0.5)
                await asyncio.sleep(min(delay + jitter, self._reconnect_delay_max))
                delay = min(delay * 2, self._reconnect_delay_max)

    async def _cancel_inflight(self) -> None:
        if self._inflight:
            await asyncio.sleep(0)
            pending = set(self._inflight)
            _, pending = await asyncio.wait(pending, timeout=0.1)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

    async def _register(self, transport: Transport) -> None:
        for route in self._provide_routes:
            response = await transport.request(
                Provide(
                    namespace_pattern=route.namespace_pattern,
                    key_pattern=route.key_pattern,
                    max_rate=route.max_rate,
                    timeout=route.timeout,
                    miss_ttl=route.miss_ttl,
                )
            )
            if not isinstance(response, Ok):
                raise ConnectionError(f"provide registration failed: {response}")
        for store_route in self._store_routes:
            response = await transport.request(
                StoreCommand(
                    namespace_pattern=store_route.namespace_pattern,
                    key_pattern=store_route.key_pattern,
                )
            )
            if not isinstance(response, Ok):
                raise ConnectionError(f"store registration failed: {response}")

    async def _read_loop(
        self,
        transport: Transport,
        *,
        on_frame: Callable[[], None] | None = None,
    ) -> None:
        while not self._closed:
            command = await transport.read_command()
            if on_frame is not None:
                on_frame()
            if await self._callback_limit.acquire():
                task = asyncio.create_task(self._dispatch_and_release(command, transport))
            else:
                task = asyncio.create_task(self._dispatch_overload(command, transport))
            self._inflight.add(task)
            task.add_done_callback(self._inflight.discard)

    async def _dispatch_overload(self, command: ServerCommand, transport: Transport) -> None:
        request_id = _request_id(command)
        if isinstance(command, Query):
            result: ServerResult = QueryResult(
                request_id=request_id,
                value=None,
                error="adapter overloaded",
                ttl_seconds=None,
            )
        else:
            result = OperationResult(request_id=request_id, error="adapter overloaded")
        await self._send_result(command, result, transport)

    async def _dispatch_and_release(self, command: ServerCommand, transport: Transport) -> None:
        try:
            await self._dispatch(command, transport)
        finally:
            await self._callback_limit.release()

    async def _dispatch(self, command: ServerCommand, transport: Transport) -> None:
        try:
            result = await asyncio.wait_for(
                self._handle(command),
                timeout=self._callback_timeout_secs,
            )
        except asyncio.CancelledError:
            result = _error_result(command, "adapter callback cancelled")
        except builtins.TimeoutError:
            result = _error_result(command, "adapter callback timed out")
        except Exception as exc:  # noqa: BLE001
            result = _error_result(command, str(exc))
        await self._send_result(command, result, transport)

    async def _handle(self, command: ServerCommand) -> ServerResult:
        if isinstance(command, Query):
            route = self._find_provider(command.namespace, command.key)
            if route is None:
                return QueryResult(
                    request_id=command.request_id,
                    value=None,
                    error=None,
                    ttl_seconds=None,
                )
            try:
                provider_result = await route.provider.get(command.namespace, command.key)
                value, ttl_seconds = _normalize_provider_result(provider_result)
            except Exception as exc:  # noqa: BLE001
                return QueryResult(
                    request_id=command.request_id,
                    value=None,
                    error=str(exc),
                    ttl_seconds=None,
                )
            return QueryResult(
                request_id=command.request_id,
                value=value,
                error=None,
                ttl_seconds=ttl_seconds,
            )

        if isinstance(command, PersistMove):
            if "::" not in command.source:
                return _error_result(
                    command,
                    f"move requires a context namespace: {command.source}",
                )
            namespace = command.source
            key_pattern = "*"
        else:
            namespace = command.namespace
            if isinstance(command, PersistSet):
                key_pattern = command.key
            elif isinstance(command, PersistSetBatch):
                key_pattern = None
            else:
                key_pattern = command.key_pattern or "*"

        store_route = self._find_store(namespace, key_pattern, command)
        if store_route is None:
            return _error_result(
                command,
                f"no store handler registered for {namespace}",
            )

        try:
            if isinstance(command, PersistSet):
                await store_route.store.set(
                    command.namespace,
                    command.key,
                    command.value,
                    command.ttl_seconds,
                )
            elif isinstance(command, PersistSetBatch):
                await store_route.store.set_many(
                    command.namespace,
                    [entry.to_dict() for entry in command.entries],
                    command.ttl_seconds,
                )
            elif isinstance(command, PersistDelete):
                await store_route.store.delete(command.namespace, command.key_pattern)
            elif isinstance(command, PersistMove):
                await store_route.store.move(command.source, command.destination)
            else:
                return _error_result(command, f"unsupported command: {command}")
        except Exception as exc:  # noqa: BLE001
            return _error_result(command, str(exc))
        return OperationResult(request_id=_request_id(command), error=None)

    def _find_provider(self, namespace: str, key: str) -> ProvideRoute | None:
        for route in self._provide_routes:
            if _matches_namespace(route.namespace_pattern, namespace) and _match_pattern(
                route.key_pattern, key
            ):
                return route
        return None

    def _find_store(
        self,
        namespace: str,
        key_pattern: str | None,
        command: ServerCommand,
    ) -> StoreRoute | None:
        for route in reversed(self._store_routes):
            if not _matches_namespace(route.namespace_pattern, namespace):
                continue
            if isinstance(command, PersistSetBatch):
                if all(
                    _patterns_overlap(route.key_pattern, entry.key) for entry in command.entries
                ):
                    return route
            elif key_pattern is not None and _patterns_overlap(route.key_pattern, key_pattern):
                return route
        return None

    async def _send_result(
        self, command: ServerCommand, result: ServerResult, transport: Transport
    ) -> None:
        if transport.closed:
            return
        with contextlib.suppress(ConnectionError, ValkyrTimeoutError):
            await transport.write_result(command, result)


def _request_id(command: ServerCommand) -> str:
    return command.request_id


def _normalize_provider_result(result: Any) -> tuple[Any | None, int | None]:
    if not isinstance(result, ProviderValue):
        return result, None
    ttl_seconds = result.ttl_seconds
    if ttl_seconds is not None and (
        isinstance(ttl_seconds, bool)
        or not isinstance(ttl_seconds, int)
        or not 0 <= ttl_seconds <= MAX_U64
    ):
        raise ValueError("provider value TTL must be a non-negative integer or None")
    if result.value is None:
        return None, None
    return result.value, ttl_seconds


def _error_result(command: ServerCommand, message: str) -> ServerResult:
    request_id = _request_id(command)
    if isinstance(command, Query):
        return QueryResult(request_id=request_id, value=None, error=message, ttl_seconds=None)
    return OperationResult(request_id=request_id, error=message)
