"""Fluent application client for Valkyr."""

import asyncio
import ssl
from collections.abc import Mapping
from typing import Any, Optional

from ._auth import authenticate
from ._errors import RouteError, ServerError, ValkyrError
from ._transport import Transport
from ._wire import (
    Command,
    Delete,
    Get,
    Move,
    Ok,
    Set,
    SetBatch,
    SetEntry,
    StatsCommand,
    StatsResponse,
)
from ._wire import (
    Miss as WireMiss,
)
from ._wire import (
    Ping as WirePing,
)
from ._wire import (
    Pong as WirePong,
)
from ._wire import (
    Unknown as WireUnknown,
)
from ._wire import (
    Value as WireValue,
)

DEFAULT_PORT = 8081


def _parse_address(host: str, port: int | None) -> tuple[str, int]:
    if port is not None:
        return host, port
    if host.startswith("["):
        bracket_end = host.rfind("]")
        if bracket_end == -1:
            raise ValueError(f"invalid IPv6 address: {host}")
        if bracket_end + 1 < len(host) and host[bracket_end + 1] == ":":
            return host[: bracket_end + 1], int(host[bracket_end + 2 :])
        return host, DEFAULT_PORT
    if ":" in host:
        host_part, port_part = host.rsplit(":", 1)
        if port_part.isdigit():
            return host_part, int(port_part)
    return host, DEFAULT_PORT


def _build_ssl_context(tls: Any | None) -> tuple[ssl.SSLContext | None, str | None]:
    if tls is None:
        return None, None
    if isinstance(tls, ssl.SSLContext):
        raise ValueError("tls must not accept an SSLContext; configure CA and server name instead")
    if tls is True:
        return ssl.create_default_context(), None
    if isinstance(tls, Mapping):
        unknown = set(tls) - {"ca", "server_hostname"}
        if unknown:
            names = ", ".join(sorted(str(name) for name in unknown))
            raise ValueError(f"unsupported TLS options: {names}")
        context = ssl.create_default_context()
        ca = tls.get("ca")
        server_hostname = tls.get("server_hostname")
        if server_hostname is not None and not isinstance(server_hostname, str):
            raise ValueError("tls server_hostname must be a string")
        if ca is not None:
            if isinstance(ca, bytes):
                context.load_verify_locations(cadata=ca.decode("utf-8"))
            elif isinstance(ca, str) and "-----BEGIN" in ca:
                context.load_verify_locations(cadata=ca)
            else:
                context.load_verify_locations(cafile=ca)
        context.check_hostname = True
        context.verify_mode = ssl.CERT_REQUIRED
        return context, server_hostname
    raise ValueError("tls must be None, True, or a mapping with ca/server_hostname")


class Result:
    """Base class for typed read outcomes."""


class Value(Result):
    """A cache hit with a decoded value and optional TTL."""

    def __init__(self, value: Any, ttl_seconds: int | None = None):
        self.value = value
        self.ttl_seconds = ttl_seconds

    def __repr__(self) -> str:
        return f"Value({self.value!r}, ttl_seconds={self.ttl_seconds})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Value):
            return NotImplemented
        return self.value == other.value and self.ttl_seconds == other.ttl_seconds


class Miss(Result):
    """A provider refresh is pending; retry once if desired."""

    def __init__(self, retry_after_ms: int = 0):
        self.retry_after_ms = retry_after_ms

    def __repr__(self) -> str:
        return f"Miss(retry_after_ms={self.retry_after_ms})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Miss):
            return NotImplemented
        return self.retry_after_ms == other.retry_after_ms


class _Unknown(Result):
    """No value exists and no provider is registered for the key."""

    _instance: Optional["_Unknown"] = None

    def __new__(cls) -> "_Unknown":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "Unknown()"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _Unknown)


Unknown = _Unknown()


class Route:
    """A namespace + key pair."""

    def __init__(self, client: "Client", namespace: str, key: str):
        self._client = client
        self._namespace = namespace
        self._key = key

    async def get(self) -> Result:
        response = await self._client._request(Get(namespace=self._namespace, key=self._key))
        return self._client._to_result(response)

    async def get_with_retry(self) -> Result:
        first = await self.get()
        if not isinstance(first, Miss):
            return first
        if first.retry_after_ms > 0:
            await asyncio.sleep(first.retry_after_ms / 1000.0)
        return await self.get()

    async def get_value(self) -> Any | Miss:
        result = await self.get()
        if isinstance(result, Value):
            return result.value
        if isinstance(result, _Unknown):
            return None
        if isinstance(result, Miss):
            return result
        raise ValkyrError(f"unexpected read result: {result}")

    async def get_raw(self) -> dict[str, Any]:
        return await self._client._raw_request(Get(namespace=self._namespace, key=self._key))

    async def set(self, value: Any, *, ttl_seconds: int | None = None) -> None:
        await self._client._expect_ok(
            Set(namespace=self._namespace, key=self._key, value=value, ttl_seconds=ttl_seconds)
        )

    async def delete(self) -> None:
        await self._client._expect_ok(Delete(namespace=self._namespace, key_pattern=self._key))

    async def move(self, to_context: str) -> None:
        source = self._namespace
        if "::" not in source:
            raise RouteError(f"move requires a context namespace, got {source}")
        base, _ = source.split("::", 1)
        destination = f"{base}::{to_context}"
        await self._client._expect_ok(Move(source=source, destination=destination))


class Namespace:
    """A namespace scope."""

    def __init__(self, client: "Client", namespace: str):
        self._client = client
        self._namespace = namespace

    def key(self, key: str) -> Route:
        if not key:
            raise RouteError("key is required")
        return Route(self._client, self._namespace, key)

    async def set_many(self, mapping: dict[str, Any], *, ttl_seconds: int | None = None) -> None:
        entries = [SetEntry(key=k, value=v) for k, v in mapping.items()]
        await self._client._expect_ok(
            SetBatch(namespace=self._namespace, entries=entries, ttl_seconds=ttl_seconds)
        )

    async def delete(self, pattern: str | None = None) -> None:
        await self._client._expect_ok(Delete(namespace=self._namespace, key_pattern=pattern))

    async def ping(self) -> None:
        await self._client.ping()

    async def stats(self) -> StatsResponse:
        return await self._client.stats()


class Client:
    """Async application client for Valkyr."""

    def __init__(self, transport: Transport, api_key: str | None):
        self._transport = transport
        self._api_key = api_key

    @classmethod
    def connect(
        cls,
        host: str,
        port: int | None = None,
        *,
        api_key: str | None = None,
        tls: Any | None = None,
        timeout_secs: float = 30.0,
        auth_timeout_secs: float = 5.0,
    ) -> "_ClientConnector":
        return _ClientConnector(
            cls,
            host,
            port,
            api_key=api_key,
            tls=tls,
            timeout_secs=timeout_secs,
            auth_timeout_secs=auth_timeout_secs,
        )

    @classmethod
    async def _connect(
        cls,
        host: str,
        port: int | None = None,
        *,
        api_key: str | None = None,
        tls: Any | None = None,
        timeout_secs: float = 30.0,
        auth_timeout_secs: float = 5.0,
    ) -> "Client":
        host, port = _parse_address(host, port)
        ssl_context, server_hostname = _build_ssl_context(tls)
        transport = await Transport.open(
            host,
            port,
            ssl_context=ssl_context,
            server_hostname=server_hostname,
            timeout_secs=timeout_secs,
        )
        client = cls(transport, api_key)
        try:
            if api_key is not None:
                await authenticate(
                    transport,
                    api_key,
                    timeout_secs=auth_timeout_secs,
                    adapter_instance=None,
                )
        except BaseException:
            await transport.close()
            raise
        return client

    async def __aenter__(self) -> "Client":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def close(self) -> None:
        await self._transport.close()

    async def _request(self, cmd: Command) -> Any:
        return await self._transport.request(cmd)

    async def _raw_request(self, cmd: Command) -> dict[str, Any]:
        return await self._transport.raw_request(cmd)

    async def _expect_ok(self, cmd: Command) -> None:
        response = await self._transport.request(cmd)
        if not isinstance(response, Ok):
            raise ServerError(f"expected ok, got {response}")

    def namespace(self, name: str) -> Namespace:
        if not name:
            raise RouteError("namespace is required")
        return Namespace(self, name)

    async def ping(self) -> None:
        response = await self._transport.request(WirePing())
        if not isinstance(response, WirePong):
            raise ServerError(f"expected pong, got {response}")

    async def stats(self) -> StatsResponse:
        response = await self._transport.request(StatsCommand())
        if not isinstance(response, StatsResponse):
            raise ServerError(f"expected stats, got {response}")
        return response

    @staticmethod
    def _to_result(response: Any) -> Result:
        if isinstance(response, WireValue):
            return Value(response.value, response.ttl_seconds)
        if isinstance(response, WireMiss):
            return Miss(response.retry_after_ms)
        if isinstance(response, WireUnknown):
            return Unknown
        if isinstance(response, StatsResponse):
            raise ServerError("unexpected stats response for get")
        raise ServerError(f"unexpected response for get: {response}")


class _ClientConnector:
    """Deferred connection object returned by `Client.connect`."""

    def __init__(
        self,
        cls: type[Client],
        host: str,
        port: int | None,
        *,
        api_key: str | None,
        tls: Any | None,
        timeout_secs: float,
        auth_timeout_secs: float,
    ):
        self._cls = cls
        self._host = host
        self._port = port
        self._api_key = api_key
        self._tls = tls
        self._timeout_secs = timeout_secs
        self._auth_timeout_secs = auth_timeout_secs
        self._client: Client | None = None

    def __await__(self) -> Any:
        return self._connect().__await__()

    async def _connect(self) -> Client:
        self._client = await self._cls._connect(
            self._host,
            self._port,
            api_key=self._api_key,
            tls=self._tls,
            timeout_secs=self._timeout_secs,
            auth_timeout_secs=self._auth_timeout_secs,
        )
        return self._client

    async def __aenter__(self) -> Client:
        return await self._connect()

    async def __aexit__(self, *exc: object) -> None:
        if self._client is not None:
            await self._client.close()
