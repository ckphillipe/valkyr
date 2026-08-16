"""Transport poisoning and authentication boundary tests."""

import asyncio
import ssl

import pytest

from valkyr import AuthPendingResult, AuthSuccess, Miss, authenticate_once
from valkyr._client import Client, _build_ssl_context
from valkyr._errors import ProtocolError, TimeoutError
from valkyr._transport import Transport
from valkyr._wire import AuthPending as WireAuthPending
from valkyr._wire import Ping, Pong, Query


class FakeWriter:
    def __init__(self) -> None:
        self.data: list[bytes] = []
        self.closed = False

    def write(self, data: bytes) -> None:
        self.data.append(data)

    async def drain(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        return None

    def is_closing(self) -> bool:
        return self.closed


class FakeReader:
    def __init__(self, line: bytes | None = None, *, wait: bool = False) -> None:
        self.line = line
        self.wait = wait

    async def readline(self) -> bytes:
        if self.wait:
            await asyncio.sleep(10)
        assert self.line is not None
        return self.line


class SlowReader:
    async def readline(self) -> bytes:
        await asyncio.sleep(0.02)
        return b"QUERY 00000000-0000-0000-0000-000000000001 /users 42\n"


class AuthTransport:
    def __init__(self, response: object) -> None:
        self.response = response

    async def request(self, command: object) -> object:
        return self.response


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("line", "expected"),
    [
        (b"not-json\n", ProtocolError),
        (b"FUTURE\n", ProtocolError),
    ],
)
async def test_malformed_and_unknown_responses_poison_ordered_transport(line, expected):
    writer = FakeWriter()
    transport = Transport(FakeReader(line), writer, timeout_secs=1.0)

    with pytest.raises(expected):
        await transport.request(Ping())

    assert transport.closed
    assert writer.closed


@pytest.mark.asyncio
async def test_response_timeout_poison_ordered_transport():
    writer = FakeWriter()
    transport = Transport(FakeReader(wait=True), writer, timeout_secs=0.01)

    with pytest.raises(TimeoutError):
        await transport.request(Ping())

    assert transport.closed
    assert writer.closed


@pytest.mark.asyncio
async def test_transport_configures_the_protocol_frame_limit(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_open_connection(*args: object, **kwargs: object):
        captured.update(kwargs)
        return FakeReader(b"PONG\n"), FakeWriter()

    monkeypatch.setattr(asyncio, "open_connection", fake_open_connection)
    await Transport.open("127.0.0.1", 8081)

    assert captured["limit"] == 1024 * 1024


@pytest.mark.asyncio
async def test_adapter_stream_reads_do_not_use_request_timeout():
    transport = Transport(SlowReader(), FakeWriter(), timeout_secs=0.001)

    assert await transport.read_command() == Query(
        request_id="00000000-0000-0000-0000-000000000001",
        namespace="/users",
        key="42",
    )


@pytest.mark.asyncio
async def test_request_decodes_text_pong():
    transport = Transport(FakeReader(b"PONG\n"), FakeWriter())

    assert await transport.request(Ping()) == Pong()


@pytest.mark.asyncio
async def test_read_command_decodes_callback_text_line():
    transport = Transport(
        FakeReader(b"QUERY 00000000-0000-0000-0000-000000000001 /users 42\n"),
        FakeWriter(),
    )

    assert await transport.read_command() == Query(
        request_id="00000000-0000-0000-0000-000000000001",
        namespace="/users",
        key="42",
    )


@pytest.mark.asyncio
async def test_raw_request_returns_validated_typed_response():
    transport = Transport(
        FakeReader(b"PONG\n"),
        FakeWriter(),
    )

    raw = await transport.raw_request(Ping())

    assert raw == Pong().to_dict()


@pytest.mark.asyncio
async def test_authenticate_once_returns_typed_pending_result():
    result = await authenticate_once(
        AuthTransport(WireAuthPending(retry_after_ms=25)),
        "key",
    )

    assert isinstance(result, AuthPendingResult)
    assert result.retry_after_ms == 25
    assert AuthSuccess is not None


@pytest.mark.asyncio
async def test_get_value_propagates_miss():
    class FakeClientTransport:
        async def request(self, command: object) -> object:
            from valkyr._wire import Miss as WireMiss

            return WireMiss(retry_after_ms=25)

    result = await Client(FakeClientTransport(), None).namespace("/users").key("42").get_value()

    assert isinstance(result, Miss)
    assert result.retry_after_ms == 25


def test_tls_rejects_arbitrary_ssl_context():
    with pytest.raises(ValueError, match="must not accept an SSLContext"):
        _build_ssl_context(ssl._create_unverified_context())


def test_tls_mapping_honors_server_name_and_keeps_verification_mandatory():
    context, server_hostname = _build_ssl_context({"server_hostname": "localhost"})

    assert context is not None
    assert context.check_hostname
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert server_hostname == "localhost"
