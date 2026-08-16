"""Async human-readable text protocol transport over TCP or TLS."""

import asyncio
import builtins
import ssl
from typing import Any

from ._errors import ConnectionError, ProtocolError, TimeoutError
from ._wire import (
    Command,
    Response,
    ServerCommand,
    ServerResult,
    command_to_line,
    response_from_line,
    server_command_from_line,
    server_result_to_line,
)


class Transport:
    """Ordered newline-framed text transport with a one MiB frame bound."""

    MAX_FRAME_BYTES = 1024 * 1024

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        timeout_secs: float = 30.0,
    ):
        self._reader, self._writer = reader, writer
        self._timeout = timeout_secs
        self._write_lock = asyncio.Lock()
        self._closed = False

    @classmethod
    async def open(
        cls,
        host: str,
        port: int,
        *,
        ssl_context: ssl.SSLContext | None = None,
        server_hostname: str | None = None,
        timeout_secs: float = 30.0,
    ) -> "Transport":
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(
                    host,
                    port,
                    ssl=ssl_context,
                    server_hostname=server_hostname,
                    limit=cls.MAX_FRAME_BYTES,
                ),
                timeout=timeout_secs,
            )
        except builtins.TimeoutError as exc:
            raise TimeoutError(f"connection to {host}:{port} timed out") from exc
        except OSError as exc:
            raise ConnectionError(f"connection to {host}:{port} failed: {exc}") from exc
        return cls(reader, writer, timeout_secs=timeout_secs)

    async def close(self) -> None:
        self._closed = True
        if self._writer.is_closing():
            return
        try:
            self._writer.close()
            await self._writer.wait_closed()
        except (OSError, RuntimeError):
            pass

    @property
    def closed(self) -> bool:
        return self._closed or self._writer.is_closing()

    async def _read_line(self, *, timeout_secs: float | None = None) -> str:
        if self._closed:
            raise ConnectionError("transport is closed")
        try:
            read = self._reader.readline()
            line = (
                await read
                if timeout_secs is None
                else await asyncio.wait_for(read, timeout=timeout_secs)
            )
        except builtins.TimeoutError as exc:
            await self.close()
            raise TimeoutError("frame read timed out") from exc
        except (OSError, RuntimeError, ValueError) as exc:
            await self.close()
            if isinstance(exc, ValueError):
                raise ProtocolError(f"frame exceeds {self.MAX_FRAME_BYTES} bytes") from exc
            raise ConnectionError(f"frame read failed: {exc}") from exc
        if not line:
            await self.close()
            raise ConnectionError("connection closed by peer")
        try:
            text = line.rstrip(b"\r\n").decode("utf-8")
        except UnicodeDecodeError as exc:
            await self.close()
            raise ProtocolError("frame is not valid UTF-8") from exc
        if not text:
            await self.close()
            raise ProtocolError("empty frame")
        return text

    async def _write_line(self, text: str) -> None:
        if self._closed:
            raise ConnectionError("transport is closed")
        try:
            self._writer.write((text + "\n").encode("utf-8"))
            await self._writer.drain()
        except (OSError, RuntimeError) as exc:
            await self.close()
            raise ConnectionError(f"write failed: {exc}") from exc

    async def request(self, cmd: Command) -> Response:
        try:
            async with self._write_lock:
                await self._write_line(command_to_line(cmd))
                line = await self._read_line(timeout_secs=self._timeout)
            return response_from_line(cmd, line)
        except asyncio.CancelledError:
            await self.close()
            raise
        except (ConnectionError, ProtocolError, TimeoutError):
            await self.close()
            raise

    async def raw_request(self, cmd: Command) -> dict[str, Any]:
        return (await self.request(cmd)).to_dict()

    async def write_result(self, command: ServerCommand, result: ServerResult) -> None:
        async with self._write_lock:
            await self._write_line(server_result_to_line(command, result))

    async def pipeline(self, cmds: list[Command]) -> list[Response]:
        if not cmds:
            return []
        try:
            async with self._write_lock:
                for cmd in cmds:
                    await self._write_line(command_to_line(cmd))
                return [
                    response_from_line(cmd, await self._read_line(timeout_secs=self._timeout))
                    for cmd in cmds
                ]
        except asyncio.CancelledError:
            await self.close()
            raise
        except (ConnectionError, ProtocolError, TimeoutError):
            await self.close()
            raise

    async def read_command(self) -> ServerCommand:
        return server_command_from_line(await self._read_line())
