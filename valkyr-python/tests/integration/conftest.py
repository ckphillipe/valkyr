"""Integration-test fixtures for Valkyr Python SDK."""

import asyncio
import contextlib
import shutil
import socket
import subprocess
import tempfile
import time
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_port(port: int, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with (
            contextlib.suppress(OSError),
            socket.create_connection(("127.0.0.1", port), timeout=0.5),
        ):
            return
        time.sleep(0.05)
    raise RuntimeError(f"server did not accept connections on port {port}")


@pytest.fixture(scope="session")
def server_bin() -> Path:
    """Locate the Valkyr server binary, building it if necessary."""
    target_dir = Path(__file__).parents[3] / "target" / "debug"
    bin_path = target_dir / "valkyr-server"
    if not bin_path.exists():
        subprocess.run(
            ["cargo", "build", "-p", "valkyr-server"],
            check=True,
            cwd=Path(__file__).parents[3],
        )
    return bin_path


def _generate_tls_material(tmp_path: Path) -> tuple[Path, Path]:
    openssl = shutil.which("openssl")
    if openssl is None:
        pytest.skip("TLS integration requires openssl to create an ephemeral certificate")
    key_file = tmp_path / "tls.key"
    certificate_file = tmp_path / "tls.crt"
    subprocess.run(
        [
            openssl,
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-keyout",
            str(key_file),
            "-out",
            str(certificate_file),
            "-days",
            "1",
            "-nodes",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost,IP:127.0.0.1",
        ],
        check=True,
        capture_output=True,
    )
    return certificate_file, key_file


@asynccontextmanager
async def _running_server(server_bin: Path, *, tls: bool) -> AsyncIterator[dict[str, Any]]:
    """Run an isolated native server, optionally with its TLS listener."""
    native_port = _free_port()
    http_port = _free_port()
    metrics_port = _free_port()
    tls_port = _free_port()
    bootstrap_key = "integration-bootstrap-key"

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        key_file = tmp_path / "bootstrap-api-key"
        key_file.write_text(bootstrap_key)
        tls_config = ""
        tls_details: dict[str, Any] = {}
        if tls:
            certificate_file, private_key_file = _generate_tls_material(tmp_path)
            tls_config = f"""
tls:
  listen: 127.0.0.1:{tls_port}
  certificate_file: {certificate_file}
  private_key_file: {private_key_file}
"""
            tls_details = {"tls_port": tls_port, "tls_ca": str(certificate_file)}
        config_file = tmp_path / "server.yml"
        config_file.write_text(
            f"""
native_listen: 127.0.0.1:{native_port}
http_listen: 127.0.0.1:{http_port}
metrics_listen: 127.0.0.1:{metrics_port}
log_filter: error
{tls_config}
auth:
  bootstrap_api_key_file: {key_file}
  session_ttl_seconds: 3600
"""
        )

        process = await asyncio.create_subprocess_exec(
            str(server_bin),
            "--config",
            str(config_file),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=tmp_path,
        )
        try:
            _wait_for_port(native_port, timeout=60.0)
            if tls:
                _wait_for_port(tls_port, timeout=60.0)
            details = {
                "host": "127.0.0.1",
                "native_port": native_port,
                "http_port": http_port,
                "metrics_port": metrics_port,
                "bootstrap_key": bootstrap_key,
            }
            details.update(tls_details)
            yield details
        finally:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=5.0)
            except TimeoutError:
                process.kill()
                await asyncio.wait_for(process.wait(), timeout=5.0)


@pytest.fixture
async def valkyr_server(server_bin: Path) -> AsyncGenerator[dict[str, Any], None]:
    """Start a temporary plain TCP Valkyr server."""
    async with _running_server(server_bin, tls=False) as details:
        yield details


@pytest.fixture
async def tls_valkyr_server(server_bin: Path) -> AsyncGenerator[dict[str, Any], None]:
    """Start a temporary server with a configured native TLS listener."""
    async with _running_server(server_bin, tls=True) as details:
        yield details
