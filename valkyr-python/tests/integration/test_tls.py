"""TLS integration tests for the Python SDK."""

import pytest

from valkyr import Client
from valkyr._errors import ConnectionError

pytestmark = [pytest.mark.integration, pytest.mark.tls]


@pytest.mark.asyncio
async def test_tls_connection(tls_valkyr_server):
    async with Client.connect(
        tls_valkyr_server["host"],
        tls_valkyr_server["tls_port"],
        api_key=tls_valkyr_server["bootstrap_key"],
        tls={"ca": tls_valkyr_server["tls_ca"], "server_hostname": "localhost"},
        auth_timeout_secs=10.0,
    ) as client:
        await client.ping()


@pytest.mark.asyncio
async def test_tls_rejects_wrong_server_name(tls_valkyr_server):
    with pytest.raises(ConnectionError):
        async with Client.connect(
            tls_valkyr_server["host"],
            tls_valkyr_server["tls_port"],
            api_key=tls_valkyr_server["bootstrap_key"],
            tls={"ca": tls_valkyr_server["tls_ca"], "server_hostname": "wrong.localhost"},
            auth_timeout_secs=10.0,
        ):
            pass
