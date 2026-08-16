"""Authentication helpers for Valkyr connections."""

import asyncio
import builtins
import time

from ._errors import AuthenticationRejectedError, AuthError
from ._transport import Transport
from ._wire import Auth as AuthCommand
from ._wire import AuthFailure, AuthSuccess
from ._wire import AuthPending as AuthPendingResponse


async def authenticate_once(
    transport: Transport, api_key: str, *, adapter_instance: str | None = None
) -> AuthSuccess | AuthPendingResponse:
    """Send one ``auth`` command and return its typed auth outcome.

    A pending response is returned for callers that want to control retry
    timing. Confirmed authentication failures remain exceptions.
    """
    response = await transport.request(
        AuthCommand(api_key=api_key, adapter_instance=adapter_instance)
    )
    if isinstance(response, AuthSuccess):
        return response
    if isinstance(response, AuthPendingResponse):
        return response
    if isinstance(response, AuthFailure):
        raise AuthenticationRejectedError(response.message)
    raise AuthError(f"authentication failed: {response}")


async def authenticate(
    transport: Transport,
    api_key: str,
    *,
    timeout_secs: float,
    adapter_instance: str | None = None,
) -> AuthSuccess:
    """Authenticate with bounded retry on ``AuthPending``."""
    deadline = time.monotonic() + timeout_secs
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise AuthError(f"authentication timeout after {timeout_secs}s")
        try:
            response = await asyncio.wait_for(
                authenticate_once(
                    transport,
                    api_key,
                    adapter_instance=adapter_instance,
                ),
                timeout=remaining,
            )
        except builtins.TimeoutError as exc:
            await transport.close()
            raise AuthError(f"authentication timeout after {timeout_secs}s") from exc
        if isinstance(response, AuthSuccess):
            return response
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise AuthError(f"authentication timeout after {timeout_secs}s")
        delay = min(response.retry_after_ms / 1000.0, remaining)
        if delay > 0:
            await asyncio.sleep(delay)
