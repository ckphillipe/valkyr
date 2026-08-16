"""Typed Valkyr Python SDK exceptions."""


class ValkyrError(Exception):
    """Base exception for all Valkyr SDK errors."""

    def __init__(self, message: str | None = None):
        super().__init__(message)
        self.message = message


class ProtocolError(ValkyrError):
    """Malformed or unsupported protocol frame."""


class ConnectionError(ValkyrError):
    """The underlying transport connection failed or was closed."""


class TimeoutError(ValkyrError):
    """A request did not receive a response within the configured timeout."""


class AuthError(ValkyrError):
    """Authentication was rejected by the server."""


class AuthenticationRejectedError(AuthError):
    """The server definitively rejected the credential."""


class AuthPending(ValkyrError):  # noqa: N818
    """Authentication is warming; retry after the supplied delay."""

    def __init__(self, message: str | None = None, *, retry_after_ms: int = 0):
        super().__init__(message)
        self.retry_after_ms = retry_after_ms


class OverloadError(ValkyrError):
    """Callback concurrency is exhausted; the server cannot be acknowledged."""


class ServerError(ValkyrError):
    """The server returned a request-level error without closing the connection."""


class RouteError(ValkyrError):
    """A route or namespace/key was required but not provided."""
