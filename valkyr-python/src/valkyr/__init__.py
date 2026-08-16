"""Python SDK for Valkyr protocol v1."""

from ._auth import authenticate_once
from ._client import Client, Miss, Result, Unknown, Value
from ._errors import (
    AuthenticationRejectedError,
    AuthError,
    AuthPending,
    ConnectionError,
    OverloadError,
    ProtocolError,
    RouteError,
    ServerError,
    TimeoutError,
    ValkyrError,
)
from ._handlers import Provider, ProviderValue, Store
from ._registration import Adapter
from ._wire import AuthPending as AuthPendingResult
from ._wire import AuthSuccess
from .adapter import AdapterClient

__all__ = [
    "Client",
    "Adapter",
    "AdapterClient",
    "Provider",
    "ProviderValue",
    "Store",
    "Value",
    "Miss",
    "Unknown",
    "Result",
    "ValkyrError",
    "ProtocolError",
    "ConnectionError",
    "TimeoutError",
    "AuthError",
    "AuthenticationRejectedError",
    "AuthPending",
    "OverloadError",
    "ServerError",
    "RouteError",
    "authenticate_once",
    "AuthSuccess",
    "AuthPendingResult",
]

__version__ = "0.1.0"
