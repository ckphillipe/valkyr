"""Handler interfaces for provider and store adapters."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ProviderValue:
    """A provider value with an optional cache TTL in whole seconds."""

    value: Any
    ttl_seconds: int | None = None


class Provider(ABC):
    """Called by Valkyr to fill a cache miss."""

    @abstractmethod
    async def get(self, namespace: str, key: str) -> Any | ProviderValue | None:
        """Return a raw value, a TTL-bearing value, or ``None`` for a miss."""


class Store(ABC):
    """Called by Valkyr before committing a durable mutation."""

    @abstractmethod
    async def set(
        self, namespace: str, key: str, value: Any, ttl_seconds: int | None = None
    ) -> None:
        """Persist a single value."""

    @abstractmethod
    async def set_many(
        self,
        namespace: str,
        entries: list[dict],
        ttl_seconds: int | None = None,
    ) -> None:
        """Persist a batch of values."""

    @abstractmethod
    async def delete(self, namespace: str, key_pattern: str | None) -> None:
        """Delete matching keys."""

    @abstractmethod
    async def move(self, source: str, destination: str) -> None:
        """Move the full source context to the full destination context."""
