"""Adapter registration model."""

import uuid
from dataclasses import dataclass

from ._handlers import Provider, Store

MAX_U32 = 2**32 - 1
MAX_U64 = 2**64 - 1


def _validate_option(name: str, value: int | None, *, maximum: int = MAX_U64) -> None:
    if value is not None and (
        isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= maximum
    ):
        raise ValueError(f"{name} must be a non-negative integer no greater than {maximum}")


def _validate_max_rate(value: int | None) -> None:
    if value == 0:
        raise ValueError("max_rate must be a non-zero u32 integer or None")
    try:
        _validate_option("max_rate", value, maximum=MAX_U32)
    except ValueError as error:
        raise ValueError("max_rate must be a non-zero u32 integer or None") from error


def _patterns_overlap(left: str, right: str) -> bool:
    if left == right or left == "*" or right == "*":
        return True
    left_positions = [
        position for position in (left.find("*"), left.find("{"), left.find("${")) if position >= 0
    ]
    right_positions = [
        position
        for position in (right.find("*"), right.find("{"), right.find("${"))
        if position >= 0
    ]
    left_prefix = left[: min(left_positions, default=len(left))]
    right_prefix = right[: min(right_positions, default=len(right))]
    return left_prefix.startswith(right_prefix) or right_prefix.startswith(left_prefix)


@dataclass(frozen=True)
class ProvideRoute:
    namespace_pattern: str
    key_pattern: str
    provider: Provider
    max_rate: int | None = None
    timeout: int | None = None
    miss_ttl: int | None = None


@dataclass(frozen=True)
class StoreRoute:
    namespace_pattern: str
    key_pattern: str
    store: Store


class Adapter:
    """Collects provider and store route registrations for an adapter."""

    def __init__(self) -> None:
        self.provide_routes: list[ProvideRoute] = []
        self.store_routes: list[StoreRoute] = []

    def provide(
        self,
        namespace_pattern: str,
        key_pattern: str,
        provider: Provider,
        *,
        max_rate: int | None = None,
        timeout: int | None = None,
        miss_ttl: int | None = None,
    ) -> "Adapter":
        _validate_max_rate(max_rate)
        _validate_option("timeout", timeout)
        _validate_option("miss_ttl", miss_ttl)
        for existing in self.provide_routes:
            if _patterns_overlap(
                existing.namespace_pattern, namespace_pattern
            ) and _patterns_overlap(existing.key_pattern, key_pattern):
                raise ValueError("overlapping provider registrations are ambiguous")
        self.provide_routes.append(
            ProvideRoute(
                namespace_pattern=namespace_pattern,
                key_pattern=key_pattern,
                provider=provider,
                max_rate=max_rate,
                timeout=timeout,
                miss_ttl=miss_ttl,
            )
        )
        return self

    def store(
        self,
        namespace_pattern: str,
        key_pattern: str,
        store: Store,
    ) -> "Adapter":
        self.store_routes.append(
            StoreRoute(
                namespace_pattern=namespace_pattern,
                key_pattern=key_pattern,
                store=store,
            )
        )
        return self

    def adapter_instance(self) -> str:
        """Return a fresh UUID for this adapter configuration."""
        return str(uuid.uuid4())
