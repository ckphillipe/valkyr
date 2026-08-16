"""Basic fluent client example for Valkyr."""

import asyncio

from valkyr import Client, Miss, Unknown, Value


async def main() -> None:
    async with Client.connect(
        "127.0.0.1:8081",
        api_key="app-key",
    ) as client:
        users = client.namespace("/users")
        user = users.key("42")

        await user.set({"name": "Ada"}, ttl_seconds=300)
        value = await user.get_with_retry()
        if isinstance(value, Value):
            print(f"value: {value.value}")
        elif isinstance(value, Miss):
            print(f"provider is warming; retry after {value.retry_after_ms}ms")
        elif value is Unknown:
            print("value is absent")
        await user.delete()

        await users.set_many(
            {
                "session-1": {"user_id": "42"},
                "session-2": {"user_id": "43"},
            }
        )
        await client.ping()
        stats = await client.stats()
        print(f"stats: {stats.stats}")


if __name__ == "__main__":
    asyncio.run(main())
