"""Provide Open-Meteo temperatures through Valkyr.

Run the adapter with a Valkyr API key:
    VALKYR_API_KEY=... PYTHON_PATH=./valkyr-python/src python example/open_meteo_temperature_adapter.py

Clients can then read a Celsius temperature with:
    GET /weather/loc::23.2535,122.5618?temperature
"""

import asyncio
import contextlib
import json
import os
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from valkyr import Adapter, AdapterClient, Provider, ProviderValue

DEFAULT_VALKYR_ENDPOINT = "127.0.0.1:8081"
LOCATION_NAMESPACE_PREFIX = "/weather/loc::"


OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"


class OpenMeteoTemperatureProvider(Provider):
    """Fetch current Celsius temperatures from Open-Meteo."""

    def __init__(self, *, timeout_seconds: float = 10.0):
        self._timeout_seconds = timeout_seconds

    async def get(self, namespace: str, key: str) -> float | int | ProviderValue | None:
        if key != "temperature":
            return None
        latitude, longitude = _coordinates_from_namespace(namespace)
        temperature = await asyncio.to_thread(
            _fetch_current_temperature_celsius,
            latitude,
            longitude,
            self._timeout_seconds,
        )
        if temperature is None:
            return None
        return ProviderValue(temperature, ttl_seconds=20)


def _coordinates_from_namespace(namespace: str) -> tuple[float, float]:
    if not namespace.startswith(LOCATION_NAMESPACE_PREFIX):
        raise ValueError(f"unsupported weather namespace: {namespace}")
    location = namespace.removeprefix(LOCATION_NAMESPACE_PREFIX)
    try:
        latitude_text, longitude_text = location.split(",", maxsplit=1)
        latitude = float(latitude_text)
        longitude = float(longitude_text)
    except ValueError as error:
        raise ValueError(f"invalid latitude,longitude context: {location}") from error
    if not -90 <= latitude <= 90 or not -180 <= longitude <= 180:
        raise ValueError(f"latitude,longitude is out of range: {location}")
    return latitude, longitude


def _fetch_current_temperature_celsius(
    latitude: float,
    longitude: float,
    timeout_seconds: float,
) -> float | int | None:
    query = urlencode(
        {
            "latitude": latitude,
            "longitude": longitude,
            "current": "temperature_2m",
            "temperature_unit": "celsius",
        }
    )
    request = Request(f"{OPEN_METEO_FORECAST_URL}?{query}")
    started_at = time.perf_counter()
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            payload: dict[str, Any] = json.load(response)
    except HTTPError as error:
        raise RuntimeError(f"Open-Meteo returned HTTP {error.code}") from error
    except URLError as error:
        raise RuntimeError("Open-Meteo request failed") from error
    finally:
        elapsed_ms = (time.perf_counter() - started_at) * 1000
        print(f"Open-Meteo request took {elapsed_ms:.1f} ms")

    try:
        temperature = payload["current"]["temperature_2m"]
    except (KeyError, TypeError) as error:
        raise RuntimeError("Open-Meteo returned an unexpected forecast response") from error
    if temperature is None:
        return None
    if isinstance(temperature, bool) or not isinstance(temperature, (int, float)):
        raise TypeError("Open-Meteo returned a non-numeric Celsius temperature")
    return temperature


async def main() -> None:
    provider = OpenMeteoTemperatureProvider()
    adapter = Adapter().provide(
        "/weather/loc",
        "temperature",
        provider,
        max_rate=20,
        timeout=500,
        miss_ttl=20,
    )
    client = await AdapterClient.connect(
        os.getenv("VALKYR_ENDPOINT", DEFAULT_VALKYR_ENDPOINT),
        api_key=os.environ["VALKYR_API_KEY"],
        adapter=adapter,
    )
    with contextlib.suppress(asyncio.CancelledError):
        await client.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
