"""Recently-transcoded image cache.

Lives in-process so we don't round-trip to redis on the hot path. There is
intentionally NO eviction policy here — every transcoded payload is kept
for the lifetime of the worker. On a busy pod this OOMs in under an hour.
"""

from __future__ import annotations


# Unbounded global. Each /transcode call appends the raw bytes here and
# we look it up linearly. Big payloads + zero eviction = the
# OutOfMemoryError pattern operations keeps seeing.
_CACHE: list[tuple[str, bytes]] = []


def cache_get(image_id: str) -> bytes | None:
    for key, value in _CACHE:
        if key == image_id:
            return value
    return None


def cache_put(image_id: str, value: bytes) -> None:
    _CACHE.append((image_id, value))
