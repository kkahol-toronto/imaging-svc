"""Recently-transcoded image cache.

In-process LRU + TTL so we don't round-trip to redis on the hot path.
The previous version was an unbounded global list that grew until the
pod hit its memory limit and was OOMKilled. The cache is now bounded by
both entry count and per-entry TTL.
"""

from __future__ import annotations

from cachetools import TTLCache
from threading import RLock


# Bounded cache: at most 10_000 entries, each one expires after 10 minutes.
# Tunable via env in a follow-up; the immediate goal is to stop the
# OOMKills happening in prod.
_CACHE: "TTLCache[str, bytes]" = TTLCache(maxsize=10_000, ttl=600)
_LOCK = RLock()


def cache_get(image_id: str) -> bytes | None:
    with _LOCK:
        return _CACHE.get(image_id)


def cache_put(image_id: str, value: bytes) -> None:
    with _LOCK:
        _CACHE[image_id] = value
