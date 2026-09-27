"""Per-key token bucket. One instance per process; the gateway routes these paths to main."""

from __future__ import annotations

import math
import time
from collections.abc import Callable, Hashable
from dataclasses import dataclass

# Below this many keys a purge is not worth the scan.
_PURGE_FLOOR = 64


@dataclass
class _Bucket:
    tokens: float
    updated: float


class RateLimiter:
    def __init__(
        self, per_second: float, burst: int, clock: Callable[[], float] = time.monotonic
    ) -> None:
        if per_second <= 0:
            raise ValueError(f"per_second must be positive: {per_second!r}")
        if burst < 1:
            raise ValueError(f"burst must be at least 1: {burst!r}")
        self._rate = per_second
        self._burst = float(burst)
        self._clock = clock
        self._buckets: dict[Hashable, _Bucket] = {}
        self._purge_at = _PURGE_FLOOR

    def allow(self, key: Hashable) -> int | None:
        """None when a token was taken; otherwise milliseconds until one is available."""
        now = self._clock()
        bucket = self._buckets.get(key)
        if bucket is None:
            bucket = _Bucket(self._burst, now)
            self._buckets[key] = bucket
            self._maybe_purge(now)
        else:
            bucket.tokens = min(self._burst, bucket.tokens + (now - bucket.updated) * self._rate)
            bucket.updated = now
        if bucket.tokens >= 1.0:
            bucket.tokens -= 1.0
            return None
        return max(1, math.ceil((1.0 - bucket.tokens) / self._rate * 1000))

    def __len__(self) -> int:
        return len(self._buckets)

    def _maybe_purge(self, now: float) -> None:
        # No background job: when the map has doubled since the last purge,
        # drop every key idle long enough for its bucket to be full again.
        if len(self._buckets) < self._purge_at:
            return
        idle = self._burst / self._rate
        for key in [k for k, b in self._buckets.items() if now - b.updated >= idle]:
            del self._buckets[key]
        self._purge_at = max(_PURGE_FLOOR, 2 * len(self._buckets))
