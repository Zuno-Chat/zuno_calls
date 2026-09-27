import pytest

from zuno_calls.ratelimit import RateLimiter


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def test_burst_then_reject_with_retry_after():
    clock = Clock()
    limiter = RateLimiter(per_second=1, burst=2, clock=clock)
    assert limiter.allow("k") is None
    assert limiter.allow("k") is None
    assert limiter.allow("k") == 1000
    clock.now += 0.5
    assert limiter.allow("k") == 500
    clock.now += 0.5
    assert limiter.allow("k") is None


def test_refill_never_exceeds_burst():
    clock = Clock()
    limiter = RateLimiter(per_second=10, burst=3, clock=clock)
    clock.now += 100
    assert [limiter.allow("k") for _ in range(4)] == [None, None, None, 100]


def test_keys_are_independent():
    limiter = RateLimiter(per_second=1, burst=1, clock=Clock())
    assert limiter.allow(("@a:x", "D1")) is None
    assert limiter.allow(("@a:x", "D2")) is None
    assert limiter.allow(("@a:x", "D1")) == 1000


def test_idle_keys_are_purged_when_the_map_doubles():
    clock = Clock()
    limiter = RateLimiter(per_second=1, burst=2, clock=clock)
    for i in range(63):
        limiter.allow(i)
    assert len(limiter) == 63
    clock.now += 5  # past burst/per_second, every bucket is full again
    limiter.allow("fresh")
    assert len(limiter) == 1
    limiter.allow("fresh")
    assert limiter.allow("fresh") == 1000


def test_non_positive_rate_is_rejected():
    with pytest.raises(ValueError):
        RateLimiter(per_second=0, burst=1)


def test_burst_below_one_is_rejected():
    with pytest.raises(ValueError):
        RateLimiter(per_second=1, burst=0)
