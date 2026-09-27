"""Outbound HTTP to Cloudflare.

Own Agent and connection pool, never ``api.http_client``: that one has a
fixed 60 s timeout and shares its pool with Synapse's identity, preview and
push traffic. Every awaited Deferred goes through make_deferred_yieldable so
Synapse's logging context rules hold inside the request handler.
"""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Mapping
from dataclasses import dataclass
from io import BytesIO
from typing import Any

from synapse.module_api import make_deferred_yieldable
from twisted.internet import error as net_error
from twisted.internet.defer import CancelledError, Deferred
from twisted.internet.defer import TimeoutError as DeferredTimeoutError
from twisted.internet.protocol import Protocol
from twisted.internet.task import deferLater
from twisted.python.failure import Failure
from twisted.web.client import (
    Agent,
    FileBodyProducer,
    HTTPConnectionPool,
    ResponseDone,
    ResponseFailed,
    ResponseNeverReceived,
)
from twisted.web.http_headers import Headers
from twisted.web.iweb import IResponse

from .metrics import UPSTREAM_ERRORS, UPSTREAM_REQUESTS, UPSTREAM_RETRIES, UPSTREAM_SECONDS

_logger = logging.getLogger(__name__)

MAX_RESPONSE_BODY = 1 << 20
# Cloudflare closes idle keep-alives on its own schedule, and a connection
# reused after that fails without a response byte (a 502 for the SFU, which
# never retries). One minute keeps the pool well inside that window.
_IDLE_CONNECTION_TIMEOUT = 60  # seconds; Twisted types the pool's field as int
_RETRYABLE_REASONS = frozenset({"timeout", "connect"})
# Only in a ResponseNeverReceived: the peer closed before any response byte
# arrived, which is as retry-safe as a failure to connect. A ResponseFailed
# carrying the same reason had a response, possibly a truncated one.
_CLOSED_BEFORE_RESPONSE = (
    net_error.ConnectionLost,
    net_error.ConnectionDone,
    net_error.ConnectionClosed,
)


@dataclass(frozen=True)
class RetryPolicy:
    retries: int = 0
    base_delay: float = 0.1  # seconds; doubles per attempt, with jitter


NO_RETRY = RetryPolicy()
TURN_RETRY = RetryPolicy(retries=2, base_delay=0.1)


@dataclass(frozen=True)
class UpstreamResponse:
    status: int
    content_type: str | None
    body: bytes


class UpstreamUnavailable(Exception):
    """No usable response: connect failure, timeout, oversized body; the metric label ``reason``."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class _BodyTooLarge(Exception):
    pass


class _Collector(Protocol):
    """Collects a response body up to a limit; past it the connection is dropped."""

    def __init__(self, limit: int) -> None:
        self._limit = limit
        self._buf = BytesIO()
        self._too_large = False
        self.deferred: Deferred[bytes] = Deferred(self._cancel)

    def _drop(self) -> None:
        # deliverBody hands a body protocol a TransportProxyProducer, which has
        # no abortConnection, so loseConnection is the effective path today;
        # abortConnection is tried first only in case of a richer transport.
        transport = self.transport
        for name in ("abortConnection", "loseConnection", "stopProducing"):
            method = getattr(transport, name, None)
            if method is not None:
                method()
                return

    def _cancel(self, _: Deferred[bytes]) -> None:
        self._drop()

    def dataReceived(self, data: bytes) -> None:
        if self.deferred.called or self._too_large:
            return
        if self._buf.tell() + len(data) > self._limit:
            # Fail here rather than at connectionLost: a sender that stalls
            # after an oversized body would otherwise be reported as a timeout.
            self._too_large = True
            self._buf = BytesIO()
            self._drop()
            if not self.deferred.called:
                self.deferred.errback(_BodyTooLarge())
            return
        self._buf.write(data)

    def connectionLost(self, reason: Failure | None = None) -> None:
        if self.deferred.called:
            return
        if self._too_large:
            self.deferred.errback(_BodyTooLarge())
        elif reason is None or reason.check(ResponseDone):  # type: ignore[no-untyped-call]
            self.deferred.callback(self._buf.getvalue())
        else:
            # Includes PotentialDataLoss: a connection-delimited body that may be short.
            self.deferred.errback(reason)


def _classify(exc: BaseException) -> str:
    if isinstance(exc, _BodyTooLarge):
        return "too_large"
    if isinstance(exc, (DeferredTimeoutError, CancelledError, net_error.TimeoutError)):
        return "timeout"
    if isinstance(exc, (net_error.ConnectError, net_error.DNSLookupError)):
        return "connect"
    if isinstance(exc, ResponseFailed):
        reasons = [r.value for r in exc.reasons]
        kinds = [_classify(r) for r in reasons]
        for kind in ("timeout", "connect"):
            if kind in kinds:
                return kind
        if isinstance(exc, ResponseNeverReceived) and any(
            isinstance(r, _CLOSED_BEFORE_RESPONSE) for r in reasons
        ):
            return "connect"
    return "other"


def _retryable(status: int) -> bool:
    return status == 429 or status >= 500


class Upstream:
    def __init__(
        self, timeout: float, *, reactor: Any = None, max_body: int = MAX_RESPONSE_BODY
    ) -> None:
        if reactor is None:
            from twisted.internet import reactor as global_reactor

            reactor = global_reactor
        self._reactor = reactor
        self._timeout = timeout
        self._max_body = max_body
        pool = HTTPConnectionPool(reactor, persistent=True)  # type: ignore[no-untyped-call]
        pool.maxPersistentPerHost = 8
        pool.cachedConnectionTimeout = _IDLE_CONNECTION_TIMEOUT
        self._pool = pool
        self._agent = Agent(reactor, connectTimeout=timeout, pool=pool)  # type: ignore[no-untyped-call]

    def close(self) -> Deferred[Any]:
        """Fires once the pooled connections this client owns are gone."""
        return self._pool.closeCachedConnections()  # type: ignore[no-any-return,no-untyped-call]

    async def request(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        body: bytes | None,
        *,
        api: str,
        retry: RetryPolicy = NO_RETRY,
        timeout: float | None = None,
    ) -> UpstreamResponse:
        """One attempt per `retry`; `timeout` bounds each attempt, defaulting to the instance's."""
        per_attempt = self._timeout if timeout is None else timeout
        attempt = 0
        while True:
            started = time.monotonic()
            try:
                response = await self._once(method, url, headers, body, per_attempt)
            except UpstreamUnavailable as e:
                UPSTREAM_ERRORS.labels(api, e.reason).inc()
                _logger.warning(
                    "upstream %s attempt %d unavailable: reason=%s error=%r",
                    api,
                    attempt + 1,
                    e.reason,
                    e.__cause__,
                )
                if e.reason not in _RETRYABLE_REASONS or attempt >= retry.retries:
                    raise
            else:
                UPSTREAM_REQUESTS.labels(api, str(response.status)).inc()
                if attempt >= retry.retries or not _retryable(response.status):
                    return response
            finally:
                UPSTREAM_SECONDS.labels(api).observe(time.monotonic() - started)
            attempt += 1
            UPSTREAM_RETRIES.labels(api).inc()
            delay = retry.base_delay * (2 ** (attempt - 1)) * (0.5 + random.random())
            await make_deferred_yieldable(deferLater(self._reactor, delay, lambda: None))

    async def _once(
        self, method: str, url: str, headers: Mapping[str, str], body: bytes | None, timeout: float
    ) -> UpstreamResponse:
        producer = FileBodyProducer(BytesIO(body)) if body is not None else None  # type: ignore[no-untyped-call]
        deadline = time.monotonic() + timeout
        try:
            # Encoding is inside the try so a header or URL the caller cannot
            # encode is an unavailable upstream, never a raw UnicodeEncodeError.
            twisted_headers = Headers(
                {k.encode("ascii"): [v.encode("latin-1")] for k, v in headers.items()}
            )
            d = self._agent.request(
                method.encode("ascii"),
                url.encode("ascii"),
                twisted_headers,
                # FileBodyProducer's @implementer(IBodyProducer) is invisible to mypy.
                producer,  # type: ignore[arg-type]
            )
            d.addTimeout(timeout, self._reactor)
            response: IResponse = await make_deferred_yieldable(d)
            collector = _Collector(self._max_body)
            response.deliverBody(collector)  # type: ignore[no-untyped-call,call-arg,misc]
            collector.deferred.addTimeout(max(0.001, deadline - time.monotonic()), self._reactor)
            payload = await make_deferred_yieldable(collector.deferred)
        except Exception as e:
            raise UpstreamUnavailable(_classify(e)) from e
        raw = response.headers.getRawHeaders(b"content-type")
        content_type = raw[0].decode("latin-1") if raw else None
        return UpstreamResponse(response.code, content_type, payload)
