"""Shared request glue: route → auth → rate limit → handler → relay.

Errors are raised as SynapseError; DirectServeJsonResource renders them as
Matrix JSON and runs the handler under Synapse's logging-context rules.
"""

from __future__ import annotations

import math
import re
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any

from synapse.module_api import DirectServeJsonResource, ModuleApi
from synapse.module_api.errors import Codes, SynapseError

from .metrics import RATE_LIMITED
from .ratelimit import RateLimiter
from .upstream import UpstreamResponse, UpstreamUnavailable

MAX_REQUEST_BODY = 256 * 1024
SESSION_ID = "{sessionId}"

# A header value Twisted decoded as UTF-8 still has to survive the latin-1
# re-encode on the way out; anything richer is dropped rather than relayed.
_HEADER_VALUE = re.compile(r"[\x21-\x7e][\x20-\x7e]*")

Route = tuple[str, tuple[str, ...]]


@dataclass(frozen=True)
class Call:
    method: str
    segments: tuple[str, ...]  # path segments after the resource prefix, percent-decoded
    content_type: str | None
    body: bytes
    user_id: str
    device_id: str | None


@dataclass(frozen=True)
class Reply:
    status: int
    content_type: str | None
    body: bytes

    @classmethod
    def relay(cls, response: UpstreamResponse) -> Reply:
        """The upstream's status, Content-Type and body, byte-for-byte."""
        return cls(response.status, response.content_type, response.body)


Handler = Callable[[Call], Awaitable[Reply]]


def reserved_words(routes: Sequence[Route]) -> frozenset[str]:
    """Literal segments used anywhere in the table (e.g. "new").

    A {sessionId} wildcard never swallows one, even for another method or
    route, so a session can never collide with a fixed sub-path.
    """
    return frozenset(seg for _, pattern in routes for seg in pattern if seg != SESSION_ID)


def _wildcard(segment: str, reserved: frozenset[str]) -> bool:
    return bool(segment) and "/" not in segment and segment not in reserved


def match(
    routes: Sequence[Route],
    method: str,
    segments: tuple[str, ...],
    reserved: frozenset[str] | None = None,
) -> bool:
    if reserved is None:
        reserved = reserved_words(routes)
    for route_method, pattern in routes:
        if route_method != method or len(pattern) != len(segments):
            continue
        if all(
            s == p or (p == SESSION_ID and _wildcard(s, reserved))
            for p, s in zip(pattern, segments, strict=True)
        ):
            return True
    return False


class ZunoResource(DirectServeJsonResource):
    """A leaf: every path below its prefix arrives here with the rest in request.postpath."""

    isLeaf = True

    def __init__(
        self,
        api: ModuleApi,
        limiter: RateLimiter,
        handler: Handler,
        *,
        routes: Sequence[Route],
        read_body: bool = True,
        max_body: int = MAX_REQUEST_BODY,
    ) -> None:
        super().__init__()
        self._api = api
        self._limiter = limiter
        self._handler = handler
        self._routes = tuple(routes)
        self._reserved = reserved_words(self._routes)
        self._read_body = read_body
        self._max_body = max_body

    async def _async_render(self, request: Any) -> None:
        # Override the base dispatcher outright (rather than defining
        # _async_render_GET/POST/PUT) so every unsupported verb reaches
        # _serve and gets the same 404 M_UNRECOGNIZED as an unmatched path,
        # instead of the base class's 405 for a method with no handler.
        await self._serve(request)

    async def _serve(self, request: Any) -> None:
        method = request.method.decode("ascii")
        segments = tuple(seg.decode("utf-8", "replace") for seg in request.postpath)
        if not match(self._routes, method, segments, self._reserved):
            raise SynapseError(404, "Unrecognized request", Codes.UNRECOGNIZED)

        requester = await self._api.get_user_by_req(request)
        user_id: str = requester.user.to_string()
        device_id: str | None = requester.device_id
        wait_ms = self._limiter.allow((user_id, device_id))
        if wait_ms is not None:
            RATE_LIMITED.inc()
            raise SynapseError(
                429,
                "Too many requests",
                Codes.LIMIT_EXCEEDED,
                additional_fields={"retry_after_ms": wait_ms},
                headers={"Retry-After": str(math.ceil(wait_ms / 1000))},
            )

        body = b""
        if self._read_body:
            request.content.seek(0)
            body = request.content.read(self._max_body + 1)
            if len(body) > self._max_body:
                raise SynapseError(413, "Request body too large", Codes.TOO_LARGE)

        content_type = request.getHeader("Content-Type")
        if content_type is not None and not _HEADER_VALUE.fullmatch(content_type):
            content_type = None
        call = Call(method, segments, content_type, body, user_id, device_id)
        try:
            reply = await self._handler(call)
        except UpstreamUnavailable:
            raise SynapseError(502, "calls service unavailable", Codes.UNKNOWN) from None

        if request._disconnected:
            return
        request.setResponseCode(reply.status)
        if reply.content_type:
            request.setHeader(b"Content-Type", reply.content_type.encode("latin-1"))
        # 204, 304 and 1xx carry neither a body nor a Content-Length.
        if reply.status >= 200 and reply.status not in (204, 304):
            request.setHeader(b"Content-Length", str(len(reply.body)).encode("ascii"))
            request.write(reply.body)
        request.finish()
