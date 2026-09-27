"""Cloudflare Calls (Realtime SFU) session and track routes, relayed byte-for-byte.

Only these (method, path) pairs are reachable; bodies are opaque because
Cloudflare's track schema keeps growing optional fields. No retry: SDP and
track state changes are not blindly retry-safe.
"""

from __future__ import annotations

import re

from synapse.module_api.errors import Codes, SynapseError

from ...config import CloudflareConfig
from ...resource import SESSION_ID, Call, Reply, Route
from ...upstream import NO_RETRY, Upstream

ROUTES: tuple[Route, ...] = (
    ("POST", ("new",)),
    ("GET", (SESSION_ID,)),
    ("POST", (SESSION_ID, "tracks", "new")),
    ("PUT", (SESSION_ID, "renegotiate")),
    ("PUT", (SESSION_ID, "tracks", "close")),
    ("PUT", (SESSION_ID, "tracks", "update")),
)

# Cloudflare session ids are 32 hex characters; anything else that reaches the
# upstream path could reshape an allowlisted route.
_PLAIN_TOKEN = re.compile(r"^[A-Za-z0-9_-]{1,128}$")


class SFUHandler:
    def __init__(self, cfg: CloudflareConfig, upstream: Upstream) -> None:
        self._prefix = f"{cfg.base_url}/v1/apps/{cfg.app_id}/sessions"
        self._authorization = f"Bearer {cfg.app_secret}"
        self._upstream = upstream

    async def __call__(self, call: Call) -> Reply:
        for segment in call.segments:
            if not _PLAIN_TOKEN.match(segment):
                raise SynapseError(400, "Invalid session id", Codes.INVALID_PARAM)
        headers = {"Authorization": self._authorization}
        body: bytes | None = None
        if call.method != "GET":
            body = call.body
            if call.content_type:
                headers["Content-Type"] = call.content_type
        response = await self._upstream.request(
            call.method,
            self._prefix + "/" + "/".join(call.segments),
            headers,
            body,
            api="sfu",
            retry=NO_RETRY,
        )
        return Reply.relay(response)
