"""Cloudflare Calls (Realtime SFU) session and track routes, relayed byte-for-byte.

Only these (method, path) pairs are reachable, and only on a session the caller
opened; bodies are opaque because Cloudflare's track schema keeps growing
optional fields. No retry: SDP and track state changes are not blindly retry-safe.
"""

from __future__ import annotations

import json
import re
from collections import deque

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

# A user past the cap loses their own oldest session, never anyone else's.
# shortcut: kept in this process only; after a restart a live session answers
# 404 and the client opens a new one. Persist them if restarts mid-call matter.
_SESSIONS_PER_USER = 32


def _session_id(body: bytes) -> str | None:
    try:
        session_id = json.loads(body)["sessionId"]
    except (ValueError, TypeError, KeyError):
        return None
    return session_id if isinstance(session_id, str) else None


class SFUHandler:
    def __init__(self, cfg: CloudflareConfig, upstream: Upstream) -> None:
        self._prefix = f"{cfg.base_url}/v1/apps/{cfg.app_id}/sessions"
        self._authorization = f"Bearer {cfg.app_secret}"
        self._upstream = upstream
        self._sessions: dict[str, deque[str]] = {}  # user_id -> sessions opened, newest last

    async def __call__(self, call: Call) -> Reply:
        for segment in call.segments:
            if not _PLAIN_TOKEN.match(segment):
                raise SynapseError(400, "Invalid session id", Codes.INVALID_PARAM)
        opening = call.segments == ("new",)
        # Call participants learn each other's session ids to pull tracks, so
        # knowing an id proves nothing.
        if not opening and call.segments[0] not in self._sessions.get(call.user_id, ()):
            raise SynapseError(404, "Unknown session", Codes.NOT_FOUND)
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
        if opening and 200 <= response.status < 300:
            session_id = _session_id(response.body)
            if session_id is not None:
                mine = self._sessions.setdefault(call.user_id, deque(maxlen=_SESSIONS_PER_USER))
                mine.append(session_id)
        return Reply.relay(response)
