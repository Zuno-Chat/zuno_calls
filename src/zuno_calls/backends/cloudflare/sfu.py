"""Cloudflare Calls (Realtime SFU) session and track routes, relayed byte-for-byte.

Only these (method, path) pairs are reachable, and only on a session the caller
opened; opening one in a named call is refused once that call is full. Bodies are
opaque because Cloudflare's track schema keeps growing optional fields. No retry:
SDP and track state changes are not blindly retry-safe.
"""

from __future__ import annotations

import json
import re
import time
from collections import deque
from collections.abc import Callable, Mapping
from typing import Any

from synapse.module_api import ModuleApi
from synapse.module_api.errors import Codes, SynapseError

from ...config import CloudflareConfig
from ...metrics import CALL_FULL
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

# A joiner's m.call.member lands only after its session opens and its tracks
# publish, so each open holds a place in the call this long.
_RESERVATION_SECONDS = 60.0
# Below this many calls a purge is not worth the scan.
_PURGE_FLOOR = 64

Place = tuple[str, str]  # (room_id, call_id)


def _session_id(body: bytes) -> str | None:
    try:
        session_id = json.loads(body)["sessionId"]
    except (ValueError, TypeError, KeyError):
        return None
    return session_id if isinstance(session_id, str) else None


def _live(content: Mapping[str, Any], call_id: str, now_ms: float) -> bool:
    memberships = content.get("memberships")
    # Synapse may hand content over frozen, lists as tuples.
    if not isinstance(memberships, (list, tuple)):
        return False
    return any(
        isinstance(m, Mapping)
        and m.get("call_id") == call_id
        and isinstance(m.get("expires_ts"), int)
        and m["expires_ts"] > now_ms
        for m in memberships
    )


class SFUHandler:
    def __init__(
        self,
        cfg: CloudflareConfig,
        upstream: Upstream,
        api: ModuleApi,
        max_participants: int,
        *,
        clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
    ) -> None:
        self._prefix = f"{cfg.base_url}/v1/apps/{cfg.app_id}/sessions"
        self._authorization = f"Bearer {cfg.app_secret}"
        self._upstream = upstream
        self._api = api
        self._max = max_participants
        self._clock = clock
        self._wall_clock = wall_clock
        self._sessions: dict[str, deque[str]] = {}  # user_id -> sessions opened, newest last
        # Opens not yet visible as m.call.member: place -> user_id -> monotonic open time.
        self._reservations: dict[Place, dict[str, float]] = {}
        self._purge_at = _PURGE_FLOOR

    async def __call__(self, call: Call) -> Reply:
        for segment in call.segments:
            if not _PLAIN_TOKEN.match(segment):
                raise SynapseError(400, "Invalid session id", Codes.INVALID_PARAM)
        opening = call.segments == ("new",)
        # Call participants learn each other's session ids to pull tracks, so
        # knowing an id proves nothing.
        if not opening and call.segments[0] not in self._sessions.get(call.user_id, ()):
            raise SynapseError(404, "Unknown session", Codes.NOT_FOUND)
        place = await self._take_place(call) if opening else None
        headers = {"Authorization": self._authorization}
        body: bytes | None = None
        if call.method != "GET":
            body = call.body
            if call.content_type:
                headers["Content-Type"] = call.content_type
        try:
            response = await self._upstream.request(
                call.method,
                self._prefix + "/" + "/".join(call.segments),
                headers,
                body,
                api="sfu",
                retry=NO_RETRY,
            )
        except BaseException:
            self._release(place, call.user_id)
            raise
        if not 200 <= response.status < 300:
            self._release(place, call.user_id)
        elif opening:
            session_id = _session_id(response.body)
            if session_id is not None:
                mine = self._sessions.setdefault(call.user_id, deque(maxlen=_SESSIONS_PER_USER))
                mine.append(session_id)
        return Reply.relay(response)

    async def _take_place(self, call: Call) -> Place | None:
        """Reserve the caller a place in the named call, or refuse it as full."""
        # Temporary: app releases up to v1.3.0 name no call and stop themselves
        # at six. Once the oldest supported release names one, a missing
        # room_id/call_id is 400 M_MISSING_PARAM.
        if "room_id" not in call.query and "call_id" not in call.query:
            return None
        room_id = call.query.get("room_id", "")
        call_id = call.query.get("call_id", "")
        if not (room_id.startswith("!") and len(room_id) <= 255 and 0 < len(call_id) <= 255):
            raise SynapseError(400, "Invalid room_id or call_id", Codes.INVALID_PARAM)
        state = await self._api.get_room_state(
            room_id, [("m.room.member", call.user_id), ("m.call.member", None)]
        )
        member = state.get(("m.room.member", call.user_id))
        if member is None or member.content.get("membership") != "join":
            raise SynapseError(403, "You are not in this room", Codes.FORBIDDEN)

        # No await from here to the reservation: Twisted runs nothing else in
        # between, so two opens racing for the last place cannot both pass.
        now_ms = self._wall_clock() * 1000
        in_call = {
            state_key
            for (event_type, state_key), event in state.items()
            if event_type == "m.call.member" and _live(event.content, call_id, now_ms)
        }
        place = (room_id, call_id)
        now = self._clock()
        held = {
            user: opened
            for user, opened in self._reservations.get(place, {}).items()
            if now - opened < _RESERVATION_SECONDS
        }
        # The caller never counts against themselves, so a rejoin always passes.
        if len((in_call | held.keys()) - {call.user_id}) >= self._max:
            CALL_FULL.inc()
            raise SynapseError(
                403,
                "This call is full",
                "IM.ZUNO.CALL_FULL",
                additional_fields={"max_participants": self._max},
            )
        held[call.user_id] = now
        self._reservations[place] = held
        self._maybe_purge(now)
        return place

    def _release(self, place: Place | None, user_id: str) -> None:
        if place is None or place not in self._reservations:
            return
        held = self._reservations[place]
        held.pop(user_id, None)
        if not held:
            del self._reservations[place]

    def _maybe_purge(self, now: float) -> None:
        # As RateLimiter: once the map has doubled, drop calls whose every
        # reservation has expired.
        if len(self._reservations) < self._purge_at:
            return
        for place in [
            p
            for p, held in self._reservations.items()
            if all(now - opened >= _RESERVATION_SECONDS for opened in held.values())
        ]:
            del self._reservations[place]
        self._purge_at = max(_PURGE_FLOOR, 2 * len(self._reservations))
