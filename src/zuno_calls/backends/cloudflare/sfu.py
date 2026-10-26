"""Cloudflare Calls (Realtime SFU) session and track routes, relayed to Cloudflare.

Only these (method, path) pairs are reachable, and only on a session the caller
opened; opening one in a named call is refused once that call is full or the
caller was removed from it. A session opened in a call is bound to it. Bodies
are relayed as sent, except that track bodies are parsed for the sessions they
pull from, which must be in the puller's call and not belong to someone removed
from it. No retry: SDP and track state changes are not blindly retry-safe.
"""

from __future__ import annotations

import json
import re
import time
from collections import deque
from collections.abc import Callable, Mapping
from typing import Any

from synapse.module_api import ModuleApi, make_deferred_yieldable
from synapse.module_api.errors import Codes, SynapseError
from twisted.internet.task import deferLater

from ...config import CloudflareConfig
from ...metrics import CALL_FULL, REMOVED_REFUSALS
from ...removal import CALL_REMOVED, CALL_REMOVED_DESCRIPTION, Removals, first_read, refusal
from ...resource import SESSION_ID, Call, Place, Reply, Route, json_object, parse_place
from ...upstream import NO_RETRY, Upstream, UpstreamResponse, UpstreamUnavailable

ROUTES: tuple[Route, ...] = (
    ("POST", ("new",)),
    ("GET", (SESSION_ID,)),
    ("POST", (SESSION_ID, "tracks", "new")),
    ("PUT", (SESSION_ID, "renegotiate")),
    ("PUT", (SESSION_ID, "tracks", "close")),
    ("PUT", (SESSION_ID, "tracks", "update")),
)

# Both name remote sessions to pull from; tracks/update can point an existing
# transceiver at another session's track.
_PULLS = (("tracks", "new"), ("tracks", "update"))
NOT_IN_CALL = "IM.ZUNO.TRACK_NOT_IN_CALL"
_DESCRIPTIONS = {CALL_REMOVED: CALL_REMOVED_DESCRIPTION, NOT_IN_CALL: "Track is not in this call"}

# Cloudflare session ids are 32 hex characters; anything else that reaches the
# upstream path could reshape an allowlisted route.
_PLAIN_TOKEN = re.compile(r"^[A-Za-z0-9_-]{1,128}$")

# A user past the cap loses their own oldest session, never anyone else's.
# Sessions are in Synapse's database too, so a restart forgets none: a
# forgotten session would be out of a removal's reach.
_SESSIONS_PER_USER = 32
# Older sessions are long dead; their rows go on the next load.
_MAX_SESSION_AGE_MS = 7 * 24 * 3600 * 1000

# A joiner's m.call.member lands only after its session opens and its tracks
# publish, so each open holds a place in the call this long.
_RESERVATION_SECONDS = 60.0
# Below this many calls a purge is not worth the scan.
_PURGE_FLOOR = 64
# Closing a session's tracks first waits out its requests already in flight,
# which could still add one, checking this often.
_DRAIN_SECONDS = 0.05

_TABLE = """CREATE TABLE IF NOT EXISTS zuno_calls_cloudflare_sessions (
    session_id TEXT NOT NULL PRIMARY KEY,
    user_id TEXT NOT NULL,
    room_id TEXT,
    call_id TEXT,
    opened_at BIGINT NOT NULL
)"""


def _load(txn: Any, oldest: int) -> list[tuple[str, str, str | None, str | None]]:
    txn.execute(_TABLE)
    txn.execute("DELETE FROM zuno_calls_cloudflare_sessions WHERE opened_at < ?", (oldest,))
    txn.execute(
        "SELECT session_id, user_id, room_id, call_id FROM zuno_calls_cloudflare_sessions"
        " ORDER BY opened_at"
    )
    return list(txn.fetchall())


def _keep(
    txn: Any,
    session_id: str,
    user_id: str,
    place: Place | None,
    now: int,
    forgotten: list[str],
) -> None:
    for old in forgotten:
        txn.execute("DELETE FROM zuno_calls_cloudflare_sessions WHERE session_id = ?", (old,))
    room_id, call_id = place or (None, None)
    txn.execute(
        "INSERT INTO zuno_calls_cloudflare_sessions"
        " (session_id, user_id, room_id, call_id, opened_at) VALUES (?, ?, ?, ?, ?)",
        (session_id, user_id, room_id, call_id, now),
    )


def _session_id(body: bytes) -> str | None:
    session_id = (json_object(body) or {}).get("sessionId")
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


def _dumps(content: object) -> bytes:
    return json.dumps(content, separators=(",", ":")).encode()


def _answer(response: UpstreamResponse) -> dict[str, Any] | None:
    """A successful answer's JSON object; None for a failure, even one Cloudflare sent as 2xx."""
    content = json_object(response.body) if 200 <= response.status < 300 else None
    return None if content is None or "errorCode" in content else content


def _refused(track: Mapping[str, Any], errcode: str) -> dict[str, Any]:
    """A pulled track's refusal in Cloudflare's own per-track error shape."""
    named = {k: track[k] for k in ("location", "sessionId", "trackName") if k in track}
    return {**named, "errorCode": errcode, "errorDescription": _DESCRIPTIONS[errcode]}


def _with_refusals(response: UpstreamResponse, refused: list[tuple[int, dict[str, Any]]]) -> Reply:
    """Cloudflare's answer with the refused tracks back at their places in tracks[]."""
    content = _answer(response)
    if content is None or not isinstance(content.get("tracks"), list):
        return Reply.relay(response)
    for index, track in refused:
        content["tracks"].insert(index, track)
    return Reply(response.status, response.content_type, _dumps(content))


class SFUHandler:
    def __init__(
        self,
        cfg: CloudflareConfig,
        upstream: Upstream,
        api: ModuleApi,
        max_participants: int,
        removals: Removals,
        *,
        clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
    ) -> None:
        self._prefix = f"{cfg.base_url}/v1/apps/{cfg.app_id}/sessions"
        self._authorization = f"Bearer {cfg.app_secret}"
        self._upstream = upstream
        self._api = api
        self._max = max_participants
        self._removals = removals
        self._clock = clock
        self._wall_clock = wall_clock
        self._sessions: dict[str, deque[str]] = {}  # user_id -> sessions opened, newest last
        # session_id -> (opener, the call it was opened in, None when it named none)
        self._opened: dict[str, tuple[str, Place | None]] = {}
        self._loaded = False
        self._busy: dict[str, set[object]] = {}  # session_id -> requests in flight to Cloudflare
        # Opens not yet visible as m.call.member: place -> user_id -> monotonic open time.
        self._reservations: dict[Place, dict[str, float]] = {}
        self._purge_at = _PURGE_FLOOR

    async def __call__(self, call: Call) -> Reply:
        for segment in call.segments:
            if not _PLAIN_TOKEN.match(segment):
                raise SynapseError(400, "Invalid session id", Codes.INVALID_PARAM)
        await self._removals.load()
        await self._load()
        if call.segments == ("new",):
            return await self._open(call)
        session_id = call.segments[0]
        # Call participants learn each other's session ids to pull tracks, so
        # knowing an id proves nothing.
        opener, place = self._opened.get(session_id, (None, None))
        if opener != call.user_id:
            raise SynapseError(404, "Unknown session", Codes.NOT_FOUND)
        route = "/".join(call.segments[1:]) or "session"
        if self._removals.has(place, call.user_id):
            raise refusal(route)
        body = call.body
        refused: list[tuple[int, dict[str, Any]]] = []
        if call.segments[1:] in _PULLS:
            content, refused = self._screen(call, place, route)
            if refused and not content["tracks"]:
                answer = {
                    "requiresImmediateRenegotiation": False,
                    "tracks": [t for _, t in refused],
                }
                return Reply(200, "application/json", _dumps(answer))
            # Relayed as parsed, so Cloudflare pulls exactly what was checked.
            body = _dumps(content)
        # No await between the checks above and this count: closing waits for
        # every request counted here, and every later one is refused above.
        request = object()
        self._busy.setdefault(session_id, set()).add(request)
        try:
            response = await self._relay(call, body)
        finally:
            self._busy[session_id].discard(request)
            if not self._busy[session_id]:
                del self._busy[session_id]
        return _with_refusals(response, refused) if refused else Reply.relay(response)

    async def _relay(self, call: Call, body: bytes) -> UpstreamResponse:
        path = "/".join(call.segments)
        if call.method == "GET":
            return await self._request("GET", path)
        return await self._request(call.method, path, body, call.content_type)

    async def _request(
        self, method: str, path: str, body: bytes | None = None, content_type: str | None = None
    ) -> UpstreamResponse:
        headers = {"Authorization": self._authorization}
        if content_type:
            headers["Content-Type"] = content_type
        return await self._upstream.request(
            method, f"{self._prefix}/{path}", headers, body, api="sfu", retry=NO_RETRY
        )

    async def _load(self) -> None:
        """Reads the sessions once per process; every other method needs them read."""
        if self._loaded:
            return
        oldest = int(self._wall_clock() * 1000) - _MAX_SESSION_AGE_MS
        rows = await first_read(self._api, "zuno_calls_load_cloudflare_sessions", _load, oldest)
        if self._loaded:  # a concurrent load got here first; later opens went into its copy
            return
        for session_id, user_id, room_id, call_id in rows:
            self._sessions.setdefault(user_id, deque()).append(session_id)
            place = None if room_id is None or call_id is None else (room_id, call_id)
            self._opened[session_id] = (user_id, place)
        self._loaded = True

    async def _open(self, call: Call) -> Reply:
        place = await self._take_place(call)
        try:
            response = await self._relay(call, call.body)
        except BaseException:
            self._release(place, call.user_id)
            raise
        session_id = _session_id(response.body)
        if not 200 <= response.status < 300:
            self._release(place, call.user_id)
        elif session_id is not None and session_id not in self._opened:
            await self._keep(session_id, call.user_id, place)
        return Reply.relay(response)

    async def _keep(self, session_id: str, user_id: str, place: Place | None) -> None:
        """Remembers a new session; past the cap the user's oldest goes, its tracks closed first."""
        mine = self._sessions.setdefault(user_id, deque())
        forgotten: list[str] = []
        while len(mine) >= _SESSIONS_PER_USER:
            old = mine.popleft()
            opened = self._opened.pop(old)  # its requests are 404 from here
            try:
                await self._idle([old])
                await self._close(old)
            except (SynapseError, UpstreamUnavailable):
                # Kept, over the cap, until a later open closes it.
                mine.appendleft(old)
                self._opened[old] = opened
                break
            forgotten.append(old)
        now = int(self._wall_clock() * 1000)
        await self._api.run_db_interaction(
            "zuno_calls_keep_cloudflare_session", _keep, session_id, user_id, place, now, forgotten
        )
        mine.append(session_id)
        self._opened[session_id] = (user_id, place)

    def _screen(
        self, call: Call, place: Place | None, route: str
    ) -> tuple[dict[str, Any], list[tuple[int, dict[str, Any]]]]:
        """The body without the tracks this caller may not pull, and their refusals by index."""
        content = json_object(call.body)
        if content is None:
            raise SynapseError(400, "Content must be a JSON object", Codes.NOT_JSON)
        tracks = content.get("tracks")
        if not isinstance(tracks, list):
            return content, []
        kept: list[Any] = []
        refused: list[tuple[int, dict[str, Any]]] = []
        for index, track in enumerate(tracks):
            errcode = self._pull_refusal(track, call.user_id, place, route)
            if errcode is None:
                kept.append(track)
                continue
            if errcode == CALL_REMOVED:
                REMOVED_REFUSALS.labels(route, "track").inc()
            refused.append((index, _refused(track, errcode)))
        return {**content, "tracks": kept}, refused

    def _pull_refusal(
        self, track: object, user_id: str, place: Place | None, route: str
    ) -> str | None:
        """Why `user_id`, on a session in `place`, may not pull `track`; None when they may.

        Raises when the puller was removed from the track's call: a pull no
        honest client makes, so the whole request goes.
        """
        if not isinstance(track, dict) or (
            track.get("location") == "local" and "sessionId" not in track
        ):
            return None
        remote = track.get("sessionId")
        opener, remote_place = (
            self._opened.get(remote, (None, None)) if isinstance(remote, str) else (None, None)
        )
        # Temporary, as the call cap: sessions from app releases up to v1.3.0
        # name no call, so a pull with an unbound side skips the binding check.
        if opener is None:
            return NOT_IN_CALL if place else None
        if self._removals.has(remote_place, user_id):
            raise refusal(route)
        if self._removals.has(remote_place or place, opener):
            return CALL_REMOVED
        if place and remote_place and remote_place != place:
            return NOT_IN_CALL
        return None

    async def cut_off(self, place: Place, user_id: str) -> None:
        """Force-closes `user_id`'s tracks in `place`; answers once every one is closed.

        On their sessions bound to the call that is every track; on their
        unbound ones, the tracks pulled from sessions bound to it.
        """
        await self._load()
        calls = {s: self._opened[s][1] for s in self._sessions.get(user_id, ())}
        sessions = [s for s, call_of in calls.items() if call_of in (place, None)]
        try:
            await self._idle(sessions)
            for session_id in sessions:
                await self._close(session_id, None if calls[session_id] == place else place)
        except UpstreamUnavailable:
            raise _unfinished() from None

    async def _idle(self, session_ids: list[str]) -> None:
        """Waits out the requests in flight on these sessions now; later ones may run on.

        A later request on a session bound to the call is refused, so only the
        ones already in flight could still add a track to close.
        """
        waiting = {s: set(self._busy.get(s, ())) for s in session_ids}
        while any(requests & self._busy.get(s, set()) for s, requests in waiting.items()):
            await make_deferred_yieldable(deferLater(_reactor(), _DRAIN_SECONDS, lambda: None))

    async def _close(self, session_id: str, pulled_from: Place | None = None) -> None:
        """Force-closes the session's open tracks, or with `pulled_from` only those it
        pulls from sessions bound to that call; raises 503 unless every one closed."""
        state = await self._request("GET", session_id)
        # A session Cloudflare no longer knows carries no media; 429 is not that.
        if 400 <= state.status < 500 and state.status != 429:
            return
        content = _answer(state)
        tracks = None if content is None else content.get("tracks", [])
        if not isinstance(tracks, list):
            raise _unfinished()
        mids = [
            t["mid"]
            for t in tracks
            if isinstance(t, dict)
            and isinstance(t.get("mid"), str)
            and t["mid"]
            and t.get("status") != "inactive"
            and (pulled_from is None or self._pulls_from(t, pulled_from))
        ]
        if not mids:
            return
        body = _dumps({"tracks": [{"mid": mid} for mid in mids], "force": True})
        closed = await self._request("PUT", f"{session_id}/tracks/close", body, "application/json")
        content = _answer(closed)
        results = None if content is None else content.get("tracks")
        if not isinstance(results, list):
            raise _unfinished()
        # close_track_error is Cloudflare's "absent or already closed".
        done = {
            r.get("mid")
            for r in results
            if isinstance(r, dict) and r.get("errorCode") in (None, "close_track_error")
        }
        if not done.issuperset(mids):
            raise _unfinished()

    def _pulls_from(self, track: Mapping[str, Any], place: Place) -> bool:
        remote = track.get("sessionId")
        return (
            track.get("location") == "remote"
            and isinstance(remote, str)
            and self._opened.get(remote, ("", None))[1] == place
        )

    async def _take_place(self, call: Call) -> Place | None:
        """Reserve the caller a place in the named call, or refuse it as full or removed."""
        # Temporary: app releases up to v1.3.0 name no call and stop themselves
        # at six. Once the oldest supported release names one, a missing
        # room_id/call_id is 400 M_MISSING_PARAM.
        if "room_id" not in call.query and "call_id" not in call.query:
            return None
        place = parse_place(call.query)
        room_id, call_id = place
        state = await self._api.get_room_state(
            room_id, [("m.room.member", call.user_id), ("m.call.member", None)]
        )
        member = state.get(("m.room.member", call.user_id))
        if member is None or member.content.get("membership") != "join":
            raise SynapseError(403, "You are not in this room", Codes.FORBIDDEN)
        now_ms = self._wall_clock() * 1000
        in_call = {
            state_key
            for (event_type, state_key), event in state.items()
            if event_type == "m.call.member" and _live(event.content, call_id, now_ms)
        }
        await self._removals.admit(place, call.user_id, bool(in_call), "sessions/new")

        # No await from here to the reservation: Twisted runs nothing else in
        # between, so two opens racing for the last place cannot both pass.
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


def _unfinished() -> SynapseError:
    # The record stands and its refusals hold; the moderator's retry finishes the close.
    return SynapseError(503, "Could not close the removed user's tracks yet", Codes.UNKNOWN)


def _reactor() -> Any:
    from twisted.internet import reactor

    return reactor
