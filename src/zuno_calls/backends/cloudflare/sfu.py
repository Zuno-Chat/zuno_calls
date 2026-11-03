"""Cloudflare Calls (Realtime SFU) session, track and data channel routes, relayed to Cloudflare.

Only these (method, path) pairs are reachable, and only on a session the caller
opened; opening one in a named call is refused once that call is full or the
caller was removed from it. A session opened in a call is bound to it. Bodies
are relayed as sent, except that track and data channel bodies are parsed for
the sessions they pull from, which must be in the puller's call and not belong
to someone removed from it. No retry: SDP and track state changes are not
blindly retry-safe.
"""

from __future__ import annotations

import json
import logging
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
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
    ("POST", (SESSION_ID, "datachannels", "establish")),
    ("POST", (SESSION_ID, "datachannels", "new")),
    ("PUT", (SESSION_ID, "datachannels", "close")),
)


@dataclass(frozen=True)
class _Resource:
    entries: str
    id_key: str
    metric_scope: str
    close_route: str
    close_flags: Mapping[str, Any]
    already_closed: tuple[str, ...]


@dataclass(frozen=True)
class _Session:
    user_id: str
    place: Place | None
    opened_at: int


_TRACKS = _Resource(
    entries="tracks",
    id_key="mid",
    metric_scope="track",
    close_route="tracks/close",
    close_flags={"force": True},
    # close_track_error is Cloudflare's "absent or already closed".
    already_closed=("close_track_error",),
)
_DATA_CHANNELS = _Resource(
    entries="dataChannels",
    id_key="id",
    metric_scope="channel",
    close_route="datachannels/close",
    close_flags={},
    already_closed=(),
)
# Each names remote sessions to pull from; tracks/update can point an existing
# transceiver at another session's track.
_PULLS: dict[tuple[str, ...], _Resource] = {
    ("tracks", "new"): _TRACKS,
    ("tracks", "update"): _TRACKS,
    ("datachannels", "new"): _DATA_CHANNELS,
}
NOT_IN_CALL = "IM.ZUNO.TRACK_NOT_IN_CALL"
SWEEP_MS = 30_000
_DESCRIPTIONS = {CALL_REMOVED: CALL_REMOVED_DESCRIPTION, NOT_IN_CALL: "Not in this call"}

# Cloudflare session ids are 32 hex characters; anything else that reaches the
# upstream path could reshape an allowlisted route.
_PLAIN_TOKEN = re.compile(r"^[A-Za-z0-9_-]{1,128}$")

# A user past the cap loses their own oldest session, never anyone else's.
# Sessions are in Synapse's database too, so a restart forgets none: a
# forgotten session would be out of a removal's reach.
_SESSIONS_PER_USER = 32
# Older sessions are long dead.
_MAX_SESSION_AGE_MS = 7 * 24 * 3600 * 1000

# A joiner's m.call.member lands only after its session opens and its tracks
# publish, so each open holds a place in the call this long.
_RESERVATION_SECONDS = 60.0
# Below this many calls a purge is not worth the scan.
_PURGE_FLOOR = 64
# Closing a session's tracks first waits out its requests already in flight,
# which could still add one, checking this often.
_DRAIN_SECONDS = 0.05
_RETRY_SECONDS = 60.0
_MAX_RETRY_SECONDS = 3600.0

_logger = logging.getLogger(__name__)

_TABLE = """CREATE TABLE IF NOT EXISTS zuno_calls_cloudflare_sessions (
    session_id TEXT NOT NULL PRIMARY KEY,
    user_id TEXT NOT NULL,
    room_id TEXT,
    call_id TEXT,
    opened_at BIGINT NOT NULL
)"""


def _load(txn: Any, oldest: int) -> list[tuple[str, str, str | None, str | None, int]]:
    txn.execute(_TABLE)
    txn.execute("DELETE FROM zuno_calls_cloudflare_sessions WHERE opened_at < ?", (oldest,))
    txn.execute(
        "SELECT session_id, user_id, room_id, call_id, opened_at"
        " FROM zuno_calls_cloudflare_sessions ORDER BY opened_at"
    )
    return list(txn.fetchall())


def _keep(txn: Any, session_id: str, user_id: str, place: Place | None, now: int) -> None:
    room_id, call_id = place or (None, None)
    txn.execute(
        "INSERT INTO zuno_calls_cloudflare_sessions"
        " (session_id, user_id, room_id, call_id, opened_at) VALUES (?, ?, ?, ?, ?)",
        (session_id, user_id, room_id, call_id, now),
    )


def _forget(txn: Any, session_ids: list[str]) -> None:
    txn.executemany(
        "DELETE FROM zuno_calls_cloudflare_sessions WHERE session_id = ?",
        [(s,) for s in session_ids],
    )


def _session_id(body: bytes) -> str | None:
    session_id = (json_object(body) or {}).get("sessionId")
    return session_id if isinstance(session_id, str) else None


def _entries(content: Mapping[str, Any], call_id: str) -> list[Mapping[str, Any]]:
    memberships = content.get("memberships")
    # Synapse may hand content over frozen, lists as tuples.
    if not isinstance(memberships, (list, tuple)):
        return []
    return [m for m in memberships if isinstance(m, Mapping) and m.get("call_id") == call_id]


def _live(content: Mapping[str, Any], call_id: str, now_ms: float) -> bool:
    return any(
        isinstance(m.get("expires_ts"), int) and m["expires_ts"] > now_ms
        for m in _entries(content, call_id)
    )


def _dumps(content: object) -> bytes:
    return json.dumps(content, separators=(",", ":")).encode()


def _answer(response: UpstreamResponse) -> dict[str, Any] | None:
    """A successful answer's JSON object; None for a failure, even one Cloudflare sent as 2xx."""
    content = json_object(response.body) if 200 <= response.status < 300 else None
    return None if content is None or "errorCode" in content else content


def _resource_id(value: object) -> bool:
    return (isinstance(value, str) and value != "") or (
        isinstance(value, int) and not isinstance(value, bool)
    )


def _refused(entry: Mapping[str, Any], errcode: str) -> dict[str, Any]:
    """A pulled entry's refusal in Cloudflare's own per-entry error shape."""
    names = ("location", "sessionId", "trackName", "dataChannelName")
    named = {k: entry[k] for k in names if k in entry}
    return {**named, "errorCode": errcode, "errorDescription": _DESCRIPTIONS[errcode]}


def _with_refusals(
    response: UpstreamResponse, pull: _Resource, refused: list[tuple[int, dict[str, Any]]]
) -> Reply:
    """Cloudflare's answer with the refused entries back at their places in its list."""
    content = _answer(response)
    if content is None or not isinstance(content.get(pull.entries), list):
        return Reply.relay(response)
    for index, entry in refused:
        content[pull.entries].insert(index, entry)
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
        self._sessions: dict[str, list[str]] = {}  # user_id -> sessions opened, newest last
        self._opened: dict[str, _Session] = {}
        self._loaded = False
        self._closers: set[str] = set()
        self._closing: set[str] = set()
        self._retry_at: dict[str, tuple[int, float]] = {}
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
        session = self._opened.get(session_id)
        if session is None or session.user_id != call.user_id or session_id in self._closing:
            raise SynapseError(404, "Unknown session", Codes.NOT_FOUND)
        place = session.place
        route = "/".join(call.segments[1:]) or "session"
        if self._removals.has(place, call.user_id):
            raise refusal(route)
        body = call.body
        refused: list[tuple[int, dict[str, Any]]] = []
        pull = _PULLS.get(call.segments[1:])
        if pull is not None:
            content, refused = self._screen(call, place, route, pull)
            if refused and not content[pull.entries]:
                answer = {pull.entries: [e for _, e in refused]}
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
        if pull is None or not refused:
            return Reply.relay(response)
        return _with_refusals(response, pull, refused)

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
        for session_id, user_id, room_id, call_id, opened_at in rows:
            self._sessions.setdefault(user_id, []).append(session_id)
            place = None if room_id is None or call_id is None else (room_id, call_id)
            self._opened[session_id] = _Session(user_id, place, opened_at)
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
            self._api.run_as_background_process(
                "zuno_calls_close_cloudflare_sessions", self._close_due, call.user_id
            )
        return Reply.relay(response)

    async def _keep(self, session_id: str, user_id: str, place: Place | None) -> None:
        now = int(self._wall_clock() * 1000)
        await self._api.run_db_interaction(
            "zuno_calls_keep_cloudflare_session", _keep, session_id, user_id, place, now
        )
        self._sessions.setdefault(user_id, []).append(session_id)
        self._opened[session_id] = _Session(user_id, place, now)

    async def sweep(self) -> None:
        if not self._loaded:
            return
        for user_id in list(self._sessions):
            self._api.run_as_background_process(
                "zuno_calls_close_cloudflare_sessions", self._close_due, user_id
            )

    async def _close_due(self, user_id: str) -> None:
        if user_id in self._closers:
            return
        self._closers.add(user_id)
        try:
            while True:
                expired, due = await self._due(user_id)
                if not expired and not due:
                    return
                if expired:
                    await self._drop(expired)
                for session_id in due:
                    await self._retire(session_id)
        finally:
            self._closers.discard(user_id)

    async def _due(self, user_id: str) -> tuple[list[str], list[str]]:
        over = await self._calls_over(user_id)
        mine = self._sessions.get(user_id, [])
        oldest = int(self._wall_clock() * 1000) - _MAX_SESSION_AGE_MS
        expired = [s for s in mine if self._opened[s].opened_at < oldest]
        kept = [s for s in mine if s not in over and s not in expired]
        excess = set(kept[: max(0, len(kept) - _SESSIONS_PER_USER)])
        now = self._clock()
        due = [
            s
            for s in mine
            if (s in over or s in excess)
            and s not in expired
            and self._retry_at.get(s, (0, now))[1] <= now
        ]
        return expired, due

    async def _calls_over(self, user_id: str) -> set[str]:
        now_ms = self._wall_clock() * 1000
        rooms: dict[str, list[tuple[str, str]]] = {}
        for session_id in self._sessions.get(user_id, []):
            session = self._opened[session_id]
            if session.place and now_ms - session.opened_at >= _RESERVATION_SECONDS * 1000:
                room_id, call_id = session.place
                rooms.setdefault(room_id, []).append((session_id, call_id))
        over: set[str] = set()
        for room_id, sessions in rooms.items():
            state = await self._api.get_room_state(room_id, [("m.call.member", user_id)])
            member = state.get(("m.call.member", user_id))
            content = {} if member is None else member.content
            over.update(s for s, call_id in sessions if not _entries(content, call_id))
        return over

    async def _retire(self, session_id: str) -> None:
        self._closing.add(session_id)
        try:
            await self._idle([session_id])
            await self._close(session_id)
        except (SynapseError, UpstreamUnavailable):
            self._back_off(session_id)
            return
        finally:
            self._closing.discard(session_id)
        await self._drop([session_id])

    def _back_off(self, session_id: str) -> None:
        failures = self._retry_at.get(session_id, (0, 0.0))[0] + 1
        delay = min(_RETRY_SECONDS * 2 ** (failures - 1), _MAX_RETRY_SECONDS)
        self._retry_at[session_id] = (failures, self._clock() + delay)
        _logger.warning("Cloudflare session %s did not close; retrying in %ds", session_id, delay)

    async def _drop(self, session_ids: list[str]) -> None:
        for session_id in session_ids:
            session = self._opened.pop(session_id)
            self._retry_at.pop(session_id, None)
            mine = self._sessions[session.user_id]
            mine.remove(session_id)
            if not mine:
                del self._sessions[session.user_id]
        await self._api.run_db_interaction(
            "zuno_calls_forget_cloudflare_sessions", _forget, session_ids
        )

    def _screen(
        self, call: Call, place: Place | None, route: str, pull: _Resource
    ) -> tuple[dict[str, Any], list[tuple[int, dict[str, Any]]]]:
        """The body without the entries this caller may not pull, and their refusals by index."""
        content = json_object(call.body)
        if content is None:
            raise SynapseError(400, "Content must be a JSON object", Codes.NOT_JSON)
        entries = content.get(pull.entries)
        if not isinstance(entries, list):
            return content, []
        kept: list[Any] = []
        refused: list[tuple[int, dict[str, Any]]] = []
        for index, entry in enumerate(entries):
            errcode = self._pull_refusal(entry, call.user_id, place, route)
            if errcode is None:
                kept.append(entry)
                continue
            if errcode == CALL_REMOVED:
                REMOVED_REFUSALS.labels(route, pull.metric_scope).inc()
            refused.append((index, _refused(entry, errcode)))
        return {**content, pull.entries: kept}, refused

    def _pull_refusal(
        self, entry: object, user_id: str, place: Place | None, route: str
    ) -> str | None:
        """Why `user_id`, on a session in `place`, may not pull `entry`; None when they may.

        Raises when the puller was removed from the entry's call: a pull no
        honest client makes, so the whole request goes.
        """
        if not isinstance(entry, dict) or (
            entry.get("location") == "local" and "sessionId" not in entry
        ):
            return None
        remote = entry.get("sessionId")
        pulled = self._opened.get(remote) if isinstance(remote, str) else None
        # Temporary, as the call cap: sessions from app releases up to v1.3.0
        # name no call, so a pull with an unbound side skips the binding check.
        if pulled is None:
            return NOT_IN_CALL if place else None
        opener, remote_place = pulled.user_id, pulled.place
        if self._removals.has(remote_place, user_id):
            raise refusal(route)
        if self._removals.has(remote_place or place, opener):
            return CALL_REMOVED
        if place and remote_place and remote_place != place:
            return NOT_IN_CALL
        return None

    async def cut_off(self, place: Place, user_id: str) -> None:
        """Force-closes `user_id`'s tracks and data channels in `place`; answers once every
        one is closed.

        On their sessions bound to the call that is every one; on their unbound
        ones, those pulled from sessions bound to it.
        """
        await self._load()
        calls = {s: self._opened[s].place for s in self._sessions.get(user_id, ())}
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
        ones already in flight could still add a track or data channel to close.
        """
        waiting = {s: set(self._busy.get(s, ())) for s in session_ids}
        while any(requests & self._busy.get(s, set()) for s, requests in waiting.items()):
            await make_deferred_yieldable(deferLater(_reactor(), _DRAIN_SECONDS, lambda: None))

    async def _close(self, session_id: str, pulled_from: Place | None = None) -> None:
        """Force-closes the session's open tracks and data channels, or with `pulled_from`
        only those it pulls from sessions bound to that call; raises 503 unless every one
        closed."""
        state = await self._request("GET", session_id)
        # A session Cloudflare no longer knows carries no media; 429 is not that.
        if 400 <= state.status < 500 and state.status != 429:
            return
        content = _answer(state)
        if content is None:
            raise _unfinished()
        for resource in (_TRACKS, _DATA_CHANNELS):
            await self._close_open(session_id, content, resource, pulled_from)

    async def _close_open(
        self,
        session_id: str,
        state: Mapping[str, Any],
        resource: _Resource,
        pulled_from: Place | None,
    ) -> None:
        listed = state.get(resource.entries, [])
        if not isinstance(listed, list):
            raise _unfinished()
        ids = [
            e[resource.id_key]
            for e in listed
            if isinstance(e, dict)
            and _resource_id(e.get(resource.id_key))
            and e.get("status") != "inactive"
            and (pulled_from is None or self._pulls_from(e, pulled_from))
        ]
        if not ids:
            return
        close = {resource.entries: [{resource.id_key: i} for i in ids], **resource.close_flags}
        closed = await self._request(
            "PUT", f"{session_id}/{resource.close_route}", _dumps(close), "application/json"
        )
        content = _answer(closed)
        results = None if content is None else content.get(resource.entries)
        if not isinstance(results, list):
            raise _unfinished()
        done = {
            r.get(resource.id_key)
            for r in results
            if isinstance(r, dict) and r.get("errorCode") in (None, *resource.already_closed)
        }
        if not done.issuperset(ids):
            raise _unfinished()

    def _pulls_from(self, entry: Mapping[str, Any], place: Place) -> bool:
        remote = entry.get("sessionId")
        pulled = self._opened.get(remote) if isinstance(remote, str) else None
        return entry.get("location") == "remote" and pulled is not None and pulled.place == place

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
    return SynapseError(503, "Could not cut the removed user off yet", Codes.UNKNOWN)


def _reactor() -> Any:
    from twisted.internet import reactor

    return reactor
