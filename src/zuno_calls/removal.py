"""Removing someone from a call: who may, the durable record, and the route.

The route is backend-neutral (``calls/remove``): it records the removal, then
has each backend cut the person off; each backend refuses them from the record.
"""

from __future__ import annotations

import math
import re
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any

from synapse.module_api import ModuleApi
from synapse.module_api.errors import Codes, SynapseError

from .metrics import REMOVALS, REMOVED_REFUSALS
from .resource import Call, Place, Reply, Route, json_object, parse_place

ROUTES: tuple[Route, ...] = (("POST", ()),)

CALL_REMOVED = "IM.ZUNO.CALL_REMOVED"
CALL_REMOVED_DESCRIPTION = "Removed from this call"

CutOff = Callable[[Place, str], Awaitable[None]]

# A record goes once a read finds its call over; this bounds the ones never read again.
MAX_AGE_MS = 7 * 24 * 3600 * 1000
_USER_ID = re.compile(r"@[^:\s]+:\S+")
# Room versions outside the numbered line: where the create event's sender is
# the creator (MSC2175, numbered from 11) and where creators outrank every
# power level (MSC4289, numbered from 12).
_IMPLICIT_CREATOR = frozenset({"org.matrix.msc3757.11", "org.matrix.hydra.11"})
_PRIVILEGED_CREATORS = frozenset({"org.matrix.hydra.11"})

_TABLE = """CREATE TABLE IF NOT EXISTS zuno_calls_removals (
    room_id TEXT NOT NULL,
    call_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    removed_by TEXT NOT NULL,
    removed_at BIGINT NOT NULL,
    PRIMARY KEY (room_id, call_id, user_id)
)"""


def refusal(route: str) -> SynapseError:
    """The whole request refused because its sender was removed from the call."""
    REMOVED_REFUSALS.labels(route, "request").inc()
    return SynapseError(403, CALL_REMOVED_DESCRIPTION, CALL_REMOVED)


async def first_read[T](api: ModuleApi, desc: str, func: Callable[..., T], *args: Any) -> T:
    """A table's first read in this process, which also creates it."""
    try:
        return await api.run_db_interaction(desc, func, *args)
    except Exception:
        # The very first reads racing on Postgres: the loser's CREATE TABLE
        # fails once the winner's commits, and the table is there after.
        return await api.run_db_interaction(desc, func, *args)


def _load(txn: Any, oldest: int) -> list[tuple[str, str, str]]:
    txn.execute(_TABLE)
    txn.execute("DELETE FROM zuno_calls_removals WHERE removed_at < ?", (oldest,))
    txn.execute("SELECT room_id, call_id, user_id FROM zuno_calls_removals")
    return list(txn.fetchall())


def _insert(txn: Any, place: Place, user_id: str, removed_by: str, now: int) -> None:
    txn.execute(
        "INSERT INTO zuno_calls_removals (room_id, call_id, user_id, removed_by, removed_at)"
        " VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
        (*place, user_id, removed_by, now),
    )


def _delete(txn: Any, place: Place) -> None:
    txn.execute("DELETE FROM zuno_calls_removals WHERE room_id = ? AND call_id = ?", place)


class Removals:
    """Who was removed from which call, in Synapse's database so a restart keeps them out.

    The table only holds calls still live, so all of it is held in memory too:
    reads are lookups, writes go to the database first.
    """

    def __init__(self, api: ModuleApi, *, wall_clock: Callable[[], float] = time.time) -> None:
        self._api = api
        self._wall_clock = wall_clock
        self._loaded = False
        self._removed: dict[Place, set[str]] = {}

    async def load(self) -> None:
        """Reads the table once per process; every other method needs it read."""
        if self._loaded:
            return
        oldest = int(self._wall_clock() * 1000) - MAX_AGE_MS
        rows = await first_read(self._api, "zuno_calls_load_removals", _load, oldest)
        if self._loaded:  # a concurrent load got here first; later writes went into its copy
            return
        for room_id, call_id, user_id in rows:
            self._removed.setdefault((room_id, call_id), set()).add(user_id)
        self._loaded = True

    def has(self, place: Place | None, user_id: str) -> bool:
        return place is not None and user_id in self._removed.get(place, ())

    async def add(self, place: Place, user_id: str, removed_by: str) -> None:
        await self.load()
        now = int(self._wall_clock() * 1000)
        await self._api.run_db_interaction(
            "zuno_calls_add_removal", _insert, place, user_id, removed_by, now
        )
        self._removed.setdefault(place, set()).add(user_id)

    async def admit(self, place: Place, user_id: str, call_live: bool, route: str) -> None:
        """Refuses a removed user an open in the call; a call none is live in ends its records."""
        if not call_live:
            await self.end(place)
        elif self.has(place, user_id):
            raise refusal(route)

    async def end(self, place: Place) -> None:
        """Forgets a call's records: its last live membership is gone."""
        if self._removed.pop(place, None) is not None:
            await self._api.run_db_interaction("zuno_calls_end_removals", _delete, place)


def _target(body: bytes) -> str:
    content = json_object(body)
    if content is None:
        raise SynapseError(400, "Content must be a JSON object", Codes.NOT_JSON)
    user_id = content.get("user_id")
    if not isinstance(user_id, str):
        raise SynapseError(400, "user_id must be a string", Codes.BAD_JSON)
    if len(user_id) > 255 or not _USER_ID.fullmatch(user_id):
        raise SynapseError(400, "Invalid user_id", Codes.INVALID_PARAM)
    return user_id


def _from(version: str, numbered: int, unstable: frozenset[str]) -> bool:
    return version in unstable or (version.isdigit() and int(version) >= numbered)


def _power(user_id: str, create: Any, levels: Any) -> float:
    """Synapse's get_user_power_level, from the create and power levels events."""
    version = str(create.content.get("room_version", "1"))
    if _from(version, 12, _PRIVILEGED_CREATORS):
        creators = set(create.content.get("additional_creators", []))
        if user_id == create.sender or user_id in creators:
            return math.inf
    if levels is None:
        implicit = _from(version, 11, _IMPLICIT_CREATOR)
        creator = create.sender if implicit else create.content.get("creator")
        return 100 if user_id == creator else 0
    level = levels.content.get("users", {}).get(user_id)
    if level is None:
        level = levels.content.get("users_default", 0)
    return 0 if level is None else int(level)


def may_remove(state: Mapping[tuple[str, str], Any], caller: str, target: str) -> bool:
    """The caller is joined, at the kick level or above, and outranks the target."""
    member = state.get(("m.room.member", caller))
    create = state.get(("m.room.create", ""))
    if member is None or member.content.get("membership") != "join" or create is None:
        return False
    if caller == target:
        return False
    levels = state.get(("m.room.power_levels", ""))
    kick = None if levels is None else levels.content.get("kick")
    mine = _power(caller, create, levels)
    return mine >= (50 if kick is None else int(kick)) and mine > _power(target, create, levels)


class RemoveHandler:
    def __init__(self, api: ModuleApi, removals: Removals, cut_offs: Sequence[CutOff]) -> None:
        self._api = api
        self._removals = removals
        self._cut_offs = tuple(cut_offs)

    async def __call__(self, call: Call) -> Reply:
        place = parse_place(call.query)
        target = _target(call.body)
        state = await self._api.get_room_state(
            place[0],
            [("m.room.create", ""), ("m.room.power_levels", ""), ("m.room.member", call.user_id)],
        )
        if not may_remove(state, call.user_id, target):
            raise SynapseError(403, "You may not remove this user", Codes.FORBIDDEN)
        # Recorded first: from here every backend refuses the target, and a
        # failed cut-off below leaves the record for the retry to finish.
        await self._removals.add(place, target, call.user_id)
        REMOVALS.inc()
        for cut_off in self._cut_offs:
            await cut_off(place, target)
        return Reply(200, "application/json", b"{}")
