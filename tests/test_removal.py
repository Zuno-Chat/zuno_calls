import json
import sqlite3

import pytest
import pytest_twisted
from conftest import FakeEvent, FakeModuleApi, as_api, expect_error, sample
from synapse.module_api.errors import Codes

from zuno_calls.removal import Removals, RemoveHandler
from zuno_calls.resource import Call

ROOM = "!room:zuno.chat"
PLACE = (ROOM, "c1")
QUERY = {"room_id": ROOM, "call_id": "c1"}
CREATOR = "@creator:zuno.chat"
ADMIN = "@admin:zuno.chat"
MOD = "@mod:zuno.chat"
MOD_2 = "@mod2:zuno.chat"
MEMBER = "@member:zuno.chat"
NOW_MS = 1_800_000_000_000
DAY = 86400.0


def room(version="10", kick=None, power_levels=True):
    """ROOM with everyone joined; the creator holds 100 unless room v12 makes them a creator."""
    users = {ADMIN: 100, MOD: 50, MOD_2: 50}
    if version != "12":
        users[CREATOR] = 100
    levels = {"users": users} if kick is None else {"users": users, "kick": kick}
    create = {"room_version": version} if version in ("11", "12") else {"creator": CREATOR}
    state = {("m.room.create", ""): FakeEvent(create, sender=CREATOR)}
    if power_levels:
        state[("m.room.power_levels", "")] = FakeEvent(levels)
    for user in (CREATOR, ADMIN, MOD, MOD_2, MEMBER):
        state[("m.room.member", user)] = FakeEvent({"membership": "join"})
    api = FakeModuleApi()
    api.rooms[ROOM] = state
    return api


def make(api, wall_clock=lambda: NOW_MS / 1000):
    removals = Removals(as_api(api), wall_clock=wall_clock)
    cut: list[tuple[tuple[str, str], str]] = []

    async def cut_off(place, user_id):
        cut.append((place, user_id))

    return RemoveHandler(as_api(api), removals, [cut_off]), removals, cut


def remove(caller, target, query=QUERY, body=None):
    if body is None:
        body = json.dumps({"user_id": target}).encode()
    return Call("POST", (), "application/json", body, caller, "DEV1", query)


def rows(api):
    try:
        return api.db.execute(
            "SELECT room_id, call_id, user_id, removed_by FROM zuno_calls_removals"
        ).fetchall()
    except sqlite3.OperationalError:  # never loaded: no table
        return []


@pytest_twisted.ensureDeferred
async def test_a_moderator_removes_a_member():
    api = room()
    handler, removals, cut = make(api)
    before = sample("zuno_calls_removals_total")
    reply = await handler(remove(MOD, MEMBER))
    assert (reply.status, reply.content_type, reply.body) == (200, "application/json", b"{}")
    assert rows(api) == [(ROOM, "c1", MEMBER, MOD)]
    assert removals.has(PLACE, MEMBER)
    assert cut == [(PLACE, MEMBER)]
    assert sample("zuno_calls_removals_total") - before == 1


@pytest.mark.parametrize(
    "caller, target, version",
    [
        (ADMIN, MOD, "10"),
        (CREATOR, ADMIN, "12"),
        (CREATOR, MEMBER, "11"),
    ],
)
@pytest_twisted.ensureDeferred
async def test_a_higher_level_at_kick_or_above_may_remove(caller, target, version):
    handler, removals, _ = make(room(version))
    assert (await handler(remove(caller, target))).status == 200
    assert removals.has(PLACE, target)


@pytest.mark.parametrize(
    "caller, target, version, kick",
    [
        (MOD, MOD_2, "10", None),
        (MOD, ADMIN, "10", None),
        (MOD, MOD, "10", None),
        (MEMBER, MOD, "10", None),
        (MEMBER, "@other:zuno.chat", "10", None),
        (MOD, MEMBER, "10", 100),
        (ADMIN, CREATOR, "12", None),
    ],
)
@pytest_twisted.ensureDeferred
async def test_anyone_else_is_forbidden(caller, target, version, kick):
    api = room(version, kick)
    handler, removals, cut = make(api)
    await expect_error(handler, remove(caller, target), 403, Codes.FORBIDDEN)
    assert (removals.has(PLACE, target), rows(api), cut) == (False, [], [])


@pytest.mark.parametrize("membership", ["leave", "invite", "ban", None])
@pytest_twisted.ensureDeferred
async def test_a_caller_not_joined_is_forbidden(membership):
    api = room()
    if membership is None:
        del api.rooms[ROOM][("m.room.member", ADMIN)]
    else:
        api.rooms[ROOM][("m.room.member", ADMIN)] = FakeEvent({"membership": membership})
    handler, _, cut = make(api)
    await expect_error(handler, remove(ADMIN, MEMBER), 403, Codes.FORBIDDEN)
    assert cut == []


@pytest_twisted.ensureDeferred
async def test_an_unknown_room_is_forbidden():
    handler, _, _ = make(FakeModuleApi())
    await expect_error(handler, remove(ADMIN, MEMBER), 403, Codes.FORBIDDEN)


@pytest.mark.parametrize("version", ["10", "11"])
@pytest_twisted.ensureDeferred
async def test_without_power_levels_only_the_creator_may_remove(version):
    handler, _, _ = make(room(version, power_levels=False))
    await expect_error(handler, remove(ADMIN, MEMBER), 403, Codes.FORBIDDEN)
    assert (await handler(remove(CREATOR, MEMBER))).status == 200


@pytest_twisted.ensureDeferred
async def test_the_same_removal_twice_is_one_record():
    api = room()
    handler, _, cut = make(api)
    await handler(remove(MOD, MEMBER))
    assert (await handler(remove(ADMIN, MEMBER))).status == 200
    assert rows(api) == [(ROOM, "c1", MEMBER, MOD)]
    assert cut == [(PLACE, MEMBER)] * 2


@pytest.mark.parametrize(
    "query, body, errcode",
    [
        ({"room_id": "room:zuno.chat", "call_id": "c1"}, None, Codes.INVALID_PARAM),
        ({"room_id": ROOM, "call_id": ""}, None, Codes.INVALID_PARAM),
        ({"room_id": ROOM}, None, Codes.INVALID_PARAM),
        ({}, None, Codes.INVALID_PARAM),
        ({"room_id": "!" + "r" * 255, "call_id": "c1"}, None, Codes.INVALID_PARAM),
        ({"room_id": ROOM, "call_id": "c" * 256}, None, Codes.INVALID_PARAM),
        (QUERY, b'{"user_id": "member"}', Codes.INVALID_PARAM),
        (QUERY, b'{"user_id": "@member"}', Codes.INVALID_PARAM),
        (QUERY, b'{"user_id": "@mem ber:zuno.chat"}', Codes.INVALID_PARAM),
        (QUERY, json.dumps({"user_id": "@" + "m" * 250 + ":zuno"}).encode(), Codes.INVALID_PARAM),
        (QUERY, b"not json", Codes.NOT_JSON),
        (QUERY, b"", Codes.NOT_JSON),
        (QUERY, b"[]", Codes.NOT_JSON),
        (QUERY, b"{}", Codes.BAD_JSON),
        (QUERY, b'{"user_id": 7}', Codes.BAD_JSON),
    ],
)
@pytest_twisted.ensureDeferred
async def test_bad_input_is_400(query, body, errcode):
    api = room()
    handler, _, cut = make(api)
    await expect_error(handler, remove(ADMIN, MEMBER, query, body), 400, errcode)
    assert (rows(api), cut) == ([], [])


@pytest_twisted.ensureDeferred
async def test_a_restart_keeps_the_removal():
    api = room()
    handler, _, _ = make(api)
    await handler(remove(MOD, MEMBER))
    fresh = Removals(as_api(api))
    await fresh.load()
    assert fresh.has(PLACE, MEMBER)
    assert not fresh.has((ROOM, "c2"), MEMBER) and not fresh.has(None, MEMBER)


@pytest_twisted.ensureDeferred
async def test_a_record_past_the_maximum_age_is_dropped_on_load():
    api = FakeModuleApi()
    await Removals(as_api(api), wall_clock=lambda: 0.0).add(PLACE, MEMBER, MOD)
    late = Removals(as_api(api), wall_clock=lambda: 7 * DAY + 1)
    await late.load()
    assert not late.has(PLACE, MEMBER) and rows(api) == []
    fresh = Removals(as_api(api), wall_clock=lambda: 7 * DAY)
    await fresh.add(PLACE, MEMBER, MOD)
    assert rows(api) == [(ROOM, "c1", MEMBER, MOD)]


@pytest_twisted.ensureDeferred
async def test_a_failed_first_load_is_tried_again():
    api = FakeModuleApi()
    real = api.run_db_interaction
    failures = [sqlite3.IntegrityError("duplicate key value violates unique constraint")]

    async def flaky(desc, func, *args):
        if failures:
            raise failures.pop()
        return await real(desc, func, *args)

    api.run_db_interaction = flaky  # type: ignore[method-assign]
    removals = Removals(as_api(api))
    await removals.load()
    await removals.add(PLACE, MEMBER, MOD)
    assert removals.has(PLACE, MEMBER)
