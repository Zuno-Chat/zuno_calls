import json

import pytest
import pytest_twisted
from conftest import FakeEvent, FakeModuleApi, FakeUpstream, as_api, as_upstream, authed
from prometheus_client import REGISTRY
from synapse.module_api.errors import Codes, SynapseError
from twisted.internet.defer import Deferred, ensureDeferred

from zuno_calls.backends.cloudflare.sfu import ROUTES, SFUHandler
from zuno_calls.config import CloudflareConfig
from zuno_calls.ratelimit import RateLimiter
from zuno_calls.resource import Call, ZunoResource
from zuno_calls.upstream import NO_RETRY, UpstreamUnavailable

CFG = CloudflareConfig(
    app_id="APP",
    app_secret="SECRET",
    turn_key_id="K",
    turn_api_token="T",
    base_url="https://cf.test",
)
ALICE = "@alice:zuno.chat"
ROOM = "!room:zuno.chat"
CALL_ID = "c1"
NOW_MS = 1_800_000_000_000
LIVE = NOW_MS + 60_000
IN_CALL = {"room_id": ROOM, "call_id": CALL_ID}


def make(upstream, api=None, cap=6, clock=lambda: 0.0):
    return SFUHandler(
        CFG,
        as_upstream(upstream),
        as_api(api or FakeModuleApi()),
        cap,
        clock=clock,
        wall_clock=lambda: NOW_MS / 1000,
    )


def call(
    method, segments, body=b'{"x":1}', content_type="application/json", user=ALICE, query=None
):
    return Call(method, tuple(segments), content_type, body, user, "DEV1", query or {})


def opening(user=ALICE, query=IN_CALL):
    return call("POST", ["new"], user=user, query=query)


def room(*members, membership="join"):
    """A FakeModuleApi whose ROOM has `members` with `membership`, none of them in a call."""
    api = FakeModuleApi()
    api.rooms[ROOM] = {("m.room.member", u): FakeEvent({"membership": membership}) for u in members}
    return api


def in_call(api, user, *memberships):
    """`user` joins ROOM and publishes `memberships`, by default one live in CALL_ID."""
    if not memberships:
        memberships = ({"call_id": CALL_ID, "device_id": "D", "expires_ts": LIVE},)
    api.rooms[ROOM][("m.room.member", user)] = FakeEvent({"membership": "join"})
    api.rooms[ROOM][("m.call.member", user)] = FakeEvent({"memberships": list(memberships)})


def full_count():
    return REGISTRY.get_sample_value("zuno_calls_call_full_total") or 0.0


async def open_session(handler, upstream, session_id="abc", user=ALICE):
    """`user` opens `session_id` through `handler`; the upstream's record is cleared after."""
    upstream.reply(201, "application/json", json.dumps({"sessionId": session_id}).encode())
    await handler(call("POST", ["new"], user=user))
    upstream.calls.clear()


async def expect_error(handler, c, code, errcode):
    with pytest.raises(SynapseError) as exc:
        await handler(c)
    assert (exc.value.code, exc.value.errcode) == (code, errcode)


@pytest.mark.parametrize(
    "method, segments, suffix",
    [
        ("POST", ["new"], "/new"),
        ("GET", ["abc"], "/abc"),
        ("POST", ["abc", "tracks", "new"], "/abc/tracks/new"),
        ("PUT", ["abc", "renegotiate"], "/abc/renegotiate"),
        ("PUT", ["abc", "tracks", "close"], "/abc/tracks/close"),
        ("PUT", ["abc", "tracks", "update"], "/abc/tracks/update"),
    ],
)
@pytest_twisted.ensureDeferred
async def test_each_route_maps_onto_cloudflare(method, segments, suffix):
    upstream = FakeUpstream()
    handler = make(upstream)
    await open_session(handler, upstream)
    upstream.reply(201, "application/json", b'{"sessionId":"abc"}')
    reply = await handler(call(method, segments))
    assert (reply.status, reply.content_type, reply.body) == (
        201,
        "application/json",
        b'{"sessionId":"abc"}',
    )
    sent = upstream.calls[0]
    assert (sent["method"], sent["url"]) == (
        method,
        "https://cf.test/v1/apps/APP/sessions" + suffix,
    )
    assert sent["headers"]["Authorization"] == "Bearer SECRET"
    assert (sent["api"], sent["retry"]) == ("sfu", NO_RETRY)
    if method == "GET":
        assert sent["body"] is None and "Content-Type" not in sent["headers"]
    else:
        assert sent["body"] == b'{"x":1}' and sent["headers"]["Content-Type"] == "application/json"


@pytest_twisted.ensureDeferred
async def test_client_authorization_is_never_forwarded():
    upstream = FakeUpstream()
    await make(upstream)(call("POST", ["new"]))
    assert set(upstream.calls[0]["headers"]) == {"Authorization", "Content-Type"}
    assert upstream.calls[0]["headers"]["Authorization"] == "Bearer SECRET"


@pytest_twisted.ensureDeferred
async def test_missing_content_type_is_not_invented():
    upstream = FakeUpstream()
    await make(upstream)(call("POST", ["new"], content_type=None))
    assert "Content-Type" not in upstream.calls[0]["headers"]
    assert upstream.calls[0]["body"] == b'{"x":1}'


@pytest.mark.parametrize("bad", ["a b", "..", "a/b", "x" * 129, "", "sess%ion"])
@pytest_twisted.ensureDeferred
async def test_bad_session_id_is_400(bad):
    upstream = FakeUpstream()
    handler = make(upstream)
    await expect_error(handler, call("PUT", [bad, "renegotiate"]), 400, Codes.INVALID_PARAM)
    assert upstream.calls == []


@pytest_twisted.ensureDeferred
async def test_another_users_session_is_404():
    upstream = FakeUpstream()
    handler = make(upstream)
    await open_session(handler, upstream)
    mallory = call("PUT", ["abc", "tracks", "close"], user="@mallory:zuno.chat")
    await expect_error(handler, mallory, 404, Codes.NOT_FOUND)
    assert upstream.calls == []


@pytest.mark.parametrize(
    "status, body",
    [
        (400, b'{"sessionId":"abc"}'),
        (201, b"not json"),
        (201, b"[]"),
        (201, b"{}"),
        (201, b'{"sessionId":7}'),
    ],
)
@pytest_twisted.ensureDeferred
async def test_a_session_not_opened_here_is_404(status, body):
    upstream = FakeUpstream()
    handler = make(upstream)
    upstream.reply(status, "application/json", body)
    await handler(call("POST", ["new"]))
    await expect_error(handler, call("GET", ["abc"]), 404, Codes.NOT_FOUND)
    assert len(upstream.calls) == 1


@pytest_twisted.ensureDeferred
async def test_past_the_cap_a_user_loses_only_their_own_oldest_session(monkeypatch):
    monkeypatch.setattr("zuno_calls.backends.cloudflare.sfu._SESSIONS_PER_USER", 2)
    upstream = FakeUpstream()
    handler = make(upstream)
    await open_session(handler, upstream, "bob1", user="@bob:zuno.chat")
    for session_id in ("s1", "s2", "s3"):
        await open_session(handler, upstream, session_id)
    await handler(call("GET", ["s2"]))
    await handler(call("GET", ["s3"]))
    await handler(call("GET", ["bob1"], user="@bob:zuno.chat"))
    await expect_error(handler, call("GET", ["s1"]), 404, Codes.NOT_FOUND)


@pytest_twisted.ensureDeferred
async def test_cloudflare_error_status_is_relayed():
    upstream = FakeUpstream()
    upstream.reply(400, "application/json", b'{"errorCode":"bad"}')
    reply = await make(upstream)(call("POST", ["new"]))
    assert (reply.status, reply.body) == (400, b'{"errorCode":"bad"}')


@pytest_twisted.ensureDeferred
async def test_wired_resource_serves_listed_routes_on_own_sessions_only():
    upstream = FakeUpstream()
    api = room("@alice:zuno.chat")
    res = ZunoResource(as_api(api), RateLimiter(100, 100), make(upstream, api), routes=ROUTES)
    for method, segments in (("GET", ["new"]), ("DELETE", ["abc"])):
        with pytest.raises(SynapseError) as exc:
            await res._async_render(authed(method, segments))
        assert exc.value.code == 404
    upstream.reply(201, "application/json", b'{"sessionId":"abc"}')
    args = {"room_id": [ROOM], "call_id": [CALL_ID]}
    await res._async_render(authed("POST", ["new"], b"{}", args=args))
    assert upstream.calls[0]["url"] == "https://cf.test/v1/apps/APP/sessions/new"
    req = authed("POST", ["abc", "tracks", "new"], b"{}")
    await res._async_render(req)
    assert req.code == 200
    assert upstream.calls[1]["url"].endswith("/sessions/abc/tracks/new")
    with pytest.raises(SynapseError) as exc:
        await res._async_render(authed("GET", ["abc"], token="tok-bot"))
    assert (exc.value.code, exc.value.errcode) == (404, Codes.NOT_FOUND)


@pytest_twisted.ensureDeferred
async def test_a_full_call_is_refused_before_cloudflare():
    api = room(ALICE)
    in_call(api, "@bob:zuno.chat")
    in_call(api, "@carol:zuno.chat")
    upstream = FakeUpstream()
    before = full_count()
    with pytest.raises(SynapseError) as exc:
        await make(upstream, api, cap=2)(opening())
    assert (exc.value.code, exc.value.errcode) == (403, "IM.ZUNO.CALL_FULL")
    assert exc.value.error_dict(None) == {
        "errcode": "IM.ZUNO.CALL_FULL",
        "error": "This call is full",
        "max_participants": 2,
    }
    assert upstream.calls == []
    assert full_count() - before == 1


@pytest_twisted.ensureDeferred
async def test_the_last_place_is_relayed_without_the_query():
    api = room(ALICE)
    in_call(api, "@bob:zuno.chat")
    upstream = FakeUpstream()
    reply = await make(upstream, api, cap=2)(opening())
    assert reply.status == 200
    assert upstream.calls[0]["url"] == "https://cf.test/v1/apps/APP/sessions/new"


@pytest_twisted.ensureDeferred
async def test_a_rejoin_never_counts_the_caller():
    api = room()
    in_call(api, ALICE)
    in_call(api, "@bob:zuno.chat")
    upstream = FakeUpstream()
    handler = make(upstream, api, cap=2)
    await handler(opening())
    await handler(opening())
    assert len(upstream.calls) == 2


@pytest.mark.parametrize(
    "memberships",
    [
        [{"call_id": CALL_ID, "expires_ts": NOW_MS}],
        [{"call_id": "other", "expires_ts": LIVE}],
        [],
        ["x"],
        [{"call_id": CALL_ID}],
        [{"call_id": CALL_ID, "expires_ts": str(LIVE)}],
        "x",
    ],
)
@pytest_twisted.ensureDeferred
async def test_a_membership_not_live_in_this_call_is_not_counted(memberships):
    api = room(ALICE)
    api.rooms[ROOM][("m.call.member", "@bob:zuno.chat")] = FakeEvent({"memberships": memberships})
    upstream = FakeUpstream()
    await make(upstream, api, cap=1)(opening())
    assert len(upstream.calls) == 1


@pytest_twisted.ensureDeferred
async def test_two_opens_racing_for_the_last_place():
    class Gated(FakeUpstream):
        gate: Deferred[None] = Deferred()

        async def request(self, *args, **kw):
            await self.gate
            return await super().request(*args, **kw)

    api = room(ALICE, "@bob:zuno.chat")
    upstream = Gated()
    handler = make(upstream, api, cap=1)
    first = ensureDeferred(handler(opening()))
    second = ensureDeferred(handler(opening("@bob:zuno.chat")))
    # Opened before awaiting, so a broken order fails the test instead of hanging it.
    upstream.gate.callback(None)
    assert (await first).status == 200
    with pytest.raises(SynapseError) as exc:
        await second
    assert (exc.value.code, exc.value.errcode) == (403, "IM.ZUNO.CALL_FULL")
    assert len(upstream.calls) == 1


@pytest.mark.parametrize(
    "failure",
    [UpstreamUnavailable("timeout"), (503, "application/json", b"{}"), (400, None, b"")],
)
@pytest_twisted.ensureDeferred
async def test_a_failed_open_releases_its_place(failure):
    api = room(ALICE, "@bob:zuno.chat")
    upstream = FakeUpstream()
    if isinstance(failure, Exception):
        upstream.replies.append(failure)
    else:
        upstream.reply(*failure)
    handler = make(upstream, api, cap=1)
    try:
        await handler(opening())
    except UpstreamUnavailable:
        pass
    assert (await handler(opening("@bob:zuno.chat"))).status == 200


@pytest_twisted.ensureDeferred
async def test_a_reservation_lasts_sixty_seconds():
    api = room(ALICE, "@bob:zuno.chat")
    now = [0.0]
    handler = make(FakeUpstream(), api, cap=1, clock=lambda: now[0])
    await handler(opening())
    now[0] = 59.9
    await expect_error(handler, opening("@bob:zuno.chat"), 403, "IM.ZUNO.CALL_FULL")
    now[0] = 60.0
    assert (await handler(opening("@bob:zuno.chat"))).status == 200


@pytest_twisted.ensureDeferred
async def test_expired_reservations_are_purged(monkeypatch):
    monkeypatch.setattr("zuno_calls.backends.cloudflare.sfu._PURGE_FLOOR", 2)
    api = room(ALICE)
    now = [0.0]
    handler = make(FakeUpstream(), api, clock=lambda: now[0])
    await handler(opening(query={"room_id": ROOM, "call_id": "a"}))
    now[0] = 60.0
    await handler(opening(query={"room_id": ROOM, "call_id": "b"}))
    assert list(handler._reservations) == [(ROOM, "b")]


@pytest.mark.parametrize("membership", ["leave", "invite", "ban", None])
@pytest_twisted.ensureDeferred
async def test_a_caller_not_joined_is_403(membership):
    api = room(ALICE, membership=membership) if membership else room()
    upstream = FakeUpstream()
    await expect_error(make(upstream, api), opening(), 403, Codes.FORBIDDEN)
    assert upstream.calls == []


@pytest.mark.parametrize(
    "query",
    [
        {"room_id": "room:zuno.chat", "call_id": CALL_ID},
        {"room_id": ROOM, "call_id": ""},
        {"room_id": ROOM},
        {"call_id": CALL_ID},
        {"room_id": "!" + "r" * 255, "call_id": CALL_ID},
        {"room_id": ROOM, "call_id": "c" * 256},
    ],
)
@pytest_twisted.ensureDeferred
async def test_a_bad_room_or_call_id_is_400(query):
    upstream = FakeUpstream()
    await expect_error(make(upstream, room(ALICE)), opening(query=query), 400, Codes.INVALID_PARAM)
    assert upstream.calls == []


@pytest_twisted.ensureDeferred
async def test_an_open_naming_no_call_is_relayed_unchecked():
    api = room()
    in_call(api, "@bob:zuno.chat")
    upstream = FakeUpstream()
    await make(upstream, api, cap=1)(opening(query={}))
    assert len(upstream.calls) == 1
