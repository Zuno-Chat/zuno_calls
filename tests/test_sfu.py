import json
from dataclasses import dataclass, field
from typing import Any, cast

import pytest
import pytest_twisted
from conftest import (
    FakeEvent,
    FakeModuleApi,
    FakeUpstream,
    as_api,
    as_upstream,
    authed,
    expect_error,
    sample,
)
from synapse.module_api.errors import Codes, SynapseError
from twisted.internet import reactor as _reactor
from twisted.internet.defer import Deferred, ensureDeferred, succeed
from twisted.internet.task import deferLater

from zuno_calls.backends.cloudflare.sfu import ROUTES, SFUHandler
from zuno_calls.config import CloudflareConfig
from zuno_calls.ratelimit import RateLimiter
from zuno_calls.removal import Removals, RemoveHandler
from zuno_calls.resource import Call, ZunoResource, match
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
reactor: Any = _reactor  # the reactor module has no usable static type


def make(upstream, api=None, cap=6, clock=lambda: 0.0, now_ms=NOW_MS, wall_clock=None):
    api = as_api(api or FakeModuleApi())
    wall_clock = wall_clock or (lambda: now_ms / 1000)
    return SFUHandler(
        CFG,
        as_upstream(upstream),
        api,
        cap,
        Removals(api, wall_clock=wall_clock),
        clock=clock,
        wall_clock=wall_clock,
    )


async def settled(handler):
    await cast(Any, handler._api).settle()


def at_once(handler, c):
    answer = ensureDeferred(handler(c))
    assert answer.called
    return answer


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
    return sample("zuno_calls_call_full_total")


async def open_session(handler, upstream, session_id="abc", user=ALICE, query=None):
    """`user` opens `session_id` through `handler`; the upstream's record is cleared after."""
    upstream.reply(201, "application/json", json.dumps({"sessionId": session_id}).encode())
    await handler(call("POST", ["new"], user=user, query=query))
    upstream.calls.clear()


@pytest.mark.parametrize(
    "method, segments, suffix",
    [
        ("POST", ["new"], "/new"),
        ("GET", ["abc"], "/abc"),
        ("POST", ["abc", "tracks", "new"], "/abc/tracks/new"),
        ("PUT", ["abc", "renegotiate"], "/abc/renegotiate"),
        ("PUT", ["abc", "tracks", "close"], "/abc/tracks/close"),
        ("PUT", ["abc", "tracks", "update"], "/abc/tracks/update"),
        ("POST", ["abc", "datachannels", "establish"], "/abc/datachannels/establish"),
        ("POST", ["abc", "datachannels", "new"], "/abc/datachannels/new"),
        ("PUT", ["abc", "datachannels", "close"], "/abc/datachannels/close"),
    ],
)
@pytest_twisted.ensureDeferred
async def test_each_route_maps_onto_cloudflare(method, segments, suffix):
    assert match(ROUTES, method, tuple(segments))
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


@pytest.mark.parametrize(
    "method, rest",
    [
        ("PUT", ["tracks", "close"]),
        ("POST", ["datachannels", "establish"]),
        ("POST", ["datachannels", "new"]),
        ("PUT", ["datachannels", "close"]),
    ],
)
@pytest_twisted.ensureDeferred
async def test_another_users_session_is_404(method, rest):
    upstream = FakeUpstream()
    handler = make(upstream)
    await open_session(handler, upstream)
    mallory = call(method, ["abc", *rest], user="@mallory:zuno.chat")
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


BOB = "@bob:zuno.chat"
MALLORY = "@mallory:zuno.chat"
PLACE = (ROOM, CALL_ID)
OTHER_CALL = {"room_id": ROOM, "call_id": "c2"}
REMOVED = {"errcode": "IM.ZUNO.CALL_REMOVED", "error": "Removed from this call"}


def a_call(expires_ts=LIVE):
    """ROOM, made by ALICE at 100, with ALICE, BOB and MALLORY live in CALL_ID."""
    api = room()
    api.rooms[ROOM][("m.room.create", "")] = FakeEvent({"room_version": "11"}, sender=ALICE)
    api.rooms[ROOM][("m.room.power_levels", "")] = FakeEvent({"users": {ALICE: 100}})
    for user in (ALICE, BOB, MALLORY):
        in_call(api, user, {"call_id": CALL_ID, "device_id": "D", "expires_ts": expires_ts})
    return api


async def removed_from_call(upstream=None):
    """Sessions a, b and m bound to CALL_ID for ALICE, BOB and MALLORY; MALLORY removed."""
    api = a_call()
    upstream = upstream or FakeUpstream()
    handler = make(upstream, api)
    for session_id, user in (("a", ALICE), ("b", BOB), ("m", MALLORY)):
        await open_session(handler, upstream, session_id, user, IN_CALL)
    await handler._removals.add(PLACE, MALLORY, ALICE)
    return handler, upstream, api


def pull(session_id, *tracks, user=BOB, route=("tracks", "new")):
    body = {"tracks": [{"location": "remote", "sessionId": s, "trackName": n} for s, n in tracks]}
    method = "POST" if route == ("tracks", "new") else "PUT"
    return call(method, [session_id, *route], json.dumps(body).encode(), user=user)


def sent_tracks(upstream):
    return json.loads(upstream.calls[-1]["body"])["tracks"]


def refusals(route, scope):
    return sample("zuno_calls_removed_refusals_total", route=route, scope=scope)


async def expect_removed(handler, c):
    with pytest.raises(SynapseError) as exc:
        await handler(c)
    assert (exc.value.code, exc.value.error_dict(None)) == (403, REMOVED)


@pytest_twisted.ensureDeferred
async def test_a_removed_person_cannot_reopen_in_the_call_full_or_not():
    api = a_call()
    upstream = FakeUpstream()
    handler = make(upstream, api, cap=2)
    await handler._removals.add(PLACE, MALLORY, ALICE)
    full, refused = full_count(), refusals("sessions/new", "request")
    await expect_removed(handler, opening(MALLORY))
    assert upstream.calls == []
    assert (full_count() - full, refusals("sessions/new", "request") - refused) == (0, 1)


@pytest_twisted.ensureDeferred
async def test_a_removed_person_may_open_in_another_call():
    handler, upstream, _ = await removed_from_call()
    assert (await handler(opening(MALLORY, OTHER_CALL))).status == 200
    assert len(upstream.calls) == 1


@pytest.mark.parametrize(
    "method, rest",
    [
        ("GET", []),
        ("POST", ["tracks", "new"]),
        ("PUT", ["renegotiate"]),
        ("PUT", ["tracks", "close"]),
        ("PUT", ["tracks", "update"]),
        ("POST", ["datachannels", "establish"]),
        ("POST", ["datachannels", "new"]),
        ("PUT", ["datachannels", "close"]),
    ],
)
@pytest_twisted.ensureDeferred
async def test_a_removed_persons_session_in_the_call_is_refused(method, rest):
    handler, upstream, _ = await removed_from_call()
    route = "/".join(rest) or "session"
    before = refusals(route, "request")
    await expect_removed(handler, call(method, ["m", *rest], user=MALLORY))
    assert upstream.calls == []
    assert refusals(route, "request") - before == 1


@pytest.mark.parametrize("query", [OTHER_CALL, None])
@pytest_twisted.ensureDeferred
async def test_a_removed_person_pulling_the_call_from_any_other_session_is_refused(query):
    handler, upstream, _ = await removed_from_call()
    await open_session(handler, upstream, "m2", MALLORY, query)
    await expect_removed(handler, pull("m2", ("b", "video"), ("a", "audio"), user=MALLORY))
    assert upstream.calls == []


@pytest_twisted.ensureDeferred
async def test_a_removed_persons_track_is_refused_per_track_the_rest_relayed():
    handler, upstream, _ = await removed_from_call()
    pulled = {"location": "remote", "sessionId": "a", "trackName": "audio", "mid": "4"}
    answer = {"requiresImmediateRenegotiation": True, "tracks": [pulled]}
    upstream.reply(200, "application/json", json.dumps(answer).encode())
    before = refusals("tracks/new", "track")
    reply = await handler(pull("b", ("m", "audio"), ("a", "audio")))
    assert sent_tracks(upstream) == [{"location": "remote", "sessionId": "a", "trackName": "audio"}]
    assert (reply.status, reply.content_type) == (200, "application/json")
    assert json.loads(reply.body)["tracks"] == [
        {
            "location": "remote",
            "sessionId": "m",
            "trackName": "audio",
            "errorCode": "IM.ZUNO.CALL_REMOVED",
            "errorDescription": "Removed from this call",
        },
        pulled,
    ]
    assert refusals("tracks/new", "track") - before == 1


@pytest.mark.parametrize("remote", ["a2", "unknown"])
@pytest_twisted.ensureDeferred
async def test_a_track_outside_the_call_is_refused_per_track(remote):
    handler, upstream, _ = await removed_from_call()
    await open_session(handler, upstream, "a2", ALICE, OTHER_CALL)
    reply = await handler(pull("b", (remote, "video")))
    assert upstream.calls == []
    assert json.loads(reply.body) == {
        "tracks": [
            {
                "location": "remote",
                "sessionId": remote,
                "trackName": "video",
                "errorCode": "IM.ZUNO.TRACK_NOT_IN_CALL",
                "errorDescription": "Not in this call",
            }
        ],
    }


@pytest_twisted.ensureDeferred
async def test_tracks_update_is_screened_too():
    handler, upstream, _ = await removed_from_call()
    reply = await handler(pull("b", ("m", "video"), route=("tracks", "update")))
    assert upstream.calls == []
    assert json.loads(reply.body)["tracks"][0]["errorCode"] == "IM.ZUNO.CALL_REMOVED"


@pytest_twisted.ensureDeferred
async def test_an_unbound_side_skips_the_binding_check():
    handler, upstream, _ = await removed_from_call()
    await open_session(handler, upstream, "bu", BOB)
    await open_session(handler, upstream, "au", ALICE)
    await handler(pull("bu", ("a", "audio"), ("unknown", "audio")))
    assert [t["sessionId"] for t in sent_tracks(upstream)] == ["a", "unknown"]
    await handler(pull("b", ("au", "audio")))
    assert [t["sessionId"] for t in sent_tracks(upstream)] == ["au"]


@pytest_twisted.ensureDeferred
async def test_a_removed_persons_unbound_session_is_refused_to_the_call():
    handler, upstream, _ = await removed_from_call()
    await open_session(handler, upstream, "mu", MALLORY)
    reply = await handler(pull("b", ("mu", "audio")))
    assert upstream.calls == []
    assert json.loads(reply.body)["tracks"][0]["errorCode"] == "IM.ZUNO.CALL_REMOVED"


@pytest_twisted.ensureDeferred
async def test_local_tracks_are_relayed_as_sent():
    handler, upstream, _ = await removed_from_call()
    push = {
        "sessionDescription": {"type": "offer", "sdp": "v=0"},
        "tracks": [{"location": "local", "mid": "0", "trackName": "audio"}],
        "autoDiscover": False,
    }
    await handler(call("POST", ["b", "tracks", "new"], json.dumps(push).encode(), user=BOB))
    assert json.loads(upstream.calls[0]["body"]) == push


@pytest_twisted.ensureDeferred
async def test_a_pulls_simulcast_object_is_relayed_untouched():
    handler, upstream, _ = await removed_from_call()
    track = {
        "location": "remote",
        "sessionId": "a",
        "trackName": "video",
        "simulcast": {"preferredRid": "h", "priorityOrdering": "asciibetical"},
    }
    body = json.dumps({"tracks": [track]}).encode()
    await handler(call("POST", ["b", "tracks", "new"], body, user=BOB))
    assert sent_tracks(upstream) == [track]


def subscribe(session_id, *remotes, user=BOB):
    channels = [
        {"location": "remote", "sessionId": s, "dataChannelName": "quality"} for s in remotes
    ]
    body = json.dumps({"dataChannels": channels}).encode()
    return call("POST", [session_id, "datachannels", "new"], body, user=user)


def sent_channels(upstream):
    return json.loads(upstream.calls[-1]["body"])["dataChannels"]


def granted(*remotes):
    channels = [
        {"location": "remote", "sessionId": s, "dataChannelName": "quality", "id": i}
        for i, s in enumerate(remotes, start=2)
    ]
    return {"dataChannels": channels}


def refused_channel(remote, errcode, description):
    return {
        "location": "remote",
        "sessionId": remote,
        "dataChannelName": "quality",
        "errorCode": errcode,
        "errorDescription": description,
    }


@pytest_twisted.ensureDeferred
async def test_establishing_the_data_channel_transport_is_relayed():
    handler, upstream, _ = await removed_from_call()
    establish = b'{"dataChannel":{"location":"remote","dataChannelName":"server-events"}}'
    offer = b'{"sessionDescription":{"type":"offer","sdp":"v=0"},"dataChannel":{"id":0}}'
    upstream.reply(200, body=offer)
    reply = await handler(call("POST", ["b", "datachannels", "establish"], establish, user=BOB))
    assert upstream.calls[0]["body"] == establish
    assert (reply.status, reply.body) == (200, offer)


@pytest_twisted.ensureDeferred
async def test_publishing_a_data_channel_is_relayed():
    handler, upstream, _ = await removed_from_call()
    publish = {"dataChannels": [{"location": "local", "dataChannelName": "quality"}]}
    await handler(
        call("POST", ["b", "datachannels", "new"], json.dumps(publish).encode(), user=BOB)
    )
    assert json.loads(upstream.calls[0]["body"]) == publish


@pytest_twisted.ensureDeferred
async def test_subscribing_to_a_peers_data_channel_in_the_call_is_relayed():
    handler, upstream, _ = await removed_from_call()
    upstream.reply(200, body=json.dumps(granted("a")).encode())
    reply = await handler(subscribe("b", "a"))
    assert sent_channels(upstream) == [
        {"location": "remote", "sessionId": "a", "dataChannelName": "quality"}
    ]
    assert json.loads(reply.body) == granted("a")


@pytest.mark.parametrize("remote", ["a2", "unknown"])
@pytest_twisted.ensureDeferred
async def test_a_data_channel_outside_the_call_is_refused_per_channel(remote):
    handler, upstream, _ = await removed_from_call()
    await open_session(handler, upstream, "a2", ALICE, OTHER_CALL)
    upstream.reply(200, body=json.dumps(granted("a")).encode())
    reply = await handler(subscribe("b", remote, "a"))
    assert [c["sessionId"] for c in sent_channels(upstream)] == ["a"]
    assert json.loads(reply.body)["dataChannels"] == [
        refused_channel(remote, "IM.ZUNO.TRACK_NOT_IN_CALL", "Not in this call"),
        *granted("a")["dataChannels"],
    ]


@pytest_twisted.ensureDeferred
async def test_a_removed_persons_data_channel_is_refused_per_channel():
    handler, upstream, _ = await removed_from_call()
    before = refusals("datachannels/new", "channel")
    reply = await handler(subscribe("b", "m"))
    assert upstream.calls == []
    assert (reply.status, json.loads(reply.body)) == (
        200,
        {"dataChannels": [refused_channel("m", "IM.ZUNO.CALL_REMOVED", "Removed from this call")]},
    )
    assert refusals("datachannels/new", "channel") - before == 1


@pytest.mark.parametrize("query", [OTHER_CALL, None])
@pytest_twisted.ensureDeferred
async def test_a_removed_person_subscribing_to_the_call_from_any_session_is_refused(query):
    handler, upstream, _ = await removed_from_call()
    await open_session(handler, upstream, "m2", MALLORY, query)
    await expect_removed(handler, subscribe("m2", "b", user=MALLORY))
    assert upstream.calls == []


@pytest.mark.parametrize("body", [b"not json", b"[]", b'"tracks"'])
@pytest_twisted.ensureDeferred
async def test_a_track_body_not_a_json_object_is_400(body):
    handler, upstream, _ = await removed_from_call()
    with pytest.raises(SynapseError) as exc:
        await handler(call("POST", ["b", "tracks", "new"], body, user=BOB))
    assert (exc.value.code, exc.value.errcode) == (400, Codes.NOT_JSON)
    assert upstream.calls == []


@pytest_twisted.ensureDeferred
async def test_a_restart_keeps_a_removed_person_out():
    _, _, api = await removed_from_call()
    upstream = FakeUpstream()
    await expect_removed(make(upstream, api), opening(MALLORY))
    assert upstream.calls == []


@pytest_twisted.ensureDeferred
async def test_a_call_whose_memberships_are_gone_drops_its_removals():
    handler, upstream, api = await removed_from_call()
    for user in (ALICE, BOB, MALLORY):
        del api.rooms[ROOM][("m.call.member", user)]
    assert (await handler(opening(MALLORY))).status == 200
    assert not handler._removals.has(PLACE, MALLORY)
    assert api.db.execute("SELECT COUNT(*) FROM zuno_calls_removals").fetchone() == (0,)


def remover(handler, api):
    return RemoveHandler(as_api(api), handler._removals, [handler.cut_off])


def removing(target=MALLORY):
    body = json.dumps({"user_id": target}).encode()
    return Call("POST", (), "application/json", body, ALICE, "DEV1", IN_CALL)


def state(*tracks):
    return json.dumps({"tracks": list(tracks)}).encode()


def requests(upstream):
    return [
        (c["method"], c["url"].removeprefix("https://cf.test/v1/apps/APP/sessions/"))
        for c in upstream.calls
    ]


@pytest_twisted.ensureDeferred
async def test_a_removal_force_closes_the_targets_tracks_in_the_call():
    upstream = FakeUpstream()
    handler, _, api = await removed_from_call(upstream)
    await open_session(handler, upstream, "mu", MALLORY)
    await open_session(handler, upstream, "m2", MALLORY, OTHER_CALL)
    await open_session(handler, upstream, "a2", ALICE, OTHER_CALL)
    upstream.reply(
        200,
        body=state(
            {"location": "local", "mid": "0", "trackName": "audio", "status": "active"},
            {"location": "remote", "sessionId": "a", "mid": "1", "status": "active"},
            {"location": "local", "mid": "2", "trackName": "video", "status": "inactive"},
        ),
    )
    upstream.reply(200, body=b'{"tracks":[{"mid":"0"},{"mid":"1"}]}')
    upstream.reply(
        200,
        body=state(
            {"location": "remote", "sessionId": "b", "mid": "0", "status": "active"},
            {"location": "remote", "sessionId": "a2", "mid": "1", "status": "active"},
            {"location": "local", "mid": "2", "trackName": "audio", "status": "active"},
        ),
    )
    closed = {"mid": "0", "errorCode": "close_track_error", "errorDescription": "already closed"}
    upstream.reply(200, body=json.dumps({"tracks": [closed]}).encode())
    reply = await remover(handler, api)(removing())
    assert (reply.status, reply.body) == (200, b"{}")
    assert requests(upstream) == [
        ("GET", "m"),
        ("PUT", "m/tracks/close"),
        ("GET", "mu"),
        ("PUT", "mu/tracks/close"),
    ]
    close_m, close_mu = (json.loads(c["body"]) for c in upstream.calls[1::2])
    assert close_m == {"tracks": [{"mid": "0"}, {"mid": "1"}], "force": True}
    assert close_mu == {"tracks": [{"mid": "0"}], "force": True}
    assert upstream.calls[1]["headers"] == {
        "Authorization": "Bearer SECRET",
        "Content-Type": "application/json",
    }


def channel(id, status="active", **named):
    return {"dataChannelName": "quality", "id": id, "status": status, **named}


@pytest_twisted.ensureDeferred
async def test_a_removal_closes_the_targets_data_channels_in_the_call():
    upstream = FakeUpstream()
    handler, _, api = await removed_from_call(upstream)
    await open_session(handler, upstream, "mu", MALLORY)
    server_events = {"location": "remote", "dataChannelName": "server-events", "id": 0}
    upstream.reply(
        200,
        body=json.dumps(
            {
                "tracks": [{"location": "local", "mid": "0", "status": "active"}],
                "dataChannels": [
                    {**server_events, "status": "active"},
                    channel(1, location="local"),
                    channel(2, location="remote", sessionId="a"),
                    channel(3, "inactive", location="remote", sessionId="b"),
                ],
            }
        ).encode(),
    )
    upstream.reply(200, body=b'{"tracks":[{"mid":"0"}]}')
    upstream.reply(200, body=b'{"dataChannels":[{"id":0},{"id":1},{"id":2}]}')
    upstream.reply(
        200,
        body=json.dumps(
            {
                "dataChannels": [
                    {**server_events, "status": "active"},
                    channel(1, location="remote", sessionId="a"),
                ]
            }
        ).encode(),
    )
    upstream.reply(200, body=b'{"dataChannels":[{"id":1}]}')
    assert (await remover(handler, api)(removing())).status == 200
    assert requests(upstream) == [
        ("GET", "m"),
        ("PUT", "m/tracks/close"),
        ("PUT", "m/datachannels/close"),
        ("GET", "mu"),
        ("PUT", "mu/datachannels/close"),
    ]
    assert json.loads(upstream.calls[2]["body"]) == {
        "dataChannels": [{"id": 0}, {"id": 1}, {"id": 2}]
    }
    assert json.loads(upstream.calls[4]["body"]) == {"dataChannels": [{"id": 1}]}


@pytest.mark.parametrize("status", [404, 400])
@pytest_twisted.ensureDeferred
async def test_a_session_cloudflare_no_longer_knows_needs_no_close(status):
    upstream = FakeUpstream()
    handler, _, api = await removed_from_call(upstream)
    upstream.reply(status, body=b'{"errorCode":"not_found"}')
    assert (await remover(handler, api)(removing())).status == 200
    assert requests(upstream) == [("GET", "m")]


@dataclass
class Gated(FakeUpstream):
    """Holds the n-th request until gates[n] fires; later ones go straight through."""

    gates: list[Deferred[None]] = field(default_factory=list)
    seen: int = 0

    async def request(self, *args, **kw):
        n, self.seen = self.seen, self.seen + 1
        if n < len(self.gates):
            await self.gates[n]
        return await super().request(*args, **kw)


@pytest_twisted.ensureDeferred
async def test_a_removal_waits_out_the_targets_request_in_flight():
    upstream = FakeUpstream()
    handler, _, api = await removed_from_call(upstream)
    await handler._removals.end(PLACE)  # back in, to be removed mid-request
    gate: Deferred[None] = Deferred()
    gated = Gated(gates=[gate])
    handler._upstream = as_upstream(gated)
    gated.reply()  # the pull in flight
    gated.reply(200, body=state({"location": "remote", "sessionId": "a", "mid": "5"}))
    gated.reply(200, body=b'{"tracks":[{"mid":"5"}]}')
    in_flight = ensureDeferred(handler(pull("m", ("a", "audio"), user=MALLORY)))
    removal = ensureDeferred(remover(handler, api)(removing()))
    await deferLater(reactor, 0.12, lambda: None)
    assert not removal.called and gated.calls == []
    await expect_removed(handler, pull("m", ("b", "audio"), user=MALLORY))
    gate.callback(None)
    assert (await in_flight).status == 200
    assert (await removal).status == 200
    assert requests(gated) == [("POST", "m/tracks/new"), ("GET", "m"), ("PUT", "m/tracks/close")]


ACTIVE = state({"location": "local", "mid": "0", "trackName": "audio", "status": "active"})
OPEN_CHANNEL = json.dumps({"dataChannels": [channel(1, location="local")]}).encode()


@pytest.mark.parametrize(
    "failure",
    [
        [(503, b"{}")],
        [(200, b'{"errorCode":"internal_error"}')],
        [(200, ACTIVE), (500, b"{}")],
        [(200, ACTIVE), (200, b'{"tracks":[{"mid":"0","errorCode":"internal_error"}]}')],
        [(200, ACTIVE), (200, b'{"tracks":[]}')],
        [(200, ACTIVE), (200, b'{"errorCode":"internal_error","tracks":[{"mid":"0"}]}')],
        [(200, ACTIVE), (200, b"not json")],
        [(200, ACTIVE), (429, b"{}")],
        [(200, ACTIVE), UpstreamUnavailable("timeout")],
        [UpstreamUnavailable("connect")],
        [(429, b"{}")],
        [(200, b'{"tracks":[],"dataChannels":"x"}')],
        [(200, OPEN_CHANNEL), (500, b"{}")],
        [(200, OPEN_CHANNEL), (200, b'{"dataChannels":[{"id":1,"errorCode":"internal_error"}]}')],
        [(200, OPEN_CHANNEL), (200, b'{"dataChannels":[]}')],
        [(200, OPEN_CHANNEL), (200, b'{"tracks":[{"id":1}]}')],
    ],
)
@pytest_twisted.ensureDeferred
async def test_a_failed_close_is_503_and_a_repeat_finishes_it(failure):
    upstream = FakeUpstream()
    handler, _, api = await removed_from_call(upstream)
    for reply in failure:
        if isinstance(reply, Exception):
            upstream.replies.append(reply)
        else:
            upstream.reply(reply[0], body=reply[1])
    with pytest.raises(SynapseError) as exc:
        await remover(handler, api)(removing())
    assert (exc.value.code, exc.value.errcode) == (503, Codes.UNKNOWN)
    assert handler._removals.has(PLACE, MALLORY)
    await expect_removed(handler, call("GET", ["m"], user=MALLORY))
    upstream.calls.clear()
    upstream.reply(200, body=ACTIVE)
    upstream.reply(200, body=b'{"tracks":[{"mid":"0"}]}')
    assert (await remover(handler, api)(removing())).status == 200
    assert requests(upstream) == [("GET", "m"), ("PUT", "m/tracks/close")]


@pytest_twisted.ensureDeferred
async def test_a_removal_waits_only_for_requests_in_flight_when_it_starts():
    upstream = FakeUpstream()
    handler, _, api = await removed_from_call(upstream)
    await open_session(handler, upstream, "mu", MALLORY)
    await handler._removals.end(PLACE)
    first: Deferred[None] = Deferred()
    second: Deferred[None] = Deferred()
    handler._upstream = as_upstream(Gated(gates=[first, second]))
    before = ensureDeferred(handler(call("GET", ["mu"], user=MALLORY)))
    removal = ensureDeferred(remover(handler, api)(removing()))
    # Unbound, so not refused: kept busy, it must not hold the removal up.
    after = ensureDeferred(handler(call("GET", ["mu"], user=MALLORY)))
    removal.addTimeout(5, reactor)
    first.callback(None)
    assert (await before).status == 200
    assert (await removal).status == 200
    assert not after.called
    second.callback(None)
    assert (await after).status == 200


CLOSED = b'{"tracks":[{"mid":"0"}]}'


def opened(session_id):
    return json.dumps({"sessionId": session_id}).encode()


def rows(api):
    return [r[0] for r in api.db.execute("SELECT session_id FROM zuno_calls_cloudflare_sessions")]


async def at_the_cap(monkeypatch, clock=lambda: 0.0, wall_clock=None):
    monkeypatch.setattr("zuno_calls.backends.cloudflare.sfu._SESSIONS_PER_USER", 2)
    upstream = FakeUpstream()
    handler = make(upstream, a_call(), clock=clock, wall_clock=wall_clock)
    await open_session(handler, upstream, "m", MALLORY, IN_CALL)
    await open_session(handler, upstream, "mu", MALLORY)
    return handler, upstream


def slowed(handler, *gated):
    upstream = Gated(gates=list(gated))
    handler._upstream = as_upstream(upstream)
    return upstream


@pytest_twisted.ensureDeferred
async def test_past_the_cap_the_open_answers_before_the_oldest_closes(monkeypatch):
    handler, _ = await at_the_cap(monkeypatch)
    slow: Deferred[None] = Deferred()
    upstream = slowed(handler, succeed(None), slow)
    upstream.reply(201, body=opened("m3"))
    upstream.reply(200, body=ACTIVE)
    upstream.reply(200, body=CLOSED)
    reply = await at_once(handler, call("POST", ["new"], user=MALLORY))
    assert (reply.status, reply.body) == (201, opened("m3"))
    assert requests(upstream) == [("POST", "new")]
    await expect_error(handler, call("GET", ["m"], user=MALLORY), 404, Codes.NOT_FOUND)
    slow.callback(None)
    await settled(handler)
    assert requests(upstream) == [("POST", "new"), ("GET", "m"), ("PUT", "m/tracks/close")]
    await expect_error(handler, call("GET", ["m"], user=MALLORY), 404, Codes.NOT_FOUND)
    assert sorted(rows(handler._api)) == ["m3", "mu"]


@pytest_twisted.ensureDeferred
async def test_an_oldest_session_that_will_not_close_is_retried_later(monkeypatch):
    now = [0.0]
    handler, upstream = await at_the_cap(monkeypatch, clock=lambda: now[0])
    upstream.reply(201, body=opened("m3"))
    upstream.reply(503, body=b"{}")
    assert (await at_once(handler, call("POST", ["new"], user=MALLORY))).status == 201
    await settled(handler)
    assert sorted(rows(handler._api)) == ["m", "m3", "mu"]
    upstream.calls.clear()
    assert (await handler(call("GET", ["m"], user=MALLORY))).status == 200
    upstream.reply(201, body=opened("m4"))
    assert (await at_once(handler, call("POST", ["new"], user=MALLORY))).status == 201
    await settled(handler)
    assert requests(upstream) == [("GET", "m"), ("POST", "new"), ("GET", "mu")]
    upstream.calls.clear()
    now[0] = 60.0
    upstream.reply(200, body=ACTIVE)
    upstream.reply(200, body=CLOSED)
    await handler.sweep()
    await settled(handler)
    assert requests(upstream) == [("GET", "m"), ("PUT", "m/tracks/close")]
    assert sorted(rows(handler._api)) == ["m3", "m4"]


@pytest_twisted.ensureDeferred
async def test_a_close_that_keeps_failing_is_retried_less_often(monkeypatch):
    now = [0.0]
    handler, upstream = await at_the_cap(monkeypatch, clock=lambda: now[0])
    upstream.reply(201, body=opened("m3"))
    upstream.reply(503, body=b"{}")
    await handler(call("POST", ["new"], user=MALLORY))
    await settled(handler)
    upstream.calls.clear()
    for at, retried in ((59.9, False), (60.0, True), (179.9, False), (180.0, True)):
        now[0] = at
        upstream.reply(503, body=b"{}")
        await handler.sweep()
        await settled(handler)
        assert requests(upstream) == ([("GET", "m")] if retried else [])
        upstream.calls.clear()
        upstream.replies.clear()


@pytest_twisted.ensureDeferred
async def test_a_session_aging_out_during_its_retry_goes_with_its_retry(monkeypatch):
    now, wall = [0.0], [NOW_MS]
    handler, upstream = await at_the_cap(
        monkeypatch, clock=lambda: now[0], wall_clock=lambda: wall[0] / 1000
    )
    upstream.reply(201, body=opened("m3"))
    upstream.reply(503, body=b"{}")
    await handler(call("POST", ["new"], user=MALLORY))
    await settled(handler)
    held: Deferred[None] = Deferred()
    upstream = slowed(handler, held)
    upstream.reply(503, body=b"{}")
    now[0] = 60.0
    await handler.sweep()
    wall[0] = NOW_MS + 7 * 86_400_000 + 1
    await handler.sweep()
    held.callback(None)
    await settled(handler)
    assert requests(upstream) == [("GET", "m")]
    assert rows(handler._api) == []
    assert handler._retry_at == {}


@pytest_twisted.ensureDeferred
async def test_a_background_close_waits_out_the_sessions_request_in_flight(monkeypatch):
    handler, _ = await at_the_cap(monkeypatch)
    held: Deferred[None] = Deferred()
    upstream = slowed(handler, held)
    upstream.reply(201, body=opened("m3"))
    upstream.reply()
    upstream.reply(200, body=ACTIVE)
    upstream.reply(200, body=CLOSED)
    in_flight = ensureDeferred(handler(call("POST", ["m", "tracks", "new"], user=MALLORY)))
    await at_once(handler, call("POST", ["new"], user=MALLORY))
    await deferLater(reactor, 0.12, lambda: None)
    assert requests(upstream) == [("POST", "new")]
    held.callback(None)
    assert (await in_flight).status == 200
    await settled(handler)
    assert requests(upstream) == [
        ("POST", "new"),
        ("POST", "m/tracks/new"),
        ("GET", "m"),
        ("PUT", "m/tracks/close"),
    ]


@pytest_twisted.ensureDeferred
async def test_two_opens_while_the_oldest_closes_close_each_session_once(monkeypatch):
    handler, _ = await at_the_cap(monkeypatch)
    slow: Deferred[None] = Deferred()
    upstream = slowed(handler, succeed(None), slow)
    upstream.reply(201, body=opened("m3"))
    upstream.reply(201, body=opened("m4"))
    upstream.reply(200, body=ACTIVE)
    upstream.reply(200, body=CLOSED)
    assert (await at_once(handler, call("POST", ["new"], user=MALLORY))).status == 201
    assert (await at_once(handler, call("POST", ["new"], user=MALLORY))).status == 201
    slow.callback(None)
    await settled(handler)
    assert requests(upstream) == [
        ("POST", "new"),
        ("POST", "new"),
        ("GET", "m"),
        ("PUT", "m/tracks/close"),
        ("GET", "mu"),
    ]
    assert sorted(rows(handler._api)) == ["m3", "m4"]


@pytest_twisted.ensureDeferred
async def test_a_removal_during_a_background_close_closes_every_session_in_the_call(
    monkeypatch,
):
    handler, _ = await at_the_cap(monkeypatch)
    slow: Deferred[None] = Deferred()
    upstream = slowed(handler, succeed(None), slow)
    upstream.reply(201, body=opened("m3"))
    upstream.reply(200, body=ACTIVE)
    upstream.reply(200, body=CLOSED)
    await at_once(handler, call("POST", ["new"], user=MALLORY))
    reply = await remover(handler, handler._api)(removing())
    assert reply.status == 200
    assert requests(upstream) == [
        ("POST", "new"),
        ("GET", "m"),
        ("PUT", "m/tracks/close"),
        ("GET", "mu"),
        ("GET", "m3"),
    ]
    slow.callback(None)
    await settled(handler)
    assert requests(upstream)[-1] == ("GET", "m")
    assert sorted(rows(handler._api)) == ["m3", "mu"]


LATER = NOW_MS + 86_400_000


def entry(expires_ts, call_id=CALL_ID):
    return {"call_id": call_id, "device_id": "D", "expires_ts": expires_ts}


def membership(api, user, content):
    if content is None:
        del api.rooms[ROOM][("m.call.member", user)]
    else:
        api.rooms[ROOM][("m.call.member", user)] = FakeEvent(content)


@pytest.mark.parametrize(
    "content", [None, {}, {"memberships": []}, {"memberships": [entry(LATER, "other")]}]
)
@pytest_twisted.ensureDeferred
async def test_a_session_whose_call_its_owner_left_is_closed(content):
    api = a_call(LATER)
    now = [NOW_MS]
    upstream = FakeUpstream()
    handler = make(upstream, api, wall_clock=lambda: now[0] / 1000)
    await open_session(handler, upstream, "a", ALICE, IN_CALL)
    await open_session(handler, upstream, "b", BOB, IN_CALL)
    membership(api, ALICE, content)
    now[0] = NOW_MS + 60_000
    upstream.reply(200, body=ACTIVE)
    upstream.reply(200, body=CLOSED)
    await handler.sweep()
    await settled(handler)
    assert requests(upstream) == [("GET", "a"), ("PUT", "a/tracks/close")]
    await expect_error(handler, call("GET", ["a"]), 404, Codes.NOT_FOUND)
    assert rows(api) == ["b"]


@pytest.mark.parametrize(
    "age_ms, content, query",
    [
        (59_999, None, IN_CALL),
        (3_600_000, {"memberships": [entry(LATER)]}, IN_CALL),
        (3_600_000, {"memberships": [entry(NOW_MS + 60_000)]}, IN_CALL),
        (3_600_000, None, None),
    ],
)
@pytest_twisted.ensureDeferred
async def test_a_session_whose_call_its_owner_may_still_be_in_is_kept(age_ms, content, query):
    api = a_call(LATER)
    membership(api, ALICE, content)
    now = [NOW_MS]
    upstream = FakeUpstream()
    handler = make(upstream, api, wall_clock=lambda: now[0] / 1000)
    await open_session(handler, upstream, "a", ALICE, query)
    now[0] = NOW_MS + age_ms
    await handler.sweep()
    await settled(handler)
    assert upstream.calls == []
    assert (await handler(call("GET", ["a"]))).status == 200


@pytest_twisted.ensureDeferred
async def test_a_session_past_the_maximum_age_is_forgotten_without_a_restart():
    api = a_call()
    now = [NOW_MS]
    upstream = FakeUpstream()
    handler = make(upstream, api, wall_clock=lambda: now[0] / 1000)
    await open_session(handler, upstream, "a", ALICE)
    now[0] = NOW_MS + 7 * 86_400_000 + 1
    await handler.sweep()
    await settled(handler)
    assert upstream.calls == []
    await expect_error(handler, call("GET", ["a"]), 404, Codes.NOT_FOUND)
    assert rows(api) == []


@pytest_twisted.ensureDeferred
async def test_a_process_that_served_no_call_sweeps_nothing():
    api = a_call()
    upstream = FakeUpstream()
    await open_session(make(upstream, api), upstream, "a", ALICE, IN_CALL)
    idle = make(upstream, api, now_ms=NOW_MS + 8 * 86_400_000)
    await idle.sweep()
    await settled(idle)
    assert upstream.calls == []
    assert rows(api) == ["a"]


@pytest_twisted.ensureDeferred
async def test_a_restart_forgets_no_session():
    api = a_call()
    upstream = FakeUpstream()
    before = make(upstream, api)
    for session_id, user in (("a", ALICE), ("m", MALLORY)):
        await open_session(before, upstream, session_id, user, IN_CALL)
    after = make(upstream, api)
    upstream.reply(200, body=ACTIVE)
    upstream.reply(200, body=b'{"tracks":[{"mid":"0"}]}')
    assert (await remover(after, api)(removing())).status == 200
    assert requests(upstream) == [("GET", "m"), ("PUT", "m/tracks/close")]
    assert (await after(call("GET", ["a"]))).status == 200
    reply = await after(pull("a", ("m", "audio"), user=ALICE))
    assert json.loads(reply.body)["tracks"][0]["errorCode"] == "IM.ZUNO.CALL_REMOVED"


@pytest_twisted.ensureDeferred
async def test_a_session_past_the_maximum_age_is_dropped_on_load():
    api = a_call()
    upstream = FakeUpstream()
    await open_session(make(upstream, api), upstream, "a", ALICE, IN_CALL)
    late = make(upstream, api, now_ms=NOW_MS + 7 * 86_400_000 + 1)
    await expect_error(late, call("GET", ["a"]), 404, Codes.NOT_FOUND)
    assert api.db.execute("SELECT COUNT(*) FROM zuno_calls_cloudflare_sessions").fetchone() == (0,)
