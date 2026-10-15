import json

import pytest
import pytest_twisted
from conftest import FakeModuleApi, FakeUpstream, as_api, as_upstream, authed
from synapse.module_api.errors import Codes, SynapseError

from zuno_calls.backends.cloudflare.sfu import ROUTES, SFUHandler
from zuno_calls.config import CloudflareConfig
from zuno_calls.ratelimit import RateLimiter
from zuno_calls.resource import Call, ZunoResource
from zuno_calls.upstream import NO_RETRY

CFG = CloudflareConfig(
    app_id="APP",
    app_secret="SECRET",
    turn_key_id="K",
    turn_api_token="T",
    base_url="https://cf.test",
)
ALICE = "@alice:zuno.chat"


def call(method, segments, body=b'{"x":1}', content_type="application/json", user=ALICE):
    return Call(method, tuple(segments), content_type, body, user, "DEV1")


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
    handler = SFUHandler(CFG, as_upstream(upstream))
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
    await SFUHandler(CFG, as_upstream(upstream))(call("POST", ["new"]))
    assert set(upstream.calls[0]["headers"]) == {"Authorization", "Content-Type"}
    assert upstream.calls[0]["headers"]["Authorization"] == "Bearer SECRET"


@pytest_twisted.ensureDeferred
async def test_missing_content_type_is_not_invented():
    upstream = FakeUpstream()
    await SFUHandler(CFG, as_upstream(upstream))(call("POST", ["new"], content_type=None))
    assert "Content-Type" not in upstream.calls[0]["headers"]
    assert upstream.calls[0]["body"] == b'{"x":1}'


@pytest.mark.parametrize("bad", ["a b", "..", "a/b", "x" * 129, "", "sess%ion"])
@pytest_twisted.ensureDeferred
async def test_bad_session_id_is_400(bad):
    upstream = FakeUpstream()
    handler = SFUHandler(CFG, as_upstream(upstream))
    await expect_error(handler, call("PUT", [bad, "renegotiate"]), 400, Codes.INVALID_PARAM)
    assert upstream.calls == []


@pytest_twisted.ensureDeferred
async def test_another_users_session_is_404():
    upstream = FakeUpstream()
    handler = SFUHandler(CFG, as_upstream(upstream))
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
    handler = SFUHandler(CFG, as_upstream(upstream))
    upstream.reply(status, "application/json", body)
    await handler(call("POST", ["new"]))
    await expect_error(handler, call("GET", ["abc"]), 404, Codes.NOT_FOUND)
    assert len(upstream.calls) == 1


@pytest_twisted.ensureDeferred
async def test_past_the_cap_a_user_loses_only_their_own_oldest_session(monkeypatch):
    monkeypatch.setattr("zuno_calls.backends.cloudflare.sfu._SESSIONS_PER_USER", 2)
    upstream = FakeUpstream()
    handler = SFUHandler(CFG, as_upstream(upstream))
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
    reply = await SFUHandler(CFG, as_upstream(upstream))(call("POST", ["new"]))
    assert (reply.status, reply.body) == (400, b'{"errorCode":"bad"}')


@pytest_twisted.ensureDeferred
async def test_wired_resource_serves_listed_routes_on_own_sessions_only():
    upstream = FakeUpstream()
    res = ZunoResource(
        as_api(FakeModuleApi()),
        RateLimiter(100, 100),
        SFUHandler(CFG, as_upstream(upstream)),
        routes=ROUTES,
    )
    for method, segments in (("GET", ["new"]), ("DELETE", ["abc"])):
        with pytest.raises(SynapseError) as exc:
            await res._async_render(authed(method, segments))
        assert exc.value.code == 404
    upstream.reply(201, "application/json", b'{"sessionId":"abc"}')
    await res._async_render(authed("POST", ["new"], b"{}"))
    req = authed("POST", ["abc", "tracks", "new"], b"{}")
    await res._async_render(req)
    assert req.code == 200
    assert upstream.calls[1]["url"].endswith("/sessions/abc/tracks/new")
    with pytest.raises(SynapseError) as exc:
        await res._async_render(authed("GET", ["abc"], token="tok-bot"))
    assert (exc.value.code, exc.value.errcode) == (404, Codes.NOT_FOUND)
