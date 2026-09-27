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


def call(method, segments, body=b'{"x":1}', content_type="application/json"):
    return Call(method, tuple(segments), content_type, body, "@alice:zuno.chat", "DEV1")


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
    upstream.reply(201, "application/json", b'{"sessionId":"abc"}')
    reply = await SFUHandler(CFG, as_upstream(upstream))(call(method, segments))
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
    with pytest.raises(SynapseError) as exc:
        await SFUHandler(CFG, as_upstream(upstream))(call("PUT", [bad, "renegotiate"]))
    assert (exc.value.code, exc.value.errcode) == (400, Codes.INVALID_PARAM)
    assert upstream.calls == []


@pytest_twisted.ensureDeferred
async def test_cloudflare_error_status_is_relayed():
    upstream = FakeUpstream()
    upstream.reply(400, "application/json", b'{"errorCode":"bad"}')
    reply = await SFUHandler(CFG, as_upstream(upstream))(call("POST", ["new"]))
    assert (reply.status, reply.body) == (400, b'{"errorCode":"bad"}')


@pytest_twisted.ensureDeferred
async def test_wired_resource_rejects_off_list_routes():
    upstream = FakeUpstream()
    res = ZunoResource(
        as_api(FakeModuleApi()),
        RateLimiter(100, 100),
        SFUHandler(CFG, as_upstream(upstream)),
        routes=ROUTES,
    )
    for method, segments in (("GET", ["new"]), ("DELETE", ["abc"])):
        with pytest.raises(SynapseError) as exc:
            await res._serve(authed(method, segments))
        assert exc.value.code == 404
    req = authed("POST", ["abc", "tracks", "new"], b"{}")
    await res._serve(req)
    assert req.code == 200
    assert upstream.calls[0]["url"].endswith("/sessions/abc/tracks/new")
