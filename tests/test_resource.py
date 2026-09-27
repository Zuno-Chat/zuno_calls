import pytest
import pytest_twisted
from conftest import FakeModuleApi, FakeRequest, as_api, authed
from prometheus_client import REGISTRY
from synapse.module_api.errors import Codes, SynapseError

from zuno_calls.ratelimit import RateLimiter
from zuno_calls.resource import Call, Reply, ZunoResource, match, reserved_words
from zuno_calls.upstream import UpstreamUnavailable

ROUTES = (
    ("POST", ("new",)),
    ("GET", ("{sessionId}",)),
    ("PUT", ("{sessionId}", "tracks", "close")),
)


def test_match_patterns():
    assert match(ROUTES, "POST", ("new",))
    assert match(ROUTES, "GET", ("abc123",))
    assert match(ROUTES, "PUT", ("abc123", "tracks", "close"))
    assert not match(ROUTES, "GET", ("new",))
    assert not match(ROUTES, "POST", ("new", ""))
    assert not match(ROUTES, "POST", ())
    assert not match(ROUTES, "PUT", ("abc123", "tracks", "open"))
    assert not match(ROUTES, "DELETE", ("abc123",))


def test_a_wildcard_never_matches_an_empty_or_slashed_segment():
    assert not match(ROUTES, "GET", ("",))
    assert not match(ROUTES, "GET", ("a/b",))


def test_reserved_words_are_the_literal_segments():
    assert reserved_words(ROUTES) == frozenset({"new", "tracks", "close"})
    assert match(ROUTES, "GET", ("abc123",), reserved_words(ROUTES))


class Recorder:
    def __init__(self, reply: Reply | Exception | None = None) -> None:
        self.calls: list[Call] = []
        self.reply = reply or Reply(201, "application/json", b'{"ok":true}')

    async def __call__(self, call: Call) -> Reply:
        self.calls.append(call)
        if isinstance(self.reply, Exception):
            raise self.reply
        return self.reply


def resource(handler=None, *, per_second=100.0, burst=100, **kw):
    handler = handler or Recorder()
    return ZunoResource(
        as_api(FakeModuleApi()), RateLimiter(per_second, burst), handler, routes=ROUTES, **kw
    ), handler


async def expect_error(res, req, code, errcode):
    with pytest.raises(SynapseError) as exc:
        await res._serve(req)
    assert (exc.value.code, exc.value.errcode) == (code, errcode)
    return exc.value


@pytest_twisted.ensureDeferred
async def test_unknown_route_is_404_before_auth():
    res, handler = resource()
    await expect_error(res, FakeRequest("GET", ["new"]), 404, Codes.UNRECOGNIZED)
    assert handler.calls == []


@pytest_twisted.ensureDeferred
async def test_missing_and_unknown_tokens_are_401():
    res, _ = resource()
    await expect_error(res, FakeRequest("POST", ["new"]), 401, Codes.MISSING_TOKEN)
    await expect_error(res, authed("POST", ["new"], token="nope"), 401, Codes.UNKNOWN_TOKEN)


@pytest_twisted.ensureDeferred
async def test_happy_path_builds_call_and_relays_reply():
    res, handler = resource()
    req = authed("PUT", ["abc", "tracks", "close"], b'{"tracks":[]}')
    await res._serve(req)
    assert handler.calls == [
        Call(
            "PUT",
            ("abc", "tracks", "close"),
            "application/json",
            b'{"tracks":[]}',
            "@alice:zuno.chat",
            "DEV1",
        )
    ]
    assert (req.code, req.written, req.finished) == (201, b'{"ok":true}', True)
    assert req.response_headers == {"content-type": "application/json", "content-length": "11"}


@pytest_twisted.ensureDeferred
async def test_a_204_carries_no_content_type_length_or_body():
    res, _ = resource(Recorder(Reply(204, None, b"")))
    req = authed("POST", ["new"])
    await res._serve(req)
    assert (req.code, req.response_headers) == (204, {})
    assert (req.written, req.finished) == (b"", True)


@pytest_twisted.ensureDeferred
async def test_rate_limit_is_per_device_and_answers_429():
    res, handler = resource(per_second=1, burst=1)
    await res._serve(authed("POST", ["new"]))
    before = REGISTRY.get_sample_value("zuno_calls_rate_limited_total") or 0.0
    err = await expect_error(res, authed("POST", ["new"]), 429, Codes.LIMIT_EXCEEDED)
    assert (REGISTRY.get_sample_value("zuno_calls_rate_limited_total") or 0.0) - before == 1
    assert err.error_dict(None)["retry_after_ms"] == 1000
    assert err.headers == {"Retry-After": "1"}
    await res._serve(authed("POST", ["new"], token="tok-alice-2"))
    assert len(handler.calls) == 2


@pytest_twisted.ensureDeferred
async def test_token_without_device_keys_on_user():
    res, handler = resource(per_second=1, burst=1)
    await res._serve(authed("POST", ["new"], token="tok-bot"))
    assert handler.calls[0].device_id is None
    await expect_error(res, authed("POST", ["new"], token="tok-bot"), 429, Codes.LIMIT_EXCEEDED)


@pytest_twisted.ensureDeferred
async def test_oversized_body_is_413():
    res, handler = resource(max_body=8)
    await expect_error(res, authed("POST", ["new"], b"123456789"), 413, Codes.TOO_LARGE)
    assert handler.calls == []
    await res._serve(authed("POST", ["new"], b"12345678"))
    assert handler.calls[0].body == b"12345678"


@pytest_twisted.ensureDeferred
async def test_read_body_false_ignores_the_body():
    res, handler = resource(read_body=False, max_body=1)
    await res._serve(authed("POST", ["new"], b"a large body that is never read"))
    assert handler.calls[0].body == b""


@pytest_twisted.ensureDeferred
async def test_upstream_unavailable_is_502():
    res, _ = resource(Recorder(UpstreamUnavailable("timeout")))
    err = await expect_error(res, authed("POST", ["new"]), 502, Codes.UNKNOWN)
    assert err.msg == "calls service unavailable"


@pytest_twisted.ensureDeferred
async def test_disconnected_client_gets_nothing_written():
    res, _ = resource()
    req = authed("POST", ["new"])
    req._disconnected = True
    await res._serve(req)
    assert (req.written, req.finished) == (b"", False)


@pytest.mark.parametrize("bad", ["text/☃", "text/plain\n", "text/plain\x00", " text/plain"])
@pytest_twisted.ensureDeferred
async def test_unrelayable_content_type_reaches_the_handler_as_none(bad):
    res, handler = resource()
    await res._serve(authed("POST", ["new"], content_type=bad))
    assert handler.calls[0].content_type is None


@pytest_twisted.ensureDeferred
async def test_empty_segment_is_404():
    res, handler = resource()
    await expect_error(res, authed("GET", [""]), 404, Codes.UNRECOGNIZED)
    assert handler.calls == []


@pytest_twisted.ensureDeferred
async def test_unsupported_verbs_are_404_through_the_real_dispatcher():
    res, handler = resource()
    assert res.isLeaf
    for method, segments in (("DELETE", ["abc"]), ("PATCH", ["new"])):
        with pytest.raises(SynapseError) as exc:
            await res._async_render(authed(method, segments))
        assert (exc.value.code, exc.value.errcode) == (404, Codes.UNRECOGNIZED)
    assert handler.calls == []


@pytest_twisted.ensureDeferred
async def test_async_render_dispatches_to_serve():
    res, _ = resource()
    req = authed("POST", ["new"])
    await res._async_render(req)
    assert (req.code, req.written, req.finished) == (201, b'{"ok":true}', True)
