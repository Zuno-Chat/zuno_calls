import json

import pytest
import pytest_twisted
from conftest import FakeModuleApi, FakeUpstream, as_api, as_upstream, authed
from synapse.module_api.errors import SynapseError

from zuno_calls.backends.cloudflare.turn import ROUTES, TurnHandler
from zuno_calls.config import CloudflareConfig
from zuno_calls.ratelimit import RateLimiter
from zuno_calls.resource import Call, ZunoResource
from zuno_calls.upstream import TURN_RETRY

CFG = CloudflareConfig(
    app_id="APP",
    app_secret="S",
    turn_key_id="KEY",
    turn_api_token="TOKEN",
    base_url="https://cf.test",
    turn_credential_ttl=1800,
)
ICE = b'{"iceServers":[{"urls":["turn:x"],"username":"u","credential":"c"}]}'


@pytest_twisted.ensureDeferred
async def test_mints_with_config_ttl_and_relays():
    upstream = FakeUpstream()
    upstream.reply(201, "application/json", ICE)
    reply = await TurnHandler(CFG, as_upstream(upstream))(
        Call("POST", (), "application/json", b'{"ttl":999999}', "@a:x", "D")
    )
    assert (reply.status, reply.content_type, reply.body) == (201, "application/json", ICE)
    sent = upstream.calls[0]
    assert (sent["method"], sent["url"]) == (
        "POST",
        "https://cf.test/v1/turn/keys/KEY/credentials/generate-ice-servers",
    )
    assert sent["headers"] == {
        "Authorization": "Bearer TOKEN",
        "Content-Type": "application/json",
    }
    assert json.loads(sent["body"]) == {"ttl": 1800}
    assert (sent["api"], sent["retry"]) == ("turn", TURN_RETRY)
    # The mint gets its own, shorter per-attempt deadline than the SFU relay.
    assert sent["timeout"] == 5.0


@pytest_twisted.ensureDeferred
async def test_cloudflare_error_is_relayed():
    upstream = FakeUpstream()
    upstream.reply(403, "application/json", b'{"error":"nope"}')
    reply = await TurnHandler(CFG, as_upstream(upstream))(Call("POST", (), None, b"", "@a:x", "D"))
    assert (reply.status, reply.body) == (403, b'{"error":"nope"}')


@pytest_twisted.ensureDeferred
async def test_wired_resource_accepts_only_post_at_the_root():
    upstream = FakeUpstream()
    res = ZunoResource(
        as_api(FakeModuleApi()),
        RateLimiter(100, 100),
        TurnHandler(CFG, as_upstream(upstream)),
        routes=ROUTES,
        read_body=False,
    )
    for method, path in (("GET", []), ("POST", ["extra"]), ("PUT", [])):
        with pytest.raises(SynapseError) as exc:
            await res._serve(authed(method, path))
        assert exc.value.code == 404
    req = authed("POST", [], b'{"ttl":5}')
    await res._serve(req)
    assert req.code == 200 and len(upstream.calls) == 1
