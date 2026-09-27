from conftest import FakeModuleApi, as_api

from zuno_calls import SFU_PATH, TURN_PATH, ZunoCalls
from zuno_calls.backends.cloudflare import sfu, turn
from zuno_calls.resource import ZunoResource

CONFIG = {
    "cloudflare": {"app_id": "a", "app_secret": "s", "turn_key_id": "k", "turn_api_token": "t"}
}


def test_paths():
    assert SFU_PATH == "/_synapse/client/zuno/calls/cloudflare/sessions"
    assert TURN_PATH == "/_synapse/client/zuno/calls/cloudflare/turn/credentials"
    assert not TURN_PATH.startswith(SFU_PATH + "/")  # two leaves at disjoint prefixes


def test_registers_both_resources():
    api = FakeModuleApi()
    ZunoCalls(ZunoCalls.parse_config(CONFIG), as_api(api))
    assert [path for path, _ in api.registered] == [SFU_PATH, TURN_PATH]
    sfu_res, turn_res = (res for _, res in api.registered)
    assert isinstance(sfu_res, ZunoResource) and sfu_res.isLeaf
    assert isinstance(turn_res, ZunoResource) and turn_res.isLeaf
    # Synapse labels synapse_http_server_* by resource class name: one per backend.
    assert type(sfu_res).__name__ == "SFUResource"
    assert type(turn_res).__name__ == "TurnResource"
    assert sfu_res._routes == sfu.ROUTES
    assert turn_res._routes == turn.ROUTES
    assert turn_res._read_body is False
    assert sfu_res._limiter is turn_res._limiter


def test_without_cloudflare_section_nothing_is_registered():
    api = FakeModuleApi()
    ZunoCalls(ZunoCalls.parse_config({}), as_api(api))
    assert api.registered == []
