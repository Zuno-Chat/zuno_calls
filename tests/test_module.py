from conftest import FakeModuleApi, as_api

from zuno_calls import REMOVE_PATH, SFU_PATH, TURN_PATH, ZunoCalls, removal
from zuno_calls.backends.cloudflare import sfu, turn
from zuno_calls.resource import ZunoResource

CONFIG = {
    "cloudflare": {"app_id": "a", "app_secret": "s", "turn_key_id": "k", "turn_api_token": "t"}
}


def test_paths():
    assert SFU_PATH == "/_synapse/client/zuno/calls/cloudflare/sessions"
    assert TURN_PATH == "/_synapse/client/zuno/calls/cloudflare/turn/credentials"
    assert REMOVE_PATH == "/_synapse/client/zuno/calls/remove"
    paths = (SFU_PATH, TURN_PATH, REMOVE_PATH)
    # Leaves at disjoint prefixes.
    assert not any(a.startswith(b + "/") for a in paths for b in paths)


def test_registers_every_resource():
    api = FakeModuleApi()
    ZunoCalls(ZunoCalls.parse_config(CONFIG), as_api(api))
    assert [path for path, _ in api.registered] == [SFU_PATH, TURN_PATH, REMOVE_PATH]
    sfu_res, turn_res, remove_res = (res for _, res in api.registered)
    assert isinstance(sfu_res, ZunoResource) and sfu_res.isLeaf
    assert isinstance(turn_res, ZunoResource) and turn_res.isLeaf
    assert isinstance(remove_res, ZunoResource) and remove_res.isLeaf
    # Synapse labels synapse_http_server_* by resource class name: one per backend.
    assert type(sfu_res).__name__ == "SFUResource"
    assert type(turn_res).__name__ == "TurnResource"
    assert type(remove_res).__name__ == "RemoveResource"
    assert sfu_res._routes == sfu.ROUTES
    assert turn_res._routes == turn.ROUTES
    assert remove_res._routes == removal.ROUTES
    assert turn_res._read_body is False
    assert sfu_res._limiter is turn_res._limiter is remove_res._limiter
    assert isinstance(sfu_res._handler, sfu.SFUHandler) and sfu_res._handler._max == 6
    assert isinstance(remove_res._handler, removal.RemoveHandler)
    assert remove_res._handler._removals is sfu_res._handler._removals
    assert remove_res._handler._cut_offs == (sfu_res._handler.cut_off,)


def test_without_cloudflare_section_nothing_is_registered():
    api = FakeModuleApi()
    ZunoCalls(ZunoCalls.parse_config({}), as_api(api))
    assert api.registered == []
