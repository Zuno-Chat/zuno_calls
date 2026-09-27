"""Synapse module: Cloudflare Calls signaling proxy and TURN credentials for Zuno Chat.

homeserver.yaml:

    modules:
      - module: zuno_calls.ZunoCalls
        config: {...}   # see docs/design.md
"""

from __future__ import annotations

from typing import Any

from synapse.module_api import ModuleApi

from .backends.cloudflare import sfu, turn
from .config import Config, parse_config
from .ratelimit import RateLimiter
from .resource import ZunoResource
from .upstream import Upstream

# Two leaves at disjoint prefixes: a leaf swallows every path below it, so
# the TURN resource can never sit under the SFU one.
SFU_PATH = "/_synapse/client/zuno/calls/cloudflare/sessions"
TURN_PATH = "/_synapse/client/zuno/calls/cloudflare/turn/credentials"


# Synapse labels its per-request series (synapse_http_server_*) by resource
# class name, so each backend gets its own class and its own series.
class SFUResource(ZunoResource):
    pass


class TurnResource(ZunoResource):
    pass


class ZunoCalls:
    def __init__(self, config: Config, api: ModuleApi) -> None:
        if config.cloudflare is None:
            return
        limiter = RateLimiter(config.rate_limit.per_second, config.rate_limit.burst)
        upstream = Upstream(config.cloudflare.timeout)
        api.register_web_resource(
            SFU_PATH,
            SFUResource(
                api, limiter, sfu.SFUHandler(config.cloudflare, upstream), routes=sfu.ROUTES
            ),
        )
        api.register_web_resource(
            TURN_PATH,
            TurnResource(
                api,
                limiter,
                turn.TurnHandler(config.cloudflare, upstream),
                routes=turn.ROUTES,
                read_body=False,
            ),
        )

    @staticmethod
    def parse_config(config: dict[str, Any]) -> Config:
        return parse_config(config)
