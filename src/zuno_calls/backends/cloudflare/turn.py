"""Cloudflare TURN credential mint. The TTL is the module's, never the client's."""

from __future__ import annotations

import json

from ...config import CloudflareConfig
from ...resource import Call, Reply, Route
from ...upstream import TURN_RETRY, Upstream

ROUTES: tuple[Route, ...] = (("POST", ()),)


class TurnHandler:
    def __init__(self, cfg: CloudflareConfig, upstream: Upstream) -> None:
        self._url = (
            f"{cfg.base_url}/v1/turn/keys/{cfg.turn_key_id}/credentials/generate-ice-servers"
        )
        self._headers = {
            "Authorization": f"Bearer {cfg.turn_api_token}",
            "Content-Type": "application/json",
        }
        self._body = json.dumps({"ttl": int(cfg.turn_credential_ttl)}).encode("ascii")
        self._timeout = cfg.turn_timeout
        self._upstream = upstream

    async def __call__(self, call: Call) -> Reply:
        response = await self._upstream.request(
            "POST",
            self._url,
            self._headers,
            self._body,
            api="turn",
            retry=TURN_RETRY,
            timeout=self._timeout,
        )
        return Reply.relay(response)
