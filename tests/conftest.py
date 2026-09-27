from __future__ import annotations

from dataclasses import dataclass, field
from io import BytesIO
from typing import cast

from synapse.module_api import ModuleApi
from synapse.module_api.errors import Codes, SynapseError

from zuno_calls.upstream import NO_RETRY, RetryPolicy, Upstream, UpstreamResponse


@dataclass
class FakeUser:
    id: str

    def to_string(self) -> str:
        return self.id


@dataclass
class FakeRequester:
    user: FakeUser
    device_id: str | None = None


ALICE = FakeRequester(FakeUser("@alice:zuno.chat"), "DEV1")
ALICE_2 = FakeRequester(FakeUser("@alice:zuno.chat"), "DEV2")
APPSERVICE = FakeRequester(FakeUser("@bot:zuno.chat"), None)


class FakeModuleApi:
    """The two ModuleApi calls the module makes, backed by a token table."""

    def __init__(self) -> None:
        self.tokens = {"tok-alice": ALICE, "tok-alice-2": ALICE_2, "tok-bot": APPSERVICE}
        self.registered: list[tuple[str, object]] = []

    def register_web_resource(self, path: str, resource: object) -> None:
        self.registered.append((path, resource))

    async def get_user_by_req(self, request, allow_guest=False, allow_expired=False):
        auth = request.getHeader("Authorization") or ""
        token = auth.removeprefix("Bearer ").strip()
        if not token:
            raise SynapseError(401, "Missing access token", Codes.MISSING_TOKEN)
        try:
            return self.tokens[token]
        except KeyError:
            raise SynapseError(401, "Unrecognised access token", Codes.UNKNOWN_TOKEN) from None


class FakeRequest:
    """The slice of SynapseRequest that ZunoResource touches."""

    def __init__(
        self,
        method: str,
        postpath: list[str],
        body: bytes = b"",
        headers: dict[str, str] | None = None,
    ) -> None:
        self.method = method.encode()
        self.postpath = [p.encode() for p in postpath]
        self.content = BytesIO(body)
        self._headers = {k.lower(): v for k, v in (headers or {}).items()}
        self.code = 200
        self.response_headers: dict[str, str] = {}
        self.written = b""
        self.finished = False
        self._disconnected = False

    def getHeader(self, name: str) -> str | None:
        return self._headers.get(name.lower())

    def setResponseCode(self, code: int, message: bytes | None = None) -> None:
        self.code = code

    def setHeader(self, name: bytes | str, value: bytes | str) -> None:
        key = name.decode() if isinstance(name, bytes) else name
        val = value.decode() if isinstance(value, bytes) else value
        self.response_headers[key.lower()] = val

    def write(self, data: bytes) -> None:
        self.written += data

    def finish(self) -> None:
        self.finished = True


def authed(
    method: str,
    postpath: list[str],
    body: bytes = b"",
    token: str = "tok-alice",
    content_type: str | None = "application/json",
) -> FakeRequest:
    headers = {"Authorization": f"Bearer {token}"}
    if content_type:
        headers["Content-Type"] = content_type
    return FakeRequest(method, postpath, body, headers)


DEFAULT_REPLY = UpstreamResponse(200, "application/json", b"{}")


@dataclass
class FakeUpstream:
    """Records what a handler sent and answers from `replies`, DEFAULT_REPLY once they run out."""

    calls: list[dict] = field(default_factory=list)
    replies: list[UpstreamResponse] = field(default_factory=list)

    def reply(
        self, status: int = 200, content_type: str | None = "application/json", body: bytes = b"{}"
    ) -> None:
        self.replies.append(UpstreamResponse(status, content_type, body))

    async def request(
        self,
        method,
        url,
        headers,
        body,
        *,
        api,
        retry: RetryPolicy = NO_RETRY,
        timeout: float | None = None,
    ):
        self.calls.append(
            {
                "method": method,
                "url": url,
                "headers": dict(headers),
                "body": body,
                "api": api,
                "retry": retry,
                "timeout": timeout,
            }
        )
        return self.replies.pop(0) if self.replies else DEFAULT_REPLY


def as_api(fake: FakeModuleApi) -> ModuleApi:
    """The fakes implement the slice the module uses; the cast keeps arg-type checked elsewhere."""
    return cast(ModuleApi, fake)


def as_upstream(fake: FakeUpstream) -> Upstream:
    return cast(Upstream, fake)
