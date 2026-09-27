"""The module inside a real Synapse. Needs Docker; run with `make e2e`.

Runs synapse:v1.161.0 with the module installed on the host network (SQLite)
against a stub Cloudflare, registers a user through the shared-secret admin
endpoint and exercises the routes through Synapse's own auth. Both ports are
overridable so two runs can share a machine.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import shutil
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

pytestmark = pytest.mark.e2e

ROOT = Path(__file__).resolve().parents[2]
SYNAPSE_PORT = int(os.environ.get("ZUNO_E2E_SYNAPSE_PORT", "18008"))
STUB_PORT = int(os.environ.get("ZUNO_E2E_STUB_PORT", "18100"))
# Image and container are per port so two runs never race on the build.
IMAGE = f"zuno-calls-e2e-{SYNAPSE_PORT}"
CONTAINER = f"zuno-calls-e2e-{SYNAPSE_PORT}"
SYNAPSE = f"http://127.0.0.1:{SYNAPSE_PORT}"
SFU = f"{SYNAPSE}/_synapse/client/zuno/calls/cloudflare/sessions"
TURN = f"{SYNAPSE}/_synapse/client/zuno/calls/cloudflare/turn/credentials"
SHARED_SECRET = "e2e-registration-secret"

HOMESERVER = f"""\
server_name: e2e
report_stats: false
pid_file: /data/homeserver.pid
signing_key_path: /data/signing.key
media_store_path: /data/media
database:
  name: sqlite3
  args:
    database: /data/homeserver.db
listeners:
  - port: {SYNAPSE_PORT}
    type: http
    bind_addresses: ['127.0.0.1']
    resources:
      - names: [client]
registration_shared_secret: {SHARED_SECRET}
macaroon_secret_key: e2e-macaroon
form_secret: e2e-form
modules:
  - module: zuno_calls.ZunoCalls
    config:
      cloudflare:
        app_id: APP
        app_secret: SECRET
        turn_key_id: KEY
        turn_api_token: TOKEN
        base_url: http://127.0.0.1:{STUB_PORT}
      rate_limit:
        per_second: 1
        burst: 3
"""


class Stub(BaseHTTPRequestHandler):
    seen: list[dict] = []

    def _answer(self) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length)
        Stub.seen.append(
            {
                "method": self.command,
                "path": self.path,
                "authorization": self.headers.get("Authorization"),
                "body": body,
            }
        )
        if self.path.endswith("/generate-ice-servers"):
            payload = (
                b'{"iceServers":[{"urls":["turn:turn.example:3478"],'
                b'"username":"u","credential":"c"}]}'
            )
        else:
            payload = b'{"sessionId":"s1","sessionDescription":{"type":"answer","sdp":"v=0"}}'
        self.send_response(201)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    do_GET = do_POST = do_PUT = _answer

    def log_message(self, *args) -> None:
        pass


def docker(*args: str, check: bool = True, **kw):
    return subprocess.run(["docker", *args], check=check, text=True, capture_output=True, **kw)


def http(method: str, url: str, body=None, token: str | None = None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read() or b"null")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"null")


def register_user() -> str:
    _, nonce = http("GET", f"{SYNAPSE}/_synapse/admin/v1/register")
    mac = hmac.new(SHARED_SECRET.encode(), digestmod=hashlib.sha1)
    mac.update(b"\x00".join([nonce["nonce"].encode(), b"alice", b"wonderland", b"notadmin"]))
    status, body = http(
        "POST",
        f"{SYNAPSE}/_synapse/admin/v1/register",
        {
            "nonce": nonce["nonce"],
            "username": "alice",
            "password": "wonderland",
            "admin": False,
            "mac": mac.hexdigest(),
        },
    )
    assert status == 200, body
    return str(body["access_token"])


def wait_for_synapse(deadline: float) -> None:
    while True:
        try:
            urllib.request.urlopen(f"{SYNAPSE}/health", timeout=2)
            return
        except Exception:
            if time.time() > deadline:
                logs = docker("logs", CONTAINER, check=False)
                raise RuntimeError(
                    "synapse did not become healthy:\n" + logs.stdout + logs.stderr
                ) from None
            time.sleep(1)


@pytest.fixture(scope="module")
def token():
    if shutil.which("docker") is None:
        pytest.skip("docker not on PATH")
    stub = ThreadingHTTPServer(("127.0.0.1", STUB_PORT), Stub)
    threading.Thread(target=stub.serve_forever, daemon=True).start()
    data = Path(tempfile.mkdtemp(prefix="zuno-calls-e2e-"))
    (data / "homeserver.yaml").write_text(HOMESERVER)
    as_user = ["--user", f"{os.getuid()}:{os.getgid()}", "-v", f"{data}:/data"]
    homeserver = [
        "--entrypoint",
        "python",
        IMAGE,
        "-m",
        "synapse.app.homeserver",
        "--config-path",
        "/data/homeserver.yaml",
    ]
    try:
        docker("build", "-f", "tests/e2e/Dockerfile", "-t", IMAGE, ".", cwd=ROOT)
        docker("rm", "-f", CONTAINER, check=False)
        docker("run", "--rm", *as_user, *homeserver, "--generate-keys")
        docker("run", "-d", "--name", CONTAINER, "--network", "host", *as_user, *homeserver)
        wait_for_synapse(time.time() + 90)
        yield register_user()
    finally:
        docker("rm", "-f", CONTAINER, check=False)
        stub.shutdown()
        stub.server_close()
        shutil.rmtree(data, ignore_errors=True)


def test_missing_token_is_401(token):
    status, body = http(
        "POST", f"{SFU}/new", {"sessionDescription": {"type": "offer", "sdp": "v=0"}}
    )
    assert (status, body["errcode"]) == (401, "M_MISSING_TOKEN")


def test_unknown_route_is_404(token):
    status, body = http("GET", f"{SFU}/new", token=token)
    assert (status, body["errcode"]) == (404, "M_UNRECOGNIZED")


def test_sfu_round_trip(token):
    Stub.seen.clear()
    offer = {"sessionDescription": {"type": "offer", "sdp": "v=0"}}
    status, body = http("POST", f"{SFU}/new", offer, token=token)
    assert (status, body["sessionId"]) == (201, "s1")
    assert Stub.seen == [
        {
            "method": "POST",
            "path": "/v1/apps/APP/sessions/new",
            "authorization": "Bearer SECRET",
            "body": json.dumps(offer).encode(),
        }
    ]


def test_turn_round_trip(token):
    Stub.seen.clear()
    status, body = http("POST", TURN, {"ttl": 999}, token=token)
    assert (status, body["iceServers"][0]["username"]) == (201, "u")
    assert Stub.seen == [
        {
            "method": "POST",
            "path": "/v1/turn/keys/KEY/credentials/generate-ice-servers",
            "authorization": "Bearer TOKEN",
            "body": json.dumps({"ttl": 7200}).encode(),
        }
    ]


# burst 3 against 6 attempts: the margin holds whether or not earlier tests spent tokens.
def test_burst_is_rate_limited(token):
    codes = [http("POST", TURN, {}, token=token)[0] for _ in range(6)]
    assert 429 in codes
