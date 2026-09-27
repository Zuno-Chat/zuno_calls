# zuno_calls design

Synapse module serving the calling backend. Synapse checks the Matrix access token on every request, so there is no enrollment, no gateway token and no remote-logout window: a device logged out elsewhere is refused at once.

## Routes

| Route | Upstream |
|---|---|
| `POST /_synapse/client/zuno/calls/cloudflare/sessions/new` | `{base_url}/v1/apps/{app_id}/sessions/new` |
| `GET  .../sessions/{sessionId}` | same suffix |
| `POST .../sessions/{sessionId}/tracks/new` | same suffix |
| `PUT  .../sessions/{sessionId}/renegotiate` | same suffix |
| `PUT  .../sessions/{sessionId}/tracks/close` | same suffix |
| `PUT  .../sessions/{sessionId}/tracks/update` | same suffix |
| `POST /_synapse/client/zuno/calls/cloudflare/turn/credentials` | `{base_url}/v1/turn/keys/{turn_key_id}/credentials/generate-ice-servers` |

The backend is named in the path so a LiveKit backend lands beside it (`.../calls/livekit/sfu/get`) and both run during a rollover. LiveKit needs none of the SFU routes: its client SDK does its own signaling and its server hands out ICE servers, so the seam is the app's `CallEngine`.

## Request flow

1. Route: method plus remaining segments against the allowlist; miss is 404 `M_UNRECOGNIZED`, before auth. Literal segments in the route table (`new`, `tracks`, ...) are reserved words a `{sessionId}` never matches; nor does it match an empty segment or one holding a slash — Twisted decodes segments first, so an encoded slash is refused here.
2. Auth: `api.get_user_by_req`; Synapse's own 401s. Rate key is (user, device), user alone for tokens without a device.
3. Rate limit: token bucket per key; over is 429 `M_LIMIT_EXCEEDED` with `retry_after_ms`, Cloudflare never called.
4. SFU path check: every segment `[A-Za-z0-9_-]{1,128}`, else 400 `M_INVALID_PARAM` — every other bad segment.
5. Body: SFU reads up to 256 KB, over is 413 `M_TOO_LARGE`; TURN ignores its body (the TTL is config).
6. Upstream: SFU one attempt, bearer App Secret, client Content-Type relayed; TURN `{"ttl": turn_credential_ttl}`, bearer API token, two retries with backoff on connect failure, timeout, 5xx or 429.
7. Relay: Cloudflare's status, Content-Type and body byte-for-byte. Unavailable is 502 `M_UNKNOWN` "calls service unavailable".

Success responses carry no CORS headers, which Synapse adds only to its own error responses; the native app never preflights.

Handlers are `async (Call) -> Reply`; `ZunoResource` is the glue (route, auth, limit, body, relay). Errors are `SynapseError`; `DirectServeJsonResource` renders them and runs handlers under Synapse's logging-context rules. Unsupported verbs are the same miss: the resource overrides Synapse's per-method dispatch, so there is one path and never a 405.

## Config

```yaml
modules:
  - module: zuno_calls.ZunoCalls
    config:
      cloudflare:
        app_id: "..."
        app_secret_path: /data/secrets/cf_calls_app_secret    # or app_secret
        turn_key_id: "..."
        turn_api_token_path: /data/secrets/cf_turn_api_token  # or turn_api_token
        base_url: https://rtc.live.cloudflare.com             # default
        turn_credential_ttl: 2h                               # default
        timeout: 10s                                          # per SFU attempt, default
        turn_timeout: 5s                                      # per TURN attempt, default
      rate_limit:                                             # per (user, device)
        per_second: 5
        burst: 10
```

- No `cloudflare` section: the module loads and registers nothing (Synapse answers 404). That is the off switch.
- Secrets: exactly one of inline or `_path`; the file is read verbatim minus one trailing newline. `_path` exists because Synapse replaces top-level keys per config file, so `modules:` cannot be split into a generated secrets file.
- Durations: `500ms`, `10s`, `2h`, `1d` or integer seconds. Every problem is reported in one `ConfigError`.
- `modules:` is shared config, so workers load it too and Synapse mounts the resources on every worker with a `client` listener; the gateway routes these paths to main alone, so the workers never see traffic. Construction does no I/O.

## Upstream client

Own Twisted `Agent` over a persistent pool (8 per host, idle connections dropped after 60 s so a keep-alive Cloudflare has closed is never reused), connect timeout = `timeout`, system CA verification, redirects never followed, response body capped at 1 MB. Each attempt is bounded by `timeout` for the SFU and `turn_timeout` for TURN, so a mint's three attempts take at most 15 s. `api.http_client` is not used: fixed 60 s timeout, shared pool with Synapse's other outbound traffic. Per process; the limiter is too.

## Metrics

`zuno_calls_rate_limited_total`, `zuno_calls_upstream_requests_total{api,code}`, `zuno_calls_upstream_seconds{api}`, `zuno_calls_upstream_retries_total{api}`, `zuno_calls_upstream_errors_total{api,reason}` with `reason` in `timeout|connect|too_large|other`. Per-backend counts and latency come from Synapse's `synapse_http_server_*`, labelled by resource class name: `SFUResource` and `TurnResource`.

## Tests

`make test`: unit tests with a fake ModuleApi and fake upstream, plus a real local Twisted server for the upstream client. `make e2e` (Docker, opt-in): the module inside `synapse:v1.161.0` against a stub Cloudflare; proves loading, `parse_config`, path registration and Synapse's auth together.
