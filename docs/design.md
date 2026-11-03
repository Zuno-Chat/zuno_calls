# zuno_calls design

Synapse module serving the calling backend. Synapse checks the Matrix access token on every request, so there is no enrollment, no gateway token and no remote-logout window: a device logged out elsewhere is refused at once.

## Routes

| Route | Upstream |
|---|---|
| `POST /_synapse/client/zuno/calls/cloudflare/sessions/new?room_id=…&call_id=…` | `{base_url}/v1/apps/{app_id}/sessions/new` |
| `GET  .../sessions/{sessionId}` | same suffix |
| `POST .../sessions/{sessionId}/tracks/new` | same suffix |
| `PUT  .../sessions/{sessionId}/renegotiate` | same suffix |
| `PUT  .../sessions/{sessionId}/tracks/close` | same suffix |
| `PUT  .../sessions/{sessionId}/tracks/update` | same suffix |
| `POST .../sessions/{sessionId}/datachannels/establish` | same suffix |
| `POST .../sessions/{sessionId}/datachannels/new` | same suffix |
| `PUT  .../sessions/{sessionId}/datachannels/close` | same suffix |
| `POST /_synapse/client/zuno/calls/cloudflare/turn/credentials` | `{base_url}/v1/turn/keys/{turn_key_id}/credentials/generate-ice-servers` |
| `POST /_synapse/client/zuno/calls/remove?room_id=…&call_id=…` | none of its own; cuts the removed person off through each backend |

`sessions/new` names the call the session is for; the query is the module's and never reaches Cloudflare. `calls/remove` sits outside `cloudflare/` because removal does not depend on the backend.

The backend is named in the path so a LiveKit backend lands beside it (`.../calls/livekit/sfu/get`) and both run during a rollover. LiveKit needs none of the SFU routes: its client SDK does its own signaling and its server hands out ICE servers, so the seam is the app's `CallEngine`.

## Request flow

1. Route: method plus remaining segments against the allowlist; miss is 404 `M_UNRECOGNIZED`, before auth. Literal segments in the route table (`new`, `tracks`, ...) are reserved words a `{sessionId}` never matches; nor does it match an empty segment or one holding a slash — Twisted decodes segments first, so an encoded slash is refused here.
2. Auth: `api.get_user_by_req`; Synapse's own 401s. Rate key is (user, device), user alone for tokens without a device.
3. Rate limit: token bucket per key; over is 429 `M_LIMIT_EXCEEDED` with `retry_after_ms`, Cloudflare never called.
4. SFU path check: every segment `[A-Za-z0-9_-]{1,128}`, else 400 `M_INVALID_PARAM` — every other bad segment.
5. SFU session owner: a `{sessionId}` must be one this user opened through `sessions/new` and not yet forgotten (see Sessions), else 404 `M_NOT_FOUND`, the same as a session that never existed. Call participants learn each other's ids to pull tracks, so an id alone proves nothing. A session opened naming a call is bound to that `(room_id, call_id)`. Every request on a session bound to a call its owner was removed from is 403 `IM.ZUNO.CALL_REMOVED`.
6. Call cap, `sessions/new` only: `room_id` must start with `!`, `call_id` be non-empty, both at most 255 characters, else 400 `M_INVALID_PARAM`. The caller must be joined to the room, else 403 `M_FORBIDDEN`. A caller removed from the call is 403 `IM.ZUNO.CALL_REMOVED`, before the cap and never counted in it. Participants are distinct users other than the caller with a live `m.call.member` membership for `call_id` (`expires_ts` later than now) or a reservation; at `max_call_participants` the open is 403 `IM.ZUNO.CALL_FULL` with `max_participants`, Cloudflare never called. Otherwise the caller holds a reservation for 60 s, enough to publish tracks and its own `m.call.member`; a failed open releases it. Counting and reserving happen with no await between them, so two opens racing for the last place cannot both pass. The caller never counts against themselves, so a rejoin always passes. Temporary: an open naming neither `room_id` nor `call_id` (app releases up to v1.3.0) skips this step; once the oldest supported release names the call, a missing one becomes 400 `M_MISSING_PARAM`.
7. Body: SFU reads up to 256 KB, over is 413 `M_TOO_LARGE`; TURN ignores its body (the TTL is config).
8. Pulls, `tracks/new`, `tracks/update` (which can point a transceiver at another session's track) and `datachannels/new` (which subscribes to another session's data channel): the body must be a JSON object, else 400 `M_NOT_JSON`, and is relayed as parsed, so Cloudflare pulls exactly what was checked. Each entry of its `tracks[]` or `dataChannels[]` that is not a plain `local` one names the session it pulls from. A pull from a session bound to a call the caller was removed from is 403 `IM.ZUNO.CALL_REMOVED` for the whole request: no honest client makes it. Otherwise a pulled entry is refused on its own, at its place in the list with Cloudflare's per-entry `errorCode`/`errorDescription` shape, when its session belongs to someone removed from the call (`IM.ZUNO.CALL_REMOVED`), or when both sessions are bound and to different calls, or the pulling session is bound and the other unknown to this process (`IM.ZUNO.TRACK_NOT_IN_CALL`, for channels too). The rest go to Cloudflare; when none is left Cloudflare is not called. A per-entry refusal never fails the request, since the app rejoins on a failed pull and calls on without a refused channel. Temporary, with the cap: a pull with an unbound side (app releases up to v1.3.0) skips the binding check, never the removal checks.
9. Upstream: SFU one attempt, bearer App Secret, client Content-Type relayed; TURN `{"ttl": turn_credential_ttl}`, bearer API token, two retries with backoff on connect failure, timeout, 5xx or 429.
10. Relay: Cloudflare's status, Content-Type and body byte-for-byte, but for step 8's refused entries put back in their list. Unavailable is 502 `M_UNKNOWN` "calls service unavailable".

Success responses carry no CORS headers, which Synapse adds only to its own error responses; the native app never preflights.

Handlers are `async (Call) -> Reply`; `ZunoResource` is the glue (route, auth, limit, body, relay). Errors are `SynapseError`; `DirectServeJsonResource` renders them and runs handlers under Synapse's logging-context rules. Unsupported verbs are the same miss: the resource overrides Synapse's per-method dispatch, so there is one path and never a 405.

## Sessions

Each session opened through `sessions/new` is kept with its owner, its call and its open time in Synapse's database (table `zuno_calls_cloudflare_sessions`, read once per process and held in memory), so a restart forgets none: a forgotten session is out of a removal's reach.

| A session is forgotten when | Closed first |
|---|---|
| It is bound to a call, older than the 60 s reservation, and its owner's `m.call.member` event no longer lists that call | Yes |
| Its owner holds more than 32 besides those, oldest first | Yes |
| It is older than seven days | No: it is long dead, and this bounds a session whose close never succeeds |

- Closing is a removal's cut-off on the whole session and runs in the background, so `sessions/new` answers as soon as Cloudflare has opened the session and it is saved.
- An account's sessions close one at a time, so none is closed twice; each open starts its owner's closes and a periodic sweep starts everyone's.
- A failed close keeps the session for a retry after a growing wait, and the account's next-oldest goes instead, so a stuck session never slows an open or holds an account over the cap for long.
- Requests on a session being closed are 404, while a removal still reaches it.
- An entry that has only expired does not end the call: nobody can refresh while Synapse is down, so counting expiry would cut live calls on a restart.
- The sweep runs on every process but acts only on one that has served a call request since it started, so a worker never closes the main process's sessions.
- Unbound sessions (app releases up to v1.3.0) go only by the cap or by age.

## Removal

`POST calls/remove?room_id=…&call_id=…` with `{"user_id": "@target:server"}`. The query is validated as for `sessions/new`; the body must be a JSON object (400 `M_NOT_JSON`) with a string `user_id` (400 `M_BAD_JSON`) that is a Matrix user ID (400 `M_INVALID_PARAM`). The caller must be joined to the room, at or above its `kick` level (50 when unset) and above the target, and not the target, else 403 `M_FORBIDDEN`. Power levels are Synapse's own computation: no `m.room.power_levels` gives the creator 100 and everyone else 0, and from room version 12 the creators outrank every level. The limiter is the shared one.

A removal that passes is recorded first, so every refusal above holds from then on, then cut off: each of the target's sessions bound to the call has every track and data channel closed (`GET /sessions/{id}` for the open mids and channel ids, then `PUT .../tracks/close` with `force: true`, no SDP, and `PUT .../datachannels/close`), and each unbound one the tracks and channels it pulls from sessions bound to the call. Requests on those sessions already in flight when the cut-off starts are waited out first, since they could still add a track or channel; later ones are not waited for, as on a bound session they are refused. A session Cloudflare answers 4xx (but 429) for carries no media and needs no close. Only once every one is closed is the answer `200 {}`; otherwise 503 `M_UNKNOWN` with the record kept, and the moderator's retry finishes it. Cloudflare names no "already closed" error for a data channel, so one that closes on its own between the read and the close costs a 503 and the retry. The route is idempotent: the same removal again answers 200, including for a target not in the call, who is kept out either way.

Records are `(room_id, call_id, user_id, removed_by, removed_at)` in Synapse's database, table `zuno_calls_removals`, created on first use through `api.run_db_interaction`, so a restart keeps a removed person out. The process reads the table once and holds it in memory; writes go to the database first. A call's records go when a `sessions/new` for it finds no live `m.call.member` for its `call_id` left; records older than seven days go on the next load, for calls never opened again.

## Config

```yaml
modules:
  - module: zuno_calls.ZunoCalls
    config:
      max_call_participants: 6                                # default; any backend
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

Own Twisted `Agent` over a persistent pool (8 per host, idle connections dropped after 60 s so a keep-alive Cloudflare has closed is never reused), connect timeout = `timeout`, system CA verification, redirects never followed, response body capped at 1 MB. Each attempt is bounded by `timeout` for the SFU and `turn_timeout` for TURN, so a mint's three attempts take at most 15 s. `api.http_client` is not used: fixed 60 s timeout, shared pool with Synapse's other outbound traffic. Per process; the limiter and the call reservations are too. Session owners and removal records are in the database.

## Metrics

`zuno_calls_rate_limited_total`, `zuno_calls_call_full_total`, `zuno_calls_removals_total` (removals that passed the power check), `zuno_calls_removed_refusals_total{route,scope}` (every `IM.ZUNO.CALL_REMOVED`, `scope` `request`, `track` or `channel`), `zuno_calls_upstream_requests_total{api,code}`, `zuno_calls_upstream_seconds{api}`, `zuno_calls_upstream_retries_total{api}`, `zuno_calls_upstream_errors_total{api,reason}` with `reason` in `timeout|connect|too_large|other`. Per-backend counts and latency come from Synapse's `synapse_http_server_*`, labelled by resource class name: `SFUResource`, `TurnResource` and `RemoveResource`.

## Tests

`make test`: unit tests with a fake ModuleApi and fake upstream, plus a real local Twisted server for the upstream client. `make e2e` (Docker, opt-in): the module inside `synapse:v1.161.0` against a stub Cloudflare; proves loading, `parse_config`, path registration, Synapse's auth, the call cap's `get_room_state`, and a removal through real power levels and the real database, then a refused rejoin.
