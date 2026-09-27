# zuno_calls

Synapse module serving Zuno Chat's calling backend: a scoped proxy for Cloudflare Calls (Realtime SFU) signaling and a Cloudflare TURN credential mint. Requests carry the user's Matrix access token; Synapse checks it. Endpoints live under `/_synapse/client/zuno/calls/cloudflare/`.

- Design and request flow: [docs/design.md](docs/design.md)
- Config: the `modules:` entry in `homeserver.yaml`, documented in docs/design.md
- Develop: `make check` (ruff, mypy, unit tests); `make e2e` runs the module inside a real Synapse in Docker

## License

Free software under the GNU Affero General Public License, version 3 or any later version. See [LICENSE](LICENSE).
