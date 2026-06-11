# AGENTS.md

Instructions for AI coding assistants. Humans read [README.md](README.md)
first; this is the agent onboarding.

## ⚠️ Keep agent docs in sync — every PR

When you change behaviour this file or any file under [`docs/`](docs/)
describes, **update the relevant doc in the same change**. Each `docs/`
file has its own per-topic reminder. The code wins on conflicts, but that
means the doc is broken — fix it now, not later.

## What this is

A Home Assistant custom integration for Meari-family battery cameras
(CloudEdge / CloudPlus / Meari / ieGeek). Pure asyncio, no build step, no
test suite, runtime is HA. Validate with [debug.py](debug.py).

## Repo map

| Path | Contents |
|------|----------|
| [custom_components/cloudplus/](custom_components/cloudplus/) | The HACS-shipped integration. Entry: `__init__.py`. |
| [custom_components/cloudplus/coordinator/](custom_components/cloudplus/coordinator/) | Per-camera worker, state machine, MPEG-TS muxer + fan-out. |
| [custom_components/cloudplus/p2p_streamer/](custom_components/cloudplus/p2p_streamer/) | Protocol stack: discovery → signaling → ICE/TURN → KCP → VVP. |
| [debug_tools/](debug_tools/) + [debug.py](debug.py) | CLI harness — same code path as HA, drives ffplay. |
| [docs/](docs/) | Architecture, protocol, streaming, motion, diagnosis. |

Module-by-module map: [docs/architecture.md](docs/architecture.md).

## Local environment notes

This repo is the public integration only. Some contributors keep
machine-local tooling and vendor-app evidence (decompiles, packet captures,
a sandbox to run the official app) *outside* version control — it's
gitignored and differs per person. If an
[`AGENTS.local.md`](AGENTS.local.md) exists at the repo root, **read it
first**: it's the entry point to any local-only agent instructions. It's
absent by default; create your own to document your setup.

## Setup

Python 3.12+. `ffmpeg`/`ffplay` on PATH. A `.env` next to `debug.py` with
`EMAIL`, `PASSWORD`, `COUNTRY_CODE`, `PHONE_CODE`, `PROFILE`.

```bash
python -m venv .venv && source .venv/bin/activate
pip install pycryptodome paho-mqtt voluptuous aiohttp homeassistant
pip install pylint   # linter (dev only)
```

## Iteration loop

No tests. Validate every behaviour change with the harness:

```bash
python debug.py list                                              # auth + discovery
python debug.py stream --device-id <id> --duration 60 --quality SD
python debug.py --debug stream --device-id <id> --duration 30    # verbose
```

For protocol changes, cross-check against any local packet captures your
environment provides (see [`AGENTS.local.md`](AGENTS.local.md)). Lint with
`pylint custom_components debug_tools debug.py` ([.pylintrc](.pylintrc)) —
keep it at **10.00/10**. Most checks are on. Prefer fixing code over 
adding suppressions.

## Code style

- Python 3.12, `from __future__ import annotations`, PEP 8.
- Type hints on new public functions; modern `X | None` unions.
- Async everywhere. Blocking I/O via `hass.async_add_executor_job`.
- HA entities use `_attr_has_entity_name = True` and device-info built from
  `coordinator.device_uuid` / `device_name` / `device_model`.
- New IoT entities are a `dataclass` spec gated on
  `coordinator.supports_iot(feature)` or `coordinator.has_iot_code(code)`.
- `_LOGGER = logging.getLogger(__name__)`. Engine log phrases are
  grep-targets — preserve them across refactors
  (see [docs/diagnosis.md](docs/diagnosis.md)).

## Project-specific rules

- **Discovery is the source of truth.** Never hardcode signaling IPs,
  regions, or TURN endpoints. Root discovery + SDP carry them.
- **Match the official app's wire timing** for live-start, wake retry, and
  source-idle re-issue. "Wait for echo" / "wait for confirmation" gating
  is wrong here. See [docs/streaming.md](docs/streaming.md).
- **Video is copied, audio is encoded.** No video transcoding. Don't add
  it. H.264 *and* HEVC must keep flowing through unchanged.
- **Don't fabricate frames.** On stall: KCP recovery → `START_LIVE`
  re-issue → reconnect. Old frames create visible time travel.
- **One Meari MQTT session per account.** See
  [docs/motion-events.md](docs/motion-events.md).
- **Config-entry topology**: one account entry (no `sn_num`) + N camera
  entries. `_linked_account_entry()` walks camera → account.

## Permissions

Allowed without asking: reading any file, editing integration/engine/doc
files, running `python debug.py …` against the user's camera, running
`pylint`.

Ask first: adding a runtime dependency (`manifest.json` `requirements`),
bumping `manifest.json` `version` (triggers
[.github/workflows/release-tags.yml](.github/workflows/release-tags.yml)),
any `git commit` / `push` / branch / tag op (especially anything touching
`main` or `dev`), or network captures / APK downloads / extra Meari-cloud
traffic beyond a normal stream.

## Security

- Never commit credentials. `.env` is gitignored.
- The vendor-shared constants in
  [const.py](custom_components/cloudplus/const.py) (`DEFAULT_CA_KEY`,
  `DEFAULT_CA_SECRET`, `DES_KEY`, `DES_IV`) come from the decompiled apps,
  not from the user — don't treat them as user secrets, but don't add real
  secrets next to them either.
- The per-camera E2EE video password lives in
  `entry.options[CONF_VIDEO_PASSWORD]`. Never log it, never include it in
  errors or telemetry.

## Deeper docs

| Question | Doc |
|----------|-----|
| Where does feature X live? | [docs/architecture.md](docs/architecture.md) |
| What does the wire actually do? | [docs/protocol.md](docs/protocol.md) |
| Why is live-start / wake / re-issue shaped this way? | [docs/streaming.md](docs/streaming.md) |
| Motion / MQTT? | [docs/motion-events.md](docs/motion-events.md) |
| Triaging a stream that "barely works"? | [docs/diagnosis.md](docs/diagnosis.md) |
