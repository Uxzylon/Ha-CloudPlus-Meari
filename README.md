# CloudEdge / Meari — Home Assistant Integration

[![hacs_badge](https://img.shields.io/badge/HACS-Custom-orange.svg)](https://github.com/hacs/integration)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A Home Assistant custom integration for battery-powered Wi-Fi cameras built on
the Meari cloud platform.

Meari is the underlying cloud and device platform; CloudEdge is one of its
main consumer-facing apps and services. The integration communicates directly
with the platform and cameras using the VVP / PPStrong P2P video protocol — no
third-party bridge, no Frigate plugin, and no extra container are required.

> ⚠️ **Unofficial.** Not affiliated with CloudEdge, Meari, or any reseller.
> Use at your own risk; cloud APIs can change without notice.

---

## Supported apps

The account profile must match the app used to register the account:

- CloudEdge
- CloudPlus / CloudHome
- ANRAN
- Arenti
- ieGeek

These apps use the same underlying Meari platform, but the server requires
each app's own identity during login.

---

## Highlights

- **Live MPEG-TS stream** served by HA itself — video is **copied** from
  the camera with no transcoding, so the stream carries whatever the camera
  natively produces:
  - **H.264** on the lower profiles (typically `SD`)
  - **HEVC / H.265** on the higher profiles (typically `HD`, `QHD`, `AUTO`)

  Audio (G.711 µ-law) is the only thing re-encoded, into AAC, so the result
  is a clean MPEG-TS that Frigate, go2rtc, the native HA `stream`
  component, ONVIF clients and most NVRs all accept directly.
- **Idle stream for battery cameras ("snap" cameras)** — while the camera
  is asleep, the integration keeps publishing a still-frame MPEG-TS with
  the same advertised codec / resolution / FPS as the live stream. The
  consumer (Frigate, go2rtc, mobile companion, …) **never sees the source
  disappear**, and the moment the camera wakes up the stream switches to
  live without a reconnect or a "stream lost" event.
- **Motion / AI alerts via MQTT** — battery cameras push PIR, person, pet,
  package and visitor events in real time through the Meari IoT broker.
- **Battery-camera wake control** — manual wake button, auto-wake on motion
  switch, and configurable wake duration.
- **Full settings surface** — most camera IoT toggles (LED, PIR, sirens,
  day/night, recording, anti-flicker, ONVIF, HomeKit, …) appear as native
  HA entities and write back to the camera.
- **PTZ** — `cloudplus.ptz` service for cameras that support it.
- **Multi-camera & multi-account** — one config entry per account, one
  device per camera, all created automatically after login.

---

## Requirements

- Home Assistant **2024.1+**
- A Meari-compatible app account with at least one camera already paired in
  the official app
- `ffmpeg` (bundled in HAOS, HA Container, and HA Supervised)
- Outbound internet access to the Meari cloud (HTTPS + MQTT/TLS) and UDP/TCP
  to the camera (LAN or TURN-relayed)

`pycryptodome` and `paho-mqtt` are pulled in automatically by HACS / pip.

---

## Installation

### HACS (recommended)

1. Open **HACS → Integrations**.
2. Three-dot menu → **Custom repositories** → paste this repo URL → category
   **Integration**.
3. Search for *CloudEdge / CloudPlus / Meari*, install, **restart HA**.

### Manual

1. Copy [custom_components/cloudplus/](custom_components/cloudplus/) into
   `<config>/custom_components/`.
2. Restart Home Assistant.

---

## Setup

1. **Settings → Devices & Services → Add Integration**.
2. Pick *CloudEdge / CloudPlus / Meari*.
3. Enter your account credentials:

   | Field         | Notes |
   |---------------|-------|
   | Email         | Same address you log into the mobile app with. |
   | Password      | Account password. |
   | Country code  | e.g. `FR`, `US`, `DE` — matches your app region. |
   | Phone code    | International dial code (e.g. `33` for France). |
   | App profile   | Choose the matching profile from [Supported apps](#supported-apps). |

4. All cameras on the account are discovered and added as individual devices.

> 💡 **Use a secondary account.** The Meari MQTT broker only allows one live
> session per account at a time. If Home Assistant logs in with the same
> account as your phone, motion events on the phone may stop arriving (and
> vice-versa). Create a second cloud account and share the camera(s) with it
> (or add it to the "family / home") before adding it to HA.

### Per-camera options

Open any camera device → **Configure**:

| Option            | Description |
|-------------------|-------------|
| Video password    | If you enabled **E2EE / video encryption** in the app, enter that 6-digit code here so the stream can be decrypted. Leave empty otherwise. |

### Per-account options

Open the account device → **Configure** — update the password / country /
phone code / app profile in place. Changes are propagated to every camera
entry automatically.

---

## Entities

Every camera shows a fixed core set of entities, plus a dynamic set
discovered from the camera's IoT model values.

### Core entities

| Entity              | Platform        | Purpose |
|---------------------|-----------------|---------|
| Camera              | `camera`        | Live preview + `stream_source` for go2rtc / Frigate. |
| Motion              | `binary_sensor` | PIR / motion / AI alert (person, pet, package, etc.). |
| Camera Awake        | `binary_sensor` | True while the camera is actively streaming. |
| Charging            | `binary_sensor` | USB charging state (battery cameras). |
| Battery             | `sensor`        | Charge percentage (battery cameras). |
| Charge Status       | `sensor`        | Discharging / Charging / Full text status. |
| Wake Camera         | `button`        | Manually wake the camera. |
| Wake on Motion      | `switch`        | Toggle auto-wake when a motion event fires. |
| Motion Timeout      | `number`        | How long the camera stays awake after motion (10–600 s). |
| Stream Host Mode    | `select`        | `IP Address` (default) or `Docker Hostname` for the stream URL — see [Streaming notes](#streaming-notes). |
| Stream Quality      | `select`        | `AUTO`, `SD`, `HD`, `QHD`, … — advertised profiles, or the native HD/SD fallback for legacy cameras. |

### IoT entities (model-dependent)

The integration reads each camera's supported IoT feature codes on first
setup and exposes only those that exist. Expect a subset of:

- **Switches**: Status LED, Motion Detection, Person Detection, Human
  Tracking, Sound Detection, Crying Detection, ONVIF, SD Recording, PIR
  Sensor, Floodlight / Siren Linkage, Face Recognition, Sleep Mode, RGB
  Light, Anti-Jamming, PTZ Patrol, Laser Toy, Pet Alarm, HomeKit, Auto
  Update, OSD Watermark, Brand Logo, Sound, Lamp.
- **Numbers** (sliders): Motion / Sound / Human / PIR Sensitivity, PIR
  Interval, Floodlight Brightness & Duration, Speaker Volume, Warm Light
  Brightness.
- **Selects**: SD Record Type, Day/Night Mode, Alarm Frequency,
  Sound/Light Alarm Type, Anti-Flicker (50/60 Hz), Full Color Mode.
- **Sensors**: Temperature, Humidity (on cameras that report them).

Cameras that don't expose a feature simply won't show its entity.

### Services

| Service          | Description |
|------------------|-------------|
| `cloudplus.ptz`  | Move (`left`/`right`/`up`/`down`) or stop a PTZ camera. Target a `camera` entity. See [services.yaml](custom_components/cloudplus/services.yaml). |

---

## Streaming notes

### What the camera entity exposes

The camera entity exposes its native stream as an HA-managed MPEG-TS source.

- **Video** is copied from the camera as-is. Depending on the chosen
  `Stream Quality`, the underlying codec is **H.264** (smaller, more robust
  — typically `SD`) or **HEVC / H.265** (higher quality, more fragile on
  battery cameras — typically `HD`, `QHD`, `AUTO`). Both codecs are
  delivered to consumers untouched, so quality is exactly what the camera
  produces.
- **Audio** (G.711 µ-law from the camera) is re-encoded to AAC so the
  output is a fully standard MPEG-TS.
- The result is reachable by Frigate, go2rtc, the native HA `stream`
  component, `media_player.play_media`, the HA mobile companion, ONVIF /
  RTSP clients, and most NVR-style add-ons.

### Idle stream (battery / snap cameras)

Battery cameras spend most of their life asleep — actually streaming only
while a PIR or motion event keeps them awake. Without help, consumers like
Frigate would see the source vanish every minute or two and fire a "lost
stream" event.

The integration solves this by **continuously publishing an idle stream**
while the camera is asleep:

- Same advertised codec, resolution and FPS as the live profile.
- Repeated still frame seeded from the most recent keyframe captured while
  the camera was awake (so it visually represents the actual scene).
- Low refresh cost — not re-encoded at high FPS.
- When the camera wakes up, the live frames take over **on the same TCP
  socket**, with no reconnect, no PAT/PMT renegotiation, and no client
  seeing the source go offline.

This is the difference between Frigate "just working" with these cameras
and Frigate being effectively unusable with them.

### Stream Host Mode

The `Stream Host Mode` select entity controls how the public URL of the
MPEG-TS server is advertised:

- `IP Address` — uses the HA host's LAN IP (e.g. `tcp://192.168.1.10:36059`).
  Best for HAOS / HA Container with host networking, or anything that
  reaches HA directly by IP.
- `Docker Hostname` — uses the container's hostname instead. Pick this if
  Frigate / go2rtc runs in the **same Docker network** as HA and resolves
  containers by name.

### Using the stream in Frigate / go2rtc

The MPEG-TS server is internal to Home Assistant — it isn't a hardcoded
RTSP URL you can paste into Frigate's config. You need to read the actual
URL out of the live HA `camera` entity and hand it to go2rtc / Frigate
each time the stream is opened.

The easiest setup is the
[`hass-expose-camera-stream-source`](https://github.com/felipecrs/hass-expose-camera-stream-source)
integration, which exposes the live `stream_source` of any HA camera
entity over a simple HTTP endpoint:

```
GET http://<ha-host>:8123/api/camera_stream_source/<camera_entity_id>
Authorization: Bearer <long-lived-access-token>
```

The response body is the current stream URL (e.g.
`tcp://192.168.1.10:36059`). go2rtc can call that endpoint on every stream
open via its `echo:` source type, which uses the stdout of an arbitrary
command as the URL:

1. **Install** `hass-expose-camera-stream-source` (HACS, integration). No
   YAML / configuration entry needed — it just enables the endpoint.
2. **Create a long-lived access token** in HA (profile menu → bottom of
   the page → *Long-Lived Access Tokens*).
3. **Point go2rtc at the endpoint**. Example for Frigate's bundled
   go2rtc, with a camera entity named `camera.cloudplus_camera`:

   ```yaml
   # frigate config.yml
   go2rtc:
     streams:
       cloudedge-camera:
         - 'echo:curl -fsSL http://<ha-host>:8123/api/camera_stream_source/camera.cloudplus_camera
             -H "Authorization: Bearer <ha-long-lived-access-token>"'

   cameras:
     cloudedge-camera:
       ffmpeg:
         inputs:
           - path: rtsp://127.0.0.1:8554/cloudedge-camera
             roles: [detect, record]
   ```

   Standalone go2rtc takes the exact same `echo:curl …` entry under its
   own `streams:` section.
4. Frigate then talks to go2rtc over RTSP, go2rtc re-resolves the HA
   endpoint each time a client connects, and you get a clean H.264 / HEVC
   feed that survives the camera sleeping (the idle stream covers the
   gap) and records / detects normally.

> 💡 If you'd rather not use the helper, you can read the `stream_source`
> attribute off the camera entity directly via the HA WebSocket or REST
> API and feed it to go2rtc however you prefer. The helper just packages
> that into a single token-protected GET.

### Deep technical details

For the underlying mechanics (KCP recovery, TURN/relay routing, HEVC
stalls, proactive `START_LIVE` re-issue, source-idle recovery), see
[docs/streaming.md](docs/streaming.md) and
[docs/protocol.md](docs/protocol.md).

---

## Troubleshooting

A standalone CLI harness (`debug.py`) reuses the exact code path the
integration takes, so you can repro problems outside Home Assistant:

```bash
# Copy .env.example or create your own .env with EMAIL/PASSWORD/COUNTRY_CODE
python debug.py list
python debug.py stream --device-id <id> --duration 60 --quality QHD
python debug.py --debug stream --device-id <id> --duration 30  # verbose logs
```

Common issues:

- **No motion events** — another device is probably holding the Meari MQTT
  session. Use a dedicated cloud account for HA (see [Setup](#setup)).
- **Stream stalls / "few frames" on QHD** — HEVC at high bitrate is fragile
  on battery cameras; dropping to SD (which is H.264) is a useful fallback.
  See [docs/diagnosis.md](docs/diagnosis.md).
- **Camera never wakes** — deep-dormancy wakes are retried automatically
  for up to ~45 s; if it consistently fails, your account may not own the
  wake right (re-share from the app).
- **Frigate loses the source** — flip *Stream Host Mode* between `IP
  Address` and `Docker Hostname` depending on your network topology.

---

## How it works (high level)

1. **Authenticate** with the Meari HTTP API (per-app-profile host family).
2. **Subscribe** to the Meari IoT MQTT broker (TLS) for motion / AI alerts.
3. **Discover** signaling servers via the native UDP root protocol.
4. **Signal** a WebRTC-like session over TCP MsgSvr (offer/answer, ICE).
5. **Relay** data through a TURN UDP allocation (or directly on the LAN).
6. **Tunnel** with KCP (reliable ARQ over UDP) and the IVA framing layer.
7. **Control** the camera with VVP commands (`START_LIVE`, heartbeat, etc.).
8. **Decrypt** the media — H.264 *or* HEVC video (3DES-ECB when the camera
   has E2EE enabled) and G.711 µ-law audio.
9. **Mux** to MPEG-TS — video is copied (whichever codec the camera sent),
   audio is encoded to AAC — and serve to all HA / Frigate / go2rtc
   consumers via a single fan-out TCP socket. The same socket carries the
   idle still-frame stream while the camera is asleep.

Each layer is documented in detail under [docs/](docs/).

---

## Documentation map

| File | Audience | What's inside |
|------|----------|---------------|
| [README.md](README.md) | Users | This file — install, setup, entities, services. |
| [AGENTS.md](AGENTS.md) | AI coding agents | Project entry points, commands, conventions, permissions. |
| [docs/architecture.md](docs/architecture.md) | Contributors | Code layout (custom_components, p2p_streamer, coordinator, debug_tools). |
| [docs/protocol.md](docs/protocol.md) | Protocol hackers | Discovery, signaling, ICE/TURN, KCP, VVP, media frames. |
| [docs/streaming.md](docs/streaming.md) | Contributors | Live-start patterns, source-idle recovery, MPEG-TS fan-out. |
| [docs/motion-events.md](docs/motion-events.md) | Contributors | Meari MQTT topics, payload shapes, fallback notification API. |
| [docs/diagnosis.md](docs/diagnosis.md) | Contributors | `debug.py` usage, log signals, how to triage stalls. |

---

## Contributing

PRs welcome. If you're an AI assistant working in this repo, start with
[AGENTS.md](AGENTS.md). If you're a human contributor, the same file is also
the fastest way to get oriented.

Before opening a PR, please:

- run `python debug.py list` and `python debug.py stream …` against your own
  camera to confirm the change doesn't break the streaming path,
- keep behaviour gated on actual evidence from local captures of the official
  app where possible,
- avoid hardcoded IPs, regions, or signaling endpoints — discovery is the
  source of truth.

---

## Releases

Normal pushes to `main` do not create releases. Maintainers create releases
manually from **Actions → Release Tags → Run workflow** and choose one of:

- `prerelease` — creates the next `vX.Y.Z-dev.N` release.
- `release` — creates the stable `vX.Y.Z` release.

The version in `custom_components/cloudplus/manifest.json` is the source of
truth. After publishing a stable release, bump that version before starting
the next prerelease cycle.

---

## License

[MIT](LICENSE) — see file for details. The product names used in this README
are trademarks of their respective owners; this project is not endorsed by any
of them.
