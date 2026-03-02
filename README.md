# CloudPlus / Meari — Home Assistant Integration

[![hacs_badge](https://img.shields.io/badge/HACS-Custom-orange.svg)](https://github.com/hacs/integration)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A Home Assistant custom integration for **CloudPlus / CloudEdge / Meari** battery-powered Wi-Fi cameras.

These cameras are sold under various brand names (CloudPlus, CloudEdge, Meari, etc.) and all use the Meari cloud platform with the VVP (PPStrong) P2P protocol.

## Features

- **Live video stream** — MPEG-TS stream source compatible with Frigate, go2rtc, and HA's native stream component
- **Idle stream** — continuous still-frame output when the camera is asleep, so Frigate never loses the stream
- **Motion detection** — binary sensor triggered by camera PIR / AI alerts (person, car, animal, package)
- **Camera wake** — button entity to manually wake the camera on demand
- **Wake on motion** — switch to enable/disable automatic camera wake on motion events
- **Motion timeout** — number entity to control how long the camera stays awake after motion (10–600 s)
- **Battery level** — sensor with dynamic icon based on charge level
- **Charging state** — binary sensor for USB charging detection
- **Camera awake** — binary sensor showing whether the camera is currently streaming
- **Stream host mode** — select entity to choose between IP address or Docker hostname for the stream URL

## Requirements

- A CloudPlus / CloudEdge / Meari-based camera with cloud account
- Home Assistant 2024.1+
- `ffmpeg` installed in your HA environment (included in HAOS and HA Docker images)
- `pycryptodome` and `paho-mqtt` (installed automatically)

## Installation

### HACS (recommended)

1. Open HACS in Home Assistant
2. Click the three-dot menu → **Custom repositories**
3. Add this repository URL with category **Integration**
4. Search for "CloudPlus / Meari" and install
5. Restart Home Assistant

### Manual

1. Copy the `custom_components/cloudplus/` folder into your Home Assistant `config/custom_components/` directory
2. Restart Home Assistant

## Configuration

1. Go to **Settings → Devices & Services → Add Integration**
2. Search for **CloudPlus / Meari**
3. Enter your Meari / CloudEdge account email and password
4. Select the camera to add
5. The integration will create all entities automatically

## Entities

| Entity | Type | Description |
|---|---|---|
| Camera | `camera` | Live view + stream source |
| Motion | `binary_sensor` | Motion / PIR / AI detection |
| Camera Awake | `binary_sensor` | Whether the camera is actively streaming |
| Charging | `binary_sensor` | USB charging state |
| Battery | `sensor` | Battery percentage (0–100%) |
| Wake Camera | `button` | Manually wake the camera |
| Wake on Motion | `switch` | Enable/disable auto-wake on motion |
| Motion Timeout | `number` | Seconds the camera stays awake after motion |
| Stream Host Mode | `select` | IP Address or Docker Hostname for stream URL |

## How It Works

The integration connects to the Meari cloud servers to authenticate and discover cameras. It uses MQTT for real-time motion/alarm event notifications and a fully reverse-engineered P2P pipeline for video:

1. **Signaling** — connects to the Meari IoT server to broker a P2P session
2. **TURN relay** — establishes a UDP relay via the Meari TURN infrastructure
3. **KCP tunnel** — reliable transport layer over UDP (ARQ protocol)
4. **VVP protocol** — PPStrong video protocol for authentication and stream control
5. **Decryption** — HEVC (H.265) video frames encrypted with 3DES ECB, G.711 µ-law audio
6. **Muxing** — ffmpeg re-encodes to H.264 MPEG-TS for broad compatibility

When the camera is asleep, an idle stream (still frame at 15 fps) is output so that downstream consumers (Frigate, go2rtc) maintain a valid connection and can immediately display live video when the camera wakes up.

## Stream Host Mode

By default, the stream URL uses the HA host's local IP address (e.g. `tcp://192.168.1.100:36059`). If you run HA in Docker and need to access the stream by container hostname instead, switch the **Stream Host Mode** select entity to "Docker Hostname".

## License

[MIT](LICENSE)
