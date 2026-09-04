# Motion Events (Meari IoT MQTT)

How the integration receives PIR / motion / AI alerts in real time and what
payload shapes to expect.

> 🛠 **Agents: keep this file in sync with the code.** Any change to MQTT
> topics, payload parsing, alarm-type mapping, or the notification-center
> fallback must be reflected here in the same change. See
> [AGENTS.md](../AGENTS.md) for the full doc-maintenance policy.

## Transport

- Official apps subscribe to Meari IoT events through **MQTT over TLS**
  using the host/port and `mqttSignature` returned by the platform config
  API.
- MQTT parameters as used by the apps and this integration:
  - **Client id** = numeric user id
  - **Username** = platform access id
  - **Password** = `mqttSignature`
  - **Clean session** = `true`
  - **Keepalive** = `300`
- The app keeps **one Meari MQTT session per account** and dispatches events
  to cameras internally. Multiple MQTT sessions with the same user-id /
  client-id can make the broker disconnect older sessions — this is the
  single most common cause of "no motion events in HA": another live login
  (phone, second HA instance, etc.) is bumping us off.
- **The broker pins the MQTT client-id to the numeric user-id.** Connecting
  with any other client-id is rejected with CONNACK *Not authorized* (verified
  against the live EU broker). So we cannot side-step the collision by picking
  a unique client-id — HA and the phone app *must* both connect as the
  user-id and therefore evict each other. There is no MQTT-layer fix; the
  remedies are a dedicated HA account (so nothing competes) **and** the
  polling fallback below (which works regardless of who owns the live socket).
- The setup docs recommend a dedicated secondary account for HA for exactly
  this reason.

## Topics

- Default motion topic:
  ```
  $bsssvr/iot/{userId}/{userId}/event/update/accepted
  ```
- When the account has a multi-login client id, the app subscribes to:
  ```
  $bsssvr/iot/{clientId}/{userId}/event/update/accepted
  ```
  instead. The integration mirrors that behaviour when it detects a
  multi-login client id in the platform config.

## Payload shapes

Event payloads are **not stable** — different cameras / firmwares wrap them
differently. The parser must accept any of:

- top-level fields directly on the message
- a `params` object
- a `data` object
- a `result` object
- an `items` array

Alarm-type fields the parser looks for, in order:

- `eventType`
- `alarmType`
- `imageAlertType`
- `alertType`
- `evt`

See [`const.py`](../custom_components/cloudplus/const.py) `ALARM_TYPE_NAMES`
and `MOTION_ALARM_TYPES` for the active mapping. The motion binary sensor
fires for any alarm in `MOTION_ALARM_TYPES = {1, 2, 11, 20}` (PIR, Motion,
Human body, Person). Other alarm types (Visitor, Noise, Package, etc.) are
classified by `motion_event.py` but currently routed only to logs / future
event sensors.

## Fallback: cloud event polling

Because the MQTT session is regularly evicted whenever the account is also
logged in on a phone (see Transport — the broker forces client-id == user-id),
polling is **not just a catch-up source, it is the path that actually works**
for most users. The MQTT push stays as the low-latency bonus when nothing
competes (e.g. a dedicated HA account).

The poller mirrors the official app's Messages tab and reads the real
per-device **event log**, once per registered camera:

```
GET /v3/app/event/list   { deviceID, day=YYYYMMDD, direction=1, index=0 }
→ { "alertMsg": [ { "msgID", "eventType", "eventTime", "deviceID", … }, … ] }
```

The per-device log remains primary over the older `/v3/app/event/new/get` summary for three
reasons, all of which broke motion for real users:

- **Read-state independent.** `event/new/get` returns an `evt` *has-unread
  flag* (not the alarm type) plus `imageAlertType`. Once the phone app reads
  the notification, `evt` flips to `"0"` and our parser — which keys on `evt`
  first — classified it as a non-motion event, so the sensor never fired.
  `event/list` always returns the full day's events regardless of read-state.
- **Correct alarm type.** `event/list` entries carry the actual `eventType`
  (e.g. `2` = Motion), so `parse_motion_event` classifies them correctly
  without relying on the ambiguous `evt` flag.
- **Stable de-dup.** Each entry has a unique `msgID`; we remember
  `(deviceID, msgID)` so the same event is never re-fired. The seed pass at
  startup records existing ids without dispatching; the set is cleared on day
  rollover and capped to stay bounded.

The poller also calls:

```
GET /v3/app/event/new/get { listAllDevice=1 } → { "result": { "device": [ { "evt", "imageAlertType", "devLocalTime", "deviceID", … }, … ] } }
```

Those summary rows are a secondary source only. They are de-duplicated by the
reported device and local event time (or a stable raw-row fallback), seeded on
startup without dispatching, and parsed with `imageAlertType` so `evt` still
behaves as a read/unread flag rather than the alarm type.

`MotionEventListener` polls every `ALARM_POLL_INTERVAL` (15 s), so worst-case
motion latency without MQTT is ~15 s.

All cloud requests have finite connect/read timeouts. If both event-log
sources fail, the account-scoped listener performs a fresh platform login,
updates the MQTT credentials used for later reconnects, and resumes polling.
This prevents an expired token or a wedged HTTPS connection from leaving
motion delivery permanently stopped until Home Assistant restarts.

## Practical guidance

- If MQTT connect logs `Bad user name or password`, low-latency MQTT push is
  unavailable for that session but cloud polling remains active.
- If MQTT events stop after running for a long time, check that no other
  client is logging in with the same account. The broker silently drops the
  older session.
- If you intentionally share an account between phone and HA, the
  event-log poll will still surface every event within ~15 s, but the live
  MQTT push will keep flapping. **Use a dedicated HA account** if you want the
  sub-second MQTT push as well.
- Alarm types observed in the wild but not currently in `MOTION_ALARM_TYPES`
  are intentionally non-motion (e.g. `21 SD card removed`, `10 Tamper`).
  Adding them to motion would create false positives.

## Event snapshots

When a motion payload (MQTT or the existing event-log poller) includes an
HTTP(S) image URL, the listener downloads it using the existing bounded
`download_snapshot` helper. It accepts ordinary JPEG data and the ieGeek/Meari
`.jpgx3` obfuscation observed on battery cameras: XOR the first 1,024 bytes
with the ASCII MD5 hex digest of `<serial>|<serial-length>|meari.stream`.
The serial is taken from the matched camera registration. This is format
decoding, not user-configured E2EE decryption; unknown formats are rejected
by their JPEG signature. No serials, image URLs or image bytes are logged.

The decoded image is installed in the camera cache before publishing motion.
While motion is active, video still conversion does not replace an event
image; this is checked both before starting conversion and when a conversion
finishes. Live still conversion resumes after motion clears. This changes
the cached still only, not the MPEG-TS live stream.

Camera attributes expose:

- `image_source`: `event`, `live`, or an empty string when no image is cached.
- `image_generation`: an incrementing counter for image replacement or clearing.
- `image_updated_at`: Unix time when the local cache received an image, or
  zero when empty. This is **not the camera's capture timestamp** and does
  not establish the age of a delayed cloud event.

A missing URL, failed download or unsupported image still delivers the
motion callback without a new image. This change adds no event-history
queries, retry loop, authentication session, notifications or AI processing.
It deliberately does not pair an image-less MQTT event with an arbitrary
latest historical event; the regular poller can deliver an image-bearing
event later. A download can delay motion delivery by the helper's timeout
(currently ten seconds); offloading downloads would require a separate
decision about motion/image ordering.

### Offline regression checks

Run from the repository root with Python 3.12+ and the standalone harness
dependencies (`pycryptodome`, `paho-mqtt`, `requests`, `aiohttp`, `voluptuous`):

```bash
python -m unittest discover -s tests -v
```

These tests reuse `debug_tools.bootstrap`, load the actual parser, listener
and coordinator methods, and mock only external services. They require no
HA server, network calls, camera photos or account credentials. Synthetic
protocol bytes exercise decoding and rejection, routing, startup seeding,
event de-duplication, unavailable images and an in-flight still conversion.
Live hardware and phone rendering still require an end-to-end test.
