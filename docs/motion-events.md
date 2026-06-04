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

- `evt`
- `eventType`
- `alarmType`
- `imageAlertType`

See [`const.py`](../custom_components/cloudplus/const.py) `ALARM_TYPE_NAMES`
and `MOTION_ALARM_TYPES` for the active mapping. The motion binary sensor
fires for any alarm in `MOTION_ALARM_TYPES = {1, 2, 11, 20}` (PIR, Motion,
Human body, Person). Other alarm types (Visitor, Noise, Package, etc.) are
classified by `motion_event.py` but currently routed only to logs / future
event sensors.

## Fallback: notification-center polling

The app also reads notification-center summaries from:

```
POST /v3/app/event/new/get   { "listAllDevice": 1, … }
```

This is a useful fallback for shared accounts or long-lived MQTT sessions
where events were recorded by the cloud but not delivered through the live
socket. The integration uses it as a recovery / catch-up source rather than
a primary signal, because polling has higher latency than the MQTT push.

## Practical guidance

- If MQTT events stop after running for a long time, check that no other
  client is logging in with the same account. The broker silently drops the
  older session.
- If you intentionally share an account between phone and HA, the
  notification-center fallback will surface most events with a few seconds
  of latency, but the live MQTT push will keep flapping. **Use a dedicated
  HA account** for production setups.
- Alarm types observed in the wild but not currently in `MOTION_ALARM_TYPES`
  are intentionally non-motion (e.g. `21 SD card removed`, `10 Tamper`).
  Adding them to motion would create false positives.
