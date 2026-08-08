# Streaming Behaviour

How the integration starts, sustains and recovers a live session on top of
the [low-level protocol](protocol.md).

> 🛠 **Agents: keep this file in sync with the code.** Any change to
> live-start ordering, wake retry, source-idle recovery, the idle stream,
> MPEG-TS fan-out, or muxer timing rules must be reflected here in the
> same change. See [AGENTS.md](../AGENTS.md) for the full doc-maintenance
> policy.

## Live-start: send immediately after handshake

Direct evidence from the official CloudEdge app pcaps: the IVA handshake
sits in KCP push `sn=0` and `START_LIVE` in KCP push `sn=1` **back-to-back**,
with no delay between them and no wait for any camera-side echo or peer
confirmation.

- In the captured KCP stream: `sn=0` carries the IVA handshake `0x7012` and
  `sn=1` carries VVP `START_LIVE` (cmd `0x11FF`), back-to-back. Reproduce with
  your local capture tooling (see [`AGENTS.local.md`](../AGENTS.local.md)).

Earlier we gated `START_LIVE` on the camera echoing its IVA handshake (or a
short grace after media-peer confirmation). Both turn out to be wrong: on
some post-dormancy starts the camera ACKs our KCP push but **never echoes
the handshake AND ignores a `START_LIVE` sent later**, so the stream
silently stalls until a full reconnect. Sending `START_LIVE` in the same
burst as the handshake — the way the official app does — fixes that class
of stalls.

The engine therefore sends `kcp.send_handshake()` and a `START_LIVE` (reason
`startup`) back-to-back at session start, before entering the main loop.
`live_started = True` is set immediately so the keepalive and idle-retry
paths take over from there.

## Dormancy wake supports both native sequences

A dormant snap (battery / solar) camera can follow either of two sequences in
official-app captures. Newer cameras provide coturn while dormant and wake from
the **SDP offer / live request**; their `status=online` push can lag 40–57 s,
so gating that path on status makes startup needlessly slow. Some older cameras
withhold coturn while dormant: the app sends its signaling + HTTP wake first,
waits for the pushed `online` status on the same MsgSvr connection, and only
then requests coturn.

The engine supports both without model-specific guesses:

- On `status=dormancy` it first requests coturn. If credentials arrive, it
  allocates the relay and enters `_negotiate_dormant_offer`: re-issue the SDP
  offer and re-fire `send_wake_connect` + HTTP `wake_device` every few seconds
  while polling for the camera's delayed SDP answer and trickle candidates.
  The answer is the wake confirmation for this fast path.
- If the dormant coturn request times out or returns no credentials, the engine
  keeps the same MsgSvr session, sends the signaling + HTTP wake immediately,
  and waits in short intervals for that camera's pushed `online` status. It
  retries coturn throughout the same wake budget and continues with the fresh
  online NAT/contact data once credentials become available.
- `DORMANCY_WAKE_TIMEOUT_S` (≈75 s) bounds each dormant wake phase before
  giving up to the next signaling candidate / a fresh session.
- If neither dormant-offer nor pre-relay wake produces coturn credentials
  within that budget, move to the next signaling candidate instead of spending
  TURN allocation retries on an empty address.

This brought cold-start latency from ~70 s (status-polling) down to ~14 s,
matching the app.

**The watchdogs must not restart underneath an active wake.** A deep-dormancy
wake routinely takes 30–45 s, but the no-first-video restart fires at
`LIVE_STARTUP_STALL_RESTART_S` (25 s) and the debug harness stall restart at
~30 s. Left unguarded they tear the session down *mid-wake*, lose all wake
progress, and — because the camera often only comes online around 40 s — the
stream never starts (observed as a session that sends `START_LIVE` then dies
immediately, 0 frames). The engine therefore exposes `awaiting_wake` while the
offer-driven wake is in progress (set on `dormancy`, cleared once the camera's
SDP answer yields candidates); the HA coordinator (`_stream_video_stale`) and
the debug stall watchdog both **skip the restart while it is set**, and the
coordinator starts its first-video clock from when the wake *completes*, not
from session start. A truly stuck wake still ends on its own when the engine's
`DORMANCY_WAKE_TIMEOUT_S` budget expires, after which the session restarts
normally.

The debug harness `--wake-timeout` default is 90 s to account for worst-case
deep-dormancy wakes (a 25–45 s wake plus first-keyframe time can exceed the
old 45 s window).

## Source-idle recovery (sustaining the stream)

The `0x888E` VVP heartbeat keeps the **P2P session** alive. Native QHD HEVC
captures send one startup `START_LIVE`, then VVP heartbeats, then `STOP_LIVE`;
they do not proactively send `START_LIVE` while video is flowing. Re-sending it
mid-flow can make WAN relay sessions go silent on this camera.

The integration therefore:

- Uses VVP heartbeats while video is flowing.
- Late client joins use the cached PAT/PMT + keyframe seed; they do **not**
  proactively send another `START_LIVE` while video is current. A join-triggered
  `START_LIVE` is only used if the source is already stale enough to be in the
  reactive recovery path.
- **Reactively** re-issues `START_LIVE` when video stalls with no
  recoverable KCP gap (~5 s, `idle_start_live_retry_s`), repeating every few
  seconds. This fires whether the camera went fully silent or is still
  sending only audio — **do not** gate it on total UDP silence, the camera
  often keeps audio going after video stops. It also covers the "camera
  ACKed our handshake/live request but never pushed video" case where
  `_video_wait_s` falls back to `now - live_started_at` so the retry path
  still arms.

The idle retry path does **not** short-circuit the reconnect check: if a
session has stayed without video past `source_idle_reconnect_s` it still
gives up, even though several retry pings were issued. Without this,
retries would loop forever on a truly stuck camera.

QHD/HEVC uses a longer reconnect budget than H.264. WAN relay captures show
that the official app tolerates short HEVC pauses and keeps the existing
session alive; reconnecting too quickly can push a snap camera back through
dormancy/offline signaling, which is much slower than waiting out a temporary
source pause.

This resumes far faster than a full reconnect and avoids re-waking a
battery camera, which is itself flaky right after a dormancy transition.
A full reconnect remains the final escalation if the camera still doesn't
resume.

## Direct-LAN media

On the camera's LAN, TURN is still allocated and advertised as fallback, but the
native app moves QHD media to the direct host pair once ICE nominates it. The
engine mirrors that: it gives startup a short direct-ICE grace before KCP, keeps
rapid ICE checks going while seeking a direct peer, stops candidate fanout once
direct KCP is confirmed, and gives confirmed direct sessions a longer
source-idle window before reconnecting.

## Timing rules

- Camera timestamps can be unstable across stalls, especially with HEVC and
  AUTO. The muxer **generates stable video PTS** at the advertised FPS and
  caps audio lead so audio cannot run seconds ahead while video is stalled.
- **Video PTS stays monotonic across reconnects.** A single muxer feeds one
  unbroken TS stream for the whole live session, but a reconnect restarts the
  ffmpeg child. The muxer carries the PTS base forward instead of resuming at
  0, otherwise consumers see a backward jump — HA's `stream_worker` wrap-
  corrects it by +2³³ and aborts with a "Timestamp discontinuity" (ffplay
  tolerates it, so the debug harness never reproduces this).
- **Do not inject old video frames** to hide loss. It creates visible time
  travel.
- When the source stops sending frames, the integration can only recover
  KCP, request keyframes, reconnect, or wait. It cannot make missing
  high-bitrate camera frames appear without transcoding or fabricating
  video.
- AUTO startup should not publish a tiny stale seed immediately. It should
  wait briefly for the adaptive stream to settle so players open on current
  media.

## MPEG-TS fan-out

- Maintain one P2P session per camera stream and fan out the same MPEG-TS
  TCP stream to all consumers.
- The local TCP listener is health-checked whenever HA requests a stream
  source. If its accept thread stopped unexpectedly, it is recreated and HA
  receives the new port instead of repeatedly opening a dead endpoint.
- New clients need **PAT/PMT plus a current keyframe seed** before live
  packets.
- Idle streams reuse the latest keyframe/still with the same advertised
  stream properties and low refresh cost; they should **not** push still
  frames at high FPS.
- Video is copied. Audio-only encoding (G.711 µ-law → AAC) is expected.
  **Avoid video transcoding** so low-power Home Assistant hosts remain
  usable.
