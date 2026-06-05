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

## Dormancy wake must be retried

Snap (battery / solar) cameras occasionally miss a single
`send_wake_connect` when deeply dormant. Sending it once and then passively
waiting up to 30 s for the camera to push `status=online` leaves the engine
idle while HA-coordinator-level watchdogs fire `Restarting stale` after
25 s and tear the session down before the camera has a chance to come
back.

**Fix**: re-fire both `sig.send_wake_connect` and the HTTP `wake_device`
every `WAKE_RETRY_S` (≈4 s) inside the dormancy wait, until either the
camera pushes `status=online` or `DORMANCY_WAKE_TIMEOUT_S` (≈45 s) elapses.
The retries are cheap and they shorten the typical post-deep-dormancy wake
to a single session — no need for a coordinator-level restart cycle.

The debug harness `--wake-timeout` default is bumped to 90 s to account for
worst-case deep-dormancy wakes; the HA coordinator already self-restarts so
it doesn't need a wider window.

## Source-idle recovery (sustaining the stream)

The `0x888E` VVP heartbeat keeps the **P2P session** alive but NOT the
video. Many of these cameras have a separate stream-idle timeout and stop
sending video after a short burst (sometimes a single opening keyframe)
unless the live request is periodically re-issued. Reference clients
confirm this: re-send the full live request every ~30 s, and re-issue it
again after a few seconds of video idle, instead of tearing the session
down.

The integration therefore:

- **Proactively** re-issues `START_LIVE` every ~30 s while video flows (HEVC
  `start_live_keepalive_s`), well inside the camera's stream-idle window.
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

This resumes far faster than a full reconnect and avoids re-waking a
battery camera, which is itself flaky right after a dormancy transition.
A full reconnect remains the final escalation if the camera still doesn't
resume.

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
- New clients need **PAT/PMT plus a current keyframe seed** before live
  packets.
- Idle streams reuse the latest keyframe/still with the same advertised
  stream properties and low refresh cost; they should **not** push still
  frames at high FPS.
- Video is copied. Audio-only encoding (G.711 µ-law → AAC) is expected.
  **Avoid video transcoding** so low-power Home Assistant hosts remain
  usable.
