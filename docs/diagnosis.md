# Diagnosis & Debug Harness

How to reproduce streaming / motion / wake bugs outside Home Assistant, and
what log lines actually mean.

> 🛠 **Agents: keep this file in sync with the code.** Any change to
> `debug.py` flags, the log-line phrases listed below, or the triage
> signals an engineer should grep for must be reflected here in the same
> change. See [AGENTS.md](../AGENTS.md) for the full doc-maintenance
> policy.

## Local repro with `debug.py`

The CLI harness reuses the exact same code path as the HA integration —
same coordinator, same engine, same muxer. If you can repro a problem here,
the fix lands in the right place; if you can't, the problem is in HA glue
(coordinator wiring, entity state, options handling), not in the protocol
stack.

```bash
# Login + list cameras (with category, supported quality profiles, …)
python debug.py list

# Open a live stream for 60 s at QHD and play it with ffplay
python debug.py stream --device-id <id> --duration 60 --quality QHD

# Same, with verbose engine logs (put --debug BEFORE the subcommand)
python debug.py --debug stream --device-id <id> --duration 30
```

Credentials are read from `.env` (`EMAIL`, `PASSWORD`, `COUNTRY_CODE`,
`PHONE_CODE`, `PROFILE`) and can be overridden with `--email`, `--password`,
etc. on the command line.

Useful flags on `stream`:

| Flag | Purpose |
|------|---------|
| `--quality` | `AUTO`, `SD`, `HD`, `QHD`, or a raw profile id. |
| `--wake-timeout` | Seconds to wait for live frames before giving up (default 90, sized for worst-case deep-dormancy wakes). |
| `--video-password` | E2EE password if the camera has it enabled. |
| `--output-file <base>` | Dump `.ts` / `.wav` and recorder/player logs under that basename. |
| `--analysis-mode full` | Also produce TS + PCM diagnostics on top of the ffplay verdict. |
| `--capture [PATH]` | Run `tcpdump` for the session (needs root → prompts for `sudo`). Bare flag writes a `.pcap` next to the artifacts; pass a `PATH` to override. Captures `-i any` (incl. VPN tunnels). |
| `--capture-filter <bpf>` | BPF filter for `--capture` (default `udp` — root discovery, STUN/TURN and P2P; HTTPS API noise skipped). |

## DEBUG logs are noisy

`--debug` enables HTTP + asyncio DEBUG too. When triaging streaming bugs,
grep for the engine-level signals:

```bash
python debug.py --debug stream … 2>&1 | grep -E \
  'Video stalled|skipped gaps|source-idle|session done|Confirmed media peer'
```

## Reading engine signals

| Log line | What it tells you |
|----------|-------------------|
| `P2P session done: video_frames=N source_frames=M` | Once `turn` and `candidates` are populated, `source_frames` is what actually arrived from the camera. If both path fields are empty, the attempt ended before the media leg and zero frames say nothing about the parser or camera source. |
| `Video stalled Xs without KCP gap: udp_idle=…` | When `udp_idle` grows in lockstep, the camera is silent (re-prompt territory) rather than us losing packets. |
| `Confirmed media peer … via direct\|turn` | Tells you whether media is flowing on the LAN directly or through the TURN relay. On the camera's LAN, expect `direct` — signaling/TURN servers are then not in the media path. |
| `Retrying signaling discovery without client UUID` | UUID-aware roots either selected a cluster that does not know the camera or could not connect. The engine is retrying the official generic root-query and MsgSvr-registration shape; the same endpoint may be retried because its registration identity is now different. |
| `Dormant coturn unavailable; waking before relay negotiation` | This camera withholds TURN credentials while dormant. The engine is waking on the current MsgSvr session, waiting briefly for its `online` push, and retrying coturn without changing clusters. |
| `Coturn ready after pre-relay wake` | The older dormant-wake fallback succeeded; TURN allocation and SDP/ICE follow next. |
| `Restarting stale` | HA coordinator watchdog — the engine didn't deliver frames in time. If you see this without retries, dormancy-wake is broken (see [streaming.md](streaming.md)). |
| `skipped gaps` | KCP-level recovery skipped over a missing range to resume at a clean IVA/VVP boundary. A handful is fine; a flood means persistent loss. |
| `source-idle` | The reactive `START_LIVE` retry fired because video stopped flowing. Repeated retries mean the camera is silent at the source. |
| `Unexpected coordinator session loop failure` | A per-camera worker hit an unforeseen error. Its final recovery boundary clears stale awake/motion state and starts a fresh authenticated session; entities should not remain frozen. |
| `Motion event cloud session reauthenticated` | Both motion polling endpoints failed and the listener renewed its cloud credentials before continuing. |
| `Local stream listener stopped` | The HA-facing TCP accept loop exited unexpectedly. The next stream-source request recreates it on a healthy port. |

HA's own `Stream ended; no additional packets` message means that one stream
consumer ran out of MPEG-TS data; `Connection refused` means its saved local
TCP endpoint was no longer listening. These errors are contained by HA's
stream worker and do not themselves stop coordinator polling. The integration
health-checks the listener on the next source request, while coordinator and
HTTP recovery are responsible for keeping entity updates alive.

## Quality matters

Battery cameras stream different codecs depending on the chosen profile:

- **SD → H.264.** Smaller frames, lower bitrate. Tends to stream reliably
  even on weak Wi-Fi / low battery. Useful diagnostic and practical
  fallback.
- **HD / QHD / AUTO → HEVC.** High bitrate, much more sensitive to RF or
  power constraints. Often the first profile to stall on a fragile camera.

Rule of thumb: if QHD shows `source_frames` ≈ a handful per minute, drop to
SD and confirm — if SD works, the limit is the camera/uplink, not the
integration. If SD also drops out, look for KCP gaps, peer-confirmation
issues, or a misrouted TURN relay (see [protocol.md](protocol.md)).

Stream Quality comes from the HTTP capability payload, not from ICE or the P2P
media channel. Modern cameras use `capability.caps.bps2`; the official legacy
fallback exposes raw `HD=0` / `SD=1` when that metadata is absent. The selector
therefore remains available for sparse shared-device payloads and defaults to
legacy SD rather than silently requesting HD.

## When to capture packets

If a behaviour seems to disagree with the official app, capture both sides:

1. Run the official app against the camera and `tcpdump` on the LAN.
2. Run `debug.py stream … --debug --capture` against the same camera — the
   `--capture` flag runs `tcpdump` for the session's lifetime and writes a
   `.pcap` next to the artifacts (no separate terminal needed). Because the
   failure is intermittent, leave `--capture` on across repeated runs until you
   catch a bad one.
3. Diff the SDP, the KCP `sn` ordering at start-of-session, the relay
   addresses in the SDP `m=audio` / `c=IN IP4 …` lines, and the VVP
   `START_LIVE` parameters. When reading the relay leg, mind that media rides
   in **TURN ChannelData**, not raw UDP — see the capture-analysis note in your
   local tooling before concluding the framing is wrong.

Reproduce and store these captures with your local tooling (see
[`AGENTS.local.md`](../AGENTS.local.md) at the repo root); they're the ground
truth when the code disagrees with a vendor change.
