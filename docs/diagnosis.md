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
| `--wake-timeout` | Seconds to wait for live frames before giving up (default 45, bumped to 90 for deep-dormancy). |
| `--video-password` | E2EE password if the camera has it enabled. |
| `--output-file <base>` | Dump `.ts` / `.wav` and recorder/player logs under that basename. |
| `--analysis-mode full` | Also produce TS + PCM diagnostics on top of the ffplay verdict. |

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
| `P2P session done: video_frames=N source_frames=M` | `source_frames` is what actually arrived from the camera. If it's tiny, the camera stopped sending — not our parser/mux. |
| `Video stalled Xs without KCP gap: udp_idle=…` | When `udp_idle` grows in lockstep, the camera is silent (re-prompt territory) rather than us losing packets. |
| `Confirmed media peer … via direct\|turn` | Tells you whether media is flowing on the LAN directly or through the TURN relay. On the camera's LAN, expect `direct` — signaling/TURN servers are then not in the media path. |
| `Restarting stale` | HA coordinator watchdog — the engine didn't deliver frames in time. If you see this without retries, dormancy-wake is broken (see [streaming.md](streaming.md)). |
| `skipped gaps` | KCP-level recovery skipped over a missing range to resume at a clean IVA/VVP boundary. A handful is fine; a flood means persistent loss. |
| `source-idle` | The reactive `START_LIVE` retry fired because video stopped flowing. Repeated retries mean the camera is silent at the source. |

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

## When to capture packets

If a behaviour seems to disagree with the official app, capture both sides:

1. Run the official app against the camera and `tcpdump` on the LAN.
2. Run `debug.py stream` against the same camera with `--debug` + a
   `tcpdump`.
3. Diff the SDP, the KCP `sn` ordering at start-of-session, the relay
   addresses in the SDP `m=audio` / `c=IN IP4 …` lines, and the VVP
   `START_LIVE` parameters.

Captures live under `reverse_engineering/network_recordings/` (gitignored)
and are the ground truth when the code disagrees with a vendor change.
