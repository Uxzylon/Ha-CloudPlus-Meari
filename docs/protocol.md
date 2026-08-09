# Protocol Reference

Wire-level notes for the Meari / VVP / PPStrong stack used by CloudEdge,
CloudPlus, Meari, ieGeek and Arenti battery cameras. These are the load-bearing
facts the integration relies on — change them at your peril.

For control-flow patterns built on top of these primitives (live-start
ordering, source-idle recovery, wake retries) see [streaming.md](streaming.md).

> 🛠 **Agents: keep this file in sync with the code.** Any change to
> discovery, signaling, PPCS/ICE/TURN handling, reliable framing, VVP commands
> or media-frame parsing must be reflected here in the same change. See
> [AGENTS.md](../AGENTS.md) for the full doc-maintenance policy.

## Discovery and HTTP API

- App profile selects both the HTTP host family and the VVP stream flag:
  - `cloudedge` → CloudEdge app/API, VVP stream flag `0`
  - `cloudplus` → CloudPlus / CloudHome app/API, VVP stream flag `1`
  - `iegeek` → ieGeek app identity, VVP stream flag `1`
  - `arenti` → Arenti app/API host family, VVP stream flag `1`
- Login starts with the app redirect endpoint, then normal signed Meari API
  requests fetch device lists, IoT config, model values, wake controls and
  OpenAPI credentials.
- Result code `1023` can appear transiently during discovery. The integration
  tries the official fallback discovery paths before treating it as fatal.
- Battery / "snap" cameras need wake commands before P2P video. The official
  app commonly sends both OpenAPI awaken *and* app remote wake — we do the
  same.

## Transport selection

- The device-list payload is authoritative. Cameras advertising
  `deviceP2P=ppcs` use the legacy PPStrong/PPCS transport below. Other cameras
  use the MsgSvr → ICE/TURN → KCP path.
- Do not fall through from PPCS to MsgSvr based on model, age or region. The
  PPCS rendezvous roots and packet key come from that device's `p2pInitApp` or
  `p2pInit` field, so there are no static server or key guesses.

## Legacy PPCS transport

- Factory-9 PPCS cameras wrap their textual DID in `deviceUUID`; decode that
  value and use the first DID token as the rendezvous identity.
- `p2pInitApp` / `p2pInit` decodes to a root hostname list and a packet-cipher
  key. Resolve those roots and query their native UDP service on port `32100`.
  Root replies provide the camera's current LAN/public endpoint candidates.
- Punch each offered endpoint and the nearby port range, matching the official
  client. The confirmed peer is the media path; PPCS does not allocate TURN or
  establish a MsgSvr session first.
- PPCS packets use the protocol's chained substitution cipher. Reliable
  channel packets use `0xD0` data and grouped `0xD1` acknowledgements with
  16-bit per-channel sequence numbers. Retransmit unacknowledged client data,
  reorder received data, and service the native `0xE0` / `0xE1` keepalive.
- Channel `0` carries VVP control. Channel `1` is a continuous legacy media
  byte stream. There is no KCP or IVA layer on this transport.

## Root discovery (MsgSvr endpoints)

- The official apps discover MsgSvr roots through the **native UDP root
  protocol on port 9253**, instead of hardcoding signaling servers.
- The request is an encrypted native message with action/config data
  and may include a stable client UUID or target platform region. Official
  captures also use the generic form with neither field; in that form the
  destination root domain selects the region.
- The response contains one or more MsgSvr endpoints. Those endpoints become
  the TCP signaling candidates.
- The integration tries UUID-aware discovery first for newer shared cameras.
  If those candidates reject the device or cannot connect, it repeats the
  native discovery and MsgSvr registration without the synthesized UUID.
  MsgSvr then assigns the session UUID, matching official CloudEdge captures.
  This avoids a UUID-selected shard masking the account/device cluster while
  preserving compatibility with cameras that require stable identity.
- Prefer root discovery over static IP fallbacks so new regions and server
  rotations keep working without code changes.

## MsgSvr signaling (TCP)

- Signaling is **TCP** to a root-discovered MsgSvr endpoint.
- The client registers with the authenticated account's numeric `userID`, app
  profile/source/version and country. Camera-owner metadata from a shared
  device must not replace that account identity. The UUID-aware compatibility
  pass supplies a stable UUID; the native fallback omits it and uses the UUID
  assigned by MsgSvr. CloudEdge 6.1.4 identifies as `6.1.4a11` on this wire.
- Device status targets the API's explicit `deviceUUID` when available, with
  the normalized serial number as the fallback used by official app data.
- The WebRTC-like flow is: register / hello → device status → coturn
  credentials → SDP offer with local *host*, *mapped* and *relay* candidates
  → SDP answer + trickle → candidate-complete.
- Dormant cameras expose two native wake sequences. Prefer coturn + offer while
  dormant when the server supplies credentials; the SDP answer then confirms
  the wake without waiting 40–60 s for `online`. Older cameras may withhold
  coturn until the client sends signaling + HTTP wake and receives an `online`
  push. Retry coturn on that same MsgSvr session rather than cycling clusters.
  See [streaming.md](streaming.md).
- The two MsgSvr wake-connect frames target the camera, not the NATS route
  identity returned by status: copy the keepalive IP/port/transport into a
  `node=dev`, `domain=<device UUID>` contact. The local-endpoint and
  `awaken_type=1` frames use distinct outer SIDs, matching official captures.
- Some newer shared snap cameras require **stable app-like client
  UUID/session values**. Randomising those can make the camera appear
  offline even when wake commands work.
- A device `status` of `unknown` means this MsgSvr cluster does not know the
  device — same effect as `offline`. Treat both as "try the next candidate"
  rather than abandoning the session.
- **Cross-region shared devices**: a device may live on a different cluster
  than its account region (e.g. an AU account holding a device whose home
  cluster is elsewhere). When UUID-aware candidates report the device
  offline/unknown, repeat generic discovery and registration against
  `{cn,as,eu,us}ce.mearicloud.com` and try the distinct regional endpoints.

## Direct-LAN punch

- When the camera is directly reachable on the LAN, the native app does not
  rely on KCP alone. It first sends a small plaintext msgsvr "connect" frame
  to the camera's `host` candidate over the data socket; the camera replies
  with its UUID. KCP then runs over that same socket.
- Frame: `msgsvr_codec` with node `0xA1`, method `0xB2`, cmd `0xC3`, type
  `0xD1`, unencrypted JSON `{"sid":"<8hex>00000001"}`, sum `&0xff` checksum,
  tail `0x9D`.
- The integration sends this punch to every `host` candidate before the KCP
  handshake and re-asserts it ~1/s until the handshake completes. Off-LAN it
  is harmless because host candidates are private addresses.

## TURN / ICE

- coturn credentials come from MsgSvr. Relay transport in captures is **UDP,
  usually port 9100**.
- The client allocates a relay, creates permissions for camera candidates,
  and channel-binds peers for ChannelData.
- Official captures still allocate TURN on LAN, but good QHD sessions move
  media to the direct host pair once ICE nominates it. The integration keeps
  TURN as fallback while preferring a confirmed direct peer.
- Both sides' relay endpoints are carried by the SDP `m=audio <port>` /
  `c=IN IP4 <ip>` lines, **NOT** by an `a=candidate ... typ srflx` / `typ
  relay` entry. Official offers therefore only enumerate `host` and (when
  available) `srflx` candidates; the relay is implicit via media/connection
  lines. The SDP parser lifts those into a synthetic `relay` candidate so the
  rest of the code can treat candidates uniformly. Do *not* also emit an
  explicit "relay-as-srflx" candidate — that is non-native and unnecessary.
- **Shared-IP relays**: when our client and the camera both allocate TURN on
  the same cluster, the camera's relay candidate can share an IP with our
  TURN server. A naive `peer[0] != turn.server_ip` rejection then drops every
  legitimate relayed KCP packet. For `via_turn=True` packets, trust the
  channel-bound peer — those bytes came from one of our explicitly-bound
  candidates and the source IP has already been validated. The `peer[0] !=
  server_ip` filter is only meaningful for direct (non-TURN) packets, where
  `is_turn_server_stun` already covers TURN-server STUN replies on the same
  socket.
- **ICE connectivity-check cadence matters on LAN.** On the same LAN the
  camera (which is ICE-**controlled**, `xts-ice-1.0.0`) gates video on ICE
  completion: it floods us with `BINDING_REQUEST`s and only starts KCP once
  our nominated check (`USE-CANDIDATE`, ICE-controlling) gets a success
  response. Official captures show the app firing checks roughly every
  **30 ms** until the pair is valid, then around **1 s** as keepalive; app
  responses to camera checks are bare 20-byte Binding successes. A lazy ~2 s
  cadence rarely nominates the direct pair before source-idle reconnect resets
  ICE, so the engine keeps the rapid cadence until a direct peer is confirmed
  (or the direct-seek window expires), then backs off to the keepalive cadence.
  Before startup KCP, the engine gives LAN/VPN host candidates a short direct
  ICE grace and sends initial KCP direct-only once direct STUN succeeds; this
  avoids racing QHD startup onto a relay path the native app would not use.

## KCP / IVA

- Media + control packets after ICE use **KCP conversation `0x0000000c`**.
- KCP payloads wrap IVA frames:
  - `0x7012` — IVA handshake
  - `0x7010` — IVA data carrying VVP payloads
- The receive window seen in official ACKs is `1024`.
- ACK packets are commonly sent as compound batches. Recent WAN QHD captures
  show the app ACKing sparsely with up to 16 ACK segments per compound packet,
  relying on cumulative `una` rather than one ACK for every inbound PUSH.
- When packet loss creates a persistent gap, recovery should resume only at
  a complete IVA/VVP boundary, and preferably at an I-frame for video
  startup.

## VVP

- VVP packets use magic `0x56565099`.
- Important commands:
  - `0x11ff` — start live
  - `0x12ff` — stop live
  - `0x888e` — heartbeat
- Modern `START_LIVE` uses parameter `8`, the camera host key, a formatted
  licence ID, a stream id, and the app-profile stream flag (`0` cloudedge /
  `1` cloudplus, ieGeek and Arenti; see
  [Discovery](#discovery-and-http-api)). Legacy PPCS authenticates with the
  first 16 host-key bytes and omits the licence component, matching the native
  packet exactly.
- Quality stream ids:
  - explicit profiles → `100 + profile_id`
  - app `AUTO` → stream id `105`
  - legacy cameras without `bps2` → raw capability stream ids; if neither
    `bps2` nor a legacy bitmask is advertised, expose `0=HD` / `1=SD` and
    default to `1`, matching the official app
- **AUTO is not our own adaptive switch.** It's the camera/app adaptive
  stream id; request it exactly as the official app does.

## Media frames

- VVP media frame types used by the stream parser:
  - `0xfc` — video I-frame
  - `0xfd` — video P-frame
  - `0xfa` — audio
- PPCS channel 1 instead carries repeated `32-byte little-endian header +
  payload` records. Sequence is at offset `0`, type at `16`, timestamp at
  `20`, and payload length at `28`; types `0xf0`, `0xf1`, and `0xfa` map to
  video I-frame, video P-frame, and audio respectively.
- Video may be H.264 or HEVC. **Detection comes from Annex-B payloads**, not
  from profile names alone.
- Some streams mix encrypted and plain frames. Parse validation must choose
  *per frame* when E2EE state is ambiguous, instead of assuming a session
  has one encryption mode.
- Camera audio is G.711 µ-law. The integration encodes it to AAC for
  MPEG-TS while copying video without transcoding.
