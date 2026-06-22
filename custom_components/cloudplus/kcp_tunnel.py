#!/usr/bin/env python3
"""
Minimal KCP (KCP protocol) implementation for Meari P2P tunnels.

The Meari P2P SDK (libppsdk.so) uses KCP for reliable data delivery over UDP.
The KCP conversation ID is hardcoded to 0x0c (12).

Protocol layers:
  1. IVA handshake (0xFF 0x01 + session IDs + 0x7012 marker) - sent raw
  2. KCP framing (conv=0x0c) for all subsequent data
  3. IVA data frame (0xFF 0x01 + same IDs + 0x7010 + len + data) as KCP payload
  4. PRTP data (NV_MsgHead + JSON + binary) inside IVA data frame

KCP segment wire format (24-byte header):
  [0x00] conv    4B LE  conversation ID (always 0x0000000c)
  [0x04] cmd     1B     command: PUSH=81, ACK=82, WASK=83, WINS=84
  [0x05] frg     1B     fragment count (0 = last/only fragment)
  [0x06] wnd     2B LE  receive window size
  [0x08] ts      4B LE  timestamp (ms)
  [0x0C] sn      4B LE  sequence number
  [0x10] una     4B LE  unacknowledged sequence number
  [0x14] len     4B LE  payload length
  [0x18] data    ...    payload

Ref: https://github.com/skywind3000/kcp
"""

import logging
import os
import struct
import time

# KCP constants
KCP_CONV = 0x0000000C  # Meari hardcoded conversation ID
KCP_CMD_PUSH = 81  # 0x51 - data
KCP_CMD_ACK = 82  # 0x52 - acknowledgment
KCP_CMD_WASK = 83  # 0x53 - window probe request
KCP_CMD_WINS = 84  # 0x54 - window probe response
KCP_HEADER_SIZE = 24
KCP_MSS = 1176  # Outbound control payloads are tiny; keep plenty of UDP headroom.
KCP_WND = 1024  # Matches the official app's receive window in ACK packets.
KCP_ACK_BATCH_SEGMENTS = 16
KCP_ACK_EVERY_SEGMENTS = 8
KCP_ACK_FLUSH_INTERVAL_S = 0.02
KCP_ACK_PROBE_SEGMENTS = 32

# IVA frame constants
IVA_MAGIC = b"\xff\x01"
IVA_FRAME_SIZE = 20
IVA_TYPE_HANDSHAKE = 0x7012
IVA_TYPE_DATA = 0x7010
VVP_FRAME_MAGIC = b"\x00\x00\x01"
VVP_FRAME_TYPES = {0xF9, 0xFA, 0xFC, 0xFD, 0xFE}

_LOGGER = logging.getLogger(__name__)


def _build_iva_frame(type_marker, data, session_id1=None, session_id2=None):
    """Build an IVA frame with given type and optional data.

    The session IDs (at offsets 4 and 8) must be consistent within a session.
    """
    if session_id1 is None:
        session_id1 = int.from_bytes(os.urandom(4), "little") & 0x0FFFFFFF
    if session_id2 is None:
        session_id2 = int.from_bytes(os.urandom(4), "little") & 0x0FFFFFFF
    header = struct.pack(
        "<BBHI I HH I",
        0xFF,
        0x01,
        0,
        session_id1,
        session_id2,
        0,
        type_marker,
        len(data),
    )
    return header + data


def build_iva_data_frame(data, session_id1=None, session_id2=None):
    """Build an IVA data frame wrapping application data."""
    return _build_iva_frame(IVA_TYPE_DATA, data, session_id1, session_id2)


def build_iva_handshake(session_id1=None, session_id2=None):
    """Build a 20-byte IVA handshake frame."""
    return _build_iva_frame(IVA_TYPE_HANDSHAKE, b"", session_id1, session_id2)


def parse_iva_frame(data):
    """Parse an IVA frame. Returns (type_marker, session_id1, session_id2, payload) or None."""
    if len(data) < IVA_FRAME_SIZE:
        return None
    if data[0] != 0xFF or data[1] != 0x01:
        return None
    session_id1 = struct.unpack_from("<I", data, 0x04)[0]
    session_id2 = struct.unpack_from("<I", data, 0x08)[0]
    type_marker = struct.unpack_from("<H", data, 0x0E)[0]
    data_len = struct.unpack_from("<I", data, 0x10)[0]
    if len(data) < IVA_FRAME_SIZE + data_len:
        return None
    payload = data[IVA_FRAME_SIZE : IVA_FRAME_SIZE + data_len] if data_len > 0 else b""
    return type_marker, session_id1, session_id2, payload


def _starts_complete_message(
    data: bytes,
    *,
    frame_types: set[int] | None = None,
) -> bool:
    if data.startswith(IVA_MAGIC):
        return frame_types is None
    return (
        len(data) >= 4
        and data.startswith(VVP_FRAME_MAGIC)
        and data[3] in VVP_FRAME_TYPES
        and (frame_types is None or data[3] in frame_types)
    )


def build_kcp_segment(cmd, sn=0, una=0, wnd=KCP_WND, ts=0, frg=0, data=b""):
    """Build a KCP segment."""
    header = struct.pack(
        "<IBBHIIII",
        KCP_CONV,
        cmd,
        frg,
        wnd,
        ts,
        sn,
        una,
        len(data),
    )
    return header + data


def parse_kcp_segment(raw):
    """Parse a KCP segment. Returns dict or None."""
    if len(raw) < KCP_HEADER_SIZE:
        return None
    conv, cmd, frg, wnd, ts, sn, una, length = struct.unpack_from("<IBBHIIII", raw, 0)
    if conv != KCP_CONV:
        return None
    payload = raw[KCP_HEADER_SIZE : KCP_HEADER_SIZE + length]
    return {
        "conv": conv,
        "cmd": cmd,
        "frg": frg,
        "wnd": wnd,
        "ts": ts,
        "sn": sn,
        "una": una,
        "len": length,
        "data": payload,
    }


def parse_kcp_segments(raw):
    """Parse one UDP payload that may contain multiple KCP segments."""
    segments = []
    pos = 0
    while pos + KCP_HEADER_SIZE <= len(raw):
        conv, cmd, frg, wnd, ts, sn, una, length = struct.unpack_from(
            "<IBBHIIII", raw, pos
        )
        if conv != KCP_CONV or cmd not in (
            KCP_CMD_PUSH,
            KCP_CMD_ACK,
            KCP_CMD_WASK,
            KCP_CMD_WINS,
        ):
            return []
        end = pos + KCP_HEADER_SIZE + length
        if end > len(raw):
            return []
        segments.append(
            {
                "conv": conv,
                "cmd": cmd,
                "frg": frg,
                "wnd": wnd,
                "ts": ts,
                "sn": sn,
                "una": una,
                "len": length,
                "data": raw[pos + KCP_HEADER_SIZE : end],
            }
        )
        pos = end

    if not segments:
        return []
    trailer = raw[pos:]
    if len(trailer) > 1 and any(trailer):
        return []
    return segments


class KcpTunnel:
    """Minimal KCP tunnel for sending/receiving data over UDP P2P.

    Handles:
      - IVA handshake with consistent session IDs
      - KCP data segments (PUSH/ACK)
      - IVA data framing around application data
    """

    def __init__(self, send_func, *, recv_wnd: int = KCP_WND):
        """
        Args:
            send_func: callable(data: bytes) -> None
                       Function to send raw UDP data to peer.
            recv_wnd: advertised receive window for outbound KCP headers.
        """
        self.send_func = send_func
        self.recv_wnd = max(1, int(recv_wnd))
        self.sn_send = 0  # Next outgoing sequence number
        self.una_recv = 0  # Highest SN seen + 1 (internal tracking)
        self.ts_base = int(time.time() * 1000) & 0xFFFFFFFF
        self.handshake_done = False

        # Local defaults retained for callers that need explicit IVA ids.
        self.session_id1 = int.from_bytes(os.urandom(4), "little") & 0x0FFFFFFF
        self.session_id2 = int.from_bytes(os.urandom(4), "little") & 0x0FFFFFFF

        # Peer's session IDs (from handshake response)
        self.peer_id1 = None
        self.peer_id2 = None

        # Sent segment buffer for retransmission (sn -> raw_segment_bytes)
        self.sent_segments = {}
        # Track which sn have been ACK'd
        self.acked_sns = set()

        # Receive-side fragment reassembly buffer
        # Accumulates KCP fragment data for multi-fragment messages.
        # frg counts DOWN: N-1, N-2, ..., 0.  frg==0 means last fragment.
        self.recv_frag_buf = []
        self.recv_frag_meta = []

        # Receive buffer for out-of-order segments: sn -> (frg, data, ts)
        self.recv_buf = {}
        # Next expected receive sequence number for ordered delivery
        # Initialized to -1 (sentinel); set to peer's first sn on first PUSH
        self.next_recv_sn = -1

        # Queue of fully reassembled messages ready for delivery
        self.recv_queue = []

        # Deferred ACK queue: list of (sn, ts) to send in batch
        self.pending_acks = []
        self.recent_acks = []
        self._last_ack_flush_at = time.monotonic()
        self.last_rx_sn = -1
        self.last_rx_ts = 0

    def _ts(self):
        """Current KCP timestamp."""
        return (int(time.time() * 1000) - self.ts_base) & 0xFFFFFFFF

    def _wnd(self):
        """Advertise free receive slots the way the official app does.

        Only genuine backlog beyond ``next_recv_sn`` occupies the window:
        out-of-order segments held in ``recv_buf`` and completed-but-unread
        messages in ``recv_queue``. Fragments of the message currently being
        reassembled (``recv_frag_buf``) are already in order *below*
        ``next_recv_sn`` — they have been received and acknowledged, so they do
        not occupy receive-window slots. Counting them shrank our advertised
        window by ~one IDR's worth of fragments (a 207 KB QHD keyframe is ~176
        segments) on every keyframe, whereas the official app holds its window
        near-constant at ~1024. On a battery camera that rate-adapts to the
        advertised window, that needless dip throttles the stream.
        """
        used = len(self.recv_buf) + len(self.recv_queue)
        return max(0, self.recv_wnd - used)

    def _remember_ack(self, sn, ts):
        if self.next_recv_sn >= 0 and sn >= self.next_recv_sn:
            in_gap = self.next_recv_sn not in self.recv_buf
            if not in_gap and sn % KCP_ACK_EVERY_SEGMENTS:
                return
        self.pending_acks.append((sn, ts))
        self.recent_acks.append((sn, ts))
        if len(self.recent_acks) > KCP_ACK_PROBE_SEGMENTS * 2:
            del self.recent_acks[:-KCP_ACK_PROBE_SEGMENTS]

    def _append_fragment(self, sn, frg, data):
        self.recv_frag_buf.append(data)
        self.recv_frag_meta.append((sn, frg, len(data)))

    def _clear_fragments(self):
        self.recv_frag_buf = []
        self.recv_frag_meta = []

    def send_handshake(self):
        """Send IVA handshake frame wrapped in KCP PUSH segment."""
        frame = build_iva_handshake()
        self.send_data(frame)

    def wrap_iva(self, data):
        """Wrap data in an IVA data frame like the official app."""
        return build_iva_data_frame(data)

    def send_data(self, data):
        """Send data through KCP. Handles fragmentation if needed."""
        offset = 0
        fragments = []
        while offset < len(data):
            chunk = data[offset : offset + KCP_MSS]
            fragments.append(chunk)
            offset += KCP_MSS

        if not fragments:
            fragments = [b""]

        # Send fragments (frg counts down: n-1, n-2, ..., 0)
        n = len(fragments)
        for i, chunk in enumerate(fragments):
            frg = n - 1 - i
            seg = build_kcp_segment(
                cmd=KCP_CMD_PUSH,
                sn=self.sn_send,
                una=max(self.next_recv_sn, 0),
                wnd=self._wnd(),
                ts=self._ts(),
                frg=frg,
                data=chunk,
            )
            self.sent_segments[self.sn_send] = (chunk, frg)
            self.send_func(seg)
            self.sn_send += 1

    def retransmit_unacked(self):
        """Retransmit all sent segments that haven't been ACK'd yet."""
        for sn in sorted(self.sent_segments.keys()):
            if sn not in self.acked_sns:
                chunk, frg = self.sent_segments[sn]
                seg = build_kcp_segment(
                    cmd=KCP_CMD_PUSH,
                    sn=sn,
                    una=max(self.next_recv_sn, 0),
                    wnd=self._wnd(),
                    ts=self._ts(),
                    frg=frg,
                    data=chunk,
                )
                self.send_func(seg)

    def _mark_remote_una(self, una):
        """Apply the peer's cumulative ACK field to our send buffer."""
        if una <= 0 or not self.sent_segments:
            return
        for sn in [sn for sn in self.sent_segments if sn < una]:
            self.acked_sns.add(sn)
            del self.sent_segments[sn]

    def send_iva_data(self, data):
        """Wrap data in IVA frame, then send through KCP."""
        iva_frame = self.wrap_iva(data)
        self.send_data(iva_frame)

    def poll_data(self):
        """Return the next queued reassembled message, or None."""
        if self.recv_queue:
            return self.recv_queue.pop(0)
        return None

    def ack_flush_due(self) -> bool:
        """Return true when pending ACKs should be sent immediately."""
        if len(self.pending_acks) >= KCP_ACK_BATCH_SEGMENTS:
            return True
        return (
            bool(self.pending_acks)
            and time.monotonic() - self._last_ack_flush_at >= KCP_ACK_FLUSH_INTERVAL_S
        )

    def flush_acks(self):
        """Send all pending ACKs as compound KCP packet(s).

        Called after processing a batch of incoming packets to reduce
        sendto() syscall overhead.  Each compound packet contains multiple
        concatenated 24-byte KCP ACK segments with the latest cumulative UNA.
        """
        if not self.pending_acks:
            return
        self._last_ack_flush_at = time.monotonic()

        # Deduplicate: keep last ts per sn
        by_sn = {}
        for sn, ts in self.pending_acks:
            by_sn[sn] = ts
        self.pending_acks.clear()

        # Use cumulative UNA (next_recv_sn) so the camera knows which
        # segments we're still missing and retransmits them (fast retransmit
        # triggers after fastack >= 2).  The camera's 128-segment send window
        # fills in ~1.2s; fast retransmit recovers gaps in ~200ms.
        # Gap skip at 2s is the safety net for persistent losses.
        una = max(self.next_recv_sn, 0)

        # The SDK tends to ACK in small compounds during media bursts; keeping
        # this tight helps sender-side loss recovery on jittery links.
        buf = bytearray()
        count = 0
        for sn in sorted(by_sn):
            buf += build_kcp_segment(
                cmd=KCP_CMD_ACK,
                sn=sn,
                una=una,
                wnd=self._wnd(),
                ts=by_sn[sn],
            )
            count += 1
            if count >= KCP_ACK_BATCH_SEGMENTS:
                self.send_func(bytes(buf))
                buf.clear()
                count = 0
        if buf:
            self.send_func(bytes(buf))

    def send_gap_nudge(self):
        """Send ACKs for segments above current gap to trigger fast retransmit.

        When next_recv_sn is missing from recv_buf but higher segments exist,
        the camera's KCP should retransmit the missing segment upon receiving
        these "skip" ACKs.  Call periodically when video stalls.
        """
        if self.next_recv_sn < 0 or not self.recv_buf:
            return False
        # Check if there's actually a gap at next_recv_sn
        if self.next_recv_sn in self.recv_buf:
            return False  # No gap — will be assembled on next process_input
        above_gap = sorted(sn for sn in self.recv_buf if sn > self.next_recv_sn)[:96]
        if not above_gap:
            return False
        una = max(self.next_recv_sn, 0)
        buf = bytearray()
        for sn in above_gap:
            seg_ts = self.recv_buf[sn][2]
            buf += build_kcp_segment(
                cmd=KCP_CMD_ACK,
                sn=sn,
                una=una,
                wnd=self._wnd(),
                ts=seg_ts,
            )
            if len(buf) >= 1200:
                self.send_func(bytes(buf))
                buf.clear()
        if buf:
            self.send_func(bytes(buf))
        return True

    def send_ack_probe(self):
        """Repeat recent ACKs as one compound packet to unstick a quiet sender."""
        if self.next_recv_sn < 0 or not self.recent_acks:
            return False
        by_sn = {}
        for sn, ts in self.recent_acks[-KCP_ACK_PROBE_SEGMENTS:]:
            by_sn[sn] = ts
        una = max(self.next_recv_sn, 0)
        buf = bytearray()
        for sn in sorted(by_sn):
            buf += build_kcp_segment(
                cmd=KCP_CMD_ACK,
                sn=sn,
                una=una,
                wnd=self._wnd(),
                ts=by_sn[sn],
            )
            if len(buf) >= 1200:
                self.send_func(bytes(buf))
                buf.clear()
        if buf:
            self.send_func(bytes(buf))
        return True

    def receive_snapshot(self):
        """Return compact receive-side state for transport diagnostics."""
        partial_bytes = sum(size for _, _, size in self.recv_frag_meta)
        first = self.recv_frag_meta[0] if self.recv_frag_meta else None
        last = self.recv_frag_meta[-1] if self.recv_frag_meta else None
        return {
            "next_sn": self.next_recv_sn,
            "last_rx_sn": self.last_rx_sn,
            "recv_buf": len(self.recv_buf),
            "queued": len(self.recv_queue),
            "partial": len(self.recv_frag_buf),
            "partial_bytes": partial_bytes,
            "partial_first": first,
            "partial_last": last,
        }

    def skip_gap(
        self,
        max_gaps: int | None = None,
        *,
        require_iva_start: bool = False,
        start_frame_types: set[int] | None = None,
    ):
        """Skip past persistent gaps to unblock delivery.

        When next_recv_sn is missing but higher segments are buffered,
        advance next_recv_sn to the next available buffered segment.
        By default all visible gaps are skipped (legacy behavior). When
        max_gaps is set, only that many gaps are skipped, which provides a
        lower-loss recovery mode for jittery links.
        When require_iva_start is true, stale fragments before the next IVA
        frame boundary are discarded so lossy recovery never emits a partial
        KCP message as video.
        start_frame_types can narrow that resume point to specific VVP frame
        types, e.g. I-frames during startup recovery.
        Returns True if any gap was skipped and messages may be available.
        """
        if self.next_recv_sn < 0 or not self.recv_buf:
            return False
        if self.next_recv_sn in self.recv_buf:
            return False  # No gap

        any_skipped = False
        total_gaps = 0
        gaps_skipped = 0
        stale_fragments = 0
        first_skip_sn = self.next_recv_sn

        # Loop: skip gap, assemble contiguous run, check for next gap, repeat
        while True:
            # Find the lowest buffered SN above the current gap
            above = sorted(sn for sn in self.recv_buf if sn > self.next_recv_sn)
            if not above:
                break
            new_sn = above[0]
            if require_iva_start:
                resume_sn = -1
                for candidate in above:
                    _, candidate_data, _ = self.recv_buf[candidate]
                    if _starts_complete_message(
                        candidate_data,
                        frame_types=start_frame_types,
                    ):
                        resume_sn = candidate
                        break
                if resume_sn < 0:
                    break
                new_sn = resume_sn
                for candidate in [sn for sn in above if sn < new_sn]:
                    del self.recv_buf[candidate]
                    stale_fragments += 1
            gap_size = new_sn - self.next_recv_sn
            total_gaps += gap_size
            gaps_skipped += 1
            # Discard any partial fragment state (the missing segment
            # was likely a fragment boundary, so existing fragments are stale)
            self._clear_fragments()
            self.next_recv_sn = new_sn
            any_skipped = True

            # Assemble contiguous segments after the gap
            while self.next_recv_sn in self.recv_buf:
                frg, data, _ = self.recv_buf[self.next_recv_sn]
                self._append_fragment(self.next_recv_sn, frg, data)
                del self.recv_buf[self.next_recv_sn]
                self.next_recv_sn += 1
                if frg == 0:
                    complete = b"".join(self.recv_frag_buf)
                    self._clear_fragments()
                    self.recv_queue.append(complete)

            # If next_recv_sn is now in recv_buf (no gap), we're done
            if self.next_recv_sn in self.recv_buf or not self.recv_buf:
                break
            if max_gaps is not None and gaps_skipped >= max_gaps:
                break
            # Otherwise there's another gap — loop and skip it too

        if any_skipped:
            stale_info = f", stale={stale_fragments}" if stale_fragments else ""
            _LOGGER.warning(
                "KCP skipped gaps: sn %s -> %s (%s missing across %s gap(s)), "
                "buf=%s, queued=%s%s",
                first_skip_sn,
                self.next_recv_sn,
                total_gaps,
                gaps_skipped,
                len(self.recv_buf),
                len(self.recv_queue),
                stale_info,
            )
        return any_skipped

    def _decode_complete_message(self, data):
        """Decode a complete KCP message into the legacy process_input result."""
        if len(data) >= IVA_FRAME_SIZE and data[0:2] == IVA_MAGIC:
            iva = parse_iva_frame(data)
            if iva:
                type_marker, id1, id2, payload = iva
                if type_marker == IVA_TYPE_HANDSHAKE:
                    self.handshake_done = True
                    self.peer_id1 = id1
                    self.peer_id2 = id2
                    return ("handshake", (id1, id2))
                if type_marker == IVA_TYPE_DATA:
                    return ("data", payload)
                return ("iva", payload)
        return ("data", data)

    def _process_kcp_segment(self, seg):
        """Process one parsed KCP segment.

        Returns ("messages", list[bytes]) for completed KCP messages, or the
        legacy non-data result tuple for ACK/probe/fragment handling.
        """
        self._mark_remote_una(seg["una"])
        cmd = seg["cmd"]

        if cmd == KCP_CMD_PUSH:
            # Update una_recv
            if seg["sn"] >= self.una_recv:
                self.una_recv = seg["sn"] + 1

            # Store in receive buffer for ordered reassembly
            sn = seg["sn"]
            self.last_rx_sn = sn
            self.last_rx_ts = seg["ts"]

            # Skip duplicate/retransmitted segments we've already processed
            if self.next_recv_sn >= 0 and sn < self.next_recv_sn:
                # Queue ACK with current cumulative una to help sender advance
                self._remember_ack(seg["sn"], seg["ts"])
                return ("dup", sn)

            self.recv_buf[sn] = (seg["frg"], seg["data"], seg["ts"])
            self._remember_ack(seg["sn"], seg["ts"])

            if self.next_recv_sn < 0:
                starts = sorted(
                    candidate_sn
                    for candidate_sn, (_, data, _) in self.recv_buf.items()
                    if _starts_complete_message(data)
                )
                if not starts:
                    return ("fragment", seg["data"])
                self.next_recv_sn = starts[0]
                stale_sns = [old for old in self.recv_buf if old < self.next_recv_sn]
                for stale_sn in stale_sns:
                    del self.recv_buf[stale_sn]
                self._clear_fragments()

            # Try to assemble complete messages from recv_buf
            messages = []
            while self.next_recv_sn in self.recv_buf:
                frg, data, _ = self.recv_buf[self.next_recv_sn]
                self._append_fragment(self.next_recv_sn, frg, data)
                del self.recv_buf[self.next_recv_sn]
                self.next_recv_sn += 1

                if frg == 0:
                    # Last fragment - assemble complete message
                    complete = b"".join(self.recv_frag_buf)
                    self._clear_fragments()
                    messages.append(complete)

            if not messages:
                # No complete message yet - still accumulating fragments
                return ("fragment", seg["data"])

            return ("messages", messages)

        if cmd == KCP_CMD_ACK:
            self.acked_sns.add(seg["sn"])
            # Clean up sent_segments for ACK'd data
            if seg["sn"] in self.sent_segments:
                del self.sent_segments[seg["sn"]]
            return ("ack", seg["sn"])

        if cmd == KCP_CMD_WASK:
            # Window probe request - respond with WINS
            wins = build_kcp_segment(
                cmd=KCP_CMD_WINS,
                sn=self.sn_send,
                una=max(self.next_recv_sn, 0),
                wnd=self._wnd(),
                ts=self._ts(),
            )
            self.send_func(wins)
            return ("wask", None)

        if cmd == KCP_CMD_WINS:
            return ("wins", seg["wnd"])

        return None

    def process_input(self, raw):
        """Process incoming UDP data.

        Returns:
            - ("handshake", (id1, id2)) for IVA handshake
            - ("data", bytes) for complete KCP data
            - ("ack", sn) for ACK
            - None for unrecognized data
        """
        if len(raw) < 4:
            return None

        # Check for IVA frame (handshake/heartbeat)
        if raw[0] == 0xFF and raw[1] == 0x01:
            iva = parse_iva_frame(raw)
            if iva:
                type_marker, id1, id2, payload = iva
                if type_marker == IVA_TYPE_HANDSHAKE:
                    self.handshake_done = True
                    self.peer_id1 = id1
                    self.peer_id2 = id2
                    return ("handshake", (id1, id2))
                if type_marker == IVA_TYPE_DATA:
                    return ("iva_data", payload)
                return ("iva", payload)
            return None

        # Check for KCP segment(s). KCP may bundle several PUSH/ACK segments
        # in a single UDP datagram.
        segments = parse_kcp_segments(raw)
        if not segments:
            return None

        first_result = None
        for seg in segments:
            result = self._process_kcp_segment(seg)
            if not result:
                continue
            if result[0] == "messages":
                for complete in result[1]:
                    if first_result is None:
                        first_result = self._decode_complete_message(complete)
                    else:
                        self.recv_queue.append(complete)
                continue
            if first_result is None:
                first_result = result

        return first_result
