"""MPEG-TS bootstrap cache for late-joining live clients."""

from __future__ import annotations

from .mpegts import (
    PAT_PID,
    PMT_PID,
    TS_PACKET_SIZE,
    VIDEO_PID,
    codec_from_pmt_packet,
    packet_has_random_access,
    packet_pid,
)
from ..p2p_streamer.codecs import CodecName, nal_types, spec_for

MAX_BOOTSTRAP_BYTES = 32 * 1024 * 1024


class StreamBootstrap:
    """Keep a clean PAT/PMT + latest-IDR TS seed for new TCP clients."""

    def __init__(self, *, max_bytes: int = MAX_BOOTSTRAP_BYTES) -> None:
        self._max_bytes = max_bytes
        self.reset()

    def reset(self) -> None:
        self._pat_packet = b""
        self._pmt_packet = b""
        self._seed = b""
        self._buf = bytearray()
        self._pending_group = bytearray()
        self._pending_has_idr = False
        self._pending_nal_types: set[int] = set()
        self._pending_probe_tail = b""
        self._generation = 0
        self._frames_since_seed = 0
        self._codec = CodecName.HEVC
        self._seed_strong = False

    def snapshot(self) -> bytes:
        if self._pending_has_idr:
            fresh = self._fresh_pending_seed()
            if fresh:
                return self._trim_to_video_start(fresh)
        if self._seed and self._pending_group:
            combined_len = len(self._seed) + len(self._pending_group)
            if combined_len <= self._max_bytes:
                return self._trim_to_video_start(
                    self._seed + bytes(self._pending_group)
                )
        return self._trim_to_video_start(self._seed)

    def state(self) -> dict[str, int | bool]:
        bytes_ready = len(self._seed)
        if self._seed and self._pending_group:
            bytes_ready = min(
                self._max_bytes + 1,
                bytes_ready + len(self._pending_group),
            )
        return {
            "ready": bool(self._seed),
            "bytes": bytes_ready if bytes_ready <= self._max_bytes else len(self._seed),
            "generation": self._generation,
            "frames_since_seed": self._frames_since_seed,
            "collecting": bool(self._pending_group),
            "pending_has_idr": self._pending_has_idr,
            "strong": self._seed_strong,
        }

    def update(self, data: bytes) -> None:
        usable = (len(data) // TS_PACKET_SIZE) * TS_PACKET_SIZE
        if usable <= 0:
            return

        for off in range(0, usable, TS_PACKET_SIZE):
            packet = data[off : off + TS_PACKET_SIZE]
            if packet[0] != 0x47:
                continue

            pid = packet_pid(packet)
            if pid == PAT_PID:
                self._pat_packet = packet
            elif pid == PMT_PID:
                self._pmt_packet = packet
                codec = codec_from_pmt_packet(packet)
                if codec:
                    self._codec = codec

            if pid == VIDEO_PID and packet[1] & 0x40:
                self._finish_pending_group()
                self._pending_group = bytearray()
                self._pending_has_idr = False
                self._pending_nal_types.clear()
                self._pending_probe_tail = b""

            if self._pending_group:
                self._pending_group.extend(packet)
            elif pid == VIDEO_PID:
                self._pending_group.extend(packet)
            elif self._seed:
                self._append_to_seed(packet)

            if pid == VIDEO_PID:
                self._scan_video_packet(packet)

    def _finish_pending_group(self) -> None:
        if not self._pending_group:
            return

        if self._pending_has_idr and self._pending_has_codec_params():
            self._buf = bytearray()
            if self._pat_packet:
                self._buf.extend(self._pat_packet)
            if self._pmt_packet:
                self._buf.extend(self._pmt_packet)
            self._buf.extend(self._pending_group)
            self._generation += 1
            self._frames_since_seed = 0
            self._seed_strong = True
            self._publish_seed()
        elif self._seed:
            self._append_to_seed(self._pending_group)
            self._frames_since_seed += 1

    def _append_to_seed(self, packet_or_group: bytes | bytearray) -> None:
        self._buf.extend(packet_or_group)
        if len(self._buf) > self._max_bytes:
            self._seed = b""
            self._buf.clear()
            self._seed_strong = False
            return
        self._publish_seed()

    def _publish_seed(self) -> None:
        if len(self._buf) <= self._max_bytes:
            self._seed = bytes(self._buf)

    def _fresh_pending_seed(self) -> bytes:
        if not self._pending_group:
            return b""
        seed = bytearray()
        if self._pat_packet:
            seed.extend(self._pat_packet)
        if self._pmt_packet:
            seed.extend(self._pmt_packet)
        seed.extend(self._pending_group)
        if len(seed) > self._max_bytes:
            return b""
        return bytes(seed)

    def _trim_to_video_start(self, data: bytes) -> bytes:
        if not data:
            return b""
        usable = (len(data) // TS_PACKET_SIZE) * TS_PACKET_SIZE
        if usable <= 0:
            return b""

        first_video = -1
        first_random_access = -1
        for off in range(0, usable, TS_PACKET_SIZE):
            packet = data[off : off + TS_PACKET_SIZE]
            if packet[0] != 0x47 or packet_pid(packet) != VIDEO_PID:
                continue
            if first_video < 0:
                first_video = off
            if packet_has_random_access(packet):
                first_random_access = off
                break

        start = first_random_access if first_random_access >= 0 else first_video
        if start < 0:
            return b""
        if start == 0:
            return data[:usable]

        seed = bytearray()
        if self._pat_packet:
            seed.extend(self._pat_packet)
        if self._pmt_packet:
            seed.extend(self._pmt_packet)
        seed.extend(data[start:usable])
        return bytes(seed)

    def _scan_video_packet(self, packet: bytes) -> None:
        payload = self._video_payload(packet)
        if not payload:
            return
        probe = self._pending_probe_tail + payload
        spec = spec_for(self._codec)
        for nal_type in nal_types(self._codec, probe):
            self._pending_nal_types.add(nal_type)
            if nal_type in spec.keyframe_nal_types:
                self._pending_has_idr = True
        self._pending_probe_tail = probe[-64:]

    def _pending_has_codec_params(self) -> bool:
        return spec_for(self._codec).param_nal_types.issubset(self._pending_nal_types)

    @staticmethod
    def _video_payload(packet: bytes) -> bytes:
        payload = StreamBootstrap._payload(packet)
        if not payload:
            return b""
        if packet[1] & 0x40 and payload[:3] == b"\x00\x00\x01":
            if len(payload) < 9:
                return b""
            return payload[9 + payload[8] :]
        return payload

    @staticmethod
    def _payload(packet: bytes) -> bytes:
        off = 4
        afc = (packet[3] >> 4) & 0x03
        if afc & 0x02:
            off = 5 + packet[4]
        if off >= TS_PACKET_SIZE:
            return b""
        return packet[off:TS_PACKET_SIZE]
