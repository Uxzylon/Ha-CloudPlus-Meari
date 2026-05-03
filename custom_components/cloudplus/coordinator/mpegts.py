"""Small MPEG-TS helpers used by the live muxer."""

from __future__ import annotations

import struct

TS_PACKET_SIZE = 188
PAT_PID = 0x0000
PMT_PID = 0x1000
VIDEO_PID = 0x0100
AUDIO_PID = 0x0101

AAC_SILENCE_FRAME = b"\xff\xf1\x60\x40\x01\x7f\xfc" b"\x01\x18\x20\x07"
AAC_FRAME_TICKS = 5760  # 1024 samples at 16 kHz in 90 kHz clock ticks.


def packet_pid(packet: bytes) -> int:
    return ((packet[1] & 0x1F) << 8) | packet[2]


def packet_pts(packet: bytes) -> int | None:
    if len(packet) != TS_PACKET_SIZE or packet[0] != 0x47 or not (packet[1] & 0x40):
        return None
    afc = (packet[3] >> 4) & 0x03
    payload_off = 4
    if afc & 0x02:
        payload_off = 5 + packet[4]
    if payload_off + 14 > TS_PACKET_SIZE:
        return None
    if packet[payload_off : payload_off + 3] != b"\x00\x00\x01":
        return None
    pts_dts_flags = (packet[payload_off + 7] >> 6) & 0x03
    if pts_dts_flags < 2:
        return None
    return _decode_pts(packet, payload_off + 9)


def packet_has_random_access(packet: bytes) -> bool:
    """Return true when a TS packet marks the start of a random-access frame."""
    if len(packet) != TS_PACKET_SIZE or packet[0] != 0x47:
        return False
    afc = (packet[3] >> 4) & 0x03
    if not (afc & 0x02):
        return False
    adaptation_len = packet[4]
    if adaptation_len < 1 or 5 + adaptation_len > TS_PACKET_SIZE:
        return False
    return bool(packet[5] & 0x40)


def rewrite_video_timing(packet: bytearray, pts_90khz: int) -> None:
    """Rewrite video PCR/PTS/DTS in one TS packet in-place."""
    if len(packet) != TS_PACKET_SIZE or packet[0] != 0x47:
        return

    afc = (packet[3] >> 4) & 0x03
    payload_off = 4
    if afc & 0x02:
        af_len = packet[4]
        payload_off = 5 + af_len
        if af_len >= 7 and (packet[5] & 0x10):
            pcr = pts_90khz & 0x1FFFFFFFF
            packet[6] = (pcr >> 25) & 0xFF
            packet[7] = (pcr >> 17) & 0xFF
            packet[8] = (pcr >> 9) & 0xFF
            packet[9] = (pcr >> 1) & 0xFF
            packet[10] = ((pcr & 1) << 7) | 0x7E
            packet[11] = 0x00

    if not (packet[1] & 0x40) or payload_off + 14 > TS_PACKET_SIZE:
        return
    if packet[payload_off : payload_off + 3] != b"\x00\x00\x01":
        return

    flags = (packet[payload_off + 7] >> 6) & 0x03
    if flags >= 2:
        marker = 0x03 if flags == 3 else 0x02
        _write_pts(packet, payload_off + 9, pts_90khz, marker)
    if flags == 3:
        _write_pts(packet, payload_off + 14, pts_90khz, 0x01)


def _mpegts_crc32(data: bytes) -> int:
    crc = 0xFFFFFFFF
    for byte in data:
        crc ^= byte << 24
        for _ in range(8):
            if crc & 0x80000000:
                crc = ((crc << 1) ^ 0x04C11DB7) & 0xFFFFFFFF
            else:
                crc = (crc << 1) & 0xFFFFFFFF
    return crc


def build_pat_packet(cc: int = 0) -> bytes:
    section = bytearray(
        [
            0x00,
            0xB0,
            0x0D,
            0x00,
            0x01,
            0xC1,
            0x00,
            0x00,
            0x00,
            0x01,
            0xE0 | ((PMT_PID >> 8) & 0x1F),
            PMT_PID & 0xFF,
        ]
    )
    section.extend(_mpegts_crc32(bytes(section)).to_bytes(4, "big"))
    return _psi_packet(PAT_PID, section, cc)


def build_pmt_packet(codec: str, cc: int = 0) -> bytes:
    video_type = 0x1B if codec == "h264" else 0x24
    section = bytearray(
        [
            0x02,
            0xB0,
            0x17,
            0x00,
            0x01,
            0xC1,
            0x00,
            0x00,
            0xE0 | (VIDEO_PID >> 8),
            VIDEO_PID & 0xFF,
            0xF0,
            0x00,
            video_type,
            0xE0 | (VIDEO_PID >> 8),
            VIDEO_PID & 0xFF,
            0xF0,
            0x00,
            0x0F,  # AAC ADTS
            0xE0 | (AUDIO_PID >> 8),
            AUDIO_PID & 0xFF,
            0xF0,
            0x00,
        ]
    )
    section.extend(_mpegts_crc32(bytes(section)).to_bytes(4, "big"))
    return _psi_packet(PMT_PID, section, cc)


def _psi_packet(pid: int, section: bytes, cc: int) -> bytes:
    packet = bytearray(
        [0x47, 0x40 | ((pid >> 8) & 0x1F), pid & 0xFF, 0x10 | (cc & 0x0F), 0x00]
    )
    packet.extend(section)
    packet.extend(b"\xff" * (TS_PACKET_SIZE - len(packet)))
    return bytes(packet)


def _encode_pts(pts_ticks: int) -> bytes:
    pts = pts_ticks & 0x1FFFFFFFF
    return bytes(
        [
            0x20 | ((pts >> 29) & 0x0E) | 1,
            (pts >> 22) & 0xFF,
            ((pts >> 14) & 0xFE) | 1,
            (pts >> 7) & 0xFF,
            ((pts << 1) & 0xFE) | 1,
        ]
    )


def _write_pts(packet: bytearray, off: int, pts_ticks: int, marker: int) -> None:
    if off + 5 > len(packet):
        return
    pts = pts_ticks & 0x1FFFFFFFF
    packet[off] = (marker << 4) | ((pts >> 29) & 0x0E) | 1
    packet[off + 1] = (pts >> 22) & 0xFF
    packet[off + 2] = ((pts >> 14) & 0xFE) | 1
    packet[off + 3] = (pts >> 7) & 0xFF
    packet[off + 4] = ((pts << 1) & 0xFE) | 1


def _decode_pts(data: bytes, off: int) -> int:
    return (
        (((data[off] >> 1) & 0x07) << 30)
        | (data[off + 1] << 22)
        | (((data[off + 2] >> 1) & 0x7F) << 15)
        | (data[off + 3] << 7)
        | ((data[off + 4] >> 1) & 0x7F)
    )


def make_audio_ts(
    frame: bytes,
    pts_90khz: int,
    cc_start: int,
    audio_pid: int = AUDIO_PID,
) -> tuple[bytes, int]:
    if not frame:
        return b"", cc_start

    pes = (
        b"\x00\x00\x01\xc0"
        + struct.pack(">H", 3 + 5 + len(frame))
        + b"\x80\x80\x05"
        + _encode_pts(pts_90khz)
        + frame
    )

    result = bytearray()
    cc = cc_start & 0x0F
    offset = 0
    first = True
    while offset < len(pes):
        remaining = len(pes) - offset
        header = bytearray(
            [
                0x47,
                (0x40 if first else 0x00) | ((audio_pid >> 8) & 0x1F),
                audio_pid & 0xFF,
                0,
            ]
        )
        if remaining < 184:
            spare = 184 - remaining
            header[3] = 0x30 | cc
            if spare == 1:
                adaptation = b"\x00"
            else:
                adaptation = bytes([spare - 1, 0x00]) + (b"\xff" * max(0, spare - 2))
            payload = pes[offset : offset + remaining]
            packet = bytes(header) + adaptation + payload
        else:
            header[3] = 0x10 | cc
            payload = pes[offset : offset + 184]
            packet = bytes(header) + payload

        result.extend(packet)
        offset += len(payload)
        cc = (cc + 1) & 0x0F
        first = False

    return bytes(result), cc
