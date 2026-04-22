"""MPEG-TS packet building and parsing utilities"""

from __future__ import annotations

import struct
from typing import Any

# ---------------------------------------------------------------------------
# AAC silence constants
# ---------------------------------------------------------------------------

# Pre-computed AAC-LC silence frame (ADTS): 16 kHz, mono.
# 11 bytes, 64 ms duration (1024 samples at 16 kHz).
# Generated via: ffmpeg -f lavfi -i anullsrc=r=16000:cl=mono \
#                -c:a aac -profile:a aac_low -b:a 32k -ar 16000 -ac 1 \
#                -t 0.08 -f adts -
AAC_SILENCE_FRAME: bytes = b"\xff\xf1\x60\x40\x01\x7f\xfc" b"\x01\x18\x20\x07"

# Duration of one AAC-LC frame in 90 kHz PTS ticks: 1024/16000*90000
AAC_FRAME_TICKS: int = 5760  # 64 ms


# ---------------------------------------------------------------------------
# PTS encoding
# ---------------------------------------------------------------------------


def encode_pts(pts_ticks: int) -> bytes:
    """Encode a 33-bit PTS value into the 5-byte PES PTS field."""
    pts = pts_ticks & 0x1FFFFFFFF
    b0 = 0x20 | ((pts >> 29) & 0x0E) | 1
    b1 = (pts >> 22) & 0xFF
    b2 = ((pts >> 14) & 0xFE) | 1
    b3 = (pts >> 7) & 0xFF
    b4 = ((pts << 1) & 0xFE) | 1
    return bytes([b0, b1, b2, b3, b4])


# ---------------------------------------------------------------------------
# Audio TS packet building
# ---------------------------------------------------------------------------


def make_audio_ts(
    frame: bytes,
    pts_90khz: int,
    cc_start: int,
    audio_pid: int = 0x101,
) -> tuple[bytes, int]:
    """Wrap one AAC ADTS frame in PES + MPEG-TS packets.

    Returns ``(ts_bytes, next_continuity_counter)``.
    """
    PKT = 188
    if not frame:
        return b"", cc_start

    # Build PES packet: start-code + stream_id + length + hdr + PTS + data
    pts_bytes = encode_pts(pts_90khz)
    pes = (
        b"\x00\x00\x01\xc0"
        + struct.pack(">H", 3 + 5 + len(frame))
        + b"\x80\x80\x05"
        + pts_bytes
        + frame
    )

    result = bytearray()
    cc = cc_start & 0x0F
    offset = 0
    first = True

    while offset < len(pes):
        remaining = len(pes) - offset
        hdr = bytearray(4)
        hdr[0] = 0x47
        pusi = 0x40 if first else 0x00
        hdr[1] = pusi | ((audio_pid >> 8) & 0x1F)
        hdr[2] = audio_pid & 0xFF

        if remaining < 184:
            # Last packet: adaptation field for stuffing.
            # AF with length=0 is 1 byte; length≥1 needs ≥2 bytes
            # (length + flags).  When remaining == 183 only 1 spare
            # byte is available, so use a zero-length AF.
            spare = 184 - remaining
            if spare == 1:
                # 1-byte AF: adaptation_field_length = 0
                hdr[3] = 0x30 | (cc & 0x0F)
                af = b"\x00"
            else:
                stuff_len = max(0, spare - 2)
                hdr[3] = 0x30 | (cc & 0x0F)
                af = bytearray([1 + stuff_len, 0x00])
                af += bytearray([0xFF] * stuff_len)
            payload = pes[offset : offset + remaining]
            pkt = bytes(hdr) + bytes(af) + payload
        else:
            hdr[3] = 0x10 | (cc & 0x0F)
            payload = pes[offset : offset + 184]
            pkt = bytes(hdr) + payload

        assert len(pkt) == PKT
        result.extend(pkt)
        offset += len(payload)
        cc = (cc + 1) & 0x0F
        first = False

    return bytes(result), cc


def make_silence_audio_ts(
    pts_90khz: int,
    cc_start: int,
    audio_pid: int = 0x101,
) -> tuple[bytes, int]:
    """Convenience: wrap the pre-computed AAC silence frame."""
    return make_audio_ts(AAC_SILENCE_FRAME, pts_90khz, cc_start, audio_pid)


# ---------------------------------------------------------------------------
# CRC / PSI packet building
# ---------------------------------------------------------------------------


def mpegts_crc32(data: bytes) -> int:
    """Compute CRC32/MPEG-2 for MPEG-TS PSI section data."""
    crc = 0xFFFFFFFF
    for byte in data:
        crc ^= byte << 24
        for _ in range(8):
            if crc & 0x80000000:
                crc = ((crc << 1) ^ 0x04C11DB7) & 0xFFFFFFFF
            else:
                crc = (crc << 1) & 0xFFFFFFFF
    return crc


def build_pmt_packet(cc: int, video_codec: str) -> bytes:
    """Build a PMT TS packet listing video + audio PIDs.

    The main muxer outputs video-only MPEG-TS.  Its PMT only lists
    the video PID.  This function builds a replacement PMT that also
    includes the audio PID so players detect both streams.
    """
    VIDEO_PID = 0x0100
    AUDIO_PID = 0x0101
    PMT_PID = 0x1000
    stream_type_v = 0x24 if video_codec != "h264" else 0x1B
    section = bytearray(
        [
            0x02,  # table_id = PMT
            0xB0,
            0x17,  # SSI=1, section_length=23
            0x00,
            0x01,  # program_number = 1
            0xC1,  # version=0, current_next=1
            0x00,
            0x00,  # section / last section = 0
            0xE0 | (VIDEO_PID >> 8),
            VIDEO_PID & 0xFF,  # PCR_PID
            0xF0,
            0x00,  # program_info_length = 0
            stream_type_v,  # video stream type
            0xE0 | (VIDEO_PID >> 8),
            VIDEO_PID & 0xFF,
            0xF0,
            0x00,  # ES_info_length = 0
            0x0F,  # AAC ADTS audio
            0xE0 | (AUDIO_PID >> 8),
            AUDIO_PID & 0xFF,
            0xF0,
            0x00,  # ES_info_length = 0
        ]
    )
    crc = mpegts_crc32(bytes(section))
    section.extend(crc.to_bytes(4, "big"))
    pkt = bytearray(188)
    pkt[0] = 0x47
    pkt[1] = 0x40 | ((PMT_PID >> 8) & 0x1F)  # PUSI=1
    pkt[2] = PMT_PID & 0xFF
    pkt[3] = 0x10 | (cc & 0x0F)
    pkt[4] = 0x00  # pointer field
    pkt[5 : 5 + len(section)] = section
    for i in range(5 + len(section), 188):
        pkt[i] = 0xFF
    return bytes(pkt)


def build_pat_packet(cc: int = 0) -> bytes:
    """Build a PAT TS packet mapping program 1 → PMT PID 0x1000."""
    PMT_PID = 0x1000
    section = bytearray(
        [
            0x00,  # table_id = PAT
            0xB0,
            0x0D,  # SSI=1, section_length=13
            0x00,
            0x01,  # transport_stream_id = 1
            0xC1,  # version=0, current_next=1
            0x00,
            0x00,  # section / last section = 0
            0x00,
            0x01,  # program_number = 1
            0xE0 | ((PMT_PID >> 8) & 0x1F),  # reserved + PMT PID high
            PMT_PID & 0xFF,  # PMT PID low
        ]
    )
    crc = mpegts_crc32(bytes(section))
    section.extend(crc.to_bytes(4, "big"))
    pkt = bytearray(188)
    pkt[0] = 0x47
    pkt[1] = 0x40  # PUSI=1, PID=0x0000
    pkt[2] = 0x00
    pkt[3] = 0x10 | (cc & 0x0F)  # payload only + CC
    pkt[4] = 0x00  # pointer field
    pkt[5 : 5 + len(section)] = section
    for i in range(5 + len(section), 188):
        pkt[i] = 0xFF
    return bytes(pkt)


# ---------------------------------------------------------------------------
# PTS / PCR decoding
# ---------------------------------------------------------------------------


def decode_pts_field(data: bytes, off: int) -> int | None:
    if off < 0 or off + 5 > len(data):
        return None
    return (
        (((data[off] >> 1) & 0x07) << 30)
        | (data[off + 1] << 22)
        | (((data[off + 2] >> 1) & 0x7F) << 15)
        | (data[off + 3] << 7)
        | ((data[off + 4] >> 1) & 0x7F)
    )


def decode_pcr_base(data: bytes, off: int) -> int | None:
    if off < 0 or off + 6 > len(data):
        return None
    return (
        (data[off] << 25)
        | (data[off + 1] << 17)
        | (data[off + 2] << 9)
        | (data[off + 3] << 1)
        | (data[off + 4] >> 7)
    )


# ---------------------------------------------------------------------------
# TS packet rewriting
# ---------------------------------------------------------------------------


def rewrite_video_ts_timing(
    chunk: bytearray,
    off: int,
    pts: int,
) -> None:
    """Rewrite PTS/DTS/PCR in a video TS packet to *pts* (90 kHz).

    Modifies ``chunk`` in-place starting at byte offset ``off``.
    Handles:
    * PCR in the adaptation field (if present)
    * PES PTS and DTS in the PUSI packet's PES header
    """
    PKT = 188

    # -- Adaptation field: rewrite PCR if present --
    afc = (chunk[off + 3] >> 4) & 0x03
    payload_off = off + 4
    if afc & 0x02:  # AF present
        af_len = chunk[off + 4]
        payload_off = off + 5 + af_len
        if af_len >= 7:  # room for PCR
            af_flags = chunk[off + 5]
            if af_flags & 0x10:  # PCR flag set
                p = off + 6
                # PCR_base (33 bits, 90 kHz) + 6 reserved + 9-bit ext
                chunk[p] = (pts >> 25) & 0xFF
                chunk[p + 1] = (pts >> 17) & 0xFF
                chunk[p + 2] = (pts >> 9) & 0xFF
                chunk[p + 3] = (pts >> 1) & 0xFF
                chunk[p + 4] = ((pts & 1) << 7) | 0x7E
                chunk[p + 5] = 0x00

    # -- PES header: rewrite PTS/DTS --
    if not (chunk[off + 1] & 0x40):  # not PUSI
        return
    if payload_off + 9 > off + PKT:
        return
    if (
        chunk[payload_off] != 0
        or chunk[payload_off + 1] != 0
        or chunk[payload_off + 2] != 1
    ):
        return

    pts_dts_flags = (chunk[payload_off + 7] >> 6) & 0x03

    if pts_dts_flags >= 2:  # PTS present
        p = payload_off + 9
        if p + 5 <= off + PKT:
            marker = 0x03 if pts_dts_flags == 3 else 0x02
            chunk[p] = (marker << 4) | ((pts >> 29) & 0x0E) | 0x01
            chunk[p + 1] = (pts >> 22) & 0xFF
            chunk[p + 2] = ((pts >> 14) & 0xFE) | 0x01
            chunk[p + 3] = (pts >> 7) & 0xFF
            chunk[p + 4] = ((pts << 1) & 0xFE) | 0x01

    if pts_dts_flags == 3:  # DTS also present
        p = payload_off + 14
        if p + 5 <= off + PKT:
            chunk[p] = (0x01 << 4) | ((pts >> 29) & 0x0E) | 0x01
            chunk[p + 1] = (pts >> 22) & 0xFF
            chunk[p + 2] = ((pts >> 14) & 0xFE) | 0x01
            chunk[p + 3] = (pts >> 7) & 0xFF
            chunk[p + 4] = ((pts << 1) & 0xFE) | 0x01


def reinterleave_ts(buf: bytearray, audio_pid: int) -> bytes:
    """Re-interleave TS packets so audio is evenly distributed.

    The ffmpeg mpegts muxer outputs video-then-audio blocks during
    startup because the AAC encoder is slower than the video copy
    codec.  This creates a multi-megabyte gap with zero audio TS
    packets, which prevents ffplay from detecting audio during its
    probe window on a real-time TCP stream.

    This method takes the raw buffered TS output and re-orders the
    packets so that audio packets are spread evenly among video
    packets, maintaining the original order within each stream.
    """
    PKT = 188
    n_packets = len(buf) // PKT
    if n_packets == 0:
        return bytes(buf)

    audio_indices: list[int] = []
    other_indices: list[int] = []
    for i in range(n_packets):
        off = i * PKT
        if buf[off] != 0x47:
            other_indices.append(i)
            continue
        pid = ((buf[off + 1] & 0x1F) << 8) | buf[off + 2]
        if pid == audio_pid:
            audio_indices.append(i)
        else:
            other_indices.append(i)

    n_audio = len(audio_indices)
    n_other = len(other_indices)
    if n_audio == 0 or n_other == 0:
        return bytes(buf)

    # Spread audio packets evenly: every (n_other/n_audio) other
    # packets, insert one audio packet.
    result = bytearray(n_packets * PKT)
    ratio = n_other / n_audio  # e.g. 100 video per 1 audio
    ai = 0  # audio index cursor
    oi = 0  # other index cursor
    wi = 0  # write cursor
    next_audio_at = ratio  # insert audio after this many other pkts

    for _ in range(n_packets):
        if ai < n_audio and (oi >= n_other or oi >= next_audio_at):
            src = audio_indices[ai] * PKT
            result[wi : wi + PKT] = buf[src : src + PKT]
            ai += 1
            next_audio_at = (ai + 1) * ratio
        else:
            src = other_indices[oi] * PKT
            result[wi : wi + PKT] = buf[src : src + PKT]
            oi += 1
        wi += PKT

    return bytes(result)


# ---------------------------------------------------------------------------
# TS window analysis
# ---------------------------------------------------------------------------


def summarize_ts_window(
    data: bytes,
    *,
    max_packets: int = 8,
) -> dict[str, Any]:
    PKT = 188
    VIDEO_PID = 0x100
    AUDIO_PID = 0x101
    summary: dict[str, Any] = {
        "total_bytes": len(data),
        "packet_count": len(data) // PKT,
        "starts_with_sync": bool(data[:1] == b"\x47"),
        "sample_packets": [],
        "first_packet_pid": None,
        "last_packet_pid": None,
    }
    for prefix in ("video", "audio"):
        summary[f"{prefix}_packet_count"] = 0
        summary[f"{prefix}_pusi_count"] = 0
        summary[f"{prefix}_first_packet_index"] = None
        summary[f"{prefix}_first_packet_cc"] = None
        summary[f"{prefix}_first_packet_is_pusi"] = None
        summary[f"{prefix}_first_packet_is_rai"] = None
        summary[f"{prefix}_first_pusi_index"] = None
        summary[f"{prefix}_first_rai_index"] = None
        summary[f"{prefix}_first_pts"] = None
        summary[f"{prefix}_last_pts"] = None
        summary[f"{prefix}_first_dts"] = None
        summary[f"{prefix}_last_dts"] = None
        summary[f"{prefix}_first_pcr"] = None
        summary[f"{prefix}_last_pcr"] = None
        summary[f"{prefix}_first_cc"] = None
        summary[f"{prefix}_last_cc"] = None

    n_packets = len(data) // PKT
    for i in range(n_packets):
        off = i * PKT
        if data[off] != 0x47:
            if len(summary["sample_packets"]) < max_packets:
                summary["sample_packets"].append(f"{i}:sync-miss")
            continue

        pid = ((data[off + 1] & 0x1F) << 8) | data[off + 2]
        cc = data[off + 3] & 0x0F
        pusi = bool(data[off + 1] & 0x40)
        afc = (data[off + 3] >> 4) & 0x03
        payload_off = off + 4
        is_rai = False
        pcr = None
        if afc & 0x02:
            af_len = data[off + 4]
            payload_off = min(off + 5 + af_len, off + PKT)
            if af_len >= 1 and off + 5 < off + PKT:
                af_flags = data[off + 5]
                is_rai = bool(af_flags & 0x40)
                if af_len >= 7 and (af_flags & 0x10):
                    pcr = decode_pcr_base(data, off + 6)

        pts = None
        dts = None
        if pusi and payload_off + 9 <= off + PKT:
            if (
                data[payload_off] == 0
                and data[payload_off + 1] == 0
                and data[payload_off + 2] == 1
            ):
                pts_dts_flags = (data[payload_off + 7] >> 6) & 0x03
                if pts_dts_flags >= 2:
                    pts = decode_pts_field(data, payload_off + 9)
                if pts_dts_flags == 3:
                    dts = decode_pts_field(data, payload_off + 14)

        if summary["first_packet_pid"] is None:
            summary["first_packet_pid"] = pid
        summary["last_packet_pid"] = pid

        prefix = None
        if pid == VIDEO_PID:
            prefix = "video"
        elif pid == AUDIO_PID:
            prefix = "audio"

        if prefix is not None:
            summary[f"{prefix}_packet_count"] += 1
            if summary[f"{prefix}_first_packet_index"] is None:
                summary[f"{prefix}_first_packet_index"] = i
                summary[f"{prefix}_first_packet_cc"] = cc
                summary[f"{prefix}_first_packet_is_pusi"] = pusi
                summary[f"{prefix}_first_packet_is_rai"] = is_rai
            if pusi:
                summary[f"{prefix}_pusi_count"] += 1
                if summary[f"{prefix}_first_pusi_index"] is None:
                    summary[f"{prefix}_first_pusi_index"] = i
            if is_rai and summary[f"{prefix}_first_rai_index"] is None:
                summary[f"{prefix}_first_rai_index"] = i
            if summary[f"{prefix}_first_cc"] is None:
                summary[f"{prefix}_first_cc"] = cc
            summary[f"{prefix}_last_cc"] = cc
            if pts is not None:
                if summary[f"{prefix}_first_pts"] is None:
                    summary[f"{prefix}_first_pts"] = pts
                summary[f"{prefix}_last_pts"] = pts
            if dts is not None:
                if summary[f"{prefix}_first_dts"] is None:
                    summary[f"{prefix}_first_dts"] = dts
                summary[f"{prefix}_last_dts"] = dts
            if pcr is not None:
                if summary[f"{prefix}_first_pcr"] is None:
                    summary[f"{prefix}_first_pcr"] = pcr
                summary[f"{prefix}_last_pcr"] = pcr

        if len(summary["sample_packets"]) < max_packets:
            parts = [
                f"{i}:pid=0x{pid:04x}",
                f"cc={cc}",
                f"pusi={1 if pusi else 0}",
            ]
            if is_rai:
                parts.append("rai=1")
            if pcr is not None:
                parts.append(f"pcr={pcr}")
            if pts is not None:
                parts.append(f"pts={pts}")
            if dts is not None and dts != pts:
                parts.append(f"dts={dts}")
            summary["sample_packets"].append(" ".join(parts))

    return summary


def compare_stream_join_boundary(
    seed_summary: dict[str, Any],
    live_summary: dict[str, Any],
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "live_first_packet_pid": live_summary.get("first_packet_pid"),
        "live_first_video_packet_is_pusi": live_summary.get(
            "video_first_packet_is_pusi"
        ),
        "live_first_video_packet_is_rai": live_summary.get("video_first_packet_is_rai"),
        "live_first_video_pusi_index": live_summary.get("video_first_pusi_index"),
        "live_first_audio_packet_is_pusi": live_summary.get(
            "audio_first_packet_is_pusi"
        ),
        "live_first_audio_pusi_index": live_summary.get("audio_first_pusi_index"),
    }
    for prefix in ("video", "audio"):
        seed_last_pts = seed_summary.get(f"{prefix}_last_pts")
        live_first_pts = live_summary.get(f"{prefix}_first_pts")
        if seed_last_pts is not None and live_first_pts is not None:
            pts_gap = int(live_first_pts) - int(seed_last_pts)
            result[f"{prefix}_pts_gap"] = pts_gap
            result[f"{prefix}_pts_gap_s"] = pts_gap / 90000.0
        seed_last_pcr = seed_summary.get(f"{prefix}_last_pcr")
        live_first_pcr = live_summary.get(f"{prefix}_first_pcr")
        if seed_last_pcr is not None and live_first_pcr is not None:
            pcr_gap = int(live_first_pcr) - int(seed_last_pcr)
            result[f"{prefix}_pcr_gap"] = pcr_gap
            result[f"{prefix}_pcr_gap_s"] = pcr_gap / 90000.0
        seed_last_cc = seed_summary.get(f"{prefix}_last_cc")
        live_first_cc = live_summary.get(f"{prefix}_first_cc")
        if seed_last_cc is not None and live_first_cc is not None:
            expected = (int(seed_last_cc) + 1) & 0x0F
            result[f"{prefix}_expected_next_cc"] = expected
            result[f"{prefix}_first_live_cc"] = int(live_first_cc)
            result[f"{prefix}_cc_delta"] = (int(live_first_cc) - expected) & 0x0F
    return result


# ---------------------------------------------------------------------------
# IDR seed validation
# ---------------------------------------------------------------------------


def is_valid_idr_seed(seed: bytes) -> bool:
    """Return True when the cached IDR seed starts on random-access video.

    The bootstrap seed must contain exactly one video access-unit
    start (PUSI) and that first video packet must carry the random
    access indicator, otherwise new clients may start from a
    predictive frame and show decode errors.
    """
    PKT = 188
    VIDEO_PID = 0x100
    if not seed or len(seed) < PKT:
        return False

    video_pusi_count = 0
    first_video_pkt: bytes | None = None
    n_pkts = len(seed) // PKT
    for i in range(n_pkts):
        off = i * PKT
        if seed[off] != 0x47:
            continue
        pid = ((seed[off + 1] & 0x1F) << 8) | seed[off + 2]
        if pid != VIDEO_PID:
            continue
        if first_video_pkt is None:
            first_video_pkt = seed[off : off + PKT]
        if seed[off + 1] & 0x40:
            video_pusi_count += 1

    if first_video_pkt is None or video_pusi_count != 1:
        return False

    afc = (first_video_pkt[3] >> 4) & 0x03
    if not (afc & 0x02):
        return False
    af_len = first_video_pkt[4]
    if af_len < 1:
        return False
    af_flags = first_video_pkt[5]
    return bool(af_flags & 0x40)


# ---------------------------------------------------------------------------
# Video packet extraction
# ---------------------------------------------------------------------------


def extract_video_packets_from_seed(seed: bytes, video_pid: int = 0x100) -> bytes:
    PKT = 188
    out = bytearray()
    for i in range(len(seed) // PKT):
        off = i * PKT
        if seed[off] != 0x47:
            continue
        pid = ((seed[off + 1] & 0x1F) << 8) | seed[off + 2]
        if pid == video_pid:
            out.extend(seed[off : off + PKT])
    return bytes(out)


def extract_video_bytestream_from_ts(data: bytes, video_pid: int = 0x100) -> bytes:
    """Best-effort extract of Annex-B video payload from MPEG-TS packets."""
    PKT = 188
    out = bytearray()
    for i in range(len(data) // PKT):
        off = i * PKT
        if data[off] != 0x47:
            continue
        pid = ((data[off + 1] & 0x1F) << 8) | data[off + 2]
        if pid != video_pid:
            continue
        afc = (data[off + 3] >> 4) & 0x03
        payload_off = off + 4
        if afc & 0x02:
            payload_off = off + 5 + data[off + 4]
        if payload_off >= off + PKT:
            continue
        if data[off + 1] & 0x40:
            if payload_off + 9 > off + PKT:
                continue
            if (
                data[payload_off] != 0
                or data[payload_off + 1] != 0
                or data[payload_off + 2] != 1
            ):
                continue
            pes_header_len = data[payload_off + 8]
            payload_off = payload_off + 9 + pes_header_len
            if payload_off >= off + PKT:
                continue
        out.extend(data[payload_off : off + PKT])
    return bytes(out)
