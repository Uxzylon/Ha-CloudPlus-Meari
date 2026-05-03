"""Stream frame decryption, parsing, and HEVC/H.264 NAL-unit utilities."""

from __future__ import annotations

import struct
import threading

from Crypto.Cipher import DES3

from .protocol import (
    STREAM_TYPE_IFRAME,
    STREAM_TYPE_PFRAME,
    STREAM_TYPE_AUDIO,
    STREAM_TYPE_INFO,
    STREAM_ENCRYPT_KEY,
)

# ---------------------------------------------------------------------------
# Stream decryption
# ---------------------------------------------------------------------------

MAX_FRAME_DATA_BYTES = 8 * 1024 * 1024
VIDEO_ENCRYPTED_HEADER_BYTES = 0x80
_TLS = threading.local()


def _stream_cipher():
    cipher = getattr(_TLS, "stream_cipher", None)
    if cipher is None:
        key = STREAM_ENCRYPT_KEY[:24]
        if len(key) < 24:
            key = key + b"\x00" * (24 - len(key))
        cipher = DES3.new(key, DES3.MODE_ECB)
        _TLS.stream_cipher = cipher
    return cipher


def _des3_ecb_decrypt_block(data: bytes, key: bytes = STREAM_ENCRYPT_KEY) -> bytes:
    k = key[:24]
    if len(k) < 24:
        k = k + b"\x00" * (24 - len(k))
    if k == STREAM_ENCRYPT_KEY.ljust(24, b"\x00"):
        return _stream_cipher().decrypt(data)
    return DES3.new(k, DES3.MODE_ECB).decrypt(data)


def _available_encrypted_len(data_len: int, offset: int, limit: int) -> int:
    available = max(0, data_len - offset)
    return min(limit, (available // 8) * 8)


def decrypt_stream_frame(data: bytearray) -> bytearray:
    if len(data) < 4:
        return data
    frame_type = data[3]
    if frame_type == STREAM_TYPE_IFRAME:
        enc_offset = 0x30
        enc_len = _available_encrypted_len(
            len(data), enc_offset, VIDEO_ENCRYPTED_HEADER_BYTES
        )
    elif frame_type == STREAM_TYPE_PFRAME:
        enc_offset = 0x28
        enc_len = _available_encrypted_len(
            len(data), enc_offset, VIDEO_ENCRYPTED_HEADER_BYTES
        )
    elif frame_type == STREAM_TYPE_AUDIO:
        enc_offset = 0x28
        remaining = len(data) - enc_offset
        enc_len = (remaining // 8) * 8
    else:
        return data
    if enc_len < 8 or len(data) < enc_offset + enc_len:
        return data
    encrypted = bytes(data[enc_offset : enc_offset + enc_len])
    decrypted = _des3_ecb_decrypt_block(encrypted, STREAM_ENCRYPT_KEY)
    data[enc_offset : enc_offset + enc_len] = decrypted
    return data


def _find_stream_start(data: bytes, start: int = 0) -> int:
    i = max(0, start)
    while i + 3 < len(data):
        if data[i] == 0 and data[i + 1] == 0 and data[i + 2] == 1:
            if data[i + 3] in (
                STREAM_TYPE_IFRAME,
                STREAM_TYPE_PFRAME,
                STREAM_TYPE_AUDIO,
                STREAM_TYPE_INFO,
            ):
                return i
        i += 1
    return -1


def _peek_video_total_len(frame: bytes, frame_type: int) -> int | None:
    enc_offset = 0x30 if frame_type == STREAM_TYPE_IFRAME else 0x28
    header_size = 0x3C if frame_type == STREAM_TYPE_IFRAME else 0x34
    enc_len = _available_encrypted_len(
        len(frame), enc_offset, VIDEO_ENCRYPTED_HEADER_BYTES
    )
    if enc_len < 16:
        return None
    header = _des3_ecb_decrypt_block(bytes(frame[enc_offset : enc_offset + enc_len]))
    data_len = struct.unpack_from("<I", header, 8)[0]
    if 0 < data_len <= MAX_FRAME_DATA_BYTES:
        return header_size + data_len
    return None


def _peek_audio_total_len(frame: bytes) -> int | None:
    if len(frame) < 0x34:
        return None
    data_len = struct.unpack_from("<I", frame, 0x30)[0]
    if 0 < data_len < 2000:
        return 0x34 + data_len
    if len(frame) < 0x38:
        return None
    header = _des3_ecb_decrypt_block(bytes(frame[0x28:0x38]))
    data_len = struct.unpack_from("<I", header, 8)[0]
    if 0 < data_len < 2000:
        return 0x34 + data_len
    return None


def _peek_frame_total_len(data: bytes, start: int) -> int | None:
    frame_type = data[start + 3]
    frame = data[start:]
    if frame_type in (STREAM_TYPE_IFRAME, STREAM_TYPE_PFRAME):
        return _peek_video_total_len(frame, frame_type)
    if frame_type == STREAM_TYPE_AUDIO:
        return _peek_audio_total_len(frame)
    if frame_type == STREAM_TYPE_INFO and len(frame) >= 8:
        data_len = struct.unpack_from("<H", frame, 6)[0]
        return 8 + data_len
    return None


# ---------------------------------------------------------------------------
# Stream frame parsing
# ---------------------------------------------------------------------------


def parse_stream_frame(data: bytes):
    if len(data) < 8:
        return None
    if data[0] != 0 or data[1] != 0 or data[2] != 1:
        return None
    frame_type = data[3]
    if frame_type == STREAM_TYPE_IFRAME:
        if len(data) < 0x3C:
            return None
        data_len = struct.unpack_from("<I", data, 0x38)[0]
        if data_len > 0 and len(data) < 0x3C + data_len:
            return None
        payload = data[0x3C : 0x3C + data_len] if data_len > 0 else data[0x3C:]
        return (frame_type, 0x3C, payload)
    elif frame_type == STREAM_TYPE_PFRAME:
        if len(data) < 0x34:
            return None
        data_len = struct.unpack_from("<I", data, 0x30)[0]
        if data_len > 0 and len(data) < 0x34 + data_len:
            return None
        payload = data[0x34 : 0x34 + data_len] if data_len > 0 else data[0x34:]
        return (frame_type, 0x34, payload)
    elif frame_type == STREAM_TYPE_AUDIO:
        if len(data) < 0x34:
            return None
        data_len = struct.unpack_from("<I", data, 0x30)[0]
        if data_len > 0 and len(data) < 0x34 + data_len:
            return None
        payload = data[0x34 : 0x34 + data_len] if data_len > 0 else data[0x34:]
        return (frame_type, 0x34, payload)
    elif frame_type == STREAM_TYPE_INFO:
        if len(data) < 8:
            return None
        data_len = struct.unpack_from("<H", data, 6)[0]
        payload = data[8 : 8 + data_len] if data_len > 0 else data[8:]
        return (frame_type, 8, payload)
    return None


def split_stream_frames(data: bytes) -> list[bytes]:
    """Split payload into candidate 00 00 01 frame chunks.

    Camera payloads can contain multiple frame chunks or leading bytes
    before the first frame marker.
    """
    if len(data) < 4:
        return []

    chunks: list[bytes] = []
    pos = 0
    while pos + 3 < len(data):
        start = _find_stream_start(data, pos)
        if start < 0:
            break

        total_len = _peek_frame_total_len(data, start)
        if total_len is not None and start + total_len <= len(data):
            chunks.append(data[start : start + total_len])
            pos = start + total_len
            continue

        next_start = _find_stream_start(data, start + 4)
        if next_start < 0:
            if start == 0:
                chunks.append(data[start:])
            break
        if next_start > start:
            chunks.append(data[start:next_start])
        pos = next_start

    if not chunks:
        return [data]
    return chunks


# ---------------------------------------------------------------------------
# HEVC / H.264 NAL-unit utilities
# ---------------------------------------------------------------------------


def _find_annexb_start_code(data: bytes, start: int) -> tuple[int, int]:
    """Return (index, length) for next Annex-B start code, or (-1, 0)."""
    n = len(data)
    i = max(0, start)
    while i + 3 < n:
        if data[i] == 0 and data[i + 1] == 0:
            if data[i + 2] == 1:
                return (i, 3)
            if i + 3 < n and data[i + 2] == 0 and data[i + 3] == 1:
                return (i, 4)
        i += 1
    return (-1, 0)


def _iter_annexb_nals(data: bytes):
    """Yield NAL unit payloads from Annex-B byte stream."""
    pos = 0
    n = len(data)
    while True:
        start, sc_len = _find_annexb_start_code(data, pos)
        if start < 0:
            break
        nal_start = start + sc_len
        next_start, _ = _find_annexb_start_code(data, nal_start)
        nal_end = next_start if next_start >= 0 else n
        if nal_end > nal_start:
            yield data[nal_start:nal_end]
        if next_start < 0:
            break
        pos = next_start


def is_idr_video_frame(
    frame_type: int, payload: bytes, require_param_sets: bool = True
) -> bool:
    """Best-effort IDR detection for H.264/H.265 Annex-B payloads.

    Some camera streams label intra frames as I-frame without guaranteeing
    a true decoder reset point. For gap recovery, only resume on a verified
    IDR when we can parse NAL units.

    When *require_param_sets* is False the check is relaxed: any frame
    containing an HEVC IDR VCL (NAL type 19 or 20) is accepted even if
    VPS/SPS/PPS are absent.  This is appropriate for gap-skip recovery
    where the coordinator will prepend its cached parameter sets before
    forwarding the frame to ffmpeg.
    """
    if frame_type != STREAM_TYPE_IFRAME:
        return False

    first_sc, _ = _find_annexb_start_code(payload, 0)
    if first_sc < 0 or first_sc > 32:
        return False

    saw_vps = False
    saw_sps = False
    saw_pps = False
    saw_idr = False

    for nal in _iter_annexb_nals(payload):
        if not nal:
            continue
        if len(nal) >= 2 and (nal[1] & 0x07) != 0:
            hevc_type = (nal[0] >> 1) & 0x3F
            if hevc_type == 32:
                saw_vps = True
            elif hevc_type == 33:
                saw_sps = True
            elif hevc_type == 34:
                saw_pps = True
            elif hevc_type in (19, 20):
                saw_idr = True
            continue

        h264_type = nal[0] & 0x1F
        if h264_type == 5:
            return True

    if not require_param_sets:
        return saw_idr
    return saw_idr and (saw_vps or saw_sps or saw_pps)
