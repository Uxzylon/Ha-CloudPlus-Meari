"""Frame split/decrypt/parse utilities."""

from __future__ import annotations

import struct

from Crypto.Cipher import DES3

from .protocol import (
    STREAM_ENCRYPT_KEY,
    STREAM_TYPE_AUDIO,
    STREAM_TYPE_IFRAME,
    STREAM_TYPE_INFO,
    STREAM_TYPE_PFRAME,
)


def _decrypt_block(data: bytes) -> bytes:
    key = STREAM_ENCRYPT_KEY[:24]
    if len(key) < 24:
        key = key + (b"\x00" * (24 - len(key)))
    return DES3.new(key, DES3.MODE_ECB).decrypt(data)


def decrypt_stream_frame(data: bytearray) -> bytearray:
    if len(data) < 4:
        return data
    frame_type = data[3]
    if frame_type == STREAM_TYPE_IFRAME:
        offset, length = 0x30, 0x80
    elif frame_type == STREAM_TYPE_PFRAME:
        offset, length = 0x28, 0x80
    elif frame_type == STREAM_TYPE_AUDIO:
        offset = 0x28
        remain = len(data) - offset
        length = (remain // 8) * 8
    else:
        return data
    if length < 8 or len(data) < offset + length:
        return data
    block = bytes(data[offset : offset + length])
    data[offset : offset + length] = _decrypt_block(block)
    return data


def split_stream_frames(data: bytes) -> list[bytes]:
    if len(data) < 4:
        return []
    starts: list[int] = []
    for i in range(0, len(data) - 3):
        if data[i] == 0 and data[i + 1] == 0 and data[i + 2] == 1:
            if data[i + 3] in (
                STREAM_TYPE_IFRAME,
                STREAM_TYPE_PFRAME,
                STREAM_TYPE_AUDIO,
                STREAM_TYPE_INFO,
            ):
                starts.append(i)
    if not starts:
        return [data]
    parts: list[bytes] = []
    for idx, start in enumerate(starts):
        end = starts[idx + 1] if idx + 1 < len(starts) else len(data)
        if end > start:
            parts.append(data[start:end])
    return parts


def parse_stream_frame(data: bytes) -> tuple[int, bytes] | None:
    if len(data) < 8 or data[0:3] != b"\x00\x00\x01":
        return None
    frame_type = data[3]
    if frame_type == STREAM_TYPE_IFRAME:
        if len(data) < 0x3C:
            return None
        data_len = struct.unpack_from("<I", data, 0x38)[0]
        payload = data[0x3C : 0x3C + data_len] if data_len > 0 else data[0x3C:]
        return (frame_type, payload)
    if frame_type in (STREAM_TYPE_PFRAME, STREAM_TYPE_AUDIO):
        if len(data) < 0x34:
            return None
        data_len = struct.unpack_from("<I", data, 0x30)[0]
        payload = data[0x34 : 0x34 + data_len] if data_len > 0 else data[0x34:]
        return (frame_type, payload)
    if frame_type == STREAM_TYPE_INFO:
        if len(data) < 8:
            return None
        data_len = struct.unpack_from("<H", data, 6)[0]
        payload = data[8 : 8 + data_len] if data_len > 0 else data[8:]
        return (frame_type, payload)
    return None
