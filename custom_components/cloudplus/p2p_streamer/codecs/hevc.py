"""HEVC payload detection helpers."""

from __future__ import annotations


def _iter_annexb_headers(data: bytes):
    i = 0
    n = len(data)
    while i < n - 4:
        if data[i] == 0 and data[i + 1] == 0:
            if data[i + 2] == 1:
                start = i + 3
            elif data[i + 2] == 0 and data[i + 3] == 1:
                start = i + 4
            else:
                i += 1
                continue
            if start < n:
                yield start
                i = start
                continue
        i += 1


def detect_hevc(payload: bytes) -> bool:
    score = 0
    for idx, off in enumerate(_iter_annexb_headers(payload)):
        if idx >= 24 or off + 1 >= len(payload):
            break
        nal_type = (payload[off] >> 1) & 0x3F
        tid_plus1 = payload[off + 1] & 0x07
        if tid_plus1 == 0:
            continue
        if nal_type in (32, 33, 34):
            score += 3
        elif nal_type in (0, 1, 19, 20, 39, 40):
            score += 1
    return score >= 4
