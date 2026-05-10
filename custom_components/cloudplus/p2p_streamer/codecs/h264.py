"""H264 payload detection helpers."""

from __future__ import annotations

from .base import GapRecoveryPolicy

GAP_RECOVERY = GapRecoveryPolicy(
    skip_wait_s=2.2,
    skip_interval_s=1.0,
    keyframe_wait_s=0.0,
)


def _iter_annexb_headers(data: bytes):
    i = 0
    n = len(data)
    while i < n - 3:
        if data[i] == 0 and data[i + 1] == 0:
            if data[i + 2] == 1:
                start = i + 3
            elif i + 3 < n and data[i + 2] == 0 and data[i + 3] == 1:
                start = i + 4
            else:
                i += 1
                continue
            if start < n:
                yield start
                i = start
                continue
        i += 1


def detect_h264(payload: bytes) -> bool:
    score = 0
    for idx, off in enumerate(_iter_annexb_headers(payload)):
        if idx >= 24:
            break
        nal_type = payload[off] & 0x1F
        if nal_type in (7, 8):
            score += 3
        elif nal_type in (1, 5, 6, 9):
            score += 1
    return score >= 4
