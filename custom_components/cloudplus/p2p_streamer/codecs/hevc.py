"""HEVC payload detection helpers."""

from __future__ import annotations

from .base import CodecRuntimePolicy, GapRecoveryPolicy

GAP_RECOVERY = GapRecoveryPolicy(
    skip_wait_s=3.6,
    skip_interval_s=1.4,
    keyframe_wait_s=8.0,
)
MPEGTS_STREAM_TYPE = 0x24
PARAM_NAL_TYPES = frozenset({32, 33, 34})
KEYFRAME_NAL_TYPES = frozenset({19, 20})
STARTUP_BACKLOG_FRAMES = 60
TIMESTAMP_TIMED_MUX = True
ADAPTIVE_ARRIVAL_TIMED_MUX = True
ADAPTIVE_TIMESTAMP_TIMED_MUX = False
RUNTIME_POLICY = CodecRuntimePolicy(
    clean_startup_seed=True,
    runtime_stall_timeout_s=18.0,
    source_idle_reconnect_s=8.0,
    startup_seed_min_generations=3,
)


def nal_type(nal: bytes) -> int | None:
    if len(nal) < 2 or (nal[1] & 0x07) == 0:
        return None
    return (nal[0] >> 1) & 0x3F


def live_pacing_buffer(
    device: dict,
    profiles: list[int],
    *,
    adaptive: bool,
) -> float:
    """Return optional mux pacing for HEVC streams."""
    _ = device, profiles
    return 0.8 if adaptive else 0.0


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
        elif nal_type in (0, 1, 16, 17, 18, 19, 20, 21, 39, 40):
            score += 1
    return score >= 4
