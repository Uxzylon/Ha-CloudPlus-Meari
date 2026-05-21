"""H264 payload detection helpers."""

from __future__ import annotations

from .base import CodecRuntimePolicy, GapRecoveryPolicy

GAP_RECOVERY = GapRecoveryPolicy(
    skip_wait_s=2.2,
    skip_interval_s=1.0,
    keyframe_wait_s=0.0,
)
MPEGTS_STREAM_TYPE = 0x1B
PARAM_NAL_TYPES = frozenset({7, 8})
KEYFRAME_NAL_TYPES = frozenset({5})
STARTUP_BACKLOG_FRAMES = 45
ARRIVAL_TIMED_MUX = False
RUNTIME_POLICY = CodecRuntimePolicy(
    clean_startup_seed=True,
    launch_gate_timeout_s=14.0,
    severe_gap_window_s=4.2,
    fast_frames_fps_factor=0.55,
    stable_frames_fps_factor=0.85,
    launch_budget_min_s=4.5,
    launch_budget_max_s=8.0,
    fast_quiet_cap_s=1.25,
    runtime_stall_timeout_s=14.0,
    source_idle_reconnect_s=14.0,
    preferred_backlog_reasons=frozenset({"ready", "seed-not-fresh"}),
    preferred_backlog_min_frames=2,
    preferred_backlog_max_video_age_s=0.8,
    allow_fast_live_flow=True,
    startup_seed_min_generations=2,
)


def live_pacing_buffer(
    device: dict,
    profiles: list[int],
    *,
    adaptive: bool,
) -> float:
    """Use mux timestamps directly for low-latency H.264 playback."""
    _ = device, profiles, adaptive
    return 0.0


def nal_type(nal: bytes) -> int | None:
    return (nal[0] & 0x1F) if nal else None


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
