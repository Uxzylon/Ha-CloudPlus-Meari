"""Codec abstractions for stream payload detection and routing."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Callable


class CodecName(str, Enum):
    H264 = "h264"
    HEVC = "hevc"
    AV1 = "av1"

    @classmethod
    def parse(cls, value: "CodecName | str | None") -> "CodecName":
        if isinstance(value, cls):
            return value
        text = str(value or cls.HEVC.value).lower()
        if text == "h265":
            text = cls.HEVC.value
        for codec in cls:
            if codec.value == text:
                return codec
        return cls.HEVC


@dataclass(frozen=True)
class GapRecoveryPolicy:
    skip_wait_s: float
    skip_interval_s: float
    keyframe_wait_s: float
    full_skip_backlog: int = 384
    full_skip_after_s: float = 5.0

    def max_gaps(self, gap_backlog: int, stall_time_s: float) -> int | None:
        if (
            gap_backlog > self.full_skip_backlog
            or stall_time_s > self.full_skip_after_s
        ):
            return None
        return 2


DEFAULT_GAP_RECOVERY = GapRecoveryPolicy(
    skip_wait_s=4.0,
    skip_interval_s=1.2,
    keyframe_wait_s=2.0,
)


@dataclass(frozen=True)
class CodecRuntimePolicy:
    clean_startup_seed: bool = False
    launch_gate_timeout_s: float = 11.0
    severe_gap_window_s: float = 5.8
    fast_frames_fps_factor: float = 0.75
    stable_frames_fps_factor: float = 1.05
    launch_budget_min_s: float = 3.8
    launch_budget_max_s: float = 9.5
    fast_quiet_cap_s: float = 1.5
    runtime_stall_timeout_s: float = 10.0
    source_idle_reconnect_s: float = 14.0
    start_live_keepalive_s: float = 30.0
    idle_start_live_retry_s: float = 5.0
    startup_seed_min_generations: int = 1
    preferred_backlog_reasons: frozenset[str] = frozenset(
        {"ready", "seed-not-fresh", "seed-awaiting-follow-frames"}
    )
    preferred_backlog_min_frames: int = 0
    preferred_backlog_max_video_age_s: float = 999.0
    allow_fast_live_flow: bool = False


DEFAULT_RUNTIME_POLICY = CodecRuntimePolicy()


@dataclass(frozen=True)
class CodecSpec:
    name: CodecName
    ffmpeg_demuxer: str
    detect: Callable[[bytes], bool]
    recovery: GapRecoveryPolicy = DEFAULT_GAP_RECOVERY
    mpegts_stream_type: int = 0x24
    param_nal_types: frozenset[int] = frozenset()
    keyframe_nal_types: frozenset[int] = frozenset()
    nal_type: Callable[[bytes], int | None] | None = None
    startup_backlog_frames: int = 45
    arrival_timed_mux: bool = False
    adaptive_arrival_timed_mux: bool | None = None
    timestamp_timed_mux: bool = False
    adaptive_timestamp_timed_mux: bool | None = None
    runtime: CodecRuntimePolicy = DEFAULT_RUNTIME_POLICY
