"""Codec abstractions for stream payload detection and routing."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable


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
class CodecSpec:
    name: str
    ffmpeg_demuxer: str
    detect: Callable[[bytes], bool]
    recovery: GapRecoveryPolicy = DEFAULT_GAP_RECOVERY
