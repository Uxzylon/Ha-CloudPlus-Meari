"""Codec abstractions for stream payload detection and routing."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class CodecSpec:
    name: str
    ffmpeg_demuxer: str
    detect: Callable[[bytes], bool]
