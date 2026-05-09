"""Codec registry for stream payload detection."""

from __future__ import annotations

from .av1 import detect_av1
from .base import DEFAULT_GAP_RECOVERY, CodecSpec, GapRecoveryPolicy
from .h264 import GAP_RECOVERY as H264_GAP_RECOVERY, detect_h264
from .hevc import GAP_RECOVERY as HEVC_GAP_RECOVERY, detect_hevc

REGISTRY: tuple[CodecSpec, ...] = (
    CodecSpec(
        name="h264",
        ffmpeg_demuxer="h264",
        detect=detect_h264,
        recovery=H264_GAP_RECOVERY,
    ),
    CodecSpec(
        name="hevc",
        ffmpeg_demuxer="hevc",
        detect=detect_hevc,
        recovery=HEVC_GAP_RECOVERY,
    ),
    CodecSpec(name="av1", ffmpeg_demuxer="av1", detect=detect_av1),
)


def detect_codec(payload: bytes, default: str = "hevc") -> str:
    for spec in REGISTRY:
        try:
            if spec.detect(payload):
                return spec.name
        except Exception:
            continue
    return default


def demuxer_for(codec_name: str) -> str:
    name = (codec_name or "").lower()
    for spec in REGISTRY:
        if spec.name == name:
            return spec.ffmpeg_demuxer
    return "hevc"


def gap_recovery_for(codec_name: str) -> GapRecoveryPolicy:
    name = (codec_name or "").lower()
    for spec in REGISTRY:
        if spec.name == name:
            return spec.recovery
    return DEFAULT_GAP_RECOVERY
