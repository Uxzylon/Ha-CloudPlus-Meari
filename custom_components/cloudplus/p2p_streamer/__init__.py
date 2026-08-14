"""Meari P2P streaming stack: discovery, signaling, ICE/TURN, KCP and VVP."""

from .quality import (
    ADAPTIVE_STREAM_ID,
    AUTO_QUALITY_LABEL,
    auto_quality_profile,
    best_quality_profile,
    default_quality_profile,
    parse_quality_setting,
    parse_quality_profiles,
    quality_options,
    quality_profile_labels,
    stream_id_for_quality,
    supports_adaptive_stream,
)
from .session import P2PStreamer

__all__ = [
    "ADAPTIVE_STREAM_ID",
    "AUTO_QUALITY_LABEL",
    "P2PStreamer",
    "auto_quality_profile",
    "best_quality_profile",
    "default_quality_profile",
    "parse_quality_setting",
    "parse_quality_profiles",
    "quality_options",
    "quality_profile_labels",
    "stream_id_for_quality",
    "supports_adaptive_stream",
]
