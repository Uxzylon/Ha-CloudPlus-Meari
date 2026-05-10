"""Quality profile helpers parsed from device capability."""

from __future__ import annotations

import json
from typing import Any

ADAPTIVE_STREAM_ID = 105
BPS2_STREAM_ID_BASE = 100


def _capability(device: dict[str, Any]) -> dict[str, Any]:
    raw = device.get("capability", "")
    capability = json.loads(raw) if isinstance(raw, str) and raw else (raw or {})
    return capability if isinstance(capability, dict) else {}


def _caps(device: dict[str, Any]) -> dict[str, Any]:
    caps = _capability(device).get("caps", {})
    return caps if isinstance(caps, dict) else {}


def parse_quality_profiles(device: dict[str, Any]) -> dict[int, str]:
    """Return available stream quality profiles from capability.bps2."""
    try:
        bps2_raw = _caps(device).get("bps2", "")
        bps2 = (
            json.loads(bps2_raw)
            if isinstance(bps2_raw, str) and bps2_raw
            else (bps2_raw or {})
        )
        if not isinstance(bps2, dict):
            return {}
        return {int(k): str(v) for k, v in bps2.items()}
    except Exception:
        return {}


def best_quality_profile(device: dict[str, Any]) -> int:
    """Pick the highest profile id, or 0 if none exists."""
    profiles = parse_quality_profiles(device)
    if not profiles:
        return 0
    return max(profiles.keys())


def safe_quality_profile(device: dict[str, Any]) -> int:
    """Pick the most compatible explicit profile for non-adaptive auto mode."""
    profiles = parse_quality_profiles(device)
    if not profiles:
        return 0
    return min(profiles.keys())


def auto_quality_profile(device: dict[str, Any]) -> int:
    """Pick a conservative non-adaptive default profile."""
    profiles = sorted(parse_quality_profiles(device))
    if not profiles:
        return 0
    if len(profiles) >= 3:
        return profiles[-2]
    return profiles[-1]


def supports_adaptive_stream(device: dict[str, Any]) -> bool:
    """Return true when the SDK exposes stream id 105 (Auto)."""
    try:
        capability = _capability(device)
        return (
            int(capability.get("ver", 0)) >= 81
            and int(_caps(device).get("adb", 0)) == 1
        )
    except Exception:
        return False


def stream_id_for_quality(device: dict[str, Any], quality: int | None) -> int:
    """Map our profile id to the VVP stream id used by the Meari SDK."""
    profiles = parse_quality_profiles(device)
    if quality is None:
        quality = (
            auto_quality_profile(device)
            if supports_adaptive_stream(device)
            else safe_quality_profile(device)
        )

    stream_id = int(quality)
    if stream_id >= BPS2_STREAM_ID_BASE:
        return stream_id
    if profiles:
        return BPS2_STREAM_ID_BASE + stream_id
    return stream_id
