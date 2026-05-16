"""Quality profile helpers parsed from device capability."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

ADAPTIVE_STREAM_ID = 105
BPS2_STREAM_ID_BASE = 100
AUTO_QUALITY_LABEL = "AUTO"
QUALITY_LABELS = {0: "SD", 1: "HD", 2: "QHD"}


@dataclass(frozen=True)
class QualityOption:
    quality: int | None
    label: str
    stream_id: int
    detail: str = ""
    is_auto: bool = False


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


def auto_quality_profile(device: dict[str, Any]) -> int | None:
    """Return the app-style Auto profile marker, or the highest profile."""
    return None if supports_adaptive_stream(device) else best_quality_profile(device)


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


def quality_label(profile_id: int, detail: str = "") -> str:
    """Return the app-facing quality label for a bps2 profile id."""
    return QUALITY_LABELS.get(profile_id, detail or f"Profile {profile_id}")


def quality_options(device: dict[str, Any]) -> list[QualityOption]:
    """Return quality options matching the official app's visible choices."""
    profiles = parse_quality_profiles(device)
    options: list[QualityOption] = []
    if supports_adaptive_stream(device):
        options.append(
            QualityOption(
                quality=None,
                label=AUTO_QUALITY_LABEL,
                stream_id=ADAPTIVE_STREAM_ID,
                is_auto=True,
            )
        )
    for profile_id, detail in sorted(profiles.items()):
        options.append(
            QualityOption(
                quality=profile_id,
                label=quality_label(profile_id, detail),
                stream_id=BPS2_STREAM_ID_BASE + profile_id,
                detail=detail,
            )
        )
    return options


def quality_profile_labels(device: dict[str, Any]) -> dict[int, str]:
    """Return explicit quality profile labels keyed by bps2 profile id."""
    return {
        opt.quality: opt.label
        for opt in quality_options(device)
        if opt.quality is not None
    }


def default_quality_profile(device: dict[str, Any]) -> int | None:
    """Default to the highest profile."""
    return best_quality_profile(device)


def stream_id_for_quality(device: dict[str, Any], quality: int | None) -> int:
    """Map our profile id to the VVP stream id used by the Meari SDK."""
    profiles = parse_quality_profiles(device)
    if quality is None:
        if supports_adaptive_stream(device):
            return ADAPTIVE_STREAM_ID
        quality = best_quality_profile(device)

    stream_id = int(quality)
    if stream_id >= BPS2_STREAM_ID_BASE:
        return stream_id
    if profiles:
        return BPS2_STREAM_ID_BASE + stream_id
    return stream_id
