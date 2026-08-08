"""Quality profile helpers parsed from device capability."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

ADAPTIVE_STREAM_ID = 105
BPS2_STREAM_ID_BASE = 100
AUTO_QUALITY_LABEL = "AUTO"
QUALITY_LABELS = {0: "SD", 1: "HD", 2: "QHD"}
LEGACY_QUALITY_LABELS = {
    1: "SD",
    2: "SD",
    3: "SD",
    4: "HD",
    5: "HD",
    6: "FHD",
    7: "FHD",
    8: "HD",
    9: "FHD",
}
LEGACY_QUALITY_DETAILS = {
    1: "360P",
    2: "240P",
    3: "480P",
    4: "720P",
    5: "1080P",
    6: "1080P@1.5M",
    7: "1080P@2M",
    8: "3MP@2M",
    9: "3MP@4M",
}


@dataclass(frozen=True)
class QualityOption:
    """A selectable stream quality option (label, profile and stream id)."""

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


def _decode_json(raw: Any, fallback: Any) -> Any:
    if isinstance(raw, str) and raw:
        return json.loads(raw)
    return raw if raw not in (None, "") else fallback


def _bps2_profiles(device: dict[str, Any]) -> dict[int, str]:
    """Return modern profiles from direct bps2 or the native msc fallback."""
    try:
        caps = _caps(device)
        bps2 = _decode_json(caps.get("bps2"), {})
        if not bps2:
            msc = _decode_json(caps.get("msc"), [])
            entries = [entry for entry in msc if isinstance(entry, dict)]
            if entries:
                selected = min(entries, key=lambda entry: int(entry.get("v_id", 0)))
                bps2 = _decode_json(selected.get("bps2"), {})
        if not isinstance(bps2, dict):
            return {}
        return {int(k): str(v) for k, v in bps2.items()}
    except (ValueError, TypeError, KeyError, AttributeError):
        return {}


def _legacy_quality_profiles(device: dict[str, Any]) -> dict[int, str]:
    """Return raw stream IDs used by pre-bps2 camera capability models."""
    caps = _caps(device)
    try:
        if int(caps.get("vst", 0) or 0) == 1:
            return {0: "HD"}
        bps = int(caps.get("bps", 0) or 0)
        version = int(_capability(device).get("ver", 0) or 0)
    except (TypeError, ValueError):
        return {0: "HD", 1: "SD"}
    profiles = {
        stream_id: (
            f"{LEGACY_QUALITY_LABELS[stream_id]} "
            f"({LEGACY_QUALITY_DETAILS[stream_id]})"
            if version >= 23
            else LEGACY_QUALITY_DETAILS[stream_id]
        )
        for stream_id in range(1, 10)
        if bps & (1 << stream_id)
    }
    return profiles or {0: "HD", 1: "SD"}


def parse_quality_profiles(device: dict[str, Any]) -> dict[int, str]:
    """Return modern bps2 profiles or the official legacy stream fallback."""
    return _bps2_profiles(device) or _legacy_quality_profiles(device)


def best_quality_profile(device: dict[str, Any]) -> int:
    """Pick the highest available profile id."""
    profiles = parse_quality_profiles(device)
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
    except (ValueError, TypeError, KeyError, AttributeError):
        return False


def quality_label(profile_id: int, detail: str = "") -> str:
    """Return the app-facing quality label for a bps2 profile id."""
    return QUALITY_LABELS.get(profile_id, detail or f"Profile {profile_id}")


def quality_options(device: dict[str, Any]) -> list[QualityOption]:
    """Return quality options matching the official app's visible choices."""
    bps2_profiles = _bps2_profiles(device)
    profiles = bps2_profiles or _legacy_quality_profiles(device)
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
                label=(quality_label(profile_id, detail) if bps2_profiles else detail),
                stream_id=(
                    BPS2_STREAM_ID_BASE + profile_id if bps2_profiles else profile_id
                ),
                detail=detail,
            )
        )
    return options


def quality_profile_labels(device: dict[str, Any]) -> dict[int, str]:
    """Return explicit quality labels keyed by modern or legacy profile id."""
    return {
        opt.quality: opt.label
        for opt in quality_options(device)
        if opt.quality is not None
    }


def parse_quality_setting(device: dict[str, Any], raw: Any) -> int | None:
    """Parse a stored quality option into a profile id, or None for Auto."""
    if raw is None or raw == "":
        return default_quality_profile(device)

    text = str(raw).strip()
    if text.upper() == AUTO_QUALITY_LABEL:
        return None

    profiles = parse_quality_profiles(device)
    bps2_profiles = _bps2_profiles(device)
    try:
        profile_id = int(text)
    except (TypeError, ValueError):
        labels = {opt.label.upper(): opt.quality for opt in quality_options(device)}
        return labels.get(text.upper(), default_quality_profile(device))

    if profile_id in profiles:
        return profile_id
    explicit_id = profile_id - BPS2_STREAM_ID_BASE
    if bps2_profiles and explicit_id in profiles:
        return explicit_id
    return default_quality_profile(device)


def default_quality_profile(device: dict[str, Any]) -> int | None:
    """Return the official app's default explicit stream profile."""
    if _bps2_profiles(device):
        return best_quality_profile(device)
    profiles = _legacy_quality_profiles(device)
    for profile_id in (1, 2, 3, 0, 4, 5, 8, 6, 7, 9):
        if profile_id in profiles:
            return profile_id
    return min(profiles)


def stream_id_for_quality(device: dict[str, Any], quality: int | None) -> int:
    """Map our profile id to the VVP stream id used by the Meari SDK."""
    bps2_profiles = _bps2_profiles(device)
    if quality is None:
        if supports_adaptive_stream(device):
            return ADAPTIVE_STREAM_ID
        quality = default_quality_profile(device)

    stream_id = int(quality)
    if stream_id >= BPS2_STREAM_ID_BASE:
        return stream_id
    if bps2_profiles:
        return BPS2_STREAM_ID_BASE + stream_id
    return stream_id
