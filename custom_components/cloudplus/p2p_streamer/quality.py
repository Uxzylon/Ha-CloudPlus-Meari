"""Quality profile helpers parsed from device capability."""

from __future__ import annotations

import json
from typing import Any


def parse_quality_profiles(device: dict[str, Any]) -> dict[int, str]:
    """Return available stream quality profiles from capability.bps2."""
    try:
        raw = device.get("capability", "")
        capability = json.loads(raw) if isinstance(raw, str) and raw else (raw or {})
        caps = capability.get("caps", {}) if isinstance(capability, dict) else {}
        bps2_raw = caps.get("bps2", "")
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
