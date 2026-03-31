"""MQTT motion event parsing helpers.

This module is intentionally Home Assistant-free so it can be validated
with local scripts against captured MQTT payloads.
"""

from __future__ import annotations

import json
from typing import Any

from .const import ALARM_TYPE_NAMES, MOTION_ALARM_TYPES


def _unwrap_event_dict(raw: dict[str, Any]) -> dict[str, Any]:
    """Drill into known wrapper keys until we reach the actual event object."""
    data: Any = raw
    for _ in range(8):
        if not isinstance(data, dict):
            break
        next_data = None
        for key in ("params", "data", "msg", "result"):
            value = data.get(key)
            if isinstance(value, dict):
                next_data = value
                break
        if next_data is None:
            break
        data = next_data
    return data if isinstance(data, dict) else {}


def _pick_first(data: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key in data and data[key] not in (None, ""):
            return data[key]
    return None


def parse_motion_event(payload: bytes) -> dict[str, Any] | None:
    """Parse MQTT payload into a normalized alarm-event dictionary.

    Returns None when payload is not a JSON alarm event payload.
    """
    try:
        raw = json.loads(payload.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None

    if not isinstance(raw, dict):
        return None

    data = _unwrap_event_dict(raw)

    evt_raw = _pick_first(data, ("evt", "eventType", "alarmType"))
    if evt_raw is None:
        evt_raw = _pick_first(raw, ("evt", "eventType", "alarmType"))

    try:
        evt_int = int(evt_raw)
    except (TypeError, ValueError):
        evt_int = -1

    device_id = _pick_first(data, ("deviceID", "deviceId"))
    license_id = _pick_first(data, ("licenseID", "licenseId", "snNum", "sn"))

    device_id_str = str(device_id).strip() if device_id is not None else ""
    license_id_str = str(license_id).strip() if license_id is not None else ""

    evt_name = ALARM_TYPE_NAMES.get(evt_int, f"type={evt_raw}")
    return {
        "evt_raw": evt_raw,
        "evt_int": evt_int,
        "evt_name": evt_name,
        "is_motion": evt_int in MOTION_ALARM_TYPES,
        "device_id": device_id_str,
        "license_id": license_id_str,
        "event": str(raw.get("event", "")).strip(),
        "raw": raw,
    }
