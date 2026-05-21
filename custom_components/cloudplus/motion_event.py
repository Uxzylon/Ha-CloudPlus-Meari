"""MQTT motion event parsing helpers.

This module is intentionally Home Assistant-free so it can be validated
with local scripts against captured MQTT payloads.
"""

from __future__ import annotations

import json
from typing import Any

from .const import ALARM_TYPE_NAMES, MOTION_ALARM_TYPES

_WRAPPER_KEYS = ("params", "data", "msg", "result", "items", "payload")
_EVENT_KEYS = ("evt", "eventType", "alarmType", "imageAlertType", "alertType")
_DEVICE_KEYS = (
    "deviceID",
    "deviceId",
    "device_id",
    "deviceid",
    "devID",
    "devId",
)
_LICENSE_KEYS = (
    "licenseID",
    "licenseId",
    "snNum",
    "sn",
    "snnum",
    "deviceSN",
    "deviceSn",
    "snIdentifier",
)


def _unwrap_event_dict(raw: dict[str, Any]) -> dict[str, Any]:
    """Drill into known wrapper keys until we reach the actual event object."""
    data: Any = raw
    for _ in range(8):
        if not isinstance(data, dict):
            break
        next_data = None
        for key in _WRAPPER_KEYS:
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


def _dicts(data: Any, limit: int = 64) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    stack = [data]
    seen: set[int] = set()
    while stack and len(out) < limit:
        item = stack.pop(0)
        if not isinstance(item, dict) or id(item) in seen:
            continue
        seen.add(id(item))
        out.append(item)
        for value in item.values():
            if isinstance(value, dict):
                stack.append(value)
            elif isinstance(value, list):
                stack.extend(value)
    return out


def _find_first(data: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for item in _dicts(data):
        value = _pick_first(item, keys)
        if value is not None:
            return value
    return None


def _as_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return -1


def _normalize(
    raw: dict[str, Any], data: dict[str, Any], evt_raw: Any
) -> dict[str, Any]:
    evt_int = _as_int(evt_raw)
    device_id = _pick_first(data, _DEVICE_KEYS)
    license_id = _pick_first(data, _LICENSE_KEYS)
    if device_id is None:
        device_id = _find_first(raw, _DEVICE_KEYS)
    if license_id is None:
        license_id = _find_first(raw, _LICENSE_KEYS)

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

    candidates = [_unwrap_event_dict(raw), raw]
    candidates.extend(_dicts(raw))
    seen: set[int] = set()

    first_event: dict[str, Any] | None = None
    for data in candidates:
        if not data or id(data) in seen:
            continue
        seen.add(id(data))
        evt_raw = _pick_first(data, _EVENT_KEYS)
        if evt_raw is None:
            continue
        event = _normalize(raw, data, evt_raw)
        if first_event is None:
            first_event = event
        if event["is_motion"]:
            return event

    return first_event
