"""Capability and IoT value helpers for Meari devices."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any


def as_int(value: Any, default: int = 0) -> int:
    """Return *value* as int, or *default* when conversion is not possible."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def parse_capabilities(device: Mapping[str, Any]) -> dict[str, Any]:
    """Return the device capability.caps dictionary."""
    raw = device.get("capability") or {}
    try:
        parsed = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, ValueError):
        return {}
    if not isinstance(parsed, Mapping):
        return {}
    caps = parsed.get("caps", parsed)
    return dict(caps) if isinstance(caps, Mapping) else {}


def normalize_iot_values(info: Mapping[Any, Any] | None) -> dict[int | str, Any]:
    """Normalize IoT code keys to integers where possible and unwrap values."""
    if not isinstance(info, Mapping):
        return {}

    values: dict[int | str, Any] = {}
    for key, value in info.items():
        if isinstance(value, Mapping):
            value = value.get("value")
        try:
            key = int(key)
        except (TypeError, ValueError):
            pass
        values[key] = value
    return values


def iot_value(values: Mapping[int | str, Any], code: int | str) -> Any:
    """Look up an IoT value by numeric or string code."""
    try:
        key: int | str = int(code)
    except (TypeError, ValueError):
        key = code
    value = values.get(key)
    if value is None:
        value = values.get(str(code))
    return value


def supports_feature(
    caps: Mapping[str, Any],
    device: Mapping[str, Any],
    feature: str | None,
) -> bool:
    """Return whether a named device feature is advertised by capability.caps."""
    if not feature:
        return False

    def cap(name: str) -> int:
        return as_int(caps.get(name))

    dev_type = as_int(device.get("devTypeID"))
    ver = cap("ver")
    pir = cap("pir")
    flt = cap("flt")
    low_power = (cap("pwm") > 0 or dev_type == 6) if ver >= 6 else dev_type in {4, 5}

    simple = {
        "motion_det": cap("md") > 0,
        "person_det": cap("pdt") > 0,
        "noise_det": cap("nst") > 0,
        "cry_det": cap("cct") > 0,
        "face_det": cap("fcd") > 0,
        "pet_alarm": cap("pet") > 0,
        "abnormal_noise": cap("nms") > 0,
        "ptz_patrol": (cap("pcr") & 4) == 4,
        "status_led": cap("led") > 0,
        "floodlight": flt > 0,
        "siren": cap("sir") > 0,
        "siren_alarm": (cap("sla") & 8) == 8 or (cap("sla") & 16) == 16,
        "rgb_light": cap("rgb") > 0,
        "warm_light": cap("wml") > 0,
        "laser": cap("las") > 0,
        "human_track": cap("ptr") > 0,
        "homekit": cap("hkt") > 0,
        "onvif": cap("ovf") > 0,
        "anti_jamming": cap("ajs") > 0,
        "auto_update": cap("aup") > 0,
        "full_color": cap("fld") > 0,
        "alarm_frequency": cap("afq") > 0,
        "temp_sensor": cap("tmpr") > 0,
        "humidity_sensor": cap("hmd") > 0,
    }
    if feature in simple:
        return simple[feature]

    if feature == "pir":
        return pir not in {5, 7} and flt != 1 and (pir > 0 or cap("plv") > 0)
    if feature == "speaker":
        return cap("ovc") > 0 if ver >= 18 else dev_type in {4, 5}
    if feature == "sd_card":
        return cap("sd") > 0 if ver >= 6 else dev_type != 6
    if feature == "light_brightness":
        ltl = cap("ltl")
        return ltl > 0 if flt != 2 or ver >= 41 else True
    if feature == "sleep_mode":
        return cap("slp") > 0 if ver >= 9 else not low_power
    if feature == "ptz":
        return cap("ptz") > 0 or cap("ptz2") > 0 or bool(device.get("msc"))

    return False
