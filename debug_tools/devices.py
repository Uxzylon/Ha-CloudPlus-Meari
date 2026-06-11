"""Device selection helpers for the debug harness."""

from __future__ import annotations

from typing import Any


def _select_device(
    devices: list[dict[str, Any]], device_id: int | None, sn: str | None
) -> dict[str, Any]:
    if not devices:
        raise RuntimeError("No camera devices found")

    if device_id is not None:
        for dev in devices:
            if int(dev.get("deviceID", -1)) == int(device_id):
                return dev
        raise RuntimeError(f"No device found with deviceID={device_id}")

    if sn:
        for dev in devices:
            if str(dev.get("snNum", "")).strip() == sn.strip():
                return dev
        raise RuntimeError(f"No device found with sn={sn}")

    return devices[0]
