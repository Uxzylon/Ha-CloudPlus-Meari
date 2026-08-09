"""``debug.py list`` command: authenticate and print discovered cameras."""

from __future__ import annotations

from .auth import _login_api_with_fallback
from .bootstrap import _bootstrap_integration_modules


async def cmd_list(args) -> int:
    mods = _bootstrap_integration_modules()
    api_cls = mods["api"].MeariApiClient
    quality_options = mods["p2p_streamer"].quality_options

    api = _login_api_with_fallback(api_cls, args)

    print("=" * 78)
    print(f"Profile: {args.profile}")
    print(f"Total devices: {len(api.devices)}")
    print(f"Snap devices: {len(api.get_snap_devices())}")
    if hasattr(api, "get_camera_devices"):
        print(f"Camera devices (snap/ipc/doorbell): {len(api.get_camera_devices())}")
    print("=" * 78)
    for i, dev in enumerate(api.devices.values(), start=1):
        profiles = quality_options(dev)
        profiles_str = (
            ", ".join(
                (
                    opt.label
                    if opt.is_auto
                    else (f"{opt.label}={opt.detail}" if opt.detail else opt.label)
                )
                for opt in profiles
            )
            if profiles
            else "none"
        )
        print(
            f"{i:2d}. id={dev.get('deviceID')} sn={dev.get('snNum')} "
            f"name={dev.get('deviceName')} category={dev.get('_category')}"
        )
        print(f"    quality profiles: {profiles_str}")
        # Show firmware version if available
        fw = dev.get("deviceVersionID", "")
        if fw:
            print(f"    firmware: {fw}")
    return 0


def _parse_quality_arg(
    value: str,
    profiles: dict[int, str] | None = None,
) -> int | None:
    text = str(value).strip().lower()
    if text in {"auto", "adaptive"}:
        return None
    for profile_id, label in (profiles or {}).items():
        normalized = str(label).strip().lower()
        if text == normalized or normalized.startswith(f"{text} "):
            return profile_id
    names = {"sd": 0, "hd": 1, "qhd": 2, "fhd": 2}
    if text in names:
        return names[text]
    return int(text, 0)
