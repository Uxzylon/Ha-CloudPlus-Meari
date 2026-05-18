from __future__ import annotations

import asyncio
import logging
import time
import types
from typing import Any
from urllib.parse import urlparse

from .auth import _login_api_with_fallback
from .bootstrap import _bootstrap_integration_modules
from .devices import _select_device


async def _create_coordinator(args) -> tuple[Any, dict[str, Any], Any]:
    mods = _bootstrap_integration_modules()
    MeariApiClient = mods["api"].MeariApiClient
    CloudEdgeMeariCoordinator = mods["coordinator"].CloudEdgeMeariCoordinator

    api = _login_api_with_fallback(MeariApiClient, args)
    if hasattr(api, "get_camera_devices"):
        devices = api.get_camera_devices()
    else:
        devices = list(api.devices.values())
    dev = _select_device(devices, args.device_id, args.sn)

    loop = asyncio.get_running_loop()
    hass = types.SimpleNamespace(loop=loop)
    coord = CloudEdgeMeariCoordinator(
        hass=hass,
        email=args.email,
        password=args.password,
        device=dev,
        video_password=getattr(args, "video_password", None),
        country_code=args.country_code,
        phone_code=args.phone_code,
        app_profile=args.profile,
        initial_frame_grab=not bool(getattr(args, "skip_initial_grab", False)),
        initial_grab_timeout=12,
        snapshot_conversion_enabled=True,
    )

    await asyncio.to_thread(coord.prefetch_lamp, api)
    await coord.async_start()
    for _ in range(100):
        if coord.stream_port:
            break
        await asyncio.sleep(0.05)

    return coord, dev, mods


async def _camera_stream_source(mods: dict[str, Any], coord: Any) -> str:
    """Build stream URL via integration camera entity code path."""
    CloudEdgeMeariCamera = mods["camera"].CloudEdgeMeariCamera
    entry_stub = types.SimpleNamespace(entry_id="dev-cli")
    entity = CloudEdgeMeariCamera(coord, entry_stub)
    source = await entity.stream_source()
    if not source:
        raise RuntimeError("Camera stream source is unavailable")
    parsed = urlparse(source)
    if parsed.scheme == "tcp" and parsed.port:
        # debug.py runs on the same host as the stream server, so force
        # loopback to avoid LAN-address selection issues.
        source = f"tcp://127.0.0.1:{parsed.port}"
    return source


def _has_live_video(coord: Any, baseline_video_time: float) -> bool:
    """Return True once live video frames have been observed."""
    stream_thread = getattr(coord, "_stream_thread", None)
    stream_active = bool(stream_thread and stream_thread.is_alive())
    last_video_time = float(
        getattr(coord, "_last_p2p_video_time", getattr(coord, "_last_video_time", 0.0))
    )
    has_new_video = last_video_time > baseline_video_time
    is_recent = (time.monotonic() - last_video_time) <= 3.0 if has_new_video else False
    return bool(coord.camera_awake and stream_active and has_new_video and is_recent)


def _maybe_restart_stalled_stream(
    coord: Any,
    baseline_video_time: float,
    stall_started_at: float | None,
    stall_timeout: int,
) -> float | None:
    """Restart stuck P2P sessions that stay alive without delivering video."""
    if _has_live_video(coord, baseline_video_time):
        return None

    stream_thread = getattr(coord, "_stream_thread", None)
    stream_active = bool(stream_thread and stream_thread.is_alive())
    if not stream_active:
        return None

    now = time.monotonic()
    if stall_started_at is None:
        return now

    if now - stall_started_at >= float(stall_timeout):
        p2p = getattr(coord, "_p2p_streamer", None)
        if p2p:
            logging.getLogger(__name__).warning(
                "No live video for %ss, restarting stalled P2P session", stall_timeout
            )
            p2p.request_stop()
        # Immediately queue another wake so coordinator can restart
        # a fresh live session as soon as teardown completes.
        coord.wake_camera()
        return None

    return stall_started_at


async def _await_live_stream(
    coord: Any,
    timeout: int = 45,
    stall_timeout: int = 15,
    wake_retry_interval: int = 20,
) -> bool:
    """Wait until coordinator enters awake live mode and produces video."""
    deadline = time.time() + timeout
    baseline_video_time = float(
        getattr(coord, "_last_p2p_video_time", getattr(coord, "_last_video_time", 0.0))
    )
    stall_started_at: float | None = None
    wake_retries_enabled = wake_retry_interval > 0
    next_wake_retry_at = (
        time.monotonic() + float(wake_retry_interval)
        if wake_retries_enabled
        else float("inf")
    )

    while time.time() < deadline:
        if _has_live_video(coord, baseline_video_time):
            return True

        if stall_timeout > 0:
            stall_started_at = _maybe_restart_stalled_stream(
                coord,
                baseline_video_time,
                stall_started_at,
                stall_timeout,
            )

        now_mono = time.monotonic()
        if wake_retries_enabled and now_mono >= next_wake_retry_at:
            coord.wake_camera()
            next_wake_retry_at = now_mono + max(1.0, float(wake_retry_interval))
        await asyncio.sleep(1)

    return False
