from __future__ import annotations

import argparse
import asyncio
import importlib.util
import logging
import os
import re
import signal
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import types
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

REPO_ROOT = Path(__file__).resolve().parent


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if not spec or not spec.loader:
        raise RuntimeError(f"Cannot load module: {name} from {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def _load_package(pkg_name: str, pkg_dir: Path):
    """Load a Python sub-package directory (containing __init__.py).

    Sets up the package in sys.modules with its proper __path__ so that
    relative imports inside the package's __init__.py and sub-modules
    resolve correctly without requiring the full HA runtime.
    """
    pkg_mod = sys.modules.get(pkg_name)
    if pkg_mod is None:
        pkg_mod = types.ModuleType(pkg_name)
        sys.modules[pkg_name] = pkg_mod
    pkg_mod.__path__ = [str(pkg_dir)]
    pkg_mod.__package__ = pkg_name
    pkg_mod.__file__ = str(pkg_dir / "__init__.py")

    init_path = pkg_dir / "__init__.py"
    spec = importlib.util.spec_from_file_location(
        pkg_name,
        init_path,
        submodule_search_locations=[str(pkg_dir)],
    )
    if not spec or not spec.loader:
        raise RuntimeError(f"Cannot load package: {pkg_name} from {pkg_dir}")
    spec.loader.exec_module(pkg_mod)
    return pkg_mod


def _bootstrap_integration_modules() -> dict[str, Any]:
    """Load integration modules without requiring Home Assistant runtime."""
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))

    # Stub minimal Home Assistant typing dependency used by coordinator.
    ha_pkg = sys.modules.setdefault("homeassistant", types.ModuleType("homeassistant"))
    if not hasattr(ha_pkg, "__path__"):
        ha_pkg.__path__ = []
    if "homeassistant.core" not in sys.modules:
        core_mod = types.ModuleType("homeassistant.core")

        class HomeAssistant:  # pragma: no cover - runtime stub
            pass

        def callback(func):  # pragma: no cover - runtime stub
            return func

        core_mod.HomeAssistant = HomeAssistant
        core_mod.callback = callback
        sys.modules["homeassistant.core"] = core_mod

    # Stubs needed by camera entity module.
    if "homeassistant.components" not in sys.modules:
        components_mod = types.ModuleType("homeassistant.components")
        components_mod.__path__ = []
        sys.modules["homeassistant.components"] = components_mod
    if "homeassistant.components.camera" not in sys.modules:
        camera_mod = types.ModuleType("homeassistant.components.camera")

        class Camera:  # pragma: no cover - runtime stub
            def __init__(self) -> None:
                pass

        class CameraEntityFeature:  # pragma: no cover - runtime stub
            STREAM = 2

        camera_mod.Camera = Camera
        camera_mod.CameraEntityFeature = CameraEntityFeature
        sys.modules["homeassistant.components.camera"] = camera_mod

    if "homeassistant.config_entries" not in sys.modules:
        cfg_mod = types.ModuleType("homeassistant.config_entries")

        class ConfigEntry:  # pragma: no cover - runtime stub
            pass

        cfg_mod.ConfigEntry = ConfigEntry
        sys.modules["homeassistant.config_entries"] = cfg_mod

    if "homeassistant.helpers" not in sys.modules:
        helpers_mod = types.ModuleType("homeassistant.helpers")
        helpers_mod.__path__ = []
        sys.modules["homeassistant.helpers"] = helpers_mod
    if "homeassistant.helpers.entity_platform" not in sys.modules:
        ep_mod = types.ModuleType("homeassistant.helpers.entity_platform")
        ep_mod.AddEntitiesCallback = object
        sys.modules["homeassistant.helpers.entity_platform"] = ep_mod

    cc_pkg = sys.modules.setdefault(
        "custom_components", types.ModuleType("custom_components")
    )
    cc_pkg.__path__ = [str(REPO_ROOT / "custom_components")]

    cloudplus_pkg = sys.modules.setdefault(
        "custom_components.cloudplus", types.ModuleType("custom_components.cloudplus")
    )
    cloudplus_pkg.__path__ = [str(REPO_ROOT / "custom_components" / "cloudplus")]

    base = REPO_ROOT / "custom_components" / "cloudplus"
    modules = {
        "const": _load_module("custom_components.cloudplus.const", base / "const.py"),
        "api": _load_module("custom_components.cloudplus.api", base / "api.py"),
        "kcp_tunnel": _load_module(
            "custom_components.cloudplus.kcp_tunnel", base / "kcp_tunnel.py"
        ),
        "meari_signaling": _load_module(
            "custom_components.cloudplus.meari_signaling", base / "meari_signaling.py"
        ),
        "turn_client": _load_module(
            "custom_components.cloudplus.turn_client", base / "turn_client.py"
        ),
        "p2p_streamer": _load_package(
            "custom_components.cloudplus.p2p_streamer", base / "p2p_streamer"
        ),
        "coordinator": _load_package(
            "custom_components.cloudplus.coordinator", base / "coordinator"
        ),
        "camera": _load_module(
            "custom_components.cloudplus.camera", base / "camera.py"
        ),
    }
    return modules


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


class CpuSampler:
    """Lightweight CPU sampler based on /proc without extra dependencies."""

    def __init__(self, pid_provider: Callable[[], list[int]], interval: float = 1.0):
        self._pid_provider = pid_provider
        self._interval = interval
        self._running = False
        self._task: asyncio.Task | None = None
        self._samples: list[float] = []
        self._last: dict[int, tuple[int, float]] = {}
        self._clk_tck = os.sysconf(os.sysconf_names["SC_CLK_TCK"])

    @staticmethod
    def _read_ticks(pid: int) -> int | None:
        try:
            with open(f"/proc/{pid}/stat", "r", encoding="utf-8") as f:
                fields = f.read().split()
            utime = int(fields[13])
            stime = int(fields[14])
            return utime + stime
        except Exception:
            return None

    async def _run(self) -> None:
        while self._running:
            now = time.time()
            total_pct = 0.0
            for pid in self._pid_provider():
                ticks = self._read_ticks(pid)
                if ticks is None:
                    continue
                prev = self._last.get(pid)
                self._last[pid] = (ticks, now)
                if not prev:
                    continue
                dticks = ticks - prev[0]
                dtime = now - prev[1]
                if dtime > 0:
                    total_pct += (dticks / self._clk_tck) / dtime * 100.0
            self._samples.append(total_pct)
            await asyncio.sleep(self._interval)

    def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> dict[str, float]:
        self._running = False
        if self._task:
            await self._task
        if not self._samples:
            return {"avg": 0.0, "max": 0.0}
        return {
            "avg": sum(self._samples) / len(self._samples),
            "max": max(self._samples),
        }


class StreamHealthTracker:
    """Track live video cadence and report freeze windows."""

    def __init__(
        self,
        logger: logging.Logger,
        *,
        label: str = "Video",
        frame_ts_reader: Callable[[Any], float] | None = None,
        frame_count_reader: Callable[[Any], int] | None = None,
        emit_logs: bool = True,
    ):
        self._logger = logger
        self._label = label
        self._frame_ts_reader = frame_ts_reader or self._default_frame_ts_reader
        self._frame_count_reader = (
            frame_count_reader or self._default_frame_count_reader
        )
        self._emit_logs = bool(emit_logs)
        self._first_frame_ts: float = 0.0
        self._last_frame_ts: float = 0.0
        self._last_frame_count: int = 0
        self._stall_start_ts: float | None = None
        self._last_stall_log_mono: float = 0.0
        self._stalls_over_1s: int = 0
        self._stalls_over_3s: int = 0
        self._recovered_stalls: int = 0
        self._max_gap_s: float = 0.0

    @staticmethod
    def _default_frame_ts_reader(coord: Any) -> float:
        return float(
            getattr(
                coord, "_last_p2p_video_time", getattr(coord, "_last_video_time", 0.0)
            )
        )

    @staticmethod
    def _default_frame_count_reader(coord: Any) -> int:
        return int(getattr(coord, "_p2p_video_frames", 0))

    def _record_stall(self, stall_s: float) -> None:
        self._max_gap_s = max(self._max_gap_s, stall_s)
        self._recovered_stalls += 1
        if stall_s >= 1.0:
            self._stalls_over_1s += 1
        if stall_s >= 3.0:
            self._stalls_over_3s += 1

    def tick(self, coord: Any) -> None:
        now_mono = time.monotonic()
        frame_ts = float(self._frame_ts_reader(coord))
        frame_count = int(self._frame_count_reader(coord))
        if frame_ts <= 0.0:
            return

        if self._first_frame_ts <= 0.0:
            self._first_frame_ts = frame_ts

        progressed = (frame_ts > self._last_frame_ts) or (
            frame_count > self._last_frame_count
        )
        if progressed:
            if self._last_frame_ts > 0.0:
                self._max_gap_s = max(
                    self._max_gap_s, max(0.0, frame_ts - self._last_frame_ts)
                )
            if self._stall_start_ts is not None:
                stall_s = max(0.0, frame_ts - self._stall_start_ts)
                self._record_stall(stall_s)
                if self._emit_logs:
                    self._logger.warning(
                        "%s stall recovered after %.2fs (video_frames=%d)",
                        self._label,
                        stall_s,
                        frame_count,
                    )
                self._stall_start_ts = None
                self._last_stall_log_mono = 0.0
            self._last_frame_ts = frame_ts
            self._last_frame_count = frame_count
            return

        if self._last_frame_ts <= 0.0:
            return
        if self._stall_start_ts is None:
            self._stall_start_ts = self._last_frame_ts

        stall_s = max(0.0, now_mono - self._stall_start_ts)
        if stall_s >= 1.0 and (
            self._last_stall_log_mono <= 0.0
            or (now_mono - self._last_stall_log_mono) >= 2.0
        ):
            if self._emit_logs:
                self._logger.warning(
                    "%s stall ongoing %.2fs (video_frames=%d)",
                    self._label,
                    stall_s,
                    frame_count,
                )
            self._last_stall_log_mono = now_mono

    def summary(self, coord: Any) -> dict[str, float | int]:
        now_mono = time.monotonic()
        frame_count = int(self._frame_count_reader(coord))
        last_frame_ts = float(self._frame_ts_reader(coord))

        active_span = 0.0
        if self._first_frame_ts > 0.0 and last_frame_ts >= self._first_frame_ts:
            active_span = max(0.0, last_frame_ts - self._first_frame_ts)

        avg_fps = 0.0
        if active_span > 0.0 and frame_count > 1:
            avg_fps = (frame_count - 1) / active_span

        unresolved_stall_s = 0.0
        if self._stall_start_ts is not None:
            unresolved_stall_s = max(0.0, now_mono - self._stall_start_ts)

        max_gap_s = max(self._max_gap_s, unresolved_stall_s)

        return {
            "video_frames": frame_count,
            "avg_fps": avg_fps,
            "active_span_s": active_span,
            "max_gap_s": max_gap_s,
            "recovered_stalls": self._recovered_stalls,
            "recovered_stalls_over_1s": self._stalls_over_1s,
            "recovered_stalls_over_3s": self._stalls_over_3s,
            "unresolved_stall_s": unresolved_stall_s,
        }


async def _create_coordinator(args) -> tuple[Any, dict[str, Any], Any]:
    mods = _bootstrap_integration_modules()
    MeariApiClient = mods["api"].MeariApiClient
    CloudEdgeMeariCoordinator = mods["coordinator"].CloudEdgeMeariCoordinator

    api = MeariApiClient(
        email=args.email,
        password=args.password,
        country_code=args.country_code,
        phone_code=args.phone_code,
        app_profile=args.profile,
    )
    api.login()
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


def _get_startup_bootstrap_state(coord: Any) -> dict[str, Any]:
    getter = getattr(coord, "get_startup_bootstrap_state", None)
    if callable(getter):
        try:
            return dict(getter())
        except Exception:
            pass

    validator = getattr(coord, "_is_valid_idr_seed", None)
    seed = getattr(coord, "_stream_idr_seed", b"")
    collecting = bool(getattr(coord, "_stream_idr_collecting", False))
    last_video = float(getattr(coord, "_last_p2p_video_time", 0.0))
    video_age_s = (time.monotonic() - last_video) if last_video > 0 else float("inf")
    seed_valid = bool(seed) and callable(validator) and bool(validator(seed))
    startup_safe = bool(seed_valid and not collecting and video_age_s < 1.0)
    return {
        "startup_safe": startup_safe,
        "block_reason": "ready" if startup_safe else "fallback-not-ready",
        "seed_valid": seed_valid,
        "seed_strong": seed_valid,
        "seed_video_bytes": 0,
        "seed_strength_reason": "",
        "collecting": collecting,
        "video_age_s": float(video_age_s),
        "latest_severe_gap_event": None,
    }


async def _await_startup_safe_bootstrap(
    coord: Any, timeout: float = 8.0
) -> tuple[bool, dict[str, Any]]:
    """Wait until bootstrap seed is safe for a fresh player join."""
    deadline = time.monotonic() + timeout
    last_state = _get_startup_bootstrap_state(coord)

    while time.monotonic() < deadline:
        last_state = _get_startup_bootstrap_state(coord)
        if bool(last_state.get("startup_safe", False)):
            return True, last_state
        await asyncio.sleep(0.1)

    return False, last_state


async def _await_preferred_player_bootstrap(
    coord: Any,
    timeout: float = 8.0,
) -> tuple[bool, dict[str, Any]]:
    """Wait until the coordinator can offer ffplay its preferred fresh join mode."""
    deadline = time.monotonic() + timeout
    last_state = _get_startup_bootstrap_state(coord)

    while time.monotonic() < deadline:
        last_state = _get_startup_bootstrap_state(coord)
        if str(last_state.get("preferred_join_mode", "pending")) == "ready-backlog":
            return True, last_state
        await asyncio.sleep(0.1)

    return False, last_state


async def _await_player_launch_calm(
    coord: Any,
    timeout: float = 6.0,
    severe_quiet_s: float = 2.5,
) -> tuple[bool, dict[str, Any]]:
    """Wait for a short severe-gap quiet period before launching ffplay."""
    deadline = time.monotonic() + timeout
    last_state = _get_startup_bootstrap_state(coord)

    while time.monotonic() < deadline:
        now_mono = time.monotonic()
        last_state = _get_startup_bootstrap_state(coord)
        latest_severe = last_state.get("latest_severe_gap_event") or {}
        severe_status = str(latest_severe.get("status", ""))
        severe_started = float(latest_severe.get("started_mono", 0.0) or 0.0)
        severe_released = float(
            latest_severe.get("quarantine_release_mono", 0.0) or 0.0
        )
        severe_reference = max(severe_started, severe_released)
        severe_active = bool(latest_severe) and severe_status != "released"
        severe_recent = (
            bool(latest_severe)
            and severe_reference > 0.0
            and (now_mono - severe_reference) < severe_quiet_s
        )
        video_age_s = float(last_state.get("video_age_s", 999.0) or 999.0)
        if not severe_active and not severe_recent and video_age_s < 0.9:
            return True, last_state
        await asyncio.sleep(0.1)

    return False, last_state


async def _await_hevc_clean_startup_seed(
    coord: Any,
    timeout: float = 12.0,
) -> tuple[bool, dict[str, Any]]:
    """Wait for a fully clean decode-probed startup seed before HEVC launch."""
    deadline = time.monotonic() + timeout
    last_state = _get_startup_bootstrap_state(coord)

    while time.monotonic() < deadline:
        last_state = _get_startup_bootstrap_state(coord)
        startup_safe = bool(last_state.get("startup_safe", False))
        seed_strong = bool(last_state.get("seed_strong", False))
        reason = str(last_state.get("seed_strength_reason", "") or "")
        frames_since_seed = int(last_state.get("frames_since_seed", 0) or 0)
        preferred_join_mode = str(last_state.get("preferred_join_mode", "") or "")
        # strict path currently marks decode-probed frames as clean+decoded
        decode_probed = "decoded" in reason or "validated" in reason
        if startup_safe and seed_strong and decode_probed and frames_since_seed >= 3:
            probe_fn = getattr(coord, "_probe_bootstrap_seed_decode", None)
            seed_bytes = bytes(getattr(coord, "_stream_idr_seed", b""))
            if callable(probe_fn) and seed_bytes:
                try:
                    ok_probe, probe_reason = probe_fn(seed_bytes, max_frames=6)
                except Exception:
                    ok_probe, probe_reason = False, "seed-probe-exception"
                last_state["hevc_gate_probe_reason"] = probe_reason
                if ok_probe and preferred_join_mode in {"ready", "ready-backlog"}:
                    return True, last_state
            elif preferred_join_mode in {"ready", "ready-backlog"}:
                return True, last_state
        await asyncio.sleep(0.1)

    return False, last_state


_PLAYER_DECODE_MARKERS = (
    "Could not find ref with POC",
    "Error constructing the frame RPS",
    "Skipping invalid undecodable NALU",
    "Invalid NAL unit",
    "decode_slice_header error",
    "Error while decoding stream",
)
_SHOWINFO_PTS_TIME_RE = re.compile(r"pts_time:([\-\d\.]+)")
_FFPLAY_TEXTURE_LINE_RE = re.compile(r"Created\s+\d+x\d+\s+texture")


def _select_gap_event_for_error_time(
    events: list[dict[str, Any]],
    when_mono: float,
) -> dict[str, Any] | None:
    candidate: dict[str, Any] | None = None
    candidate_start = -1.0
    for event in events:
        started = float(event.get("started_mono", 0.0) or 0.0)
        if started <= 0.0 or when_mono < started:
            continue
        release_mono = float(
            event.get(
                "quarantine_release_mono",
                event.get("output_reset_mono", started),
            )
            or started
        )
        horizon = max(started + 8.0, release_mono + 4.0)
        if when_mono > horizon:
            continue
        if started >= candidate_start:
            candidate = event
            candidate_start = started
    return candidate


def _monitor_player_decode_correlation(
    coord: Any,
    log_path: str,
    stop_event: threading.Event,
    state: dict[str, Any],
) -> None:
    log = logging.getLogger(__name__)
    offset = 0
    partial = ""

    def _drain_once() -> None:
        nonlocal offset, partial
        if not os.path.isfile(log_path):
            return
        try:
            with open(log_path, encoding="utf-8", errors="replace") as fh:
                fh.seek(offset)
                chunk = fh.read()
                offset = fh.tell()
        except Exception:
            return

        if not chunk:
            return

        partial += chunk.replace("\r", "\n")
        lines = partial.split("\n")
        partial = lines.pop() if lines else ""
        for raw_line in lines:
            line = raw_line.strip()
            if not line or not any(marker in line for marker in _PLAYER_DECODE_MARKERS):
                continue

            when_mono = time.monotonic()
            getter = getattr(coord, "get_gap_skip_events_snapshot", None)
            events = getter() if callable(getter) else []
            event = _select_gap_event_for_error_time(events, when_mono)
            if event is None:
                state["startup_count"] = int(state.get("startup_count", 0)) + 1
                startup_samples = state.setdefault("startup_samples", [])
                if len(startup_samples) < 3:
                    startup_samples.append(line)
                if int(state["startup_count"]) <= 3:
                    player_started_mono = float(
                        state.get("player_started_mono", when_mono) or when_mono
                    )
                    log.warning(
                        "Player decode error before any correlated gap event (+%.2fs): %s",
                        when_mono - player_started_mono,
                        line,
                    )
                continue

            event_id = int(event.get("event_id", 0) or 0)
            severity = str(event.get("severity", "unknown"))
            bucket = state.setdefault("by_event", {}).setdefault(
                event_id,
                {
                    "count": 0,
                    "severity": severity,
                    "first_after_s": None,
                    "last_after_s": None,
                    "samples": [],
                },
            )
            delta = when_mono - float(event.get("started_mono", when_mono) or when_mono)
            bucket["count"] += 1
            bucket["severity"] = severity
            if bucket["first_after_s"] is None:
                bucket["first_after_s"] = delta
            bucket["last_after_s"] = delta
            if len(bucket["samples"]) < 2:
                bucket["samples"].append(line)
            if int(bucket["count"]) <= 3:
                log.warning(
                    "Player decode error linked to gap event #%d (%s, +%.2fs): %s",
                    event_id,
                    severity,
                    delta,
                    line,
                )

    while not stop_event.is_set():
        _drain_once()
        stop_event.wait(0.2)
    _drain_once()


def _monitor_player_visual_state(
    log_path: str,
    stop_event: threading.Event,
    state: dict[str, Any],
) -> None:
    log = logging.getLogger(__name__)
    offset = 0
    partial = ""

    def _drain_once() -> None:
        nonlocal offset, partial
        if not os.path.isfile(log_path):
            return
        try:
            with open(log_path, encoding="utf-8", errors="replace") as fh:
                fh.seek(offset)
                chunk = fh.read()
                offset = fh.tell()
        except Exception:
            return

        if not chunk:
            return

        partial += chunk.replace("\r", "\n")
        lines = partial.split("\n")
        partial = lines.pop() if lines else ""
        for raw_line in lines:
            line = raw_line.strip()
            if not line:
                continue
            when_mono = time.monotonic()

            if "[ffplay_buffer" in line and " w:" in line and " h:" in line:
                state["buffer_lines"] = int(state.get("buffer_lines", 0) or 0) + 1
                if float(state.get("first_buffer_mono", 0.0) or 0.0) <= 0.0:
                    state["first_buffer_mono"] = when_mono

            if _FFPLAY_TEXTURE_LINE_RE.search(line):
                state["texture_lines"] = int(state.get("texture_lines", 0) or 0) + 1
                if float(state.get("texture_created_mono", 0.0) or 0.0) <= 0.0:
                    state["texture_created_mono"] = when_mono
                    player_started_mono = float(
                        state.get("player_started_mono", when_mono) or when_mono
                    )
                    log.info(
                        "Player visual ready: ffplay created its video texture after %.2fs",
                        when_mono - player_started_mono,
                    )

            if "A-V:" in line and "aq=" in line and "vq=" in line:
                state["stats_count"] = int(state.get("stats_count", 0) or 0) + 1
                state["last_stats_mono"] = when_mono
                if float(state.get("first_stats_mono", 0.0) or 0.0) <= 0.0:
                    state["first_stats_mono"] = when_mono

    while not stop_event.is_set():
        _drain_once()
        now_mono = time.monotonic()
        player_started_mono = float(state.get("player_started_mono", 0.0) or 0.0)
        if player_started_mono > 0.0:
            texture_created_mono = float(state.get("texture_created_mono", 0.0) or 0.0)
            if texture_created_mono <= 0.0 and not bool(
                state.get("late_texture_warned", False)
            ):
                if now_mono - player_started_mono > 8.0:
                    log.warning(
                        "Player visual readiness lagging after %.2fs: ffplay still has no video texture",
                        now_mono - player_started_mono,
                    )
                    state["late_texture_warned"] = True
            first_stats_mono = float(state.get("first_stats_mono", 0.0) or 0.0)
            if (
                texture_created_mono > 0.0
                and first_stats_mono <= 0.0
                and not bool(state.get("late_stats_warned", False))
            ):
                if now_mono - texture_created_mono > 6.0:
                    log.warning(
                        "Player visual progress lagging after texture creation: no ffplay stats activity for %.2fs",
                        now_mono - texture_created_mono,
                    )
                    state["late_stats_warned"] = True
        stop_event.wait(0.2)
    _drain_once()


def _summarize_player_visual_state(state: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    player_started_mono = float(state.get("player_started_mono", 0.0) or 0.0)
    texture_created_mono = float(state.get("texture_created_mono", 0.0) or 0.0)
    first_buffer_mono = float(state.get("first_buffer_mono", 0.0) or 0.0)
    first_stats_mono = float(state.get("first_stats_mono", 0.0) or 0.0)
    stats_count = int(state.get("stats_count", 0) or 0)

    result["player_texture_created"] = bool(texture_created_mono > 0.0)
    result["player_texture_lines"] = int(state.get("texture_lines", 0) or 0)
    result["player_buffer_lines"] = int(state.get("buffer_lines", 0) or 0)
    result["player_visual_stats_count"] = stats_count
    if player_started_mono > 0.0 and texture_created_mono > 0.0:
        result["player_texture_open_latency_s"] = round(
            texture_created_mono - player_started_mono, 3
        )
    if player_started_mono > 0.0 and first_buffer_mono > 0.0:
        result["player_first_buffer_latency_s"] = round(
            first_buffer_mono - player_started_mono, 3
        )
    if player_started_mono > 0.0 and first_stats_mono > 0.0:
        result["player_first_stats_latency_s"] = round(
            first_stats_mono - player_started_mono, 3
        )

    visual_issues: list[str] = []
    if not result["player_texture_created"]:
        visual_issues.append("no-video-texture")
    elif float(result.get("player_texture_open_latency_s", 0.0) or 0.0) > 8.0:
        visual_issues.append(
            f"late-video-texture:{float(result['player_texture_open_latency_s']):.2f}s"
        )
    if stats_count <= 0:
        visual_issues.append("no-ffplay-stats")
    elif float(result.get("player_first_stats_latency_s", 0.0) or 0.0) > 10.0:
        visual_issues.append(
            f"late-ffplay-stats:{float(result['player_first_stats_latency_s']):.2f}s"
        )
    result["player_visual_has_issues"] = bool(visual_issues)
    if visual_issues:
        result["player_visual_issues"] = visual_issues
    return result


def _print_player_decode_correlation(coord: Any, state: dict[str, Any]) -> None:
    getter = getattr(coord, "get_gap_skip_events_snapshot", None)
    events = getter() if callable(getter) else []
    startup_count = int(state.get("startup_count", 0) or 0)
    by_event = state.get("by_event", {})
    if startup_count <= 0 and not events and not by_event:
        return

    print("\nGap recovery correlation")
    print("-" * 78)
    print(f"  startup_player_decode_errors: {startup_count}")
    startup_samples = state.get("startup_samples", [])
    if startup_samples:
        print(f"  startup_player_decode_errors_sample: {startup_samples[:3]}")

    for event in events:
        event_id = int(event.get("event_id", 0) or 0)
        severity = str(event.get("severity", "unknown"))
        strict_release = bool(event.get("strict_release", False))
        bucket = by_event.get(event_id, {})
        parts = [
            f"gap_event_{event_id}:",
            f"severity={severity}",
            f"strict_release={'yes' if strict_release else 'no'}",
            f"player_decode_errors={int(bucket.get('count', 0) or 0)}",
        ]
        if event.get("mode"):
            parts.append(f"mode={event['mode']}")
        if "gap_size" in event:
            parts.append(f"gap={int(event.get('gap_size', 0) or 0)}")
        if "stall_s" in event:
            parts.append(f"stall_s={float(event.get('stall_s', 0.0) or 0.0):.2f}")
        if "backlog_s" in event:
            parts.append(f"backlog_s={float(event.get('backlog_s', 0.0) or 0.0):.2f}")
        if "payload_idle_s" in event:
            parts.append(f"idle_s={float(event.get('payload_idle_s', 0.0) or 0.0):.2f}")
        if "quarantine_drops" in event:
            parts.append(
                f"quarantine_drops={int(event.get('quarantine_drops', 0) or 0)}"
            )
        if event.get("release_reason"):
            parts.append(f"release={event['release_reason']}")
        if bucket.get("first_after_s") is not None:
            parts.append(f"first_error_after_s={float(bucket['first_after_s']):.2f}")
        print("  " + " ".join(parts))
        if bucket.get("samples"):
            print(f"  gap_event_{event_id}_sample: {bucket['samples'][0]}")


def _print_stream_join_diagnostics(coord: Any) -> None:
    getter = getattr(coord, "get_stream_join_diagnostics_snapshot", None)
    events = getter() if callable(getter) else []
    if not events:
        return

    print("\nStream join diagnostics")
    print("-" * 78)
    for event in events:
        event_id = int(event.get("event_id", 0) or 0)
        seed_summary = dict(event.get("seed_summary") or {})
        live_summary = dict(event.get("live_summary") or {})
        boundary = dict(event.get("boundary") or {})
        print(
            "  "
            f"join_event_{event_id}: "
            f"mode={event.get('mode', 'unknown')} "
            f"reason={event.get('reason', 'unknown')} "
            f"wait_s={float(event.get('wait_s', 0.0) or 0.0):.2f} "
            f"seed_generation={int(event.get('seed_generation', 0) or 0)}/"
            f"{int(event.get('required_seed_generation', 0) or 0)} "
            f"seed_bytes={int(event.get('seed_bytes', 0) or 0)} "
            f"live_chunks={int(event.get('live_chunk_count', 0) or 0)} "
            f"seed_offset_s={float(event.get('seed_offset_s', 0.0) or 0.0):.3f}"
        )
        print(
            "  "
            f"seed_video_pts={seed_summary.get('video_first_pts')}->{seed_summary.get('video_last_pts')} "
            f"seed_audio_pts={seed_summary.get('audio_first_pts')}->{seed_summary.get('audio_last_pts')}"
        )
        live_first_pid = boundary.get("live_first_packet_pid")
        live_first_pid_text = (
            f"0x{int(live_first_pid):04x}" if isinstance(live_first_pid, int) else "n/a"
        )
        parts = [
            f"live_first_pid={live_first_pid_text}",
            f"live_video_pusi={boundary.get('live_first_video_packet_is_pusi')}",
            f"live_video_rai={boundary.get('live_first_video_packet_is_rai')}",
            f"live_video_pusi_index={boundary.get('live_first_video_pusi_index')}",
            f"live_audio_pusi={boundary.get('live_first_audio_packet_is_pusi')}",
            f"live_audio_pusi_index={boundary.get('live_first_audio_pusi_index')}",
        ]
        if "video_pts_gap_s" in boundary:
            parts.append(f"video_pts_gap_s={float(boundary['video_pts_gap_s']):.3f}")
        if "video_pcr_gap_s" in boundary:
            parts.append(f"video_pcr_gap_s={float(boundary['video_pcr_gap_s']):.3f}")
        if "video_first_live_cc" in boundary:
            parts.append(
                f"video_cc={int(boundary['video_first_live_cc'])} "
                f"exp={int(boundary.get('video_expected_next_cc', 0) or 0)} "
                f"delta={int(boundary.get('video_cc_delta', 0) or 0)}"
            )
        if "audio_pts_gap_s" in boundary:
            parts.append(f"audio_pts_gap_s={float(boundary['audio_pts_gap_s']):.3f}")
        if "audio_first_live_cc" in boundary:
            parts.append(
                f"audio_cc={int(boundary['audio_first_live_cc'])} "
                f"exp={int(boundary.get('audio_expected_next_cc', 0) or 0)} "
                f"delta={int(boundary.get('audio_cc_delta', 0) or 0)}"
            )
        print("  " + " ".join(parts))
        if seed_summary.get("sample_packets"):
            print(
                f"  join_event_{event_id}_seed_packets: {seed_summary['sample_packets']}"
            )
        if live_summary.get("sample_packets"):
            print(
                f"  join_event_{event_id}_live_packets: {live_summary['sample_packets']}"
            )


def _collect_ts_decode_error_timeline(
    ts_path: str, log: logging.Logger
) -> dict[str, Any]:
    """Approximate recorded-TS decode-error timestamps using ffmpeg showinfo."""
    result: dict[str, Any] = {}

    proc = subprocess.run(
        [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "info",
            "-i",
            ts_path,
            "-map",
            "0:v:0",
            "-an",
            "-vf",
            "showinfo",
            "-f",
            "null",
            "-",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=45,
        check=False,
    )
    raw = (proc.stdout or b"").decode(errors="replace")
    if not raw:
        return result

    last_pts_time = 0.0
    error_times: list[float] = []
    error_samples: list[str] = []
    for raw_line in raw.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        pts_match = _SHOWINFO_PTS_TIME_RE.search(line)
        if pts_match:
            try:
                last_pts_time = float(pts_match.group(1))
            except ValueError:
                pass
        if any(marker in line for marker in _PLAYER_DECODE_MARKERS):
            error_time = max(0.0, last_pts_time)
            error_times.append(error_time)
            if len(error_samples) < 5:
                error_samples.append(f"{error_time:.3f}s {line}")

    if error_times:
        result["first_decode_error_s"] = round(error_times[0], 3)
        result["decode_error_timestamps_s"] = [
            round(ts_s, 3) for ts_s in error_times[:12]
        ]
        result["decode_error_timeline_sample"] = error_samples

    return result


def _print_ts_decode_correlation(
    coord: Any,
    recorder_started_mono: float | None,
    ts_metrics: dict[str, Any],
) -> None:
    if recorder_started_mono is None:
        return
    error_times = ts_metrics.get("decode_error_timestamps_s") or []
    if not error_times:
        return

    getter = getattr(coord, "get_gap_skip_events_snapshot", None)
    events = getter() if callable(getter) else []
    if not events:
        return

    print("\nRecorded TS decode correlation")
    print("-" * 78)

    first_error_s = float(ts_metrics.get("first_decode_error_s", error_times[0]) or 0.0)
    first_when_mono = recorder_started_mono + first_error_s
    first_event = _select_gap_event_for_error_time(events, first_when_mono)
    print(f"  first_ts_decode_error_s: {first_error_s:.3f}")
    if first_event is None:
        print(
            "  first_ts_decode_error_gap_event: none (join/startup-side or uncorrelated)"
        )
    else:
        event_id = int(first_event.get("event_id", 0) or 0)
        severity = str(first_event.get("severity", "unknown"))
        started_mono = float(
            first_event.get("started_mono", first_when_mono) or first_when_mono
        )
        release_mono = float(
            first_event.get(
                "quarantine_release_mono",
                first_event.get("output_reset_mono", started_mono),
            )
            or started_mono
        )
        print(
            "  first_ts_decode_error_gap_event: "
            f"#{event_id} severity={severity} release={first_event.get('release_reason', 'unknown')}"
        )
        print(
            f"  first_ts_decode_error_after_gap_s: {max(0.0, first_when_mono - started_mono):.3f}"
        )
        print(
            f"  first_ts_decode_error_after_release_s: {max(0.0, first_when_mono - release_mono):.3f}"
        )

    sampled_counts: dict[int, int] = {}
    uncorrelated = 0
    event_lookup = {
        int(event.get("event_id", 0) or 0): event
        for event in events
        if int(event.get("event_id", 0) or 0) > 0
    }
    for ts_s in error_times[:12]:
        event = _select_gap_event_for_error_time(
            events, recorder_started_mono + float(ts_s)
        )
        if event is None:
            uncorrelated += 1
            continue
        event_id = int(event.get("event_id", 0) or 0)
        sampled_counts[event_id] = sampled_counts.get(event_id, 0) + 1

    if uncorrelated:
        print(
            f"  sampled_ts_decode_errors_without_gap: {uncorrelated}/{len(error_times[:12])}"
        )
    for event_id, count in sorted(sampled_counts.items()):
        severity = str(event_lookup.get(event_id, {}).get("severity", "unknown"))
        print(
            f"  gap_event_{event_id}_sampled_ts_decode_errors: {count}/{len(error_times[:12])} ({severity})"
        )


async def cmd_list(args) -> int:
    mods = _bootstrap_integration_modules()
    MeariApiClient = mods["api"].MeariApiClient
    parse_quality_profiles = mods["p2p_streamer"].parse_quality_profiles

    api = MeariApiClient(
        email=args.email,
        password=args.password,
        country_code=args.country_code,
        phone_code=args.phone_code,
        app_profile=args.profile,
    )
    api.login()

    print("=" * 78)
    print(f"Profile: {args.profile}")
    print(f"Total devices: {len(api.devices)}")
    print(f"Snap devices: {len(api.get_snap_devices())}")
    if hasattr(api, "get_camera_devices"):
        print(f"Camera devices (snap/ipc/doorbell): {len(api.get_camera_devices())}")
    print("=" * 78)
    for i, dev in enumerate(api.devices.values(), start=1):
        profiles = parse_quality_profiles(dev)
        profiles_str = (
            ", ".join(f"{k}={v}" for k, v in sorted(profiles.items()))
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


async def _run_probe(url: str, duration: int) -> tuple[int, list[str]]:
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "warning",
        "-nostdin",
        "-rw_timeout",
        "5000000",
        "-fflags",
        "nobuffer",
        "-i",
        url,
        "-t",
        str(duration),
        "-f",
        "null",
        "-",
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )

    lines: list[str] = []
    assert proc.stderr is not None
    while True:
        line = await proc.stderr.readline()
        if not line:
            break
        lines.append(line.decode(errors="replace").rstrip())

    rc = await proc.wait()
    return rc, lines


async def _run_idle_client_probe(url: str, duration: int) -> tuple[int, list[str]]:
    """Keep a TCP client attached for idle/still benchmarking duration."""
    parsed = urlparse(url)
    if parsed.scheme != "tcp" or not parsed.hostname or not parsed.port:
        return 2, [f"unsupported idle probe URL: {url}"]

    host = parsed.hostname
    port = int(parsed.port)
    seconds = max(1, int(duration))
    deadline = time.monotonic() + float(seconds)
    total_bytes = 0
    reconnects = 0
    reader: asyncio.StreamReader | None = None
    writer: asyncio.StreamWriter | None = None

    async def _close_client() -> None:
        nonlocal writer, reader
        if writer is not None:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
        writer = None
        reader = None

    while time.monotonic() < deadline:
        if reader is None or writer is None:
            try:
                reader, writer = await asyncio.open_connection(host, port)
            except Exception:
                reconnects += 1
                await asyncio.sleep(0.25)
                continue

        timeout = max(0.1, min(1.0, deadline - time.monotonic()))
        try:
            chunk = await asyncio.wait_for(reader.read(32768), timeout=timeout)
        except asyncio.TimeoutError:
            continue
        except Exception:
            reconnects += 1
            await _close_client()
            continue

        if not chunk:
            reconnects += 1
            await _close_client()
            await asyncio.sleep(0.1)
            continue

        total_bytes += len(chunk)

    await _close_client()
    return 0, [
        f"idle_client_bytes={total_bytes}",
        f"idle_client_reconnects={reconnects}",
    ]


def _build_stream_player_cmd(
    url: str,
    duration: int = 0,
    codec: str = "hevc",
) -> list[str]:
    """Build ffplay command for visible live playback."""
    if not shutil.which("ffplay"):
        raise RuntimeError("ffplay not found")

    cmd = [
        "ffplay",
        "-hide_banner",
        "-stats",
        "-loglevel",
        "verbose",
        "-window_title",
        "CloudEdge live",
        "-fflags",
        "+discardcorrupt",
        "-sync",
        "ext",
        "-infbuf",
        "-analyzeduration",
        "1000000",
        "-probesize",
        "524288",
        "-f",
        "mpegts",
    ]
    if int(duration) > 0:
        cmd.extend(["-t", str(int(duration))])
    cmd.append(url)
    return cmd


def _build_stream_recorder_cmd(
    url: str,
    output_path: str,
    duration: int = 0,
) -> list[str]:
    """Build ffmpeg command to record the TCP stream to a .ts file.

    Connects as a second TCP client to the stream server, so it gets
    the same data the player sees without interfering with playback.
    """
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "warning",
        "-nostdin",
        "-rw_timeout",
        "5000000",
        "-fflags",
        "+discardcorrupt+nobuffer",
        "-analyzeduration",
        "20000000",
        "-probesize",
        "8388608",
        "-f",
        "mpegts",
        "-i",
        url,
        "-c",
        "copy",
        "-f",
        "mpegts",
        "-y",
        output_path,
    ]
    if int(duration) > 0:
        cmd.insert(-4, "-t")
        cmd.insert(-4, str(int(duration)))
    return cmd


def _stop_player_process(proc: subprocess.Popen | None) -> None:
    """Terminate ffplay reliably so CLI exits without manual Ctrl+C."""
    if proc is None or proc.poll() is not None:
        return

    attempts: list[tuple[str, Callable[[], None], float]] = [
        ("SIGINT", lambda: proc.send_signal(signal.SIGINT), 1.0),
        ("terminate", proc.terminate, 1.5),
        ("kill", proc.kill, 1.0),
    ]
    for label, action, timeout_s in attempts:
        if proc.poll() is not None:
            return
        try:
            action()
        except Exception:
            continue
        try:
            proc.wait(timeout=timeout_s)
            return
        except subprocess.TimeoutExpired:
            logging.getLogger(__name__).debug(
                "ffplay did not exit after %s, escalating", label
            )
        except Exception:
            return


def _build_pcm_recorder_cmd(
    url: str,
    output_path: str,
    duration: int = 0,
) -> list[str]:
    """Build ffmpeg command to extract raw PCM audio from the stream.

    Connects as a third TCP client (same TS data) and decodes audio to
    16-bit signed-LE mono 16 kHz WAV for objective silence analysis.
    """
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "warning",
        "-nostdin",
        "-rw_timeout",
        "5000000",
        "-fflags",
        "+discardcorrupt+nobuffer",
        "-analyzeduration",
        "20000000",
        "-probesize",
        "8388608",
        "-f",
        "mpegts",
        "-i",
        url,
        "-vn",  # drop video
        "-acodec",
        "pcm_s16le",  # raw PCM
        "-ar",
        "16000",  # 16 kHz (matches our AAC encoder)
        "-ac",
        "1",  # mono
        "-f",
        "wav",
        "-y",
        output_path,
    ]
    if int(duration) > 0:
        cmd.insert(-4, "-t")
        cmd.insert(-4, str(int(duration)))
    return cmd


def _analyze_pcm_audio(
    wav_path: str,
    log: logging.Logger,
    chunk_ms: int = 64,
    silence_threshold_rms: float = 50.0,
) -> dict[str, Any]:
    """Analyse a raw PCM WAV file for silence gaps and audible content.

    Splits the audio into *chunk_ms*-length windows and classifies each
    as silence (RMS < *silence_threshold_rms*) or audible.

    Returns a dict with:
      pcm_duration_s, pcm_audible_pct, pcm_silence_pct,
      pcm_silence_gap_count, pcm_silence_gap_durations_s (top 10),
      pcm_max_silence_gap_s, pcm_avg_rms, pcm_max_rms,
      pcm_audible_segments (count of continuous audible runs),
    """
    import struct
    import math

    result: dict[str, Any] = {}

    if not os.path.isfile(wav_path) or os.path.getsize(wav_path) < 100:
        log.warning("PCM recording missing or too small: %s", wav_path)
        return result

    # Read WAV — we know it is 16-bit LE mono 16 kHz from our own ffmpeg cmd.
    with open(wav_path, "rb") as f:
        header = f.read(44)  # standard WAV header
        if len(header) < 44 or header[:4] != b"RIFF":
            log.warning("Not a valid WAV file: %s", wav_path)
            return result
        pcm_data = f.read()

    if len(pcm_data) < 2:
        log.warning("PCM recording has no audio data")
        return result

    sample_rate = 16000
    bytes_per_sample = 2  # s16le
    chunk_samples = int(sample_rate * chunk_ms / 1000)
    chunk_bytes = chunk_samples * bytes_per_sample
    total_samples = len(pcm_data) // bytes_per_sample
    total_duration = total_samples / sample_rate

    result["pcm_duration_s"] = round(total_duration, 2)

    # Classify each chunk
    chunk_rms_values: list[float] = []
    is_silence: list[bool] = []
    offset = 0
    while offset + chunk_bytes <= len(pcm_data):
        samples = struct.unpack(
            f"<{chunk_samples}h", pcm_data[offset : offset + chunk_bytes]
        )
        sum_sq = sum(s * s for s in samples)
        rms = math.sqrt(sum_sq / chunk_samples)
        chunk_rms_values.append(rms)
        is_silence.append(rms < silence_threshold_rms)
        offset += chunk_bytes

    if not chunk_rms_values:
        return result

    n_chunks = len(chunk_rms_values)
    n_silent = sum(is_silence)
    n_audible = n_chunks - n_silent
    chunk_dur = chunk_ms / 1000.0

    result["pcm_total_chunks"] = n_chunks
    result["pcm_audible_chunks"] = n_audible
    result["pcm_silence_chunks"] = n_silent
    result["pcm_audible_pct"] = round(100.0 * n_audible / n_chunks, 1)
    result["pcm_silence_pct"] = round(100.0 * n_silent / n_chunks, 1)

    # RMS stats for audible chunks
    audible_rms = [r for r, s in zip(chunk_rms_values, is_silence) if not s]
    if audible_rms:
        result["pcm_avg_rms_audible"] = round(sum(audible_rms) / len(audible_rms), 1)
        result["pcm_max_rms"] = round(max(audible_rms), 1)
        result["pcm_min_rms_audible"] = round(min(audible_rms), 1)
    result["pcm_avg_rms_all"] = round(sum(chunk_rms_values) / len(chunk_rms_values), 1)

    # Detect silence gaps (contiguous silent runs)
    silence_gaps: list[float] = []
    audible_segments = 0
    in_gap = False
    gap_start = 0
    for i, silent in enumerate(is_silence):
        if silent:
            if not in_gap:
                in_gap = True
                gap_start = i
        else:
            if in_gap:
                gap_dur = (i - gap_start) * chunk_dur
                silence_gaps.append(gap_dur)
                in_gap = False
            audible_segments += 1 if (i == 0 or is_silence[i - 1]) else 0
    # Close trailing gap
    if in_gap:
        silence_gaps.append((n_chunks - gap_start) * chunk_dur)

    # Count audible segments (contiguous audible runs) and their durations
    audible_seg_count = 0
    audible_seg_lengths: list[float] = []
    in_audible = False
    seg_start = 0
    for idx, silent in enumerate(is_silence):
        if not silent:
            if not in_audible:
                audible_seg_count += 1
                seg_start = idx
                in_audible = True
        else:
            if in_audible:
                audible_seg_lengths.append((idx - seg_start) * chunk_dur)
                in_audible = False
    if in_audible:
        audible_seg_lengths.append((len(is_silence) - seg_start) * chunk_dur)

    result["pcm_audible_segments"] = audible_seg_count
    result["pcm_silence_gap_count"] = len(silence_gaps)
    if silence_gaps:
        result["pcm_max_silence_gap_s"] = round(max(silence_gaps), 3)
        result["pcm_min_silence_gap_s"] = round(min(silence_gaps), 3)
        result["pcm_avg_silence_gap_s"] = round(
            sum(silence_gaps) / len(silence_gaps), 3
        )
        result["pcm_silence_gap_durations_s"] = [
            round(g, 3) for g in sorted(silence_gaps, reverse=True)[:10]
        ]
        # Count very short gaps that suggest pipeline fragmentation
        result["pcm_short_gaps_under_200ms"] = sum(1 for g in silence_gaps if g < 0.2)

    if audible_seg_lengths:
        result["pcm_max_audible_seg_s"] = round(max(audible_seg_lengths), 3)
        result["pcm_avg_audible_seg_s"] = round(
            sum(audible_seg_lengths) / len(audible_seg_lengths), 3
        )
        result["pcm_audible_seg_durations_s"] = [
            round(g, 3) for g in sorted(audible_seg_lengths, reverse=True)[:10]
        ]

    # Timeline summary: first/last audible chunk position
    audible_indices = [i for i, s in enumerate(is_silence) if not s]
    if audible_indices:
        result["pcm_first_audible_at_s"] = round(audible_indices[0] * chunk_dur, 2)
        result["pcm_last_audible_at_s"] = round(audible_indices[-1] * chunk_dur, 2)

    return result


def _analyze_recorded_ts(ts_path: str, log: logging.Logger) -> dict[str, Any]:
    """Analyse a recorded MPEG-TS file for video/audio quality metrics.

    Uses ffprobe to extract per-frame timing, then computes:
    - total decoded frames (video + audio)
    - average video FPS
    - frame-to-frame gap histogram (detects skips/freezes)
    - duplicate DTS count (indicates frozen output)
    - decode errors counted by ffmpeg second-pass
    """
    result: dict[str, Any] = {}
    if not os.path.isfile(ts_path) or os.path.getsize(ts_path) < 1000:
        log.warning("TS recording missing or too small: %s", ts_path)
        return result

    # --- ffprobe: extract per-frame timestamps for video stream ---
    # Use best_effort_timestamp_time which is always populated (pkt_pts_time
    # can be N/A for copy-mode TS recordings).
    # Output csv: key_frame,best_effort_timestamp_time
    try:
        raw = subprocess.check_output(
            [
                "ffprobe",
                "-hide_banner",
                "-loglevel",
                "error",
                "-select_streams",
                "v:0",
                "-show_entries",
                "frame=key_frame,best_effort_timestamp_time",
                "-of",
                "csv=p=0",
                ts_path,
            ],
            timeout=30,
            stderr=subprocess.DEVNULL,
        ).decode(errors="replace")
    except Exception as e:
        log.warning("ffprobe frame extraction failed: %s", e)
        return result

    pts_list: list[float] = []
    n_keyframes = 0
    for line in raw.strip().splitlines():
        parts = line.split(",")
        if len(parts) < 2:
            continue
        # csv order: best_effort_timestamp_time, key_frame
        # Find the float value (timestamp) and the 0/1 (key_frame)
        ts_val = None
        kf_val = None
        for p in parts:
            p = p.strip()
            if p in ("0", "1") and kf_val is None:
                kf_val = p
            else:
                try:
                    ts_val = float(p)
                except (ValueError, TypeError):
                    pass
        if ts_val is not None:
            pts_list.append(ts_val)
            if kf_val == "1":
                n_keyframes += 1

    result["video_frames_decoded"] = len(pts_list)
    result["video_keyframes"] = n_keyframes

    if len(pts_list) >= 2:
        pts_list.sort()
        span = pts_list[-1] - pts_list[0]
        result["video_span_s"] = round(span, 3)
        result["video_avg_fps"] = (
            round((len(pts_list) - 1) / span, 2) if span > 0 else 0
        )

        # Frame gaps
        gaps = [pts_list[i + 1] - pts_list[i] for i in range(len(pts_list) - 1)]
        if gaps:
            median_gap = sorted(gaps)[len(gaps) // 2]
            result["video_median_frame_gap_ms"] = round(median_gap * 1000, 1)
            result["video_max_frame_gap_ms"] = round(max(gaps) * 1000, 1)
            # Count gaps that exceed 3x median (likely skips/stalls)
            skip_threshold = max(median_gap * 3, 0.15)
            skips = [g for g in gaps if g > skip_threshold]
            result["video_skip_count"] = len(skips)
            if skips:
                result["video_skip_durations_s"] = [
                    round(g, 3) for g in sorted(skips, reverse=True)[:10]
                ]
            # Duplicate PTS (frozen frames)
            n_dup = sum(1 for g in gaps if g < 0.001)
            result["video_duplicate_pts"] = n_dup

    # --- ffprobe: extract audio frame timestamps ---
    try:
        raw_audio = subprocess.check_output(
            [
                "ffprobe",
                "-hide_banner",
                "-loglevel",
                "error",
                "-select_streams",
                "a:0",
                "-show_entries",
                "frame=best_effort_timestamp_time",
                "-of",
                "csv=p=0",
                ts_path,
            ],
            timeout=15,
            stderr=subprocess.DEVNULL,
        ).decode(errors="replace")
    except Exception as e:
        log.warning("ffprobe audio extraction failed: %s", e)
        raw_audio = ""

    audio_pts: list[float] = []
    for line in raw_audio.strip().splitlines():
        try:
            audio_pts.append(float(line.strip()))
        except (ValueError, TypeError):
            continue

    result["audio_frames_decoded"] = len(audio_pts)
    if len(audio_pts) >= 2:
        audio_pts.sort()
        a_span = audio_pts[-1] - audio_pts[0]
        result["audio_span_s"] = round(a_span, 3)
        a_gaps = [audio_pts[i + 1] - audio_pts[i] for i in range(len(audio_pts) - 1)]
        if a_gaps:
            a_median = sorted(a_gaps)[len(a_gaps) // 2]
            result["audio_median_gap_ms"] = round(a_median * 1000, 1)
            result["audio_max_gap_ms"] = round(max(a_gaps) * 1000, 1)
            a_skip_threshold = max(a_median * 3, 0.15)
            a_skips = [g for g in a_gaps if g > a_skip_threshold]
            result["audio_gap_count"] = len(a_skips)
            if a_skips:
                result["audio_gap_durations_s"] = [
                    round(g, 3) for g in sorted(a_skips, reverse=True)[:10]
                ]

    # --- ffmpeg decode pass: count errors ---
    try:
        err_raw = subprocess.check_output(
            [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "error",
                "-i",
                ts_path,
                "-f",
                "null",
                "-",
            ],
            timeout=30,
            stderr=subprocess.STDOUT,
        ).decode(errors="replace")
        error_lines = [
            l
            for l in err_raw.splitlines()
            if l.strip() and "non monotonically" not in l
        ]
        result["decode_error_lines"] = len(error_lines)
        if error_lines:
            result["decode_errors_sample"] = error_lines[:5]
    except subprocess.CalledProcessError as e:
        err_out = (e.output or b"").decode(errors="replace")
        error_lines = [l for l in err_out.splitlines() if l.strip()]
        result["decode_error_lines"] = len(error_lines)
        if error_lines:
            result["decode_errors_sample"] = error_lines[:5]
    except Exception as e:
        log.warning("TS decode error check failed: %s", e)

    if int(result.get("decode_error_lines", 0) or 0) > 0:
        try:
            result.update(_collect_ts_decode_error_timeline(ts_path, log))
        except Exception as e:
            log.warning("TS decode timeline correlation failed: %s", e)

    return result


def _analyze_player_log(log_path: str, log: logging.Logger) -> dict[str, Any]:
    """Analyse ffplay output for player-visible decode and continuity issues."""
    result: dict[str, Any] = {}
    if not os.path.isfile(log_path) or os.path.getsize(log_path) < 1:
        log.warning("Player log missing or empty: %s", log_path)
        return result

    try:
        raw = Path(log_path).read_text(encoding="utf-8", errors="replace")
    except Exception as e:
        log.warning("Player log read failed: %s", e)
        return result

    lines = [
        line.strip()
        for chunk in raw.replace("\r", "\n").splitlines()
        for line in [chunk.strip()]
        if line
    ]
    if not lines:
        return result

    benign_patterns = (
        "max_analyze_duration",
        "Could not find codec parameters for stream 0",
        "Successfully connected to",
        "Created 2304x1296 texture",
        "auto-inserting filter",
        "tb:1/16000",
        "w:2304 h:1296 pixfmt:",
    )
    decode_markers = (
        "Could not find ref with POC",
        "Error constructing the frame RPS",
        "Skipping invalid undecodable NALU",
        "Invalid NAL unit",
        "decode_slice_header error",
        "Error while decoding stream",
        "concealing",
        "corrupt",
    )
    continuity_markers = (
        "Continuity check failed",
        "Packet corrupt",
        "non monotonically increasing dts",
        "invalid dropping",
        "Past duration",
        "timestamp discontinuity",
    )

    benign_lines = [line for line in lines if any(p in line for p in benign_patterns)]
    decode_lines = [
        line for line in lines if any(marker in line for marker in decode_markers)
    ]
    continuity_lines = [
        line for line in lines if any(marker in line for marker in continuity_markers)
    ]
    significant_lines = [
        line
        for line in lines
        if any(
            token in line.lower()
            for token in ("error", "invalid", "corrupt", "continuity")
        )
        and not any(p in line for p in benign_patterns)
    ]

    poc_values: list[int] = []
    for line in decode_lines:
        match = re.search(r"POC\s+(\d+)", line)
        if match:
            try:
                poc_values.append(int(match.group(1)))
            except ValueError:
                pass

    result["player_log_lines"] = len(lines)
    result["player_benign_lines"] = len(benign_lines)
    result["player_decode_error_lines"] = len(decode_lines)
    result["player_continuity_error_lines"] = len(continuity_lines)
    result["player_significant_error_lines"] = len(significant_lines)
    result["player_has_issues"] = bool(decode_lines or continuity_lines)
    if poc_values:
        result["player_error_poc_first"] = min(poc_values)
        result["player_error_poc_last"] = max(poc_values)
    if decode_lines:
        result["player_decode_errors_sample"] = decode_lines[:8]
    if continuity_lines:
        result["player_continuity_errors_sample"] = continuity_lines[:8]
    elif significant_lines and not decode_lines:
        result["player_significant_errors_sample"] = significant_lines[:8]
    return result


def _analyze_media_client_log(
    log_path: str,
    log: logging.Logger,
    *,
    prefix: str,
    benign_patterns: tuple[str, ...] = (),
    treat_significant_as_issue: bool = False,
) -> dict[str, Any]:
    """Analyse ffmpeg/ffplay client logs for decode and continuity problems."""
    result: dict[str, Any] = {}
    if not os.path.isfile(log_path) or os.path.getsize(log_path) < 1:
        log.warning("%s log missing or empty: %s", prefix, log_path)
        return result

    try:
        raw = Path(log_path).read_text(encoding="utf-8", errors="replace")
    except Exception as e:
        log.warning("%s log read failed: %s", prefix, e)
        return result

    lines = [
        line.strip()
        for chunk in raw.replace("\r", "\n").splitlines()
        for line in [chunk.strip()]
        if line
    ]
    if not lines:
        return result

    shared_benign_patterns = (
        "max_analyze_duration",
        "Could not find codec parameters for stream 0",
        "Successfully connected to",
        "Created 2304x1296 texture",
        "auto-inserting filter",
        "tb:1/16000",
        "w:2304 h:1296 pixfmt:",
    ) + tuple(benign_patterns)
    decode_markers = (
        "Could not find ref with POC",
        "Error constructing the frame RPS",
        "Skipping invalid undecodable NALU",
        "Invalid NAL unit",
        "decode_slice_header error",
        "Error while decoding stream",
        "concealing",
        "corrupt",
    )
    continuity_markers = (
        "Continuity check failed",
        "Packet corrupt",
        "non monotonically increasing dts",
        "invalid dropping",
        "Past duration",
        "timestamp discontinuity",
    )

    benign_lines = [
        line for line in lines if any(p in line for p in shared_benign_patterns)
    ]
    decode_lines = [
        line
        for line in lines
        if any(marker in line for marker in decode_markers)
        and not any(p in line for p in shared_benign_patterns)
    ]
    continuity_lines = [
        line
        for line in lines
        if any(marker in line for marker in continuity_markers)
        and not any(p in line for p in shared_benign_patterns)
    ]
    timestamp_discontinuity_lines = [
        line for line in continuity_lines if "timestamp discontinuity" in line
    ]
    significant_lines = [
        line
        for line in lines
        if any(
            token in line.lower()
            for token in ("error", "invalid", "corrupt", "continuity", "dropping")
        )
        and not any(p in line for p in shared_benign_patterns)
    ]

    poc_values: list[int] = []
    for line in decode_lines:
        match = re.search(r"POC\s+(\d+)", line)
        if match:
            try:
                poc_values.append(int(match.group(1)))
            except ValueError:
                pass

    result[f"{prefix}_log_lines"] = len(lines)
    result[f"{prefix}_benign_lines"] = len(benign_lines)
    result[f"{prefix}_decode_error_lines"] = len(decode_lines)
    result[f"{prefix}_continuity_error_lines"] = len(continuity_lines)
    result[f"{prefix}_timestamp_discontinuity_lines"] = len(
        timestamp_discontinuity_lines
    )
    result[f"{prefix}_significant_error_lines"] = len(significant_lines)
    result[f"{prefix}_has_issues"] = bool(
        decode_lines
        or continuity_lines
        or (treat_significant_as_issue and significant_lines)
    )
    if poc_values:
        result[f"{prefix}_error_poc_first"] = min(poc_values)
        result[f"{prefix}_error_poc_last"] = max(poc_values)
    if decode_lines:
        result[f"{prefix}_decode_errors_sample"] = decode_lines[:8]
    if continuity_lines:
        result[f"{prefix}_continuity_errors_sample"] = continuity_lines[:8]
    elif significant_lines and not decode_lines:
        result[f"{prefix}_significant_errors_sample"] = significant_lines[:8]
    return result


def _analyze_player_log(log_path: str, log: logging.Logger) -> dict[str, Any]:
    """Analyse ffplay output for player-visible decode and continuity issues."""
    return _analyze_media_client_log(log_path, log, prefix="player")


def _analyze_recorder_log(log_path: str, log: logging.Logger) -> dict[str, Any]:
    """Analyse recorder ffmpeg stderr for transport/decode warnings."""
    return _analyze_media_client_log(
        log_path,
        log,
        prefix="recorder",
        benign_patterns=("frame=", "size=", "video:", "audio:"),
        treat_significant_as_issue=True,
    )


async def cmd_stream(args) -> int:
    coord = None
    player_proc = None
    player_log_fh = None
    recorder_proc: subprocess.Popen | None = None
    recorder_log_fh = None
    pcm_recorder_proc: subprocess.Popen | None = None
    recorder_started_mono: float | None = None
    player_started_mono: float | None = None
    player_decode_corr_stop = threading.Event()
    player_decode_corr_thread: threading.Thread | None = None
    player_visual_stop = threading.Event()
    player_visual_thread: threading.Thread | None = None
    player_decode_corr_state: dict[str, Any] = {
        "player_started_mono": 0.0,
        "startup_count": 0,
        "startup_samples": [],
        "by_event": {},
    }
    player_visual_state: dict[str, Any] = {
        "player_started_mono": 0.0,
        "texture_created_mono": 0.0,
        "first_buffer_mono": 0.0,
        "first_stats_mono": 0.0,
        "last_stats_mono": 0.0,
        "stats_count": 0,
        "texture_lines": 0,
        "buffer_lines": 0,
        "late_texture_warned": False,
        "late_stats_warned": False,
    }
    artifact_base = os.path.join(
        tempfile.gettempdir(),
        f"cloudedge_stream_{os.getpid()}_{int(time.time() * 1000)}",
    )
    ts_record_path = f"{artifact_base}.ts"
    pcm_record_path = f"{artifact_base}.wav"
    player_log_path = f"{artifact_base}_player.log"
    recorder_log_path = f"{artifact_base}_recorder.log"
    try:
        setattr(args, "skip_initial_grab", bool(args.wake and args.play))
        if args.wake and args.play:
            logging.getLogger(__name__).info(
                "Auto-disabling startup frame grab for player wake mode"
            )

        coord, dev, mods = await _create_coordinator(args)
        # Apply quality override from CLI
        quality_arg = getattr(args, "quality", None)
        if quality_arg is not None:
            coord.set_vvp_quality(quality_arg)
        if args.play:
            # Gap-fill during stalls must advance PTS at the same rate as
            # realtime audio so the muxer interleaves cleanly.  The keepalive
            # loop caps gap-fill FPS to _video_mux_target_fps (which equals
            # the setts BSF FPS), so we just need a high ceiling here.
            setattr(coord, "_live_gap_fill_after_seconds", 0.3)
            setattr(coord, "_live_gap_fill_fps", 30.0)  # capped by mux target FPS
        adaptive_recovery_for_player = bool(args.wake and args.play)
        setattr(coord, "_p2p_allow_lossy_gap_skip", False)
        setattr(coord, "_p2p_adaptive_lossy_gap_skip", adaptive_recovery_for_player)
        if adaptive_recovery_for_player:
            logging.getLogger(__name__).info(
                "Auto-enabling adaptive gap recovery for wake+play stability"
            )
        url = await _camera_stream_source(mods, coord)

        print("=" * 78)
        print(f"Device: {dev.get('deviceName')} ({dev.get('snNum')})")
        print(f"Stream URL: {url}")
        print("=" * 78)

        baseline_video_time = float(
            getattr(
                coord, "_last_p2p_video_time", getattr(coord, "_last_video_time", 0.0)
            )
        )
        health_source = StreamHealthTracker(
            logging.getLogger(__name__),
            label="Source video",
        )
        # When a local player is requested, wait for live frames before launch
        # to avoid opening a persistent black window.
        effective_wait_live = bool(args.play)
        effective_stall_timeout = 0
        effective_wake_retry_interval = 0
        if args.wake and args.play:
            # In player mode, a first keyframe may appear before a stable live
            # session is established. Keep self-healing enabled by default so
            # playback does not freeze on a static frame.
            effective_stall_timeout = 15
            effective_wake_retry_interval = 8
            logging.getLogger(__name__).info(
                "Player wake recovery enabled: stall_timeout=%ss wake_retry=%ss",
                effective_stall_timeout,
                effective_wake_retry_interval,
            )

        if args.wake:
            coord.wake_camera()
            if effective_wait_live:
                print("Waiting for live video...")
                live_ok = await _await_live_stream(
                    coord,
                    timeout=args.wake_timeout,
                    stall_timeout=effective_stall_timeout,
                    wake_retry_interval=effective_wake_retry_interval,
                )
                print(f"live_ready={live_ok}")
                if not live_ok:
                    raise RuntimeError(
                        "No live video frames received after wake attempts. "
                        "Aborting to avoid a black player window."
                    )

        if args.play:
            active_codec = str(getattr(coord, "_video_codec", "hevc")).lower()
            player_cmd = _build_stream_player_cmd(
                url,
                duration=0,
                codec=active_codec,
            )
            # Hold player launch until the live feed shows a minimum of
            # stable cadence. Launching too early often opens ffplay on a
            # black window while the first GOP is still recovering.
            launch_deadline = time.monotonic() + 10.0
            start_frames = int(getattr(coord, "_p2p_video_frames", 0))
            while time.monotonic() < launch_deadline:
                await asyncio.sleep(0.25)
                frames = int(getattr(coord, "_p2p_video_frames", 0))
                last_video = float(getattr(coord, "_last_p2p_video_time", 0.0))
                age = (time.monotonic() - last_video) if last_video > 0 else 999.0
                if (frames - start_frames) >= 12 and age < 0.8:
                    break
            calm_ready, calm_state = await _await_player_launch_calm(
                coord, timeout=6.0, severe_quiet_s=2.5
            )
            logging.getLogger(__name__).info(
                "Player launch calm gate %s (latest_severe_id=%s, latest_severe_status=%s, video_age=%.2fs)",
                "ready" if calm_ready else "timeout; launching anyway",
                (calm_state.get("latest_severe_gap_event") or {}).get("event_id"),
                (calm_state.get("latest_severe_gap_event") or {}).get("status", "none"),
                float(calm_state.get("video_age_s", 999.0) or 999.0),
            )
            seed_ready, seed_state = await _await_preferred_player_bootstrap(
                coord, timeout=18.0
            )
            logging.getLogger(__name__).info(
                "Player bootstrap seed %s before launch (reason=%s, preferred=%s, backlog_ready=%s, backlog_target=%s, strong=%s, generation=%s/%s, seed_age=%.2fs, follow_frames=%s, video_age=%.2fs, bytes=%s)",
                "ready" if seed_ready else "not ready; proceeding with live join",
                seed_state.get("block_reason", "unknown"),
                seed_state.get("preferred_join_mode", "unknown"),
                seed_state.get("backlog_ready", False),
                seed_state.get("backlog_follow_video_pusi_target", 0),
                seed_state.get("seed_strong", False),
                seed_state.get("seed_generation", 0),
                seed_state.get("required_seed_generation", 0),
                float(seed_state.get("seed_age_s", 999.0) or 999.0),
                seed_state.get("frames_since_seed", 0),
                float(seed_state.get("video_age_s", 999.0) or 999.0),
                seed_state.get("seed_video_bytes", 0),
            )
            logging.getLogger(__name__).info("Delaying player 1s for stream startup")
            await asyncio.sleep(1)
            seed_ready_final, seed_state_final = (
                await _await_preferred_player_bootstrap(coord, timeout=8.0)
            )
            logging.getLogger(__name__).info(
                "Player bootstrap recheck %s immediately before launch (reason=%s, preferred=%s, backlog_ready=%s, backlog_target=%s, strong=%s, generation=%s/%s, seed_age=%.2fs, follow_frames=%s, video_age=%.2fs, bytes=%s)",
                "ready" if seed_ready_final else "not ready; proceeding with live join",
                seed_state_final.get("block_reason", "unknown"),
                seed_state_final.get("preferred_join_mode", "unknown"),
                seed_state_final.get("backlog_ready", False),
                seed_state_final.get("backlog_follow_video_pusi_target", 0),
                seed_state_final.get("seed_strong", False),
                seed_state_final.get("seed_generation", 0),
                seed_state_final.get("required_seed_generation", 0),
                float(seed_state_final.get("seed_age_s", 999.0) or 999.0),
                seed_state_final.get("frames_since_seed", 0),
                float(seed_state_final.get("video_age_s", 999.0) or 999.0),
                seed_state_final.get("seed_video_bytes", 0),
            )
            if not seed_ready_final:
                logging.getLogger(__name__).info(
                    "Preferred player bootstrap still unavailable; waiting up to 4s more before launch"
                )
                seed_ready_final, seed_state_final = (
                    await _await_preferred_player_bootstrap(
                        coord,
                        timeout=4.0,
                    )
                )
                logging.getLogger(__name__).info(
                    "Player bootstrap final wait %s before launch (reason=%s, preferred=%s, backlog_ready=%s, backlog_target=%s, strong=%s, generation=%s/%s, seed_age=%.2fs, follow_frames=%s, video_age=%.2fs, bytes=%s)",
                    (
                        "ready"
                        if seed_ready_final
                        else "not ready; proceeding with live join"
                    ),
                    seed_state_final.get("block_reason", "unknown"),
                    seed_state_final.get("preferred_join_mode", "unknown"),
                    seed_state_final.get("backlog_ready", False),
                    seed_state_final.get("backlog_follow_video_pusi_target", 0),
                    seed_state_final.get("seed_strong", False),
                    seed_state_final.get("seed_generation", 0),
                    seed_state_final.get("required_seed_generation", 0),
                    float(seed_state_final.get("seed_age_s", 999.0) or 999.0),
                    seed_state_final.get("frames_since_seed", 0),
                    float(seed_state_final.get("video_age_s", 999.0) or 999.0),
                    seed_state_final.get("seed_video_bytes", 0),
                )
                if not seed_ready_final:
                    logging.getLogger(__name__).warning(
                        "No preferred backlog bootstrap became available in time; launching anyway with live join fallback"
                    )
            if active_codec == "hevc":
                hevc_seed_ready, hevc_seed_state = await _await_hevc_clean_startup_seed(
                    coord,
                    timeout=12.0,
                )
                logging.getLogger(__name__).info(
                    "HEVC clean-seed gate %s before launch (reason=%s, probe=%s, strong=%s, startup_safe=%s, seed_age=%.2fs, video_age=%.2fs, generation=%s/%s)",
                    (
                        "ready"
                        if hevc_seed_ready
                        else "timeout; launching with best available seed"
                    ),
                    hevc_seed_state.get("seed_strength_reason", ""),
                    hevc_seed_state.get("hevc_gate_probe_reason", ""),
                    hevc_seed_state.get("seed_strong", False),
                    hevc_seed_state.get("startup_safe", False),
                    float(hevc_seed_state.get("seed_age_s", 999.0) or 999.0),
                    float(hevc_seed_state.get("video_age_s", 999.0) or 999.0),
                    hevc_seed_state.get("seed_generation", 0),
                    hevc_seed_state.get("required_seed_generation", 0),
                )
            play_env = os.environ.copy()
            player_log_fh = open(player_log_path, "wb")

            logging.getLogger(__name__).info("Launching player: %s", player_cmd[0])
            player_proc = subprocess.Popen(
                player_cmd,
                stdin=subprocess.DEVNULL,
                stdout=player_log_fh,
                stderr=subprocess.STDOUT,
                env=play_env,
            )
            player_started_mono = time.monotonic()
            player_decode_corr_state["player_started_mono"] = player_started_mono
            player_visual_state["player_started_mono"] = player_started_mono
            player_decode_corr_stop.clear()
            player_decode_corr_thread = threading.Thread(
                target=_monitor_player_decode_correlation,
                args=(
                    coord,
                    player_log_path,
                    player_decode_corr_stop,
                    player_decode_corr_state,
                ),
                daemon=True,
            )
            player_decode_corr_thread.start()
            player_visual_stop.clear()
            player_visual_thread = threading.Thread(
                target=_monitor_player_visual_state,
                args=(player_log_path, player_visual_stop, player_visual_state),
                daemon=True,
            )
            player_visual_thread.start()

            # Give ffplay a short head start to lock to video.
            await asyncio.sleep(0.6)

            # Sidecar ffmpeg clients are stricter than ffplay about early
            # stream probing. Wait for post-launch frame progression so they
            # don't fail with "could not find codec parameters".
            sidecar_start_frames = int(getattr(coord, "_p2p_video_frames", 0))
            sidecar_deadline = time.monotonic() + 6.0
            while time.monotonic() < sidecar_deadline:
                await asyncio.sleep(0.2)
                frames = int(getattr(coord, "_p2p_video_frames", 0))
                last_video = float(getattr(coord, "_last_p2p_video_time", 0.0))
                age = (time.monotonic() - last_video) if last_video > 0 else 999.0
                if (frames - sidecar_start_frames) >= 8 and age < 1.0:
                    break
            await _await_startup_safe_bootstrap(coord, timeout=4.0)

            # Launch a separate ffmpeg recorder as a second TCP client
            recorder_cmd = _build_stream_recorder_cmd(
                url,
                ts_record_path,
                duration=0,
            )
            logging.getLogger(__name__).info(
                "Launching stream recorder: ffmpeg → %s",
                ts_record_path,
            )
            recorder_log_fh = open(recorder_log_path, "wb")
            recorder_proc = subprocess.Popen(
                recorder_cmd,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=recorder_log_fh,
                env=play_env,
            )
            recorder_started_mono = time.monotonic()

            # Launch PCM audio recorder as a third TCP client
            pcm_cmd = _build_pcm_recorder_cmd(
                url,
                pcm_record_path,
                duration=0,
            )
            logging.getLogger(__name__).info(
                "Launching PCM audio recorder: ffmpeg → %s",
                pcm_record_path,
            )
            pcm_recorder_proc = subprocess.Popen(
                pcm_cmd,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                env=play_env,
            )

            # Surface failures quickly instead of silently continuing.
            await asyncio.sleep(1)
            if recorder_proc.poll() is not None:
                logging.getLogger(__name__).warning(
                    "Recorder exited immediately with rc=%s, retrying once",
                    recorder_proc.returncode,
                )
                if recorder_log_fh is not None:
                    recorder_log_fh.flush()
                    recorder_log_fh.close()
                recorder_log_fh = open(recorder_log_path, "ab")
                recorder_proc = subprocess.Popen(
                    recorder_cmd,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=recorder_log_fh,
                    env=play_env,
                )
                recorder_started_mono = time.monotonic()
                await asyncio.sleep(1.2)
            if pcm_recorder_proc.poll() is not None:
                logging.getLogger(__name__).warning(
                    "PCM recorder exited immediately with rc=%s, retrying once",
                    pcm_recorder_proc.returncode,
                )
                pcm_recorder_proc = subprocess.Popen(
                    pcm_cmd,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    env=play_env,
                )
                await asyncio.sleep(1.2)
            if player_proc.poll() is not None:
                raise RuntimeError(
                    f"player exited early with rc={player_proc.returncode}"
                )

            # --- Audio diagnostic monitor ---
            def _audio_diag(pid: int) -> None:
                """Poll PipeWire/PulseAudio to verify ffplay audio output."""
                log = logging.getLogger(__name__)
                checked = 0
                found_stream = False
                while player_proc.poll() is None:
                    time.sleep(2)
                    checked += 1
                    try:
                        raw = subprocess.check_output(
                            ["pactl", "list", "sink-inputs"],
                            timeout=3,
                            stderr=subprocess.DEVNULL,
                        ).decode(errors="replace")
                    except Exception:
                        if checked <= 2:
                            log.warning("Audio diag: pactl not available")
                        break
                    # Find sink-input belonging to our ffplay.
                    # ffplay uses SDL, which registers as "SDL Application"
                    # and may report a different PID (e.g. thread PID).
                    blocks = raw.split("Sink Input #")
                    ffplay_block = None
                    ffplay_idx = None
                    for block in blocks[1:]:
                        is_pid = f'pid = "{pid}"' in block or f"pid = {pid}" in block
                        is_sdl = (
                            "SDL Application" in block
                            or "ffplay" in block.lower()
                            or "mpv" in block.lower()
                        )
                        if is_pid or is_sdl:
                            ffplay_idx = block.split("\n")[0].strip()
                            ffplay_block = block
                            break
                    if ffplay_block is not None:
                        muted = "UNKNOWN"
                        vol = "UNKNOWN"
                        corked = False
                        app_name = ""
                        sink_id = "UNKNOWN"
                        for line in ffplay_block.splitlines():
                            ls = line.strip()
                            if ls.startswith("Mute:"):
                                muted = ls
                            elif ls.startswith("Volume:") and "Base" not in ls:
                                vol = ls
                            elif ls.startswith("Corked:"):
                                corked = "yes" in ls.lower()
                            elif "application.name" in ls:
                                app_name = ls
                            elif ls.startswith("Sink:"):
                                sink_id = ls
                        # Resolve sink name
                        sink_name = sink_id
                        if sink_id != "UNKNOWN":
                            try:
                                sinks_raw = subprocess.check_output(
                                    ["pactl", "list", "sinks", "short"],
                                    timeout=2,
                                    stderr=subprocess.DEVNULL,
                                ).decode(errors="replace")
                                sid = sink_id.replace("Sink:", "").strip()
                                for sl in sinks_raw.splitlines():
                                    parts = sl.split("\t")
                                    if len(parts) >= 2 and parts[0].strip() == sid:
                                        sink_name = f"Sink: {sid} ({parts[1]})"
                                        break
                            except Exception:
                                pass
                        if not found_stream:
                            log.info(
                                "Audio diag: FOUND sink-input #%s — %s | "
                                "%s | %s | %s | corked=%s",
                                ffplay_idx,
                                app_name,
                                sink_name,
                                muted,
                                vol,
                                corked,
                            )
                            found_stream = True
                        elif checked % 5 == 0:
                            log.info(
                                "Audio diag: sink-input #%s — %s | %s | %s | corked=%s",
                                ffplay_idx,
                                sink_name,
                                muted,
                                vol,
                                corked,
                            )
                    elif not found_stream and checked <= 5:
                        n_blocks = len(blocks) - 1
                        apps = []
                        for block in blocks[1:]:
                            for line in block.splitlines():
                                if (
                                    "application.name" in line
                                    or "application.process.id" in line
                                ):
                                    apps.append(line.strip())
                        log.warning(
                            "Audio diag: no sink-input for PID %d " "(%d inputs: %s)",
                            pid,
                            n_blocks,
                            "; ".join(apps[:10]),
                        )
                if not found_stream:
                    log.warning(
                        "Audio diag: ffplay NEVER registered an audio "
                        "stream with PipeWire/PulseAudio (PID %d)",
                        pid,
                    )

            _audio_diag_thread = threading.Thread(
                target=_audio_diag,
                args=(player_proc.pid,),
                daemon=True,
            )
            _audio_diag_thread.start()

        end_at = None
        if not args.play and args.duration > 0:
            end_at = time.time() + args.duration
        stall_started_at: float | None = None
        while True:
            if end_at is not None and time.time() >= end_at:
                break
            if args.play and player_proc is not None and args.duration > 0:
                # With --play, duration is wall-clock ffplay open time.
                if player_started_mono is not None and (
                    time.monotonic() - player_started_mono
                ) >= float(args.duration):
                    break
                if player_proc.poll() is not None:
                    break
            health_source.tick(coord)

            if effective_stall_timeout > 0:
                stall_started_at = _maybe_restart_stalled_stream(
                    coord,
                    baseline_video_time,
                    stall_started_at,
                    effective_stall_timeout,
                )
            await asyncio.sleep(1)

        _t0 = time.time()
        if player_proc is not None and player_proc.poll() is None:
            _stop_player_process(player_proc)
        # Stop recorder too
        _stop_player_process(recorder_proc)
        _stop_player_process(pcm_recorder_proc)
        if player_log_fh is not None:
            player_log_fh.flush()
            player_log_fh.close()
            player_log_fh = None
        if player_decode_corr_thread is not None:
            player_decode_corr_stop.set()
            player_decode_corr_thread.join(timeout=1.5)
            player_decode_corr_thread = None
        if player_visual_thread is not None:
            player_visual_stop.set()
            player_visual_thread.join(timeout=1.5)
            player_visual_thread = None
        if recorder_log_fh is not None:
            recorder_log_fh.flush()
            recorder_log_fh.close()
            recorder_log_fh = None
        logging.getLogger(__name__).info(
            "Player/recorder stopped in %.1fs",
            time.time() - _t0,
        )

        source_summary = health_source.summary(coord)

        print("\nStream health (source ingress)")
        print("-" * 78)
        print(f"video_frames: {int(source_summary['video_frames'])}")
        print(f"avg_fps: {float(source_summary['avg_fps']):.2f}")
        print(f"max_video_gap_s: {float(source_summary['max_gap_s']):.2f}")
        print(f"recovered_stalls: {int(source_summary['recovered_stalls'])}")
        print(
            f"recovered_stalls_over_1s: {int(source_summary['recovered_stalls_over_1s'])}"
        )
        print(
            f"recovered_stalls_over_3s: {int(source_summary['recovered_stalls_over_3s'])}"
        )
        unresolved = float(source_summary["unresolved_stall_s"])
        if unresolved > 0.0:
            print(f"unresolved_stall_s: {unresolved:.2f}")

        # --- TS recording analysis ---
        if args.play:
            log = logging.getLogger(__name__)
            run_failed = False
            _t_player = time.time()
            player_metrics = _analyze_player_log(player_log_path, log)
            player_metrics.update(_summarize_player_visual_state(player_visual_state))
            player_metrics["player_has_issues"] = bool(
                player_metrics.get("player_has_issues", False)
                or player_metrics.get("player_visual_has_issues", False)
            )
            log.info("Player log analysis completed in %.1fs", time.time() - _t_player)
            if player_metrics:
                print("\nPlayer log analysis (what ffplay actually reported)")
                print("-" * 78)
                for k, v in player_metrics.items():
                    if isinstance(v, list):
                        print(f"  {k}: {v}")
                    else:
                        print(f"  {k}: {v}")
                run_failed = run_failed or bool(
                    player_metrics.get("player_has_issues", False)
                )
                _print_player_decode_correlation(coord, player_decode_corr_state)
                _print_stream_join_diagnostics(coord)

            _t_rec = time.time()
            recorder_metrics = _analyze_recorder_log(recorder_log_path, log)
            log.info("Recorder log analysis completed in %.1fs", time.time() - _t_rec)
            recorder_only_timestamp_discontinuities = False
            if recorder_metrics:
                print("\nRecorder log analysis (what ffmpeg recorder reported)")
                print("-" * 78)
                for k, v in recorder_metrics.items():
                    if isinstance(v, list):
                        print(f"  {k}: {v}")
                    else:
                        print(f"  {k}: {v}")
                recorder_only_timestamp_discontinuities = bool(
                    int(recorder_metrics.get("recorder_decode_error_lines", 0) or 0)
                    == 0
                    and int(
                        recorder_metrics.get("recorder_continuity_error_lines", 0) or 0
                    )
                    > 0
                    and int(
                        recorder_metrics.get("recorder_continuity_error_lines", 0) or 0
                    )
                    == int(
                        recorder_metrics.get(
                            "recorder_timestamp_discontinuity_lines", 0
                        )
                        or 0
                    )
                )
                run_failed = run_failed or bool(
                    recorder_metrics.get("recorder_has_issues", False)
                    and not recorder_only_timestamp_discontinuities
                )
            else:
                print("\nRecorder log analysis (what ffmpeg recorder reported)")
                print("-" * 78)
                print("  recorder stderr was empty")
                recorder_only_timestamp_discontinuities = False

            _t1 = time.time()
            ts_metrics = _analyze_recorded_ts(ts_record_path, log)
            log.info("TS analysis completed in %.1fs", time.time() - _t1)
            ts_minor_glitch = False
            if ts_metrics:
                print("\nTS recording analysis (what the player received)")
                print("-" * 78)
                for k, v in ts_metrics.items():
                    if k == "decode_errors_sample":
                        print(f"  {k}:")
                        for line in v:
                            print(f"    {line}")
                    elif isinstance(v, list):
                        print(f"  {k}: {v}")
                    elif isinstance(v, float):
                        print(f"  {k}: {v:.2f}")
                    else:
                        print(f"  {k}: {v}")
                _print_ts_decode_correlation(coord, recorder_started_mono, ts_metrics)
                video_skip_durations = [
                    float(v)
                    for v in (ts_metrics.get("video_skip_durations_s", []) or [])
                ]
                max_video_skip_s = (
                    max(video_skip_durations) if video_skip_durations else 0.0
                )
                ts_minor_glitch = bool(
                    int(ts_metrics.get("decode_error_lines", 0) or 0) == 0
                    and int(ts_metrics.get("audio_gap_count", 0) or 0) == 0
                    and int(ts_metrics.get("video_skip_count", 0) or 0) <= 1
                    and max_video_skip_s < 1.0
                )
                run_failed = run_failed or bool(
                    int(ts_metrics.get("decode_error_lines", 0) or 0) > 0
                    or int(ts_metrics.get("audio_gap_count", 0) or 0) > 0
                    or (
                        int(ts_metrics.get("video_skip_count", 0) or 0) > 0
                        and not ts_minor_glitch
                    )
                )
            else:
                run_failed = True

            if player_metrics and ts_metrics:
                player_has_issues = bool(player_metrics.get("player_has_issues", False))
                ts_looks_clean = (
                    int(ts_metrics.get("decode_error_lines", 0) or 0) == 0
                    and int(ts_metrics.get("video_skip_count", 0) or 0) == 0
                )
                if player_has_issues and ts_looks_clean:
                    print("\nResult consistency warning")
                    print("-" * 78)
                    print(
                        "  ffplay reported player-visible decode/continuity issues even though"
                    )
                    print(
                        "  the recorded TS analysis looked clean. Treat the run as failed or"
                    )
                    print(
                        "  degraded; the player log is authoritative for viewer experience."
                    )
                    run_failed = True

            # --- PCM audio content analysis ---
            _t2 = time.time()
            pcm_metrics = _analyze_pcm_audio(pcm_record_path, log)
            log.info("PCM analysis completed in %.1fs", time.time() - _t2)
            if pcm_metrics:
                print("\nPCM audio analysis (actual audible content)")
                print("-" * 78)
                for k, v in pcm_metrics.items():
                    if isinstance(v, list):
                        print(f"  {k}: {v}")
                    elif isinstance(v, float):
                        print(f"  {k}: {v:.2f}")
                    else:
                        print(f"  {k}: {v}")
            else:
                run_failed = True

            print("\nOverall test verdict")
            print("-" * 78)
            if run_failed:
                print("  degraded/fail")
                print(
                    "  One or more of: player log, recorder log, or TS analysis reported"
                )
                print("  viewer-visible issues. Exit status will be non-zero.")
                return 3
            if ts_minor_glitch or recorder_only_timestamp_discontinuities:
                print("  acceptable")
                print(
                    "  ffplay stayed clean; only a short isolated TS skip and/or recorder"
                )
                print("  timestamp-offset warnings remained. Exit status will be zero.")
                return 0
            print("  clean")
            print("  Player log, recorder log, and TS analysis all look clean.")

        return 0
    finally:
        _stop_player_process(player_proc)
        _stop_player_process(recorder_proc)
        _stop_player_process(pcm_recorder_proc)
        if player_log_fh is not None:
            try:
                player_log_fh.flush()
                player_log_fh.close()
            except Exception:
                pass
        if player_decode_corr_thread is not None:
            player_decode_corr_stop.set()
            player_decode_corr_thread.join(timeout=1.0)
        if recorder_log_fh is not None:
            try:
                recorder_log_fh.flush()
                recorder_log_fh.close()
            except Exception:
                pass
        if coord:
            await coord.async_stop()


async def cmd_bench(args) -> int:
    coord = None
    try:
        setattr(args, "skip_initial_grab", bool(args.idle_still))
        if args.idle_still:
            logging.getLogger(__name__).info(
                "Auto-disabling startup frame grab for idle-still benchmark"
            )

        coord, dev, mods = await _create_coordinator(args)
        bench_mode = "idle-still" if args.idle_still else "live"
        live_ok: bool | None = None
        if not args.idle_still:
            coord.wake_camera()
            live_ok = await _await_live_stream(
                coord,
                timeout=args.wake_timeout,
                stall_timeout=0,
            )

        url = await _camera_stream_source(mods, coord)
        print("=" * 78)
        print(f"Benchmarking device: {dev.get('deviceName')} ({dev.get('snNum')})")
        print(f"Mode: {bench_mode}")
        print(f"Input URL: {url}")
        print(f"Duration: {args.duration}s")
        print(f"camera_awake={coord.camera_awake}")
        if live_ok is None:
            print("live_ready=n/a (idle-still mode)")
        else:
            print(f"live_ready={live_ok}")
        print("=" * 78)

        if not args.idle_still and not bool(live_ok):
            print("ERROR: no live video frames received after wake attempts")
            return 2

        def _pid_provider() -> list[int]:
            pids = [os.getpid()]
            ff = getattr(coord, "_ffmpeg_proc", None)
            if ff and ff.poll() is None:
                pids.append(ff.pid)
            return pids

        sampler = CpuSampler(_pid_provider, interval=1.0)
        sampler.start()

        try:
            if args.idle_still:
                probe_rc, probe_lines = await asyncio.wait_for(
                    _run_idle_client_probe(url, args.duration),
                    timeout=args.duration + 20,
                )
            else:
                probe_rc, probe_lines = await asyncio.wait_for(
                    _run_probe(url, args.duration),
                    timeout=args.duration + 20,
                )
        except asyncio.TimeoutError:
            probe_rc, probe_lines = 124, ["probe timeout"]
        cpu = await sampler.stop()

        warn_lines = [
            ln
            for ln in probe_lines
            if "non monoton" in ln.lower()
            or "timestamp" in ln.lower()
            or "dts" in ln.lower()
            or "pts" in ln.lower()
            or "invalid" in ln.lower()
            or "dropped" in ln.lower()
        ]

        print("\nProbe summary")
        print("-" * 78)
        if args.idle_still:
            print("probe engine: idle-client")
        else:
            print("probe engine: ffmpeg")
        print(f"probe rc: {probe_rc}")
        if args.idle_still and probe_lines:
            print("probe details:")
            for ln in probe_lines:
                print(f"  {ln}")
        print(f"warning-like lines: {len(warn_lines)}")
        if warn_lines:
            print("first warnings:")
            for ln in warn_lines[:20]:
                print(f"  {ln}")

        print("\nCPU summary")
        print("-" * 78)
        print(f"avg cpu% (python + integration ffmpeg): {cpu['avg']:.2f}")
        print(f"max cpu% (python + integration ffmpeg): {cpu['max']:.2f}")

        # Non-zero probe RC is informative but not always fatal for live inputs.
        return 0
    finally:
        if coord:
            await coord.async_stop()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="CloudEdge integration local harness")

    p.add_argument("--email", required=True, help="Account email")
    p.add_argument("--password", required=True, help="Account password")
    p.add_argument("--country-code", default="FR", help="Country code (e.g. FR)")
    p.add_argument("--phone-code", default="33", help="Phone code (e.g. 33)")
    p.add_argument(
        "--profile",
        default="cloudedge",
        choices=["cloudedge", "cloudplus", "iegeek"],
        help="App profile",
    )
    p.add_argument("--debug", action="store_true", help="Enable verbose logs")

    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("list", help="Login and list devices")

    p_stream = sub.add_parser(
        "stream", help="Start stream server using integration coordinator"
    )
    p_stream.add_argument(
        "--device-id", type=int, default=None, help="Select a specific deviceID"
    )
    p_stream.add_argument("--sn", default=None, help="Select a specific serial number")
    p_stream.add_argument(
        "--duration",
        type=int,
        default=0,
        help="Duration in seconds (with --play: ffplay on-screen time; without --play: harness run time, 0 = until Ctrl+C)",
    )
    p_stream.add_argument("--wake", action="store_true", help="Wake camera immediately")
    p_stream.add_argument(
        "--wake-timeout",
        type=int,
        default=45,
        help="Seconds to wait for live frames before playback/stream readiness checks",
    )
    p_stream.add_argument(
        "--play",
        dest="play",
        action="store_true",
        help="Auto-launch local ffplay preview",
    )
    p_stream.add_argument(
        "--quality",
        type=int,
        default=None,
        help="VVP quality profile ID (from 'list' command, e.g. 0=SD, 2=HD). Default: highest",
    )
    p_stream.add_argument(
        "--video-password",
        default=None,
        help="Optional camera video-encryption password (E2EE)",
    )

    p_bench = sub.add_parser(
        "bench", help="Run stream stability + CPU benchmark (live or idle-still)"
    )
    p_bench.add_argument(
        "--device-id", type=int, default=None, help="Select a specific deviceID"
    )
    p_bench.add_argument("--sn", default=None, help="Select a specific serial number")
    p_bench.add_argument(
        "--duration", type=int, default=45, help="Benchmark duration in seconds"
    )
    p_bench.add_argument(
        "--idle-still",
        action="store_true",
        help="Benchmark idle/still stream without waking camera",
    )
    p_bench.add_argument(
        "--wake-timeout",
        type=int,
        default=45,
        help="Seconds to wait for live frames before benchmarking (live mode)",
    )
    return p


async def _async_main(args) -> int:
    if args.command == "list":
        return await cmd_list(args)
    if args.command == "stream":
        return await cmd_stream(args)
    if args.command == "bench":
        return await cmd_bench(args)
    raise RuntimeError(f"Unknown command: {args.command}")


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if getattr(args, "debug", False):
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )
    else:
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
        )
    return asyncio.run(_async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
