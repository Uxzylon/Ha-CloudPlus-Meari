from __future__ import annotations

import argparse
import asyncio
import importlib.util
import logging
import os
import signal
import shutil
import subprocess
import sys
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
        "p2p_streamer": _load_module(
            "custom_components.cloudplus.p2p_streamer", base / "p2p_streamer.py"
        ),
        "coordinator": _load_module(
            "custom_components.cloudplus.coordinator", base / "coordinator.py"
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
        self._frame_count_reader = frame_count_reader or self._default_frame_count_reader
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
            getattr(coord, "_last_p2p_video_time", getattr(coord, "_last_video_time", 0.0))
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

        progressed = (frame_ts > self._last_frame_ts) or (frame_count > self._last_frame_count)
        if progressed:
            if self._last_frame_ts > 0.0:
                self._max_gap_s = max(self._max_gap_s, max(0.0, frame_ts - self._last_frame_ts))
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
            self._last_stall_log_mono <= 0.0 or (now_mono - self._last_stall_log_mono) >= 2.0
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


async def cmd_list(args) -> int:
    mods = _bootstrap_integration_modules()
    MeariApiClient = mods["api"].MeariApiClient

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
        print(
            f"{i:2d}. id={dev.get('deviceID')} sn={dev.get('snNum')} "
            f"name={dev.get('deviceName')} category={dev.get('_category')}"
        )
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

    codec_l = (codec or "hevc").lower()
    if codec_l == "h264":
        analyzeduration = "250000"
        probesize = "131072"
    else:
        analyzeduration = "1000000"
        probesize = "524288"

    cmd = [
        "ffplay",
        "-hide_banner",
        "-stats",
        "-loglevel",
        "info",
        "-window_title",
        "CloudEdge live",
        "-fflags",
        "+discardcorrupt",
        "-sync",
        "video",
        "-analyzeduration",
        analyzeduration,
        "-probesize",
        probesize,
        "-f",
        "mpegts",
    ]
    if int(duration) > 0:
        cmd.extend(["-t", str(int(duration))])
    cmd.append(url)
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


async def cmd_stream(args) -> int:
    coord = None
    player_proc = None
    try:
        setattr(args, "skip_initial_grab", bool(args.wake and args.play))
        if args.wake and args.play:
            logging.getLogger(__name__).info(
                "Auto-disabling startup frame grab for player wake mode"
            )

        coord, dev, mods = await _create_coordinator(args)
        if args.play:
            # Keep a light gap-fill profile in player mode so short transport
            # hiccups do not look like repeated pause/resume jitter.
            setattr(coord, "_live_gap_fill_after_seconds", 1.2)
            setattr(coord, "_live_gap_fill_fps", 1.5)
        auto_lossy_for_player = bool(args.wake and args.play)
        setattr(coord, "_p2p_allow_lossy_gap_skip", False)
        setattr(coord, "_p2p_adaptive_lossy_gap_skip", auto_lossy_for_player)
        if auto_lossy_for_player:
            logging.getLogger(__name__).info(
                "Auto-enabling adaptive lossy recovery for wake+play stability"
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
                duration=int(args.duration),
                codec=active_codec,
            )
            logging.getLogger(__name__).info("Launching ffplay player")
            player_proc = subprocess.Popen(
                player_cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            # Surface failures quickly instead of silently continuing.
            await asyncio.sleep(1)
            if player_proc.poll() is not None:
                raise RuntimeError(
                    f"ffplay exited early with rc={player_proc.returncode}"
                )

        end_at = time.time() + args.duration if args.duration > 0 else None
        stall_started_at: float | None = None
        while end_at is None or time.time() < end_at:
            health_source.tick(coord)

            if effective_stall_timeout > 0:
                stall_started_at = _maybe_restart_stalled_stream(
                    coord,
                    baseline_video_time,
                    stall_started_at,
                    effective_stall_timeout,
                )
            await asyncio.sleep(1)

        if player_proc is not None and player_proc.poll() is None:
            _stop_player_process(player_proc)

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
        return 0
    finally:
        _stop_player_process(player_proc)
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
        choices=["cloudedge", "cloudplus"],
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
        help="Run duration in seconds (0 = until Ctrl+C)",
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
