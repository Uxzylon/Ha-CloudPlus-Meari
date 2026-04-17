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
        profiles_str = ", ".join(
            f"{k}={v}" for k, v in sorted(profiles.items())
        ) if profiles else "none"
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
        "100000",
        "-probesize",
        "65536",
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
        "ffmpeg", "-hide_banner", "-loglevel", "warning",
        "-fflags", "+discardcorrupt+nobuffer",
        "-f", "mpegts",
        "-i", url,
        "-c", "copy",
        "-f", "mpegts", "-y", output_path,
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
        "ffmpeg", "-hide_banner", "-loglevel", "warning",
        "-fflags", "+discardcorrupt+nobuffer",
        "-f", "mpegts",
        "-i", url,
        "-vn",                     # drop video
        "-acodec", "pcm_s16le",    # raw PCM
        "-ar", "16000",            # 16 kHz (matches our AAC encoder)
        "-ac", "1",                # mono
        "-f", "wav", "-y", output_path,
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
    result["pcm_avg_rms_all"] = round(
        sum(chunk_rms_values) / len(chunk_rms_values), 1
    )

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

    # Count audible segments (contiguous audible runs)
    audible_seg_count = 0
    in_audible = False
    for silent in is_silence:
        if not silent:
            if not in_audible:
                audible_seg_count += 1
                in_audible = True
        else:
            in_audible = False

    result["pcm_audible_segments"] = audible_seg_count
    result["pcm_silence_gap_count"] = len(silence_gaps)
    if silence_gaps:
        result["pcm_max_silence_gap_s"] = round(max(silence_gaps), 3)
        result["pcm_avg_silence_gap_s"] = round(
            sum(silence_gaps) / len(silence_gaps), 3
        )
        result["pcm_silence_gap_durations_s"] = [
            round(g, 3)
            for g in sorted(silence_gaps, reverse=True)[:10]
        ]

    # Timeline summary: first/last audible chunk position
    audible_indices = [i for i, s in enumerate(is_silence) if not s]
    if audible_indices:
        result["pcm_first_audible_at_s"] = round(
            audible_indices[0] * chunk_dur, 2
        )
        result["pcm_last_audible_at_s"] = round(
            audible_indices[-1] * chunk_dur, 2
        )

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
                "ffprobe", "-hide_banner", "-loglevel", "error",
                "-select_streams", "v:0",
                "-show_entries", "frame=key_frame,best_effort_timestamp_time",
                "-of", "csv=p=0",
                ts_path,
            ],
            timeout=30, stderr=subprocess.DEVNULL,
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
        result["video_avg_fps"] = round((len(pts_list) - 1) / span, 2) if span > 0 else 0

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
                result["video_skip_durations_s"] = [round(g, 3) for g in sorted(skips, reverse=True)[:10]]
            # Duplicate PTS (frozen frames)
            n_dup = sum(1 for g in gaps if g < 0.001)
            result["video_duplicate_pts"] = n_dup

    # --- ffprobe: extract audio frame timestamps ---
    try:
        raw_audio = subprocess.check_output(
            [
                "ffprobe", "-hide_banner", "-loglevel", "error",
                "-select_streams", "a:0",
                "-show_entries", "frame=best_effort_timestamp_time",
                "-of", "csv=p=0",
                ts_path,
            ],
            timeout=15, stderr=subprocess.DEVNULL,
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
                result["audio_gap_durations_s"] = [round(g, 3) for g in sorted(a_skips, reverse=True)[:10]]

    # --- ffmpeg decode pass: count errors ---
    try:
        err_raw = subprocess.check_output(
            [
                "ffmpeg", "-hide_banner", "-loglevel", "error",
                "-i", ts_path,
                "-f", "null", "-",
            ],
            timeout=30, stderr=subprocess.STDOUT,
        ).decode(errors="replace")
        error_lines = [l for l in err_raw.splitlines() if l.strip()]
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

    return result


async def cmd_stream(args) -> int:
    coord = None
    player_proc = None
    recorder_proc: subprocess.Popen | None = None
    pcm_recorder_proc: subprocess.Popen | None = None
    ts_record_path = "/tmp/cloudedge_stream_recording.ts"
    pcm_record_path = "/tmp/cloudedge_audio_recording.wav"
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
        auto_lossy_for_player = bool(args.wake and args.play)
        setattr(coord, "_p2p_allow_lossy_gap_skip", auto_lossy_for_player)
        setattr(coord, "_p2p_adaptive_lossy_gap_skip", auto_lossy_for_player)
        if auto_lossy_for_player:
            logging.getLogger(__name__).info(
                "Auto-enabling lossy gap skip for wake+play stability"
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
            logging.getLogger(__name__).info("Delaying player 1s for stream startup")
            await asyncio.sleep(1)
            play_env = os.environ.copy()
            _player_log = open("/tmp/player_test.log", "wb")

            logging.getLogger(__name__).info("Launching player: %s", player_cmd[0])
            player_proc = subprocess.Popen(
                player_cmd,
                stdin=subprocess.DEVNULL,
                stdout=_player_log,
                stderr=subprocess.STDOUT,
                env=play_env,
            )

            # Launch a separate ffmpeg recorder as a second TCP client
            recorder_cmd = _build_stream_recorder_cmd(
                url, ts_record_path, duration=int(args.duration),
            )
            logging.getLogger(__name__).info(
                "Launching stream recorder: ffmpeg → %s", ts_record_path,
            )
            recorder_proc = subprocess.Popen(
                recorder_cmd,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                env=play_env,
            )

            # Launch PCM audio recorder as a third TCP client
            pcm_cmd = _build_pcm_recorder_cmd(
                url, pcm_record_path, duration=int(args.duration),
            )
            logging.getLogger(__name__).info(
                "Launching PCM audio recorder: ffmpeg → %s", pcm_record_path,
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
                            timeout=3, stderr=subprocess.DEVNULL,
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
                        is_pid = (f"pid = \"{pid}\"" in block
                                  or f"pid = {pid}" in block)
                        is_sdl = "SDL Application" in block or "ffplay" in block.lower() or "mpv" in block.lower()
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
                                    timeout=2, stderr=subprocess.DEVNULL,
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
                                ffplay_idx, app_name, sink_name, muted, vol, corked,
                            )
                            found_stream = True
                        elif checked % 5 == 0:
                            log.info(
                                "Audio diag: sink-input #%s — %s | %s | %s | corked=%s",
                                ffplay_idx, sink_name, muted, vol, corked,
                            )
                    elif not found_stream and checked <= 5:
                        n_blocks = len(blocks) - 1
                        apps = []
                        for block in blocks[1:]:
                            for line in block.splitlines():
                                if "application.name" in line or "application.process.id" in line:
                                    apps.append(line.strip())
                        log.warning(
                            "Audio diag: no sink-input for PID %d "
                            "(%d inputs: %s)",
                            pid, n_blocks, "; ".join(apps[:10]),
                        )
                if not found_stream:
                    log.warning(
                        "Audio diag: ffplay NEVER registered an audio "
                        "stream with PipeWire/PulseAudio (PID %d)", pid,
                    )

            _audio_diag_thread = threading.Thread(
                target=_audio_diag, args=(player_proc.pid,), daemon=True,
            )
            _audio_diag_thread.start()

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

        _t0 = time.time()
        if player_proc is not None and player_proc.poll() is None:
            _stop_player_process(player_proc)
        # Stop recorder too
        _stop_player_process(recorder_proc)
        _stop_player_process(pcm_recorder_proc)
        logging.getLogger(__name__).info(
            "Player/recorder stopped in %.1fs", time.time() - _t0,
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
            _t1 = time.time()
            ts_metrics = _analyze_recorded_ts(ts_record_path, log)
            log.info("TS analysis completed in %.1fs", time.time() - _t1)
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

        return 0
    finally:
        _stop_player_process(player_proc)
        _stop_player_process(recorder_proc)
        _stop_player_process(pcm_recorder_proc)
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
    p_stream.add_argument(
        "--quality",
        type=int,
        default=None,
        help="VVP quality profile ID (from 'list' command, e.g. 0=SD, 2=HD). Default: highest",
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
