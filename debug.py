from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
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
        "motion_event": _load_module(
            "custom_components.cloudplus.motion_event", base / "motion_event.py"
        ),
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


def _decode_mqtt_publish(packet: bytes) -> tuple[str, bytes]:
    if not packet:
        raise ValueError("Empty packet")
    packet_type = (packet[0] >> 4) & 0x0F
    if packet_type != 3:
        raise ValueError(f"Not a PUBLISH packet (type={packet_type})")

    rem_len = 0
    mul = 1
    idx = 1
    for _ in range(4):
        enc = packet[idx]
        idx += 1
        rem_len += (enc & 0x7F) * mul
        if (enc & 0x80) == 0:
            break
        mul *= 128
    else:
        raise ValueError("Malformed MQTT remaining length")

    body = packet[idx : idx + rem_len]
    if len(body) < 2:
        raise ValueError("MQTT body too short")

    topic_len = int.from_bytes(body[0:2], "big")
    if len(body) < 2 + topic_len:
        raise ValueError("MQTT topic truncated")

    topic = body[2 : 2 + topic_len].decode("utf-8", errors="replace")
    payload = body[2 + topic_len :]
    return topic, payload


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
        initial_grab_timeout=int(getattr(args, "initial_grab_timeout", 12)),
        snapshot_conversion_enabled=bool(getattr(args, "enable_snapshots", True)),
    )

    audio_gain_db = getattr(args, "audio_gain_db", None)
    if audio_gain_db is not None:
        # Apply CLI override before coordinator starts ffmpeg muxing.
        coord._audio_gain_db = float(audio_gain_db)

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


async def cmd_motion(args) -> int:
    mods = _bootstrap_integration_modules()
    parse_motion_event = mods["motion_event"].parse_motion_event

    packet_path = (REPO_ROOT / args.packet).resolve()
    packet = packet_path.read_bytes()
    topic, payload = _decode_mqtt_publish(packet)
    parsed = parse_motion_event(payload)

    print("=" * 78)
    print(f"Packet: {packet_path}")
    print(f"Topic:  {topic}")
    print("=" * 78)

    if not parsed:
        print("FAIL: payload is not a valid alarm event")
        return 1

    print(
        json.dumps(
            {
                "event": parsed.get("event"),
                "evt_raw": parsed.get("evt_raw"),
                "evt_int": parsed.get("evt_int"),
                "evt_name": parsed.get("evt_name"),
                "is_motion": parsed.get("is_motion"),
                "device_id": parsed.get("device_id"),
                "license_id": parsed.get("license_id"),
            },
            indent=2,
        )
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


async def _wait_for_mux_video_stream(url: str, timeout: int = 15) -> bool:
    """Wait until the stream URL exposes a video track via ffprobe.

    H264 sessions can briefly restart the muxer on codec switch. Launching
    ffplay too early can result in audio-only startup with no preview window.
    """
    deadline = time.monotonic() + max(1, int(timeout))
    while time.monotonic() < deadline:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe",
            "-v",
            "error",
            "-show_streams",
            "-of",
            "compact=p=0:nk=1",
            url,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=6)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            await asyncio.sleep(0.4)
            continue

        if proc.returncode == 0 and out:
            txt = out.decode(errors="replace")
            if "|video|" in txt:
                return True

        await asyncio.sleep(0.4)

    return False


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
) -> list[str]:
    """Build ffplay command for visible live playback."""
    if not shutil.which("ffplay"):
        raise RuntimeError("ffplay not found")

    return [
        "ffplay",
        "-hide_banner",
        "-nostats",
        "-window_title",
        "CloudEdge live",
        "-fflags",
        "+discardcorrupt",
        "-sync",
        "audio",
        "-analyzeduration",
        "3000000",
        "-probesize",
        "1000000",
        "-f",
        "mpegts",
        url,
    ]


async def cmd_stream(args) -> int:
    coord = None
    player_proc = None
    try:
        if args.wake and args.play and not args.skip_initial_grab:
            logging.getLogger(__name__).info(
                "Auto-disabling startup frame grab for player wake mode"
            )
            args.skip_initial_grab = True

        coord, dev, mods = await _create_coordinator(args)
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
        # When a local player is requested, default to waiting for live frames
        # to avoid opening a persistent black window.
        effective_wait_live = bool(args.wait_live or args.play)
        effective_stall_timeout = int(args.restart_stall_timeout)
        effective_wake_retry_interval = int(args.wake_retry_interval)
        if args.wake and args.play:
            # In player mode, a first keyframe may appear before a stable live
            # session is established. Keep self-healing enabled by default so
            # playback does not freeze on a static frame.
            if effective_stall_timeout <= 0:
                effective_stall_timeout = 12
            if effective_wake_retry_interval <= 0:
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
            probe_timeout = max(8, min(25, int(args.wake_timeout)))
            mux_video_ready = await _wait_for_mux_video_stream(
                url,
                timeout=probe_timeout,
            )
            if not mux_video_ready:
                raise RuntimeError(
                    "Video track not ready on stream source; refusing to launch ffplay audio-only."
                )

            player_cmd = _build_stream_player_cmd(url)
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
        next_wake_retry = (
            time.monotonic() + float(effective_wake_retry_interval)
            if effective_wake_retry_interval > 0
            else float("inf")
        )
        stall_started_at: float | None = None
        while end_at is None or time.time() < end_at:
            if effective_stall_timeout > 0:
                stall_started_at = _maybe_restart_stalled_stream(
                    coord,
                    baseline_video_time,
                    stall_started_at,
                    effective_stall_timeout,
                )
            if (
                args.wake
                and effective_wake_retry_interval > 0
                and time.monotonic() >= next_wake_retry
            ):
                # Keep nudging wake until real live frames arrive.
                if not _has_live_video(coord, baseline_video_time):
                    coord.wake_camera()
                next_wake_retry = time.monotonic() + float(
                    effective_wake_retry_interval
                )
            await asyncio.sleep(1)
        return 0
    finally:
        if player_proc and player_proc.poll() is None:
            player_proc.send_signal(signal.SIGINT)
        if coord:
            await coord.async_stop()


async def cmd_bench(args) -> int:
    coord = None
    try:
        if args.idle_still and not args.skip_initial_grab:
            logging.getLogger(__name__).info(
                "Auto-disabling startup frame grab for idle-still benchmark"
            )
            args.skip_initial_grab = True

        coord, dev, mods = await _create_coordinator(args)
        bench_mode = "idle-still" if args.idle_still else "live"
        live_ok: bool | None = None
        if not args.idle_still:
            coord.wake_camera()
            live_ok = await _await_live_stream(
                coord,
                timeout=args.wake_timeout,
                stall_timeout=args.restart_stall_timeout,
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

    p_motion = sub.add_parser("motion", help="Parse motion packet from capture")
    p_motion.add_argument(
        "--packet",
        default="record-cloudedge-streaming/66/5.bin",
        help="Path to MQTT publish packet (.bin)",
    )

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
        "--wait-live",
        action="store_true",
        help="Wait for live frames before launching local player",
    )
    p_stream.add_argument(
        "--wake-timeout",
        type=int,
        default=45,
        help="Seconds to wait for live frames when --wait-live is used",
    )
    p_stream.add_argument(
        "--restart-stall-timeout",
        type=int,
        default=0,
        help="Seconds without live video before forcing P2P restart (0 disables, but --wake --play auto-uses 12)",
    )
    p_stream.add_argument(
        "--wake-retry-interval",
        type=int,
        default=0,
        help="Repeat wake every N seconds during stream (0 disables, but --wake --play auto-uses 8)",
    )
    p_stream.add_argument(
        "--skip-initial-grab",
        action="store_true",
        help="Skip coordinator startup frame-grab (faster start, less reliable wake)",
    )
    p_stream.add_argument(
        "--initial-grab-timeout",
        type=int,
        default=12,
        help="Seconds to wait for startup frame-grab before continuing",
    )
    p_stream.add_argument(
        "--audio-gain-db",
        type=float,
        default=None,
        help="Override ffmpeg audio gain in dB (e.g. 12, 18, 24)",
    )
    p_stream_snapshots = p_stream.add_mutually_exclusive_group()
    p_stream_snapshots.add_argument(
        "--enable-snapshots",
        dest="enable_snapshots",
        action="store_true",
        help="Enable HEVC->JPEG snapshot conversion during stream (default)",
    )
    p_stream_snapshots.add_argument(
        "--disable-snapshots",
        dest="enable_snapshots",
        action="store_false",
        help="Disable HEVC->JPEG snapshot conversion during stream",
    )
    p_stream.set_defaults(enable_snapshots=True)
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
    p_bench.add_argument(
        "--restart-stall-timeout",
        type=int,
        default=0,
        help="Seconds without live video before forcing P2P restart (0 disables, live mode)",
    )
    p_bench.add_argument(
        "--skip-initial-grab",
        action="store_true",
        help="Skip coordinator startup frame-grab (faster start, less reliable wake)",
    )
    p_bench.add_argument(
        "--initial-grab-timeout",
        type=int,
        default=12,
        help="Seconds to wait for startup frame-grab before continuing",
    )
    p_bench.add_argument(
        "--audio-gain-db",
        type=float,
        default=None,
        help="Override ffmpeg audio gain in dB during benchmark",
    )
    p_bench_snapshots = p_bench.add_mutually_exclusive_group()
    p_bench_snapshots.add_argument(
        "--enable-snapshots",
        dest="enable_snapshots",
        action="store_true",
        help="Enable HEVC->JPEG snapshot conversion during benchmark (default)",
    )
    p_bench_snapshots.add_argument(
        "--disable-snapshots",
        dest="enable_snapshots",
        action="store_false",
        help="Disable HEVC->JPEG snapshot conversion during benchmark",
    )
    p_bench.set_defaults(enable_snapshots=True)
    return p


async def _async_main(args) -> int:
    if args.command == "list":
        return await cmd_list(args)
    if args.command == "motion":
        return await cmd_motion(args)
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
