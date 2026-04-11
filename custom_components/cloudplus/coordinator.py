"""Coordinator for CloudEdge / Meari camera — manages lifecycle.

Runs MQTT listener, P2P streaming, ffmpeg muxer, TCP stream server,
and idle stream in background threads. Manages camera wake/sleep state
and motion detection.

Follows the same architecture as the home_v reference coordinator.
"""

from __future__ import annotations

import fcntl
import logging
import os
import queue
import re
import socket
import ssl
import subprocess
import threading
import time
from typing import Any, Callable

from homeassistant.core import HomeAssistant

from .const import (
    DEFAULT_APP_PROFILE,
    DEFAULT_COUNTRY_CODE,
    DEFAULT_MOTION_TIMEOUT,
    DEFAULT_PHONE_CODE,
    DOMAIN,
)
from .api import MeariApiClient, format_sn
from .motion_event import parse_motion_event
from .p2p_streamer import P2PStreamer

_LOGGER = logging.getLogger(__name__)

# Polling intervals
STATUS_POLL_INTERVAL = 60.0
BATTERY_POLL_INTERVAL = 300.0


class CloudEdgeMeariCoordinator:
    """Manages connection to a single CloudEdge / Meari camera."""

    # Watch states
    _W_IDLE = 0
    _W_HANDSHAKING = 1
    _W_STREAMING = 2

    def __init__(
        self,
        hass: HomeAssistant,
        email: str,
        password: str,
        device: dict[str, Any],
        country_code: str = DEFAULT_COUNTRY_CODE,
        phone_code: str = DEFAULT_PHONE_CODE,
        app_profile: str = DEFAULT_APP_PROFILE,
        initial_frame_grab: bool = True,
        initial_grab_timeout: int = 12,
        snapshot_conversion_enabled: bool = True,
        snapshot_min_interval: float = 3.0,
    ) -> None:
        self.hass = hass
        self._email = email
        self._password = password
        self._country_code = country_code
        self._phone_code = phone_code
        self._app_profile = app_profile
        self._device = device
        self._device_id = device["deviceID"]
        self._sn_num = device.get("snNum", "")
        self._device_name = device.get("deviceName", self._sn_num)
        self._device_category = str(device.get("_category", "")).lower()
        self._is_snap = self._device_category == "snap"
        self._host_key = device.get("hostKey", "")
        self._motion_timeout = DEFAULT_MOTION_TIMEOUT
        self._initial_frame_grab = initial_frame_grab
        self._initial_grab_timeout = max(3, min(initial_grab_timeout, 45))
        self._snapshot_conversion_enabled = bool(snapshot_conversion_enabled)

        # Shared state
        self._latest_image: bytes | None = None
        self._latest_video_kf: bytes | None = None  # raw Annex-B keyframe for deferred JPEG conversion
        self._idle_scene_kf: bytes | None = None  # cached last scene keyframe for idle keepalive
        self._idle_video_kf: bytes | None = None  # lightweight keyframe used for long idle keepalive
        # Backward-compatible alias used by older debug tooling.
        self._latest_hevc_kf: bytes | None = None
        self._video_codec: str = "hevc"  # "hevc" or "h264"
        self._idle_since: float = time.monotonic()
        self._idle_scene_hold_seconds: float = 3.0
        self._idle_keepalive_fps_initial: float = 15.0
        self._idle_keepalive_fps_steady: float = 2.0
        self._idle_keepalive_fps_with_clients: float = 15.0
        self._idle_keepalive_settle_seconds: float = 5.0
        self._live_gap_fill_after_seconds: float = 0.5
        self._live_gap_fill_fps: float = 3.0
        self._audio_gain_db: float = 18.0
        self._last_p2p_video_time: float = 0.0  # monotonic timestamp of last camera video frame
        self._last_p2p_audio_time: float = 0.0  # monotonic timestamp of last camera audio frame
        self._last_video_time: float = 0.0  # monotonic timestamp of last video write
        self._p2p_video_frames: int = 0
        self._p2p_audio_frames: int = 0
        self._p2p_audio_bytes: int = 0
        self._p2p_audio_non_ff_bytes: int = 0
        self._p2p_audio_all_ff_frames: int = 0
        self._last_snapshot_convert_time: float = 0.0
        self._snapshot_convert_interval: float = max(0.5, float(snapshot_min_interval))
        self._snapshot_convert_lock = threading.Lock()
        self._idle_scene_convert_lock = threading.Lock()
        self._motion_detected = False
        self._motion_type: str = ""
        self._last_motion_time: float = 0.0
        self._available = False

        # Camera awake state
        self._camera_awake = False

        # Motion wake control
        self._motion_wake_enabled = True

        # Battery state
        self._battery_percent: int | None = None
        self._battery_charging: bool = False

        # Background thread
        self._thread: threading.Thread | None = None
        self._running = False

        # API client (created in background thread)
        self._api: MeariApiClient | None = None

        # Manual wake trigger
        self._wake_event = threading.Event()
        # Set after startup frame-grab phase completes for the current session.
        self._startup_ready = threading.Event()

        # Listeners
        self._motion_callbacks: list[Callable[[], None]] = []
        self._update_callbacks: list[Callable[[], None]] = []

        # MQTT client
        self._mqtt_client: Any = None
        self._mqtt_connected = False

        # Stream server (MPEG-TS over TCP for HA stream / Frigate)
        self._stream_port: int = 0
        self._stream_server_sock: socket.socket | None = None
        self._stream_clients: list[socket.socket] = []
        self._stream_pending: dict[socket.socket, bytearray] = {}
        self._stream_backlog: bytearray = bytearray()
        # Keep bootstrap backlog small so newly connected readers lock quickly
        # without inheriting seconds of old latency.
        self._stream_backlog_max_bytes: int = 131072
        # Keep enough headroom for downstream remuxers (go2rtc/frigate)
        # without forcing reconnect loops on short backpressure spikes.
        self._stream_max_pending_bytes: int = 524288
        self._stream_send_chunk_bytes: int = 262144
        self._stream_max_send_iters: int = 8
        self._stream_clients_lock = threading.Lock()
        self._stream_accept_thread: threading.Thread | None = None
        self._stream_epoch: float = 0.0  # wall-clock anchor for MPEG-TS timestamps

        # ffmpeg muxer (H264/HEVC + G.711 µ-law → MPEG-TS)
        self._ffmpeg_proc: subprocess.Popen | None = None
        self._ffmpeg_reader_thread: threading.Thread | None = None
        self._audio_write_fd: int = -1
        self._audio_primed = threading.Event()  # set when real camera audio arrives
        self._audio_writer_thread: threading.Thread | None = None
        self._audio_real_started: bool = False
        # Keep audio buffering shallow and realtime-friendly.
        self._audio_queue: queue.Queue = queue.Queue(maxsize=256)
        self._audio_queue_drops: int = 0
        self._audio_throttle_drops: int = 0
        self._audio_realtime_next_ts: float = 0.0
        self._audio_soft_queue_limit: int = 160

        # Video write queue to decouple P2P receive from ffmpeg stdin backpressure.
        self._video_queue: queue.Queue = queue.Queue(maxsize=192)
        self._video_lag_drop_threshold: int = 24
        self._video_queue_drops: int = 0
        self._video_mux_target_fps: float = 15.0
        self._video_fps_ema: float = 15.0
        self._video_fps_samples: int = 0
        self._video_fps_last_frame_time: float = 0.0
        self._video_size_hint: tuple[int, int] | None = None
        self._video_size_codec: str | None = None
        self._video_size_probe_after: float = 0.0
        self._video_size_probe_inflight: bool = False
        self._video_size_probe_lock = threading.Lock()
        self._black_keyframe_lock = threading.Lock()
        self._h264_sps_nal: bytes | None = None
        self._h264_pps_nal: bytes | None = None
        self._hevc_vps_nal: bytes | None = None
        self._hevc_sps_nal: bytes | None = None
        self._hevc_pps_nal: bytes | None = None

        # P2P streamer
        self._p2p_streamer: P2PStreamer | None = None
        self._stream_thread: threading.Thread | None = None
        self._stream_grab_only = False
        self._live_stream_requested = False
        # Lossy KCP gap skip trades continuity for liveness; keep it off by
        # default to avoid visible pause->jump artifacts.
        self._p2p_allow_lossy_gap_skip: bool = False
        # Adaptive lossy mode ramps recovery aggressiveness from observed
        # live-link behavior without forcing deepest skip from frame 1.
        self._p2p_adaptive_lossy_gap_skip: bool = False

        # Keep idle video alive so Frigate clients always see
        # continuous video packets even while battery cameras sleep.
        self._idle_video_keepalive = True

        # Stream host mode: "ip" (default) or "docker"
        self._stream_host_mode: str = "ip"

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def available(self) -> bool:
        return self._available

    @property
    def latest_image(self) -> bytes | None:
        return self._latest_image

    @property
    def motion_detected(self) -> bool:
        return self._motion_detected

    @property
    def motion_type(self) -> str:
        return self._motion_type

    @property
    def device_uuid(self) -> str:
        return self._sn_num

    @property
    def device_name(self) -> str:
        return self._device_name

    @property
    def is_battery_camera(self) -> bool:
        return self._is_snap

    @property
    def device_category(self) -> str:
        return self._device_category or "unknown"

    @property
    def device_model(self) -> str:
        if self._is_snap:
            return "Battery Camera (snap)"
        return f"Camera ({self.device_category})"

    @property
    def device_id(self) -> int:
        return self._device_id

    @property
    def stream_port(self) -> int:
        """TCP port for the MPEG-TS stream server."""
        return self._stream_port

    @property
    def camera_awake(self) -> bool:
        return self._camera_awake

    @property
    def motion_wake_enabled(self) -> bool:
        return self._motion_wake_enabled

    @property
    def motion_timeout(self) -> int:
        return self._motion_timeout

    @property
    def battery_percent(self) -> int | None:
        return self._battery_percent

    @property
    def battery_charging(self) -> bool:
        return self._battery_charging

    @property
    def stream_host_mode(self) -> str:
        return self._stream_host_mode

    @property
    def stream_host(self) -> str:
        """Return the host to use in stream URLs."""
        if self._stream_host_mode == "docker":
            return socket.gethostname()
        # IP mode — open a UDP socket to determine the LAN-facing IP.
        # No data is actually sent; connect() just selects the interface.
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except OSError:
            return "127.0.0.1"

    # ------------------------------------------------------------------
    # Setters
    # ------------------------------------------------------------------

    def set_motion_wake_enabled(self, enabled: bool) -> None:
        self._motion_wake_enabled = enabled
        _LOGGER.info(
            "Motion wake %s for %s",
            "enabled" if enabled else "disabled",
            self._sn_num,
        )
        self._fire_update()

    def set_motion_timeout(self, timeout: int) -> None:
        self._motion_timeout = max(10, min(timeout, 600))
        _LOGGER.info(
            "Motion timeout set to %ds for %s",
            self._motion_timeout, self._sn_num,
        )
        self._fire_update()

    def set_stream_host_mode(self, mode: str) -> None:
        """Set stream host mode: 'ip' or 'docker'."""
        self._stream_host_mode = mode
        _LOGGER.info(
            "Stream host mode set to %s for %s", mode, self._sn_num,
        )
        self._fire_update()

    def wake_camera(self) -> None:
        """Trigger a manual camera wake."""
        self._wake_event.set()
        _LOGGER.info("Manual wake requested for %s", self._sn_num)

    # ------------------------------------------------------------------
    # Callback management
    # ------------------------------------------------------------------

    def register_motion_callback(self, cb: Callable[[], None]) -> Callable[[], None]:
        self._motion_callbacks.append(cb)
        return lambda: self._motion_callbacks.remove(cb)

    def register_update_callback(self, cb: Callable[[], None]) -> Callable[[], None]:
        self._update_callbacks.append(cb)
        return lambda: self._update_callbacks.remove(cb)

    def _fire_motion(self) -> None:
        if self.hass.loop.is_closed():
            return
        for cb in self._motion_callbacks:
            self.hass.loop.call_soon_threadsafe(cb)

    def _fire_update(self) -> None:
        if self.hass.loop.is_closed():
            return
        for cb in self._update_callbacks:
            self.hass.loop.call_soon_threadsafe(cb)

    # ------------------------------------------------------------------
    # Stream server (MPEG-TS over TCP for HA stream / Frigate)
    # ------------------------------------------------------------------

    def _start_stream_server(self) -> None:
        """Start TCP server to serve MPEG-TS stream to clients."""
        try:
            self._stream_server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._stream_server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._stream_server_sock.bind(("0.0.0.0", 0))
            self._stream_port = self._stream_server_sock.getsockname()[1]
            self._stream_server_sock.listen(5)
            self._stream_server_sock.settimeout(2)
            self._stream_epoch = time.time()
            self._stream_accept_thread = threading.Thread(
                target=self._accept_stream_clients, daemon=True,
            )
            self._stream_accept_thread.start()
            _LOGGER.info(
                "Stream server started on port %d for %s",
                self._stream_port, self._sn_num,
            )
        except OSError as e:
            _LOGGER.error("Failed to start stream server: %s", e)

    def _stop_stream_server(self) -> None:
        """Stop the TCP stream server and disconnect all clients."""
        if self._stream_server_sock:
            try:
                self._stream_server_sock.close()
            except OSError:
                pass
            self._stream_server_sock = None
        if self._stream_accept_thread:
            self._stream_accept_thread.join(timeout=5)
            self._stream_accept_thread = None
        with self._stream_clients_lock:
            for c in self._stream_clients:
                try:
                    c.close()
                except OSError:
                    pass
            self._stream_clients.clear()
            self._stream_pending.clear()
            self._stream_backlog.clear()

    def _accept_stream_clients(self) -> None:
        """Accept loop for TCP stream clients (runs in thread)."""
        while self._running and self._stream_server_sock:
            try:
                client, addr = self._stream_server_sock.accept()
                client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                client.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1 << 20)
                # Non-blocking fanout avoids head-of-line blocking when one
                # downstream consumer is slower than the others.
                client.setblocking(False)
                with self._stream_clients_lock:
                    self._stream_clients.append(client)
                    self._stream_pending[client] = bytearray(self._stream_backlog)
                _LOGGER.debug("Stream client connected from %s", addr)
            except socket.timeout:
                continue
            except OSError:
                break

    def _append_stream_backlog(self, data: bytes) -> None:
        """Keep a bounded MPEG-TS backlog for new client bootstrap."""
        if not data:
            return
        self._stream_backlog.extend(data)
        if len(self._stream_backlog) > self._stream_backlog_max_bytes:
            overflow = len(self._stream_backlog) - self._stream_backlog_max_bytes
            del self._stream_backlog[:overflow]

        # Keep backlog aligned to MPEG-TS packet boundaries.
        if self._stream_backlog:
            first_sync = self._stream_backlog.find(b"\x47")
            if first_sync < 0:
                self._stream_backlog.clear()
                return
            if first_sync > 0:
                del self._stream_backlog[:first_sync]
            rem = len(self._stream_backlog) % 188
            if rem:
                del self._stream_backlog[-rem:]

    def _broadcast_stream(self, data: bytes) -> None:
        """Send MPEG-TS data to all connected stream clients."""
        if not data:
            return
        with self._stream_clients_lock:
            self._append_stream_backlog(data)
            dead: list[socket.socket] = []
            for client in list(self._stream_clients):
                pending = self._stream_pending.get(client)
                if pending is None:
                    pending = bytearray()
                    self._stream_pending[client] = pending
                if len(pending) + len(data) > self._stream_max_pending_bytes:
                    # Disconnect lagging consumers instead of trimming bytes in
                    # place, which can leave remuxers stuck on corrupted state.
                    dead.append(client)
                    continue
                pending.extend(data)
                try:
                    if pending:
                        for _ in range(self._stream_max_send_iters):
                            if not pending:
                                break
                            sent = client.send(pending[: self._stream_send_chunk_bytes])
                            if sent > 0:
                                del pending[:sent]
                                continue
                            if sent == 0:
                                dead.append(client)
                                break
                            break
                except (BlockingIOError, InterruptedError):
                    # Client socket is temporarily backpressured.
                    pass
                except (BrokenPipeError, ConnectionError, OSError, ValueError):
                    dead.append(client)
            for c in dead:
                if c in self._stream_clients:
                    self._stream_clients.remove(c)
                self._stream_pending.pop(c, None)
                try:
                    c.close()
                except OSError:
                    pass

    # ------------------------------------------------------------------
    # ffmpeg muxer (H264/HEVC + G.711 µ-law → MPEG-TS)
    # ------------------------------------------------------------------

    def _start_ffmpeg_muxer(self) -> None:
        """Start ffmpeg to mux camera video + G.711 audio into MPEG-TS."""
        if self._ffmpeg_proc is not None:
            return

        audio_r, audio_w = os.pipe()
        self._audio_write_fd = audio_w
        video_fmt = "h264" if self._video_codec == "h264" else "hevc"
        video_input_fps = max(5.0, min(60.0, float(self._video_mux_target_fps)))
        video_setts = (
            f"setts=pts=N/({video_input_fps:.3f}*TB):"
            f"dts=N/({video_input_fps:.3f}*TB)"
        )

        cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", "warning",
            # Video input (H264/HEVC NALs from stdin)
            "-fflags", "+genpts+igndts+discardcorrupt",
            "-err_detect", "ignore_err",
            "-probesize", "131072",
            "-analyzeduration", "500000",
            "-framerate", f"{video_input_fps:.3f}",
            "-thread_queue_size", "128",
            "-f", video_fmt, "-i", "pipe:0",
            # Audio input (G.711 µ-law from pipe)
            "-thread_queue_size", "256",
            "-f", "mulaw", "-ar", "8000", "-ac", "1",
            "-i", f"pipe:{audio_r}",
            # Output: pass-through camera video, transcode audio
            "-map", "0:v", "-map", "1:a",
            "-c:v", "copy",
            # Stabilize output timestamps from raw Annex-B input to avoid
            # burst/pause playback patterns caused by transport jitter.
            "-bsf:v", video_setts,
            # Keep camera audio in original G.711 µ-law format (no transcode)
            # to reduce CPU and avoid codec conversion artifacts.
            "-c:a", "copy",
            "-max_delay", "0",
            "-muxpreload", "0",
            "-muxdelay", "0",
            "-flush_packets", "1",
            "-f", "mpegts",
            "-mpegts_flags", "+resend_headers+pat_pmt_at_frames",
            "pipe:1",
        ]

        try:
            self._ffmpeg_proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                pass_fds=(audio_r,),
            )
            os.close(audio_r)

            # Audio callback runs on the P2P receive thread. Keep audio pipe
            # non-blocking so backpressure cannot stall video reception.
            try:
                flags = fcntl.fcntl(self._audio_write_fd, fcntl.F_GETFL)
                fcntl.fcntl(self._audio_write_fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
            except Exception:
                pass

            # Drop stale buffered audio from previous sessions.
            while True:
                try:
                    self._audio_queue.get_nowait()
                except queue.Empty:
                    break
            self._audio_queue_drops = 0
            self._audio_throttle_drops = 0
            self._video_queue_drops = 0
            self._audio_real_started = False
            self._audio_realtime_next_ts = 0.0

            # Serialize all writes through a dedicated writer thread so
            # partial non-blocking writes never corrupt realtime audio flow.
            proc_ref = self._ffmpeg_proc
            self._audio_writer_thread = threading.Thread(
                target=self._audio_writer,
                args=(proc_ref,),
                daemon=True,
            )
            self._audio_writer_thread.start()

            # Enlarge stdin pipe buffer to 1MB.
            # Without this, any brief ffmpeg decode stutter fills the
            # default 64KB pipe in <0.5s, blocking the recv loop
            # → no ACKs → camera send window fills → stream dies.
            try:
                fcntl.fcntl(
                    self._ffmpeg_proc.stdin.fileno(), 1031, 1048576
                )  # F_SETPIPE_SZ
            except Exception:
                pass

            # Feed µ-law silence at real-time rate until camera audio
            # arrives.  A burst would cause audio PTS to jump ahead
            # of video PTS under +genpts, leading to A/V desync.
            self._audio_primed.clear()
            self._silence_feeder_thread = threading.Thread(
                target=self._silence_feeder, daemon=True,
            )
            self._silence_feeder_thread.start()

            self._ffmpeg_reader_thread = threading.Thread(
                target=self._ffmpeg_stdout_reader, daemon=True,
            )
            self._ffmpeg_reader_thread.start()
            proc_ref = self._ffmpeg_proc
            threading.Thread(
                target=self._video_pacer, args=(proc_ref,), daemon=True,
            ).start()
            if self._idle_video_keepalive:
                threading.Thread(
                    target=self._video_keepalive, args=(proc_ref,), daemon=True,
                ).start()
            threading.Thread(
                target=self._log_ffmpeg_stderr,
                args=(self._ffmpeg_proc, "muxer"),
                daemon=True,
            ).start()
            _LOGGER.debug("ffmpeg muxer started")

            # Feed initial keyframe so muxer starts producing output
            # immediately.  Keepalive will then sustain the idle stream.
            if self._latest_video_kf:
                self._feed_video(self._latest_video_kf)
            self._last_video_time = time.monotonic()

        except FileNotFoundError:
            _LOGGER.error("ffmpeg not found — streaming will not work")
            os.close(audio_r)
            self._close_audio_fd()
            self._ffmpeg_proc = None
        except Exception as e:
            _LOGGER.error("Failed to start ffmpeg muxer: %s", e)
            os.close(audio_r)
            self._close_audio_fd()
            self._ffmpeg_proc = None

    def _stop_ffmpeg_muxer(self) -> None:
        """Stop the ffmpeg muxer."""
        self._audio_primed.set()  # stop silence feeder thread
        if self._ffmpeg_proc:
            try:
                self._ffmpeg_proc.stdin.close()
            except OSError:
                pass
            self._close_audio_fd()
            try:
                self._ffmpeg_proc.terminate()
                self._ffmpeg_proc.wait(timeout=5)
            except Exception:
                try:
                    self._ffmpeg_proc.kill()
                    self._ffmpeg_proc.wait(timeout=2)
                except Exception:
                    pass
            self._ffmpeg_proc = None

        if self._audio_writer_thread:
            self._audio_writer_thread.join(timeout=0.8)
            if self._audio_writer_thread.is_alive():
                _LOGGER.debug("Audio writer thread still alive after stop timeout")
            self._audio_writer_thread = None

        while True:
            try:
                self._audio_queue.get_nowait()
            except queue.Empty:
                break

        if self._ffmpeg_reader_thread:
            self._ffmpeg_reader_thread.join(timeout=1.5)
            if self._ffmpeg_reader_thread.is_alive():
                _LOGGER.debug("ffmpeg reader thread still alive after stop timeout")
            self._ffmpeg_reader_thread = None

    def _close_audio_fd(self) -> None:
        if self._audio_write_fd >= 0:
            try:
                os.close(self._audio_write_fd)
            except OSError:
                pass
            self._audio_write_fd = -1

    def _ffmpeg_stdout_reader(self) -> None:
        """Read MPEG-TS from ffmpeg stdout and broadcast to TCP clients."""
        try:
            while self._ffmpeg_proc and self._ffmpeg_proc.poll() is None:
                # Smaller reads reduce burstiness seen by downstream clients.
                data = self._ffmpeg_proc.stdout.read1(8192)
                if not data:
                    break
                self._broadcast_stream(data)
        except Exception as e:
            _LOGGER.debug("ffmpeg reader stopped: %s", e)
        # Auto-restart muxer if coordinator is still running
        if self._running and (
            self._ffmpeg_proc is None or self._ffmpeg_proc.poll() is not None
        ):
            _LOGGER.warning("ffmpeg muxer exited unexpectedly, restarting")
            self._ffmpeg_proc = None
            time.sleep(2)
            self._start_ffmpeg_muxer()

    @staticmethod
    def _log_ffmpeg_stderr(proc: subprocess.Popen, label: str) -> None:
        """Log ffmpeg stderr lines so errors are visible in HA logs."""
        try:
            for raw_line in proc.stderr:
                line = raw_line.decode(errors="replace").rstrip()
                if line:
                    _LOGGER.debug("ffmpeg[%s]: %s", label, line)
        except Exception:
            pass

    def _feed_video(self, data: bytes) -> None:
        """Queue video frame data for paced writing to ffmpeg stdin."""
        try:
            self._video_queue.put_nowait(data)
        except queue.Full:
            # Keep latency bounded: discard one stale queued frame and
            # enqueue the newest camera frame.
            self._video_queue_drops += 1
            try:
                self._video_queue.get_nowait()
            except queue.Empty:
                return
            try:
                self._video_queue.put_nowait(data)
            except queue.Full:
                # Queue was immediately refilled by producer races.
                self._video_queue_drops += 1

    def _video_pacer(self, proc_ref: subprocess.Popen) -> None:
        """Write queued video frames to ffmpeg at steady realtime cadence.

        Cadence follows the live-estimated source FPS so cameras with
        different frame rates remain smooth without static assumptions.
        """
        last_write = 0.0
        while self._ffmpeg_proc is proc_ref and proc_ref.poll() is None:
            try:
                data = self._video_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            interval = 1.0 / max(1.0, float(self._video_mux_target_fps))
            qsize = self._video_queue.qsize()
            if qsize > (self._video_lag_drop_threshold * 2):
                # If backlog grows, drain slightly faster to avoid stale frames.
                interval = max(interval * 0.9, 1.0 / 60.0)

            now = time.monotonic()
            elapsed = now - last_write
            if elapsed < interval:
                time.sleep(interval - elapsed)

            if proc_ref.stdin is None:
                break
            try:
                proc_ref.stdin.write(data)
                last_write = time.monotonic()
                self._last_video_time = last_write
            except (BrokenPipeError, OSError, ValueError):
                break

    def _video_keepalive(self, proc_ref: subprocess.Popen) -> None:
        """Re-feed last keyframe to produce idle frames and fill gaps.

        When no camera data flows (idle or network hiccup), this
        injects the last keyframe at stream cadence so the MPEG-TS
        stream never goes silent. Frigate / go2rtc interpret sparse
        packet output as stalls, so keep idle output regular.

        Keep the most recent scene briefly after going idle, then switch
        to a cached idle keyframe derived from the same camera bitstream
        for decoder compatibility. Falls back to black only when no real
        scene keyframe is available yet.
        """
        while (
            self._ffmpeg_proc is proc_ref
            and proc_ref.poll() is None
        ):
            if self._camera_awake:
                # Keep stream continuity during brief live stalls so go2rtc/
                # Frigate don't report intermittent "no video" states.
                time.sleep(0.1)
                now_mono = time.monotonic()
                last_live = self._last_p2p_video_time
                if (
                    self._latest_video_kf
                    and last_live > 0.0
                    and (now_mono - last_live) >= self._live_gap_fill_after_seconds
                ):
                    live_interval = 1.0 / max(1.0, self._live_gap_fill_fps)
                    if now_mono - self._last_video_time >= live_interval:
                        self._feed_video(self._latest_video_kf)
                continue

            now_mono = time.monotonic()
            idle_elapsed = (now_mono - self._idle_since) if self._idle_since > 0.0 else 0.0
            fps = self._idle_keepalive_fps_initial
            if idle_elapsed >= self._idle_keepalive_settle_seconds:
                fps = self._idle_keepalive_fps_steady
            # Keep stream time advancing at realtime pace for active downstream
            # consumers (go2rtc/frigate), otherwise idle low-fps can look stalled.
            with self._stream_clients_lock:
                has_clients = bool(self._stream_clients)
            if has_clients:
                fps = max(fps, self._idle_keepalive_fps_with_clients)
            interval = 1.0 / max(1.0, fps)
            # Wake frequently and top up frames with adaptive idle cadence.
            time.sleep(interval / 2.0)
            now_mono = time.monotonic()
            source = self._latest_video_kf or self._idle_video_kf
            if (
                (self._idle_scene_kf or self._latest_video_kf or self._idle_video_kf)
                and self._idle_since > 0.0
                and (now_mono - self._idle_since) >= self._idle_scene_hold_seconds
            ):
                # Keep showing the real last scene if conversion is pending.
                # Fall back to black only when no scene keyframe exists.
                source = self._idle_scene_kf or self._latest_video_kf or self._idle_video_kf
            if (
                source
                and now_mono - self._last_video_time >= interval
            ):
                self._feed_video(source)

    def _silence_feeder(self) -> None:
        """Feed µ-law silence at real-time rate until camera audio arrives."""
        while (
            not self._audio_primed.is_set()
            and self._ffmpeg_proc is not None
            and self._ffmpeg_proc.poll() is None
        ):
            if self._camera_awake:
                chunk = 800    # 100 ms at 8 kHz mono µ-law
                interval = 0.1
            else:
                # Idle mode: one silence chunk per second is enough to keep
                # the audio track alive while minimizing encoder CPU load.
                chunk = 8000
                interval = 1.0
            self._queue_audio(b"\xff" * chunk)
            self._audio_primed.wait(timeout=interval)

    def _queue_audio(self, data: bytes) -> None:
        """Queue audio without ever blocking the P2P receive thread."""
        if self._audio_write_fd < 0 or not data:
            return
        is_silence = all(b == 0xFF for b in data)

        # During catch-up bursts, camera audio can arrive far faster than
        # realtime and flood the queue. Keep only a bounded lead so video
        # transport stays prioritized and audio avoids long stale lag.
        if self._camera_awake and not is_silence:
            now_mono = time.monotonic()
            if self._audio_realtime_next_ts <= 0.0:
                self._audio_realtime_next_ts = now_mono
            chunk_secs = max(0.005, len(data) / 8000.0)
            queued = self._audio_queue.qsize()
            if queued >= max(40, self._audio_soft_queue_limit // 2) and (
                now_mono + 0.60 < self._audio_realtime_next_ts
            ):
                self._audio_throttle_drops += 1
                self._audio_realtime_next_ts = (
                    max(now_mono, self._audio_realtime_next_ts) + chunk_secs
                )
                return
            self._audio_realtime_next_ts = (
                max(now_mono, self._audio_realtime_next_ts) + chunk_secs
            )

        # Prefer trimming stale backlog before hitting hard queue full drops.
        while self._audio_queue.qsize() > self._audio_soft_queue_limit:
            try:
                self._audio_queue.get_nowait()
                self._audio_throttle_drops += 1
            except queue.Empty:
                break

        if is_silence and self._audio_queue.qsize() >= max(24, self._audio_soft_queue_limit // 3):
            return

        while True:
            try:
                self._audio_queue.put_nowait(data)
                return
            except queue.Full:
                # Under pressure, discard pure silence chunks first so
                # real microphone content is less likely to be dropped.
                if is_silence:
                    return
                # Preserve low latency under pressure by dropping oldest audio.
                self._audio_queue_drops += 1
                try:
                    self._audio_queue.get_nowait()
                except queue.Empty:
                    return

    def _audio_writer(self, proc_ref: subprocess.Popen) -> None:
        """Drain queued audio to ffmpeg and correctly handle partial writes."""
        pending = memoryview(b"")
        while self._ffmpeg_proc is proc_ref and proc_ref.poll() is None:
            if not pending:
                try:
                    chunk = self._audio_queue.get(timeout=0.2)
                except queue.Empty:
                    continue
                if not chunk:
                    continue
                pending = memoryview(chunk)
            try:
                written = os.write(self._audio_write_fd, pending)
                if written <= 0:
                    raise OSError("audio pipe write returned no bytes")
                pending = pending[written:]
            except BlockingIOError:
                # Pipe is full; briefly back off and retry the same bytes.
                time.sleep(0.002)
            except (BrokenPipeError, OSError):
                break

    def _feed_audio(self, data: bytes) -> None:
        """Write G.711 µ-law audio data to ffmpeg audio input."""
        if self._audio_write_fd >= 0:
            self._audio_primed.set()  # stop silence feeder
            if not self._audio_real_started:
                # Drop queued bootstrap silence so first real audio is not delayed.
                self._audio_real_started = True
                self._audio_realtime_next_ts = time.monotonic()
                while True:
                    try:
                        self._audio_queue.get_nowait()
                    except queue.Empty:
                        break
            self._queue_audio(data)

    # ------------------------------------------------------------------
    # Black keyframe generator (bootstrap for keepalive)
    # ------------------------------------------------------------------

    def _generate_black_keyframe(self, codec: str | None = None) -> None:
        """Generate a black keyframe for initial keepalive.

        Called once at startup before the muxer starts so that the
        keepalive thread has a frame to inject even before the camera
        has sent any real video.
        """
        if not self._black_keyframe_lock.acquire(blocking=False):
            return
        try:
            size = self._video_size_hint
            if not size or size[0] <= 0 or size[1] <= 0:
                self._idle_video_kf = None
                _LOGGER.debug(
                    "Skipping synthetic black keyframe: source resolution unknown for %s",
                    self._sn_num,
                )
                return

            width, height = size
            fmt = (codec or self._video_codec or "hevc").lower()
            if fmt == "h264":
                video_args = [
                    "-c:v", "libx264",
                    "-preset", "ultrafast",
                    "-tune", "zerolatency",
                    "-pix_fmt", "yuv420p",
                    "-x264-params", "keyint=1:min-keyint=1:scenecut=0",
                    "-f", "h264", "pipe:1",
                ]
            else:
                fmt = "hevc"
                video_args = [
                    "-c:v", "libx265",
                    "-x265-params", "log-level=error",
                    "-f", "hevc", "pipe:1",
                ]

            try:
                result = subprocess.run(
                    [
                        "ffmpeg", "-hide_banner", "-loglevel", "error",
                        "-f", "lavfi",
                        "-i", f"color=c=black:s={width}x{height}:r=1:d=0.1",
                        "-frames:v", "1",
                        *video_args,
                    ],
                    capture_output=True,
                    timeout=10,
                )
                if result.returncode == 0 and result.stdout:
                    self._idle_video_kf = result.stdout
                    _LOGGER.debug(
                        "Generated black %s keyframe (%dx%d, %d bytes)",
                        fmt,
                        width,
                        height,
                        len(result.stdout),
                    )
                else:
                    _LOGGER.warning("Failed to generate black %s keyframe", fmt)
            except Exception as e:
                _LOGGER.warning("Failed to generate black %s keyframe: %s", fmt, e)
        finally:
            self._black_keyframe_lock.release()

    def _reset_video_cadence_state(self) -> None:
        self._video_fps_samples = 0
        self._video_fps_last_frame_time = 0.0
        self._video_fps_ema = max(5.0, min(60.0, float(self._video_mux_target_fps)))

    @staticmethod
    def _default_video_fps_for_codec(codec: str) -> float:
        codec_l = (codec or "hevc").lower()
        if codec_l == "h264":
            # H264 streams from these cameras are typically ~10 fps.
            # Using a higher startup FPS compresses PTS (setts) and can look
            # like 2x playback followed by pauses in downstream players.
            return 10.0
        return 15.0

    def _observe_video_cadence(self, frame_ts: float) -> None:
        prev = self._video_fps_last_frame_time
        self._video_fps_last_frame_time = frame_ts
        if prev <= 0.0:
            return

        dt = frame_ts - prev
        if dt <= 0.0 or dt > 0.5 or dt < 0.012:
            return

        inst_fps = max(5.0, min(60.0, 1.0 / dt))
        if self._video_fps_samples <= 0:
            self._video_fps_ema = inst_fps
        else:
            self._video_fps_ema = (self._video_fps_ema * 0.94) + (inst_fps * 0.06)
        self._video_fps_samples += 1

        if self._video_codec == "hevc":
            min_fps, max_fps = 13.5, 17.0
        else:
            # Keep H264 cadence centered around realtime source ingress.
            min_fps, max_fps = 8.5, 12.0

        target = max(min_fps, min(max_fps, self._video_fps_ema))
        alpha = 0.25 if self._video_fps_samples <= 15 else 0.08
        blended = (self._video_mux_target_fps * (1.0 - alpha)) + (target * alpha)
        self._video_mux_target_fps = max(min_fps, min(max_fps, blended))

    def _probe_keyframe_resolution(self, frame: bytes, codec: str) -> tuple[int, int] | None:
        fmt = "h264" if codec == "h264" else "hevc"
        try:
            proc = subprocess.run(
                [
                    "ffprobe", "-v", "error",
                    "-f", fmt,
                    "-show_entries", "stream=width,height",
                    "-of", "csv=p=0:s=x",
                    "pipe:0",
                ],
                input=frame,
                capture_output=True,
                timeout=3,
            )
        except Exception:
            return None

        if proc.returncode != 0 or not proc.stdout:
            return None

        out = proc.stdout.decode(errors="replace").strip().splitlines()
        if not out:
            return None

        m = re.search(r"(\d+)x(\d+)", out[0])
        if not m:
            return None

        w = int(m.group(1))
        h = int(m.group(2))
        if w <= 0 or h <= 0:
            return None
        return (w, h)

    def _refresh_video_resolution_hint(self, frame: bytes, codec: str) -> None:
        now_mono = time.monotonic()
        if now_mono < self._video_size_probe_after:
            return
        if self._video_size_hint and self._video_size_codec == codec:
            return

        with self._video_size_probe_lock:
            if self._video_size_probe_inflight:
                return
            self._video_size_probe_inflight = True

        frame_copy = bytes(frame)

        def _probe_worker() -> None:
            try:
                size = self._probe_keyframe_resolution(frame_copy, codec)
                now_probe = time.monotonic()
                if not size:
                    self._video_size_probe_after = now_probe + 10.0
                    return

                prev = self._video_size_hint
                self._video_size_hint = size
                self._video_size_codec = codec
                self._video_size_probe_after = now_probe + 30.0
                if prev != size:
                    _LOGGER.info(
                        "Detected %s stream geometry for %s: %dx%d",
                        codec.upper(),
                        self._sn_num,
                        size[0],
                        size[1],
                    )
                    self._generate_black_keyframe(codec)
                    if (
                        self._camera_awake
                        and self._idle_video_kf
                        and self._ffmpeg_proc is not None
                        and self._ffmpeg_proc.poll() is None
                    ):
                        # Prime mux output immediately after geometry discovery
                        # so local players do not wait for the next real IDR.
                        self._feed_video(self._idle_video_kf)
            finally:
                with self._video_size_probe_lock:
                    self._video_size_probe_inflight = False

        threading.Thread(target=_probe_worker, daemon=True).start()

    def _update_parameter_set_cache(self, data: bytes, codec: str) -> None:
        """Cache latest parameter-set NAL units for decoder recovery."""
        codec = (codec or "hevc").lower()
        if codec == "h264" and self._h264_sps_nal and self._h264_pps_nal:
            return
        if codec != "h264" and self._hevc_vps_nal and self._hevc_sps_nal and self._hevc_pps_nal:
            return

        for off, unit in self._iter_annexb_nal_units(data):
            b0 = data[off]
            if codec == "h264":
                nal_type = b0 & 0x1F
                if nal_type not in (7, 8):
                    continue
                if nal_type == 7:
                    self._h264_sps_nal = unit
                else:
                    self._h264_pps_nal = unit
                continue

            if off + 1 >= len(data):
                continue
            b1 = data[off + 1]
            if (b1 & 0x07) == 0:
                continue
            nal_type = (b0 >> 1) & 0x3F
            if nal_type == 32:
                self._hevc_vps_nal = unit
            elif nal_type == 33:
                self._hevc_sps_nal = unit
            elif nal_type == 34:
                self._hevc_pps_nal = unit

    def _prepend_parameter_sets_if_needed(
        self,
        data: bytes,
        codec: str,
        nal_types: set[int],
    ) -> bytes:
        """Prepend cached VPS/SPS/PPS when a keyframe arrives without them."""
        codec = (codec or "hevc").lower()
        if codec == "h264":
            prefix: list[bytes] = []
            if 7 not in nal_types and self._h264_sps_nal:
                prefix.append(self._h264_sps_nal)
            if 8 not in nal_types and self._h264_pps_nal:
                prefix.append(self._h264_pps_nal)
            if prefix:
                return b"".join(prefix) + data
            return data

        prefix = []
        if 32 not in nal_types and self._hevc_vps_nal:
            prefix.append(self._hevc_vps_nal)
        if 33 not in nal_types and self._hevc_sps_nal:
            prefix.append(self._hevc_sps_nal)
        if 34 not in nal_types and self._hevc_pps_nal:
            prefix.append(self._hevc_pps_nal)
        if prefix:
            return b"".join(prefix) + data
        return data

    def _build_idle_scene_keyframe(self) -> None:
        """Cache an idle keyframe from the latest real camera scene.

        Re-encoding idle frames with a different encoder profile can
        desync HEVC/H264 parameter sets across idle→live transitions.
        Keep the camera bitstream untouched for seamless decoder handoff.
        """
        if not self._idle_scene_convert_lock.acquire(blocking=False):
            return
        try:
            src = self._latest_video_kf
            if not src:
                return
            self._idle_scene_kf = bytes(src)
        except Exception as e:
            _LOGGER.debug("Idle scene keyframe update failed: %s", e)
        finally:
            self._idle_scene_convert_lock.release()

    # ------------------------------------------------------------------
    # Snapshot conversion (video keyframe → JPEG)
    # ------------------------------------------------------------------

    def _convert_latest_kf(self) -> None:
        """Convert the saved video keyframe to JPEG in a background thread."""
        if not self._snapshot_conversion_enabled:
            return
        if not self._snapshot_convert_lock.acquire(blocking=False):
            return
        try:
            data = self._latest_video_kf
            if not data:
                return
            self._last_snapshot_convert_time = time.monotonic()
            jpeg = self._video_to_jpeg(data)
            if jpeg:
                self._latest_image = jpeg
                self._fire_update()
        finally:
            self._snapshot_convert_lock.release()

    def _video_to_jpeg(self, video_data: bytes) -> bytes | None:
        """Convert raw H264/HEVC frame data to JPEG using ffmpeg."""
        video_fmt = "h264" if self._video_codec == "h264" else "hevc"
        try:
            proc = subprocess.run(
                [
                    "ffmpeg", "-hide_banner", "-loglevel", "error",
                    "-f", video_fmt,
                    "-probesize", "32768",
                    "-analyzeduration", "500000",
                    "-i", "pipe:0",
                    "-vframes", "1",
                    "-f", "image2pipe",
                    "-vcodec", "mjpeg",
                    "-q:v", "5",
                    "pipe:1",
                ],
                input=video_data,
                capture_output=True,
                timeout=10,
            )
            if proc.returncode == 0 and proc.stdout:
                return proc.stdout
        except (subprocess.TimeoutExpired, FileNotFoundError, Exception) as e:
            _LOGGER.debug("ffmpeg conversion failed: %s", e)
        return None

    # ------------------------------------------------------------------
    # Start / Stop
    # ------------------------------------------------------------------

    async def async_start(self) -> None:
        if self._running:
            return
        self._running = True
        self._start_stream_server()
        self._available = True
        # Generate a black keyframe and start the persistent muxer.
        # The muxer runs for the entire coordinator lifetime — keepalive
        # injects idle keyframes continuously with no timestamp
        # discontinuities for downstream readers.
        self._generate_black_keyframe()
        self._start_ffmpeg_muxer()
        self._thread = threading.Thread(
            target=self._watch_loop,
            name=f"cloudplus_{self._sn_num}",
            daemon=True,
        )
        self._thread.start()
        _LOGGER.info("Started CloudEdge / Meari coordinator for %s", self._sn_num)

    async def async_stop(self) -> None:
        self._running = False
        self._stop_mqtt()
        # Stop P2P streamer if active
        if self._p2p_streamer:
            self._p2p_streamer.request_stop()
        self._stop_ffmpeg_muxer()
        self._stop_stream_server()
        if self._stream_thread:
            self._stream_thread.join(timeout=4)
            if self._stream_thread.is_alive():
                _LOGGER.debug("Stream worker still alive after stop timeout")
            self._stream_thread = None
        if self._thread:
            self._thread.join(timeout=4)
            if self._thread.is_alive():
                _LOGGER.debug("Coordinator watch loop still alive after stop timeout")
            self._thread = None
        self._available = False
        _LOGGER.info("Stopped CloudEdge / Meari coordinator for %s", self._sn_num)

    # ------------------------------------------------------------------
    # MQTT
    # ------------------------------------------------------------------

    def _start_mqtt(self) -> None:
        """Connect to Meari MQTT broker for motion events."""
        if not self._api or not self._api.mqtt_host:
            return

        try:
            import paho.mqtt.client as mqtt
        except ImportError:
            _LOGGER.warning("paho-mqtt not installed — using HTTP polling only")
            return

        user_id = str(self._api.user_id)
        topic = f"$bsssvr/iot/{user_id}/{user_id}/event/update/accepted"

        def on_connect(client, userdata, flags, rc, *args):
            if rc == 0:
                self._mqtt_connected = True
                client.subscribe(topic, qos=2)
                _LOGGER.info("MQTT connected, subscribed to %s", topic)
            else:
                _LOGGER.warning("MQTT connect failed: rc=%d", rc)

        def on_disconnect(client, userdata, *args):
            self._mqtt_connected = False
            _LOGGER.debug("MQTT disconnected")

        def on_message(client, userdata, msg):
            try:
                self._handle_mqtt_message(msg.topic, msg.payload)
            except Exception as e:
                _LOGGER.debug("MQTT message error: %s", e)

        try:
            client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                client_id=user_id,
                clean_session=True,
                protocol=mqtt.MQTTv311,
            )
        except (AttributeError, TypeError):
            client = mqtt.Client(
                client_id=user_id,
                clean_session=True,
                protocol=mqtt.MQTTv311,
            )

        client.username_pw_set(self._api.access_id, self._api.mqtt_signature)

        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE
        client.tls_set_context(ssl_ctx)

        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        client.on_message = on_message
        client.reconnect_delay_set(min_delay=3, max_delay=60)

        self._mqtt_client = client

        try:
            client.connect_async(
                self._api.mqtt_host, self._api.mqtt_port, keepalive=300,
            )
            client.loop_start()
        except Exception as e:
            _LOGGER.warning("MQTT connection failed: %s", e)
            self._mqtt_connected = False

    def _stop_mqtt(self) -> None:
        if self._mqtt_client:
            try:
                self._mqtt_client.loop_stop()
                self._mqtt_client.disconnect()
            except Exception:
                pass
            self._mqtt_client = None
            self._mqtt_connected = False

    def _handle_mqtt_message(self, topic: str, payload: bytes) -> None:
        """Parse and dispatch an MQTT motion event."""
        parsed = parse_motion_event(payload)
        if not parsed:
            return

        device_id_str = parsed["device_id"]
        license_id = parsed["license_id"]

        if device_id_str:
            if device_id_str != str(self._device_id):
                return
        elif license_id:
            if self._sn_num and license_id != self._sn_num:
                return
        else:
            # Ignore unidentified alarm events to avoid cross-device false triggers.
            return

        evt_name = parsed["evt_name"]
        is_motion = parsed["is_motion"]

        _LOGGER.info(
            "MQTT event: %s (device=%s, license=%s, motion=%s)",
            evt_name, device_id_str or "?", license_id or "?", is_motion,
        )

        if is_motion:
            self._motion_detected = True
            self._motion_type = evt_name
            self._last_motion_time = time.time()
            self._fire_motion()
            self._fire_update()

    # ------------------------------------------------------------------
    # P2P streaming (runs in separate thread)
    # ------------------------------------------------------------------

    @staticmethod
    def _iter_nal_headers(data: bytes):
        """Yield Annex-B NAL header offsets from a bytestream."""
        i = 0
        n = len(data)
        while i < n - 3:
            if data[i] == 0 and data[i + 1] == 0:
                if data[i + 2] == 1:
                    start = i + 3
                elif i + 3 < n and data[i + 2] == 0 and data[i + 3] == 1:
                    start = i + 4
                else:
                    i += 1
                    continue
                if start < n:
                    yield start
                    i = start
                    continue
            i += 1

    @staticmethod
    def _iter_annexb_nal_units(data: bytes):
        """Yield (nal_header_offset, full_nal_unit_with_start_code)."""
        marks: list[tuple[int, int]] = []
        i = 0
        n = len(data)
        while i < n - 3:
            if data[i] == 0 and data[i + 1] == 0:
                if data[i + 2] == 1:
                    marks.append((i, i + 3))
                    i += 3
                    continue
                if i + 3 < n and data[i + 2] == 0 and data[i + 3] == 1:
                    marks.append((i, i + 4))
                    i += 4
                    continue
            i += 1

        for idx, (unit_start, nal_start) in enumerate(marks):
            next_start = marks[idx + 1][0] if (idx + 1) < len(marks) else n
            if nal_start >= next_start:
                continue
            yield nal_start, bytes(data[unit_start:next_start])

    def _detect_video_codec(self, data: bytes) -> str | None:
        """Detect camera payload codec from Annex-B NAL units."""
        h264_ps = 0
        hevc_ps = 0
        h264_score = 0
        hevc_score = 0

        for idx, off in enumerate(self._iter_nal_headers(data)):
            if idx >= 24:
                break
            b0 = data[off]
            h264_type = b0 & 0x1F
            if h264_type in (7, 8):
                h264_ps += 1
                h264_score += 3
            elif h264_type in (1, 5, 6, 9):
                h264_score += 1

            if off + 1 >= len(data):
                continue
            b1 = data[off + 1]
            hevc_type = (b0 >> 1) & 0x3F
            tid_plus1 = b1 & 0x07
            if tid_plus1 == 0:
                continue
            if hevc_type in (32, 33, 34):
                hevc_ps += 1
                hevc_score += 3
            elif hevc_type in (0, 1, 19, 20, 21, 39, 40):
                hevc_score += 1

        if h264_ps and not hevc_ps:
            return "h264"
        if hevc_ps and not h264_ps:
            return "hevc"
        if h264_score >= hevc_score + 2:
            return "h264"
        if hevc_score >= h264_score + 2:
            return "hevc"
        return None

    def _is_video_keyframe(self, data: bytes, codec: str) -> bool:
        """Return True if payload contains a keyframe for the given codec."""
        codec = (codec or "hevc").lower()
        for off in self._iter_nal_headers(data):
            b0 = data[off]
            if codec == "h264":
                nal_type = b0 & 0x1F
                if nal_type in (5, 7, 8):
                    return True
            else:
                nal_type = (b0 >> 1) & 0x3F
                if nal_type in (32, 33, 34, 19, 20):
                    return True
        return False

    def _collect_nal_types(self, data: bytes, codec: str) -> set[int]:
        """Return NAL unit types present in a bytestream payload."""
        codec = (codec or "hevc").lower()
        types: set[int] = set()
        for off in self._iter_nal_headers(data):
            b0 = data[off]
            if codec == "h264":
                types.add(b0 & 0x1F)
                continue
            if off + 1 >= len(data):
                continue
            b1 = data[off + 1]
            if (b1 & 0x07) == 0:
                continue
            types.add((b0 >> 1) & 0x3F)
        return types

    def _switch_video_codec(self, codec: str) -> None:
        """Switch muxer input codec when camera stream codec changes."""
        new_codec = (codec or "").lower()
        if new_codec not in {"hevc", "h264"}:
            return
        if new_codec == self._video_codec:
            return

        old_codec = self._video_codec
        self._video_codec = new_codec
        _LOGGER.info(
            "Detected %s stream for %s, switching muxer from %s to %s",
            new_codec.upper(),
            self._sn_num,
            old_codec,
            new_codec,
        )

        while not self._video_queue.empty():
            try:
                self._video_queue.get_nowait()
            except queue.Empty:
                break

        self._video_mux_target_fps = self._default_video_fps_for_codec(new_codec)
        self._video_size_codec = None
        self._reset_video_cadence_state()
        self._h264_sps_nal = None
        self._h264_pps_nal = None
        self._hevc_vps_nal = None
        self._hevc_sps_nal = None
        self._hevc_pps_nal = None
        self._idle_scene_kf = None
        self._generate_black_keyframe(new_codec)
        self._stop_ffmpeg_muxer()
        self._start_ffmpeg_muxer()

    def _begin_streaming(self, api: MeariApiClient, grab_only: bool = False) -> None:
        """Start P2P streaming in a background thread.

        If grab_only=True, stop after the first keyframe is captured
        (used for initial frame grab without a full live session).
        """
        if self._stream_thread and self._stream_thread.is_alive():
            return

        if not grab_only:
            self._live_stream_requested = True
            # New live session: reset audio priming state so stale idle silence
            # cannot leak into the next live period and drift A/V sync.
            self._audio_real_started = False
            self._audio_realtime_next_ts = 0.0
            while True:
                try:
                    self._audio_queue.get_nowait()
                except queue.Empty:
                    break

        grab_start: float | None = None  # set on first keyframe in grab mode
        GRAB_DURATION = 5.0  # seconds to stream before stopping in grab mode
        GRAB_HARD_TIMEOUT = 20.0  # max grab session length even without keyframes

        got_keyframe = False
        self._p2p_video_frames = 0
        self._p2p_audio_frames = 0
        self._p2p_audio_bytes = 0
        self._p2p_audio_non_ff_bytes = 0
        self._p2p_audio_all_ff_frames = 0
        self._last_p2p_audio_time = 0.0
        self._video_mux_target_fps = self._default_video_fps_for_codec(self._video_codec)
        self._reset_video_cadence_state()
        self._h264_sps_nal = None
        self._h264_pps_nal = None
        self._hevc_vps_nal = None
        self._hevc_sps_nal = None
        self._hevc_pps_nal = None
        have_h264_sps = False
        have_h264_pps = False
        have_hevc_vps = False
        have_hevc_sps = False
        have_hevc_pps = False
        bootstrap_injected = False

        def on_video(data: bytes):
            nonlocal grab_start
            nonlocal got_keyframe
            nonlocal have_h264_sps
            nonlocal have_h264_pps
            nonlocal have_hevc_vps
            nonlocal have_hevc_sps
            nonlocal have_hevc_pps
            nonlocal bootstrap_injected
            now_mono = time.monotonic()
            self._last_p2p_video_time = now_mono
            self._p2p_video_frames += 1
            self._observe_video_cadence(now_mono)

            detected_codec = self._detect_video_codec(data)
            if detected_codec and detected_codec != self._video_codec:
                self._switch_video_codec(detected_codec)
                got_keyframe = False
                have_h264_sps = False
                have_h264_pps = False
                have_hevc_vps = False
                have_hevc_sps = False
                have_hevc_pps = False
                bootstrap_injected = False

            codec = self._video_codec
            self._update_parameter_set_cache(data, codec)
            nal_types = self._collect_nal_types(data, codec)
            has_param_payload = False
            params_ready = False
            if codec == "h264":
                have_h264_sps = have_h264_sps or (self._h264_sps_nal is not None) or (7 in nal_types)
                have_h264_pps = have_h264_pps or (self._h264_pps_nal is not None) or (8 in nal_types)
                has_param_payload = (7 in nal_types) or (8 in nal_types)
                params_ready = have_h264_sps and have_h264_pps
            else:
                have_hevc_vps = have_hevc_vps or (self._hevc_vps_nal is not None) or (32 in nal_types)
                have_hevc_sps = have_hevc_sps or (self._hevc_sps_nal is not None) or (33 in nal_types)
                have_hevc_pps = have_hevc_pps or (self._hevc_pps_nal is not None) or (34 in nal_types)
                has_param_payload = (32 in nal_types) or (33 in nal_types) or (34 in nal_types)
                params_ready = have_hevc_vps and have_hevc_sps and have_hevc_pps

            is_kf = self._is_video_keyframe(data, codec)
            if is_kf or has_param_payload:
                self._refresh_video_resolution_hint(data, codec)

            # Wait until codec parameter sets are seen and then first keyframe.
            # H264/H265 streams may deliver SPS/PPS/VPS separately.
            if not got_keyframe:
                if not params_ready:
                    return
                if not bootstrap_injected and self._idle_video_kf:
                    self._feed_video(self._idle_video_kf)
                    bootstrap_injected = True
                if has_param_payload and not is_kf:
                    self._feed_video(data)
                    return
                if is_kf:
                    got_keyframe = True
                    _LOGGER.debug(
                        "First %s keyframe after parameter sets, feeding muxer",
                        codec.upper(),
                    )
                else:
                    return

            # Feed video to ffmpeg muxer. For keyframes, prepend cached
            # parameter sets if the camera did not include them in-band.
            feed_data = (
                self._prepend_parameter_sets_if_needed(data, codec, nal_types)
                if is_kf
                else data
            )
            self._feed_video(feed_data)

            if is_kf:
                # Save raw keyframe; convert to JPEG in a
                # separate thread so we never block the recv loop
                # (subprocess spawn takes 1-2 s per call).
                self._latest_video_kf = bytes(feed_data)
                self._latest_hevc_kf = self._latest_video_kf
                if self._snapshot_conversion_enabled:
                    now_mono = time.monotonic()
                    should_convert = grab_only or (
                        (now_mono - self._last_snapshot_convert_time) >= self._snapshot_convert_interval
                        and not self._snapshot_convert_lock.locked()
                    )
                    if should_convert:
                        threading.Thread(
                            target=self._convert_latest_kf,
                            daemon=True,
                        ).start()
                if grab_only:
                    if grab_start is None:
                        grab_start = time.time()
                        _LOGGER.debug("Grab: first keyframe, streaming for %.0fs", GRAB_DURATION)
                    elif time.time() - grab_start >= GRAB_DURATION:
                        _LOGGER.debug("Grab: %.0fs elapsed, stopping", GRAB_DURATION)
                        streamer.request_stop()

        def on_audio(data: bytes):
            self._last_p2p_audio_time = time.monotonic()
            self._p2p_audio_frames += 1
            self._p2p_audio_bytes += len(data)
            non_ff = sum(1 for b in data if b != 0xFF)
            self._p2p_audio_non_ff_bytes += non_ff
            if data and non_ff == 0:
                self._p2p_audio_all_ff_frames += 1
            self._feed_audio(data)

        def on_login():
            _LOGGER.info("VVP login OK for %s", self._sn_num)

        def on_disconnect():
            _LOGGER.info("P2P stream ended for %s", self._sn_num)

        streamer = P2PStreamer(
            api=api,
            device=self._device,
            on_video=on_video,
            on_audio=on_audio,
            on_login=on_login,
            on_disconnect=on_disconnect,
            allow_lossy_gap_skip=bool(self._p2p_allow_lossy_gap_skip),
            adaptive_lossy_gap_skip=bool(self._p2p_adaptive_lossy_gap_skip),
        )
        self._p2p_streamer = streamer
        self._stream_grab_only = grab_only

        if grab_only:
            grab_deadline = time.time() + GRAB_HARD_TIMEOUT

            def _grab_watcher() -> None:
                while self._running and self._p2p_streamer is streamer:
                    now = time.time()
                    if grab_start is not None and now - grab_start >= GRAB_DURATION:
                        _LOGGER.debug("Grab watcher: %.0fs elapsed, stopping", GRAB_DURATION)
                        streamer.request_stop()
                        return
                    if now >= grab_deadline:
                        _LOGGER.debug("Grab watcher: hard timeout %.0fs reached, stopping", GRAB_HARD_TIMEOUT)
                        streamer.request_stop()
                        return
                    time.sleep(0.5)

            threading.Thread(
                target=_grab_watcher,
                name=f"cloudplus_grab_watch_{self._sn_num}",
                daemon=True,
            ).start()

        def _stream_worker():
            restart_live = False
            try:
                v, b = streamer.run_session()
                _LOGGER.info(
                    "P2P session done for %s: %d video frames, %d bytes, %d audio frames, %d audio bytes (non_ff=%d, all_ff_frames=%d, audio_q_drops=%d, audio_throttle_drops=%d, video_q_drops=%d)",
                    self._sn_num,
                    v,
                    b,
                    self._p2p_audio_frames,
                    self._p2p_audio_bytes,
                    self._p2p_audio_non_ff_bytes,
                    self._p2p_audio_all_ff_frames,
                    self._audio_queue_drops,
                    self._audio_throttle_drops,
                    self._video_queue_drops,
                )
                if self._audio_queue_drops:
                    _LOGGER.warning(
                        "Audio queue dropped %d chunks for %s (writer backpressure)",
                        self._audio_queue_drops,
                        self._sn_num,
                    )
                if self._audio_throttle_drops:
                    _LOGGER.info(
                        "Audio burst shaper dropped %d stale chunks for %s",
                        self._audio_throttle_drops,
                        self._sn_num,
                    )
                if (
                    self._p2p_audio_frames >= 40
                    and self._p2p_audio_non_ff_bytes <= max(8, self._p2p_audio_bytes // 200)
                ):
                    _LOGGER.warning(
                        "Audio payload appears silent for %s (mostly 0xFF frames)",
                        self._sn_num,
                    )
            except Exception as e:
                _LOGGER.error("P2P stream error for %s: %s", self._sn_num, e)
            finally:
                self._p2p_streamer = None
                self._stream_grab_only = False
                if grab_only and not self._startup_ready.is_set():
                    # Startup waits for grab teardown before issuing manual wake.
                    self._startup_ready.set()

                # Keep live mode resilient: if a session drops while live mode
                # is still requested, restart immediately (main.py behavior).
                restart_live = bool(
                    self._running
                    and not grab_only
                    and self._live_stream_requested
                    and self._api is api
                )

                # Drain video queue so pacer thread exits quickly
                while not self._video_queue.empty():
                    try:
                        self._video_queue.get_nowait()
                    except queue.Empty:
                        break

                if restart_live:
                    _LOGGER.info("Live stream dropped, restarting for %s", self._sn_num)
                    self._stream_thread = None
                    # Keep awake state while reconnecting to avoid UI flicker.
                    self._camera_awake = True
                    self._idle_since = 0.0
                    self._idle_scene_kf = None
                    self._fire_update()
                    time.sleep(0.8)
                    self._begin_streaming(api, grab_only=False)
                else:
                    threading.Thread(
                        target=self._build_idle_scene_keyframe,
                        daemon=True,
                    ).start()
                    # Restart silence feeder so idle audio keeps flowing.
                    self._audio_primed.clear()
                    threading.Thread(
                        target=self._silence_feeder, daemon=True,
                    ).start()
                    self._camera_awake = False
                    self._idle_since = time.monotonic()
                    self._fire_update()

        self._stream_thread = threading.Thread(
            target=_stream_worker,
            name=f"cloudplus_p2p_{self._sn_num}",
            daemon=True,
        )
        self._stream_thread.start()
        if not grab_only:
            self._camera_awake = True
            self._idle_since = 0.0
            self._idle_scene_kf = None
        self._fire_update()

    def _end_streaming(self) -> None:
        """Stop the running P2P stream if any."""
        self._live_stream_requested = False
        if self._p2p_streamer:
            self._p2p_streamer.request_stop()
        # Thread will clean up via _stream_worker finally block

    # ------------------------------------------------------------------
    # Camera wake
    # ------------------------------------------------------------------

    def _do_wake(self, api: MeariApiClient) -> None:
        """Wake the camera via API."""
        if not self._is_snap:
            _LOGGER.debug(
                "Skipping wake for non-snap camera %s (category=%s)",
                self._sn_num,
                self._device_category,
            )
            return
        try:
            api.wake_device(self._sn_num, self._device_id)
            _LOGGER.info("Wake command sent for %s", self._sn_num)
        except Exception as e:
            _LOGGER.error("Wake failed: %s", e)

    # ------------------------------------------------------------------
    # Main watch loop
    # ------------------------------------------------------------------

    def _watch_loop(self) -> None:
        """Main event loop: login, listen for events, stream on motion."""
        _LOGGER.debug("Watch loop starting for %s", self._sn_num)

        while self._running:
            try:
                self._run_session()
            except Exception as e:
                _LOGGER.error("Session error: %s", e)

            if self._running:
                self._available = False
                self._fire_update()
                _LOGGER.info("Reconnecting in 30s...")
                for _ in range(30):
                    if not self._running:
                        return
                    time.sleep(1)

        self._available = False
        self._fire_update()

    def _run_session(self) -> None:
        """Single session: login → MQTT → grab initial frame → event loop."""
        self._startup_ready.clear()
        # Login
        api = MeariApiClient(
            email=self._email,
            password=self._password,
            country_code=self._country_code,
            phone_code=self._phone_code,
            app_profile=self._app_profile,
        )
        try:
            api.login()
        except PermissionError:
            _LOGGER.error("Login failed: invalid credentials")
            return
        except Exception as e:
            _LOGGER.error("Login failed: %s", e)
            return

        self._api = api
        self._available = True
        self._fire_update()

        # Start MQTT for motion events
        self._start_mqtt()

        # Initial battery poll (snap cameras only)
        if self._is_snap:
            self._poll_battery()

        # Optional startup frame grab (enabled by default for HA integration).
        # Dev/test tooling can disable this to reduce startup-to-live latency.
        if self._initial_frame_grab:
            if self._is_snap:
                _LOGGER.info("Waking camera for initial frame grab...")
                self._do_wake(api)
                time.sleep(3)
            else:
                _LOGGER.info(
                    "Starting initial frame grab without wake for %s (category=%s)",
                    self._sn_num,
                    self._device_category,
                )
            self._begin_streaming(api, grab_only=True)
            # Wait for the grab to finish, but do not block startup for too long.
            if self._stream_thread:
                self._stream_thread.join(timeout=self._initial_grab_timeout)
                if self._stream_thread.is_alive():
                    _LOGGER.warning(
                        "Initial frame grab timed out after %ss for %s, continuing startup",
                        self._initial_grab_timeout,
                        self._sn_num,
                    )
                    teardown_deadline = time.time() + 18
                    while self._stream_thread and self._stream_thread.is_alive() and time.time() < teardown_deadline:
                        if self._p2p_streamer:
                            self._p2p_streamer.request_stop()
                        self._stream_thread.join(timeout=1.5)
                    if self._stream_thread and self._stream_thread.is_alive():
                        _LOGGER.warning(
                            "Startup grab teardown still pending for %s; deferring wake until stream exits",
                            self._sn_num,
                        )
                if not self._stream_thread.is_alive():
                    self._stream_thread = None
            if self._latest_image:
                _LOGGER.info("Initial frame captured for %s", self._sn_num)
            else:
                _LOGGER.warning("No initial frame captured for %s", self._sn_num)
        else:
            _LOGGER.info("Initial frame grab disabled for %s", self._sn_num)

        if not (self._stream_thread and self._stream_thread.is_alive() and self._stream_grab_only):
            self._startup_ready.set()

        _LOGGER.info("Connected and listening for %s", self._sn_num)

        last_battery_poll = time.time()
        motion_deadline = 0.0  # Not streaming yet

        try:
            while self._running:
                now = time.time()

                # Manual wake check
                if self._wake_event.is_set():
                    self._wake_event.clear()
                    self._live_stream_requested = True
                    # Set state immediately so UI updates without delay
                    self._camera_awake = True
                    self._idle_since = 0.0
                    self._fire_update()
                    motion_deadline = now + self._motion_timeout

                    stream_alive = bool(self._stream_thread and self._stream_thread.is_alive())
                    stale_live = False
                    if stream_alive and not self._stream_grab_only:
                        # If a live session is still "alive" but no fresh
                        # video has arrived for several seconds, force a
                        # full rollover so wake can start a fresh session.
                        stale_live = (time.monotonic() - self._last_p2p_video_time) > 8.0

                    # Manual wake should prioritize a fresh live mode. If a
                    # grab-only or stale live session is still running,
                    # preempt it first.
                    if stream_alive and (self._stream_grab_only or stale_live):
                        if stale_live:
                            _LOGGER.warning(
                                "Manual wake: preempting stale stream for %s", self._sn_num,
                            )
                        if self._p2p_streamer:
                            self._p2p_streamer.request_stop()
                        if self._stream_thread:
                            self._stream_thread.join(timeout=4)
                        stream_alive = bool(self._stream_thread and self._stream_thread.is_alive())
                        if not stream_alive:
                            self._stream_thread = None

                    if not stream_alive:
                        if self._is_snap:
                            self._do_wake(api)
                        self._begin_streaming(api)

                # Motion-triggered wake
                if self._motion_detected and self._motion_wake_enabled:
                    self._live_stream_requested = True
                    motion_deadline = max(
                        motion_deadline,
                        self._last_motion_time + self._motion_timeout,
                    )
                    if not self._camera_awake:
                        # Set state immediately so UI updates without delay
                        self._camera_awake = True
                        self._idle_since = 0.0
                        self._fire_update()
                        if self._is_snap:
                            self._do_wake(api)
                        self._begin_streaming(api)

                # Streaming timeout — end session when deadline passes
                if self._camera_awake and now > motion_deadline:
                    _LOGGER.info("Stream timeout for %s, going idle", self._sn_num)
                    self._end_streaming()
                    self._motion_detected = False
                    self._motion_type = ""
                    self._fire_update()

                # Periodic battery poll
                if self._is_snap and now - last_battery_poll >= BATTERY_POLL_INTERVAL:
                    self._poll_battery()
                    last_battery_poll = now

                time.sleep(1)

        finally:
            self._end_streaming()
            if self._stream_thread:
                self._stream_thread.join(timeout=15)
                self._stream_thread = None
            self._stop_mqtt()
            self._api = None

    # ------------------------------------------------------------------
    # Polling helpers
    # ------------------------------------------------------------------

    def _poll_battery(self) -> None:
        """Poll battery info."""
        if not self._is_snap:
            return
        if not self._api:
            return
        try:
            info = self._api.get_battery_info(self._sn_num)
            if not info:
                return

            pct = info.get("154")
            charge = info.get("156")

            if pct is not None:
                try:
                    pct_int = int(pct)
                    if 0 <= pct_int <= 100:
                        changed = self._battery_percent != pct_int
                        self._battery_percent = pct_int
                        if changed:
                            self._fire_update()
                except (ValueError, TypeError):
                    pass

            if charge is not None:
                try:
                    charge_int = int(charge)
                    is_charging = charge_int == 1
                    changed = self._battery_charging != is_charging
                    self._battery_charging = is_charging
                    if changed:
                        self._fire_update()
                except (ValueError, TypeError):
                    pass
        except Exception as e:
            _LOGGER.debug("Battery poll failed: %s", e)
