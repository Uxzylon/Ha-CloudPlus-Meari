"""Coordinator for CloudPlus / Meari camera — manages lifecycle.

Runs MQTT listener, P2P streaming, ffmpeg muxer, TCP stream server,
and idle stream in background threads. Manages camera wake/sleep state
and motion detection.

Follows the same architecture as the home_v reference coordinator.
"""

from __future__ import annotations

import fcntl
import json
import logging
import os
import queue
import socket
import ssl
import subprocess
import threading
import time
from typing import Any, Callable

from homeassistant.core import HomeAssistant

from .const import (
    ALARM_TYPE_NAMES,
    DEFAULT_MOTION_TIMEOUT,
    DOMAIN,
    MOTION_ALARM_TYPES,
)
from .api import MeariApiClient, format_sn
from .p2p_streamer import P2PStreamer

_LOGGER = logging.getLogger(__name__)

# Polling intervals
STATUS_POLL_INTERVAL = 60.0
BATTERY_POLL_INTERVAL = 300.0


class CloudPlusCoordinator:
    """Manages connection to a single CloudPlus / Meari camera."""

    # Watch states
    _W_IDLE = 0
    _W_HANDSHAKING = 1
    _W_STREAMING = 2

    def __init__(
        self, hass: HomeAssistant, email: str, password: str, device: dict[str, Any]
    ) -> None:
        self.hass = hass
        self._email = email
        self._password = password
        self._device = device
        self._device_id = device["deviceID"]
        self._sn_num = device.get("snNum", "")
        self._device_name = device.get("deviceName", self._sn_num)
        self._host_key = device.get("hostKey", "")
        self._motion_timeout = DEFAULT_MOTION_TIMEOUT

        # Shared state
        self._latest_image: bytes | None = None
        self._latest_hevc_kf: bytes | None = None  # raw HEVC for deferred JPEG conversion
        self._last_video_time: float = 0.0  # monotonic timestamp of last video write
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
        self._stream_clients_lock = threading.Lock()
        self._stream_accept_thread: threading.Thread | None = None
        self._stream_epoch: float = 0.0  # wall-clock anchor for MPEG-TS timestamps

        # ffmpeg muxer (HEVC + G.711 µ-law → MPEG-TS)
        self._ffmpeg_proc: subprocess.Popen | None = None
        self._ffmpeg_reader_thread: threading.Thread | None = None
        self._audio_write_fd: int = -1
        self._audio_primed = threading.Event()  # set when real camera audio arrives

        # Video pacing queue — smooths network jitter for wallclock PTS
        self._video_queue: queue.Queue = queue.Queue(maxsize=60)

        # P2P streamer
        self._p2p_streamer: P2PStreamer | None = None
        self._stream_thread: threading.Thread | None = None

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

    def _accept_stream_clients(self) -> None:
        """Accept loop for TCP stream clients (runs in thread)."""
        while self._running and self._stream_server_sock:
            try:
                client, addr = self._stream_server_sock.accept()
                client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                client.settimeout(5)
                with self._stream_clients_lock:
                    self._stream_clients.append(client)
                _LOGGER.debug("Stream client connected from %s", addr)
            except socket.timeout:
                continue
            except OSError:
                break

    def _broadcast_stream(self, data: bytes) -> None:
        """Send MPEG-TS data to all connected stream clients."""
        with self._stream_clients_lock:
            dead: list[socket.socket] = []
            for client in self._stream_clients:
                try:
                    client.sendall(data)
                except (BrokenPipeError, ConnectionError, OSError):
                    dead.append(client)
            for c in dead:
                self._stream_clients.remove(c)
                try:
                    c.close()
                except OSError:
                    pass

    # ------------------------------------------------------------------
    # ffmpeg muxer (HEVC + G.711 µ-law → MPEG-TS)
    # ------------------------------------------------------------------

    def _start_ffmpeg_muxer(self) -> None:
        """Start ffmpeg to mux HEVC video + G.711 audio into MPEG-TS."""
        if self._ffmpeg_proc is not None:
            return

        audio_r, audio_w = os.pipe()
        self._audio_write_fd = audio_w

        cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", "warning",
            # Video input (HEVC NALs from stdin)
            "-use_wallclock_as_timestamps", "1",
            "-err_detect", "ignore_err",
            "-probesize", "500000",
            "-analyzeduration", "1000000",
            "-framerate", "15",
            "-thread_queue_size", "512",
            "-f", "hevc", "-i", "pipe:0",
            # Audio input (G.711 µ-law from pipe)
            "-use_wallclock_as_timestamps", "1",
            "-thread_queue_size", "1024",
            "-f", "mulaw", "-ar", "8000", "-ac", "1",
            "-i", f"pipe:{audio_r}",
            # Output: pass-through HEVC video, transcode audio
            "-map", "0:v", "-map", "1:a",
            "-c:v", "copy",
            "-c:a", "aac", "-b:a", "32k",
            "-max_delay", "0",
            "-flush_packets", "1",
            "-f", "mpegts",
            "-mpegts_flags", "resend_headers",
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
            threading.Thread(
                target=self._video_pacer, daemon=True,
            ).start()
            threading.Thread(
                target=self._video_keepalive, daemon=True,
            ).start()
            threading.Thread(
                target=self._log_ffmpeg_stderr,
                args=(self._ffmpeg_proc, "muxer"),
                daemon=True,
            ).start()
            _LOGGER.debug("ffmpeg muxer started")

            # Feed initial keyframe so muxer starts producing output
            # immediately.  Keepalive will then sustain the idle stream.
            if self._latest_hevc_kf:
                self._feed_video(self._latest_hevc_kf)
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

        if self._ffmpeg_reader_thread:
            self._ffmpeg_reader_thread.join(timeout=5)
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
                data = self._ffmpeg_proc.stdout.read1(32768)
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
            pass  # Drop frame rather than block recv loop

    def _video_pacer(self) -> None:
        """Write queued video frames to ffmpeg at a steady 1/15 s cadence.

        use_wallclock_as_timestamps stamps each frame when it hits
        ffmpeg's demuxer.  Network jitter causes frames to arrive in
        bursts, producing irregular PTS → fast/slow/pause playback.
        This thread smooths delivery so wallclock PTS are regular.
        """
        INTERVAL = 1.0 / 15
        last_write = 0.0
        while self._ffmpeg_proc and self._ffmpeg_proc.poll() is None:
            try:
                data = self._video_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            # Ensure minimum spacing between writes
            now = time.monotonic()
            elapsed = now - last_write
            if elapsed < INTERVAL:
                time.sleep(INTERVAL - elapsed)
            proc = self._ffmpeg_proc
            if proc is None or proc.stdin is None:
                break
            try:
                proc.stdin.write(data)
                last_write = time.monotonic()
                self._last_video_time = last_write
            except (BrokenPipeError, OSError, ValueError):
                break

    def _video_keepalive(self) -> None:
        """Re-feed last keyframe to produce idle frames and fill gaps.

        When no camera data flows (idle or network hiccup), this
        injects the last keyframe at ~2 fps so the MPEG-TS stream
        never goes silent.  Frigate / go2rtc interpret silence as
        signal loss, so continuous frames are essential.
        """
        while (
            self._ffmpeg_proc is not None
            and self._ffmpeg_proc.poll() is None
        ):
            time.sleep(0.5)
            if (
                self._latest_hevc_kf
                and time.monotonic() - self._last_video_time > 0.5
            ):
                self._feed_video(self._latest_hevc_kf)

    def _silence_feeder(self) -> None:
        """Feed µ-law silence at real-time rate until camera audio arrives."""
        CHUNK = 800   # 100 ms of 8 kHz mono µ-law
        INTERVAL = 0.1
        while (
            not self._audio_primed.is_set()
            and self._ffmpeg_proc is not None
            and self._ffmpeg_proc.poll() is None
        ):
            try:
                os.write(self._audio_write_fd, b"\xff" * CHUNK)
            except OSError:
                break
            self._audio_primed.wait(timeout=INTERVAL)

    def _feed_audio(self, data: bytes) -> None:
        """Write G.711 µ-law audio data to ffmpeg audio input."""
        if self._audio_write_fd >= 0:
            self._audio_primed.set()  # stop silence feeder
            try:
                os.write(self._audio_write_fd, data)
            except (BrokenPipeError, OSError):
                pass

    # ------------------------------------------------------------------
    # Black keyframe generator (bootstrap for keepalive)
    # ------------------------------------------------------------------

    def _generate_black_keyframe(self) -> None:
        """Generate a black HEVC keyframe for initial keepalive.

        Called once at startup before the muxer starts so that the
        keepalive thread has a frame to inject even before the camera
        has sent any real video.
        """
        try:
            result = subprocess.run(
                [
                    "ffmpeg", "-hide_banner", "-loglevel", "error",
                    "-f", "lavfi",
                    "-i", "color=c=black:s=1920x1080:r=1:d=0.1",
                    "-frames:v", "1",
                    "-c:v", "libx265",
                    "-x265-params", "log-level=error",
                    "-f", "hevc", "pipe:1",
                ],
                capture_output=True,
                timeout=10,
            )
            if result.returncode == 0 and result.stdout:
                self._latest_hevc_kf = result.stdout
                _LOGGER.debug(
                    "Generated black keyframe (%d bytes)", len(result.stdout),
                )
            else:
                _LOGGER.warning("Failed to generate black keyframe")
        except Exception as e:
            _LOGGER.warning("Failed to generate black keyframe: %s", e)

    # ------------------------------------------------------------------
    # Snapshot conversion (HEVC keyframe → JPEG)
    # ------------------------------------------------------------------

    def _convert_latest_kf(self) -> None:
        """Convert the saved HEVC keyframe to JPEG in a background thread."""
        data = self._latest_hevc_kf
        if not data:
            return
        jpeg = self._hevc_to_jpeg(data)
        if jpeg:
            self._latest_image = jpeg
            self._fire_update()

    def _hevc_to_jpeg(self, hevc_data: bytes) -> bytes | None:
        """Convert raw HEVC frame data to JPEG using ffmpeg."""
        try:
            proc = subprocess.run(
                [
                    "ffmpeg", "-hide_banner", "-loglevel", "error",
                    "-f", "hevc",
                    "-probesize", "32768",
                    "-analyzeduration", "500000",
                    "-i", "pipe:0",
                    "-vframes", "1",
                    "-f", "image2pipe",
                    "-vcodec", "mjpeg",
                    "-q:v", "5",
                    "pipe:1",
                ],
                input=hevc_data,
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
        # Generate a black HEVC keyframe and start the persistent muxer.
        # The muxer runs for the entire coordinator lifetime — keepalive
        # injects the keyframe at ~2 fps during idle, producing a
        # continuous MPEG-TS stream with no timestamp discontinuities.
        self._generate_black_keyframe()
        self._start_ffmpeg_muxer()
        self._thread = threading.Thread(
            target=self._watch_loop,
            name=f"cloudplus_{self._sn_num}",
            daemon=True,
        )
        self._thread.start()
        _LOGGER.info("Started CloudPlus coordinator for %s", self._sn_num)

    async def async_stop(self) -> None:
        self._running = False
        self._stop_mqtt()
        # Stop P2P streamer if active
        if self._p2p_streamer:
            self._p2p_streamer.request_stop()
        self._stop_ffmpeg_muxer()
        self._stop_stream_server()
        if self._stream_thread:
            self._stream_thread.join(timeout=15)
            self._stream_thread = None
        if self._thread:
            self._thread.join(timeout=15)
            self._thread = None
        self._available = False
        _LOGGER.info("Stopped CloudPlus coordinator for %s", self._sn_num)

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
        try:
            raw = json.loads(payload.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return

        data = raw
        if "params" in data:
            data = data["params"]
        if "data" in data:
            data = data["data"]
        if "msg" in data and isinstance(data["msg"], dict):
            data = data["msg"]

        evt_type = data.get("evt", data.get("eventType", ""))
        device_id_str = str(data.get("deviceID", data.get("deviceId", "")))

        try:
            evt_int = int(evt_type)
        except (ValueError, TypeError):
            evt_int = -1

        if device_id_str and device_id_str != str(self._device_id):
            return

        evt_name = ALARM_TYPE_NAMES.get(evt_int, f"type={evt_type}")
        is_motion = evt_int in MOTION_ALARM_TYPES

        _LOGGER.info(
            "MQTT event: %s (device=%s, motion=%s)",
            evt_name, device_id_str, is_motion,
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

    def _begin_streaming(self, api: MeariApiClient, grab_only: bool = False) -> None:
        """Start P2P streaming in a background thread.

        If grab_only=True, stop after the first keyframe is captured
        (used for initial frame grab without a full live session).
        """
        if self._stream_thread and self._stream_thread.is_alive():
            return

        grab_start: float | None = None  # set on first keyframe in grab mode
        GRAB_DURATION = 5.0  # seconds to stream before stopping in grab mode

        def _is_hevc_keyframe(data: bytes) -> bool:
            i = 0
            while i < len(data) - 5:
                if data[i:i + 4] == b"\x00\x00\x00\x01":
                    nal_type = (data[i + 4] >> 1) & 0x3F
                    if nal_type in (32, 33, 34, 19, 20):
                        return True
                    i += 5
                elif data[i:i + 3] == b"\x00\x00\x01":
                    nal_type = (data[i + 3] >> 1) & 0x3F
                    if nal_type in (32, 33, 34, 19, 20):
                        return True
                    i += 4
                else:
                    i += 1
            return False

        got_keyframe = False

        def on_video(data: bytes):
            nonlocal grab_start, got_keyframe
            is_kf = _is_hevc_keyframe(data)

            # Drop P-frames before first keyframe — the HEVC decoder
            # can't decode them (missing references) and they cause
            # "Could not find ref with POC" errors + dropped frames.
            if not got_keyframe:
                if is_kf:
                    got_keyframe = True
                    _LOGGER.debug("First HEVC keyframe, feeding muxer")
                else:
                    return  # Drop pre-keyframe P-frames

            # Feed video to ffmpeg muxer
            self._feed_video(data)

            if is_kf:
                # Save raw HEVC keyframe; convert to JPEG in a
                # separate thread so we never block the recv loop
                # (subprocess spawn takes 1-2 s per call).
                self._latest_hevc_kf = bytes(data)
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
        )
        self._p2p_streamer = streamer

        def _stream_worker():
            try:
                v, b = streamer.run_session()
                _LOGGER.info(
                    "P2P session done for %s: %d video frames, %d bytes",
                    self._sn_num, v, b,
                )
            except Exception as e:
                _LOGGER.error("P2P stream error for %s: %s", self._sn_num, e)
            finally:
                self._p2p_streamer = None
                # Drain video queue so pacer thread exits quickly
                while not self._video_queue.empty():
                    try:
                        self._video_queue.get_nowait()
                    except queue.Empty:
                        break
                # Restart silence feeder so idle audio keeps flowing.
                # The video keepalive thread automatically resumes
                # injecting the last keyframe, producing a frozen
                # frame.  The muxer stays running — no process
                # restart, no timestamp/CC discontinuity.
                self._audio_primed.clear()
                threading.Thread(
                    target=self._silence_feeder, daemon=True,
                ).start()
                self._camera_awake = False
                self._fire_update()

        self._stream_thread = threading.Thread(
            target=_stream_worker,
            name=f"cloudplus_p2p_{self._sn_num}",
            daemon=True,
        )
        self._stream_thread.start()
        if not grab_only:
            self._camera_awake = True
        self._fire_update()

    def _end_streaming(self) -> None:
        """Stop the running P2P stream if any."""
        if self._p2p_streamer:
            self._p2p_streamer.request_stop()
        # Thread will clean up via _stream_worker finally block

    # ------------------------------------------------------------------
    # Camera wake
    # ------------------------------------------------------------------

    def _do_wake(self, api: MeariApiClient) -> None:
        """Wake the camera via API."""
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
        # Login
        api = MeariApiClient(email=self._email, password=self._password)
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

        # Initial battery poll
        self._poll_battery()

        # Wake camera to grab one frame via P2P, then go back to idle
        _LOGGER.info("Waking camera for initial frame grab...")
        self._do_wake(api)
        time.sleep(3)
        self._begin_streaming(api, grab_only=True)
        # Wait for the grab to finish (connection + 5s stream + teardown)
        if self._stream_thread:
            self._stream_thread.join(timeout=45)
            self._stream_thread = None
        if self._latest_image:
            _LOGGER.info("Initial frame captured for %s", self._sn_num)
        else:
            _LOGGER.warning("No initial frame captured for %s", self._sn_num)

        _LOGGER.info("Connected and listening for %s", self._sn_num)

        last_battery_poll = time.time()
        motion_deadline = 0.0  # Not streaming yet

        try:
            while self._running:
                now = time.time()

                # Manual wake check
                if self._wake_event.is_set():
                    self._wake_event.clear()
                    # Set state immediately so UI updates without delay
                    self._camera_awake = True
                    self._fire_update()
                    motion_deadline = now + self._motion_timeout
                    if not (self._stream_thread and self._stream_thread.is_alive()):
                        self._do_wake(api)
                        time.sleep(3)
                        self._begin_streaming(api)

                # Motion-triggered wake
                if self._motion_detected and self._motion_wake_enabled:
                    motion_deadline = max(
                        motion_deadline,
                        self._last_motion_time + self._motion_timeout,
                    )
                    if not self._camera_awake:
                        # Set state immediately so UI updates without delay
                        self._camera_awake = True
                        self._fire_update()
                        self._do_wake(api)
                        time.sleep(3)
                        self._begin_streaming(api)

                # Streaming timeout — end session when deadline passes
                if self._camera_awake and now > motion_deadline:
                    _LOGGER.info("Stream timeout for %s, going idle", self._sn_num)
                    self._end_streaming()
                    self._motion_detected = False
                    self._motion_type = ""
                    self._fire_update()

                # Periodic battery poll
                if now - last_battery_poll >= BATTERY_POLL_INTERVAL:
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
