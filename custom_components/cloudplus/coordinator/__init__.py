"""Per-camera coordinator: worker, state machine, MPEG-TS mux and fan-out."""

from __future__ import annotations

import asyncio
import logging
import subprocess
import threading
import time
from typing import Any, Callable, TYPE_CHECKING

from homeassistant.core import HomeAssistant

from ..api import MeariApiClient
from ..const import (
    CONF_STREAM_QUALITY,
    DEFAULT_APP_PROFILE,
    DEFAULT_COUNTRY_CODE,
    DEFAULT_MOTION_TIMEOUT,
    DEFAULT_PHONE_CODE,
    DOMAIN,
)
from ..p2p_streamer import (
    ADAPTIVE_STREAM_ID,
    P2PStreamer,
    parse_quality_setting,
    stream_id_for_quality,
)
from ..p2p_streamer.session_support import DIRECT_VIDEO_STALL_RESTART_S
from ..p2p_streamer.codecs import (
    CodecName,
    CodecParameterCache,
    codec_live_pacing_buffer,
    demuxer_for,
    detect_codec,
    is_keyframe,
    runtime_policy_for,
    startup_backlog_frames,
    uses_arrival_timed_mux,
    uses_timestamp_timed_mux,
)
from .iot import parse_capabilities
from .motion import MotionEventListener
from .muxer import FfmpegMuxer
from .state import CoordinatorStateMixin
from .stream_server import StreamServer

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry

_LOGGER = logging.getLogger(__name__)

IDLE_ADVERTISED_FPS = 15.0
IDLE_REFRESH_INTERVAL = 0.5
IDLE_NO_CLIENT_SLEEP = 1.0
IDLE_FRAME_RETRY_INITIAL_S = 20.0
IDLE_FRAME_RETRY_MAX_S = 180.0
IDLE_GRAB_WAKE_RETRY_S = 8.0
IDLE_GRAB_STREAM_RETRY_S = 1.5
BATTERY_POLL_INTERVAL = 300.0
STATUS_POLL_INTERVAL = 300.0
LIVE_VIDEO_STALL_RESTART_S = 12.0
LIVE_STARTUP_STALL_RESTART_S = 25.0
LIVE_VIDEO_MIN_INTERVAL = 1.0 / 30.0
LIVE_ADAPTIVE_IPC_MUX_HOLD_S = 1.0
LIVE_ADAPTIVE_IPC_MUX_QUIET_S = 1.2
LIVE_P2P_VIDEO_GAP_S = 1.0
SNAPSHOT_CARD_REFRESH_INTERVAL = 3.0
SNAPSHOT_CARD_REQUEST_TTL = 30.0


class CloudEdgeMeariCoordinator(CoordinatorStateMixin):
    """Small runtime coordinator used by debug.py and camera entity."""

    def __init__(
        self,
        hass: HomeAssistant,
        email: str,
        password: str,
        device: dict[str, Any],
        video_password: str | None = None,
        country_code: str = DEFAULT_COUNTRY_CODE,
        phone_code: str = DEFAULT_PHONE_CODE,
        app_profile: str = DEFAULT_APP_PROFILE,
        initial_frame_grab: bool = True,
        initial_grab_timeout: int = 45,
        snapshot_conversion_enabled: bool = True,
        snapshot_min_interval: float = 10.0,
        entry: "ConfigEntry | None" = None,
    ) -> None:
        self.hass = hass
        self._entry = entry
        self._email = email
        self._password = password
        self._device = device
        self._configured_video_password = (video_password or "").strip()
        self._video_password = self._configured_video_password
        self._video_e2ee_enabled: bool | None = None
        self._country_code = country_code
        self._phone_code = phone_code
        self._app_profile = app_profile

        self._device_id = int(device.get("deviceID", 0) or 0)
        self._sn_num = str(device.get("snNum", ""))
        self._device_name = str(device.get("deviceName", self._sn_num) or self._sn_num)
        self._device_category = str(device.get("_category", "")).lower()
        self._capabilities = parse_capabilities(device)
        battery_capable_doorbell = (
            self._device_category in {"doorbell", "picturedoorbell", "voicebell"}
            and (self._as_int(self._capabilities.get("bat")) or 0) > 0
        )
        self._is_snap = self._device_category == "snap" or battery_capable_doorbell
        self._iot_data: dict[int | str, Any] = {}

        self._available = False
        self._camera_awake = False
        self._latest_image: bytes | None = None
        self._latest_image_source = ""
        self._latest_image_generation = 0
        self._latest_image_updated_at = 0.0
        self._latest_video_kf: bytes | None = None
        self._snapshot_conversion_enabled = bool(snapshot_conversion_enabled)
        self._snapshot_convert_interval = max(1.0, float(snapshot_min_interval))
        self._last_snapshot_convert_time = 0.0
        self._snapshot_requested_until = 0.0
        self._snapshot_convert_lock = threading.Lock()
        self._motion_type = ""
        self._motion_detected = False
        self._last_motion_time = 0.0
        self._motion_wake_enabled = True
        self._motion_timeout = DEFAULT_MOTION_TIMEOUT
        self._stream_host_mode = "ip"
        self._initial_frame_grab = bool(initial_frame_grab)
        self._initial_grab_timeout = max(30.0, float(initial_grab_timeout))

        self._battery_percent: int | None = None
        self._battery_charging = False
        self._has_lamp = False
        self._lamp_on = False
        self._has_ptz = self.supports_iot("ptz")

        self._running = False
        self._thread: threading.Thread | None = None
        self._stream_thread: threading.Thread | None = None
        self._idle_thread: threading.Thread | None = None
        self._api: MeariApiClient | None = None
        self._p2p_streamer: P2PStreamer | None = None
        self._last_p2p_diagnostics: dict[str, Any] = {}
        self._motion_listener: MotionEventListener | None = None
        self._motion_listener_key: tuple[str, ...] | None = None
        self._motion_listener_unsub: Callable[[], None] | None = None
        self._wake_event = threading.Event()
        self._idle_stop = threading.Event()
        self._idle_frame_ready = threading.Event()
        self._idle_frame_refresh = threading.Event()
        self._idle_stream_reset = threading.Event()
        self._stream_restart = threading.Event()

        self._stream_server = StreamServer(self._request_stream_bootstrap)
        self._muxer = FfmpegMuxer(self._stream_server.broadcast)

        self._video_codec = CodecName.HEVC
        self._video_mux_target_fps = 15.0
        self._p2p_session_generation = 0
        self._stream_started_at = 0.0
        self._wake_active_at = 0.0
        self._last_p2p_video_time = 0.0
        self._last_p2p_audio_time = 0.0
        self._last_video_time = 0.0
        self._last_mux_video_mono = 0.0
        self._last_mux_video_timestamp_ms: int | None = None
        self._last_p2p_video_gap_time = 0.0
        self._live_mux_ready = False
        self._live_first_video_time = 0.0
        self._keep_live_mux_on_restart = False
        self._p2p_video_frames = 0
        self._live_deadline = 0.0
        self._idle_video_frame: bytes | None = None
        self._idle_video_codec = self._video_codec
        self._idle_frame_lock = threading.Lock()

        self._stream_idr_seed = b""
        self._stream_idr_collecting = False
        self._stream_idr_seed_generation = 0
        self._startup_safe_min_seed_generation = 0

        quality_setting = entry.options.get(CONF_STREAM_QUALITY) if entry else None
        self._vvp_quality: int | None = parse_quality_setting(device, quality_setting)
        self._codec_params = CodecParameterCache()
        self._stream_started_keyframe = False

        self._update_callbacks: list[Callable[[], None]] = []
        self._motion_callbacks: list[Callable[[], None]] = []

    async def async_start(self) -> None:
        if self._running:
            return
        self._running = True
        self._stream_server.start()
        self._start_idle_loop()
        self._available = True
        self._thread = threading.Thread(target=self._watch_loop, daemon=True)
        self._thread.start()
        self._fire_update()

    async def async_stop(self) -> None:
        self._running = False
        await self._run_blocking(self._stop_blocking)
        self._available = False
        self._set_camera_awake(False)
        self._fire_update()

    async def _run_blocking(self, func: Callable[[], None]) -> None:
        add_job = getattr(self.hass, "async_add_executor_job", None)
        if callable(add_job):
            await add_job(func)
        else:
            await asyncio.to_thread(func)

    def _stop_blocking(self) -> None:
        self._stop_motion_listener()
        self._idle_stop.set()
        self._stop_streamer(join_timeout=4)
        if self._idle_thread is not None:
            self._idle_thread.join(timeout=4)
            self._idle_thread = None
        if self._stream_thread is not None:
            self._stream_thread.join(timeout=4)
            self._stream_thread = None
        if self._thread is not None:
            self._thread.join(timeout=4)
            self._thread = None
        self._muxer.stop()
        self._stream_server.stop()

    def _watch_loop(self) -> None:
        while self._running:
            try:
                api = MeariApiClient(
                    email=self._email,
                    password=self._password,
                    country_code=self._country_code,
                    phone_code=self._phone_code,
                    app_profile=self._app_profile,
                )
                api.login()
                self._api = api
                self._available = True
                self._fire_update()
                try:
                    self._start_motion_listener(api)
                    self._session_loop()
                finally:
                    self._stop_motion_listener()
                    self._stop_streamer(join_timeout=4)
                    self._api = None
            except (OSError, RuntimeError, ValueError, KeyError) as exc:
                _LOGGER.warning("Coordinator session loop failed: %s", exc)
            except Exception:  # pylint: disable=broad-exception-caught
                # This is the final boundary of a daemon thread. Letting an
                # unforeseen library/protocol exception escape would freeze
                # every entity at its last value until HA restarts.
                _LOGGER.exception("Unexpected coordinator session loop failure")
            self._available = False
            self._set_motion(False)
            self._set_camera_awake(False)
            self._fire_update()
            if self._running:
                time.sleep(2)

    def _session_loop(self) -> None:
        self._poll_status()
        if self._is_snap:
            self._snap_session_loop()
        else:
            self._ipc_session_loop()

    def _snap_session_loop(self) -> None:
        self._poll_battery()
        if self._initial_frame_grab and not self._idle_frame_ready.is_set():
            self._run_initial_frame_grab()

        last_battery_poll = time.monotonic()
        last_status_poll = time.monotonic()
        idle_retry_s = IDLE_FRAME_RETRY_INITIAL_S
        next_grab_retry = time.monotonic() + idle_retry_s
        while self._running:
            now = time.monotonic()
            if now - last_battery_poll >= BATTERY_POLL_INTERVAL:
                self._poll_battery()
                last_battery_poll = now
            if now - last_status_poll >= STATUS_POLL_INTERVAL:
                self._poll_status()
                last_status_poll = now

            if (
                self._initial_frame_grab
                and (
                    self._idle_frame_refresh.is_set()
                    or not self._idle_frame_ready.is_set()
                )
                and (self._idle_frame_refresh.is_set() or now >= next_grab_retry)
            ):
                self._idle_frame_refresh.clear()
                if self._run_initial_frame_grab():
                    self._stream_restart.clear()
                    idle_retry_s = IDLE_FRAME_RETRY_INITIAL_S
                else:
                    _LOGGER.warning(
                        "Idle frame unavailable for %s; retrying in %.0fs",
                        self._sn_num,
                        idle_retry_s,
                    )
                    idle_retry_s = min(
                        IDLE_FRAME_RETRY_MAX_S,
                        idle_retry_s * 1.5,
                    )
                next_grab_retry = time.monotonic() + idle_retry_s

            self._consume_wake_event()
            if self._motion_detected and self._motion_wake_enabled:
                self._live_deadline = max(
                    self._live_deadline,
                    self._last_motion_time + float(self._motion_timeout),
                )

            if now < self._live_deadline:
                self._run_live_until_deadline()
                continue

            self._set_camera_awake(False)
            time.sleep(0.2)

    def _ipc_session_loop(self) -> None:
        self._set_camera_awake(True)
        last_status_poll = time.monotonic()
        while self._running:
            now = time.monotonic()
            if self._stream_restart.is_set():
                self._stream_restart.clear()
                self._stop_streamer(join_timeout=2)
                self._muxer.stop()
                self._stream_server.reset_bootstrap()
            if now - last_status_poll >= STATUS_POLL_INTERVAL:
                self._poll_status()
                last_status_poll = now
            self._consume_wake_event()
            should_stream = (
                self._stream_server.client_count > 0
                or time.monotonic() < self._live_deadline
            )
            if not should_stream:
                self._stop_streamer(join_timeout=2)
                self._muxer.stop()
                time.sleep(1)
                continue

            if not self._stream_thread or not self._stream_thread.is_alive():
                self._stream_thread = None
                self._start_streamer_once()
            elif self._stream_video_stale(now):
                self._restart_stale_stream(now, context="ipc")
                continue

            worker = self._stream_thread
            if worker is None:
                time.sleep(1)
                continue
            worker.join(timeout=1)
            if not worker.is_alive() and self._stream_thread is worker:
                self._stream_thread = None

    def _run_initial_frame_grab(self) -> bool:
        self._set_camera_awake(True)
        self._idle_frame_ready.clear()
        deadline = time.monotonic() + self._initial_grab_timeout
        next_wake = 0.0
        next_stream = 0.0
        while self._running and time.monotonic() < deadline:
            now = time.monotonic()
            if now >= next_wake:
                self._send_wake()
                next_wake = now + IDLE_GRAB_WAKE_RETRY_S
            worker = self._stream_thread
            if (worker is None or not worker.is_alive()) and now >= next_stream:
                self._stream_thread = None
                self._start_streamer_once(grab_only=True)
                next_stream = now + IDLE_GRAB_STREAM_RETRY_S
            if self._idle_frame_ready.wait(timeout=0.2):
                break
        got_frame = self._idle_frame_ready.is_set()
        self._stop_streamer(join_timeout=4)
        self._set_camera_awake(False)
        return got_frame

    def _run_live_until_deadline(self) -> None:
        self._set_camera_awake(True)
        while self._running and time.monotonic() < self._live_deadline:
            now = time.monotonic()
            self._consume_wake_event()
            if self._stream_restart.is_set():
                self._stream_restart.clear()
                self._stop_streamer(join_timeout=2)
                self._muxer.stop()
                self._stream_server.reset_bootstrap()
                self._stream_started_keyframe = False
                self._reset_live_mux_timing()
                continue
            worker = self._stream_thread
            if worker is None or not worker.is_alive():
                keep_mux = worker is not None or self._keep_live_mux_on_restart
                self._stream_thread = None
                self._keep_live_mux_on_restart = False
                if not keep_mux:
                    self._muxer.stop()
                self._start_streamer_once()
            elif self._stream_video_stale(now):
                self._restart_stale_stream(now, context="live")
                time.sleep(0.5)
                continue

            worker = self._stream_thread
            if worker is None:
                time.sleep(0.5)
                continue
            worker.join(timeout=0.5)
            if not worker.is_alive() and self._stream_thread is worker:
                self._keep_live_mux_on_restart = True
                self._stream_thread = None
                time.sleep(0.5)

        if self._running and time.monotonic() < self._live_deadline:
            return
        self._request_streamer_stop()
        self._set_motion(False)
        self._prime_idle_stream()
        if time.monotonic() >= self._live_deadline:
            self._set_camera_awake(False)
        self._join_streamer(join_timeout=1)

    def _consume_wake_event(self) -> None:
        if not self._wake_event.is_set():
            return
        self._wake_event.clear()
        self._extend_live_deadline()
        self._send_wake()

    def _send_wake(self) -> None:
        if not self._is_snap or self._api is None:
            return
        try:
            self._api.wake_device(self._sn_num, self._device_id)
        except (OSError, RuntimeError, ValueError, KeyError) as exc:
            _LOGGER.debug("Wake failed for %s: %s", self._sn_num, exc)

    def _start_motion_listener(self, api: MeariApiClient) -> None:
        self._stop_motion_listener()
        key = (
            self._email,
            self._country_code,
            self._phone_code,
            self._app_profile,
        )
        listeners = self.hass.data.setdefault(DOMAIN, {}).setdefault(
            "_motion_listeners", {}
        )
        listener = listeners.get(key)
        if listener is None:
            listener = MotionEventListener(api)
            listeners[key] = listener
        self._motion_listener_unsub = listener.register(
            self._device_id, self._sn_num, self._note_motion
        )
        self._motion_listener = listener
        self._motion_listener_key = key
        self._motion_listener.start()

    def _stop_motion_listener(self) -> None:
        unsub = self._motion_listener_unsub
        listener = self._motion_listener
        key = self._motion_listener_key
        self._motion_listener_unsub = None
        self._motion_listener = None
        self._motion_listener_key = None
        if unsub is not None:
            unsub()
        if listener is not None and not listener.has_callbacks:
            listener.stop()
            listeners = self.hass.data.get(DOMAIN, {}).get("_motion_listeners", {})
            if key is not None and listeners.get(key) is listener:
                listeners.pop(key, None)

    def _start_idle_loop(self) -> None:
        if not self._is_snap:
            return
        if self._idle_thread is not None and self._idle_thread.is_alive():
            return
        self._idle_stop.clear()
        self._idle_thread = threading.Thread(
            target=self._idle_loop,
            name=f"cloudplus_idle_{self._sn_num}",
            daemon=True,
        )
        self._idle_thread.start()

    def _request_stream_bootstrap(self) -> bool:
        if not self._running:
            return False
        if self._is_snap and not self._camera_awake:
            return False
        streamer = self._p2p_streamer
        if streamer is None:
            return False
        streamer.request_keyframe()
        return True

    def get_latest_image(self) -> bytes | None:
        self._snapshot_requested_until = time.monotonic() + SNAPSHOT_CARD_REQUEST_TTL
        return self._latest_image

    def set_vvp_quality(self, quality: int | None) -> None:
        if quality == self._vvp_quality:
            return
        self._vvp_quality = quality
        self._codec_params.clear(self._video_codec)
        self._stream_started_keyframe = False
        self._reset_live_mux_timing()
        self._stream_restart.set()
        if self._is_snap:
            self._idle_frame_ready.clear()
            self._idle_frame_refresh.set()
            self._idle_stream_reset.set()
            with self._idle_frame_lock:
                self._idle_video_frame = None
                self._latest_video_kf = None
            self._latest_image = None
            self._latest_image_source = ""
            self._latest_image_generation += 1
            self._latest_image_updated_at = 0.0
        self._fire_update()

    def _idle_loop(self) -> None:
        next_frame = time.monotonic()
        pts_interval = IDLE_REFRESH_INTERVAL
        while self._running and not self._idle_stop.is_set():
            if self._idle_stream_reset.is_set():
                self._idle_stream_reset.clear()
                self._muxer.stop()
                self._stream_server.reset_bootstrap()
                next_frame = time.monotonic()

            if self._camera_awake:
                next_frame = time.monotonic()
                self._idle_stop.wait(0.1)
                continue

            with self._idle_frame_lock:
                frame = self._idle_video_frame
                codec = self._idle_video_codec
            if not frame:
                self._idle_stop.wait(0.2)
                continue

            has_clients = self._stream_server.client_count > 0
            bootstrap_ready = bool(
                self._stream_server.bootstrap_state().get("ready", False)
            )
            if not has_clients and bootstrap_ready:
                self._muxer.stop()
                next_frame = time.monotonic()
                self._idle_stop.wait(IDLE_NO_CLIENT_SLEEP)
                continue

            self._muxer.start(codec, advertised_fps=IDLE_ADVERTISED_FPS)
            self._muxer.write_video(frame, pts_interval_s=pts_interval)
            next_frame += IDLE_REFRESH_INTERVAL
            delay = max(0.0, next_frame - time.monotonic())
            if delay > IDLE_REFRESH_INTERVAL:
                next_frame = time.monotonic() + IDLE_REFRESH_INTERVAL
                delay = IDLE_REFRESH_INTERVAL
            self._idle_stop.wait(delay)

    def _prime_idle_stream(self) -> None:
        with self._idle_frame_lock:
            frame = self._idle_video_frame
            codec = self._idle_video_codec
        if not frame:
            return
        self._muxer.start(codec, advertised_fps=IDLE_ADVERTISED_FPS)
        self._muxer.replace_video(frame, pts_interval_s=IDLE_REFRESH_INTERVAL)

    def _stop_streamer(self, join_timeout: float = 4.0) -> None:
        self._request_streamer_stop()
        self._join_streamer(join_timeout)

    def _request_streamer_stop(self) -> None:
        streamer = self._p2p_streamer
        if streamer is not None:
            streamer.request_stop()

    def _join_streamer(self, join_timeout: float = 4.0) -> None:
        worker = self._stream_thread
        if worker is not None and worker is not threading.current_thread():
            worker.join(timeout=join_timeout)
            if not worker.is_alive() and self._stream_thread is worker:
                self._stream_thread = None

    def _stream_video_stale(self, now: float) -> bool:
        worker = self._stream_thread
        if worker is None or not worker.is_alive() or self._stream_started_at <= 0.0:
            return False
        streamer = self._p2p_streamer
        if streamer is not None and getattr(streamer, "awaiting_wake", False):
            # Dormant camera still being woken: the engine owns this window
            # (DORMANCY_WAKE_TIMEOUT_S). Don't restart underneath it, and only
            # start the first-video clock once the wake completes.
            self._wake_active_at = now
            return False
        if self._last_p2p_video_time >= self._stream_started_at:
            timeout = max(
                LIVE_VIDEO_STALL_RESTART_S,
                runtime_policy_for(self._video_codec).source_idle_reconnect_s,
            )
            if streamer is not None and getattr(streamer, "direct_confirmed", False):
                # A confirmed direct LAN peer is hard-won on battery cameras;
                # let the engine ride out brief silences (it stays patient for
                # DIRECT_SOURCE_IDLE_RECONNECT_S) rather than restarting it back
                # onto the lossy relay.
                timeout = max(timeout, DIRECT_VIDEO_STALL_RESTART_S)
            return now - self._last_p2p_video_time >= timeout
        baseline = max(self._stream_started_at, self._wake_active_at)
        return now - baseline >= LIVE_STARTUP_STALL_RESTART_S

    def _restart_stale_stream(self, now: float, *, context: str) -> None:
        if self._last_p2p_video_time >= self._stream_started_at:
            idle_s = now - self._last_p2p_video_time
            reason = f"no video for {idle_s:.1f}s"
        else:
            idle_s = now - self._stream_started_at
            reason = f"no first video after {idle_s:.1f}s"
        _LOGGER.warning(
            "Restarting stale %s P2P stream for %s (%s)",
            context,
            self._sn_num,
            reason,
        )
        self._keep_live_mux_on_restart = True
        self._stop_streamer(join_timeout=2)
        self._reset_live_mux_timing()

    def _reset_live_mux_timing(self) -> None:
        self._last_mux_video_mono = 0.0
        self._last_mux_video_timestamp_ms = None

    def _next_live_video_interval(
        self, now: float, timestamp_ms: int | None
    ) -> float | None:
        last = self._last_mux_video_mono
        self._last_mux_video_mono = now
        adaptive = self._stream_quality() is None

        if uses_arrival_timed_mux(self._video_codec, adaptive=adaptive):
            if last <= 0.0:
                return None
            return max(LIVE_VIDEO_MIN_INTERVAL, now - last)

        if (
            not uses_timestamp_timed_mux(self._video_codec, adaptive=adaptive)
            or timestamp_ms is None
        ):
            return None

        current_ts = int(timestamp_ms) & 0xFFFFFFFF
        previous_ts = self._last_mux_video_timestamp_ms
        self._last_mux_video_timestamp_ms = current_ts
        if previous_ts is None:
            return None
        delta_ms = (current_ts - previous_ts) & 0xFFFFFFFF
        if delta_ms == 0 or delta_ms > 0x7FFFFFFF:
            return None
        return max(0.001, delta_ms / 1000.0)

    def _stream_quality(self) -> int | None:
        return self._vvp_quality

    def _remember_idle_frame(self, codec: str, payload: bytes) -> None:
        frame = bytes(payload)
        with self._idle_frame_lock:
            self._idle_video_codec = codec
            self._idle_video_frame = frame
            self._latest_video_kf = frame
        self._idle_frame_ready.set()
        self._maybe_convert_snapshot(codec, frame)

    def _maybe_convert_snapshot(self, codec: str, payload: bytes) -> None:
        if not self._snapshot_conversion_enabled or not payload:
            return
        if self._motion_detected and self._latest_image_source == "event":
            return
        now = time.monotonic()
        interval = self._snapshot_convert_interval
        if now <= self._snapshot_requested_until:
            interval = min(interval, SNAPSHOT_CARD_REFRESH_INTERVAL)
        if (
            self._latest_image is not None
            and now - self._last_snapshot_convert_time < interval
        ):
            return
        if not self._snapshot_convert_lock.acquire(  # pylint: disable=consider-using-with
            blocking=False
        ):
            return
        self._last_snapshot_convert_time = now
        threading.Thread(
            target=self._convert_snapshot,
            args=(codec, bytes(payload)),
            name=f"cloudplus_snapshot_{self._sn_num}",
            daemon=True,
        ).start()

    def _convert_snapshot(self, codec: str, payload: bytes) -> None:
        try:
            jpeg = self._video_to_jpeg(codec, payload)
            # An event may arrive while ffmpeg is converting an older frame.
            if jpeg and not (
                self._motion_detected and self._latest_image_source == "event"
            ):
                self._latest_image = jpeg
                self._latest_image_source = "live"
                self._latest_image_generation += 1
                self._latest_image_updated_at = time.time()
                self._fire_update()
        finally:
            self._snapshot_convert_lock.release()

    @staticmethod
    def _video_to_jpeg(codec: CodecName | str, payload: bytes) -> bytes | None:
        video_format = demuxer_for(codec)
        try:
            proc = subprocess.run(
                [
                    "ffmpeg",
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-f",
                    video_format,
                    "-probesize",
                    "32768",
                    "-analyzeduration",
                    "500000",
                    "-i",
                    "pipe:0",
                    "-vframes",
                    "1",
                    "-f",
                    "image2pipe",
                    "-vcodec",
                    "mjpeg",
                    "-q:v",
                    "5",
                    "pipe:1",
                ],
                input=payload,
                capture_output=True,
                timeout=10,
                check=False,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            _LOGGER.debug("Snapshot conversion failed: %s", exc)
            return None
        return proc.stdout if proc.returncode == 0 and proc.stdout else None

    def _start_streamer_once(self, *, grab_only: bool = False) -> None:
        if self._stream_thread is not None and self._stream_thread.is_alive():
            return
        if self._api is None:
            return

        self._p2p_session_generation += 1
        self._stream_started_at = time.monotonic()
        self._stream_started_keyframe = False
        self._reset_live_mux_timing()
        self._last_p2p_video_gap_time = self._stream_started_at
        self._live_mux_ready = False
        self._live_first_video_time = 0.0

        def stream_allowed() -> bool:
            return (
                grab_only or not self._is_snap or time.monotonic() < self._live_deadline
            )

        def on_video(payload: bytes, timestamp_ms: int | None = None) -> None:
            if not stream_allowed():
                return
            now = time.monotonic()
            previous_video_time = self._last_p2p_video_time
            self._last_p2p_video_time = now
            self._last_video_time = now
            self._p2p_video_frames += 1
            if self._live_first_video_time <= 0.0:
                self._live_first_video_time = now
                self._last_p2p_video_gap_time = now
            if (
                previous_video_time >= self._stream_started_at
                and now - previous_video_time >= LIVE_P2P_VIDEO_GAP_S
            ):
                self._last_p2p_video_gap_time = now
                if not self._live_mux_ready:
                    self._muxer.reset_live_timing()
                    self._reset_live_mux_timing()
            detected = detect_codec(payload, default=self._video_codec)
            if detected != self._video_codec:
                self._video_codec = detected
                self._codec_params.clear(detected)
                self._muxer.stop()
                self._stream_started_keyframe = False
                self._reset_live_mux_timing()

            self._codec_params.update(self._video_codec, payload)
            if not self._codec_params.ready(self._video_codec):
                return
            frame_is_keyframe = is_keyframe(self._video_codec, payload)
            if not self._stream_started_keyframe:
                if not frame_is_keyframe:
                    return
                self._stream_started_keyframe = True

            payload = self._codec_params.with_params(self._video_codec, payload)
            if frame_is_keyframe:
                self._remember_idle_frame(self._video_codec, payload)
            if self._hold_startup_mux(now):
                return
            self._muxer.start(
                self._video_codec,
                advertised_fps=self._video_mux_target_fps,
                pacing_buffer_s=self._live_pacing_buffer_s(),
            )
            self._muxer.write_video(
                payload,
                pts_interval_s=self._next_live_video_interval(now, timestamp_ms),
            )
            if grab_only and frame_is_keyframe and self._p2p_streamer is not None:
                self._p2p_streamer.request_stop()

        def on_audio(payload: bytes) -> None:
            if not stream_allowed():
                return
            self._last_p2p_audio_time = time.monotonic()
            self._muxer.write_audio(payload)

        def _worker() -> None:
            streamer = P2PStreamer(
                api=self._api,
                device=self._device,
                on_video=on_video,
                on_audio=on_audio,
                on_login=lambda: None,
                on_disconnect=lambda: None,
                vvp_quality=self._stream_quality(),
                video_password=self._video_password,
                client_id=self.get_iot_value(1),
            )
            self._p2p_streamer = streamer
            try:
                streamer.run_session()
            finally:
                self._last_p2p_diagnostics = streamer.diagnostics
                self._p2p_streamer = None

        self._stream_thread = threading.Thread(target=_worker, daemon=True)
        self._stream_thread.start()

    def _hold_startup_mux(self, now: float) -> bool:
        if self._live_mux_ready:
            return False
        if not self._stream_uses_adaptive_id():
            self._live_mux_ready = True
            return False
        first_video_time = self._live_first_video_time or self._stream_started_at
        if now - first_video_time < LIVE_ADAPTIVE_IPC_MUX_HOLD_S:
            return True
        if now - self._last_p2p_video_gap_time < LIVE_ADAPTIVE_IPC_MUX_QUIET_S:
            return True
        self._live_mux_ready = True
        return False

    def _stream_uses_adaptive_id(self) -> bool:
        return (
            stream_id_for_quality(self._device, self._stream_quality())
            == ADAPTIVE_STREAM_ID
        )

    def _live_pacing_buffer_s(self) -> float:
        return codec_live_pacing_buffer(
            self._video_codec,
            self._device,
            sorted(self.quality_profiles),
            adaptive=self._stream_uses_adaptive_id(),
        )

    def _count_recent_gap_events(self, *, severity: str, within_s: float) -> int:
        _ = severity
        _ = within_s
        return 0

    def _is_valid_idr_seed(self, seed: bytes) -> bool:
        return bool(seed)

    def _probe_bootstrap_seed_decode(
        self, seed: bytes, *, max_frames: int = 6
    ) -> tuple[bool, str]:
        _ = max_frames
        if not seed:
            seed = self._stream_server.bootstrap_snapshot()
        strong = bool(seed) and bool(
            self._stream_server.bootstrap_state().get("strong", False)
        )
        return (strong, "idr-with-params" if strong else "seed-not-strong")

    def get_gap_skip_events_snapshot(self) -> list[dict[str, Any]]:
        return []

    def get_stream_join_diagnostics_snapshot(self) -> list[dict[str, Any]]:
        return []

    def get_startup_bootstrap_state(self) -> dict[str, Any]:
        now = time.monotonic()
        video_age_s = (
            (now - self._last_p2p_video_time)
            if self._last_p2p_video_time > 0
            else 999.0
        )
        bootstrap = self._stream_server.bootstrap_state()
        seed_generation = int(bootstrap.get("generation", 0) or 0)
        if seed_generation != self._stream_idr_seed_generation:
            self._stream_idr_seed = self._stream_server.bootstrap_snapshot()
            self._stream_idr_seed_generation = seed_generation
        seed_valid = bool(bootstrap.get("ready", False))
        seed_strong = seed_valid and bool(bootstrap.get("strong", False))
        policy = runtime_policy_for(self._video_codec)
        backlog_frame_target = startup_backlog_frames(self._video_codec)
        frames_since_seed = int(bootstrap.get("frames_since_seed", 0) or 0)
        follow_frame_target = min(
            backlog_frame_target,
            max(4, int(float(self._video_mux_target_fps or 15.0) * 1.2)),
        )
        required_generation = max(
            int(policy.startup_seed_min_generations),
            int(self._startup_safe_min_seed_generation),
            1,
        )
        backlog_ready = (
            seed_strong
            and seed_generation >= required_generation
            and frames_since_seed >= follow_frame_target
        )
        startup_safe = video_age_s < 1.0 and backlog_ready
        return {
            "startup_safe": startup_safe,
            "block_reason": (
                "ready"
                if startup_safe
                else (
                    "seed-awaiting-idr"
                    if video_age_s < 1.0 and not seed_valid
                    else "backlog-warming" if video_age_s < 1.0 else "video-not-recent"
                )
            ),
            "seed_valid": seed_valid,
            "seed_strong": seed_strong,
            "seed_video_bytes": int(bootstrap.get("bytes", 0) or 0),
            "seed_strength_reason": (
                "idr-with-params"
                if seed_strong
                else "seed-awaiting-params" if seed_valid else "seed-empty"
            ),
            "seed_mono": 0.0,
            "seed_generation": seed_generation,
            "required_seed_generation": required_generation,
            "seed_age_s": 0.0,
            "frames_since_seed": frames_since_seed,
            "collecting": bool(bootstrap.get("collecting", False)),
            "video_age_s": float(video_age_s),
            "backlog_follow_video_pusi_target": follow_frame_target,
            "backlog_ready": bool(backlog_ready),
            "backlog_generation_safe": True,
            "backlog_candidate_bytes": int(bootstrap.get("bytes", 0) or 0),
            "preferred_join_mode": "ready-backlog" if backlog_ready else "pending",
            "adaptive_stream": self._stream_uses_adaptive_id(),
            "latest_severe_gap_event": None,
        }
