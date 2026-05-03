from __future__ import annotations

import logging
import socket
import subprocess
import threading
import time
from typing import Any, Callable, TYPE_CHECKING

from homeassistant.core import HomeAssistant

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry

from ..api import MeariApiClient
from ..const import (
    DEFAULT_APP_PROFILE,
    DEFAULT_COUNTRY_CODE,
    DEFAULT_MOTION_TIMEOUT,
    DEFAULT_PHONE_CODE,
    IOT_CODE_BATTERY_PERCENT,
    IOT_CODE_CHARGE_STATUS,
    IOT_CODE_LAMP,
    IOT_CODE_VIDEO_ENCRYPTION,
    PTZ_DIRECTIONS,
)
from ..p2p_streamer import P2PStreamer, parse_quality_profiles
from ..p2p_streamer.codecs import detect_codec
from .iot import iot_value, normalize_iot_values, parse_capabilities, supports_feature
from .motion import MotionEventListener
from .muxer import FfmpegMuxer
from .stream_server import StreamServer

_LOGGER = logging.getLogger(__name__)

IDLE_ADVERTISED_FPS = 15.0
IDLE_REFRESH_INTERVAL = 1.0
IDLE_NO_CLIENT_SLEEP = 1.0
BATTERY_POLL_INTERVAL = 300.0
STATUS_POLL_INTERVAL = 300.0


class CloudEdgeMeariCoordinator:
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
        self._is_snap = self._device_category == "snap"
        self._capabilities = parse_capabilities(device)
        self._iot_data: dict[int | str, Any] = {}

        self._available = False
        self._camera_awake = False
        self._latest_image: bytes | None = None
        self._latest_video_kf: bytes | None = None
        self._snapshot_conversion_enabled = bool(snapshot_conversion_enabled)
        self._snapshot_convert_interval = max(1.0, float(snapshot_min_interval))
        self._last_snapshot_convert_time = 0.0
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
        self._motion_listener: MotionEventListener | None = None
        self._wake_event = threading.Event()
        self._idle_stop = threading.Event()
        self._idle_frame_ready = threading.Event()

        self._stream_server = StreamServer()
        self._muxer = FfmpegMuxer(self._stream_server.broadcast)

        self._video_codec = "hevc"
        self._video_mux_target_fps = 15.0
        self._p2p_session_generation = 0
        self._last_p2p_video_time = 0.0
        self._last_p2p_audio_time = 0.0
        self._last_video_time = 0.0
        self._p2p_video_frames = 0
        self._live_deadline = 0.0
        self._idle_video_frame: bytes | None = None
        self._idle_video_codec = self._video_codec
        self._idle_frame_lock = threading.Lock()

        self._stream_idr_seed = b""
        self._stream_idr_collecting = False
        self._stream_idr_seed_generation = 0
        self._startup_safe_min_seed_generation = 0

        self._vvp_quality: int | None = None
        self._h264_sps: bytes | None = None
        self._h264_pps: bytes | None = None
        self._hevc_vps: bytes | None = None
        self._hevc_sps: bytes | None = None
        self._hevc_pps: bytes | None = None
        self._stream_started_keyframe = False

        self._update_callbacks: list[Callable[[], None]] = []
        self._motion_callbacks: list[Callable[[], None]] = []

    @property
    def available(self) -> bool:
        return self._available

    @property
    def latest_image(self) -> bytes | None:
        return self._latest_image

    @property
    def motion_type(self) -> str:
        return self._motion_type

    @property
    def motion_detected(self) -> bool:
        return self._motion_detected

    @property
    def device_uuid(self) -> str:
        return self._sn_num

    @property
    def device_name(self) -> str:
        return self._device_name

    @property
    def device_model(self) -> str:
        return f"Camera ({self._device_category or 'unknown'})"

    @property
    def device_id(self) -> int:
        return self._device_id

    @property
    def is_battery_camera(self) -> bool:
        return self._is_snap

    @property
    def camera_awake(self) -> bool:
        return self._camera_awake

    @property
    def battery_percent(self) -> int | None:
        return self._battery_percent

    @property
    def battery_charging(self) -> bool:
        return self._battery_charging

    @property
    def motion_wake_enabled(self) -> bool:
        return self._motion_wake_enabled

    @property
    def motion_timeout(self) -> int:
        return self._motion_timeout

    @property
    def has_lamp(self) -> bool:
        return self._has_lamp

    @property
    def lamp_on(self) -> bool:
        return self._lamp_on

    @property
    def has_ptz(self) -> bool:
        return self._has_ptz

    def supports_iot(self, feature: str | None) -> bool:
        return supports_feature(self._capabilities, self._device, feature)

    def has_iot_code(self, code: int | str) -> bool:
        return self.get_iot_value(code) is not None

    def get_iot_value(self, code: int | str) -> Any:
        return iot_value(self._iot_data, code)

    @property
    def stream_port(self) -> int:
        return self._stream_server.port

    @property
    def stream_host_mode(self) -> str:
        return self._stream_host_mode

    @property
    def stream_host(self) -> str:
        if self._stream_host_mode == "docker":
            return socket.gethostname()
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except OSError:
            return "127.0.0.1"

    @property
    def quality_profiles(self) -> dict[int, str]:
        return parse_quality_profiles(self._device)

    @property
    def vvp_quality(self) -> int | None:
        return self._vvp_quality

    def set_vvp_quality(self, quality: int | None) -> None:
        self._vvp_quality = quality
        self._fire_update()

    def set_stream_host_mode(self, mode: str) -> None:
        if mode not in {"ip", "docker"}:
            return
        self._stream_host_mode = mode
        self._fire_update()

    def set_motion_wake_enabled(self, enabled: bool) -> None:
        self._motion_wake_enabled = bool(enabled)
        if self._motion_wake_enabled and self._motion_detected:
            self._wake_event.set()
            self._extend_live_deadline()
            self._set_camera_awake(True)
        self._fire_update()

    def set_motion_timeout(self, seconds: int) -> None:
        self._motion_timeout = max(10, min(600, int(seconds)))
        if self._camera_awake:
            self._extend_live_deadline()
        self._fire_update()

    def prefetch_battery(self, api: MeariApiClient) -> None:
        self._api = api
        if not self._is_snap:
            return
        try:
            info = api.get_battery_info(self._sn_num)
            if not info and api.openapi_server:
                info = api.get_device_iot_config(self._sn_num)
            changed = self._apply_iot_values(info)
            changed = self._apply_battery_info(info) or changed
            changed = self._apply_video_encryption_info(info) or changed
            if changed:
                self._fire_update()
        except Exception as exc:
            _LOGGER.warning("Battery prefetch failed for %s: %s", self._sn_num, exc)

    def prefetch_status(self, api: MeariApiClient) -> None:
        self.prefetch_lamp(api)

    def prefetch_lamp(self, api: MeariApiClient) -> None:
        self._api = api
        if not api.openapi_server:
            return
        try:
            iot = api.get_device_iot_config(self._sn_num)
        except Exception as exc:
            _LOGGER.debug("Lamp/status prefetch failed for %s: %s", self._sn_num, exc)
            return
        changed = self._apply_iot_values(iot)
        changed = self._apply_battery_info(iot) or changed
        changed = self._apply_video_encryption_info(iot) or changed
        lamp = self._as_int(self.get_iot_value(IOT_CODE_LAMP))
        if lamp is not None:
            changed = changed or not self._has_lamp or self._lamp_on != (lamp == 1)
            self._has_lamp = True
            self._lamp_on = lamp == 1
        if changed:
            self._fire_update()

    def set_lamp(self, enabled: bool) -> bool:
        return self.set_iot_value(IOT_CODE_LAMP, 1 if enabled else 0)

    def set_iot_value(self, code: int | str, value: Any) -> bool:
        api = self._api
        if api is None:
            return False
        ok = api.set_device_iot_value(self._sn_num, str(code), value)
        if ok:
            self._iot_data.update(normalize_iot_values({code: value}))
            if str(code) == IOT_CODE_LAMP:
                self._has_lamp = True
                self._lamp_on = self._as_int(value) == 1
            self._fire_update()
        return ok

    def ptz_move(self, direction: str) -> bool:
        api = self._api
        if api is None or direction not in PTZ_DIRECTIONS:
            return False
        return api.ptz_start(self._sn_num, direction)

    def ptz_stop(self) -> bool:
        api = self._api
        return bool(api and api.ptz_stop(self._sn_num))

    def wake_camera(self) -> None:
        self._wake_event.set()
        self._extend_live_deadline()
        self._set_camera_awake(True)

    def register_motion_callback(self, cb: Callable[[], None]) -> Callable[[], None]:
        self._motion_callbacks.append(cb)
        return lambda: self._remove_callback(self._motion_callbacks, cb)

    def register_update_callback(self, cb: Callable[[], None]) -> Callable[[], None]:
        self._update_callbacks.append(cb)
        return lambda: self._remove_callback(self._update_callbacks, cb)

    @staticmethod
    def _remove_callback(
        callbacks: list[Callable[[], None]],
        cb: Callable[[], None],
    ) -> None:
        try:
            callbacks.remove(cb)
        except ValueError:
            pass

    def _fire_update(self) -> None:
        if self.hass.loop.is_closed():
            return
        for cb in list(self._update_callbacks):
            self.hass.loop.call_soon_threadsafe(cb)

    def _fire_motion(self) -> None:
        if self.hass.loop.is_closed():
            return
        for cb in list(self._motion_callbacks):
            self.hass.loop.call_soon_threadsafe(cb)

    def _set_camera_awake(self, awake: bool) -> None:
        awake = bool(awake)
        if self._camera_awake == awake:
            return
        self._camera_awake = awake
        self._fire_update()

    def _set_motion(self, detected: bool, motion_type: str = "") -> None:
        detected = bool(detected)
        motion_type = motion_type if detected else ""
        changed = self._motion_detected != detected or self._motion_type != motion_type
        self._motion_detected = detected
        self._motion_type = motion_type
        if changed:
            self._fire_motion()
            self._fire_update()

    def _extend_live_deadline(self, seconds: float | None = None) -> None:
        duration = float(seconds if seconds is not None else self._motion_timeout)
        self._live_deadline = max(self._live_deadline, time.monotonic() + duration)

    def _note_motion(self, motion_type: str) -> None:
        self._last_motion_time = time.monotonic()
        self._set_motion(True, motion_type)
        if self._is_snap and self._motion_wake_enabled:
            self._wake_event.set()
            self._extend_live_deadline()
            self._set_camera_awake(True)

    @staticmethod
    def _as_int(value: Any) -> int | None:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _iot_value(self, info: dict[str, Any], code: str) -> Any:
        return iot_value(normalize_iot_values(info), code)

    def _apply_iot_values(self, info: dict[str, Any]) -> bool:
        values = normalize_iot_values(info)
        if not values:
            return False
        updated = dict(self._iot_data)
        updated.update(values)
        if updated == self._iot_data:
            return False
        self._iot_data = updated
        return True

    def _apply_battery_info(self, info: dict[str, Any]) -> bool:
        if not info:
            return False

        changed = False
        percent = self._as_int(self._iot_value(info, IOT_CODE_BATTERY_PERCENT))
        charging = self._as_int(self._iot_value(info, IOT_CODE_CHARGE_STATUS))

        if percent is not None and 0 <= percent <= 100:
            changed = changed or self._battery_percent != percent
            self._battery_percent = percent
        if charging is not None:
            is_charging = charging == 1
            changed = changed or self._battery_charging != is_charging
            self._battery_charging = is_charging
        return changed

    def _apply_video_encryption_info(self, info: dict[str, Any]) -> bool:
        state = self._as_int(self._iot_value(info, IOT_CODE_VIDEO_ENCRYPTION))
        if state is None:
            return False

        enabled = state == 1
        changed = self._video_e2ee_enabled != enabled
        self._video_e2ee_enabled = enabled

        effective_password = self._configured_video_password if enabled else ""
        if self._video_password != effective_password:
            changed = True
            self._video_password = effective_password
            if self._configured_video_password and not enabled:
                _LOGGER.debug(
                    "Ignoring stored video password for %s because E2EE is disabled",
                    self._sn_num,
                )
        return changed

    def _poll_battery(self) -> None:
        api = self._api
        if not self._is_snap or api is None:
            return
        for attempt in range(2):
            try:
                info = api.get_battery_info(self._sn_num)
                if not info and api.openapi_server:
                    info = api.get_device_iot_config(self._sn_num)
                changed = self._apply_iot_values(info)
                changed = self._apply_battery_info(info) or changed
                changed = self._apply_video_encryption_info(info) or changed
                if changed:
                    self._fire_update()
                return
            except Exception as exc:
                if attempt == 0:
                    _LOGGER.debug("Battery poll retry for %s: %s", self._sn_num, exc)
                    try:
                        api.login()
                    except Exception:
                        pass
                    continue
                _LOGGER.debug("Battery poll failed for %s: %s", self._sn_num, exc)

    def _poll_status(self) -> None:
        api = self._api
        if api is None or not api.openapi_server:
            return
        try:
            iot = api.get_device_iot_config(self._sn_num)
        except Exception as exc:
            _LOGGER.debug("Status poll failed for %s: %s", self._sn_num, exc)
            return

        changed = self._apply_iot_values(iot)
        changed = self._apply_battery_info(iot) or changed
        changed = self._apply_video_encryption_info(iot) or changed
        lamp = self._as_int(self.get_iot_value(IOT_CODE_LAMP))
        if lamp is not None:
            changed = changed or not self._has_lamp or self._lamp_on != (lamp == 1)
            self._has_lamp = True
            self._lamp_on = lamp == 1
        if changed:
            self._fire_update()

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
        self._available = False
        self._set_camera_awake(False)
        self._fire_update()

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
                self._start_motion_listener(api)
                try:
                    self._session_loop()
                finally:
                    self._stop_motion_listener()
                    self._stop_streamer(join_timeout=4)
                    self._api = None
            except Exception as exc:
                _LOGGER.warning("Coordinator session loop failed: %s", exc)
                self._available = False
                self._set_camera_awake(False)
                self._fire_update()
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
            self._send_wake()
            self._run_initial_frame_grab()

        last_battery_poll = time.monotonic()
        last_status_poll = time.monotonic()
        next_grab_retry = time.monotonic() + 30.0
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
                and not self._idle_frame_ready.is_set()
                and now >= next_grab_retry
            ):
                self._send_wake()
                self._run_initial_frame_grab()
                next_grab_retry = time.monotonic() + 60.0

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

            worker = self._stream_thread
            if worker is None:
                time.sleep(1)
                continue
            worker.join(timeout=1)
            if not worker.is_alive() and self._stream_thread is worker:
                self._stream_thread = None

    def _run_initial_frame_grab(self) -> None:
        self._set_camera_awake(True)
        self._idle_frame_ready.clear()
        self._start_streamer_once(grab_only=True)
        deadline = time.monotonic() + self._initial_grab_timeout
        while self._running and time.monotonic() < deadline:
            if self._idle_frame_ready.wait(timeout=0.2):
                break
        self._stop_streamer(join_timeout=4)
        self._set_camera_awake(False)

    def _run_live_until_deadline(self) -> None:
        self._set_camera_awake(True)
        while self._running and time.monotonic() < self._live_deadline:
            self._consume_wake_event()
            if not self._stream_thread or not self._stream_thread.is_alive():
                self._stream_thread = None
                self._muxer.stop()
                self._start_streamer_once()

            worker = self._stream_thread
            if worker is None:
                time.sleep(0.5)
                continue
            worker.join(timeout=0.5)
            if not worker.is_alive() and self._stream_thread is worker:
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
        except Exception as exc:
            _LOGGER.debug("Wake failed for %s: %s", self._sn_num, exc)

    def _start_motion_listener(self, api: MeariApiClient) -> None:
        self._stop_motion_listener()
        self._motion_listener = MotionEventListener(
            api, self._device_id, self._sn_num, self._note_motion
        )
        self._motion_listener.start()

    def _stop_motion_listener(self) -> None:
        listener = self._motion_listener
        self._motion_listener = None
        if listener is not None:
            listener.stop()

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

    def _idle_loop(self) -> None:
        next_frame = time.monotonic()
        pts_interval = IDLE_REFRESH_INTERVAL
        while self._running and not self._idle_stop.is_set():
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
        now = time.monotonic()
        if (
            self._latest_image is not None
            and now - self._last_snapshot_convert_time < self._snapshot_convert_interval
        ):
            return
        if not self._snapshot_convert_lock.acquire(blocking=False):
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
            if jpeg:
                self._latest_image = jpeg
                self._fire_update()
        finally:
            self._snapshot_convert_lock.release()

    @staticmethod
    def _video_to_jpeg(codec: str, payload: bytes) -> bytes | None:
        video_format = "h264" if codec == "h264" else "hevc"
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
            )
        except Exception as exc:
            _LOGGER.debug("Snapshot conversion failed: %s", exc)
            return None
        return proc.stdout if proc.returncode == 0 and proc.stdout else None

    def _start_streamer_once(self, *, grab_only: bool = False) -> None:
        if self._stream_thread is not None and self._stream_thread.is_alive():
            return
        if self._api is None:
            return

        self._p2p_session_generation += 1
        self._stream_started_keyframe = False

        def stream_allowed() -> bool:
            return (
                grab_only or not self._is_snap or time.monotonic() < self._live_deadline
            )

        def on_video(payload: bytes) -> None:
            if not stream_allowed():
                return
            self._last_p2p_video_time = time.monotonic()
            self._last_video_time = self._last_p2p_video_time
            self._p2p_video_frames += 1
            detected = detect_codec(payload, default=self._video_codec)
            if detected != self._video_codec:
                self._video_codec = detected
                self._muxer.stop()
                self._stream_started_keyframe = False

            self._remember_codec_params(self._video_codec, payload)
            if not self._codec_params_ready(self._video_codec):
                return
            is_keyframe = self._is_keyframe(self._video_codec, payload)
            if not self._stream_started_keyframe:
                if not is_keyframe:
                    return
                self._stream_started_keyframe = True

            payload = self._with_codec_params(self._video_codec, payload)
            if is_keyframe:
                self._remember_idle_frame(self._video_codec, payload)
            self._muxer.start(
                self._video_codec,
                advertised_fps=self._video_mux_target_fps,
            )
            self._muxer.write_video(payload)
            if grab_only and is_keyframe and self._p2p_streamer is not None:
                self._p2p_streamer.request_stop()

        def on_audio(payload: bytes) -> None:
            if not stream_allowed():
                return
            self._last_p2p_audio_time = time.monotonic()
            self._muxer.write_audio(payload)

        def _worker() -> None:
            self._p2p_streamer = P2PStreamer(
                api=self._api,
                device=self._device,
                on_video=on_video,
                on_audio=on_audio,
                on_login=lambda: None,
                on_disconnect=lambda: None,
                vvp_quality=self._vvp_quality,
                video_password=self._video_password,
            )
            self._p2p_streamer.run_session()
            self._p2p_streamer = None

        self._stream_thread = threading.Thread(target=_worker, daemon=True)
        self._stream_thread.start()

    def _count_recent_gap_events(self, *, severity: str, within_s: float) -> int:
        _ = severity
        _ = within_s
        return 0

    def _is_valid_idr_seed(self, seed: bytes) -> bool:
        return bool(seed)

    @staticmethod
    def _annexb_units(payload: bytes) -> list[bytes]:
        starts: list[tuple[int, int]] = []
        i = 0
        while i + 3 < len(payload):
            if payload[i] == 0 and payload[i + 1] == 0:
                if payload[i + 2] == 1:
                    starts.append((i, 3))
                    i += 3
                    continue
                if i + 3 < len(payload) and payload[i + 2] == 0 and payload[i + 3] == 1:
                    starts.append((i, 4))
                    i += 4
                    continue
            i += 1
        units: list[bytes] = []
        for idx, (start, _sc_len) in enumerate(starts):
            end = starts[idx + 1][0] if idx + 1 < len(starts) else len(payload)
            if end > start:
                units.append(payload[start:end])
        return units

    def _remember_codec_params(self, codec: str, payload: bytes) -> None:
        for unit in self._annexb_units(payload):
            nal = unit[3:] if unit.startswith(b"\x00\x00\x01") else unit[4:]
            if not nal:
                continue
            if codec == "h264":
                nal_type = nal[0] & 0x1F
                if nal_type == 7:
                    self._h264_sps = unit
                elif nal_type == 8:
                    self._h264_pps = unit
            else:
                if len(nal) < 2:
                    continue
                nal_type = (nal[0] >> 1) & 0x3F
                if nal_type == 32:
                    self._hevc_vps = unit
                elif nal_type == 33:
                    self._hevc_sps = unit
                elif nal_type == 34:
                    self._hevc_pps = unit

    def _codec_params_ready(self, codec: str) -> bool:
        if codec == "h264":
            return self._h264_sps is not None and self._h264_pps is not None
        return (
            self._hevc_vps is not None
            and self._hevc_sps is not None
            and self._hevc_pps is not None
        )

    def _with_codec_params(self, codec: str, payload: bytes) -> bytes:
        units = self._annexb_units(payload)
        if not units:
            return payload
        nal_types: set[int] = set()
        has_idr = False
        for unit in units:
            nal = unit[3:] if unit.startswith(b"\x00\x00\x01") else unit[4:]
            if not nal:
                continue
            if codec == "h264":
                nal_type = nal[0] & 0x1F
                nal_types.add(nal_type)
                has_idr = has_idr or nal_type == 5
            elif len(nal) >= 2:
                nal_type = (nal[0] >> 1) & 0x3F
                nal_types.add(nal_type)
                has_idr = has_idr or nal_type in {19, 20}
        if not has_idr:
            return payload
        prefix: list[bytes] = []
        if codec == "h264":
            if 7 not in nal_types and self._h264_sps:
                prefix.append(self._h264_sps)
            if 8 not in nal_types and self._h264_pps:
                prefix.append(self._h264_pps)
        else:
            if 32 not in nal_types and self._hevc_vps:
                prefix.append(self._hevc_vps)
            if 33 not in nal_types and self._hevc_sps:
                prefix.append(self._hevc_sps)
            if 34 not in nal_types and self._hevc_pps:
                prefix.append(self._hevc_pps)
        return b"".join(prefix) + payload if prefix else payload

    def _is_keyframe(self, codec: str, payload: bytes) -> bool:
        for unit in self._annexb_units(payload):
            nal = unit[3:] if unit.startswith(b"\x00\x00\x01") else unit[4:]
            if not nal:
                continue
            if codec == "h264":
                if (nal[0] & 0x1F) == 5:
                    return True
            elif len(nal) >= 2:
                if ((nal[0] >> 1) & 0x3F) in {19, 20}:
                    return True
        return False

    def _probe_bootstrap_seed_decode(
        self, seed: bytes, *, max_frames: int = 6
    ) -> tuple[bool, str]:
        _ = max_frames
        if not seed:
            seed = self._stream_server.bootstrap_snapshot()
        return (bool(seed), "validated-idr" if seed else "seed-empty")

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
        backlog_frame_target = 60 if self._video_codec == "hevc" else 45
        backlog_ready = seed_valid and self._p2p_video_frames > backlog_frame_target
        startup_safe = video_age_s < 1.0 and backlog_ready
        required_generation = max(1, int(self._startup_safe_min_seed_generation))
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
            "seed_strong": seed_valid,
            "seed_video_bytes": int(bootstrap.get("bytes", 0) or 0),
            "seed_strength_reason": "validated-idr" if seed_valid else "seed-empty",
            "seed_mono": 0.0,
            "seed_generation": seed_generation,
            "required_seed_generation": required_generation,
            "seed_age_s": 0.0,
            "frames_since_seed": int(bootstrap.get("frames_since_seed", 0) or 0),
            "collecting": bool(bootstrap.get("collecting", False)),
            "video_age_s": float(video_age_s),
            "backlog_follow_video_pusi_target": backlog_frame_target,
            "backlog_ready": bool(backlog_ready),
            "backlog_generation_safe": True,
            "backlog_candidate_bytes": int(bootstrap.get("bytes", 0) or 0),
            "preferred_join_mode": "ready-backlog" if backlog_ready else "pending",
            "latest_severe_gap_event": None,
        }
