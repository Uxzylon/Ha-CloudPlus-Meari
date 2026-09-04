"""Coordinator mixin exposing camera availability and IoT-derived state."""

from __future__ import annotations

import logging
import socket
import time
from typing import Any, Callable

from ..api import MeariApiClient
from ..const import (
    IOT_CODE_BATTERY_PERCENT,
    IOT_CODE_CHARGE_STATUS,
    IOT_CODE_LAMP,
    IOT_CODE_VIDEO_ENCRYPTION,
    PTZ_DIRECTIONS,
)
from ..p2p_streamer import (
    quality_profile_labels,
    supports_adaptive_stream,
)
from .iot import iot_value, normalize_iot_values, supports_feature

_LOGGER = logging.getLogger(__name__)


class CoordinatorStateMixin:
    """Mixin exposing availability and IoT-derived state to HA entities."""

    _latest_image_source: str
    _latest_image_generation: int = 0
    _latest_image_updated_at: float

    @property
    def available(self) -> bool:
        return self._available

    @property
    def latest_image(self) -> bytes | None:
        return self._latest_image

    @property
    def latest_image_source(self) -> str:
        return self._latest_image_source

    @property
    def latest_image_generation(self) -> int:
        return self._latest_image_generation

    @property
    def latest_image_updated_at(self) -> float:
        return self._latest_image_updated_at

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
        if self._running:
            return self._stream_server.ensure_running()
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
        return quality_profile_labels(self._device)

    @property
    def supports_auto_quality(self) -> bool:
        return supports_adaptive_stream(self._device)

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
        except (OSError, RuntimeError, ValueError, KeyError) as exc:
            _LOGGER.warning("Battery prefetch failed for %s: %s", self._sn_num, exc)

    def prefetch_status(self, api: MeariApiClient) -> None:
        self.prefetch_lamp(api)

    def prefetch_lamp(self, api: MeariApiClient) -> None:
        self._api = api
        if not api.openapi_server:
            return
        try:
            iot = api.get_device_iot_config(self._sn_num)
        except (OSError, RuntimeError, ValueError, KeyError) as exc:
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

    def _note_motion(self, motion_type: str, event_image: bytes | None = None) -> None:
        if event_image:
            self._latest_image = event_image
            self._latest_image_source = "event"
            self._latest_image_generation += 1
            self._latest_image_updated_at = time.time()
            # Keep the exact alarm image authoritative while motion is active.
            # A wake-up may first replay an old keyframe from the previous session.
            self._last_snapshot_convert_time = time.monotonic()
            self._fire_update()
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

    def _reauthenticate_api(self, api: MeariApiClient, context: str) -> bool:
        try:
            api.login()
        except (OSError, RuntimeError, ValueError, KeyError) as exc:
            _LOGGER.debug(
                "%s reauthentication failed for %s: %s",
                context,
                self._sn_num,
                exc,
            )
            return False
        _LOGGER.info("%s reauthenticated for %s", context, self._sn_num)
        return True

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
            except (OSError, RuntimeError, ValueError, KeyError) as exc:
                if attempt == 0:
                    _LOGGER.debug("Battery poll retry for %s: %s", self._sn_num, exc)
                    self._reauthenticate_api(api, "Battery poll")
                    continue
                _LOGGER.debug("Battery poll failed for %s: %s", self._sn_num, exc)

    def _poll_status(self) -> None:
        api = self._api
        if api is None or not api.openapi_server:
            return
        iot: dict[str, Any] = {}
        for attempt in range(2):
            try:
                iot = api.get_device_iot_config(self._sn_num)
                break
            except (OSError, RuntimeError, ValueError, KeyError) as exc:
                if attempt == 0:
                    _LOGGER.debug("Status poll retry for %s: %s", self._sn_num, exc)
                    self._reauthenticate_api(api, "Status poll")
                    continue
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
