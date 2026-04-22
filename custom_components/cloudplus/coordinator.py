"""Coordinator for CloudEdge / Meari camera — manages lifecycle.

Runs MQTT listener, P2P streaming, ffmpeg muxer, TCP stream server,
and idle stream in background threads. Manages camera wake/sleep state
and motion detection.

Follows the same architecture as the home_v reference coordinator.
"""

from __future__ import annotations

import collections
import fcntl
import logging
import os
import queue
import re
import select
import socket
import ssl
import struct
import subprocess
import threading
import time
from typing import Any, Callable, TYPE_CHECKING

from homeassistant.core import HomeAssistant

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry

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

from .meari_commands import (
    ABNORMAL_NOISE_ENABLE,
    ALARM_FREQUENCY,
    ANTI_JAMMING,
    AUTO_UPDATE,
    BATTERY_PERCENT,
    BELL_PHONE,
    BLE_SWITCH,
    CHARGE_STATUS,
    CHIME_PRO_MOTION_VOLUME,
    CHIME_PRO_RING_TYPE,
    CHIME_PRO_RING_VOLUME,
    COME_DEVICE_VOLUME,
    CRY_DET_ENABLE,
    DAY_NIGHT_MODE,
    FACE_RECOGNITION_SWITCH,
    FLIGHT_BRIGHTNESS,
    FLIGHT_LIGHT_SWITCH,
    FLIGHT_LINK_LIGHTING_ENABLE,
    FLIGHT_LINK_SIREN_ENABLE,
    FLIGHT_MANUAL_LIGHTING_DURATION,
    FLIGHT_PIR_DURATION,
    FRAME_RATE,
    FULL_COLOR_MODE,
    H265_ENABLE,
    HOMEKIT_ENABLE,
    HUMAN_DET_ENABLE,
    HUMAN_FRAME_ENABLE,
    HUMAN_SENSITIVITY_LEVEL,
    HUMAN_TRACK_ENABLE,
    INFRARED_LIGHT,
    JINGLE_VOLUME,
    LANGUAGE,
    LASER_SWITCH,
    LED_ENABLE,
    LOGO_SWITCH,
    MEN_PHONE,
    MONITOR_TIME_SWITCH,
    MOTION_DET_ENABLE,
    MOTION_DET_SENSITIVITY,
    MUSIC_PLAY_MODE,
    MUSIC_VOLUME,
    NO_FLK,
    ONVIF_ENABLE,
    OSD_ENABLE,
    OSD_STYLE,
    PET_ALARM,
    PET_ALARM_ENABLE,
    PET_THROW_WARNING,
    PIR_DET_ENABLE,
    PIR_DET_SENSITIVITY,
    PIR_JIM,
    PIR_TRIGGER_INTERVAL,
    PLUG_LOW_POWER_MODE,
    POWER_ON_VOLUME,
    RAE_SOUND,
    RECORD_RESOLUTION,
    RECORD_SWITCH,
    RELAY_ENABLE,
    RGB_LIGHT_MODE,
    RGB_LIGHT_SWITCH,
    SD_RECORD_TYPE,
    SEN_SOUND,
    SHOT_RESOLUTION,
    SHOT_TYPE,
    SLEEP_MODE,
    SMART_DET,
    SMART_DET_FRAME,
    SMART_DET_SENSITIVITY,
    SOT_TIME,
    SOUND_DET_ENABLE,
    SOUND_DET_SENSITIVITY,
    SOUND_LIGHT_ENABLE,
    SOUND_LIGHT_TYPE,
    SOUND_SWITCH,
    SPEAK_VOLUME,
    TEASE_DURATION,
    TEASE_MODE,
    TIME_FORMAT_SWITCH,
    TIMED_PTZ_PATROL,
    TIMING_SHOT,
    TIMING_SHOT_SWITCH,
    UPLOAD_VIDEO,
    WARM_LIGHT_BRI,
    WIRELESS_CHIME_ENABLE,
    WIRELESS_CHIME_VOLUME,
)


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
        entry: "ConfigEntry | None" = None,
    ) -> None:
        self.hass = hass
        self._entry = entry
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

        # Parse device capabilities
        self._capabilities: dict = {}
        try:
            import json as _json
            cap_raw = device.get("capability", "")
            if cap_raw:
                cap = _json.loads(cap_raw) if isinstance(cap_raw, str) else cap_raw
                self._capabilities = cap.get("caps", {})
        except Exception:
            pass

        # Detection Capabilities (Refined from MeariDeviceUtil.java)
        self._has_motion_det = self._capabilities.get("md", 0) > 0
        self._has_person_det = self._capabilities.get("pdt", 0) > 0
        self._has_noise_det = self._capabilities.get("nst", 0) > 0
        self._has_cry_det = self._capabilities.get("cct", 0) > 0
        self._has_face_det = self._capabilities.get("fcd", 0) > 0
        self._has_pet_alarm = self._capabilities.get("pet", 0) > 0
        self._has_abnormal_noise = self._capabilities.get("nms", 0) > 0

        # PIR Logic from isSupportPir
        pir = self._capabilities.get("pir", 0)
        flt = self._capabilities.get("flt", 0)
        plv = self._capabilities.get("plv", 0)  # MultiLevelPir
        self._has_pir = (pir != 5 and pir != 7 and flt != 1) and (pir > 0 or plv > 0)
        self._has_double_pir = (pir == 5 or pir == 6)

        # Power & Low Power
        dev_type = device.get("devTypeID", 0)
        ver = self._capabilities.get("ver", 0)
        pwm = self._capabilities.get("pwm", 0)
        bat = self._capabilities.get("bat", 0)
        if ver >= 6:
            self._is_low_power = pwm > 0 or dev_type == 6
        else:
            self._is_low_power = dev_type == 4 or dev_type == 5
        self._is_battery_powered = bat == 1 or self._is_low_power

        # PTZ Logic from isSupportPtz
        ptz = self._capabilities.get("ptz", 0)
        ptz2 = self._capabilities.get("ptz2", 0)
        self._has_ptz = ptz > 0 or ptz2 > 0
        self._has_ptz2 = ptz2 > 0 or not self._device.get("msc", "") == ""
        pcr = self._capabilities.get("pcr", 0)
        self._has_ptz_patrol = (pcr & 4) == 4
        self._has_ptz_presets = (pcr & 1) == 1
        self._has_ptz_calibration = self._capabilities.get("ptc", 0) == 1

        # Audio Logic
        self._has_microphone = self._capabilities.get("men", 0) > 0
        ovc = self._capabilities.get("ovc", 0)
        if ver >= 18:
            self._has_speaker = ovc > 0
        else:
            self._has_speaker = dev_type == 4 or dev_type == 5

        # Storage
        if ver >= 6:
            self._has_sd_card = self._capabilities.get("sd", 0) > 0
        else:
            self._has_sd_card = dev_type != 6
        self._has_sd_card_2 = self._capabilities.get("sd2", 0) > 0

        # Lighting & Siren
        self._has_status_led = self._capabilities.get("led", 0) > 0
        self._has_floodlight = self._capabilities.get("flt", 0) > 0
        ltl = self._capabilities.get("ltl", 0)
        if flt == 2:
            self._has_light_brightness = (ltl > 0) if ver >= 41 else True
        else:
            self._has_light_brightness = ltl > 0

        self._has_siren = self._capabilities.get("sir", 0) > 0
        sla = self._capabilities.get("sla", 0)
        self._has_siren_alarm = (sla & 8) == 8 or (sla & 16) == 16
        self._has_rgb_light = self._capabilities.get("rgb", 0) > 0
        self._has_infrared = self._capabilities.get("ir", 0) > 0
        self._has_warm_light = self._capabilities.get("wml", 0) > 0
        self._has_white_light = self._capabilities.get("wtl", 0) > 0
        self._has_laser = self._capabilities.get("las", 0) > 0

        # Doorbell / Chime Logic (RNG field)
        rng = self._capabilities.get("rng", -1)
        self._has_mechanical_bell = rng != -1 and (rng & 1) == 1
        self._has_wireless_bell = rng != -1 and (rng & 62) != 0  # 2|4|8|16|32
        self._has_doorbell = self._capabilities.get("dor", 0) > 0

        # System & AI
        if ver >= 9:
            self._has_sleep_mode = self._capabilities.get("slp", 0) > 0
        else:
            self._has_sleep_mode = not self._is_low_power

        self._has_human_track = self._capabilities.get("ptr", 0) > 0
        self._has_homekit = self._capabilities.get("hkt", 0) > 0
        self._has_baby_monitor = self._capabilities.get("mrda", 0) > 0
        self._has_ota_updates = self._capabilities.get("ota", 0) > 0
        self._has_onvif = self._capabilities.get("ovf", 0) > 0
        self._has_p2p = self._capabilities.get("p2p", 0) > 0
        self._has_anti_jamming = self._capabilities.get("ajs", 0) > 0
        self._has_bluetooth = self._capabilities.get("ble", 0) > 0
        self._has_auto_update = self._capabilities.get("aup", 0) > 0
        self._has_full_color = self._capabilities.get("fld", 0) > 0
        self._has_alarm_plan = self._capabilities.get("alp", 0) > 0
        self._has_alarm_frequency = self._capabilities.get("afq", 0) > 0
        self._has_temp_sensor = self._capabilities.get("tmpr", 0) > 0
        self._has_hmd_sensor = self._capabilities.get("hmd", 0) > 0

        # IOT Data Storage
        self._iot_data: dict[int, Any] = {}

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
        self._idle_keepalive_fps_initial: float = 5.0
        self._idle_keepalive_fps_steady: float = 2.0
        self._idle_keepalive_fps_with_clients: float = 2.0
        self._idle_keepalive_settle_seconds: float = 3.0
        self._live_gap_fill_after_seconds: float = 0.5
        self._live_gap_fill_fps: float = 3.0
        self._audio_gain_db: float = 24.0
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
        self._stream_clients: list[tuple[socket.socket, collections.deque, threading.Event]] = []
        self._stream_backlog: bytearray = bytearray()
        # During startup, accumulate all TS data without trimming so
        # new clients get the complete stream from the beginning
        # (including PAT/PMT + first keyframe + eventually audio).
        # Once audio PID is detected in the backlog, switch to a
        # bounded rolling window.
        self._stream_backlog_has_audio: bool = False
        # Keep bootstrap backlog small so newly connected readers lock quickly
        # without inheriting seconds of old latency.
        self._stream_backlog_max_bytes: int = 512 * 1024  # 512KB
        # Per-client write queue depth limit (each item is one flush ~8-32KB).
        self._stream_client_queue_limit: int = 500
        # IDR keyframe seed: PAT + PMT + complete IDR frame TS data.
        # Sent to new clients so the decoder can always initialise
        # (VPS/SPS/PPS for HEVC, SPS/PPS for H.264).
        self._stream_idr_seed: bytes = b""
        self._stream_idr_collecting: bool = False
        self._stream_idr_buf: bytearray = bytearray()
        self._stream_clients_lock = threading.Lock()
        self._stream_accept_thread: threading.Thread | None = None
        self._stream_epoch: float = 0.0  # wall-clock anchor for MPEG-TS timestamps

        # ffmpeg muxer (H264/HEVC + G.711 µ-law → MPEG-TS)
        self._ffmpeg_proc: subprocess.Popen | None = None
        self._ffmpeg_reader_thread: threading.Thread | None = None
        self._audio_write_fd: int = -1
        self._audio_enc_proc: subprocess.Popen | None = None
        self._audio_aac_deque: collections.deque = collections.deque(maxlen=50)  # ~3.2 s reserve
        self._audio_primed = threading.Event()  # set when real camera audio arrives
        self._muxer_video_started = threading.Event()  # set after first video write to ffmpeg
        self._silence_feeder_gen: int = 0  # generation counter to cancel stale feeders
        self._audio_writer_thread: threading.Thread | None = None
        self._audio_real_started: bool = False
        # Keep audio buffering shallow and realtime-friendly.
        self._audio_queue: queue.Queue = queue.Queue(maxsize=50)
        self._audio_queue_drops: int = 0
        self._audio_throttle_drops: int = 0
        self._audio_realtime_next_ts: float = 0.0
        self._audio_soft_queue_limit: int = 40

        # Video write queue to decouple P2P receive from ffmpeg stdin backpressure.
        self._video_queue: queue.Queue = queue.Queue(maxsize=150)
        self._video_lag_drop_threshold: int = 8
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

        # VVP quality profile: None = auto (highest from bps2 capability)
        self._vvp_quality: int | None = None

        # Restore persisted settings from config entry options
        if self._entry is not None:
            opts = self._entry.options
            sn = self._sn_num
            self._motion_timeout = opts.get(f"{sn}_motion_timeout", DEFAULT_MOTION_TIMEOUT)
            self._motion_wake_enabled = opts.get(f"{sn}_motion_wake_enabled", True)
            self._stream_host_mode = opts.get(f"{sn}_stream_host_mode", "ip")
            self._vvp_quality = opts.get(f"{sn}_vvp_quality")

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    def prefetch_battery(self, api: "MeariApiClient") -> None:
        """Pre-load battery info using an already-authenticated API client.

        Call this during integration setup (before entity platforms load) so
        battery sensors have a value immediately instead of showing *unknown*
        until the background thread logs in.
        """
        if not self._is_snap:
            return
        try:
            info = api.get_battery_info(self._sn_num)
            if not info:
                return
            pct = info.get(BATTERY_PERCENT)
            charge = info.get(CHARGE_STATUS)
            if pct is not None:
                pct_int = int(pct)
                if 0 <= pct_int <= 100:
                    self._battery_percent = pct_int
            if charge is not None:
                self._battery_charging = int(charge) == 1
        except Exception as exc:
            _LOGGER.warning("Prefetch battery failed for %s: %s", self._sn_num, exc)

    def prefetch_status(self, api: "MeariApiClient") -> None:
        """Pre-load status using an already-authenticated API client."""
        try:
            iot = api.get_device_iot_config(self._sn_num)
            if not iot:
                return

            new_iot_data = {}
            for k, v in iot.items():
                try:
                    new_iot_data[int(k)] = v
                except (ValueError, TypeError):
                    new_iot_data[k] = v
            self._iot_data = new_iot_data

        except Exception as exc:
            _LOGGER.warning("Prefetch status failed for %s: %s", self._sn_num, exc)

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
    def has_ptz(self) -> bool:
        return self._has_ptz

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

    def _persist_option(self, key: str, value) -> None:
        """Persist a per-device setting to config entry options."""
        if self._entry is None:
            return
        full_key = f"{self._sn_num}_{key}"
        new_opts = dict(self._entry.options)
        new_opts[full_key] = value
        try:
            self.hass.config_entries.async_update_entry(
                self._entry, options=new_opts,
            )
        except Exception:
            _LOGGER.debug("Could not persist option %s", full_key, exc_info=True)

    def set_motion_wake_enabled(self, enabled: bool) -> None:
        self._motion_wake_enabled = enabled
        self._persist_option("motion_wake_enabled", enabled)
        _LOGGER.info(
            "Motion wake %s for %s",
            "enabled" if enabled else "disabled",
            self._sn_num,
        )
        self._fire_update()

    def set_motion_timeout(self, timeout: int) -> None:
        self._motion_timeout = max(10, min(timeout, 600))
        self._persist_option("motion_timeout", self._motion_timeout)
        _LOGGER.info(
            "Motion timeout set to %ds for %s",
            self._motion_timeout, self._sn_num,
        )
        self._fire_update()

    def set_stream_host_mode(self, mode: str) -> None:
        """Set stream host mode: 'ip' or 'docker'."""
        self._stream_host_mode = mode
        self._persist_option("stream_host_mode", mode)
        _LOGGER.info(
            "Stream host mode set to %s for %s", mode, self._sn_num,
        )
        self._fire_update()

    @property
    def quality_profiles(self) -> dict[int, str]:
        """Return available quality profiles ``{id: label}`` from device caps."""
        from .p2p_streamer import parse_quality_profiles
        return parse_quality_profiles(self._device)

    @property
    def vvp_quality(self) -> int | None:
        """Current VVP quality override (``None`` = auto/highest)."""
        return self._vvp_quality

    def set_vvp_quality(self, quality: int | None) -> None:
        """Set VVP quality profile for future P2P sessions."""
        self._vvp_quality = quality
        self._persist_option("vvp_quality", quality)
        _LOGGER.info(
            "VVP quality set to %s for %s",
            quality if quality is not None else "auto",
            self._sn_num,
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
            for c, _q, ev in self._stream_clients:
                ev.set()  # wake writer thread so it exits
                try:
                    c.close()
                except OSError:
                    pass
            self._stream_clients.clear()
            self._stream_backlog.clear()
            self._stream_backlog_has_audio = False
            self._stream_idr_seed = b""
            self._stream_idr_collecting = False
            self._stream_idr_buf.clear()

    def _accept_stream_clients(self) -> None:
        """Accept loop for TCP stream clients (runs in thread)."""
        while self._running and self._stream_server_sock:
            try:
                client, addr = self._stream_server_sock.accept()
                client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                client.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1 << 20)
                # Blocking socket with timeout — each client gets its
                # own writer thread that does sendall(), so a slow client
                # cannot block the reader or other clients.
                client.settimeout(10.0)
                q: collections.deque = collections.deque(
                    maxlen=self._stream_client_queue_limit,
                )
                ev = threading.Event()
                # Seed the queue so the player can initialise quickly.
                # Prefer the IDR seed (PAT+PMT+keyframe with VPS/SPS/PPS)
                # so the decoder starts cleanly.  Fall back to the
                # rolling backlog if no keyframe has been captured yet.
                with self._stream_clients_lock:
                    if self._stream_idr_seed:
                        q.append(self._stream_idr_seed)
                    elif self._stream_backlog:
                        q.append(bytes(self._stream_backlog))
                    self._stream_clients.append((client, q, ev))
                threading.Thread(
                    target=self._client_writer,
                    args=(client, q, ev),
                    daemon=True,
                ).start()
                _LOGGER.debug("Stream client connected from %s", addr)
            except socket.timeout:
                continue
            except OSError:
                break

    def _client_writer(
        self,
        client: socket.socket,
        q: collections.deque,
        ev: threading.Event,
    ) -> None:
        """Dedicated writer thread per TCP client — blocking sendall."""
        try:
            while self._running:
                ev.wait(timeout=0.5)
                ev.clear()
                while q:
                    chunk = q.popleft()
                    client.sendall(chunk)
        except (BrokenPipeError, ConnectionError, OSError, TimeoutError):
            pass
        finally:
            with self._stream_clients_lock:
                self._stream_clients = [
                    (c, qq, ee) for c, qq, ee in self._stream_clients
                    if c is not client
                ]
            try:
                client.close()
            except OSError:
                pass

    def _append_stream_backlog(self, data: bytes) -> None:
        """Keep an MPEG-TS backlog for new client bootstrap.

        During startup, accumulates ALL data so the backlog contains
        the complete stream from the beginning (PAT/PMT + keyframe +
        audio).  The ffmpeg muxer emits audio TS packets only after a
        significant delay (~10-20s), so keeping the full stream start
        ensures new clients can probe both audio and video.

        Once audio is detected, switches to a bounded rolling window
        to keep memory bounded.  When trimming, finds the latest PAT
        packet boundary to ensure the remaining data starts with valid
        TS program information.
        """
        if not data:
            return
        PKT = 188
        self._stream_backlog.extend(data)

        # Scan new data for audio PID 0x101 to detect transition.
        if not self._stream_backlog_has_audio:
            scan_start = max(0, len(self._stream_backlog) - len(data))
            scan_start = (scan_start // PKT) * PKT
            n_total = len(self._stream_backlog) // PKT
            for i in range(scan_start // PKT, n_total):
                off = i * PKT
                if off + PKT > len(self._stream_backlog):
                    break
                if self._stream_backlog[off] != 0x47:
                    continue
                pid = ((self._stream_backlog[off + 1] & 0x1F) << 8) | self._stream_backlog[off + 2]
                if pid == 0x101:
                    self._stream_backlog_has_audio = True
                    _LOGGER.info(
                        "Backlog: audio detected at %.2f MB, "
                        "switching to rolling mode",
                        len(self._stream_backlog) / 1048576,
                    )
                    break

        # In startup mode (no audio yet), keep everything — don't trim.
        # Cap at 64MB to prevent runaway memory if audio never appears.
        if not self._stream_backlog_has_audio:
            if len(self._stream_backlog) > 64 * 1024 * 1024:
                _LOGGER.warning("Backlog: 64MB cap reached without audio, trimming")
                self._stream_backlog_has_audio = True  # force rolling mode
            else:
                return

        # Rolling mode: trim to bounded size.
        if len(self._stream_backlog) > self._stream_backlog_max_bytes:
            overflow = len(self._stream_backlog) - self._stream_backlog_max_bytes
            # Find the nearest PAT packet (PID 0x0000) after the trim point
            # to ensure the backlog starts with valid program info.
            trim_at = overflow
            search_end = min(trim_at + 256 * PKT, len(self._stream_backlog))
            for i in range(trim_at // PKT, search_end // PKT):
                off = i * PKT
                if off + PKT > len(self._stream_backlog):
                    break
                if self._stream_backlog[off] != 0x47:
                    continue
                pid = ((self._stream_backlog[off + 1] & 0x1F) << 8) | self._stream_backlog[off + 2]
                if pid == 0x0000:  # PAT
                    trim_at = off
                    break
            del self._stream_backlog[:trim_at]

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
        """Send MPEG-TS data to all connected stream clients.

        Appends ``data`` to each client's write queue and signals its
        writer thread.  Never blocks on socket I/O — the per-client
        writer threads handle actual sending via blocking sendall().
        """
        if not data:
            return
        chunk = bytes(data)
        with self._stream_clients_lock:
            self._append_stream_backlog(chunk)
            self._update_idr_seed(chunk)
            for _client, q, ev in self._stream_clients:
                q.append(chunk)
                ev.set()

    # ------------------------------------------------------------------
    # ffmpeg muxer (H264/HEVC + G.711 µ-law → MPEG-TS)
    # ------------------------------------------------------------------

    def _start_ffmpeg_muxer(self) -> None:
        """Start ffmpeg to mux camera video into MPEG-TS.

        Uses two ffmpeg processes:

        1. **Main muxer**: video only (stdin) → MPEG-TS.
           No audio input means zero interleaving delay — each video
           frame is output immediately.  The ``_ffmpeg_stdout_reader``
           patches the PMT to include an audio PID and injects real
           AAC ADTS frames from the audio encoder.

        2. **Audio encoder**: µ-law camera audio (pipe) → raw AAC-LC ADTS frames.
           Runs independently; produces frames only when real audio arrives.
        """
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
        # -- Main muxer: video only → MPEG-TS ----------------------------
        # setts BSF assigns monotonic PTS/DTS so ffmpeg can mux valid TS.
        # The stdout reader overwrites all PTS/DTS/PCR in the TS output
        # with wall-clock timestamps for smooth playback.
        # No audio input eliminates interleaving delay — every video
        # frame is output immediately.  The stdout reader patches the PMT
        # to include an audio PID and injects real AAC TS packets.
        cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", "info",
            # Video input (H264/HEVC NALs from stdin)
            "-fflags", "+genpts+igndts+discardcorrupt",
            "-err_detect", "ignore_err",
            "-probesize", "32768",
            "-analyzeduration", "0",
            "-framerate", f"{video_input_fps:.3f}",
            "-thread_queue_size", "128",
            "-f", video_fmt, "-i", "pipe:0",
            # Output: video-only mux — audio injected by stdout reader
            "-map", "0:v",
            "-c:v", "copy",
            "-bsf:v", video_setts,
            "-max_delay", "0",
            "-muxpreload", "0",
            "-muxdelay", "0",
            "-flush_packets", "1",
            "-f", "mpegts",
            "-mpegts_flags", "+resend_headers+pat_pmt_at_frames",
            "pipe:1",
        ]

        # -- Audio encoder: µ-law → raw AAC-LC ADTS frames (no container) --
        audio_enc_cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", "warning",
            "-f", "mulaw", "-ar", "8000", "-ac", "1",
            "-i", f"pipe:{audio_r}",
            "-filter:a",
            f"volume={self._audio_gain_db:.1f}dB,alimiter=limit=0.92",
            "-c:a", "aac", "-profile:a", "aac_low",
            "-ar", "16000", "-ac", "1", "-b:a", "32k",
            "-flush_packets", "1",
            "-f", "adts", "pipe:1",
        ]

        try:
            # Start main muxer (video only → MPEG-TS)
            self._ffmpeg_proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            # Start audio encoder (µ-law → raw AAC-LC ADTS frames)
            self._audio_enc_proc = subprocess.Popen(
                audio_enc_cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
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

            # Keep audio pipe buffer small for low latency (16KB).
            try:
                fcntl.fcntl(self._audio_write_fd, 1031, 16384)  # F_SETPIPE_SZ
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
            self._audio_aac_deque.clear()
            self._muxer_video_started.clear()
            # Reset backlog to accumulate from start of this muxer session.
            self._stream_backlog.clear()
            self._stream_backlog_has_audio = False
            self._stream_idr_seed = b""
            self._stream_idr_collecting = False
            self._stream_idr_buf.clear()

            # Serialize all writes through a dedicated writer thread so
            # partial non-blocking writes never corrupt realtime audio flow.
            proc_ref = self._ffmpeg_proc
            self._audio_writer_thread = threading.Thread(
                target=self._audio_writer,
                args=(proc_ref,),
                daemon=True,
            )
            self._audio_writer_thread.start()

            # Reader thread: collects AAC ADTS frames from audio encoder stdout
            threading.Thread(
                target=self._audio_aac_reader,
                daemon=True,
            ).start()

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

            # Enlarge stdout pipe to 1MB so the muxer never blocks
            # on stdout while the reader is busy broadcasting.
            try:
                fcntl.fcntl(
                    self._ffmpeg_proc.stdout.fileno(), 1031, 1048576
                )  # F_SETPIPE_SZ
            except Exception:
                pass

            # Feed µ-law silence at real-time rate until camera audio
            # arrives.  A burst would cause audio PTS to jump ahead
            # of video PTS under +genpts, leading to A/V desync.
            self._audio_primed.clear()
            self._silence_feeder_gen += 1
            self._silence_feeder_thread = threading.Thread(
                target=self._silence_feeder, args=(self._silence_feeder_gen,), daemon=True,
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
        """Stop the ffmpeg muxer and audio encoder."""
        self._audio_primed.set()  # stop silence feeder thread
        if self._ffmpeg_proc:
            self._close_audio_fd()
            # Terminate ffmpeg first.  Python's BufferedWriter.close()
            # calls flush() which blocks indefinitely when the pipe
            # buffer is full and ffmpeg isn't reading.  Sending SIGTERM
            # makes ffmpeg exit, which unblocks any pending pipe I/O.
            try:
                self._ffmpeg_proc.terminate()
            except OSError:
                pass
            # Close stdin at the OS level, bypassing Python's flush.
            # Detach Python's FileIO wrapper first so its finalizer
            # doesn't attempt a second os.close() (EBADF warning).
            try:
                self._ffmpeg_proc.stdin.raw.closefd = False
                os.close(self._ffmpeg_proc.stdin.fileno())
            except (OSError, ValueError, AttributeError):
                pass
            try:
                self._ffmpeg_proc.wait(timeout=5)
            except Exception:
                try:
                    self._ffmpeg_proc.kill()
                    self._ffmpeg_proc.wait(timeout=2)
                except Exception:
                    pass
            self._ffmpeg_proc = None

        # Stop audio encoder
        if self._audio_enc_proc:
            try:
                self._audio_enc_proc.terminate()
                self._audio_enc_proc.wait(timeout=3)
            except Exception:
                try:
                    self._audio_enc_proc.kill()
                    self._audio_enc_proc.wait(timeout=1)
                except Exception:
                    pass
            self._audio_enc_proc = None

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

    # ------------------------------------------------------------------
    # AAC silence injection for continuous audio in MPEG-TS
    # ------------------------------------------------------------------
    # Pre-computed AAC-LC silence frame (ADTS): 16 kHz, mono.
    # 11 bytes, 64 ms duration (1024 samples at 16 kHz).
    # Generated via: ffmpeg -f lavfi -i anullsrc=r=16000:cl=mono \
    #                -c:a aac -profile:a aac_low -b:a 32k -ar 16000 -ac 1 \
    #                -t 0.08 -f adts -
    _AAC_SILENCE_FRAME: bytes = (
        b"\xff\xf1\x60\x40\x01\x7f\xfc"
        b"\x01\x18\x20\x07"
    )
    # Duration of one AAC-LC frame in 90 kHz PTS ticks: 1024/16000*90000
    _AAC_FRAME_TICKS: int = 5760  # 64 ms

    @staticmethod
    def _encode_pts(pts_ticks: int) -> bytes:
        """Encode a 33-bit PTS value into the 5-byte PES PTS field."""
        pts = pts_ticks & 0x1FFFFFFFF
        b0 = 0x20 | ((pts >> 29) & 0x0E) | 1
        b1 = (pts >> 22) & 0xFF
        b2 = ((pts >> 14) & 0xFE) | 1
        b3 = (pts >> 7) & 0xFF
        b4 = ((pts << 1) & 0xFE) | 1
        return bytes([b0, b1, b2, b3, b4])

    @classmethod
    def _make_audio_ts(
        cls, frame: bytes, pts_90khz: int, cc_start: int,
        audio_pid: int = 0x101,
    ) -> tuple[bytes, int]:
        """Wrap one AAC ADTS frame in PES + MPEG-TS packets.

        Returns ``(ts_bytes, next_continuity_counter)``.
        """
        PKT = 188
        if not frame:
            return b"", cc_start

        # Build PES packet: start-code + stream_id + length + hdr + PTS + data
        pts_bytes = cls._encode_pts(pts_90khz)
        pes = (
            b"\x00\x00\x01\xc0"
            + struct.pack(">H", 3 + 5 + len(frame))
            + b"\x80\x80\x05"
            + pts_bytes
            + frame
        )

        result = bytearray()
        cc = cc_start & 0x0F
        offset = 0
        first = True

        while offset < len(pes):
            remaining = len(pes) - offset
            hdr = bytearray(4)
            hdr[0] = 0x47
            pusi = 0x40 if first else 0x00
            hdr[1] = pusi | ((audio_pid >> 8) & 0x1F)
            hdr[2] = audio_pid & 0xFF

            if remaining < 184:
                # Last packet: adaptation field for stuffing.
                # AF with length=0 is 1 byte; length≥1 needs ≥2 bytes
                # (length + flags).  When remaining == 183 only 1 spare
                # byte is available, so use a zero-length AF.
                spare = 184 - remaining
                if spare == 1:
                    # 1-byte AF: adaptation_field_length = 0
                    hdr[3] = 0x30 | (cc & 0x0F)
                    af = b"\x00"
                else:
                    stuff_len = max(0, spare - 2)
                    hdr[3] = 0x30 | (cc & 0x0F)
                    af = bytearray([1 + stuff_len, 0x00])
                    af += bytearray([0xFF] * stuff_len)
                payload = pes[offset: offset + remaining]
                pkt = bytes(hdr) + bytes(af) + payload
            else:
                hdr[3] = 0x10 | (cc & 0x0F)
                payload = pes[offset: offset + 184]
                pkt = bytes(hdr) + payload

            assert len(pkt) == PKT
            result.extend(pkt)
            offset += len(payload)
            cc = (cc + 1) & 0x0F
            first = False

        return bytes(result), cc

    @classmethod
    def _make_silence_audio_ts(
        cls, pts_90khz: int, cc_start: int, audio_pid: int = 0x101,
    ) -> tuple[bytes, int]:
        """Convenience: wrap the pre-computed AAC silence frame."""
        return cls._make_audio_ts(
            cls._AAC_SILENCE_FRAME, pts_90khz, cc_start, audio_pid,
        )

    @staticmethod
    def _mpegts_crc32(data: bytes) -> int:
        """Compute CRC32/MPEG-2 for MPEG-TS PSI section data."""
        crc = 0xFFFFFFFF
        for byte in data:
            crc ^= byte << 24
            for _ in range(8):
                if crc & 0x80000000:
                    crc = ((crc << 1) ^ 0x04C11DB7) & 0xFFFFFFFF
                else:
                    crc = (crc << 1) & 0xFFFFFFFF
        return crc

    def _build_pmt_packet(self, cc: int) -> bytes:
        """Build a PMT TS packet listing video + audio PIDs.

        The main muxer outputs video-only MPEG-TS.  Its PMT only lists
        the video PID.  This method builds a replacement PMT that also
        includes the audio PID so players detect both streams.
        """
        VIDEO_PID = 0x0100
        AUDIO_PID = 0x0101
        PMT_PID = 0x1000
        stream_type_v = 0x24 if self._video_codec != "h264" else 0x1B
        section = bytearray([
            0x02,                              # table_id = PMT
            0xB0, 0x17,                        # SSI=1, section_length=23
            0x00, 0x01,                        # program_number = 1
            0xC1,                              # version=0, current_next=1
            0x00, 0x00,                        # section / last section = 0
            0xE0 | (VIDEO_PID >> 8), VIDEO_PID & 0xFF,   # PCR_PID
            0xF0, 0x00,                        # program_info_length = 0
            stream_type_v,                     # video stream type
            0xE0 | (VIDEO_PID >> 8), VIDEO_PID & 0xFF,
            0xF0, 0x00,                        # ES_info_length = 0
            0x0F,                              # AAC ADTS audio
            0xE0 | (AUDIO_PID >> 8), AUDIO_PID & 0xFF,
            0xF0, 0x00,                        # ES_info_length = 0
        ])
        crc = self._mpegts_crc32(bytes(section))
        section.extend(crc.to_bytes(4, "big"))
        pkt = bytearray(188)
        pkt[0] = 0x47
        pkt[1] = 0x40 | ((PMT_PID >> 8) & 0x1F)  # PUSI=1
        pkt[2] = PMT_PID & 0xFF
        pkt[3] = 0x10 | (cc & 0x0F)
        pkt[4] = 0x00                          # pointer field
        pkt[5: 5 + len(section)] = section
        for i in range(5 + len(section), 188):
            pkt[i] = 0xFF
        return bytes(pkt)

    def _build_pat_packet(self, cc: int = 0) -> bytes:
        """Build a PAT TS packet mapping program 1 → PMT PID 0x1000."""
        PMT_PID = 0x1000
        section = bytearray([
            0x00,                              # table_id = PAT
            0xB0, 0x0D,                        # SSI=1, section_length=13
            0x00, 0x01,                        # transport_stream_id = 1
            0xC1,                              # version=0, current_next=1
            0x00, 0x00,                        # section / last section = 0
            0x00, 0x01,                        # program_number = 1
            0xE0 | ((PMT_PID >> 8) & 0x1F),   # reserved + PMT PID high
            PMT_PID & 0xFF,                    # PMT PID low
        ])
        crc = self._mpegts_crc32(bytes(section))
        section.extend(crc.to_bytes(4, "big"))
        pkt = bytearray(188)
        pkt[0] = 0x47
        pkt[1] = 0x40                         # PUSI=1, PID=0x0000
        pkt[2] = 0x00
        pkt[3] = 0x10 | (cc & 0x0F)          # payload only + CC
        pkt[4] = 0x00                         # pointer field
        pkt[5: 5 + len(section)] = section
        for i in range(5 + len(section), 188):
            pkt[i] = 0xFF
        return bytes(pkt)

    def _update_idr_seed(self, data: bytes) -> None:
        """Track IDR keyframes for clean mid-stream client seeding.

        Scans broadcast TS data for video keyframe starts (PUSI +
        Random Access Indicator).  Collects a complete keyframe's TS
        packets (from IDR start to next video PUSI) prepended with a
        clean PAT + PMT.  The resulting seed ensures any new client
        receives VPS/SPS/PPS (HEVC) or SPS/PPS (H.264) so the decoder
        can initialise immediately without waiting for the next live
        keyframe.
        """
        PKT = 188
        VIDEO_PID = 0x100
        n_pkts = len(data) // PKT

        for i in range(n_pkts):
            off = i * PKT
            if data[off] != 0x47:
                continue
            pid = ((data[off + 1] & 0x1F) << 8) | data[off + 2]
            is_video_pusi = (pid == VIDEO_PID) and bool(data[off + 1] & 0x40)

            if is_video_pusi:
                # Check Random Access Indicator in adaptation field
                afc = (data[off + 3] >> 4) & 0x03
                is_rai = False
                if (afc & 0x02) and data[off + 4] >= 1:
                    is_rai = bool(data[off + 5] & 0x40)

                if is_rai:
                    # New keyframe — finalise previous collection
                    if self._stream_idr_collecting and self._stream_idr_buf:
                        self._stream_idr_seed = bytes(self._stream_idr_buf)
                        _LOGGER.debug(
                            "IDR seed captured: %d bytes (%d TS pkts)",
                            len(self._stream_idr_seed),
                            len(self._stream_idr_seed) // PKT,
                        )
                    # Start new collection with clean PAT + PMT header
                    self._stream_idr_collecting = True
                    self._stream_idr_buf = bytearray()
                    self._stream_idr_buf.extend(self._build_pat_packet())
                    self._stream_idr_buf.extend(self._build_pmt_packet(0))
                elif self._stream_idr_collecting:
                    # Non-keyframe video frame → previous IDR is complete
                    self._stream_idr_seed = bytes(self._stream_idr_buf)
                    self._stream_idr_collecting = False
                    _LOGGER.debug(
                        "IDR seed captured: %d bytes (%d TS pkts)",
                        len(self._stream_idr_seed),
                        len(self._stream_idr_seed) // PKT,
                    )

            # While collecting, include every packet (video + audio)
            if self._stream_idr_collecting:
                self._stream_idr_buf.extend(data[off: off + PKT])

    @staticmethod
    def _rewrite_video_ts_timing(
        chunk: bytearray, off: int, pts: int,
    ) -> None:
        """Rewrite PTS/DTS/PCR in a video TS packet to *pts* (90 kHz).

        Modifies ``chunk`` in-place starting at byte offset ``off``.
        Handles:
        * PCR in the adaptation field (if present)
        * PES PTS and DTS in the PUSI packet's PES header
        """
        PKT = 188

        # -- Adaptation field: rewrite PCR if present --
        afc = (chunk[off + 3] >> 4) & 0x03
        payload_off = off + 4
        if afc & 0x02:                               # AF present
            af_len = chunk[off + 4]
            payload_off = off + 5 + af_len
            if af_len >= 7:                           # room for PCR
                af_flags = chunk[off + 5]
                if af_flags & 0x10:                   # PCR flag set
                    p = off + 6
                    # PCR_base (33 bits, 90 kHz) + 6 reserved + 9-bit ext
                    chunk[p + 0] = (pts >> 25) & 0xFF
                    chunk[p + 1] = (pts >> 17) & 0xFF
                    chunk[p + 2] = (pts >> 9) & 0xFF
                    chunk[p + 3] = (pts >> 1) & 0xFF
                    chunk[p + 4] = ((pts & 1) << 7) | 0x7E
                    chunk[p + 5] = 0x00

        # -- PES header: rewrite PTS/DTS --
        if not (chunk[off + 1] & 0x40):               # not PUSI
            return
        if payload_off + 9 > off + PKT:
            return
        if (chunk[payload_off] != 0
                or chunk[payload_off + 1] != 0
                or chunk[payload_off + 2] != 1):
            return

        pts_dts_flags = (chunk[payload_off + 7] >> 6) & 0x03

        if pts_dts_flags >= 2:                        # PTS present
            p = payload_off + 9
            if p + 5 <= off + PKT:
                marker = 0x03 if pts_dts_flags == 3 else 0x02
                chunk[p + 0] = (marker << 4) | ((pts >> 29) & 0x0E) | 0x01
                chunk[p + 1] = (pts >> 22) & 0xFF
                chunk[p + 2] = ((pts >> 14) & 0xFE) | 0x01
                chunk[p + 3] = (pts >> 7) & 0xFF
                chunk[p + 4] = ((pts << 1) & 0xFE) | 0x01

        if pts_dts_flags == 3:                        # DTS also present
            p = payload_off + 14
            if p + 5 <= off + PKT:
                chunk[p + 0] = (0x01 << 4) | ((pts >> 29) & 0x0E) | 0x01
                chunk[p + 1] = (pts >> 22) & 0xFF
                chunk[p + 2] = ((pts >> 14) & 0xFE) | 0x01
                chunk[p + 3] = (pts >> 7) & 0xFF
                chunk[p + 4] = ((pts << 1) & 0xFE) | 0x01

    @staticmethod
    def _reinterleave_ts(buf: bytearray, audio_pid: int) -> bytes:
        """Re-interleave TS packets so audio is evenly distributed.

        The ffmpeg mpegts muxer outputs video-then-audio blocks during
        startup because the AAC encoder is slower than the video copy
        codec.  This creates a multi-megabyte gap with zero audio TS
        packets, which prevents ffplay from detecting audio during its
        probe window on a real-time TCP stream.

        This method takes the raw buffered TS output and re-orders the
        packets so that audio packets are spread evenly among video
        packets, maintaining the original order within each stream.
        """
        PKT = 188
        n_packets = len(buf) // PKT
        if n_packets == 0:
            return bytes(buf)

        audio_indices: list[int] = []
        other_indices: list[int] = []
        for i in range(n_packets):
            off = i * PKT
            if buf[off] != 0x47:
                other_indices.append(i)
                continue
            pid = ((buf[off + 1] & 0x1F) << 8) | buf[off + 2]
            if pid == audio_pid:
                audio_indices.append(i)
            else:
                other_indices.append(i)

        n_audio = len(audio_indices)
        n_other = len(other_indices)
        if n_audio == 0 or n_other == 0:
            return bytes(buf)

        # Spread audio packets evenly: every (n_other/n_audio) other
        # packets, insert one audio packet.
        result = bytearray(n_packets * PKT)
        ratio = n_other / n_audio  # e.g. 100 video per 1 audio
        ai = 0  # audio index cursor
        oi = 0  # other index cursor
        wi = 0  # write cursor
        next_audio_at = ratio  # insert audio after this many other pkts

        for _ in range(n_packets):
            if ai < n_audio and (oi >= n_other or oi >= next_audio_at):
                src = audio_indices[ai] * PKT
                result[wi: wi + PKT] = buf[src: src + PKT]
                ai += 1
                next_audio_at = (ai + 1) * ratio
            else:
                src = other_indices[oi] * PKT
                result[wi: wi + PKT] = buf[src: src + PKT]
                oi += 1
            wi += PKT

        return bytes(result)

    def _ffmpeg_stdout_reader(self) -> None:
        """Read video-only MPEG-TS from ffmpeg, add audio, broadcast.

        The main muxer handles only video (no audio input) so every
        frame is output immediately with no interleaving delay.  This
        reader:

        1. Replaces the video-only PMT with one that also lists the
           audio PID so downstream players detect both streams.
        2. Injects audio TS packets at wall-clock real-time rate using
           real AAC frames from the audio encoder, or silence fill.
           Audio PTS is wall-clock based (not tied to video frame
           count) so with ``-sync audio`` the player gets a perfectly
           smooth master clock regardless of video delivery jitter.
        """
        PKT = 188
        AUDIO_PID = 0x101
        VIDEO_PID = 0x100
        FRAME_TICKS = self._AAC_FRAME_TICKS  # 5760 (64 ms @ 90 kHz)
        FLUSH_SIZE = 8 * 1024
        FLUSH_INTERVAL = 0.03  # seconds

        buf = bytearray()
        last_flush = time.monotonic()
        audio_cc = 0                       # continuity counter for PID 0x101
        next_audio_pts = -FRAME_TICKS      # seed so first flush injects at PTS 0
        stream_start: float = 0.0          # monotonic time when first video released
        last_video_pts = 0                 # last PTS assigned to a video frame
        _flush_count = 0
        pat_out_cc = 0                     # PAT CC for output
        pmt_out_cc = 0                     # PMT CC for output

        # -- Jitter buffer: absorb network delivery bursts --------
        # Frames are queued on arrival and released at a steady pace
        # so downstream players/recorders see smooth inter-frame gaps
        # instead of burst-then-stall patterns.
        _jitter_q: collections.deque[bytearray] = collections.deque()
        _frame_acc = bytearray()           # partial frame accumulator
        _jitter_primed = False
        _first_video_time = 0.0            # monotonic time when first video queued
        _next_release = 0.0                # monotonic time for next release
        JITTER_DEPTH_S = 2.0               # seconds to buffer before release
        RELEASE_INTERVAL_S = 1.0 / 15.0    # release pace (~67 ms)
        PMT_PID = 0x1000
        PAT_PID = 0x0000

        def _flush() -> None:
            nonlocal buf, last_flush, audio_cc
            nonlocal next_audio_pts, stream_start, last_video_pts, _flush_count
            nonlocal _frame_acc, _jitter_primed, _next_release
            nonlocal pat_out_cc, pmt_out_cc, _first_video_time
            now = time.monotonic()

            # == 1. Ingest: parse new TS into per-frame groups ==
            if buf:
                n_complete = (len(buf) // PKT) * PKT
                if n_complete > 0:
                    chunk = bytes(buf[:n_complete])
                    buf = bytearray(buf[n_complete:])
                    for i in range(n_complete // PKT):
                        off = i * PKT
                        if chunk[off] != 0x47:
                            continue
                        pid = ((chunk[off + 1] & 0x1F) << 8) | chunk[off + 2]
                        if pid in (PMT_PID, PAT_PID):
                            continue       # skip; we regenerate on output
                        if pid == VIDEO_PID:
                            if chunk[off + 1] & 0x40:   # PUSI → new frame
                                if _frame_acc:
                                    _jitter_q.append(bytearray(_frame_acc))
                                _frame_acc = bytearray()
                                if _first_video_time == 0.0:
                                    _first_video_time = now
                            _frame_acc.extend(chunk[off: off + PKT])

            # == 2. Release: pop frames at steady pace ==
            # Prime by time elapsed since first video, not frame count.
            # This handles both 2fps idle keepalive and 15fps live.
            if not _jitter_primed and _first_video_time > 0:
                if now - _first_video_time >= JITTER_DEPTH_S:
                    _jitter_primed = True
                    _next_release = now
                    stream_start = now

            kept = bytearray()
            if _jitter_primed:
                # If release clock fell behind (buffer was empty after a
                # stall), snap forward to prevent burst catch-up.
                if _next_release < now - 2 * RELEASE_INTERVAL_S:
                    _next_release = now
                while _jitter_q and now >= _next_release:
                    frame = _jitter_q.popleft()
                    # PTS from release clock → smooth monotonic pace
                    release_pts = int((_next_release - stream_start) * 90000)
                    current_pts = max(last_video_pts + 900, release_pts)
                    last_video_pts = current_pts
                    # Rewrite PTS/DTS/PCR in every packet of this frame
                    frame_ba = bytearray(frame)
                    for j in range(len(frame_ba) // PKT):
                        poff = j * PKT
                        self._rewrite_video_ts_timing(
                            frame_ba, poff, current_pts,
                        )
                    # Prepend PAT + PMT before each frame group so
                    # go2rtc / RTSP clients can always find the streams.
                    pat_out_cc = (pat_out_cc + 1) & 0x0F
                    pmt_out_cc = (pmt_out_cc + 1) & 0x0F
                    kept.extend(self._build_pat_packet(pat_out_cc))
                    kept.extend(self._build_pmt_packet(pmt_out_cc))
                    kept.extend(frame_ba)
                    _next_release += RELEASE_INTERVAL_S

            # == 3. Audio injection at wall-clock rate ==
            # Audio flows continuously once primed.  During video
            # stalls the jitter buffer empties and video PTS pauses,
            # but audio keeps advancing at wall-clock so the player
            # never hears silence gaps.  When video resumes, the
            # release clock snaps forward and A/V re-sync immediately.
            # No output before primed (prevents audio-only TS
            # confusing go2rtc/RTSP demuxers at startup).
            if _jitter_primed and stream_start > 0.0:
                wall_pts = int((now - stream_start) * 90000)
            else:
                wall_pts = 0

            audio_ts = bytearray()
            injected = 0
            live_audio = self._camera_awake and self._audio_real_started
            if live_audio:
                # Clamp: never inject more than ~5 frames of catch-up
                # silence per flush.  The PTS jump ensures we don't
                # build up unbounded lag when video stalls.
                earliest = wall_pts - 5 * FRAME_TICKS
                if next_audio_pts + FRAME_TICKS < earliest:
                    next_audio_pts = earliest - FRAME_TICKS
            while next_audio_pts + FRAME_TICKS <= wall_pts:
                try:
                    real_frame = self._audio_aac_deque.popleft()
                except IndexError:
                    real_frame = None
                next_audio_pts += FRAME_TICKS
                if real_frame:
                    frame_bytes, audio_cc = self._make_audio_ts(
                        real_frame, next_audio_pts, audio_cc, AUDIO_PID,
                    )
                else:
                    frame_bytes, audio_cc = self._make_silence_audio_ts(
                        next_audio_pts, audio_cc, AUDIO_PID,
                    )
                audio_ts.extend(frame_bytes)
                injected += 1
            if audio_ts:
                kept.extend(audio_ts)

            if not kept:
                last_flush = now
                return

            result = self._reinterleave_ts(bytearray(kept), AUDIO_PID)

            _flush_count += 1
            if _flush_count <= 5 or _flush_count % 1000 == 0:
                _LOGGER.info(
                    "TS flush #%d: %dKB, audio_injected=%d, wall_pts=%d, "
                    "jitter_buf=%d",
                    _flush_count, len(result) // 1024,
                    injected, wall_pts, len(_jitter_q),
                )

            self._broadcast_stream(result)
            last_flush = now

        last_data_time = time.monotonic()
        flush_errors = 0
        try:
            stdout_fd = self._ffmpeg_proc.stdout
            while self._ffmpeg_proc and self._ffmpeg_proc.poll() is None:
                # Non-blocking: wait up to FLUSH_INTERVAL for data.
                # If no video arrives (camera stall), we still call
                # _flush() so audio keeps flowing at wall-clock rate.
                ready, _, _ = select.select(
                    [stdout_fd], [], [], FLUSH_INTERVAL,
                )
                if ready:
                    data = stdout_fd.read1(65536)
                    if not data:
                        break
                    buf.extend(data)
                    last_data_time = time.monotonic()
                now = time.monotonic()
                # Watchdog: kill ffmpeg if no output for 8s while video
                # is still being fed (pacer updates _last_video_time).
                if (
                    now - last_data_time > 8.0
                    and self._last_video_time > 0
                    and now - self._last_video_time < 3.0
                ):
                    _LOGGER.warning(
                        "Muxer output stalled %.1fs while video flows, restarting",
                        now - last_data_time,
                    )
                    try:
                        self._ffmpeg_proc.kill()
                        self._ffmpeg_proc.wait(timeout=2)
                    except Exception:
                        pass
                    break
                if (
                    len(buf) >= FLUSH_SIZE
                    or (now - last_flush) >= FLUSH_INTERVAL
                ):
                    try:
                        _flush()
                    except Exception as exc:
                        flush_errors += 1
                        if flush_errors <= 5:
                            _LOGGER.warning(
                                "TS flush error (%d): %r", flush_errors, exc,
                                exc_info=True,
                            )
                        buf.clear()
                        last_flush = time.monotonic()
            try:
                _flush()
            except Exception:
                pass
        except Exception as e:
            _LOGGER.warning("ffmpeg reader stopped: %s", e)
        # Auto-restart muxer if coordinator is still running.
        # Covers both "ffmpeg exited" and "reader died while ffmpeg
        # was alive" (e.g. stdout pipe full → ffmpeg blocked → stale).
        if self._running:
            if self._ffmpeg_proc and self._ffmpeg_proc.poll() is None:
                _LOGGER.warning(
                    "Reader exited while ffmpeg alive, killing muxer"
                )
                try:
                    self._ffmpeg_proc.kill()
                    self._ffmpeg_proc.wait(timeout=2)
                except Exception:
                    pass
            if (
                self._ffmpeg_proc is None
                or self._ffmpeg_proc.poll() is not None
            ):
                _LOGGER.warning("ffmpeg muxer exited, restarting")
                # Clear our thread ref so _stop_ffmpeg_muxer won't
                # deadlock trying to join us.
                self._ffmpeg_reader_thread = None
                self._stop_ffmpeg_muxer()
                # Drain stale video so the new muxer starts fresh.
                while not self._video_queue.empty():
                    try:
                        self._video_queue.get_nowait()
                    except queue.Empty:
                        break
                time.sleep(1)
                self._start_ffmpeg_muxer()

    @staticmethod
    def _log_ffmpeg_stderr(proc: subprocess.Popen, label: str) -> None:
        """Log ffmpeg stderr lines so errors are visible in HA logs."""
        try:
            for raw_line in proc.stderr:
                line = raw_line.decode(errors="replace").rstrip()
                if line:
                    _LOGGER.info("ffmpeg[%s]: %s", label, line)
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
        """Write queued video frames to ffmpeg stdin with bounded writes.

        Uses select() to check pipe writability before each os.write()
        so the pacer never blocks indefinitely when ffmpeg is stuck.
        The stdin pipe stays in blocking mode (required by ffmpeg), but
        select() timeout prevents permanent hangs.  If the pipe stays
        full for >3 seconds, the current frame is dropped.
        """
        try:
            stdin_fd = proc_ref.stdin.fileno()
        except (ValueError, OSError):
            return

        while self._ffmpeg_proc is proc_ref and proc_ref.poll() is None:
            try:
                data = self._video_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            pending = memoryview(data)
            stall_start: float | None = None
            while pending:
                if self._ffmpeg_proc is not proc_ref or proc_ref.poll() is not None:
                    return
                try:
                    _, writable, _ = select.select([], [stdin_fd], [], 0.5)
                except (ValueError, OSError):
                    return
                if writable:
                    try:
                        written = os.write(stdin_fd, pending)
                        if written <= 0:
                            return
                        pending = pending[written:]
                        stall_start = None
                    except (BrokenPipeError, OSError):
                        return
                else:
                    if stall_start is None:
                        stall_start = time.monotonic()
                    elif time.monotonic() - stall_start > 3.0:
                        _LOGGER.debug(
                            "Video pacer: stdin pipe full for 3s, dropping frame",
                        )
                        break  # drop frame after 3s stall
            self._last_video_time = time.monotonic()
            self._muxer_video_started.set()

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
                # During live P2P, do NOT inject gap-fill keyframes.
                # Gap-fill causes "old frames between new ones" because
                # duplicate keyframes get monotonic PTS from the muxer,
                # making the player show stale frozen content interleaved
                # with real video.  Instead, let the player freeze
                # naturally on the last decoded frame during stalls.
                time.sleep(0.2)
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

    def _silence_feeder(self, gen: int) -> None:
        """Feed µ-law silence to the audio encoder to keep it producing AAC.

        Phase 1 — Primer: feed 3 chunks to bootstrap the encoder.
        Phase 2 — Gap fill: keep running for the muxer lifetime.
           When no camera audio has arrived for >150 ms AND the
           AAC output deque is nearly empty, feed silence so the
           encoder produces natural AAC frames instead of the
           TS-level injector having to use pre-baked dead-silence
           AAC frames (which cause audible hard cuts).
           ``_queue_audio`` already drops silence when the queue
           is backlogged, so this never competes with real audio.
        """
        _LOGGER.info("Silence feeder: waiting for video start")
        self._muxer_video_started.wait()
        if gen != self._silence_feeder_gen:
            _LOGGER.info("Silence feeder: superseded by newer feeder, exiting")
            return
        _LOGGER.info("Silence feeder: video started, priming ffmpeg audio")

        PRIMER_CHUNKS = 3  # 300 ms of silence at 8 kHz
        SILENCE_CHUNK = b"\xff" * 800  # 100 ms at 8 kHz
        AUDIO_DRY_THRESHOLD = 0.12  # seconds with no camera audio
        DEQUE_LOW = 5  # only feed when deque is nearly empty

        for i in range(PRIMER_CHUNKS):
            if self._ffmpeg_proc is None or self._ffmpeg_proc.poll() is not None:
                break
            self._queue_audio(SILENCE_CHUNK)
            if i == 0:
                _LOGGER.info("Silence feeder: primed %d/%d", i + 1, PRIMER_CHUNKS)
            time.sleep(0.1)

        _LOGGER.info("Silence feeder: primer done, entering gap-fill mode")

        # Phase 2: keep encoder fed during camera audio stalls
        while (
            gen == self._silence_feeder_gen
            and self._ffmpeg_proc is not None
            and self._ffmpeg_proc.poll() is None
        ):
            time.sleep(0.04)
            now = time.monotonic()
            audio_age = now - self._last_p2p_audio_time if self._last_p2p_audio_time > 0 else 999.0
            if (
                audio_age > AUDIO_DRY_THRESHOLD
                and len(self._audio_aac_deque) < DEQUE_LOW
                and self._audio_queue.qsize() < 4
            ):
                self._queue_audio(SILENCE_CHUNK)

        _LOGGER.info("Silence feeder: exiting (gen=%d)", gen)

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
            if queued >= max(10, self._audio_soft_queue_limit // 2) and (
                now_mono + 1.5 < self._audio_realtime_next_ts
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

        if is_silence and self._audio_queue.qsize() >= max(6, self._audio_soft_queue_limit // 3):
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
        """Drain queued audio to ffmpeg at approximately real-time pace.

        Rate-limiting here keeps the AAC encoder output aligned with
        wall-clock consumption in ``_flush()``.  Without pacing, KCP
        burst recovery delivers many audio chunks at once, which the
        encoder converts instantly.  The resulting deque overshoot
        causes either frame eviction (stutter) or growing latency
        followed by a bulk trim (audible gap).

        Each 800-byte µ-law chunk ≈ 100 ms at 8 kHz.  We track a
        virtual clock and only write the next chunk when that clock
        says it's time.  A small lead (``MAX_LEAD``) absorbs jitter
        without letting the pipeline run ahead of real-time.
        """
        pending = memoryview(b"")
        total_written = 0
        write_count = 0
        next_write_time = 0.0  # monotonic; when the next chunk *should* be written
        MAX_LEAD = 2.5         # allow encoder to run ~2.5 s ahead of wall-clock
        while self._ffmpeg_proc is proc_ref and proc_ref.poll() is None:
            if not pending:
                try:
                    chunk = self._audio_queue.get(timeout=0.2)
                except queue.Empty:
                    continue
                if not chunk:
                    continue
                # Rate-limit: wait until our virtual clock allows the next write
                now = time.monotonic()
                if next_write_time <= 0.0:
                    next_write_time = now  # first chunk: start the clock
                wait = next_write_time - now - MAX_LEAD
                if wait > 0.001:
                    time.sleep(wait)
                chunk_secs = len(chunk) / 8000.0
                next_write_time = max(time.monotonic(), next_write_time) + chunk_secs
                pending = memoryview(chunk)
            try:
                written = os.write(self._audio_write_fd, pending)
                if written <= 0:
                    raise OSError("audio pipe write returned no bytes")
                pending = pending[written:]
                total_written += written
                write_count += 1
                if write_count in (1, 10, 50, 100, 500):
                    _LOGGER.info("Audio writer: %d writes, %d bytes total", write_count, total_written)
            except BlockingIOError:
                # Pipe is full; briefly back off and retry the same bytes.
                time.sleep(0.002)
            except (BrokenPipeError, OSError) as e:
                _LOGGER.info("Audio writer: pipe error after %d bytes: %s", total_written, e)
                break
        _LOGGER.info("Audio writer: exited, total %d bytes written", total_written)

    def _audio_aac_reader(self) -> None:
        """Read raw AAC ADTS frames from the audio encoder and queue them.

        The audio encoder outputs ADTS-wrapped AAC-LC (``-f adts``) to
        its stdout.  Each frame starts with the 0xFFF sync word; the
        frame length is embedded in the 7-byte ADTS header (bytes 3-5).
        We parse complete frames and put them in ``_audio_aac_deque``
        for ``_flush()`` to pick up.
        """
        ADTS_HEADER_SIZE = 7
        proc = self._audio_enc_proc
        if proc is None:
            return
        buf = bytearray()
        frame_count = 0
        try:
            while proc.poll() is None:
                data = proc.stdout.read(4096)
                if not data:
                    break
                buf.extend(data)
                # Extract complete ADTS frames (variable length)
                while len(buf) >= ADTS_HEADER_SIZE:
                    # Find ADTS sync word (0xFFF)
                    sync_pos = -1
                    for j in range(len(buf) - 1):
                        if buf[j] == 0xFF and (buf[j + 1] & 0xF0) == 0xF0:
                            sync_pos = j
                            break
                    if sync_pos < 0:
                        buf.clear()
                        break
                    if sync_pos > 0:
                        del buf[:sync_pos]
                    if len(buf) < ADTS_HEADER_SIZE:
                        break
                    # Frame length from ADTS header bytes 3-5 (13-bit field)
                    frame_len = (
                        (buf[3] & 0x03) << 11
                        | buf[4] << 3
                        | (buf[5] >> 5) & 0x07
                    )
                    if frame_len < ADTS_HEADER_SIZE:
                        # Corrupt header; skip this sync
                        del buf[:1]
                        continue
                    if len(buf) < frame_len:
                        break
                    frame = bytes(buf[:frame_len])
                    del buf[:frame_len]
                    self._audio_aac_deque.append(frame)
                    frame_count += 1
                    if frame_count in (1, 10, 100):
                        _LOGGER.info(
                            "Audio encoder: %d AAC frames queued", frame_count
                        )
        except Exception as e:
            _LOGGER.debug("Audio AAC reader stopped: %s", e)
        _LOGGER.info("Audio AAC reader exited, %d frames total", frame_count)

    def _feed_audio(self, data: bytes) -> None:
        """Write G.711 µ-law audio data to ffmpeg audio input."""
        if self._audio_write_fd >= 0:
            self._audio_primed.set()  # mark that camera audio has been seen
            if not self._audio_real_started:
                self._audio_real_started = True
                self._audio_realtime_next_ts = time.monotonic()
                # Don't drain queued silence — let it flow through to keep
                # audio PTS continuous.  The silence feeder runs for the
                # entire muxer lifetime, filling gaps naturally.
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
        # Start at 15 fps; the cadence tracker will converge to the actual
        # camera rate regardless of codec.
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

        # Dynamic bounds: let the EMA converge to whatever the camera sends.
        min_fps, max_fps = 5.0, 60.0

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
                        and not self._muxer_video_started.is_set()
                    ):
                        # Prime mux output immediately after geometry discovery
                        # so local players do not wait for the next real IDR.
                        # Only when muxer hasn't started yet — during live
                        # streaming this would inject a stale frame.
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
        # Muxer start is deferred until the first real video keyframe
        # arrives so ffmpeg probes against live data with the correct
        # codec format.  Starting it eagerly here would use a stale
        # default codec (hevc) and no video bytes, causing the probe to
        # time out and the muxer to exit immediately with code 255.
        self._generate_black_keyframe()
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
                    # Only inject a bootstrap black frame when the muxer
                    # has not started yet.  During live P2P, skip it —
                    # the real keyframe is imminent and injecting a stale
                    # black frame causes "old frame flash" artifacts.
                    if not self._muxer_video_started.is_set():
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
                    # Lazily start the muxer on the first real keyframe so
                    # ffmpeg probes against live data with the correct codec.
                    if self._ffmpeg_proc is None or self._ffmpeg_proc.poll() is not None:
                        # Pre-set latest keyframe so the bootstrap inside
                        # _start_ffmpeg_muxer can feed it to stdin immediately.
                        kf_data = self._prepend_parameter_sets_if_needed(
                            data, codec, self._collect_nal_types(data, codec),
                        )
                        self._latest_video_kf = bytes(kf_data)
                        self._generate_black_keyframe(codec)
                        self._start_ffmpeg_muxer()
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
            vvp_quality=self._vvp_quality,
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
                    # Restart silence feeder so audio keeps flowing during
                    # the gap between P2P sessions.  Without this, the muxer
                    # accumulates video-only TS packets while audio is silent.
                    self._audio_real_started = False
                    self._audio_primed.clear()
                    self._silence_feeder_gen += 1
                    threading.Thread(
                        target=self._silence_feeder, args=(self._silence_feeder_gen,), daemon=True,
                    ).start()
                    time.sleep(0.8)
                    self._begin_streaming(api, grab_only=False)
                else:
                    threading.Thread(
                        target=self._build_idle_scene_keyframe,
                        daemon=True,
                    ).start()
                    # Restart silence feeder so idle audio keeps flowing.
                    self._audio_primed.clear()
                    self._silence_feeder_gen += 1
                    threading.Thread(
                        target=self._silence_feeder, args=(self._silence_feeder_gen,), daemon=True,
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

        # Initial state poll
        self._poll_status()

        # Optional startup frame grab (enabled by default for HA integration).
        # Dev/test tooling can disable this to reduce startup-to-live latency.
        if not self._is_snap:
            # IPC cameras: start live streaming immediately (no idle mode).
            _LOGGER.info(
                "Starting continuous live stream for IPC camera %s",
                self._sn_num,
            )
            self._live_stream_requested = True
            self._begin_streaming(api)
        elif self._initial_frame_grab:
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
        last_status_poll = time.time()
        motion_deadline = 0.0  # Not streaming yet

        try:
            while self._running:
                now = time.time()

                if self._is_snap:
                    # --- Battery camera: wake / motion / timeout logic ---

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
                    if now - last_battery_poll >= BATTERY_POLL_INTERVAL:
                        self._poll_battery()
                        last_battery_poll = now

                else:
                    # --- IPC camera: keep live stream running ---
                    stream_alive = bool(self._stream_thread and self._stream_thread.is_alive())
                    if not stream_alive:
                        _LOGGER.info("IPC stream not alive, restarting for %s", self._sn_num)
                        self._stream_thread = None
                        self._live_stream_requested = True
                        self._begin_streaming(api)

                # Periodic lamp state poll (all camera types)
                if now - last_status_poll >= BATTERY_POLL_INTERVAL:
                    self._poll_status()
                    last_status_poll = now

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
        """Poll battery info, re-login once if the call fails."""
        if not self._is_snap:
            return
        if not self._api:
            return
        for attempt in range(2):
            try:
                info = self._api.get_battery_info(self._sn_num)
                if not info:
                    if attempt == 0:
                        _LOGGER.debug(
                            "Battery poll returned empty for %s, retrying after re-login",
                            self._sn_num,
                        )
                        self._api.login()
                        continue
                    return

                pct = info.get(BATTERY_PERCENT)
                charge = info.get(CHARGE_STATUS)

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
                return  # success
            except Exception as e:
                if attempt == 0:
                    _LOGGER.debug(
                        "Battery poll failed for %s (%s), retrying after re-login",
                        self._sn_num, e,
                    )
                    try:
                        self._api.login()
                    except Exception:
                        _LOGGER.warning("Battery poll re-login failed for %s", self._sn_num)
                        return
                else:
                    _LOGGER.warning("Battery poll failed for %s: %s", self._sn_num, e)

    def _poll_status(self) -> None:
        """Poll status from the device IoT config."""
        if not self._api:
            return
        try:
            iot = self._api.get_device_iot_config(self._sn_num)
            if not iot:
                return

            need_update = False
            new_iot_data = {}
            for k, v in iot.items():
                try:
                    new_iot_data[int(k)] = v
                except (ValueError, TypeError):
                    new_iot_data[k] = v

            if new_iot_data != self._iot_data:
                self._iot_data = new_iot_data
                need_update = True

            if need_update:
                self._fire_update()

        except Exception as e:
            _LOGGER.debug("Status poll failed for %s: %s", self._sn_num, e)

    def get_iot_value(self, code: int) -> Any:
        """Get a value from the cached IOT data."""
        return self._iot_data.get(code)

    def set_iot_value(self, code: int, value: Any) -> None:
        """Set an IOT value via the API and update cache."""
        if not self._api:
            return
        try:
            # Most values are integers, but some might be strings or JSON strings.
            self._api.set_device_iot_value(self._sn_num, str(code), value)
            self._iot_data[code] = int(value) == 1
            self._fire_update()
        except Exception as e:
            _LOGGER.warning("Set IOT value %s failed for %s: %s", code, self._sn_num, e)

    def ptz_move(self, direction: str) -> None:
        """Start PTZ movement in the given direction (left/right/up/down)."""
        if not self._has_ptz:
            _LOGGER.warning("PTZ not supported on %s", self._sn_num)
            return
        if not self._api:
            return
        try:
            self._api.ptz_start(
                self._sn_num, direction, use_ptz2=self._has_ptz2,
            )
            _LOGGER.info("PTZ move %s on %s", direction, self._sn_num)
        except Exception as e:
            _LOGGER.warning("PTZ move failed for %s: %s", self._sn_num, e)

    def ptz_stop(self) -> None:
        """Stop any ongoing PTZ movement."""
        if not self._has_ptz:
            return
        if not self._api:
            return
        try:
            self._api.ptz_stop(self._sn_num, use_ptz2=self._has_ptz2)
            _LOGGER.info("PTZ stop on %s", self._sn_num)
        except Exception as e:
            _LOGGER.warning("PTZ stop failed for %s: %s", self._sn_num, e)
