"""Coordinator for CloudEdge / Meari camera — manages lifecycle.

Runs MQTT listener, P2P streaming, ffmpeg muxer, TCP stream server,
and idle stream in background threads. Manages camera wake/sleep state
and motion detection.

Sub-modules:

    mpegts.py  — MPEG-TS packet building, PTS/PCR parsing, IDR seed utils
    nal.py     — Annex-B NAL parsing, codec detection, keyframe detection
    stream.py  — TCP stream server, backlog bootstrap, join diagnostics
    video.py   — codec switching, cadence tracking, snapshots, IDR validation
    p2p.py     — P2P session start/stop and restart handling
    lifecycle.py — MQTT, wake control, and session/watch loop
    media.py   — ffmpeg muxer/audio pipeline and gap-skip recovery
"""

from __future__ import annotations

import collections
import logging
import queue
import socket
import subprocess
import threading
import time
from typing import Any, Callable, TYPE_CHECKING

from homeassistant.core import HomeAssistant

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry

from ..const import (
    DEFAULT_APP_PROFILE,
    DEFAULT_COUNTRY_CODE,
    DEFAULT_MOTION_TIMEOUT,
    DEFAULT_PHONE_CODE,
    DOMAIN,
)
from ..api import MeariApiClient, format_sn
from ..p2p_streamer import P2PStreamer
from .stream import CoordinatorStreamMixin
from .p2p import CoordinatorP2PMixin
from .video import CoordinatorVideoMixin
from .lifecycle import CoordinatorLifecycleMixin
from .media import CoordinatorMediaMixin

_LOGGER = logging.getLogger(__name__)

# Polling intervals
STATUS_POLL_INTERVAL = 60.0
BATTERY_POLL_INTERVAL = 300.0


class CloudEdgeMeariCoordinator(
    CoordinatorMediaMixin,
    CoordinatorStreamMixin,
    CoordinatorVideoMixin,
    CoordinatorP2PMixin,
    CoordinatorLifecycleMixin,
):
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
        self._has_lamp = self._capabilities.get("led") == 1
        self._has_ptz = (
            self._capabilities.get("ptz") == 1 or self._capabilities.get("ptz2") == 1
        )
        self._has_ptz2 = self._capabilities.get("ptz2") == 1
        self._motion_timeout = DEFAULT_MOTION_TIMEOUT
        self._initial_frame_grab = initial_frame_grab
        self._initial_grab_timeout = max(3, min(initial_grab_timeout, 45))
        self._snapshot_conversion_enabled = bool(snapshot_conversion_enabled)

        # Shared state
        self._latest_image: bytes | None = None
        self._latest_video_kf: bytes | None = (
            None  # raw Annex-B keyframe for deferred JPEG conversion
        )
        self._idle_scene_kf: bytes | None = (
            None  # cached last scene keyframe for idle keepalive
        )
        self._idle_video_kf: bytes | None = (
            None  # lightweight keyframe used for long idle keepalive
        )
        # Backward-compatible alias used by older debug tooling.
        self._latest_hevc_kf: bytes | None = None
        self._video_codec: str = "hevc"  # "hevc" or "h264"
        self._idle_since: float = time.monotonic()
        self._idle_scene_hold_seconds: float = 3.0
        self._idle_keepalive_fps_initial: float = 5.0
        self._idle_keepalive_fps_steady: float = 2.0
        self._idle_keepalive_fps_with_clients: float = 2.0
        self._idle_keepalive_settle_seconds: float = 3.0
        self._live_gap_fill_after_seconds: float = 0.3
        self._live_gap_fill_fps: float = 30.0
        self._audio_gain_db: float = 24.0
        self._last_p2p_video_time: float = (
            0.0  # monotonic timestamp of last camera video frame
        )
        self._last_p2p_audio_time: float = (
            0.0  # monotonic timestamp of last camera audio frame
        )
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

        # Lamp state
        self._lamp_on: bool = False

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
        self._stream_clients: list[
            tuple[socket.socket, collections.deque, threading.Event]
        ] = []
        self._pending_stream_clients: collections.deque[
            tuple[socket.socket, collections.deque, threading.Event, float, str]
        ] = collections.deque()
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
        self._stream_idr_seed_pts: int = 0  # video PTS when IDR seed was captured
        self._stream_idr_seed_mono: float = 0.0
        self._stream_idr_seed_p2p_frame_index: int = 0
        self._stream_idr_seed_video_bytes: int = 0
        self._stream_idr_seed_is_strong: bool = False
        self._stream_idr_seed_strength_reason: str = ""
        self._stream_idr_seed_generation: int = 0
        self._startup_safe_min_seed_generation: int = 0
        self._stream_idr_collecting: bool = False
        self._stream_idr_buf: bytearray = bytearray()
        self._stream_broadcast_video_pts: int = (
            0  # latest video PTS in broadcast output
        )
        self._stream_broadcast_audio_pts: int = (
            0  # latest audio PTS in broadcast output
        )
        self._stream_pid_last_cc: dict[int, int] = {}
        self._gap_skip_reset_seq: int = 0
        self._p2p_session_generation: int = 0
        self._gap_skip_event_counter: int = 0
        self._last_gap_skip_event_id: int = 0
        self._last_gap_skip_diag: dict[str, Any] | None = None
        self._gap_skip_seq_to_event_id: dict[int, int] = {}
        self._gap_skip_events: collections.deque[dict[str, Any]] = collections.deque(
            maxlen=32
        )
        self._gap_skip_events_lock = threading.Lock()
        self._recent_video_rai_ts_sizes: collections.deque[int] = collections.deque(
            maxlen=12
        )
        self._recovery_decode_probe_cache: collections.OrderedDict[
            str, tuple[bool, str]
        ] = collections.OrderedDict()
        self._recovery_decode_probe_cache_max: int = 96
        self._stream_join_event_counter: int = 0
        self._stream_join_events: collections.deque[dict[str, Any]] = collections.deque(
            maxlen=16
        )
        self._stream_join_events_by_client: dict[int, dict[str, Any]] = {}
        self._seed_probe_reject_streak: int = 0
        self._seed_probe_first_reject_mono: float = 0.0
        self._stream_clients_lock = threading.Lock()
        self._stream_accept_thread: threading.Thread | None = None
        self._stream_epoch: float = 0.0  # wall-clock anchor for MPEG-TS timestamps

        # ffmpeg muxer (H264/HEVC + G.711 µ-law → MPEG-TS)
        self._ffmpeg_proc: subprocess.Popen | None = None
        self._ffmpeg_reader_thread: threading.Thread | None = None
        self._audio_write_fd: int = -1
        self._audio_enc_proc: subprocess.Popen | None = None
        self._audio_aac_deque: collections.deque = (
            collections.deque()
        )  # unbounded; trimmed explicitly
        self._audio_aac_overflow: int = 0  # frames trimmed from deque front
        self._audio_silence_drops: int = 0  # silence frames dropped in _queue_audio
        self._audio_flush_silence: int = (
            0  # silence TS injected because deque was empty
        )
        self._audio_flush_real: int = 0  # real AAC frames injected from deque
        self._audio_primed = threading.Event()  # set when real camera audio arrives
        self._muxer_video_started = (
            threading.Event()
        )  # set after first video write to ffmpeg
        self._silence_feeder_gen: int = 0  # generation counter to cancel stale feeders
        self._audio_writer_thread: threading.Thread | None = None
        self._audio_real_started: bool = False
        # Keep audio buffering shallow and realtime-friendly.
        self._audio_queue: queue.Queue = queue.Queue(maxsize=80)
        self._audio_queue_drops: int = 0
        self._audio_throttle_drops: int = 0
        self._audio_realtime_next_ts: float = 0.0
        self._audio_soft_queue_limit: int = 60

        # Video write queue to decouple P2P receive from ffmpeg stdin backpressure.
        self._video_queue: queue.Queue = queue.Queue(maxsize=150)
        self._video_lag_drop_threshold: int = 8
        self._video_queue_drops: int = 0
        self._force_param_sets_on_next_keyframe: bool = False
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
        self._session_rebootstrap_requested: bool = False
        self._consecutive_live_bootstrap_failures: int = 0
        self._last_rebootstrap_mono: float = 0.0
        # Default to adaptive recovery instead of unconditional lossy skip.
        # This preserves continuity on small losses while still allowing
        # escalation under sustained degradation.
        self._p2p_allow_lossy_gap_skip: bool = False
        # Adaptive lossy mode ramps recovery aggressiveness from observed
        # live-link behavior without forcing deepest skip from frame 1.
        self._p2p_adaptive_lossy_gap_skip: bool = True

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
            self._motion_timeout = opts.get(
                f"{sn}_motion_timeout", DEFAULT_MOTION_TIMEOUT
            )
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
            pct = info.get("154")
            charge = info.get("156")
            if pct is not None:
                pct_int = int(pct)
                if 0 <= pct_int <= 100:
                    self._battery_percent = pct_int
            if charge is not None:
                self._battery_charging = int(charge) == 1
        except Exception as exc:
            _LOGGER.warning("Prefetch battery failed for %s: %s", self._sn_num, exc)

    def prefetch_lamp(self, api: "MeariApiClient") -> None:
        """Pre-load lamp state using an already-authenticated API client."""
        if not self._has_lamp:
            return
        try:
            iot = api.get_device_iot_config(self._sn_num)
            if not iot:
                return
            lamp_val = iot.get("167")
            if lamp_val is not None:
                self._lamp_on = int(lamp_val) == 1
        except Exception as exc:
            _LOGGER.warning("Prefetch lamp failed for %s: %s", self._sn_num, exc)

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
    def has_lamp(self) -> bool:
        return self._has_lamp

    @property
    def lamp_on(self) -> bool:
        return self._lamp_on

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
                self._entry,
                options=new_opts,
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
            self._motion_timeout,
            self._sn_num,
        )
        self._fire_update()

    def set_stream_host_mode(self, mode: str) -> None:
        """Set stream host mode: 'ip' or 'docker'."""
        self._stream_host_mode = mode
        self._persist_option("stream_host_mode", mode)
        _LOGGER.info(
            "Stream host mode set to %s for %s",
            mode,
            self._sn_num,
        )
        self._fire_update()

    @property
    def quality_profiles(self) -> dict[int, str]:
        """Return available quality profiles ``{id: label}`` from device caps."""
        from ..p2p_streamer import parse_quality_profiles

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
                return  # success
            except Exception as e:
                if attempt == 0:
                    _LOGGER.debug(
                        "Battery poll failed for %s (%s), retrying after re-login",
                        self._sn_num,
                        e,
                    )
                    try:
                        self._api.login()
                    except Exception:
                        _LOGGER.warning(
                            "Battery poll re-login failed for %s", self._sn_num
                        )
                        return
                else:
                    _LOGGER.warning("Battery poll failed for %s: %s", self._sn_num, e)

    def _poll_lamp(self) -> None:
        """Poll lamp state from the device IoT config."""
        if not self._has_lamp:
            return
        if not self._api:
            return
        try:
            iot = self._api.get_device_iot_config(self._sn_num)
            if not iot:
                return
            lamp_val = iot.get("167")
            if lamp_val is not None:
                is_on = int(lamp_val) == 1
                if self._lamp_on != is_on:
                    self._lamp_on = is_on
                    self._fire_update()
        except Exception as e:
            _LOGGER.debug("Lamp poll failed for %s: %s", self._sn_num, e)

    def set_lamp(self, on: bool) -> None:
        """Turn the lamp on or off via the API."""
        if not self._has_lamp:
            return
        if not self._api:
            return
        try:
            self._api.set_device_iot_value(self._sn_num, "167", 1 if on else 0)
            self._lamp_on = on
            self._fire_update()
        except Exception as e:
            _LOGGER.warning("Lamp set failed for %s: %s", self._sn_num, e)

    def ptz_move(self, direction: str) -> None:
        """Start PTZ movement in the given direction (left/right/up/down)."""
        if not self._has_ptz:
            _LOGGER.warning("PTZ not supported on %s", self._sn_num)
            return
        if not self._api:
            return
        try:
            self._api.ptz_start(
                self._sn_num,
                direction,
                use_ptz2=self._has_ptz2,
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
