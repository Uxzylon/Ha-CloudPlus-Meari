"""Lifecycle, MQTT, and session-loop helpers for the coordinator."""

from __future__ import annotations

import logging
import ssl
import time

from ..api import MeariApiClient
from ..motion_event import parse_motion_event

_LOGGER = logging.getLogger(__name__)
BATTERY_POLL_INTERVAL = 300.0


class CoordinatorLifecycleMixin:
    """Owns MQTT setup, wake control, and the session/watch loop."""

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
            except Exception as exc:
                _LOGGER.debug("MQTT message error: %s", exc)

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
                self._api.mqtt_host,
                self._api.mqtt_port,
                keepalive=300,
            )
            client.loop_start()
        except Exception as exc:
            _LOGGER.warning("MQTT connection failed: %s", exc)
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
            return

        evt_name = parsed["evt_name"]
        is_motion = parsed["is_motion"]

        _LOGGER.info(
            "MQTT event: %s (device=%s, license=%s, motion=%s)",
            evt_name,
            device_id_str or "?",
            license_id or "?",
            is_motion,
        )

        if is_motion:
            self._motion_detected = True
            self._motion_type = evt_name
            self._last_motion_time = time.time()
            self._fire_motion()
            self._fire_update()

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
        except Exception as exc:
            _LOGGER.error("Wake failed: %s", exc)

    def _watch_loop(self) -> None:
        """Main event loop: login, listen for events, stream on motion."""
        _LOGGER.debug("Watch loop starting for %s", self._sn_num)

        while self._running:
            try:
                self._run_session()
            except Exception as exc:
                _LOGGER.error("Session error: %s", exc)

            if self._running:
                self._available = False
                self._fire_update()
                if self._session_rebootstrap_requested:
                    self._session_rebootstrap_requested = False
                    backoff = 3
                    _LOGGER.warning(
                        "Forcing full session rebootstrap for %s (reconnect in %ss)",
                        self._sn_num,
                        backoff,
                    )
                else:
                    backoff = 30
                    _LOGGER.info("Reconnecting in %ss...", backoff)
                for _ in range(backoff):
                    if not self._running:
                        return
                    time.sleep(1)

        self._available = False
        self._fire_update()

    def _run_session(self) -> None:
        """Single session: login -> MQTT -> grab initial frame -> event loop."""
        self._startup_ready.clear()
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
        except Exception as exc:
            _LOGGER.error("Login failed: %s", exc)
            return

        self._api = api
        self._available = True
        self._fire_update()
        self._start_mqtt()
        self._refresh_video_encryption_state()
        if self._video_password:
            _LOGGER.info(
                "Using configured video password for %s stream auth (device encryption flag=%s)",
                self._sn_num,
                self._video_encryption_enabled,
            )

        if self._is_snap:
            self._poll_battery()
        if self._has_lamp:
            self._poll_lamp()

        if not self._is_snap:
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
            if self._stream_thread:
                self._stream_thread.join(timeout=self._initial_grab_timeout)
                if self._stream_thread.is_alive():
                    _LOGGER.warning(
                        "Initial frame grab timed out after %ss for %s, continuing startup",
                        self._initial_grab_timeout,
                        self._sn_num,
                    )
                    teardown_deadline = time.time() + 18
                    while (
                        self._stream_thread
                        and self._stream_thread.is_alive()
                        and time.time() < teardown_deadline
                    ):
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

        if not (
            self._stream_thread
            and self._stream_thread.is_alive()
            and self._stream_grab_only
        ):
            self._startup_ready.set()

        _LOGGER.info("Connected and listening for %s", self._sn_num)

        last_battery_poll = time.time()
        last_lamp_poll = time.time()
        motion_deadline = 0.0

        try:
            while self._running:
                now = time.time()

                if self._session_rebootstrap_requested:
                    _LOGGER.warning(
                        "Session rebootstrap requested for %s, ending current session loop",
                        self._sn_num,
                    )
                    return

                if self._is_snap:
                    if self._wake_event.is_set():
                        self._wake_event.clear()
                        self._live_stream_requested = True
                        self._camera_awake = True
                        self._idle_since = 0.0
                        self._fire_update()
                        motion_deadline = now + self._motion_timeout

                        stream_alive = bool(
                            self._stream_thread and self._stream_thread.is_alive()
                        )
                        stale_live = False
                        if stream_alive and not self._stream_grab_only:
                            if (
                                self._last_p2p_video_time <= 0
                                and self._last_p2p_audio_time <= 0
                            ):
                                stale_live = False
                            else:
                                now_mono = time.monotonic()
                                video_age = (
                                    now_mono - self._last_p2p_video_time
                                    if self._last_p2p_video_time > 0
                                    else 999.0
                                )
                                audio_age = (
                                    now_mono - self._last_p2p_audio_time
                                    if self._last_p2p_audio_time > 0
                                    else 999.0
                                )
                                stale_live = video_age > 20.0 and audio_age > 12.0

                        if stream_alive and (self._stream_grab_only or stale_live):
                            if stale_live:
                                _LOGGER.warning(
                                    "Manual wake: preempting stale stream for %s",
                                    self._sn_num,
                                )
                            if self._p2p_streamer:
                                self._p2p_streamer.request_stop()
                            if self._stream_thread:
                                self._stream_thread.join(timeout=4)
                            stream_alive = bool(
                                self._stream_thread and self._stream_thread.is_alive()
                            )
                            if not stream_alive:
                                self._stream_thread = None

                        if not stream_alive:
                            self._do_wake(api)
                            self._begin_streaming(api)

                    if self._motion_detected and self._motion_wake_enabled:
                        self._live_stream_requested = True
                        motion_deadline = max(
                            motion_deadline,
                            self._last_motion_time + self._motion_timeout,
                        )
                        if not self._camera_awake:
                            self._camera_awake = True
                            self._idle_since = 0.0
                            self._fire_update()
                            self._do_wake(api)
                            self._begin_streaming(api)

                    if self._camera_awake and now > motion_deadline:
                        _LOGGER.info("Stream timeout for %s, going idle", self._sn_num)
                        self._end_streaming()
                        self._motion_detected = False
                        self._motion_type = ""
                        self._fire_update()

                    if now - last_battery_poll >= BATTERY_POLL_INTERVAL:
                        self._poll_battery()
                        last_battery_poll = now

                else:
                    stream_alive = bool(
                        self._stream_thread and self._stream_thread.is_alive()
                    )
                    if not stream_alive:
                        _LOGGER.info(
                            "IPC stream not alive, restarting for %s", self._sn_num
                        )
                        self._stream_thread = None
                        self._live_stream_requested = True
                        self._begin_streaming(api)

                if self._has_lamp and now - last_lamp_poll >= BATTERY_POLL_INTERVAL:
                    self._poll_lamp()
                    last_lamp_poll = now

                time.sleep(1)

        finally:
            self._end_streaming()
            if self._stream_thread:
                self._stream_thread.join(timeout=15)
                self._stream_thread = None
            self._stop_mqtt()
            self._api = None
