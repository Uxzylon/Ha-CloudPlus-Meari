"""MQTT motion listener for Meari cloud events."""

from __future__ import annotations

import json
import logging
import ssl
import threading
import time
from typing import Callable

import paho.mqtt.client as mqtt

from ..api import MeariApiClient
from ..motion_event import parse_motion_event

_LOGGER = logging.getLogger(__name__)
ALARM_POLL_INTERVAL = 15.0
ALARM_POLL_ERROR_INTERVAL = 60.0
# Cap on remembered event ids so the dedup set can't grow without bound.
_SEEN_ALARM_CAP = 4000


def _event_topics(api: MeariApiClient) -> list[str]:
    user_id = str(api.user_id or "").strip()
    if not user_id:
        return []

    prefixes = [user_id]
    client_id = str(getattr(api, "client_id", "") or "").strip()
    if client_id and client_id != user_id:
        prefixes.insert(0, client_id)

    topics = [
        f"$bsssvr/iot/{prefix}/{user_id}/event/update/accepted" for prefix in prefixes
    ]
    return list(dict.fromkeys(topics))


class MotionEventListener:
    """Account-scoped Meari MQTT listener that dispatches camera motion events."""

    def __init__(self, api: MeariApiClient) -> None:
        self._api = api
        self._client = None
        self._callbacks: list[tuple[str, str, Callable[[str], None]]] = []
        self._lock = threading.Lock()
        self._stop_poll = threading.Event()
        self._poll_thread: threading.Thread | None = None
        self._seen_alarm_keys: set[tuple[str, str]] = set()
        self._poll_day: str = ""

    @property
    def has_callbacks(self) -> bool:
        with self._lock:
            return bool(self._callbacks)

    def register(
        self,
        device_id: int,
        sn_num: str,
        on_motion: Callable[[str], None],
    ) -> Callable[[], None]:
        item = (str(device_id), str(sn_num), on_motion)
        with self._lock:
            self._callbacks.append(item)

        def unsubscribe() -> None:
            with self._lock:
                try:
                    self._callbacks.remove(item)
                except ValueError:
                    pass

        return unsubscribe

    def start(self) -> None:
        self._start_alarm_poll()
        if self._client is not None:
            return
        if not self._api.mqtt_host:
            return

        user_id = str(self._api.user_id or "").strip()
        topics = _event_topics(self._api)
        if not user_id or not topics:
            return

        def on_connect(client, _userdata, _flags, rc, *_args):
            if rc == 0:
                for topic in topics:
                    client.subscribe(topic, qos=2)
                _LOGGER.debug(
                    "MQTT motion listener subscribed to %s",
                    ", ".join(topics),
                )
            else:
                _LOGGER.warning("MQTT motion listener connect failed: rc=%s", rc)

        def on_disconnect(_client, _userdata, _flags, rc, *_args):
            if rc:
                _LOGGER.debug("MQTT motion listener disconnected: rc=%s", rc)

        def on_message(_client, _userdata, msg):
            self._handle_payload(msg.payload)

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

        self._client = client
        try:
            client.connect_async(
                self._api.mqtt_host, self._api.mqtt_port, keepalive=300
            )
            client.loop_start()
        except (OSError, ValueError) as exc:
            _LOGGER.warning("MQTT motion listener failed to start: %s", exc)
            self._client = None
            try:
                client.disconnect()
            except OSError:
                pass

    def stop(self) -> None:
        self._stop_alarm_poll()
        client = self._client
        self._client = None
        if client is None:
            return
        try:
            client.loop_stop()
            client.disconnect()
        except OSError:
            pass

    def _start_alarm_poll(self) -> None:
        if self._poll_thread is not None and self._poll_thread.is_alive():
            return
        self._stop_poll.clear()
        self._poll_thread = threading.Thread(target=self._alarm_poll_loop, daemon=True)
        self._poll_thread.start()

    def _stop_alarm_poll(self) -> None:
        self._stop_poll.set()
        if self._poll_thread is not None:
            self._poll_thread.join(timeout=2)
            self._poll_thread = None

    def _alarm_poll_loop(self) -> None:
        # The first pass seeds the "already seen" set without dispatching, so
        # pre-existing events don't fire motion at startup. This is the primary
        # delivery path whenever the live MQTT push is unavailable — most often
        # because the broker requires clientId == userID, so a phone logged in
        # to the same account keeps evicting our MQTT session.
        seeded = False
        while not self._stop_poll.is_set():
            try:
                self._poll_device_events(dispatch=seeded)
                seeded = True
                wait_s = ALARM_POLL_INTERVAL
            except (OSError, RuntimeError, ValueError, KeyError) as exc:
                _LOGGER.debug("Alarm event poll failed: %s", exc)
                wait_s = ALARM_POLL_ERROR_INTERVAL
            self._stop_poll.wait(wait_s)

    def _poll_device_events(self, dispatch: bool) -> None:
        """Poll each registered device's event log and dispatch new motions."""
        day = time.strftime("%Y%m%d")
        if day != self._poll_day:
            # Day rolled over: yesterday's ids won't reappear in today's log, so
            # drop them. New events on the fresh day still dispatch normally.
            self._poll_day = day
            self._seen_alarm_keys.clear()

        with self._lock:
            devices = {(dev_id, sn) for dev_id, sn, _cb in self._callbacks}

        for device_id, _sn in devices:
            try:
                events = self._api.get_device_events(device_id, day)
            except (OSError, RuntimeError, ValueError, KeyError) as exc:
                _LOGGER.debug("Event list poll failed for %s: %s", device_id, exc)
                continue
            for raw_event in events:
                if not isinstance(raw_event, dict):
                    continue
                msg_id = str(
                    raw_event.get("msgID")
                    or raw_event.get("msgId")
                    or raw_event.get("eventTime")
                    or ""
                ).strip()
                if not msg_id:
                    continue
                key = (str(device_id), msg_id)
                if key in self._seen_alarm_keys:
                    continue
                self._seen_alarm_keys.add(key)
                if dispatch:
                    self._handle_payload(json.dumps(raw_event).encode())

        if len(self._seen_alarm_keys) > _SEEN_ALARM_CAP:
            # Keep the set bounded; trimming the oldest is fine because only
            # events newer than the last poll can ever be dispatched.
            self._seen_alarm_keys = set(list(self._seen_alarm_keys)[-_SEEN_ALARM_CAP:])

    def _matching_callbacks(
        self, device_id: str, license_id: str
    ) -> list[Callable[[str], None]]:
        with self._lock:
            callbacks = list(self._callbacks)
        out: list[Callable[[str], None]] = []
        for registered_device_id, registered_sn, callback in callbacks:
            if device_id and device_id == registered_device_id:
                out.append(callback)
            elif not device_id and license_id and license_id == registered_sn:
                out.append(callback)
        return out

    def _handle_payload(self, payload: bytes) -> None:
        try:
            event = parse_motion_event(payload)
        except (ValueError, KeyError, TypeError) as exc:
            _LOGGER.debug("MQTT motion payload parse failed: %s", exc)
            return
        if not event or not event["is_motion"]:
            return

        device_id = event["device_id"]
        license_id = event["license_id"]
        if not device_id and not license_id:
            return

        motion_type = str(event["evt_name"])
        for callback in self._matching_callbacks(device_id, license_id):
            callback(motion_type)
