"""MQTT motion listener for Meari cloud events."""

from __future__ import annotations

import logging
import ssl
from typing import Callable

from ..api import MeariApiClient
from ..motion_event import parse_motion_event

_LOGGER = logging.getLogger(__name__)


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
    """Subscribe to cloud motion events and dispatch events for one camera."""

    def __init__(
        self,
        api: MeariApiClient,
        device_id: int,
        sn_num: str,
        on_motion: Callable[[str], None],
    ) -> None:
        self._api = api
        self._device_id = str(device_id)
        self._sn_num = sn_num
        self._on_motion = on_motion
        self._client = None

    def start(self) -> None:
        if not self._api.mqtt_host:
            return
        try:
            import paho.mqtt.client as mqtt
        except ImportError:
            _LOGGER.warning("paho-mqtt is not installed; motion events disabled")
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
        except Exception as exc:
            _LOGGER.warning("MQTT motion listener failed to start: %s", exc)
            self.stop()

    def stop(self) -> None:
        client = self._client
        self._client = None
        if client is None:
            return
        try:
            client.loop_stop()
            client.disconnect()
        except Exception:
            pass

    def _handle_payload(self, payload: bytes) -> None:
        try:
            event = parse_motion_event(payload)
        except Exception as exc:
            _LOGGER.debug("MQTT motion payload parse failed: %s", exc)
            return
        if not event or not event["is_motion"]:
            return

        device_id = event["device_id"]
        license_id = event["license_id"]
        if device_id and device_id != self._device_id:
            return
        if not device_id and license_id and license_id != self._sn_num:
            return
        if not device_id and not license_id:
            return

        self._on_motion(str(event["evt_name"]))
