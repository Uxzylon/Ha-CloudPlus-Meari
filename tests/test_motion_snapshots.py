"""Offline regression tests using the existing standalone integration loader."""

from __future__ import annotations

import hashlib
import importlib
import json
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from debug_tools.bootstrap import _bootstrap_integration_modules

MODULES = _bootstrap_integration_modules()
MOTION = importlib.import_module("custom_components.cloudplus.coordinator.motion")
COORDINATOR = MODULES["coordinator"].CloudEdgeMeariCoordinator
# Synthetic protocol bytes; no camera captures, credentials or device identifiers.
JPEG = b"\xff\xd8\xff" + bytes(range(256)) * 8
SERIAL = "synthetic-camera"
IMAGE_URL = "https://example.invalid/alarm.jpgx3"


def obfuscate(data: bytes, serial: str = SERIAL) -> bytes:
    """Build a synthetic Meari payload including its unchanged suffix."""
    material = f"{serial}|{len(serial)}|meari.stream".encode()
    key = hashlib.md5(material, usedforsecurity=False).hexdigest().encode()
    return bytes(value ^ key[index % len(key)] for index, value in
                 enumerate(data[:1024])) + data[1024:]


class MotionSnapshotTests(unittest.TestCase):
    """Exercise parsing, camera routing and backward-compatible failures."""

    def setUp(self):
        self.api = Mock()
        self.api.download_snapshot.return_value = obfuscate(JPEG)
        self.listener = MOTION.MotionEventListener(self.api)
        self.received = Mock()
        self.other = Mock()
        self.listener.register(42, SERIAL, self.received)
        self.listener.register(43, "other-camera", self.other)

    def dispatch(self, **overrides):
        event = {"deviceID": "42", "eventType": 2, "imageUrl": IMAGE_URL}
        event.update(overrides)
        self.listener._handle_payload(json.dumps({"params": event}).encode())

    def test_plain_jpeg_is_unchanged(self):
        self.assertEqual(MOTION._decode_event_snapshot(JPEG, SERIAL), JPEG)

    def test_obfuscated_image_preserves_suffix(self):
        self.assertEqual(MOTION._decode_event_snapshot(obfuscate(JPEG), SERIAL), JPEG)

    def test_wrong_camera_and_invalid_data_are_rejected(self):
        for data, serial in [(obfuscate(JPEG), "wrong-camera"),
                             (b"", SERIAL), (b"invalid", SERIAL),
                             (obfuscate(JPEG), "")]:
            with self.subTest(serial=serial, size=len(data)):
                self.assertIsNone(MOTION._decode_event_snapshot(data, serial))

    def test_nested_url_and_invalid_scheme(self):
        self.assertEqual(MOTION._find_image_url(
            {"items": [{"data": {"picUrl": IMAGE_URL}}]}), IMAGE_URL)
        self.assertEqual(MOTION._find_image_url({"imageUrl": "file:///tmp/a"}), "")

    def test_event_photo_only_reaches_owning_camera(self):
        self.dispatch()
        self.received.assert_called_once_with("Motion", JPEG)
        self.other.assert_not_called()

    def test_serial_only_routing(self):
        self.dispatch(deviceID="", licenseID=SERIAL)
        self.received.assert_called_once_with("Motion", JPEG)
        self.other.assert_not_called()

    def test_missing_image_still_delivers_motion_without_extra_lookup(self):
        self.dispatch(imageUrl="")
        self.received.assert_called_once_with("Motion", None)
        self.api.download_snapshot.assert_not_called()
        self.api.get_device_events.assert_not_called()

    def test_failed_download_still_delivers_motion(self):
        self.api.download_snapshot.return_value = None
        self.dispatch()
        self.received.assert_called_once_with("Motion", None)

    def test_invalid_image_still_delivers_motion(self):
        self.api.download_snapshot.return_value = b"invalid"
        self.dispatch()
        self.received.assert_called_once_with("Motion", None)

    def test_non_motion_does_not_download(self):
        self.dispatch(eventType=21)
        self.api.download_snapshot.assert_not_called()
        self.received.assert_not_called()

    def test_poll_startup_seeds_without_notifying_then_delivers_new_image(self):
        old = {"deviceID": "42", "eventType": 2, "msgID": "old", "imageUrl": IMAGE_URL}
        self.api.get_device_events.side_effect = lambda device, _day: [old] if device == "42" else []
        self.api.get_new_device_events.return_value = []
        self.listener._poll_device_events(dispatch=False)
        self.received.assert_not_called()
        self.api.download_snapshot.assert_not_called()
        self.api.get_device_events.side_effect = (
            lambda device, _day: [old, dict(old, msgID="new")] if device == "42" else []
        )
        self.listener._poll_device_events(dispatch=True)
        self.listener._poll_device_events(dispatch=True)
        self.received.assert_called_once_with("Motion", JPEG)


class SnapshotCacheTests(unittest.TestCase):
    """Check event publication order and a conversion already in progress."""

    def setUp(self):
        self.coord = SimpleNamespace(
            _latest_image=None, _latest_image_source="", _latest_image_generation=0,
            _latest_image_updated_at=0.0, _motion_detected=False, _is_snap=False,
            _fire_update=Mock(), _set_motion=Mock(), _video_to_jpeg=Mock(return_value=JPEG),
            _snapshot_convert_lock=threading.Lock(),
        )

    def test_event_image_is_available_when_motion_is_published(self):
        def check_image(*_args):
            self.assertEqual(self.coord._latest_image, JPEG)
            self.assertEqual(self.coord._latest_image_source, "event")
            self.assertGreater(self.coord._latest_image_updated_at, 0)
        self.coord._set_motion.side_effect = check_image
        COORDINATOR._note_motion(self.coord, "Motion", JPEG)
        self.coord._set_motion.assert_called_once_with(True, "Motion")
        self.assertEqual(self.coord._latest_image_generation, 1)

    def test_missing_image_does_not_claim_new_freshness(self):
        COORDINATOR._note_motion(self.coord, "Motion")
        self.assertEqual(self.coord._latest_image_updated_at, 0)
        self.assertEqual(self.coord._latest_image_source, "")
        self.coord._set_motion.assert_called_once_with(True, "Motion")

    def test_inflight_video_conversion_cannot_replace_active_event_image(self):
        def convert(_codec, _payload):
            COORDINATOR._note_motion(self.coord, "Motion", b"event-photo")
            self.coord._motion_detected = True
            return JPEG
        self.coord._video_to_jpeg.side_effect = convert
        self.coord._snapshot_convert_lock.acquire()
        COORDINATOR._convert_snapshot(self.coord, "h264", b"frame")
        self.assertEqual(self.coord._latest_image, b"event-photo")
        self.assertEqual(self.coord._latest_image_source, "event")
        self.assertFalse(self.coord._snapshot_convert_lock.locked())

    def test_live_conversion_resumes_after_motion(self):
        self.coord._latest_image_source = "event"
        self.coord._snapshot_convert_lock.acquire()
        COORDINATOR._convert_snapshot(self.coord, "h264", b"frame")
        self.assertEqual(self.coord._latest_image, JPEG)
        self.assertEqual(self.coord._latest_image_source, "live")
        self.assertFalse(self.coord._snapshot_convert_lock.locked())


if __name__ == "__main__":
    unittest.main()
