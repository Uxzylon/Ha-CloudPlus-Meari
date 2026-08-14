"""Small app-style keepalive helpers for P2P viewing sessions."""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

_LOGGER = logging.getLogger(__name__)

SNAP_KEEPALIVE_CODES = ("1007", "1010", "1013")
SNAP_KEEPALIVE_INTERVAL_S = 10.0


class SnapDeviceKeepalive:
    """Poll the same lightweight IoT values the official app reads while live."""

    def __init__(
        self,
        api: Any,
        sn_num: str,
        *,
        enabled: bool,
        interval_s: float = SNAP_KEEPALIVE_INTERVAL_S,
    ) -> None:
        self._api = api
        self._sn_num = sn_num
        self._enabled = enabled
        self._interval_s = interval_s
        self._next_at = 0.0
        self._busy = False
        self._lock = threading.Lock()

    def tick(self, now: float | None = None) -> None:
        """Schedule one non-blocking keepalive poll when due."""
        if not self._enabled:
            return
        now = time.monotonic() if now is None else now
        with self._lock:
            if self._busy or now < self._next_at:
                return
            self._busy = True
            self._next_at = now + self._interval_s
        threading.Thread(
            target=self._poll,
            name=f"cloudplus_snap_keepalive_{self._sn_num}",
            daemon=True,
        ).start()

    def _poll(self) -> None:
        try:
            fetch = getattr(self._api, "get_device_iot_values", None)
            if fetch is None:
                return
            fetch(self._sn_num, SNAP_KEEPALIVE_CODES)
        except (OSError, RuntimeError, ValueError, KeyError):
            _LOGGER.debug("Snap live keepalive failed", exc_info=True)
        finally:
            with self._lock:
                self._busy = False
