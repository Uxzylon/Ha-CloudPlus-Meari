"""Shared P2P session constants, identity helpers, and the cluster-miss error.

Split out of ``engine.py`` so the streaming loop and the orchestration class can
share these without an import cycle.
"""

from __future__ import annotations

import hashlib
import os
import threading
from typing import Any

from ..kcp_tunnel import parse_iva_frame

IVA_HEARTBEAT_S = 3.0
VVP_HEARTBEAT_S = 10.0
AUTH_FALLBACK_NO_VIDEO_S = 10.0
# Cold-wake of a deeply asleep snap/battery camera is camera-side and highly
# variable — measured anywhere from ~8 s to ~57 s. Keep the single-session wake
# budget above the observed worst case so a slow wake completes in one session
# instead of timing out and restarting (a restart can miss the camera's
# `online` push in the teardown gap). Watchdogs hold off for the whole window
# via `P2PStreamer.awaiting_wake`.
DORMANCY_WAKE_TIMEOUT_S = 75.0
WAKE_RETRY_S = 4.0
AUTH_FALLBACK_RESULT = (-1, -1)
SIGNALING_CONNECT_TIMEOUT_S = 5.0
CLIENT_KEYFRAME_REQUEST_DEBOUNCE_S = 4.0
_CLIENT_SESSION_LOCK = threading.Lock()


class SignalingClusterMiss(RuntimeError):
    """The MsgSvr endpoint is reachable but does not know this device."""


def _unwrap_iva_payload(data: bytes) -> bytes:
    if len(data) >= 20 and data[0:2] == b"\xff\x01":
        iva = parse_iva_frame(data)
        if iva:
            type_marker, _, _, payload = iva
            if type_marker == 0x7012:
                return b""
            return payload
    return data


def _client_uuid_for(api: Any, client_id: str | None = None) -> str:
    override = str(os.environ.get("CLOUDPLUS_CLIENT_UUID") or "").strip().lower()
    if len(override) == 16 and all(ch in "0123456789abcdef" for ch in override):
        return override
    identity = str(client_id or getattr(api, "user_id", "") or "").strip()
    cache = getattr(api, "_p2p_client_uuids", None)
    if not isinstance(cache, dict):
        cache = {}
        setattr(api, "_p2p_client_uuids", cache)
    existing = str(cache.get(identity) or "")
    if existing:
        return existing
    seed = "|".join(
        (
            "cloudplus-p2p",
            str(getattr(api, "app_profile", "") or ""),
            identity,
        )
    )
    value = hashlib.md5(seed.encode()).hexdigest()[:16]
    cache[identity] = value
    return value


def _next_session_index(api: Any) -> int:
    with _CLIENT_SESSION_LOCK:
        current = int(getattr(api, "_p2p_session_index", 0) or 0)
        index = current + 1 if current < 99 else 1
        setattr(api, "_p2p_session_index", index)
        return index


def _client_id_for(api: Any, device: dict[str, Any], override: Any = None) -> str:
    for value in (
        override,
        device.get("_iot_client_id"),
        device.get("iotClientId"),
        device.get("clientId"),
        getattr(api, "user_id", None),
    ):
        text = str(value or "").strip()
        if text:
            return text
    return "0"
