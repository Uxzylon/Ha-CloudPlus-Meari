"""LAN-specific P2P helpers."""

from __future__ import annotations

import json
import os
from typing import Any

from ..msgsvr_codec import MAGIC, NODE_CLIENT, TAIL

LAN_CONNECT_METHOD = 0xB2
LAN_CONNECT_CMD = 0xC3
LAN_CONNECT_TYPE = 0xD1


def build_lan_connect_frame() -> bytes:
    """Build the native plaintext MsgSvr LAN connect punch."""
    sid = os.urandom(4).hex() + "00000001"
    payload = json.dumps({"sid": sid}, separators=(",", ":")).encode()
    size = len(payload)
    header = bytes(
        (
            MAGIC,
            NODE_CLIENT,
            LAN_CONNECT_METHOD,
            LAN_CONNECT_CMD,
            LAN_CONNECT_TYPE,
            MAGIC,
            size & 0xFF,
            (size >> 8) & 0x7F,
        )
    )
    return header + payload + bytes((sum(payload) & 0xFF, TAIL))


def host_candidate_endpoints(candidates: list[dict[str, Any]]) -> list[tuple[str, int]]:
    """Return unique host-candidate endpoints suitable for direct LAN punching."""
    endpoints: list[tuple[str, int]] = []
    for cand in candidates:
        if str(cand.get("type") or "").lower() != "host":
            continue
        ip = str(cand.get("ip") or "").strip()
        try:
            port = int(cand.get("port") or 0)
        except (TypeError, ValueError):
            continue
        endpoint = (ip, port)
        if ip and port and endpoint not in endpoints:
            endpoints.append(endpoint)
    return endpoints
