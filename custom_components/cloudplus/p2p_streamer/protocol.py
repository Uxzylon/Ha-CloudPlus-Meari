"""VVP (PPStrong Video Protocol) constants, packet builders, and format helpers."""

from __future__ import annotations

import hashlib
import json
import struct
from typing import Any

# ---------------------------------------------------------------------------
# VVP protocol constants
# ---------------------------------------------------------------------------
VVP_MAGIC = 0x56565099
VVP_CMD_START_LIVE = 0x11FF
VVP_CMD_STOP = 0x0001
VVP_CMD_HEARTBEAT = 0x888E
VVP_HEADER_SIZE = 60

# Camera proprietary stream frame types
STREAM_TYPE_INFO = 0xF9
STREAM_TYPE_AUDIO = 0xFA
STREAM_TYPE_IFRAME = 0xFC
STREAM_TYPE_PFRAME = 0xFD
STREAM_TYPE_PHOTO = 0xFE

# 3DES key for stream decryption
STREAM_ENCRYPT_KEY = b"!mearicloud2.0!"


# ---------------------------------------------------------------------------
# Device / session helpers
# ---------------------------------------------------------------------------


def format_licence_id(sn: str) -> str:
    if not sn:
        return ""
    if len(sn) == 9:
        return "00000000000" + sn
    return sn


def parse_quality_profiles(device: dict[str, Any]) -> dict[int, str]:
    """Return ``{profile_id: label}`` from the device ``bps2`` capability.

    Example: ``{0: "640x360", 1: "2304x1296@1Mbps", 2: "2304x1296@2Mbps"}``.
    Returns an empty dict when no capability information is available.
    """
    try:
        cap_raw = device.get("capability", "")
        if isinstance(cap_raw, str):
            cap = json.loads(cap_raw) if cap_raw else {}
        else:
            cap = cap_raw
        caps = cap.get("caps", {})
        bps2_raw = caps.get("bps2", "")
        if isinstance(bps2_raw, str):
            bps2 = json.loads(bps2_raw) if bps2_raw else {}
        else:
            bps2 = bps2_raw
        if not bps2:
            return {}
        return {int(k): str(v) for k, v in bps2.items()}
    except Exception:
        return {}


def _best_quality_from_device(device: dict[str, Any]) -> int:
    """Pick the highest quality profile from the device capability ``bps2``.

    ``bps2`` is a JSON-encoded dict whose keys are quality profile IDs like
    ``{"0": "640x360", "1": "2304x1296@1Mbps", "2": "2304x1296@2Mbps"}``.
    We want the highest numeric key that has the best resolution/bitrate.
    Returns 0 when no capability information is available (camera default).
    """
    profiles = parse_quality_profiles(device)
    if not profiles:
        return 0
    return max(profiles.keys())


# ---------------------------------------------------------------------------
# VVP packet building
# ---------------------------------------------------------------------------


def build_vvp_auth_md5(
    host_key: str,
    seq: int,
    cmd: int,
    param: int,
    licence_id: str | None = None,
    auth_flag: int = 0,
) -> str:
    # Legacy cameras expect the first 16 chars of hostKey.
    # E2EE mode extends auth material to "video_password + hostKey", which
    # must be used in full
    password = host_key if (auth_flag == 1 or len(host_key) > 32) else host_key[:16]
    parts = [
        "admin",
        password,
        str(VVP_MAGIC),
        str(seq),
        str(cmd),
        str(param),
        "meari.p2p.ppcs",
    ]
    if licence_id:
        parts.append(licence_id)
    return hashlib.md5("|".join(parts).encode()).hexdigest()


def build_vvp_packet(
    cmd: int,
    seq: int,
    host_key: str,
    param: int = 8,
    channel: int = 0,
    video_id: int = 0,
    quality: int = 0,
    licence_id: str | None = None,
    auth_flag: int = 0,
) -> bytes:
    auth = build_vvp_auth_md5(host_key, seq, cmd, param, licence_id, auth_flag)
    pkt = bytearray(VVP_HEADER_SIZE)
    struct.pack_into(">I", pkt, 0x00, VVP_MAGIC)
    struct.pack_into(">I", pkt, 0x04, 1)
    struct.pack_into(">I", pkt, 0x08, seq)
    struct.pack_into(">I", pkt, 0x0C, cmd)
    pkt[0x10:0x30] = auth.encode("ascii")
    struct.pack_into(">I", pkt, 0x30, param)
    struct.pack_into("<I", pkt, 0x34, channel)
    pkt[0x38] = video_id & 0xFF
    pkt[0x39] = 0x01
    pkt[0x3A] = quality & 0xFF
    pkt[0x3B] = 0x00
    return bytes(pkt)
