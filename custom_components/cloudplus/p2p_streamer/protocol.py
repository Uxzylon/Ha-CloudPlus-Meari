"""VVP packet helpers and stream frame constants."""

from __future__ import annotations

import hashlib
import struct

from .quality import best_quality_profile

VVP_MAGIC = 0x56565099
VVP_CMD_START_LIVE = 0x11FF
VVP_CMD_STOP = 0x12FF
VVP_CMD_HEARTBEAT = 0x888E
VVP_HEADER_SIZE = 60

STREAM_TYPE_INFO = 0xF9
STREAM_TYPE_AUDIO = 0xFA
STREAM_TYPE_IFRAME = 0xFC
STREAM_TYPE_PFRAME = 0xFD
STREAM_TYPE_PHOTO = 0xFE

STREAM_ENCRYPT_KEY = b"!mearicloud2.0!"


def format_licence_id(sn: str) -> str:
    if not sn:
        return ""
    if len(sn) == 9:
        return "00000000000" + sn
    return sn


def build_vvp_auth_md5(
    host_key: str,
    seq: int,
    cmd: int,
    param: int,
    licence_id: str | None = None,
) -> str:
    password = host_key if len(host_key) > 32 else host_key[:16]
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
    *,
    param: int = 8,
    channel: int = 0,
    stream_id: int = 0,
    stream_flag: int = 0,
    reserve: int = 0,
    licence_id: str | None = None,
) -> bytes:
    auth = build_vvp_auth_md5(host_key, seq, cmd, param, licence_id)
    pkt = bytearray(60)
    struct.pack_into(">I", pkt, 0x00, VVP_MAGIC)
    struct.pack_into(">I", pkt, 0x04, 1)
    struct.pack_into(">I", pkt, 0x08, seq)
    struct.pack_into(">I", pkt, 0x0C, cmd)
    pkt[0x10:0x30] = auth.encode("ascii")
    struct.pack_into(">I", pkt, 0x30, param)
    struct.pack_into("<I", pkt, 0x34, channel)
    pkt[0x38] = stream_id & 0xFF
    pkt[0x39] = stream_flag & 0xFF
    pkt[0x3A] = reserve & 0xFF
    pkt[0x3B] = 0x00
    return bytes(pkt)


def _best_quality_from_device(device: dict) -> int:
    try:
        return int(best_quality_profile(device))
    except (ValueError, TypeError, KeyError):
        return 0
