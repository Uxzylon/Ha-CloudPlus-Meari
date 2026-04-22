"""Network utilities: local IP detection, signaling server resolution, ICE helpers."""

from __future__ import annotations

import os
import socket
import struct

from ..turn_client import (
    _build_stun,
    _encode_attr,
    _add_integrity,
    _encode_xor_address,
    _decode_xor_address,
    BINDING_REQUEST,
    BINDING_RESPONSE,
    ATTR_USERNAME,
    ATTR_XOR_MAPPED_ADDRESS,
    ATTR_MESSAGE_INTEGRITY,
    MAGIC_COOKIE,
)


# ---------------------------------------------------------------------------
# Local IP detection
# ---------------------------------------------------------------------------


def _is_private_ip(ip: str) -> bool:
    try:
        parts = ip.split(".")
        if parts[0] == "10":
            return True
        if parts[0] == "172" and 16 <= int(parts[1]) <= 31:
            return True
        if parts[0] == "192" and parts[1] == "168":
            return True
        if parts[0] == "127":
            return True
    except (IndexError, ValueError):
        pass
    return False


def _get_local_ips() -> list[str]:
    ips: list[str] = []
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ip = info[4][0]
            if ip not in ips and not ip.startswith("127."):
                ips.append(ip)
    except Exception:
        pass
    if not ips:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ips.append(s.getsockname()[0])
            s.close()
        except Exception:
            ips.append("0.0.0.0")
    return ips


# ---------------------------------------------------------------------------
# Signaling server resolution
# ---------------------------------------------------------------------------


def _resolve_signaling_server() -> tuple[str, int]:
    candidates = [("47.254.142.96", 28974)]
    domain = "euce.mearicloud.com"
    try:
        ip = socket.gethostbyname(domain)
        candidates.append((ip, 28974))
        candidates.append((ip, 9253))
    except socket.gaierror:
        pass
    for ip, port in candidates:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2.0)
            s.connect((ip, port))
            s.close()
            return ip, port
        except (socket.timeout, ConnectionRefusedError, OSError):
            continue
    return "47.254.142.96", 28974


# ---------------------------------------------------------------------------
# ICE / STUN helpers
# ---------------------------------------------------------------------------


def _build_ice_response(
    binding_req: dict, local_ice_pwd: str, peer_ip: str, peer_port: int
) -> bytes:
    txn_id = binding_req["txn_id"]
    ice_key = local_ice_pwd.encode()
    mapped = _encode_xor_address(peer_ip, peer_port)
    attrs = _encode_attr(ATTR_XOR_MAPPED_ADDRESS, mapped)
    attrs = _add_integrity(BINDING_RESPONSE, attrs, txn_id, ice_key)
    msg, _ = _build_stun(BINDING_RESPONSE, attrs, txn_id)
    return msg


def _send_direct_ice_binding(
    sock,
    peer_ip: str,
    peer_port: int,
    local_ufrag: str,
    remote_ufrag: str,
    remote_pwd: str,
) -> None:
    username = f"{remote_ufrag}:{local_ufrag}"
    attrs = _encode_attr(ATTR_USERNAME, username.encode())
    attrs += _encode_attr(0x0024, struct.pack(">I", 1862270975))
    attrs += _encode_attr(
        0x802A, struct.pack(">Q", int.from_bytes(os.urandom(8), "big"))
    )
    attrs += _encode_attr(0x0025, b"")
    txn_id = os.urandom(12)
    ice_key = remote_pwd.encode()
    attrs = _add_integrity(BINDING_REQUEST, attrs, txn_id, ice_key)
    msg, _ = _build_stun(BINDING_REQUEST, attrs, txn_id)
    sock.sendto(msg, (peer_ip, peer_port))
