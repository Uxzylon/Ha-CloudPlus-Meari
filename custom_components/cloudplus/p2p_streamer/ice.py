"""ICE/STUN helpers used by the minimal streamer session."""

from __future__ import annotations

import os
import struct

from ..turn_client import (
    ATTR_USERNAME,
    ATTR_XOR_MAPPED_ADDRESS,
    BINDING_REQUEST,
    BINDING_RESPONSE,
    _add_integrity,
    _build_stun,
    _encode_attr,
)


def build_ice_response(
    binding_request: dict, local_ice_pwd: str, peer_ip: str, peer_port: int
) -> bytes:
    """Build a STUN binding response for an incoming ICE check."""
    txn_id = binding_request["txn_id"]
    attrs = _encode_attr(
        ATTR_XOR_MAPPED_ADDRESS, _encode_xor_address(peer_ip, peer_port)
    )
    attrs = _add_integrity(BINDING_RESPONSE, attrs, txn_id, local_ice_pwd.encode())
    msg, _ = _build_stun(BINDING_RESPONSE, attrs, txn_id)
    return msg


def build_ice_binding_request(
    *,
    local_ufrag: str,
    remote_ufrag: str,
    remote_pwd: str,
    use_candidate: bool = True,
) -> bytes:
    """Build an ICE STUN binding request payload."""
    username = f"{remote_ufrag}:{local_ufrag}"
    attrs = _encode_attr(ATTR_USERNAME, username.encode())
    attrs += _encode_attr(0x0024, struct.pack(">I", 1862270975))
    attrs += _encode_attr(
        0x802A, struct.pack(">Q", int.from_bytes(os.urandom(8), "big"))
    )
    if use_candidate:
        attrs += _encode_attr(0x0025, b"")
    txn_id = os.urandom(12)
    attrs = _add_integrity(BINDING_REQUEST, attrs, txn_id, remote_pwd.encode())
    msg, _ = _build_stun(BINDING_REQUEST, attrs, txn_id)
    return msg


def _encode_xor_address(ip: str, port: int) -> bytes:
    # Kept local to avoid importing internal helper from turn_client.
    import socket

    magic_cookie = 0x2112A442
    magic_bytes = struct.pack(">I", magic_cookie)
    xor_port = port ^ (magic_cookie >> 16)
    ip_bytes = socket.inet_aton(ip)
    xor_ip = bytes(a ^ b for a, b in zip(ip_bytes, magic_bytes))
    return struct.pack(">BBH", 0, 1, xor_port) + xor_ip
