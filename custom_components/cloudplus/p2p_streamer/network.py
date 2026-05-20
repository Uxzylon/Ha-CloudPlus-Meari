"""Network utilities: local IP detection, signaling server resolution, ICE helpers."""

from __future__ import annotations

import logging
import os
import socket
import struct
import time
from dataclasses import dataclass
from urllib.parse import urlparse

from ..turn_client import (
    _build_stun,
    _encode_attr,
    _add_integrity,
    _encode_xor_address,
    _decode_xor_address,
    _parse_stun,
    BINDING_REQUEST,
    BINDING_RESPONSE,
    DATA_INDICATION,
    ATTR_USERNAME,
    ATTR_XOR_MAPPED_ADDRESS,
    ATTR_DATA,
    ATTR_XOR_PEER_ADDRESS,
)
from .root_discovery import discover_msgsvr_endpoints

_LOGGER = logging.getLogger(__name__)

# Keep successful signaling endpoint for a short period to avoid repeated
# connect-probe storms during reconnect loops.
_SIG_CACHE_TTL_S = 180.0
_SIG_CACHE: dict[str, tuple[tuple[str, int], float]] = {}


@dataclass(slots=True)
class PeerPacket:
    """One UDP packet with TURN framing removed when present."""

    data: bytes
    source: tuple[str, int]
    peer: tuple[str, int] | None
    via_turn: bool
    stun: dict | None = None


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


def _extract_host(value: str | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if "://" in raw:
        try:
            host = urlparse(raw).hostname or ""
            return host.strip().lower()
        except Exception:
            return ""
    return raw.lower()


def _signaling_cache_key(
    platform_domain_hint: str | None,
    openapi_server_hint: str | None,
    api_server_hint: str | None = None,
    client_id_hint: str | int | None = None,
) -> str:
    platform = _extract_host(platform_domain_hint)
    openapi = _extract_host(openapi_server_hint)
    api = _extract_host(api_server_hint)
    return f"{platform}|{openapi}|{api}|{client_id_hint or ''}"


def _resolve_signaling_server(
    *,
    platform_domain_hint: str | None = None,
    openapi_server_hint: str | None = None,
    api_server_hint: str | None = None,
    client_id_hint: str | int | None = None,
    timeout_s: float = 0.7,
) -> tuple[str, int]:
    candidates = _resolve_signaling_server_candidates(
        platform_domain_hint=platform_domain_hint,
        openapi_server_hint=openapi_server_hint,
        api_server_hint=api_server_hint,
        client_id_hint=client_id_hint,
        timeout_s=timeout_s,
    )
    if candidates:
        return candidates[0]
    raise RuntimeError("No signaling endpoints discovered")


def _cache_signaling_endpoint(
    endpoint: tuple[str, int] | None,
    *,
    platform_domain_hint: str | None = None,
    openapi_server_hint: str | None = None,
    api_server_hint: str | None = None,
    client_id_hint: str | int | None = None,
    ttl_s: float = _SIG_CACHE_TTL_S,
) -> None:
    if endpoint is None:
        return
    cache_key = _signaling_cache_key(
        platform_domain_hint,
        openapi_server_hint,
        api_server_hint,
        client_id_hint,
    )
    _SIG_CACHE[cache_key] = (endpoint, time.monotonic() + ttl_s)


def _forget_signaling_endpoint(
    endpoint: tuple[str, int] | None,
    *,
    platform_domain_hint: str | None = None,
    openapi_server_hint: str | None = None,
    api_server_hint: str | None = None,
    client_id_hint: str | int | None = None,
) -> None:
    if endpoint is None:
        return
    cache_key = _signaling_cache_key(
        platform_domain_hint,
        openapi_server_hint,
        api_server_hint,
        client_id_hint,
    )
    cached_entry = _SIG_CACHE.get(cache_key)
    if cached_entry and cached_entry[0] == endpoint:
        _SIG_CACHE.pop(cache_key, None)


def _resolve_signaling_server_candidates(
    *,
    platform_domain_hint: str | None = None,
    openapi_server_hint: str | None = None,
    api_server_hint: str | None = None,
    client_id_hint: str | int | None = None,
    timeout_s: float = 0.7,
) -> list[tuple[str, int]]:
    now = time.monotonic()
    cache_key = _signaling_cache_key(
        platform_domain_hint,
        openapi_server_hint,
        api_server_hint,
        client_id_hint,
    )
    cached_entry = _SIG_CACHE.get(cache_key)
    cached = None
    if cached_entry and now < cached_entry[1]:
        cached = cached_entry[0]
    elif cached_entry:
        _SIG_CACHE.pop(cache_key, None)

    env_host = _extract_host(os.environ.get("CLOUDPLUS_SIGNALING_HOST"))
    env_port = str(os.environ.get("CLOUDPLUS_SIGNALING_PORT") or "").strip()
    if env_host and env_port.isdigit():
        pinned = (env_host, int(env_port))
        _SIG_CACHE[cache_key] = (pinned, now + _SIG_CACHE_TTL_S)
        return [pinned]

    root_endpoints: list[tuple[str, int]] = []
    if cached is None:
        root_endpoints = discover_msgsvr_endpoints(
            platform_domain_hint=platform_domain_hint,
            openapi_server_hint=openapi_server_hint,
            api_server_hint=api_server_hint,
            client_id_hint=client_id_hint,
            timeout_s=timeout_s,
        )

    ordered_candidates: list[tuple[str, int]] = []
    if cached is not None:
        ordered_candidates.append(cached)
    for candidate in root_endpoints:
        if candidate not in ordered_candidates:
            ordered_candidates.append(candidate)

    if ordered_candidates:
        _LOGGER.info(
            "Resolved signaling candidates=%s (platform_hint=%s, openapi_hint=%s)",
            ", ".join(f"{ip}:{port}" for ip, port in ordered_candidates[:8]),
            _extract_host(platform_domain_hint),
            _extract_host(openapi_server_hint),
        )
        return ordered_candidates

    _LOGGER.warning(
        "No signaling endpoints discovered (platform_hint=%s, openapi_hint=%s)",
        _extract_host(platform_domain_hint),
        _extract_host(openapi_server_hint),
    )
    return []


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


def _unwrap_peer_packet(turn, raw: bytes, addr: tuple[str, int]) -> PeerPacket:
    if len(raw) < 4:
        return PeerPacket(raw, addr, None, False)

    if (raw[0] & 0xC0) == 0x40:
        ch, length = struct.unpack(">HH", raw[:4])
        data = raw[4 : 4 + length]
        peer = turn.reverse_channels.get(ch)
        return PeerPacket(data, addr, peer, True, _parse_stun(data))

    msg = _parse_stun(raw)
    if msg and msg["type"] == DATA_INDICATION:
        data = msg["attrs"].get(ATTR_DATA, b"")
        peer = None
        if ATTR_XOR_PEER_ADDRESS in msg["attrs"]:
            peer_ip, peer_port = _decode_xor_address(
                msg["attrs"][ATTR_XOR_PEER_ADDRESS]
            )
            if peer_ip is not None and peer_port is not None:
                peer = (peer_ip, peer_port)
        return PeerPacket(data, addr, peer, True, _parse_stun(data))

    if msg:
        return PeerPacket(raw, addr, None, False, msg)
    if addr[0] == getattr(turn, "server_ip", None):
        return PeerPacket(raw, addr, None, True, None)
    return PeerPacket(raw, addr, addr, False, None)


def recv_peer_packet(turn, timeout: float = 1.0) -> PeerPacket | None:
    """Receive one UDP packet and unwrap TURN ChannelData/DataIndication.

    ``TurnClient.recv_data()`` intentionally hides the socket source address.
    The streamer needs it for direct ICE checks, so this helper keeps both the
    raw source address and the logical peer address.
    """
    turn.sock.settimeout(timeout)
    try:
        raw, addr = turn.sock.recvfrom(65536)
    except socket.timeout:
        return None
    return _unwrap_peer_packet(turn, raw, addr)


def recv_peer_packets(
    turn,
    *,
    timeout: float = 0.08,
    max_packets: int = 512,
) -> list[PeerPacket]:
    """Receive one packet, then drain immediately available UDP bursts."""
    first = recv_peer_packet(turn, timeout=timeout)
    if first is None:
        return []

    packets = [first]
    sock = turn.sock
    sock.setblocking(False)
    try:
        for _ in range(max(0, max_packets - 1)):
            try:
                raw, addr = sock.recvfrom(65536)
            except (BlockingIOError, OSError):
                break
            packets.append(_unwrap_peer_packet(turn, raw, addr))
    finally:
        sock.setblocking(True)
        sock.settimeout(timeout)
    return packets
