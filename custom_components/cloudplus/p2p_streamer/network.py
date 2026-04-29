"""Network utilities: local IP detection, signaling server resolution, ICE helpers."""

from __future__ import annotations

import logging
import os
import socket
import struct
import time
from urllib.parse import urlparse

from ..turn_client import (
    _build_stun,
    _encode_attr,
    _add_integrity,
    _encode_xor_address,
    BINDING_REQUEST,
    BINDING_RESPONSE,
    ATTR_USERNAME,
    ATTR_XOR_MAPPED_ADDRESS,
)


_LOGGER = logging.getLogger(__name__)

# Keep successful signaling endpoint for a short period to avoid repeated
# connect-probe storms during reconnect loops.
_SIG_CACHE_TTL_S = 180.0
_SIG_CACHE_ENDPOINT: tuple[str, int] | None = None
_SIG_CACHE_EXPIRES_AT: float = 0.0


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


def _derive_platform_domain_from_openapi(host: str) -> str:
    if host.startswith("openapi-"):
        return host[len("openapi-") :]
    return host


def _probe_tcp_connect(ip: str, port: int, timeout_s: float) -> bool:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.settimeout(timeout_s)
        s.connect((ip, port))
        return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False
    finally:
        try:
            s.close()
        except Exception:
            pass


def _resolve_signaling_server(
    *,
    platform_domain_hint: str | None = None,
    openapi_server_hint: str | None = None,
    timeout_s: float = 2.0,
) -> tuple[str, int]:
    candidates = _resolve_signaling_server_candidates(
        platform_domain_hint=platform_domain_hint,
        openapi_server_hint=openapi_server_hint,
        timeout_s=timeout_s,
    )
    if candidates:
        return candidates[0]
    return ("47.91.73.19", 28974)


def _resolve_signaling_server_candidates(
    *,
    platform_domain_hint: str | None = None,
    openapi_server_hint: str | None = None,
    timeout_s: float = 2.0,
) -> list[tuple[str, int]]:
    global _SIG_CACHE_ENDPOINT, _SIG_CACHE_EXPIRES_AT

    now = time.monotonic()
    if _SIG_CACHE_ENDPOINT is not None and now < _SIG_CACHE_EXPIRES_AT:
        return [_SIG_CACHE_ENDPOINT]

    env_host = _extract_host(os.environ.get("CLOUDPLUS_SIGNALING_HOST"))
    env_port = str(os.environ.get("CLOUDPLUS_SIGNALING_PORT") or "").strip()
    if env_host and env_port.isdigit():
        pinned = (env_host, int(env_port))
        _SIG_CACHE_ENDPOINT = pinned
        _SIG_CACHE_EXPIRES_AT = now + _SIG_CACHE_TTL_S
        return [pinned]

    ports: list[int] = []
    env_ports = str(os.environ.get("CLOUDPLUS_SIGNALING_PORTS") or "").strip()
    if env_ports:
        for item in env_ports.split(","):
            item = item.strip()
            if item.isdigit():
                p = int(item)
                if 1 <= p <= 65535 and p not in ports:
                    ports.append(p)
    if not ports:
        # Observed in fresh CloudEdge capture (2026-04-29): msgsvr on 18849,
        # with older deployments still using 28974 and some fallbacks on 9253.
        ports = [18849, 28974, 9253]

    domain_candidates: list[str] = []
    for candidate in (
        _extract_host(platform_domain_hint),
        _derive_platform_domain_from_openapi(_extract_host(openapi_server_hint)),
        _extract_host(openapi_server_hint),
        "euce.mearicloud.com",
        "mearicloud.com",
    ):
        if candidate and candidate not in domain_candidates:
            domain_candidates.append(candidate)

    ip_candidates: list[str] = []
    for domain in domain_candidates:
        try:
            infos = socket.getaddrinfo(domain, None, socket.AF_INET)
        except socket.gaierror:
            continue
        for info in infos:
            ip = info[4][0]
            if ip and ip not in ip_candidates:
                ip_candidates.append(ip)

    # Keep known historical/current msgsvr IPs as last-resort only.
    for fallback_ip in ("47.91.76.116", "47.91.73.19", "47.254.142.96"):
        if fallback_ip not in ip_candidates:
            ip_candidates.append(fallback_ip)

    ordered_candidates: list[tuple[str, int]] = []
    for ip in ip_candidates:
        for port in ports:
            if _probe_tcp_connect(ip, port, timeout_s=timeout_s):
                resolved = (ip, port)
                if resolved not in ordered_candidates:
                    ordered_candidates.append(resolved)

    # If probe is inconclusive, still provide deterministic fallback candidates.
    for ip in ip_candidates:
        for port in ports:
            candidate = (ip, port)
            if candidate not in ordered_candidates:
                ordered_candidates.append(candidate)

    if ordered_candidates:
        _SIG_CACHE_ENDPOINT = ordered_candidates[0]
        _SIG_CACHE_EXPIRES_AT = time.monotonic() + _SIG_CACHE_TTL_S
        _LOGGER.info(
            "Resolved signaling candidates=%s (platform_hint=%s, openapi_hint=%s)",
            ", ".join(f"{ip}:{port}" for ip, port in ordered_candidates[:6]),
            _extract_host(platform_domain_hint),
            _extract_host(openapi_server_hint),
        )
        return ordered_candidates

    fallback = ("47.91.73.19", ports[0])
    _LOGGER.warning(
        "Signaling endpoint probe failed; falling back to %s:%d",
        fallback[0],
        fallback[1],
    )
    _SIG_CACHE_ENDPOINT = fallback
    _SIG_CACHE_EXPIRES_AT = time.monotonic() + min(30.0, _SIG_CACHE_TTL_S)
    return [fallback]


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
