"""Signaling endpoint resolution for msgsvr."""

from __future__ import annotations

import socket
import time
from urllib.parse import urlparse

_CACHE: tuple[tuple[str, int], float] | None = None


def _extract_host(value: str | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if "://" in raw:
        try:
            return (urlparse(raw).hostname or "").strip().lower()
        except Exception:
            return ""
    return raw.lower()


def _derive_platform_domain(openapi_host: str) -> str:
    if openapi_host.startswith("openapi-"):
        return openapi_host[len("openapi-") :]
    return openapi_host


def _probe(ip: str, port: int, timeout_s: float) -> bool:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.settimeout(timeout_s)
        sock.connect((ip, port))
        return True
    except Exception:
        return False
    finally:
        try:
            sock.close()
        except Exception:
            pass


def resolve_signaling_endpoint(
    *,
    openapi_hint: str | None,
    timeout_s: float = 1.0,
) -> tuple[str, int]:
    global _CACHE
    now = time.monotonic()
    if _CACHE is not None:
        endpoint, expire = _CACHE
        if now < expire:
            return endpoint

    ports = [18849, 28974, 9253]
    domains: list[str] = []
    for domain in (
        _derive_platform_domain(_extract_host(openapi_hint)),
        _extract_host(openapi_hint),
        "euce.mearicloud.com",
        "mearicloud.com",
    ):
        if domain and domain not in domains:
            domains.append(domain)

    ips: list[str] = []
    for domain in domains:
        try:
            infos = socket.getaddrinfo(domain, None, socket.AF_INET)
        except Exception:
            continue
        for info in infos:
            ip = info[4][0]
            if ip and ip not in ips:
                ips.append(ip)

    for fallback_ip in ("47.91.76.116", "47.91.73.19", "47.254.142.96"):
        if fallback_ip not in ips:
            ips.append(fallback_ip)

    for ip in ips:
        for port in ports:
            if _probe(ip, port, timeout_s):
                endpoint = (ip, port)
                _CACHE = (endpoint, now + 180.0)
                return endpoint

    endpoint = ("47.91.73.19", 28974)
    _CACHE = (endpoint, now + 20.0)
    return endpoint
