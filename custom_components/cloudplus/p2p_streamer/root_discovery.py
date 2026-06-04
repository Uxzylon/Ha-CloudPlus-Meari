"""Native-compatible UDP MsgSvr root discovery."""

from __future__ import annotations

import json
import logging
import socket
import time
from urllib.parse import urlparse

from ..msgsvr_codec import (
    CMD_STATUS,
    METHOD_DIRECT,
    NODE_CLIENT,
    build_frame,
    parse_frame,
)

_LOGGER = logging.getLogger(__name__)

DEFAULT_PLATFORM_DOMAINS = (
    "euce.mearicloud.com",
    "usce.mearicloud.com",
    "asce.mearicloud.com",
    "cnce.mearicloud.com",
)
ROOT_DISCOVERY_PORT = 9253
ROOT_DISCOVERY_VERSION = 15259
_ROOT_CACHE_TTL_S = 600.0
_ROOT_CACHE: dict[str, tuple[list[tuple[str, int]], float]] = {}


def _host(value: str | None) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    if "://" in raw:
        try:
            return (urlparse(raw).hostname or "").strip().lower()
        except Exception:
            return ""
    return raw


def _platform_from_openapi(host: str) -> str:
    return host[len("openapi-") :] if host.startswith("openapi-") else host


def _domain_code(*hints: str | None) -> str:
    for hint in hints:
        host = _host(hint).replace("-", ".")
        for part in host.split("."):
            if part in {"eu", "us", "as", "cn"}:
                return part
            if part in {"euce", "usce", "asce", "cnce"}:
                return part[:2]
    return ""


def _dedupe(items) -> list:
    out = []
    for item in items:
        if item and item not in out:
            out.append(item)
    return out


def _resolve_ips(host: str) -> list[str]:
    try:
        infos = socket.getaddrinfo(host, None, socket.AF_INET)
    except socket.gaierror:
        return []
    return _dedupe(info[4][0] for info in infos if info[4][0])


def _domains(
    platform_domain_hint: str | None,
    openapi_server_hint: str | None,
) -> list[str]:
    openapi_host = _host(openapi_server_hint)
    hinted = _dedupe(
        (
            _host(platform_domain_hint),
            _platform_from_openapi(openapi_host),
        )
    )
    return _dedupe((*hinted, *DEFAULT_PLATFORM_DOMAINS))


def _timestamp() -> str:
    now = time.time()
    return f"{int(now)}.{int((now % 1) * 1000)}"


def _query_conf(
    endpoint: tuple[str, int],
    body: dict,
    timeout_s: float,
) -> dict | None:
    payload = json.dumps(body, separators=(",", ":"))
    frame = build_frame(NODE_CLIENT, METHOD_DIRECT, CMD_STATUS, payload)
    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(timeout_s)
        sock.sendto(frame, endpoint)
        data, _addr = sock.recvfrom(4096)
        parsed = parse_frame(data).get("json")
        return parsed if isinstance(parsed, dict) else None
    except (OSError, ValueError, json.JSONDecodeError):
        return None
    finally:
        if sock is not None:
            sock.close()


def _contact_endpoints(response: dict) -> list[tuple[str, int]]:
    contact = response.get("contact")
    if not isinstance(contact, dict):
        return []
    if str(contact.get("transport") or "").lower() not in {"", "tcp"}:
        return []
    try:
        port = int(contact.get("port") or 0)
    except (TypeError, ValueError):
        return []
    if not 1 <= port <= 65535:
        return []

    endpoints = []
    contact_ip = _host(contact.get("ip"))
    if contact_ip:
        endpoints.append((contact_ip, port))
    host = _host(response.get("host") or contact.get("host") or contact.get("domain"))
    endpoints.extend((ip, port) for ip in (_resolve_ips(host) if host else []))
    return _dedupe(endpoints)


def _dnssvr_endpoint(response: dict) -> tuple[str, int] | None:
    dnssvr = response.get("dnssvr")
    if not isinstance(dnssvr, dict):
        return None
    try:
        port = int(dnssvr.get("port") or 0)
    except (TypeError, ValueError):
        return None
    if not 1 <= port <= 65535:
        return None

    host = _host(response.get("dnssvr_host") or dnssvr.get("host"))
    ip = _host(dnssvr.get("ip"))
    if not ip and host:
        resolved = _resolve_ips(host)
        ip = resolved[0] if resolved else ""
    return (ip, port) if ip else None


def _conf_body(
    *,
    code: str,
    client_id_hint: str | int | None,
    parent: bool = False,
) -> dict:
    body = {"action": "conf", "ver": ROOT_DISCOVERY_VERSION, "t": _timestamp()}
    if client_id_hint:
        body["uuid"] = str(client_id_hint)
    if code:
        body["domain"] = code
    if parent:
        body["tag"] = "parent"
    return body


def _parent_queries(
    *,
    code: str,
    client_id_hint: str | int | None,
) -> list[dict]:
    native_uuid = _conf_body(code="", client_id_hint=client_id_hint, parent=True)
    native = _conf_body(code=code, client_id_hint=client_id_hint, parent=True)
    fallback = _conf_body(code="", client_id_hint=None, parent=True)
    return _dedupe((native_uuid, native, fallback))


def _root_queries(
    *,
    code: str,
    client_id_hint: str | int | None,
) -> list[dict]:
    native_uuid = _conf_body(code="", client_id_hint=client_id_hint)
    native = _conf_body(code=code, client_id_hint=client_id_hint)
    native_no_uuid = _conf_body(code="", client_id_hint=None)
    region_only = _conf_body(code=code, client_id_hint=None)
    return _dedupe((native_uuid, native_no_uuid, native, region_only))


def discover_msgsvr_endpoints(
    *,
    platform_domain_hint: str | None = None,
    openapi_server_hint: str | None = None,
    api_server_hint: str | None = None,
    client_id_hint: str | int | None = None,
    timeout_s: float = 0.7,
) -> list[tuple[str, int]]:
    """Return native root-discovered TCP MsgSvr endpoints."""
    code = _domain_code(api_server_hint, platform_domain_hint, openapi_server_hint)
    cache_key = "|".join(
        (
            _host(platform_domain_hint),
            _host(openapi_server_hint),
            _host(api_server_hint),
            code,
            str(client_id_hint or ""),
        )
    )
    now = time.monotonic()
    cached = _ROOT_CACHE.get(cache_key)
    if cached and now < cached[1]:
        return list(cached[0])
    if cached:
        _ROOT_CACHE.pop(cache_key, None)

    domains = _domains(platform_domain_hint, openapi_server_hint)
    if code:
        hinted = bool(_host(platform_domain_hint) or _host(openapi_server_hint))
        domains = _dedupe((f"{code}ce.mearicloud.com", *(domains if hinted else ())))

    endpoints: list[tuple[str, int]] = []
    for domain in domains:
        domain_ips = _resolve_ips(domain)
        domain_endpoints: list[tuple[str, int]] = []
        for root_ip in domain_ips:
            root = None
            for body in _root_queries(code=code, client_id_hint=client_id_hint):
                root = _query_conf((root_ip, ROOT_DISCOVERY_PORT), body, timeout_s)
                if root is None:
                    break
                if isinstance(root, dict) and (
                    isinstance(root.get("contact"), dict)
                    or isinstance(root.get("dnssvr"), dict)
                ):
                    break
            if isinstance(root, dict):
                domain_endpoints.extend(_contact_endpoints(root))

            dns_endpoint = _dnssvr_endpoint(root) if isinstance(root, dict) else None
            if dns_endpoint is not None:
                for parent_body in _parent_queries(
                    code=code, client_id_hint=client_id_hint
                ):
                    parent = _query_conf(dns_endpoint, parent_body, timeout_s)
                    if isinstance(parent, dict):
                        domain_endpoints.extend(_contact_endpoints(parent))
                    if domain_endpoints:
                        break
            if domain_endpoints:
                break
        endpoints.extend(_dedupe(domain_endpoints))

    endpoints = _dedupe(endpoints)
    if endpoints:
        _ROOT_CACHE[cache_key] = (endpoints, now + _ROOT_CACHE_TTL_S)
        _LOGGER.info(
            "Root-discovered MsgSvr endpoints=%s (platform_hint=%s, openapi_hint=%s)",
            ", ".join(f"{ip}:{port}" for ip, port in endpoints[:4]),
            _host(platform_domain_hint),
            _host(openapi_server_hint),
        )
    return endpoints
