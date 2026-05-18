"""SDP and signaling candidate helpers."""

from __future__ import annotations

import socket
from typing import Any

from ..meari_signaling import MsgSvrClient


def parse_sdp_answer(sdp: str) -> tuple[str, str, list[dict[str, Any]]]:
    camera_ufrag = ""
    camera_pwd = ""
    camera_candidates: list[dict[str, Any]] = []
    camera_sdp_ip = ""
    camera_sdp_port = 0

    for line in sdp.replace("\\n", "\n").splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("a=ice-ufrag:"):
            camera_ufrag = line.split(":", 1)[1].strip()
        elif line.startswith("a=ice-pwd:"):
            camera_pwd = line.split(":", 1)[1].strip()
        elif line.startswith("c=IN IP4 "):
            camera_sdp_ip = line.split("c=IN IP4 ", 1)[1].strip()
        elif line.startswith("m=audio "):
            parts = line.split()
            if len(parts) > 1 and parts[1].isdigit():
                camera_sdp_port = int(parts[1])
        elif line.startswith("a=candidate:"):
            parts = line.split()
            if len(parts) >= 6:
                camera_candidates.append(
                    {
                        "ip": parts[4],
                        "port": int(parts[5]),
                        "type": parts[7] if len(parts) > 7 else "relay",
                    }
                )

    if not camera_candidates and camera_sdp_ip and camera_sdp_port:
        camera_candidates.append(
            {"ip": camera_sdp_ip, "port": camera_sdp_port, "type": "relay"}
        )

    return camera_ufrag, camera_pwd, camera_candidates


def collect_trickle_candidates(
    sig: MsgSvrClient, camera_candidates: list[dict[str, Any]]
) -> None:
    old_timeout = sig.sock.gettimeout()
    try:
        sig.sock.settimeout(0.5)
        for _ in range(10):
            try:
                extra = sig._recv_webrtc_content()
            except socket.timeout:
                break
            except Exception:
                break
            if not isinstance(extra, dict):
                continue
            sdp = extra.get("sdp", "")
            if sdp:
                _, _, extra_candidates = parse_sdp_answer(sdp)
                camera_candidates.extend(extra_candidates)
            candidate = extra.get("candidate")
            if isinstance(candidate, dict):
                ip = candidate.get("ip")
                port = candidate.get("port")
                if ip and port:
                    camera_candidates.append(
                        {
                            "ip": ip,
                            "port": int(port),
                            "type": candidate.get("type", "relay"),
                        }
                    )
    finally:
        sig.sock.settimeout(old_timeout)


def add_candidate_once(
    candidates: list[dict[str, Any]],
    candidate: dict[str, Any],
) -> bool:
    ip = candidate.get("ip")
    port = candidate.get("port")
    if not ip or not port:
        return False
    normalized = {
        "ip": str(ip),
        "port": int(port),
        "type": str(candidate.get("type") or "relay"),
    }
    for existing in candidates:
        if (
            existing.get("ip") == normalized["ip"]
            and int(existing.get("port", 0)) == normalized["port"]
        ):
            return False
    candidates.append(normalized)
    return True


def format_endpoint(endpoint: tuple[str, int] | None) -> str:
    if endpoint is None:
        return ""
    return f"{endpoint[0]}:{endpoint[1]}"
