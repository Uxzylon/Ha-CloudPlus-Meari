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

    def add_candidate(ip: str, port: int, cand_type: str) -> None:
        if not ip or not port:
            return
        candidate = {"ip": ip, "port": int(port), "type": cand_type or "relay"}
        for existing in camera_candidates:
            if (
                existing["ip"] == candidate["ip"]
                and existing["port"] == candidate["port"]
            ):
                return
        camera_candidates.append(candidate)

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
        elif line.startswith("m="):
            parts = line.split()
            try:
                if len(parts) > 1:
                    camera_sdp_port = int(parts[1])
            except (TypeError, ValueError):
                pass
        elif line.startswith("a=candidate:"):
            parts = line.split()
            if len(parts) >= 6:
                try:
                    add_candidate(
                        parts[4],
                        int(parts[5]),
                        parts[7] if len(parts) > 7 else "relay",
                    )
                except (TypeError, ValueError):
                    pass

    if camera_sdp_ip and camera_sdp_port:
        media_candidate = {
            "ip": camera_sdp_ip,
            "port": camera_sdp_port,
            "type": "relay",
        }
        camera_candidates = [
            media_candidate,
            *[
                cand
                for cand in camera_candidates
                if cand["ip"] != camera_sdp_ip or cand["port"] != camera_sdp_port
            ],
        ]

    return camera_ufrag, camera_pwd, camera_candidates


def candidates_from_response(response: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract ICE candidates from a WebRTC signaling response."""
    candidates: list[dict[str, Any]] = []
    sdp = response.get("sdp", "")
    if sdp:
        _, _, candidates = parse_sdp_answer(str(sdp))

    candidate = response.get("candidate")
    if isinstance(candidate, dict):
        ip = candidate.get("ip")
        port = candidate.get("port")
        if ip and port:
            try:
                candidates.append(
                    {
                        "ip": str(ip),
                        "port": int(port),
                        "type": str(candidate.get("type") or "relay"),
                    }
                )
            except (TypeError, ValueError):
                pass
    return candidates


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
            camera_candidates.extend(candidates_from_response(extra))
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
