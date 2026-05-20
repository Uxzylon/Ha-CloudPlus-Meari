"""TURN-side relay probe used by the native SDK before media starts."""

from __future__ import annotations

import logging
import socket

_LOGGER = logging.getLogger(__name__)

_MAGIC = 0xE6
_TAIL = 0x9D
_PROBE_PORT = 9371
_PROBE_TIMEOUT_S = 1.2
_PROBE_COUNT = 4


def _frame(tag: str, code: int = 0x04) -> bytes:
    payload = tag.encode("ascii", errors="ignore")[:10].ljust(10, b"\x00")
    payload += bytes((code, 0, 0, 0, 0, 0))
    header = bytes((_MAGIC, 0xA1, 0xB1, 0xC6, 0xD1, _MAGIC, len(payload), 0))
    return header + payload + bytes((sum(payload) & 0xFF, _TAIL))


def probe_relay(host: str, tag: str) -> bool:
    """Mirror the SDK's short TCP probe to the TURN host."""
    host = str(host or "").strip()
    tag = str(tag or "").strip()
    if not host or not tag:
        return False

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(_PROBE_TIMEOUT_S)
    try:
        sock.connect((host, _PROBE_PORT))
        packet = _frame(tag)
        ok = False
        for _ in range(_PROBE_COUNT):
            sock.sendall(packet)
            try:
                response = sock.recv(64)
            except socket.timeout:
                continue
            ok = ok or (len(response) >= 19 and response[18] in (0x82, 0x84))
        return ok
    except OSError as err:
        _LOGGER.debug("TURN relay probe failed for %s:%d: %s", host, _PROBE_PORT, err)
        return False
    finally:
        try:
            sock.close()
        except OSError:
            pass
