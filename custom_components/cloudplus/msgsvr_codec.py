"""Binary MsgSvr framing shared by signaling and root discovery."""

from __future__ import annotations

import json
import socket

from Crypto.Cipher import DES3

MSGSVR_KEY = b"__#!HIRCloud5.0!#__\x00\x00\x00\x00\x00"

MAGIC = 0xE6
TAIL = 0x9D

NODE_CLIENT = 0xA1

METHOD_DIRECT = 0xB1
METHOD_ROUTED = 0xB2

CMD_REGISTER = 0xC1
CMD_WEBRTC = 0xC6
CMD_STATUS = 0xC7
CMD_HEARTBEAT = 0xC9
CMD_CONNECT = 0xD1

TYPE_DEFAULT = 0xD3


def _des3_encrypt(data: bytes) -> bytes:
    pad_len = 8 - (len(data) % 8)
    cipher = DES3.new(MSGSVR_KEY, DES3.MODE_ECB)
    return cipher.encrypt(data + bytes([pad_len] * pad_len))


def _des3_decrypt(data: bytes) -> bytes:
    data = data[: len(data) - (len(data) % 8)]
    if not data:
        return b""
    decrypted = DES3.new(MSGSVR_KEY, DES3.MODE_ECB).decrypt(data)
    pad_len = decrypted[-1]
    if 1 <= pad_len <= 8 and decrypted[-pad_len:] == bytes([pad_len] * pad_len):
        return decrypted[:-pad_len]
    return decrypted.rstrip(b"\x00")


def _payload_checksum(payload: bytes) -> int:
    return sum(payload) & 0xFF


def _payload_xor_checksum(payload: bytes) -> int:
    value = 0
    for byte in payload:
        value ^= byte
    return value


def build_frame(
    node: int,
    method: int,
    cmd: int,
    payload_json: str,
    encrypt: bool = True,
) -> bytes:
    payload = payload_json.encode("utf-8")
    if encrypt:
        payload = _des3_encrypt(payload)

    payload_len = len(payload)
    header = bytearray(8)
    header[0] = MAGIC
    header[1] = node
    header[2] = method
    header[3] = cmd
    header[4] = TYPE_DEFAULT
    header[5] = MAGIC
    header[6] = payload_len & 0xFF
    header[7] = ((payload_len >> 8) & 0x7F) | (0x80 if encrypt else 0)

    return bytes(header) + payload + bytes([_payload_checksum(payload), TAIL])


def parse_frame(data: bytes) -> dict:
    if len(data) < 10:
        raise ValueError(f"MsgSvr frame too short: {len(data)} bytes")
    if data[0] != MAGIC or data[5] != MAGIC:
        raise ValueError(f"Invalid MsgSvr frame header: {data[:8].hex()}")

    payload_len = data[6] | ((data[7] & 0x7F) << 8)
    frame_len = payload_len + 10
    if len(data) < frame_len:
        raise ValueError(f"Incomplete MsgSvr frame: {len(data)}/{frame_len}")
    if data[frame_len - 1] != TAIL:
        raise ValueError(f"Invalid MsgSvr frame tail: 0x{data[frame_len - 1]:02x}")

    payload = data[8 : 8 + payload_len]
    checksum = data[8 + payload_len]
    if checksum not in {_payload_checksum(payload), _payload_xor_checksum(payload)}:
        raise ValueError("Invalid MsgSvr frame checksum")
    if data[7] & 0x80:
        payload = _des3_decrypt(payload)

    try:
        parsed = json.loads(payload.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        parsed = {"_raw": payload.hex()[:200]}

    return {
        "node": data[1],
        "method": data[2],
        "cmd": data[3],
        "type": data[4],
        "encrypted": bool(data[7] & 0x80),
        "json": parsed,
    }


def recv_exact(sock: socket.socket, size: int) -> bytes:
    data = b""
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise ConnectionError(f"Connection closed (got {len(data)}/{size})")
        data += chunk
    return data


def recv_frame(sock: socket.socket) -> dict:
    header = recv_exact(sock, 8)
    payload_len = header[6] | ((header[7] & 0x7F) << 8)
    return parse_frame(header + recv_exact(sock, payload_len + 2))
