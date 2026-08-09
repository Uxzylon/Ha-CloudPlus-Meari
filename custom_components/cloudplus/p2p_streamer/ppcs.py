"""Legacy PPCS rendezvous and reliable UDP transport.

Older Meari cameras advertise ``deviceP2P=ppcs`` and use the PPStrong
transport directly instead of MsgSvr, ICE/TURN and KCP.  This module mirrors
the portable subset of that transport used by the official app.
"""

from __future__ import annotations

import logging
import re
import socket
import time
from dataclasses import dataclass
from typing import Iterable

_LOGGER = logging.getLogger(__name__)

PPCS_ROOT_PORT = 32100
PPCS_CHANNEL_CHUNK = 1024
PPCS_PUNCH_RADIUS = 3
PPCS_RETRANSMIT_S = 0.05
PPCS_KEEPALIVE_S = 2.0
PPCS_MEDIA_HEADER_SIZE = 32
PPCS_MEDIA_MAX_DATA = 8 * 1024 * 1024
PPCS_MEDIA_IFRAME = 0xF0
PPCS_MEDIA_PFRAME = 0xF1
PPCS_MEDIA_AUDIO = 0xFA

_PPCS_TABLE = bytes.fromhex(
    "7c9ce84a13dedcb22f2123e4307b3d8cbc0b270c3cf79ae7087196009785efc1"
    "1fc4dba1c2ebd901faba3b05b81587832872d18b5ad6da9358feaacc6e1bf0a3"
    "88ab43c00db545384f502266207f075b14981d9ba72ab9a8cbf1fc4947063eb10"
    "e043a945eee541134dd4df9ecc7c9e3781a6f706ba4bda95dd5f8e5bb26af4237"
    "d8e1020aae5f1cc573094e6924906d12b319ad748a2940f52dbea559e0f479d24"
    "bce8982488425c6912ba2fb8fe9a6b09e3f65f603312eac0f952c5ced39b7336c"
    "567eb4a0fd7a815351868d9f77ff6a80dfe2bf10d775645776f355cdd0c818e6"
    "364162cf99f2324c67606192cad3ea637d16b68ed46835c3529d46441e17"
)

_INIT_SEED = bytes(
    (
        0x49,
        0x59,
        0x43,
        0x3D,
        0xB5,
        0xBF,
        0x6D,
        0xA3,
        0x47,
        0x53,
        0x4F,
        0x61,
        0x65,
        0xE3,
        0x71,
        0xE9,
        0x67,
        0x7F,
        0x02,
        0x03,
        0x0B,
        0xAD,
        0xB3,
        0x89,
        0x2B,
        0x2F,
        0x35,
        0xC1,
        0x6B,
        0x8B,
        0x95,
        0x97,
        0x11,
        0xE5,
        0xA7,
        0x0D,
        0xEF,
        0xF1,
        0x05,
        0x07,
        0x83,
        0xFB,
        0x9D,
        0x3B,
        0xC5,
        0xC7,
        0x13,
        0x17,
        0x1D,
        0x1F,
        0x25,
        0x29,
        0xD3,
        0xDF,
    )
)

_DID_RE = re.compile(r"^([A-Z]{1,8})-(\d{1,10})-([A-Z]{1,8})$")


@dataclass(frozen=True)
class PpcsIdentity:
    """Decoded PPCS device identity."""

    prefix: str
    number: int
    suffix: str

    @property
    def payload(self) -> bytes:
        return (
            self.prefix.encode("ascii").ljust(8, b"\x00")
            + self.number.to_bytes(4, "big")
            + self.suffix.encode("ascii").ljust(8, b"\x00")
        )


@dataclass(frozen=True)
class PpcsMediaFrame:
    """One legacy PPCS media header and its codec payload."""

    frame_type: int
    sequence: int
    timestamp_ms: int
    payload: bytes


class PpcsMediaFrameBuffer:
    """Reassemble legacy 32-byte media headers from reliable channel chunks."""

    def __init__(self) -> None:
        self._buffer = bytearray()

    def feed(self, data: bytes) -> list[PpcsMediaFrame]:
        self._buffer.extend(data)
        frames: list[PpcsMediaFrame] = []
        while len(self._buffer) >= PPCS_MEDIA_HEADER_SIZE:
            data_len = int.from_bytes(self._buffer[28:32], "little")
            if data_len > PPCS_MEDIA_MAX_DATA:
                self._buffer.clear()
                raise ValueError("Invalid PPCS media frame length")
            total_len = PPCS_MEDIA_HEADER_SIZE + data_len
            if len(self._buffer) < total_len:
                break
            frames.append(
                PpcsMediaFrame(
                    frame_type=self._buffer[16],
                    sequence=int.from_bytes(self._buffer[0:4], "little"),
                    timestamp_ms=int.from_bytes(self._buffer[20:24], "little"),
                    payload=bytes(self._buffer[PPCS_MEDIA_HEADER_SIZE:total_len]),
                )
            )
            del self._buffer[:total_len]
        return frames


def decode_ppcs_uuid(device_uuid: str) -> PpcsIdentity:
    """Decode the factory-9 UUID wrapper and return its first DID token."""
    encoded = str(device_uuid or "").strip()
    if not encoded.endswith("B"):
        raise ValueError("Invalid PPCS device UUID")

    decoded = bytearray()
    for value in encoded[:-1].encode("ascii"):
        if 0x41 <= value <= 0x4A:  # A-J -> 0-9
            value -= 0x11
        elif 0x30 <= value <= 0x39:  # 0-9 -> A-J
            value += 0x11
        elif 0x61 <= value <= 0x70:  # a-p -> K-Z
            value -= 0x16
        elif 0x71 <= value <= 0x7A:  # q-z -> a-j
            value -= 0x10
        elif 0x4B <= value <= 0x5A:  # K-Z -> k-z
            value += 0x20
        decoded.append(value)

    token = decoded.decode("ascii").split(",", 1)[0]
    match = _DID_RE.fullmatch(token)
    if match is None:
        raise ValueError("Invalid decoded PPCS DID")
    number = int(match.group(2))
    if number > 0xFFFFFFFF:
        raise ValueError("PPCS DID number is out of range")
    return PpcsIdentity(match.group(1), number, match.group(3))


def decode_ppcs_init(init_string: str) -> tuple[list[str], bytes]:
    """Decode a device-provided PPCS root list and packet key."""
    encoded, separator, key_text = str(init_string or "").partition(":")
    if not separator or not encoded or not key_text or len(encoded) % 2:
        raise ValueError("Invalid PPCS init string")

    decoded = bytearray()
    checksum = 0x39
    for index in range(len(encoded) // 2):
        first, second = encoded[index * 2 : index * 2 + 2].encode("ascii")
        value = (second + first * 16 + 0xAF) & 0xFF
        byte = checksum ^ value ^ _INIT_SEED[index % len(_INIT_SEED)]
        decoded.append(byte)
        checksum ^= byte

    roots = [item.strip() for item in decoded.decode("ascii").split(",")]
    roots = [item for item in roots if item]
    key = key_text.encode("ascii")
    if not roots or not key:
        raise ValueError("PPCS init string contains no roots or key")
    return roots, key


def _cipher_state(key: bytes) -> tuple[int, int, int, int]:
    state = [0, 0, 0, 0]
    for value in key[:20]:
        state[0] = (state[0] + value) & 0xFF
        state[1] = (state[1] - value) & 0xFF
        state[2] = (state[2] + value // 3) & 0xFF
        state[3] ^= value
    return tuple(state)


def encrypt_ppcs_packet(data: bytes, key: bytes) -> bytes:
    """Apply the chained PPCS substitution cipher."""
    state = _cipher_state(key)
    output = bytearray()
    previous = 0
    for value in data:
        encrypted = value ^ _PPCS_TABLE[(previous + state[previous & 3]) & 0xFF]
        output.append(encrypted)
        previous = encrypted
    return bytes(output)


def decrypt_ppcs_packet(data: bytes, key: bytes) -> bytes:
    """Reverse the chained PPCS substitution cipher."""
    state = _cipher_state(key)
    output = bytearray()
    previous = 0
    for value in data:
        output.append(value ^ _PPCS_TABLE[(previous + state[previous & 3]) & 0xFF])
        previous = value
    return bytes(output)


def _packet(packet_type: int, payload: bytes = b"") -> bytes:
    return b"\xf1" + bytes((packet_type,)) + len(payload).to_bytes(2, "big") + payload


def _parse_packet(data: bytes) -> tuple[int, bytes] | None:
    if len(data) < 4 or data[0] != 0xF1:
        return None
    length = int.from_bytes(data[2:4], "big")
    if len(data) != length + 4:
        return None
    return data[1], data[4:]


def _sockaddr_payload(endpoint: tuple[str, int] | None) -> bytes:
    if endpoint is None:
        return b"\x00\x02\x00\x00" + b"\x00" * 12
    ip, port = endpoint
    return (
        b"\x00\x02"
        + int(port).to_bytes(2, "little")
        + socket.inet_aton(ip)[::-1]
        + b"\x00" * 8
    )


def _parse_sockaddr(payload: bytes) -> tuple[str, int] | None:
    if len(payload) < 8 or payload[:2] != b"\x00\x02":
        return None
    port = int.from_bytes(payload[2:4], "little")
    ip = socket.inet_ntoa(payload[4:8][::-1])
    if not port or ip == "0.0.0.0":
        return None
    return ip, port


def _resolve_roots(hosts: Iterable[str]) -> list[tuple[str, int]]:
    roots: list[tuple[str, int]] = []
    for host in hosts:
        try:
            infos = socket.getaddrinfo(
                host, PPCS_ROOT_PORT, socket.AF_INET, socket.SOCK_DGRAM
            )
        except socket.gaierror:
            _LOGGER.debug("Could not resolve PPCS root %s", host)
            continue
        for info in infos:
            endpoint = (str(info[4][0]), int(info[4][1]))
            if endpoint not in roots:
                roots.append(endpoint)
    if not roots:
        raise OSError("No PPCS roots resolved")
    return roots


def _route_local_ip(endpoint: tuple[str, int]) -> str:
    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        probe.connect(endpoint)
        return str(probe.getsockname()[0])
    finally:
        probe.close()


class PpcsTransport:
    """PPCS peer discovery and ordered reliable channels over one UDP socket."""

    def __init__(self, init_string: str, device_uuid: str) -> None:
        hosts, self._key = decode_ppcs_init(init_string)
        self._roots = _resolve_roots(hosts)
        self._identity = decode_ppcs_uuid(device_uuid)
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.bind(("0.0.0.0", 0))
        self._sock.settimeout(0.1)
        self.peer: tuple[str, int] | None = None
        self.candidate_count = 0
        self._send_sequence = [0] * 8
        self._recv_sequence = [0] * 8
        self._recv_pending: list[dict[int, bytes]] = [{} for _ in range(8)]
        self._outstanding: dict[tuple[int, int], tuple[bytes, float]] = {}
        self._next_keepalive = 0.0
        self._debug_peer_types: set[int] = set()
        self._debug_channels: set[int] = set()

    @property
    def socket(self) -> socket.socket:
        return self._sock

    def close(self) -> None:
        try:
            self._sock.close()
        except OSError:
            pass

    def _send(
        self, packet_type: int, payload: bytes, endpoint: tuple[str, int]
    ) -> None:
        frame = encrypt_ppcs_packet(_packet(packet_type, payload), self._key)
        self._sock.sendto(frame, endpoint)

    def _recv(self) -> tuple[int, bytes, tuple[str, int]] | None:
        try:
            data, endpoint = self._sock.recvfrom(65535)
        except (socket.timeout, BlockingIOError):
            return None
        parsed = _parse_packet(decrypt_ppcs_packet(data, self._key))
        if parsed is None:
            return None
        return parsed[0], parsed[1], (str(endpoint[0]), int(endpoint[1]))

    def _p2p_request(self, local_endpoint: tuple[str, int] | None) -> bytes:
        return self._identity.payload + _sockaddr_payload(local_endpoint)

    def _punch(self, endpoint: tuple[str, int]) -> None:
        ip, port = endpoint
        for candidate_port in range(
            max(1, port - PPCS_PUNCH_RADIUS),
            min(65535, port + PPCS_PUNCH_RADIUS) + 1,
        ):
            self._send(0x41, self._identity.payload, (ip, candidate_port))

    def connect(self, timeout_s: float) -> tuple[str, int]:
        """Discover and confirm the first direct LAN/public camera peer."""
        local_ip = _route_local_ip(self._roots[0])
        local_port = int(self._sock.getsockname()[1])
        local_endpoint = (local_ip, local_port)
        root_ips = {endpoint[0] for endpoint in self._roots}
        candidates: set[tuple[str, int]] = set()
        deadline = time.monotonic() + timeout_s
        next_root_request = 0.0
        next_punch = 0.0
        first_root_request = True

        while time.monotonic() < deadline:
            now = time.monotonic()
            if now >= next_root_request:
                for root_ip, root_port in self._roots:
                    self._send(0x00, b"", (root_ip, root_port))
                    if first_root_request:
                        unknown_ip = ("0.0.0.0", local_port)
                        self._send(
                            0x20,
                            self._p2p_request(unknown_ip),
                            (root_ip, root_port),
                        )
                    self._send(
                        0x20, self._p2p_request(local_endpoint), (root_ip, root_port)
                    )
                    if first_root_request:
                        for offset in (1, 2):
                            self._send(
                                0x20,
                                self._p2p_request(local_endpoint),
                                (root_ip, root_port + offset),
                            )
                first_root_request = False
                next_root_request = now + 1.0

            if candidates and now >= next_punch:
                for endpoint in tuple(candidates):
                    self._punch(endpoint)
                next_punch = now + 0.25

            received = self._recv()
            if received is None:
                continue
            packet_type, payload, endpoint = received
            if endpoint[0] in root_ips:
                if packet_type == 0x40:
                    candidate = _parse_sockaddr(payload)
                    if candidate is not None and candidate not in candidates:
                        candidates.add(candidate)
                        self.candidate_count = len(candidates)
                        _LOGGER.debug("PPCS root offered peer %s:%d", *candidate)
                        self._punch(candidate)
                continue

            if packet_type not in (0x41, 0x42) or payload != self._identity.payload:
                continue
            if endpoint not in candidates:
                candidates.add(endpoint)
                self.candidate_count = len(candidates)
            if packet_type == 0x41:
                self._send(0x41, self._identity.payload, endpoint)
                continue

            self.peer = endpoint
            self._next_keepalive = time.monotonic()
            _LOGGER.info("PPCS direct peer confirmed: %s:%d", *endpoint)
            return endpoint

        raise TimeoutError("PPCS peer discovery timed out")

    def send_channel(self, channel: int, data: bytes) -> None:
        """Queue bytes on one reliable PPCS channel."""
        if self.peer is None or not 0 <= channel < len(self._send_sequence):
            raise RuntimeError("PPCS transport is not connected")
        for offset in range(0, len(data), PPCS_CHANNEL_CHUNK):
            chunk = data[offset : offset + PPCS_CHANNEL_CHUNK]
            sequence = self._send_sequence[channel]
            self._send_sequence[channel] = (sequence + 1) & 0xFFFF
            payload = b"\xd1" + bytes((channel,)) + sequence.to_bytes(2, "big") + chunk
            frame = encrypt_ppcs_packet(_packet(0xD0, payload), self._key)
            self._sock.sendto(frame, self.peer)
            self._outstanding[(channel, sequence)] = (frame, time.monotonic())

    def _send_ack(self, channel: int, sequences: list[int]) -> None:
        if self.peer is None or not sequences:
            return
        payload = (
            b"\xd1"
            + bytes((channel,))
            + len(sequences).to_bytes(2, "big")
            + b"".join(sequence.to_bytes(2, "big") for sequence in sequences)
        )
        self._send(0xD1, payload, self.peer)

    def _accept_data(
        self, channel: int, sequence: int, data: bytes
    ) -> list[tuple[int, bytes]]:
        expected = self._recv_sequence[channel]
        delta = (sequence - expected) & 0xFFFF
        if delta == 0:
            output = [(channel, data)]
            expected = (expected + 1) & 0xFFFF
            pending = self._recv_pending[channel]
            while expected in pending:
                output.append((channel, pending.pop(expected)))
                expected = (expected + 1) & 0xFFFF
            self._recv_sequence[channel] = expected
            return output
        if delta < 0x8000:
            pending = self._recv_pending[channel]
            if len(pending) < 2048:
                pending.setdefault(sequence, data)
        return []

    def _handle_peer_packet(
        self,
        packet_type: int,
        payload: bytes,
    ) -> tuple[list[tuple[int, bytes]], tuple[int, int] | None]:
        if packet_type == 0xE0:
            self._send(0xE1, b"", self.peer)
        elif packet_type == 0x42 and payload == self._identity.payload:
            self._send(0xE0, b"", self.peer)
        elif packet_type == 0x41 and payload == self._identity.payload:
            self._send(0x41, self._identity.payload, self.peer)
        elif packet_type == 0xD1 and len(payload) >= 4 and payload[0] == 0xD1:
            channel = payload[1]
            count = int.from_bytes(payload[2:4], "big")
            if channel < len(self._send_sequence) and len(payload) == 4 + count * 2:
                for offset in range(4, len(payload), 2):
                    sequence = int.from_bytes(payload[offset : offset + 2], "big")
                    self._outstanding.pop((channel, sequence), None)
        elif packet_type == 0xD0 and len(payload) >= 4 and payload[0] == 0xD1:
            channel = payload[1]
            sequence = int.from_bytes(payload[2:4], "big")
            if channel < len(self._recv_sequence):
                if channel not in self._debug_channels:
                    self._debug_channels.add(channel)
                    _LOGGER.debug(
                        "PPCS reliable channel %d active (first_seq=%d bytes=%d)",
                        channel,
                        sequence,
                        len(payload) - 4,
                    )
                ordered = self._accept_data(channel, sequence, payload[4:])
                return ordered, (channel, sequence)
        return [], None

    def poll(self, timeout_s: float = 0.1) -> list[tuple[int, bytes]]:
        """Service retransmits/keepalives and return newly ordered channel data."""
        if self.peer is None:
            raise RuntimeError("PPCS transport is not connected")
        self._sock.settimeout(max(0.001, timeout_s))
        now = time.monotonic()
        for key, (frame, sent_at) in tuple(self._outstanding.items()):
            if now - sent_at >= PPCS_RETRANSMIT_S:
                self._sock.sendto(frame, self.peer)
                self._outstanding[key] = (frame, now)
        if now >= self._next_keepalive:
            self._send(0xE0, b"", self.peer)
            self._next_keepalive = now + PPCS_KEEPALIVE_S

        ordered: list[tuple[int, bytes]] = []
        acknowledgements: dict[int, list[int]] = {}
        received = self._recv()
        if received is not None:
            self._sock.setblocking(False)
        try:
            for _ in range(256):
                if received is None:
                    break
                packet_type, payload, endpoint = received
                if endpoint == self.peer:
                    if packet_type not in self._debug_peer_types:
                        self._debug_peer_types.add(packet_type)
                        _LOGGER.debug("PPCS peer packet type=0x%02x", packet_type)
                    channel_data, acknowledgement = self._handle_peer_packet(
                        packet_type,
                        payload,
                    )
                    ordered.extend(channel_data)
                    if acknowledgement is not None:
                        channel, sequence = acknowledgement
                        acknowledgements.setdefault(channel, []).append(sequence)
                received = self._recv()
        finally:
            self._sock.settimeout(max(0.001, timeout_s))

        for channel, sequences in acknowledgements.items():
            for offset in range(0, len(sequences), 128):
                self._send_ack(channel, sequences[offset : offset + 128])
        return ordered
