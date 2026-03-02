#!/usr/bin/env python3
"""
Minimal TURN/STUN client for Meari camera relay connections.

Implements just enough of RFC 5766 (TURN) to:
  1. Allocate a relay address
  2. Create permissions for the camera peer
  3. Bind a channel for efficient data transfer
  4. Send/receive data through the relay

The Meari Coturn server uses:
  - Realm: "hangzhou"
  - Username: "{timestamp}:mearicloud"
  - Password: provided by signaling server
  - Transport: UDP on port 9100
"""

import binascii
import hashlib
import hmac
import os
import socket
import struct

# STUN message types
BINDING_REQUEST = 0x0001
BINDING_RESPONSE = 0x0101
ALLOCATE_REQUEST = 0x0003
ALLOCATE_RESPONSE = 0x0103
ALLOCATE_ERROR = 0x0113
CREATE_PERM_REQUEST = 0x0008
CREATE_PERM_RESPONSE = 0x0108
CREATE_PERM_ERROR = 0x0118
CHANNEL_BIND_REQUEST = 0x0009
CHANNEL_BIND_RESPONSE = 0x0109
BINDING_ERROR = 0x0111
REFRESH_REQUEST = 0x0004
REFRESH_RESPONSE = 0x0104
SEND_INDICATION = 0x0016
DATA_INDICATION = 0x0017

# Magic cookie (RFC 5389)
MAGIC_COOKIE = 0x2112A442
MAGIC_BYTES = struct.pack(">I", MAGIC_COOKIE)

# STUN attribute types
ATTR_MAPPED_ADDRESS = 0x0001
ATTR_USERNAME = 0x0006
ATTR_MESSAGE_INTEGRITY = 0x0008
ATTR_ERROR_CODE = 0x0009
ATTR_CHANNEL_NUMBER = 0x000C
ATTR_LIFETIME = 0x000D
ATTR_XOR_PEER_ADDRESS = 0x0012
ATTR_DATA = 0x0013
ATTR_REALM = 0x0014
ATTR_NONCE = 0x0015
ATTR_XOR_RELAYED_ADDRESS = 0x0016
ATTR_REQUESTED_TRANSPORT = 0x0019
ATTR_XOR_MAPPED_ADDRESS = 0x0020
ATTR_SOFTWARE = 0x8022
ATTR_FINGERPRINT = 0x8028

UDP_TRANSPORT_VALUE = 17  # UDP protocol number
FINGERPRINT_XOR = 0x5354554E  # "STUN" in ASCII


def _pad4(data):
    """Pad to 4-byte boundary."""
    r = len(data) % 4
    return data + b'\x00' * ((4 - r) % 4)


def _encode_attr(attr_type, value):
    """Encode a STUN attribute (type + length + value + padding)."""
    return struct.pack(">HH", attr_type, len(value)) + _pad4(value)


def _encode_xor_address(ip, port):
    """Encode XOR-PEER-ADDRESS / XOR-MAPPED-ADDRESS for IPv4."""
    xor_port = port ^ (MAGIC_COOKIE >> 16)
    ip_bytes = socket.inet_aton(ip)
    xor_ip = bytes(a ^ b for a, b in zip(ip_bytes, MAGIC_BYTES))
    return struct.pack(">BBH", 0, 1, xor_port) + xor_ip


def _decode_xor_address(data):
    """Decode an XOR address attribute."""
    if len(data) < 8:
        return None, None
    family = data[1]
    port = struct.unpack(">H", data[2:4])[0] ^ (MAGIC_COOKIE >> 16)
    if family == 1:  # IPv4
        ip_bytes = bytes(a ^ b for a, b in zip(data[4:8], MAGIC_BYTES))
        return socket.inet_ntoa(ip_bytes), port
    return None, port


def _build_stun(msg_type, attrs_bytes, txn_id=None):
    """Build a STUN message. Returns (message_bytes, txn_id)."""
    if txn_id is None:
        txn_id = os.urandom(12)
    header = struct.pack(">HHI", msg_type, len(attrs_bytes), MAGIC_COOKIE) + txn_id
    return header + attrs_bytes, txn_id


def _add_integrity(msg_type, attrs_bytes, txn_id, key):
    """Add MESSAGE-INTEGRITY to a STUN message."""
    # Compute length including the integrity attribute (4 header + 20 HMAC = 24)
    total_attr_len = len(attrs_bytes) + 24
    header = struct.pack(">HHI", msg_type, total_attr_len, MAGIC_COOKIE) + txn_id
    hmac_val = hmac.new(key, header + attrs_bytes, hashlib.sha1).digest()
    attrs_bytes += _encode_attr(ATTR_MESSAGE_INTEGRITY, hmac_val)
    return attrs_bytes


def _add_fingerprint(msg_type, attrs_bytes, txn_id):
    """Add FINGERPRINT attribute to a STUN message.

    FINGERPRINT = CRC-32(message up to FINGERPRINT) XOR 0x5354554E.
    The message length in the header includes FINGERPRINT (8 bytes).
    """
    # Build message with length including FINGERPRINT (4 header + 4 CRC = 8)
    total_attr_len = len(attrs_bytes) + 8
    header = struct.pack(">HHI", msg_type, total_attr_len, MAGIC_COOKIE) + txn_id
    msg_so_far = header + attrs_bytes
    crc = binascii.crc32(msg_so_far) & 0xFFFFFFFF
    fp_value = crc ^ FINGERPRINT_XOR
    attrs_bytes += _encode_attr(ATTR_FINGERPRINT, struct.pack(">I", fp_value))
    return attrs_bytes


def _parse_stun(data):
    """Parse a STUN message into type, txn_id, and attributes dict."""
    if len(data) < 20:
        return None
    msg_type, msg_len, cookie = struct.unpack(">HHI", data[:8])
    # STUN magic cookie (RFC 5389) - must be present
    if cookie != MAGIC_COOKIE:
        return None
    txn_id = data[8:20]

    attrs = {}
    pos = 20
    while pos + 4 <= len(data) and pos < 20 + msg_len:
        attr_type, attr_len = struct.unpack(">HH", data[pos:pos + 4])
        attr_value = data[pos + 4:pos + 4 + attr_len]
        attrs[attr_type] = attr_value
        pos += 4 + ((attr_len + 3) & ~3)

    return {"type": msg_type, "txn_id": txn_id, "attrs": attrs}


class TurnClient:
    """Minimal TURN relay client."""

    def __init__(self, server_ip, server_port, username, password, realm="hangzhou"):
        self.server_ip = server_ip
        self.server_port = server_port
        self.username = username
        self.password = password
        self.realm = realm
        self.sock = None
        self.nonce = None
        self.relay_ip = None
        self.relay_port = None
        self.mapped_ip = None
        self.mapped_port = None
        self.local_port = None
        self._channel_counter = 0x4000
        self.channels = {}  # (ip, port) -> channel_number
        self.reverse_channels = {}  # channel_number -> (ip, port)

    @property
    def _key(self):
        """TURN long-term credential HMAC key = MD5(user:realm:pass)."""
        return hashlib.md5(
            f"{self.username}:{self.realm}:{self.password}".encode()
        ).digest()

    def connect(self):
        """Create and bind UDP socket."""
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(5.0)
        self.sock.bind(("", 0))
        self.local_port = self.sock.getsockname()[1]
        # Increase receive buffer to handle video data bursts (4MB)
        try:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
        except Exception:
            pass

    def _send(self, data):
        self.sock.sendto(data, (self.server_ip, self.server_port))

    def _recv(self, timeout=5.0):
        self.sock.settimeout(timeout)
        data, addr = self.sock.recvfrom(65536)
        return data

    def drain_socket(self):
        """Drain any buffered packets from the socket (non-blocking)."""
        drained = 0
        self.sock.setblocking(False)
        try:
            for _ in range(200):
                try:
                    self.sock.recvfrom(65536)
                    drained += 1
                except (BlockingIOError, OSError):
                    break
        finally:
            self.sock.setblocking(True)
            self.sock.settimeout(5.0)
        if drained:
            print(f"[TURN] Drained {drained} buffered packets")

    def _stun_request(self, msg_type, extra_attrs=b"", auth=True):
        """Build, send, and receive a STUN request with optional auth.

        Filters out unrelated packets (ICE binding checks, Data Indications)
        that may arrive on the same socket while waiting for the TURN response.
        Matches responses by transaction ID to prevent consuming wrong responses.
        """
        attrs = extra_attrs
        if auth and self.nonce:
            attrs += _encode_attr(ATTR_USERNAME, self.username.encode())
            attrs += _encode_attr(ATTR_REALM, self.realm.encode())
            attrs += _encode_attr(ATTR_NONCE, self.nonce)

        txn_id = os.urandom(12)
        if auth and self.nonce:
            attrs = _add_integrity(msg_type, attrs, txn_id, self._key)

        msg, txn_id = _build_stun(msg_type, attrs, txn_id)
        self._send(msg)

        # Expected response type: success = msg_type | 0x0100, error = msg_type | 0x0110
        expected_success = msg_type | 0x0100
        expected_error = msg_type | 0x0110

        # Try to receive matching response, skip unrelated packets
        for _attempt in range(50):  # Max 50 packets before giving up
            try:
                resp_data = self._recv(timeout=3.0)
            except socket.timeout:
                return None
            parsed = _parse_stun(resp_data)
            if parsed is None:
                continue  # Not a valid STUN packet (e.g., ChannelData)
            if parsed["type"] in (expected_success, expected_error):
                # Match by transaction ID to avoid consuming wrong responses
                if parsed["txn_id"] == txn_id:
                    return parsed
                # Wrong txn_id — stale/duplicate response, skip it
                continue
            # Also match binding request/response (different categories)
            if msg_type == BINDING_REQUEST and parsed["type"] in (
                    BINDING_RESPONSE, BINDING_ERROR):
                if parsed["txn_id"] == txn_id:
                    return parsed
                continue
            # Skip unrelated packets (ICE checks, Data Indications, etc.)
            continue
        return None

    def stun_binding(self):
        """STUN Binding request to discover public IP."""
        resp = self._stun_request(BINDING_REQUEST, auth=False)
        if resp and resp["type"] == BINDING_RESPONSE:
            if ATTR_XOR_MAPPED_ADDRESS in resp["attrs"]:
                self.mapped_ip, self.mapped_port = _decode_xor_address(
                    resp["attrs"][ATTR_XOR_MAPPED_ADDRESS])
            elif ATTR_MAPPED_ADDRESS in resp["attrs"]:
                data = resp["attrs"][ATTR_MAPPED_ADDRESS]
                self.mapped_port = struct.unpack(">H", data[2:4])[0]
                self.mapped_ip = socket.inet_ntoa(data[4:8])
            return True
        return False

    def allocate(self):
        """TURN Allocate - get a relay address.

        Does the two-step auth dance: first request gets 401 + nonce,
        second request includes credentials.
        """
        transport_attr = _encode_attr(
            ATTR_REQUESTED_TRANSPORT,
            struct.pack(">I", UDP_TRANSPORT_VALUE << 24),
        )

        # Step 1: unauthenticated request to get nonce
        resp = self._stun_request(ALLOCATE_REQUEST, transport_attr, auth=False)
        if resp and resp["type"] == ALLOCATE_ERROR:
            if ATTR_NONCE in resp["attrs"]:
                self.nonce = resp["attrs"][ATTR_NONCE]
            if ATTR_REALM in resp["attrs"]:
                self.realm = resp["attrs"][ATTR_REALM].rstrip(b'\x00').decode()

        if not self.nonce:
            raise RuntimeError("TURN server did not provide nonce")

        # Step 2: authenticated request
        resp = self._stun_request(ALLOCATE_REQUEST, transport_attr, auth=True)
        if resp and resp["type"] == ALLOCATE_RESPONSE:
            if ATTR_XOR_RELAYED_ADDRESS in resp["attrs"]:
                self.relay_ip, self.relay_port = _decode_xor_address(
                    resp["attrs"][ATTR_XOR_RELAYED_ADDRESS])
            if ATTR_XOR_MAPPED_ADDRESS in resp["attrs"]:
                self.mapped_ip, self.mapped_port = _decode_xor_address(
                    resp["attrs"][ATTR_XOR_MAPPED_ADDRESS])
            return True
        elif resp:
            err = resp["attrs"].get(ATTR_ERROR_CODE, b"")
            if len(err) >= 4:
                code = err[2] * 100 + err[3]
                reason = err[4:].decode("utf-8", errors="replace")
                raise RuntimeError(f"TURN Allocate error {code}: {reason}")

        return False

    def create_permission(self, peer_ip):
        """Create permission for a peer IP."""
        addr_attr = _encode_attr(ATTR_XOR_PEER_ADDRESS,
                                 _encode_xor_address(peer_ip, 0))
        resp = self._stun_request(CREATE_PERM_REQUEST, addr_attr)
        if resp:
            if resp["type"] == CREATE_PERM_RESPONSE:
                return True
            err_attr = resp.get("attrs", {}).get(ATTR_ERROR_CODE, b"")
            if len(err_attr) >= 4:
                err_code = err_attr[2] * 100 + err_attr[3]
                print(f"[TURN] CreatePermission error for {peer_ip}: {err_code}")
            return False
        print(f"[TURN] CreatePermission timeout for {peer_ip}")
        return False

    def refresh(self, lifetime=600):
        """Send TURN Refresh to verify allocation is alive."""
        attrs = _encode_attr(ATTR_LIFETIME, struct.pack(">I", lifetime))
        resp = self._stun_request(REFRESH_REQUEST, attrs)
        if resp and resp["type"] == REFRESH_RESPONSE:
            return True
        if resp:
            err = resp["attrs"].get(ATTR_ERROR_CODE, b"")
            if len(err) >= 4:
                code = err[2] * 100 + err[3]
                print(f"[TURN] Refresh error: {code}")
        else:
            print(f"[TURN] Refresh timeout")
        return False

    def channel_bind(self, peer_ip, peer_port):
        """Bind a channel number to a peer address for efficient relay.

        Retries if the first response is not a ChannelBind response
        (can happen when ICE checks arrive interleaved).
        """
        ch = self._channel_counter
        self._channel_counter += 1

        attrs = _encode_attr(ATTR_CHANNEL_NUMBER, struct.pack(">HH", ch, 0))
        attrs += _encode_attr(ATTR_XOR_PEER_ADDRESS,
                              _encode_xor_address(peer_ip, peer_port))

        for attempt in range(3):
            resp = self._stun_request(CHANNEL_BIND_REQUEST, attrs)
            if resp and resp["type"] == CHANNEL_BIND_RESPONSE:
                self.channels[(peer_ip, peer_port)] = ch
                self.reverse_channels[ch] = (peer_ip, peer_port)
                return ch
            if resp:
                rtype = resp["type"]
                # Check for error response
                err_attr = resp.get("attrs", {}).get(ATTR_ERROR_CODE, b"")
                if len(err_attr) >= 4:
                    err_code = err_attr[2] * 100 + err_attr[3]
                    err_reason = err_attr[4:].decode("utf-8", errors="replace")
                    print(f"[TURN] ChannelBind error for {peer_ip}:{peer_port}: "
                          f"{err_code} {err_reason} (type=0x{rtype:04X})")
                elif rtype != CHANNEL_BIND_RESPONSE:
                    print(f"[TURN] ChannelBind unexpected response type=0x{rtype:04X} "
                          f"for {peer_ip}:{peer_port} (attempt {attempt+1})")
                if rtype in (CHANNEL_BIND_RESPONSE, 0x0119):
                    break  # Real response (success or error), don't retry
            else:
                print(f"[TURN] ChannelBind timeout for {peer_ip}:{peer_port} "
                      f"(attempt {attempt+1})")
        return None

    def send_to_peer(self, peer_ip, peer_port, data):
        """Send data to a peer through the TURN relay."""
        key = (peer_ip, peer_port)
        if key in self.channels:
            # ChannelData: [channel:2][length:2][data][padding]
            ch = self.channels[key]
            frame = struct.pack(">HH", ch, len(data)) + data
            if len(frame) % 4:
                frame += b'\x00' * (4 - len(frame) % 4)
            self._send(frame)
        else:
            # Send Indication (no auth needed for indications)
            attrs = _encode_attr(ATTR_XOR_PEER_ADDRESS,
                                 _encode_xor_address(peer_ip, peer_port))
            attrs += _encode_attr(ATTR_DATA, data)
            msg, _ = _build_stun(SEND_INDICATION, attrs)
            self._send(msg)

    def recv_data(self, timeout=5.0):
        """Receive data from TURN relay.

        Returns (data_bytes, peer_ip, peer_port) or (None, None, None) on timeout.
        """
        self.sock.settimeout(timeout)
        try:
            raw, addr = self.sock.recvfrom(65536)
        except socket.timeout:
            return None, None, None

        if len(raw) < 4:
            return raw, None, None

        # ChannelData: first 2 bits = 01 (channel numbers 0x4000-0x7FFF)
        if (raw[0] & 0xC0) == 0x40:
            ch, length = struct.unpack(">HH", raw[:4])
            peer = self.reverse_channels.get(ch)
            peer_ip = peer[0] if peer else None
            peer_port = peer[1] if peer else None
            return raw[4:4 + length], peer_ip, peer_port

        # STUN Data Indication
        msg = _parse_stun(raw)
        if msg and msg["type"] == DATA_INDICATION:
            data = msg["attrs"].get(ATTR_DATA, b"")
            peer_ip, peer_port = None, None
            if ATTR_XOR_PEER_ADDRESS in msg["attrs"]:
                peer_ip, peer_port = _decode_xor_address(
                    msg["attrs"][ATTR_XOR_PEER_ADDRESS])
            return data, peer_ip, peer_port

        # STUN response (e.g., binding response for ICE)
        if msg:
            return raw, None, None

        return raw, None, None

    def send_ice_binding(self, peer_ip, peer_port, local_ufrag, remote_ufrag,
                         remote_pwd, use_candidate=True):
        """Send ICE STUN Binding request through TURN relay.

        This is used for ICE connectivity checks. The binding request is
        sent as TURN data to the peer's address.
        """
        username = f"{remote_ufrag}:{local_ufrag}"
        # Build STUN Binding Request with ICE attributes
        attrs = _encode_attr(ATTR_USERNAME, username.encode())

        # PRIORITY attribute (0x0024)
        attrs += _encode_attr(0x0024, struct.pack(">I", 1862270975))

        # ICE-CONTROLLING (0x802A)
        attrs += _encode_attr(0x802A, struct.pack(">Q", int.from_bytes(os.urandom(8), "big")))

        # USE-CANDIDATE (0x0025) - empty attribute
        if use_candidate:
            attrs += _encode_attr(0x0025, b"")

        # Add MESSAGE-INTEGRITY with ICE password as key
        txn_id = os.urandom(12)
        ice_key = remote_pwd.encode()
        attrs = _add_integrity(BINDING_REQUEST, attrs, txn_id, ice_key)

        msg, _ = _build_stun(BINDING_REQUEST, attrs, txn_id)

        # Send through TURN relay
        self.send_to_peer(peer_ip, peer_port, msg)

    def close(self):
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None
