#!/usr/bin/env python3
"""
Meari msgsvr signaling protocol client.

Reverse-engineered from libppsdk.so (Ghidra decompilation).
Protocol: TCP with 8-byte header + 3DES-ECB encrypted JSON + checksum + tail.

Wire frame format:
  [0]    0xE6         magic
  [1]    node         0xA1=client_send
  [2]    method       0xB1=direct, 0xB2=routed
  [3]    cmd          0xC1=register, 0xC6=webrtc, 0xC7=status, 0xC9=heartbeat, 0xD1=connect
  [4]    type         0xD3 (default)
  [5]    0xE6         magic
  [6]    len_lo       payload length low byte
  [7]    len_hi|enc   payload length high 7 bits + bit7=encrypted
  [8..N] payload      3DES-ECB encrypted JSON (PKCS5 padded)
  [N+1]  checksum     additive sum of all payload bytes
  [N+2]  0x9D         tail marker

Encryption: 3DES ECB with key "__#!HIRCloud5.0!#__" (padded to 24 bytes with zeros).
"""

import base64
import json
import socket
import threading
import time
import uuid as uuid_mod

from .msgsvr_codec import (
    CMD_CONNECT,
    CMD_HEARTBEAT,
    CMD_REGISTER,
    CMD_STATUS,
    CMD_WEBRTC,
    METHOD_DIRECT,
    METHOD_ROUTED,
    NODE_CLIENT,
    build_frame as _build_frame,
    recv_frame as _recv_frame,
)
from .turn_client import close_socket


class MsgSvrClient:
    """Meari signaling (msgsvr) protocol client.

    Handles registration, device status queries, wake, TURN credential
    negotiation, and SDP offer/answer exchange via webrtcsvr.
    """

    def __init__(self, server_host, server_port=28974, *, session_index: int = 1):
        self.server_host = server_host
        self.server_port = server_port
        self.session_index = max(1, min(99, int(session_index or 1)))
        self.sock = None
        self.uuid = None
        self.token = None
        self.webrtcsvr = None  # {ip, port, domain, tag}
        self.heartbeat_interval_s = 30.0
        self._registered_at = time.monotonic()
        self._register_extra: dict[str, str | int] = {}
        self._io_lock = threading.Lock()

    def _session_suffix(self) -> str:
        return f"{self.session_index:02d}"

    def _session_id(self) -> str:
        return f"{self.uuid}:{self._session_suffix()}"

    def connect(self, timeout_s=10.0):
        """TCP connect to signaling server."""
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(float(timeout_s))
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        self.sock.connect((self.server_host, self.server_port))

    def _send_unlocked(self, method, cmd, payload_json):
        """Build and send a frame."""
        sock = self.sock
        if sock is None:
            raise ConnectionError("Signaling socket is closed")
        frame = _build_frame(NODE_CLIENT, method, cmd, payload_json)
        sock.sendall(frame)

    def _send(self, method, cmd, payload_json):
        with self._io_lock:
            self._send_unlocked(method, cmd, payload_json)

    def _recv(self):
        """Receive one frame."""
        with self._io_lock:
            sock = self.sock
            if sock is None:
                raise ConnectionError("Signaling socket is closed")
            return _recv_frame(sock)

    def _send_webrtc(self, inner_json, to_webrtcsvr=True):
        """Send a webrtcsvr-routed message with base64-encoded content."""
        content = base64.b64encode(
            json.dumps(inner_json, separators=(",", ":")).encode()
        ).decode()

        msg = {
            "from": {
                "node": "client",
                "domain": self.uuid,
                "transport": "tcp",
                "type": "binary",
            },
        }
        if to_webrtcsvr and self.webrtcsvr:
            msg["to"] = {
                "node": "webrtcsvr",
                "domain": self.webrtcsvr["domain"],
                "transport": "tcp",
                "type": "binary",
                "ip": self.webrtcsvr["ip"],
                "port": self.webrtcsvr["port"],
            }
        else:
            msg["node"] = "webrtcsvr"

        msg["content"] = content
        method = METHOD_ROUTED if to_webrtcsvr and self.webrtcsvr else METHOD_DIRECT
        self._send(method, CMD_WEBRTC, json.dumps(msg, separators=(",", ":")))

    def recv_webrtc_content(self):
        """Receive a webrtcsvr response and decode the inner content."""
        resp = self._recv()
        if resp["json"] and "content" in resp["json"]:
            inner_b64 = resp["json"]["content"]
            return json.loads(base64.b64decode(inner_b64))
        return resp["json"]

    # ---- High-level protocol steps ----

    def register(
        self,
        client_id,
        brand="77",
        app_ver="5.9.2a16",
        country="FR",
        sdk_ver: int = 15259,
        client_uuid: str | None = None,
    ):
        """Step 1: Register with signaling server. Returns uuid and token."""
        body = {
            "action": "register",
            "transport": "tcp",
            "type": "binary",
            "nat": {
                "local_port": self.sock.getsockname()[1] if self.sock else 0,
                "medium": [{"mode": "xts", "transport": "udp", "type": "binary"}],
            },
            "ver": int(sdk_ver),
            "runtime": 0,
            "extra_params": {
                "brand": brand,
                "clientid": str(client_id),
                "v": app_ver,
                "c": country,
            },
        }
        if client_uuid:
            body["uuid"] = str(client_uuid)
        msg = json.dumps(body, separators=(",", ":"))

        self._send(METHOD_DIRECT, CMD_REGISTER, msg)
        resp = self._recv()
        data = resp["json"]

        if data.get("result") != "OK":
            raise RuntimeError(f"Register failed: {data}")

        self.uuid = data["uuid"]
        self.token = data["token"]
        self.heartbeat_interval_s = float(data.get("hb") or 30.0)
        self._registered_at = time.monotonic()
        self._register_extra = {
            "brand": str(brand),
            "clientid": str(client_id),
            "v": str(app_ver),
            "c": str(country),
            "sdk_ver": int(sdk_ver),
        }
        return data

    def send_heartbeat(self, *, read_response: bool = False):
        """Keep the msgsvr session alive during long-running P2P streams."""
        if not self.uuid or not self.token:
            return
        msg = json.dumps(
            {
                "uuid": self.uuid,
                "token": self.token,
                "ver": int(self._register_extra.get("sdk_ver", 15259)),
                "runtime": int(max(0.0, time.monotonic() - self._registered_at)),
                "extra_params": {
                    key: value
                    for key, value in self._register_extra.items()
                    if key != "sdk_ver"
                },
            },
            separators=(",", ":"),
        )
        with self._io_lock:
            self._send_unlocked(METHOD_DIRECT, CMD_HEARTBEAT, msg)
            if not read_response or self.sock is None:
                return
            old_timeout = self.sock.gettimeout()
            try:
                self.sock.settimeout(1.0)
                _recv_frame(self.sock)
            except (OSError, ValueError):
                pass
            finally:
                try:
                    self.sock.settimeout(old_timeout)
                except OSError:
                    pass

    def webrtc_hello(self):
        """Step 2: Hello to webrtcsvr, get tag for later SDP exchange."""
        self._send_webrtc({"cmd": "hello"}, to_webrtcsvr=False)
        inner = self.recv_webrtc_content()

        # The response's outer "from" has the webrtcsvr address
        # We need to re-read from the raw frame to get the outer envelope
        # Actually, recv_webrtc_content already decoded the inner content
        # We need to capture the outer frame too.
        # Let's redo this properly.
        return inner

    def webrtc_hello_full(self):
        """Step 2: Hello to webrtcsvr, capturing full response."""
        inner, from_info = self._send_webrtc_hello()
        self.webrtcsvr = {
            "ip": from_info.get("ip"),
            "port": from_info.get("port"),
            "domain": from_info.get("domain", "webrtcsvr.eu"),
            "tag": inner.get("tag", ""),
        }
        return inner

    def _send_webrtc_hello(self):
        content = base64.b64encode(b'{"cmd":"hello"}').decode()
        msg = json.dumps(
            {
                "from": {
                    "node": "client",
                    "domain": self.uuid,
                    "transport": "tcp",
                    "type": "binary",
                },
                "node": "webrtcsvr",
                "content": content,
            },
            separators=(",", ":"),
        )

        self._send(METHOD_DIRECT, CMD_WEBRTC, msg)
        resp = self._recv()
        data = resp["json"]

        from_info = data.get("from", {})
        inner = json.loads(base64.b64decode(data.get("content", "")))
        return inner, from_info

    def query_device_status(self, device_uuid):
        """Step 3: Query device status. Returns status dict."""
        sid = uuid_mod.uuid4().hex[:16]
        msg = json.dumps(
            {
                "action": "status",
                "uuid": device_uuid,
                "sid": sid,
            },
            separators=(",", ":"),
        )

        self._send(METHOD_DIRECT, CMD_STATUS, msg)
        resp = self._recv()
        return resp["json"]

    def send_wake_connect(self, _device_uuid, device_contact, local_ips, local_port):
        """Step 4: Send connection/wake request to device."""
        outer_sid = uuid_mod.uuid4().hex[:16]
        msg = json.dumps(
            {
                "sid": outer_sid,
                "uuid": self.uuid,
                "params": {
                    "sid": uuid_mod.uuid4().hex[:8] + "00000001",
                    "local": {
                        "ip": local_ips,
                        "port": local_port,
                    },
                },
                "contact": device_contact,
            },
            separators=(",", ":"),
        )

        self._send(METHOD_DIRECT, CMD_CONNECT, msg)
        first = self._recv()["json"]

        msg = json.dumps(
            {
                "sid": outer_sid,
                "uuid": self.uuid,
                "params": {"attach": {"awaken_type": 1}},
                "contact": device_contact,
            },
            separators=(",", ":"),
        )
        self._send(METHOD_DIRECT, CMD_CONNECT, msg)
        return self._recv()["json"] or first

    def request_coturn(self, device_uuid):
        """Step 5: Request TURN credentials via webrtcsvr."""
        sid = self._session_id()
        self._send_webrtc(
            {
                "cmd": "option",
                "sid": sid,
                "caller": self.uuid,
                "callee": device_uuid,
                "method": "coturn",
            }
        )
        return self.recv_webrtc_content()

    def send_offer(self, device_uuid, sdp):
        """Step 6: Send SDP offer with ICE candidates."""
        sid = self._session_id()
        tag = self.webrtcsvr["tag"] + self._session_suffix()

        self._send_webrtc(
            {
                "cmd": "offer",
                "sid": sid,
                "caller": self.uuid,
                "callee": device_uuid,
                "channel": 0,
                "stream_type": 1,
                "sdp": sdp,
                "tag": tag,
            }
        )
        return self.recv_webrtc_content()

    def send_candidate_complete(self, device_uuid):
        """Step 7: Signal ICE candidate negotiation complete."""
        sid = self._session_id()
        self._send_webrtc(
            {
                "cmd": "candidate",
                "sid": sid,
                "caller": self.uuid,
                "callee": device_uuid,
                "candidate": {
                    "cmd": "nego",
                    "state": "completed",
                    "mode": 1,
                },
            }
        )
        return self.recv_webrtc_content()

    def send_logout(self, device_uuid):
        """Disconnect session."""
        sid = self._session_id()
        self._send_webrtc(
            {
                "cmd": "logout",
                "sid": sid,
                "caller": self.uuid,
                "callee": device_uuid,
            }
        )

    def wait_for_status(self, device_uuid, target="online", timeout=30):
        """Wait for device status to change (e.g., dormancy -> online)."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            self.sock.settimeout(max(1, deadline - time.time()))
            try:
                resp = self._recv()
                data = resp["json"]
                if data.get("action") == "status" and data.get("uuid") == device_uuid:
                    status = data.get("status", "")
                    if status == target:
                        return data
            except socket.timeout:
                continue
        return None

    def close(self):
        sock = self.sock
        self.sock = None
        close_socket(sock)
