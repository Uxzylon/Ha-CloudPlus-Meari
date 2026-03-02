"""P2P streaming engine for CloudPlus / Meari cameras.

Ported from main.py — handles signaling, TURN, ICE, KCP, VVP, and
stream decryption. Designed for use by the HA coordinator.

Usage:
    streamer = P2PStreamer(api, device, on_video=cb, on_audio=cb)
    streamer.run_session()   # blocking — call from a thread
    streamer.request_stop()  # from another thread
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import socket
import struct
import time
import traceback
from collections import deque
from typing import Any, Callable, Optional

from Crypto.Cipher import DES3

from .meari_signaling import MsgSvrClient
from .turn_client import (
    TurnClient,
    _parse_stun,
    _build_stun,
    _encode_attr,
    _add_integrity,
    _encode_xor_address,
    _decode_xor_address,
    BINDING_REQUEST,
    BINDING_RESPONSE,
    DATA_INDICATION,
    ATTR_USERNAME,
    ATTR_XOR_MAPPED_ADDRESS,
    ATTR_MESSAGE_INTEGRITY,
    MAGIC_COOKIE,
    ATTR_DATA,
    ATTR_XOR_PEER_ADDRESS,
)
from .kcp_tunnel import KcpTunnel, parse_kcp_segment, parse_iva_frame
from .api import MeariApiClient, format_sn

_LOGGER = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# VVP (PPStrong Video Protocol) constants
# ---------------------------------------------------------------------------
VVP_MAGIC = 0x56565099
VVP_CMD_START_LIVE = 0x11FF
VVP_CMD_STOP = 0x0001
VVP_CMD_HEARTBEAT = 0x888E
VVP_HEADER_SIZE = 60

# Stream frame types
STREAM_TYPE_INFO = 0xF9
STREAM_TYPE_AUDIO = 0xFA
STREAM_TYPE_IFRAME = 0xFC
STREAM_TYPE_PFRAME = 0xFD
STREAM_TYPE_PHOTO = 0xFE

# 3DES key for stream decryption
STREAM_ENCRYPT_KEY = b"!mearicloud2.0!"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def format_licence_id(sn: str) -> str:
    if not sn:
        return ""
    if len(sn) == 9:
        return "00000000000" + sn
    return sn


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


def _resolve_signaling_server() -> tuple[str, int]:
    candidates = [("47.254.142.96", 28974)]
    domain = "euce.mearicloud.com"
    try:
        ip = socket.gethostbyname(domain)
        candidates.append((ip, 28974))
        candidates.append((ip, 9253))
    except socket.gaierror:
        pass
    for ip, port in candidates:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2.0)
            s.connect((ip, port))
            s.close()
            return ip, port
        except (socket.timeout, ConnectionRefusedError, OSError):
            continue
    return "47.254.142.96", 28974


# ---------------------------------------------------------------------------
# VVP packet building
# ---------------------------------------------------------------------------

def build_vvp_auth_md5(
    host_key: str, seq: int, cmd: int, param: int,
    licence_id: str | None = None, auth_flag: int = 0,
) -> str:
    password = host_key if auth_flag == 1 else host_key[:16]
    parts = [
        "admin", password, str(VVP_MAGIC), str(seq),
        str(cmd), str(param), "meari.p2p.ppcs",
    ]
    if licence_id:
        parts.append(licence_id)
    return hashlib.md5("|".join(parts).encode()).hexdigest()


def build_vvp_packet(
    cmd: int, seq: int, host_key: str, param: int = 8,
    channel: int = 0, video_id: int = 0, quality: int = 0,
    licence_id: str | None = None, auth_flag: int = 0,
) -> bytes:
    auth = build_vvp_auth_md5(host_key, seq, cmd, param, licence_id, auth_flag)
    pkt = bytearray(VVP_HEADER_SIZE)
    struct.pack_into(">I", pkt, 0x00, VVP_MAGIC)
    struct.pack_into(">I", pkt, 0x04, 1)
    struct.pack_into(">I", pkt, 0x08, seq)
    struct.pack_into(">I", pkt, 0x0C, cmd)
    pkt[0x10:0x30] = auth.encode("ascii")
    struct.pack_into(">I", pkt, 0x30, param)
    struct.pack_into("<I", pkt, 0x34, channel)
    pkt[0x38] = video_id & 0xFF
    pkt[0x39] = 0x01
    pkt[0x3A] = quality & 0xFF
    pkt[0x3B] = 0x00
    return bytes(pkt)


# ---------------------------------------------------------------------------
# Stream frame decryption + parsing
# ---------------------------------------------------------------------------

def _des3_ecb_decrypt_block(data: bytes, key: bytes) -> bytes:
    k = key[:24]
    if len(k) < 24:
        k = k + b"\x00" * (24 - len(k))
    cipher = DES3.new(k, DES3.MODE_ECB)
    return cipher.decrypt(data)


def decrypt_stream_frame(data: bytearray) -> bytearray:
    if len(data) < 4:
        return data
    frame_type = data[3]
    if frame_type == STREAM_TYPE_IFRAME:
        enc_offset, enc_len = 0x30, 0x80
    elif frame_type == STREAM_TYPE_PFRAME:
        enc_offset, enc_len = 0x28, 0x80
    elif frame_type == STREAM_TYPE_AUDIO:
        enc_offset = 0x28
        remaining = len(data) - enc_offset
        enc_len = (remaining // 8) * 8
    else:
        return data
    if enc_len < 8 or len(data) < enc_offset + enc_len:
        return data
    encrypted = bytes(data[enc_offset : enc_offset + enc_len])
    decrypted = _des3_ecb_decrypt_block(encrypted, STREAM_ENCRYPT_KEY)
    data[enc_offset : enc_offset + enc_len] = decrypted
    return data


def parse_stream_frame(data: bytes):
    if len(data) < 8:
        return None
    if data[0] != 0 or data[1] != 0 or data[2] != 1:
        return None
    frame_type = data[3]
    if frame_type == STREAM_TYPE_IFRAME:
        if len(data) < 0x3C:
            return None
        data_len = struct.unpack_from("<I", data, 0x38)[0]
        payload = data[0x3C : 0x3C + data_len] if data_len > 0 else data[0x3C:]
        return (frame_type, 0x3C, payload)
    elif frame_type == STREAM_TYPE_PFRAME:
        if len(data) < 0x34:
            return None
        data_len = struct.unpack_from("<I", data, 0x30)[0]
        payload = data[0x34 : 0x34 + data_len] if data_len > 0 else data[0x34:]
        return (frame_type, 0x34, payload)
    elif frame_type == STREAM_TYPE_AUDIO:
        if len(data) < 0x34:
            return None
        data_len = struct.unpack_from("<I", data, 0x30)[0]
        payload = data[0x34 : 0x34 + data_len] if data_len > 0 else data[0x34:]
        return (frame_type, 0x34, payload)
    elif frame_type == STREAM_TYPE_INFO:
        if len(data) < 8:
            return None
        data_len = struct.unpack_from("<H", data, 6)[0]
        payload = data[8 : 8 + data_len] if data_len > 0 else data[8:]
        return (frame_type, 8, payload)
    return None


# ---------------------------------------------------------------------------
# ICE helpers
# ---------------------------------------------------------------------------

def _build_ice_response(binding_req: dict, local_ice_pwd: str,
                        peer_ip: str, peer_port: int) -> bytes:
    xor_addr = _encode_xor_address(peer_ip, peer_port)
    attrs = _encode_attr(ATTR_XOR_MAPPED_ADDRESS, xor_addr)
    txn_id = binding_req["txn_id"]
    ice_key = local_ice_pwd.encode()
    attrs = _add_integrity(BINDING_RESPONSE, attrs, txn_id, ice_key)
    msg, _ = _build_stun(BINDING_RESPONSE, attrs, txn_id)
    return msg


def _send_direct_ice_binding(sock, peer_ip: str, peer_port: int,
                              local_ufrag: str, remote_ufrag: str,
                              remote_pwd: str) -> None:
    username = f"{remote_ufrag}:{local_ufrag}"
    attrs = _encode_attr(ATTR_USERNAME, username.encode())
    attrs += _encode_attr(0x0024, struct.pack(">I", 1862270975))
    attrs += _encode_attr(0x802A, struct.pack(">Q",
        int.from_bytes(os.urandom(8), "big")))
    attrs += _encode_attr(0x0025, b"")
    txn_id = os.urandom(12)
    ice_key = remote_pwd.encode()
    attrs = _add_integrity(BINDING_REQUEST, attrs, txn_id, ice_key)
    msg, _ = _build_stun(BINDING_REQUEST, attrs, txn_id)
    sock.sendto(msg, (peer_ip, peer_port))


# ---------------------------------------------------------------------------
# P2PStreamer — manages one streaming session
# ---------------------------------------------------------------------------

class P2PStreamer:
    """Runs P2P streaming sessions for a CloudPlus camera.

    Callbacks:
      on_video(data: bytes)  — raw HEVC video payload (I/P frame)
      on_audio(data: bytes)  — raw G.711 µ-law audio payload
      on_login()             — called when VVP login succeeds
      on_disconnect()        — called when session ends
    """

    def __init__(
        self,
        api: MeariApiClient,
        device: dict[str, Any],
        *,
        on_video: Callable[[bytes], None] | None = None,
        on_audio: Callable[[bytes], None] | None = None,
        on_login: Callable[[], None] | None = None,
        on_disconnect: Callable[[], None] | None = None,
        remote: bool = False,
    ) -> None:
        self._api = api
        self._device = device
        self._sn_num = device["snNum"]
        self._device_uuid = format_sn(self._sn_num)
        self._host_key = device.get("hostKey", "")
        self._dev_name = device.get("deviceName", "Camera")

        self.on_video = on_video
        self.on_audio = on_audio
        self.on_login = on_login
        self.on_disconnect = on_disconnect
        self._remote = remote

        self._running = False
        self._video_count = 0
        self._total_bytes = 0

    def request_stop(self) -> None:
        """Request the streaming loop to stop (thread-safe)."""
        self._running = False

    @property
    def video_count(self) -> int:
        return self._video_count

    # ------------------------------------------------------------------
    # Main entry point — call from a thread
    # ------------------------------------------------------------------

    def run_session(self) -> tuple[int, int]:
        """Run one P2P streaming session. Returns (video_frames, total_bytes).

        Blocks until the session ends (camera sleeps, error, or stop requested).
        For battery cameras call this in a reconnect loop.
        """
        self._running = True
        self._video_count = 0
        self._total_bytes = 0

        sig = None
        try:
            sig_ip, sig_port = _resolve_signaling_server()
            _LOGGER.debug("Connecting to signaling %s:%d", sig_ip, sig_port)
            sig = MsgSvrClient(sig_ip, sig_port)
            sig.connect()

            v, b = self._do_stream(sig)
            self._video_count = v
            self._total_bytes = b
            return (v, b)
        except Exception as e:
            _LOGGER.error("P2P session error: %s", e)
            return (self._video_count, self._total_bytes)
        finally:
            if sig:
                try:
                    sig.send_logout(self._device_uuid)
                except Exception:
                    pass
                sig.close()
            if self.on_disconnect:
                try:
                    self.on_disconnect()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # Internal streaming pipeline
    # ------------------------------------------------------------------

    def _do_stream(self, sig: MsgSvrClient) -> tuple[int, int]:
        """Internal: full P2P pipeline. Returns (video_count, bytes)."""
        api = self._api
        device_uuid = self._device_uuid
        host_key = self._host_key
        sn_num = self._sn_num
        remote = self._remote

        # Register
        reg = sig.register(
            client_id=api.user_id,
            brand="77",
            country=api.country_code,
        )

        # Hello webrtcsvr
        sig.webrtc_hello_full()

        # Query device status
        status = sig.query_device_status(device_uuid)
        dev_status = status.get("status", "unknown")
        dev_contact = status.get("contact", {})
        dev_nat = status.get("nat", {})

        # Wake if dormant
        if dev_status == "dormancy":
            _LOGGER.info("Camera dormant, waking...")
            keepalive = dev_contact.get("keepalive", {})
            local_ips = _get_local_ips()
            sig.send_wake_connect(device_uuid, keepalive, local_ips, 16685)
            try:
                api.wake_device(sn_num, self._device.get("deviceID", 0))
            except Exception:
                pass
            online_status = sig.wait_for_status(device_uuid, "online", timeout=30)
            if online_status:
                dev_status = "online"
                dev_contact = online_status.get("contact", dev_contact)
                dev_nat = online_status.get("nat", dev_nat)
            else:
                _LOGGER.warning("Camera did not come online")
                return (0, 0)

        if dev_status != "online":
            _LOGGER.warning("Camera not online (status=%s)", dev_status)
            return (0, 0)

        # Request TURN credentials
        coturn = sig.request_coturn(device_uuid)
        coturn_ip = coturn.get("coturn_ip", "")
        coturn_port = coturn.get("coturn_port", 9100)
        coturn_user = coturn.get("username", "")
        coturn_pwd = coturn.get("pwd", "")

        # Allocate TURN relay
        turn = TurnClient(coturn_ip, coturn_port, coturn_user, coturn_pwd)
        turn.connect()
        if not turn.allocate():
            _LOGGER.error("TURN allocation failed")
            turn.close()
            return (0, 0)

        try:
            return self._stream_with_turn(
                sig, turn, device_uuid, host_key, sn_num,
                dev_nat, coturn_ip, remote,
            )
        finally:
            turn.close()

    def _stream_with_turn(
        self, sig, turn, device_uuid, host_key, sn_num,
        dev_nat, coturn_ip, remote,
    ) -> tuple[int, int]:
        """SDP exchange, ICE, KCP, VVP — the core streaming loop."""
        api = self._api
        local_ips = _get_local_ips()
        ice_ufrag = os.urandom(4).hex()
        ice_pwd = os.urandom(12).hex()

        # Build SDP offer
        sdp_lines = [
            "v=0",
            f"o=- {int(time.time())} {int(time.time())} IN IP4 0.0.0.0",
            "s=ice", "t=0 0",
            f"a=ice-ufrag:{ice_ufrag}", f"a=ice-pwd:{ice_pwd}",
            f"m=audio {turn.relay_port} RTP / AVP 0",
            f"c=IN IP4 {turn.relay_ip}",
        ]
        if not remote:
            for lip in local_ips:
                ip_hex = socket.inet_aton(lip).hex()
                sdp_lines.append(
                    f"a=candidate:H{ip_hex} 1 UDP 1694498815 "
                    f"{lip} {turn.local_port} typ host"
                )
        if not remote and turn.mapped_ip:
            ip_hex = socket.inet_aton(local_ips[0]).hex()
            sdp_lines.append(
                f"a=candidate:S{ip_hex} 1 UDP 1862270975 "
                f"{turn.mapped_ip} {turn.mapped_port} typ srflx"
            )
        if turn.relay_ip:
            relay_hex = socket.inet_aton(turn.relay_ip).hex()
            sdp_lines.append(
                f"a=candidate:R{relay_hex} 1 UDP 16777215 "
                f"{turn.relay_ip} {turn.relay_port} typ srflx"
            )
        sdp = "\n".join(sdp_lines) + "\n"

        # Send SDP offer
        answer = sig.send_offer(device_uuid, sdp)

        # Parse camera SDP answer
        camera_sdp = answer.get("sdp", "")
        camera_ufrag = ""
        camera_pwd = ""
        camera_candidates: list[dict] = []
        camera_sdp_ip = ""
        camera_sdp_port = 0

        for line in camera_sdp.replace("\\n", "\n").split("\n"):
            line = line.strip()
            if line.startswith("a=ice-ufrag:"):
                camera_ufrag = line.split(":", 1)[1].strip()
            elif line.startswith("a=ice-pwd:"):
                camera_pwd = line.split(":", 1)[1].strip()
            elif line.startswith("c=IN IP4 "):
                camera_sdp_ip = line.split("c=IN IP4 ")[1].strip()
            elif line.startswith("m=audio "):
                try:
                    camera_sdp_port = int(line.split()[1])
                except (IndexError, ValueError):
                    pass
            elif line.startswith("a=candidate:"):
                parts = line.split()
                if len(parts) >= 8:
                    cand = {"ip": parts[4], "port": int(parts[5]), "type": parts[7]}
                    for i in range(8, len(parts) - 1, 2):
                        if parts[i] == "raddr":
                            cand["raddr"] = parts[i + 1]
                        elif parts[i] == "rport":
                            cand["rport"] = int(parts[i + 1])
                    camera_candidates.append(cand)

        # Synthesize relay candidate from SDP c=/m=
        has_relay = any(c["type"] == "relay" for c in camera_candidates)
        if not has_relay and camera_sdp_ip and camera_sdp_port:
            camera_candidates.append({
                "ip": camera_sdp_ip, "port": camera_sdp_port, "type": "relay",
            })

        # Read trickled candidates
        try:
            sig.sock.settimeout(2.0)
            for _ in range(5):
                try:
                    extra = sig._recv_webrtc_content()
                    if isinstance(extra, dict):
                        extra_sdp = extra.get("sdp", "")
                        extra_cand = extra.get("candidate", {})
                        if extra_sdp:
                            for ln in extra_sdp.replace("\\n", "\n").split("\n"):
                                ln = ln.strip()
                                if ln.startswith("a=candidate:"):
                                    pts = ln.split()
                                    if len(pts) >= 8:
                                        camera_candidates.append({
                                            "ip": pts[4], "port": int(pts[5]),
                                            "type": pts[7],
                                        })
                        if extra_cand and isinstance(extra_cand, dict):
                            cip = extra_cand.get("ip")
                            cport = extra_cand.get("port")
                            if cip and cport:
                                camera_candidates.append({
                                    "ip": cip, "port": int(cport),
                                    "type": extra_cand.get("type", "relay"),
                                })
                            elif extra_cand.get("state") == "completed":
                                break
                except socket.timeout:
                    break
                except Exception:
                    break
            sig.sock.settimeout(10.0)
        except Exception:
            pass

        # TURN permissions + channel binds
        camera_wan_ip = dev_nat.get("wan_ip", "")
        perm_ips = {c["ip"] for c in camera_candidates}
        if camera_wan_ip:
            perm_ips.add(camera_wan_ip)
        perm_ips.add(coturn_ip)
        turn.drain_socket()
        for pip in perm_ips:
            turn.create_permission(pip)
        turn.drain_socket()
        for c in camera_candidates:
            turn.channel_bind(c["ip"], c["port"])
        turn.refresh()

        # Send candidate_complete
        cand_resp = sig.send_candidate_complete(device_uuid)
        if isinstance(cand_resp, dict):
            cr_sdp = cand_resp.get("sdp", "")
            if cr_sdp:
                for ln in cr_sdp.replace("\\n", "\n").split("\n"):
                    ln = ln.strip()
                    if ln.startswith("a=candidate:"):
                        pts = ln.split()
                        if len(pts) >= 8:
                            tc = {"ip": pts[4], "port": int(pts[5]), "type": pts[7]}
                            if tc not in camera_candidates:
                                camera_candidates.append(tc)
                                turn.create_permission(tc["ip"])
                                turn.channel_bind(tc["ip"], tc["port"])

        # Pick target candidate
        camera_relay = camera_srflx = camera_host = None
        for c in camera_candidates:
            if c["type"] == "relay":
                camera_relay = c
            elif c["type"] == "srflx":
                camera_srflx = c
            elif c["type"] == "host":
                camera_host = c
        target_candidate = camera_relay or camera_srflx or camera_host
        if not target_candidate:
            _LOGGER.error("No camera candidate found")
            return (0, 0)
        target_ip = target_candidate["ip"]
        target_port = target_candidate["port"]

        # ICE + KCP + VVP combined phase
        camera_addrs = {(c["ip"], c["port"]) for c in camera_candidates}
        confirmed_peer: list = [None]
        send_addr_holder: list = [None]

        def _udp_send(data):
            if confirmed_peer[0]:
                cp_ip, cp_port, is_direct = confirmed_peer[0]
                if is_direct:
                    try:
                        turn.sock.sendto(data, (cp_ip, cp_port))
                    except Exception:
                        pass
                else:
                    try:
                        turn.send_to_peer(cp_ip, cp_port, data)
                    except Exception:
                        pass
                return
            sent_to: set = set()
            for c in camera_candidates:
                key = (c["ip"], c["port"])
                if key in sent_to:
                    continue
                sent_to.add(key)
                try:
                    turn.send_to_peer(c["ip"], c["port"], data)
                except Exception:
                    pass
            if not remote and send_addr_holder[0] and _is_private_ip(send_addr_holder[0][0]):
                try:
                    turn.sock.sendto(data, send_addr_holder[0])
                except Exception:
                    pass

        kcp = KcpTunnel(_udp_send)

        def _send_ice_checks():
            for c in camera_candidates:
                turn.send_ice_binding(
                    c["ip"], c["port"], ice_ufrag, camera_ufrag, camera_pwd)
                if not remote and _is_private_ip(c["ip"]):
                    _send_direct_ice_binding(
                        turn.sock, c["ip"], c["port"],
                        ice_ufrag, camera_ufrag, camera_pwd)

        _send_ice_checks()

        # VVP login
        licence_id = format_licence_id(sn_num) if sn_num else None
        vvp_seq = 0
        vvp_login = build_vvp_packet(
            cmd=VVP_CMD_START_LIVE, seq=vvp_seq, host_key=host_key,
            param=8, licence_id=licence_id,
        )
        vvp_seq += 1
        kcp.send_handshake()
        kcp.send_iva_data(vvp_login)

        # State
        ice_deadline = time.time() + 30
        ice_resend_at = time.time() + 2
        login_resend_at = time.time() + 2
        heartbeat_at = time.time() + 3
        iva_heartbeat_at = time.time() + 3
        turn_refresh_at = time.time() + 60
        stun_keepalive_at = time.time() + 5

        ice_count = 0
        confirmed_addr = None
        request_addrs: set = set()
        got_iva_handshake = False
        login_ok = False
        direct_addr = None
        turn.sock.settimeout(0.5)

        stream_frame_count = 0
        stream_video_count = 0
        stream_total_bytes = 0
        stream_start_time = time.time()
        last_video_time = None
        last_kcp_data_time = None
        last_nudge_time = 0.0
        last_skip_time = 0.0
        kcp_push_count = 0

        def _handle_stream_payload(payload):
            nonlocal stream_frame_count, stream_video_count, stream_total_bytes
            nonlocal last_video_time
            if not payload or len(payload) < 4:
                return True
            if payload[0] != 0 or payload[1] != 0 or payload[2] != 1:
                return True
            frame_type = payload[3]
            stream_frame_count += 1
            if frame_type in (STREAM_TYPE_IFRAME, STREAM_TYPE_PFRAME, STREAM_TYPE_AUDIO):
                decrypted = decrypt_stream_frame(bytearray(payload))
                parsed = parse_stream_frame(bytes(decrypted))
            else:
                parsed = parse_stream_frame(payload)
            if parsed:
                ftype, _, media_data = parsed
                if ftype in (STREAM_TYPE_IFRAME, STREAM_TYPE_PFRAME):
                    stream_video_count += 1
                    stream_total_bytes += len(media_data)
                    last_video_time = time.time()
                    if self.on_video:
                        self.on_video(media_data)
                    return True
                elif ftype == STREAM_TYPE_AUDIO:
                    if self.on_audio:
                        self.on_audio(media_data)
                    return True
            return True

        def _drain_kcp_queue():
            while True:
                queued = kcp.poll_data()
                if not queued:
                    break
                qpayload = queued
                if len(queued) >= 20 and queued[0] == 0xFF and queued[1] == 0x01:
                    qiva = parse_iva_frame(queued)
                    if qiva:
                        qt, _, _, qp = qiva
                        if qt == 0x7012:
                            continue
                        qpayload = qp
                if qpayload:
                    _handle_stream_payload(qpayload)

        _packet_buf: deque = deque()

        while self._running and time.time() < ice_deadline:
            now = time.time()

            # Heartbeats
            if login_ok and now >= heartbeat_at:
                hb = build_vvp_packet(
                    cmd=VVP_CMD_HEARTBEAT, seq=vvp_seq,
                    host_key=host_key, param=8, licence_id=licence_id)
                kcp.send_iva_data(hb)
                vvp_seq += 1
                heartbeat_at = now + 10

            if login_ok and now >= iva_heartbeat_at:
                kcp.send_handshake()
                iva_heartbeat_at = now + 3

            if login_ok and now >= turn_refresh_at:
                try:
                    turn.refresh(lifetime=600)
                except Exception:
                    pass
                turn_refresh_at = now + 60

            if not login_ok and now >= stun_keepalive_at:
                try:
                    keepalive_msg, _ = _build_stun(BINDING_REQUEST, b"")
                    turn.sock.sendto(keepalive_msg, (turn.server_ip, turn.server_port))
                except Exception:
                    pass
                stun_keepalive_at = now + 10

            # Batch drain
            if not _packet_buf:
                kcp.flush_acks()
                try:
                    raw, addr = turn.sock.recvfrom(65536)
                    _packet_buf.append((raw, addr))
                    turn.sock.setblocking(False)
                    try:
                        for _ in range(2000):
                            r2, a2 = turn.sock.recvfrom(65536)
                            _packet_buf.append((r2, a2))
                    except (BlockingIOError, OSError):
                        pass
                    finally:
                        turn.sock.setblocking(True)
                        turn.sock.settimeout(0.5)
                except socket.timeout:
                    if now >= ice_resend_at:
                        _send_ice_checks()
                        kcp.retransmit_unacked()
                        ice_resend_at = now + 2
                    if now >= login_resend_at:
                        kcp.retransmit_unacked()
                        login_resend_at = now + 5
                    continue

            raw, addr = _packet_buf.popleft()
            if len(raw) < 4:
                continue

            data = raw
            source_addr = addr
            via_turn = False

            # Unwrap TURN framing
            if (raw[0] & 0xC0) == 0x40:
                ch_num, length = struct.unpack(">HH", raw[:4])
                data = raw[4 : 4 + length]
                peer = turn.reverse_channels.get(ch_num)
                if peer:
                    source_addr = peer
                via_turn = True
                inner_stun = _parse_stun(data)
                if inner_stun:
                    if inner_stun["type"] == BINDING_REQUEST:
                        pip, pport = peer if peer else (addr[0], addr[1])
                        resp = _build_ice_response(inner_stun, ice_pwd, pip, pport)
                        turn.send_to_peer(pip, pport, resp)
                        ice_count += 1
                        request_addrs.add((pip, pport))
                        continue
                    elif inner_stun["type"] == BINDING_RESPONSE:
                        pip, pport = peer if peer else (addr[0], addr[1])
                        confirmed_addr = (pip, pport)
                        ice_count += 1
                        kcp.retransmit_unacked()
                        continue
            elif raw[0:2] != b"\xff\x01":
                msg = _parse_stun(raw)
                if msg:
                    if msg["type"] == DATA_INDICATION:
                        inner_data = msg["attrs"].get(ATTR_DATA, b"")
                        pip, pport = None, None
                        if ATTR_XOR_PEER_ADDRESS in msg["attrs"]:
                            pip, pport = _decode_xor_address(msg["attrs"][ATTR_XOR_PEER_ADDRESS])
                        inner_msg = _parse_stun(inner_data)
                        if inner_msg and inner_msg["type"] == BINDING_REQUEST:
                            resp = _build_ice_response(inner_msg, ice_pwd, pip, pport)
                            turn.send_to_peer(pip, pport, resp)
                            ice_count += 1
                            request_addrs.add((pip, pport))
                            if pip and pport and (pip, pport) != (target_ip, target_port):
                                target_ip, target_port = pip, pport
                                if (pip, pport) not in turn.channels:
                                    turn.channel_bind(pip, pport)
                            continue
                        elif inner_msg and inner_msg["type"] == BINDING_RESPONSE:
                            confirmed_addr = (pip, pport) if pip else addr
                            ice_count += 1
                            if pip and pport:
                                target_ip, target_port = pip, pport
                                if (pip, pport) not in turn.channels:
                                    turn.channel_bind(pip, pport)
                            continue
                        data = inner_data
                        if pip:
                            source_addr = (pip, pport)
                            if (pip, pport) != (target_ip, target_port):
                                target_ip, target_port = pip, pport
                                if (pip, pport) not in turn.channels:
                                    turn.create_permission(pip)
                                    turn.channel_bind(pip, pport)
                        via_turn = True
                    elif msg["type"] == BINDING_REQUEST:
                        resp = _build_ice_response(msg, ice_pwd, addr[0], addr[1])
                        if not remote and _is_private_ip(addr[0]):
                            turn.sock.sendto(resp, addr)
                        else:
                            turn.send_to_peer(addr[0], addr[1], resp)
                        ice_count += 1
                        request_addrs.add(addr)
                        continue
                    elif msg["type"] == BINDING_RESPONSE:
                        if addr[0] == turn.server_ip:
                            continue
                        confirmed_addr = addr
                        if not remote and not send_addr_holder[0] and addr[0] != coturn_ip:
                            send_addr_holder[0] = addr
                            direct_addr = addr
                            kcp.retransmit_unacked()
                        continue
                    else:
                        continue

            # KCP processing
            kcp_seg = parse_kcp_segment(data)
            if kcp_seg:
                result = kcp.process_input(data)
                kcp.flush_acks()
                if kcp_seg["cmd"] == 81:
                    kcp_push_count += 1
                    last_kcp_data_time = time.time()
                    if not confirmed_peer[0] and source_addr:
                        cp_ip, cp_port = source_addr
                        is_direct = not via_turn and not remote and _is_private_ip(cp_ip)
                        confirmed_peer[0] = (cp_ip, cp_port, is_direct)
                if result:
                    rtype, rdata = result
                    if rtype == "handshake":
                        got_iva_handshake = True
                        if not direct_addr:
                            if confirmed_addr:
                                direct_addr = confirmed_addr
                            else:
                                matching = request_addrs & camera_addrs
                                if matching:
                                    direct_addr = matching.pop()
                            if direct_addr and direct_addr[0] != coturn_ip:
                                send_addr_holder[0] = direct_addr
                    elif rtype == "data" and rdata:
                        payload = rdata
                        if len(rdata) >= 20 and rdata[0] == 0xFF and rdata[1] == 0x01:
                            iva = parse_iva_frame(rdata)
                            if iva:
                                tmark, _, _, ipayload = iva
                                if tmark == 0x7012:
                                    continue
                                payload = ipayload
                        if payload:
                            if not login_ok:
                                login_ok = True
                                stream_start_time = time.time()
                                if self.on_login:
                                    self.on_login()
                                hb = build_vvp_packet(
                                    cmd=VVP_CMD_HEARTBEAT, seq=vvp_seq,
                                    host_key=host_key, param=8,
                                    licence_id=licence_id)
                                kcp.send_iva_data(hb)
                                vvp_seq += 1
                            ice_deadline = max(ice_deadline, time.time() + 30)
                            _handle_stream_payload(payload)
                        _drain_kcp_queue()

                # Stall recovery + deadline management
                if login_ok:
                    kcp_alive = last_kcp_data_time and time.time() - last_kcp_data_time < 20
                    if not kcp_alive and last_kcp_data_time:
                        break
                    if last_video_time and time.time() - last_video_time < 10:
                        ice_deadline = max(ice_deadline, time.time() + 15)
                    elif kcp_alive:
                        ice_deadline = max(ice_deadline, time.time() + 10)
                    if last_video_time and time.time() - last_video_time > 0.2:
                        if time.time() - last_nudge_time > 0.2:
                            kcp.send_gap_nudge()
                            last_nudge_time = time.time()
                        stall_time = time.time() - last_video_time
                        if stall_time > 0.8 and time.time() - last_skip_time > 0.8 and kcp.skip_gap():
                            last_skip_time = time.time()
                            kcp.flush_acks()
                            _drain_kcp_queue()
                continue

            # Raw IVA frame
            if len(data) >= 2 and data[0] == 0xFF and data[1] == 0x01:
                iva = parse_iva_frame(data)
                if iva and iva[0] == 0x7012:
                    got_iva_handshake = True
                continue

        # Connection loop done — enter continuation receiver if we got video
        if not login_ok:
            _LOGGER.warning("VVP login failed")
            return (stream_video_count, stream_total_bytes)

        if last_video_time and time.time() - last_video_time > 10:
            return (stream_video_count, stream_total_bytes)

        # Continuation receiver
        v2, b2 = self._receive_stream(
            turn, kcp, ice_pwd, host_key, licence_id,
            vvp_seq, stream_frame_count, stream_video_count,
            stream_total_bytes, stream_start_time,
        )
        return (v2, b2)

    # ------------------------------------------------------------------
    # Continuation receiver
    # ------------------------------------------------------------------

    def _receive_stream(
        self, turn, kcp, ice_pwd, host_key, licence_id,
        vvp_seq_start, frame_count_start, video_count_start,
        bytes_start, start_time,
    ) -> tuple[int, int]:
        """Continue receiving video after the connection loop."""
        frame_count = frame_count_start
        video_frame_count = video_count_start
        total_bytes = bytes_start
        last_video_time: float | None = None
        last_kcp_data_time = time.time()
        last_nudge_time = 0.0
        last_skip_time = 0.0
        vvp_seq = vvp_seq_start
        last_heartbeat = time.time()
        last_iva_heartbeat = time.time()
        last_turn_refresh = time.time()

        def _process_kcp_message(msg_data):
            nonlocal frame_count, video_frame_count, total_bytes, last_video_time
            payload = msg_data
            if len(msg_data) >= 20 and msg_data[0] == 0xFF and msg_data[1] == 0x01:
                iva = parse_iva_frame(msg_data)
                if iva:
                    tmark, _, _, ipayload = iva
                    if tmark == 0x7012:
                        return True
                    payload = ipayload
            if not payload or len(payload) < 4:
                return True
            if payload[0] == 0 and payload[1] == 0 and payload[2] == 1:
                frame_type = payload[3]
                frame_count += 1
                if frame_type in (STREAM_TYPE_IFRAME, STREAM_TYPE_PFRAME, STREAM_TYPE_AUDIO):
                    decrypted = decrypt_stream_frame(bytearray(payload))
                    parsed = parse_stream_frame(bytes(decrypted))
                else:
                    parsed = parse_stream_frame(payload)
                if parsed:
                    ftype, _, media_data = parsed
                    if ftype in (STREAM_TYPE_IFRAME, STREAM_TYPE_PFRAME):
                        video_frame_count += 1
                        total_bytes += len(media_data)
                        last_video_time = time.time()
                        if self.on_video:
                            self.on_video(media_data)
                        return True
                    elif ftype == STREAM_TYPE_AUDIO:
                        if self.on_audio:
                            self.on_audio(media_data)
                        return True
            return True

        # Drain queued KCP messages
        while True:
            queued = kcp.poll_data()
            if not queued:
                break
            _process_kcp_message(queued)

        # Send initial heartbeat
        if host_key:
            hb = build_vvp_packet(
                cmd=VVP_CMD_HEARTBEAT, seq=vvp_seq,
                host_key=host_key, param=8, licence_id=licence_id)
            kcp.send_iva_data(hb)
            vvp_seq += 1

        _recv_buf: deque = deque()
        timeout_count = 0

        while self._running:
            if not _recv_buf:
                kcp.flush_acks()
                turn.sock.settimeout(0.5)
                try:
                    raw, addr = turn.sock.recvfrom(65536)
                    _recv_buf.append((raw, addr))
                    turn.sock.setblocking(False)
                    try:
                        for _ in range(2000):
                            r2, a2 = turn.sock.recvfrom(65536)
                            _recv_buf.append((r2, a2))
                    except (BlockingIOError, OSError):
                        pass
                    finally:
                        turn.sock.setblocking(True)
                        turn.sock.settimeout(0.5)
                except socket.timeout:
                    now = time.time()
                    if host_key and now - last_heartbeat >= 10:
                        hb = build_vvp_packet(
                            cmd=VVP_CMD_HEARTBEAT, seq=vvp_seq,
                            host_key=host_key, param=8, licence_id=licence_id)
                        kcp.send_iva_data(hb)
                        vvp_seq += 1
                        last_heartbeat = now
                    if now - last_iva_heartbeat >= 3:
                        kcp.send_handshake()
                        last_iva_heartbeat = now
                    if now - last_turn_refresh > 60:
                        try:
                            turn.refresh(lifetime=600)
                        except Exception:
                            pass
                        last_turn_refresh = now
                    if now - start_time > 10 and video_frame_count == 0 and frame_count == 0:
                        timeout_count += 1
                        if timeout_count >= 60:
                            break
                    if last_kcp_data_time and now - last_kcp_data_time > 20:
                        _LOGGER.debug("No KCP data for 20s, ending session")
                        break
                    if last_video_time and now - last_video_time > 0.2:
                        if now - last_nudge_time > 0.2:
                            kcp.send_gap_nudge()
                            last_nudge_time = now
                        stall = now - last_video_time
                        if stall > 0.8 and now - last_skip_time > 0.8 and kcp.skip_gap():
                            last_skip_time = now
                            kcp.flush_acks()
                            while True:
                                q = kcp.poll_data()
                                if not q:
                                    break
                                _process_kcp_message(q)
                    continue

            raw, addr = _recv_buf.popleft()
            timeout_count = 0
            if len(raw) < 4:
                continue

            # Unwrap TURN framing
            data = raw
            if (raw[0] & 0xC0) == 0x40:
                ch, length = struct.unpack(">HH", raw[:4])
                data = raw[4 : 4 + length]
                inner_stun = _parse_stun(data)
                if inner_stun:
                    if inner_stun["type"] == BINDING_REQUEST and ice_pwd:
                        peer = turn.reverse_channels.get(ch)
                        pip, pport = peer if peer else (addr[0], addr[1])
                        resp = _build_ice_response(inner_stun, ice_pwd, pip, pport)
                        turn.send_to_peer(pip, pport, resp)
                        continue
                    elif inner_stun["type"] == BINDING_RESPONSE:
                        continue
            elif raw[0:2] != b"\xff\x01":
                msg = _parse_stun(raw)
                if msg:
                    if msg["type"] == DATA_INDICATION:
                        inner = msg["attrs"].get(ATTR_DATA, b"")
                        pip, pport = None, None
                        if ATTR_XOR_PEER_ADDRESS in msg["attrs"]:
                            pip, pport = _decode_xor_address(msg["attrs"][ATTR_XOR_PEER_ADDRESS])
                        inner_msg = _parse_stun(inner)
                        if inner_msg and inner_msg["type"] == BINDING_REQUEST and ice_pwd:
                            resp = _build_ice_response(inner_msg, ice_pwd, pip, pport)
                            turn.send_to_peer(pip, pport, resp)
                            continue
                        data = inner
                    elif msg["type"] == BINDING_REQUEST and ice_pwd:
                        resp = _build_ice_response(msg, ice_pwd, addr[0], addr[1])
                        if _is_private_ip(addr[0]):
                            turn.sock.sendto(resp, addr)
                        else:
                            turn.send_to_peer(addr[0], addr[1], resp)
                        continue
                    else:
                        continue

            if len(data) < 4:
                continue

            # KCP processing
            seg = parse_kcp_segment(data)
            if seg:
                if seg["cmd"] == 81:
                    last_kcp_data_time = time.time()
                result = kcp.process_input(data)
                kcp.flush_acks()
                if result:
                    rtype, rdata = result
                    if rtype == "data" and rdata:
                        _process_kcp_message(rdata)
                        while True:
                            q = kcp.poll_data()
                            if not q:
                                break
                            _process_kcp_message(q)
                # Stall recovery
                now = time.time()
                if last_video_time and now - last_video_time > 0.8:
                    if now - last_skip_time > 0.8 and kcp.skip_gap():
                        last_skip_time = now
                        kcp.flush_acks()
                        while True:
                            q = kcp.poll_data()
                            if not q:
                                break
                            _process_kcp_message(q)
                if last_kcp_data_time and now - last_kcp_data_time > 20:
                    break
            elif data[0] == 0xFF and data[1] == 0x01:
                kcp.process_input(data)

            # Heartbeats
            now = time.time()
            if host_key and now - last_heartbeat >= 10:
                hb = build_vvp_packet(
                    cmd=VVP_CMD_HEARTBEAT, seq=vvp_seq,
                    host_key=host_key, param=8, licence_id=licence_id)
                kcp.send_iva_data(hb)
                vvp_seq += 1
                last_heartbeat = now
            if now - last_iva_heartbeat >= 3:
                kcp.send_handshake()
                last_iva_heartbeat = now

        return (video_frame_count, total_bytes)
