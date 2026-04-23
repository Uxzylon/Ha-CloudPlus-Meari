"""P2PStreamer — manages P2P streaming sessions for CloudEdge / Meari cameras."""

from __future__ import annotations

import logging
import os
import socket
import struct
import time
from collections import deque
from typing import Any, Callable

from ..meari_signaling import MsgSvrClient
from ..turn_client import (
    TurnClient,
    _parse_stun,
    _build_stun,
    _decode_xor_address,
    BINDING_REQUEST,
    BINDING_RESPONSE,
    DATA_INDICATION,
    ATTR_DATA,
    ATTR_XOR_PEER_ADDRESS,
)
from ..kcp_tunnel import KcpTunnel, parse_kcp_segment, parse_iva_frame
from ..api import MeariApiClient, format_sn

from .protocol import (
    VVP_CMD_START_LIVE,
    VVP_CMD_HEARTBEAT,
    STREAM_TYPE_AUDIO,
    STREAM_TYPE_IFRAME,
    STREAM_TYPE_PFRAME,
    format_licence_id,
    build_vvp_packet,
    _best_quality_from_device,
)
from .codec import (
    decrypt_stream_frame,
    parse_stream_frame,
    split_stream_frames,
    _is_idr_video_frame,
)
from .network import (
    _is_private_ip,
    _get_local_ips,
    _resolve_signaling_server,
    _build_ice_response,
    _send_direct_ice_binding,
)

_LOGGER = logging.getLogger(__name__)


class P2PStreamer:
    """Runs P2P streaming sessions for a CloudEdge / Meari camera.

    Callbacks:
      on_video(data: bytes)  — raw HEVC video payload (I/P frame)
      on_audio(data: bytes)  — raw G.711 µ-law audio payload
      on_login()             — called when VVP login succeeds
    on_disconnect()        — called when session ends
    on_gap_skip(diag)      — called when KCP gap-skip invalidates refs
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
        on_gap_skip: Callable[[dict[str, Any]], None] | None = None,
        remote: bool = False,
        allow_lossy_gap_skip: bool = False,
        adaptive_lossy_gap_skip: bool = False,
        vvp_quality: int | None = None,
        video_password: str | None = None,
    ) -> None:
        self._api = api
        self._device = device
        self._sn_num = device["snNum"]
        self._device_uuid = format_sn(self._sn_num)
        self._host_key = device.get("hostKey", "")
        self._video_password = (video_password or "").strip()
        self._dev_name = device.get("deviceName", "Camera")

        self.on_video = on_video
        self.on_audio = on_audio
        self.on_login = on_login
        self.on_disconnect = on_disconnect
        self.on_gap_skip = on_gap_skip
        self._remote = remote
        self._allow_lossy_gap_skip = bool(allow_lossy_gap_skip)
        self._adaptive_lossy_gap_skip = bool(adaptive_lossy_gap_skip)
        self._vvp_quality = (
            vvp_quality
            if vvp_quality is not None
            else _best_quality_from_device(device)
        )
        if self._vvp_quality:
            _LOGGER.info(
                "VVP quality profile %d selected for %s",
                self._vvp_quality,
                self._sn_num,
            )

        self._running = False
        self._video_count = 0
        self._total_bytes = 0
        self._audio_decrypt: bool | None = None  # None = auto-detect
        self._active_sock: socket.socket | None = None
        self._active_sig: MsgSvrClient | None = None
        self._gap_skip_event_seq = 0

    def _auth_host_key(self) -> str:
        """Return host-key material used for VVP auth.

        CloudEdge video-encryption mode authenticates with
        ``video_password + hostKey`` instead of plain ``hostKey``.
        """
        if self._video_password and self._host_key:
            return f"{self._video_password}{self._host_key}"
        return self._host_key

    @staticmethod
    def _classify_gap_skip_severity(
        *,
        severe_link: bool,
        gap_size: int,
        backlog_secs: float,
        stall_s: float,
        recovery_aggr: float,
    ) -> str:
        if (
            severe_link
            or gap_size >= 6
            or backlog_secs >= 3.5
            or stall_s >= 7.0
            or recovery_aggr >= 0.88
        ):
            return "severe"
        if gap_size >= 2 or backlog_secs >= 1.2 or stall_s >= 2.5:
            return "moderate"
        return "light"

    def _notify_gap_skip(self, diag: dict[str, Any] | None = None) -> dict[str, Any]:
        info = dict(diag or {})
        self._gap_skip_event_seq += 1
        info.setdefault("event_id", self._gap_skip_event_seq)
        info.setdefault("started_mono", time.monotonic())
        info.setdefault("wall_time", time.time())
        if self.on_gap_skip:
            try:
                self.on_gap_skip(dict(info))
            except Exception:
                pass
        return info

    def request_stop(self) -> None:
        """Request the streaming loop to stop (thread-safe)."""
        _LOGGER.debug("P2P request_stop (id=%s)", hex(id(self)))
        self._running = False
        sig = self._active_sig
        if sig is not None:
            try:
                sig.close()
            except Exception:
                pass

        sock = self._active_sock
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                sock.close()
            except OSError:
                pass

    @property
    def video_count(self) -> int:
        return self._video_count

    def _parse_stream_chunk(self, chunk: bytes):
        """Parse one stream chunk.

        Video frames are always 3DES-decrypted.  Audio encryption is
        firmware-dependent — some cameras encrypt audio, others send it
        plaintext.  The first audio frame is probed: if the raw
        ``data_len`` field at 0x30 looks valid the audio is plaintext;
        otherwise we decrypt and check again.
        """
        if len(chunk) < 4:
            return None
        frame_type = chunk[3]

        if frame_type in (STREAM_TYPE_IFRAME, STREAM_TYPE_PFRAME):
            decrypted = decrypt_stream_frame(bytearray(chunk))
            return parse_stream_frame(bytes(decrypted))

        if frame_type == STREAM_TYPE_AUDIO:
            if self._audio_decrypt is True:
                decrypted = decrypt_stream_frame(bytearray(chunk))
                return parse_stream_frame(bytes(decrypted))
            if self._audio_decrypt is False:
                return parse_stream_frame(chunk)

            # --- auto-detect on first audio frame ---
            if len(chunk) >= 0x34:
                remaining = len(chunk) - 0x34
                raw_dl = struct.unpack_from("<I", chunk, 0x30)[0]
                if 0 < raw_dl <= remaining and raw_dl < 2000:
                    self._audio_decrypt = False
                    _LOGGER.info(
                        "Audio auto-detect: plaintext (data_len=%d, avail=%d)",
                        raw_dl,
                        remaining,
                    )
                    return parse_stream_frame(chunk)

                decrypted = decrypt_stream_frame(bytearray(chunk))
                dec_dl = struct.unpack_from("<I", bytes(decrypted), 0x30)[0]
                if 0 < dec_dl <= remaining and dec_dl < 2000:
                    self._audio_decrypt = True
                    _LOGGER.info(
                        "Audio auto-detect: encrypted (data_len=%d, avail=%d)",
                        dec_dl,
                        remaining,
                    )
                    return parse_stream_frame(bytes(decrypted))

            # fallback — decrypt (backward compat)
            _LOGGER.warning("Audio auto-detect: ambiguous, defaulting to encrypted")
            self._audio_decrypt = True
            decrypted = decrypt_stream_frame(bytearray(chunk))
            return parse_stream_frame(bytes(decrypted))

        return parse_stream_frame(chunk)

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
        self._audio_decrypt = None  # re-detect each session

        sig = None
        try:
            sig_ip, sig_port = _resolve_signaling_server()
            _LOGGER.debug("Connecting to signaling %s:%d", sig_ip, sig_port)
            sig = MsgSvrClient(sig_ip, sig_port)
            self._active_sig = sig
            sig.connect()

            v, b = self._do_stream(sig)
            self._video_count = v
            self._total_bytes = b
            return (v, b)
        except Exception as e:
            if (
                not self._running
                and isinstance(e, OSError)
                and getattr(e, "errno", None) == 9
            ):
                _LOGGER.debug("P2P session interrupted during stop: %s", e)
            else:
                _LOGGER.error("P2P session error: %s", e)
            return (self._video_count, self._total_bytes)
        finally:
            self._active_sig = None
            self._active_sock = None
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
        host_key = self._auth_host_key()
        sn_num = self._sn_num
        remote = self._remote

        if self._video_password:
            _LOGGER.debug(
                "Video encryption password supplied for %s (auth key extended)",
                sn_num,
            )

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
        self._active_sock = turn.sock
        if not turn.allocate():
            _LOGGER.error("TURN allocation failed")
            turn.close()
            self._active_sock = None
            return (0, 0)

        try:
            return self._stream_with_turn(
                sig,
                turn,
                device_uuid,
                host_key,
                sn_num,
                dev_nat,
                coturn_ip,
                remote,
            )
        finally:
            self._active_sock = None
            turn.close()

    def _stream_with_turn(
        self,
        sig,
        turn,
        device_uuid,
        host_key,
        sn_num,
        dev_nat,
        coturn_ip,
        remote,
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
            "s=ice",
            "t=0 0",
            f"a=ice-ufrag:{ice_ufrag}",
            f"a=ice-pwd:{ice_pwd}",
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
            camera_candidates.append(
                {
                    "ip": camera_sdp_ip,
                    "port": camera_sdp_port,
                    "type": "relay",
                }
            )

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
                                        camera_candidates.append(
                                            {
                                                "ip": pts[4],
                                                "port": int(pts[5]),
                                                "type": pts[7],
                                            }
                                        )
                        if extra_cand and isinstance(extra_cand, dict):
                            cip = extra_cand.get("ip")
                            cport = extra_cand.get("port")
                            if cip and cport:
                                camera_candidates.append(
                                    {
                                        "ip": cip,
                                        "port": int(cport),
                                        "type": extra_cand.get("type", "relay"),
                                    }
                                )
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
            if (
                not remote
                and send_addr_holder[0]
                and _is_private_ip(send_addr_holder[0][0])
            ):
                try:
                    turn.sock.sendto(data, send_addr_holder[0])
                except Exception:
                    pass

        kcp = KcpTunnel(_udp_send)

        def _send_ice_checks():
            for c in camera_candidates:
                turn.send_ice_binding(
                    c["ip"], c["port"], ice_ufrag, camera_ufrag, camera_pwd
                )
                if not remote and _is_private_ip(c["ip"]):
                    _send_direct_ice_binding(
                        turn.sock,
                        c["ip"],
                        c["port"],
                        ice_ufrag,
                        camera_ufrag,
                        camera_pwd,
                    )

        _send_ice_checks()

        # VVP login
        licence_id = format_licence_id(sn_num) if sn_num else None
        vvp_seq = 0
        vvp_login = build_vvp_packet(
            cmd=VVP_CMD_START_LIVE,
            seq=vvp_seq,
            host_key=host_key,
            param=8,
            licence_id=licence_id,
            quality=self._vvp_quality,
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
        start_live_retry_at = time.time() + 2.5
        turn_refresh_at = time.time() + 60
        stun_keepalive_at = time.time() + 5

        ice_count = 0
        confirmed_addr = None
        request_addrs: set = set()
        got_iva_handshake = False
        login_ok = False
        login_ok_at: float | None = None
        no_video_timeout = False
        direct_addr = None
        turn.sock.settimeout(0.08)

        stream_frame_count = 0
        stream_video_count = 0
        stream_total_bytes = 0
        stream_start_time = time.time()
        no_video_restart_sec = 15.0
        last_video_time = None
        last_kcp_data_time = None
        last_kcp_payload_time = time.time()
        last_video_payload_time = time.time()
        last_nudge_time = 0.0
        last_skip_time = 0.0
        last_gap_sn = -1
        last_gap_since = 0.0
        gap_nudges = 0
        push_rate_ema = 140.0
        last_push_time = 0.0
        audio_catchup_until = 0.0
        audio_catchup_stride = 4
        audio_catchup_seen = 0
        skip_expect_recovery_by = 0.0
        skip_failures = 0
        skip_backoff_until = 0.0
        last_skip_video_time = 0.0
        long_stall_marks: deque[float] = deque()
        recent_skip_marks: deque[float] = deque()
        recent_severe_skip_marks: deque[float] = deque()
        recent_skip_budget_marks: deque[float] = deque()
        severe_skip_quiet_until = 0.0
        lossy_session_mode = False
        lossy_reentry_block_until = 0.0
        recovery_aggr = 0.0
        link_class = "clean"
        kcp_push_count = 0
        flow = {
            "udp_packets": 0,
            "turn_channel_packets": 0,
            "data_indications": 0,
            "kcp_segments": 0,
            "kcp_push": 0,
            "kcp_data_msgs": 0,
            "stream_payload_calls": 0,
            "stream_chunks": 0,
            "stream_parse_ok": 0,
            "stream_parse_fail": 0,
            "video_chunks": 0,
            "audio_chunks": 0,
            "tx_heartbeat": 0,
            "tx_start_live": 0,
            "tx_ice_check": 0,
            "tx_iva_handshake": 0,
        }

        # Initialize IDR diagnostics variables early (before they might be referenced in _log_flow_summary)
        _skip_pframes_until_iframe = True
        _idr_wait_drops = 0
        _idr_wait_started_at = time.time()
        _idr_wait_reason = "session-start"
        _idr_wait_events = 1
        _forced_resume_count = 0
        _trusted_idr_seen = False
        _first_trusted_idr_at = 0.0
        _video_forwarded_before_trusted_idr = 0
        _video_forwarded_after_trusted_idr = 0
        # Frame type statistics
        _iframes_received = 0
        _pframes_received = 0
        _iframes_rejected = 0
        _active_gap_skip_diag: dict[str, Any] | None = None

        def _log_flow_summary(reason: str) -> None:
            recv_buf_size = len(getattr(kcp, "recv_buf", {}))
            recv_frag_parts = len(getattr(kcp, "recv_frag_buf", []))
            next_recv_sn = int(getattr(kcp, "next_recv_sn", -1))
            _LOGGER.debug(
                "Flow summary (%s): udp=%d turn_ch=%d data_ind=%d kcp_seg=%d kcp_push=%d "
                "kcp_data=%d payloads=%d chunks=%d parsed_ok=%d parsed_fail=%d "
                "video_chunks=%d audio_chunks=%d video_frames=%d bytes=%d "
                "kcp_recv_buf=%d kcp_frag_parts=%d next_sn=%d",
                reason,
                flow["udp_packets"],
                flow["turn_channel_packets"],
                flow["data_indications"],
                flow["kcp_segments"],
                flow["kcp_push"],
                flow["kcp_data_msgs"],
                flow["stream_payload_calls"],
                flow["stream_chunks"],
                flow["stream_parse_ok"],
                flow["stream_parse_fail"],
                flow["video_chunks"],
                flow["audio_chunks"],
                stream_video_count,
                stream_total_bytes,
                recv_buf_size,
                recv_frag_parts,
                next_recv_sn,
            )
            _LOGGER.debug(
                "Flow tx (%s): heartbeat=%d start_live=%d ice_checks=%d iva_handshake=%d",
                reason,
                flow["tx_heartbeat"],
                flow["tx_start_live"],
                flow["tx_ice_check"],
                flow["tx_iva_handshake"],
            )
            _LOGGER.debug(
                "IDR gate (%s): trusted=%s first_idr_after=%.2fs wait_events=%d forced_resume=%d "
                "fwd_before_idr=%d fwd_after_idr=%d",
                reason,
                _trusted_idr_seen,
                (
                    (_first_trusted_idr_at - stream_start_time)
                    if _first_trusted_idr_at > 0
                    else -1.0
                ),
                _idr_wait_events,
                _forced_resume_count,
                _video_forwarded_before_trusted_idr,
                _video_forwarded_after_trusted_idr,
            )
            _LOGGER.info(
                "Frame types (%s): iframes_received=%d iframes_rejected=%d pframes_received=%d",
                reason,
                _iframes_received,
                _iframes_rejected,
                _pframes_received,
            )

        # After a KCP gap skip with significant packet loss, the HEVC
        # reference chain is broken (P-frames reference lost data → POC
        # errors → visual corruption).  Drop P-frames until the next
        # I-frame resets the decoder's reference chain.

        def _handle_stream_payload(payload):
            nonlocal stream_frame_count, stream_video_count, stream_total_bytes
            nonlocal last_video_time, last_video_payload_time
            nonlocal audio_catchup_until, audio_catchup_seen
            nonlocal _skip_pframes_until_iframe, _idr_wait_drops, _idr_wait_started_at
            nonlocal _idr_wait_reason, _idr_wait_events, _forced_resume_count
            nonlocal _trusted_idr_seen, _first_trusted_idr_at
            nonlocal _video_forwarded_before_trusted_idr, _video_forwarded_after_trusted_idr
            nonlocal _iframes_received, _pframes_received, _iframes_rejected
            nonlocal _active_gap_skip_diag
            if not payload or len(payload) < 4:
                return True
            flow["stream_payload_calls"] += 1
            for chunk in split_stream_frames(payload):
                if len(chunk) < 4:
                    continue
                if chunk[0] != 0 or chunk[1] != 0 or chunk[2] != 1:
                    continue
                flow["stream_chunks"] += 1
                frame_type = chunk[3]
                stream_frame_count += 1
                parsed = self._parse_stream_chunk(chunk)
                if not parsed:
                    flow["stream_parse_fail"] += 1
                    continue
                flow["stream_parse_ok"] += 1
                ftype, _, media_data = parsed
                if ftype in (STREAM_TYPE_IFRAME, STREAM_TYPE_PFRAME):
                    flow["video_chunks"] += 1
                    stream_video_count += 1
                    stream_total_bytes += len(media_data)
                    self._video_count += 1
                    self._total_bytes += len(media_data)
                    now_frame = time.time()
                    last_video_payload_time = now_frame
                    # Track frame types for diagnostics
                    if ftype == STREAM_TYPE_IFRAME:
                        _iframes_received += 1
                    elif ftype == STREAM_TYPE_PFRAME:
                        _pframes_received += 1
                    # After KCP gap skip: drop P-frames until next I-frame
                    if _skip_pframes_until_iframe:
                        force_resume = (
                            ftype == STREAM_TYPE_IFRAME
                            and _idr_wait_started_at > 0
                            and (time.time() - _idr_wait_started_at) > 12.0
                        )
                        # For decoder-reset recovery, accept bare IDR VCL (no
                        # param sets) because the coordinator will prepend its
                        # cached VPS/SPS/PPS before forwarding to ffmpeg.
                        decoder_reset_recovery = _idr_wait_reason in (
                            "kcp-gap",
                            "video-stall",
                        )
                        is_valid_idr = _is_idr_video_frame(
                            ftype,
                            media_data,
                            require_param_sets=not decoder_reset_recovery,
                        )
                        if is_valid_idr or force_resume:
                            if force_resume:
                                _forced_resume_count += 1
                                _LOGGER.warning(
                                    "IDR wait timeout after %s (%.2fs) - forcing resume on I-frame",
                                    _idr_wait_reason,
                                    time.time() - _idr_wait_started_at,
                                )
                            elif not _trusted_idr_seen:
                                _trusted_idr_seen = True
                                _first_trusted_idr_at = time.time()
                                _LOGGER.info(
                                    "Trusted IDR acquired (reason=%s, wait=%.2fs, dropped=%d, size=%d)",
                                    _idr_wait_reason,
                                    time.time() - _idr_wait_started_at,
                                    _idr_wait_drops,
                                    len(media_data),
                                )
                            elif _idr_wait_reason in ("kcp-gap", "video-stall"):
                                gap_event_id = int(
                                    (_active_gap_skip_diag or {}).get("event_id", 0)
                                )
                                gap_severity = str(
                                    (_active_gap_skip_diag or {}).get(
                                        "severity", "unknown"
                                    )
                                )
                                reset_mode = str(
                                    (_active_gap_skip_diag or {}).get(
                                        "mode", _idr_wait_reason
                                    )
                                )
                                _LOGGER.info(
                                    "IDR frame recovered after %s #%d (%s, wait=%.2fs, dropped=%d frames, size=%d)",
                                    reset_mode,
                                    gap_event_id,
                                    gap_severity,
                                    time.time() - _idr_wait_started_at,
                                    _idr_wait_drops,
                                    len(media_data),
                                )
                            _skip_pframes_until_iframe = False
                            _LOGGER.debug(
                                "IDR after decoder reset — resumed (%d frames dropped)",
                                _idr_wait_drops,
                            )
                            _idr_wait_drops = 0
                            _idr_wait_started_at = 0.0
                            _active_gap_skip_diag = None
                        else:
                            # I-frame that failed IDR validation (likely incomplete NAL structure)
                            if ftype == STREAM_TYPE_IFRAME:
                                _iframes_rejected += 1
                                gap_event_id = int(
                                    (_active_gap_skip_diag or {}).get("event_id", 0)
                                )
                                gap_severity = str(
                                    (_active_gap_skip_diag or {}).get(
                                        "severity", "unknown"
                                    )
                                )
                                if _idr_wait_reason in ("kcp-gap", "video-stall"):
                                    reset_mode = str(
                                        (_active_gap_skip_diag or {}).get(
                                            "mode", _idr_wait_reason
                                        )
                                    )
                                    _LOGGER.warning(
                                        "Rejected I-frame failing IDR validation after %s #%d (%s, size=%d bytes, potential corruption)",
                                        reset_mode,
                                        gap_event_id,
                                        gap_severity,
                                        len(media_data),
                                    )
                                else:
                                    _LOGGER.warning(
                                        "Rejected I-frame failing IDR validation (size=%d bytes, potential corruption)",
                                        len(media_data),
                                    )
                            _idr_wait_drops += 1
                            continue
                    if _trusted_idr_seen:
                        _video_forwarded_after_trusted_idr += 1
                    else:
                        _video_forwarded_before_trusted_idr += 1
                    last_video_time = now_frame
                    if self.on_video:
                        self.on_video(media_data)
                    continue
                if ftype == STREAM_TYPE_AUDIO:
                    flow["audio_chunks"] += 1
                    if self.on_audio:
                        self.on_audio(media_data)
            return True

        def _drain_kcp_queue() -> int:
            drained = 0
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
                    drained += 1
            return drained

        def _kcp_gap_state() -> tuple[bool, int, int]:
            next_sn = int(getattr(kcp, "next_recv_sn", -1))
            recv_buf = getattr(kcp, "recv_buf", {})
            if next_sn < 0 or not recv_buf:
                return (False, 0, 0)
            if next_sn in recv_buf:
                return (False, 0, 0)
            higher = [sn for sn in recv_buf if sn > next_sn]
            if not higher:
                return (False, 0, 0)
            min_above = min(higher)
            max_above = max(higher)
            gap_size = max(1, min_above - next_sn)
            backlog = (max_above - next_sn) + 1
            return (True, gap_size, backlog)

        def _attempt_kcp_gap_recovery(now_ts: float) -> None:
            nonlocal last_nudge_time, last_skip_time, last_kcp_payload_time
            nonlocal last_video_payload_time
            nonlocal last_gap_sn, last_gap_since, gap_nudges
            nonlocal audio_catchup_until, audio_catchup_seen
            nonlocal push_rate_ema
            nonlocal skip_expect_recovery_by, skip_failures, skip_backoff_until
            nonlocal last_skip_video_time
            nonlocal long_stall_marks
            nonlocal recent_skip_marks
            nonlocal recent_severe_skip_marks
            nonlocal recent_skip_budget_marks
            nonlocal severe_skip_quiet_until
            nonlocal lossy_session_mode
            nonlocal lossy_reentry_block_until
            nonlocal recovery_aggr
            nonlocal link_class
            nonlocal _skip_pframes_until_iframe, _idr_wait_started_at
            nonlocal _idr_wait_reason, _idr_wait_events
            nonlocal _active_gap_skip_diag
            nonlocal vvp_seq, start_live_retry_at
            if not login_ok or last_kcp_data_time is None:
                last_gap_sn = -1
                last_gap_since = 0.0
                gap_nudges = 0
                skip_expect_recovery_by = 0.0
                skip_backoff_until = 0.0
                skip_failures = 0
                recovery_aggr = 0.0
                return
            if last_video_time is None:
                last_gap_sn = -1
                last_gap_since = 0.0
                gap_nudges = 0
                skip_expect_recovery_by = 0.0
                skip_backoff_until = 0.0
                skip_failures = 0
                recovery_aggr = 0.0
                return

            if skip_expect_recovery_by > 0.0 and now_ts >= skip_expect_recovery_by:
                if last_video_time <= last_skip_video_time:
                    skip_failures = min(skip_failures + 1, 6)
                    skip_backoff_until = now_ts + min(4.0, 0.5 + (skip_failures * 0.6))
                else:
                    skip_failures = max(0, skip_failures - 2)
                skip_expect_recovery_by = 0.0

            stall_time = now_ts - last_video_time
            if stall_time <= 0.15:
                recovery_aggr = max(0.0, recovery_aggr * 0.92)
                # De-escalate lossy session mode once the stream proves healthy again.
                if lossy_session_mode and recovery_aggr < 0.18 and skip_failures == 0:
                    while long_stall_marks and (now_ts - long_stall_marks[0]) > 20.0:
                        long_stall_marks.popleft()
                    if not long_stall_marks:
                        lossy_session_mode = False
                        lossy_reentry_block_until = max(
                            lossy_reentry_block_until, now_ts + 7.0
                        )
                        _LOGGER.info(
                            "Auto-de-escalating lossy session mode (stream healthy, recovery_aggr=%.3f)",
                            recovery_aggr,
                        )
                return

            if stall_time > 3.0:
                if not long_stall_marks or (now_ts - long_stall_marks[-1]) > 2.0:
                    long_stall_marks.append(now_ts)
            while long_stall_marks and (now_ts - long_stall_marks[0]) > 20.0:
                long_stall_marks.popleft()
            long_stalls_recent = len(long_stall_marks)
            while recent_skip_marks and (now_ts - recent_skip_marks[0]) > 12.0:
                recent_skip_marks.popleft()
            recent_skip_count = len(recent_skip_marks)
            while (
                recent_skip_budget_marks
                and (now_ts - recent_skip_budget_marks[0]) > 10.0
            ):
                recent_skip_budget_marks.popleft()
            skip_budget_10s = len(recent_skip_budget_marks)
            while (
                recent_severe_skip_marks
                and (now_ts - recent_severe_skip_marks[0]) > 20.0
            ):
                recent_severe_skip_marks.popleft()
            severe_skip_recent_count = len(recent_severe_skip_marks)

            gap_pending, gap_size, backlog = _kcp_gap_state()
            next_sn = int(getattr(kcp, "next_recv_sn", -1))
            if gap_pending:
                if next_sn != last_gap_sn:
                    last_gap_sn = next_sn
                    last_gap_since = now_ts
                    gap_nudges = 0
            else:
                last_gap_sn = -1
                last_gap_since = 0.0
                gap_nudges = 0

            backlog_secs = backlog / max(1.0, push_rate_ema)

            elapsed_s = max(1.0, now_ts - stream_start_time)
            live_fps = stream_video_count / elapsed_s if stream_video_count > 0 else 0.0
            fps_pressure = max(0.0, min(1.0, (10.5 - live_fps) / 10.5))
            stall_pressure = max(0.0, min(1.0, (stall_time - 0.8) / 3.0))
            long_stall_pressure = max(0.0, min(1.0, long_stalls_recent / 3.0))
            backlog_pressure = max(0.0, min(1.0, backlog_secs / 2.0))
            failure_pressure = max(0.0, min(1.0, skip_failures / 3.0))
            gap_pressure = max(0.0, min(1.0, gap_size / 6.0))
            pressure = (
                (0.30 * stall_pressure)
                + (0.25 * long_stall_pressure)
                + (0.20 * fps_pressure)
                + (0.15 * backlog_pressure)
                + (0.10 * failure_pressure)
            )
            if self._adaptive_lossy_gap_skip:
                pressure = min(1.0, pressure + 0.15)
            recovery_aggr = (recovery_aggr * 0.75) + (pressure * 0.25)

            link_instability = (
                (0.40 * backlog_pressure)
                + (0.25 * long_stall_pressure)
                + (0.20 * gap_pressure)
                + (0.15 * min(1.0, recent_skip_count / 4.0))
            )
            if recovery_aggr >= 0.78 or link_instability >= 0.80:
                link_class = "critical"
            elif recovery_aggr >= 0.56 or link_instability >= 0.62:
                link_class = "lossy"
            elif recovery_aggr >= 0.34 or link_instability >= 0.42:
                link_class = "stressed"
            else:
                link_class = "clean"

            skip_budget_cap_10s = 3
            if link_class == "clean":
                skip_budget_cap_10s = 2
            elif link_class == "stressed":
                skip_budget_cap_10s = 3
            elif link_class == "lossy":
                skip_budget_cap_10s = 4
            elif link_class == "critical":
                skip_budget_cap_10s = 5

            if (
                skip_budget_10s >= skip_budget_cap_10s
                and stall_time > 2.0
                and (now_ts - stream_start_time) > 8.0
            ):
                _LOGGER.warning(
                    "Skip budget exceeded (%d/%d in 10s, class=%s, stall=%.2fs) - restarting session",
                    skip_budget_10s,
                    skip_budget_cap_10s,
                    link_class,
                    stall_time,
                )
                self.request_stop()
                return

            nudge_interval = max(
                0.03,
                min(
                    0.25,
                    (0.06 + (backlog_secs * 0.04)) * (1.0 - (0.40 * recovery_aggr)),
                ),
            )

            if gap_pending and now_ts - last_nudge_time > nudge_interval:
                if kcp.send_gap_nudge():
                    last_nudge_time = now_ts
                    gap_nudges += 1

            payload_idle = now_ts - last_kcp_payload_time
            video_payload_idle = now_ts - last_video_payload_time
            recovery_payload_idle = max(payload_idle, video_payload_idle)
            gap_age = (now_ts - last_gap_since) if last_gap_since > 0.0 else 0.0
            adaptive_wait_base = max(
                2.5, min(6.0, 4.5 - (0.65 * min(backlog_secs, 2.5)))
            )
            adaptive_wait = max(
                1.4, adaptive_wait_base * (1.0 - (0.45 * recovery_aggr))
            )
            min_nudges_base = (
                3 if backlog_secs >= 2.0 else 3 if backlog_secs >= 1.0 else 4
            )
            min_nudges = max(1, int(round(min_nudges_base - (2.0 * recovery_aggr))))
            small_gap = bool(
                gap_pending and gap_size <= 4 and backlog <= 128 and backlog_secs < 0.9
            )
            tiny_gap = bool(
                gap_pending and gap_size <= 2 and backlog <= 64 and backlog_secs < 0.45
            )
            if small_gap:
                adaptive_wait = max(adaptive_wait, 2.2)
                min_nudges = max(min_nudges, 5)
            if tiny_gap:
                adaptive_wait = max(adaptive_wait, 2.8)
                min_nudges = max(min_nudges, 6)

            if (
                not self._allow_lossy_gap_skip
                and not lossy_session_mode
                and now_ts >= lossy_reentry_block_until
                and gap_pending
                and (
                    (stall_time > 2.5 and backlog_secs > 0.8)
                    or (
                        self._adaptive_lossy_gap_skip
                        and (now_ts - stream_start_time) > 8.0
                        and stall_time > 2.0
                        and (recovery_aggr > 0.40 or live_fps < 7.0)
                    )
                    or (long_stalls_recent >= 1 and stall_time > 1.6)
                )
            ):
                lossy_session_mode = True
                _LOGGER.warning(
                    "Auto-escalating to lossy recovery for this session (stall=%.2fs, long_stalls=%d)",
                    stall_time,
                    long_stalls_recent,
                )

            effective_lossy = (
                self._allow_lossy_gap_skip
                or lossy_session_mode
                or (
                    self._adaptive_lossy_gap_skip
                    and gap_pending
                    and recovery_aggr >= 0.55
                )
                or skip_failures >= 1
                or (stall_time > 8.0 and gap_pending and backlog_secs > 1.6)
                or (gap_pending and long_stalls_recent >= 2 and stall_time > 2.6)
            )
            if effective_lossy:
                lossy_scale = 0.50 if self._allow_lossy_gap_skip else 0.58
                lossy_scale = max(0.35, lossy_scale - (0.18 * recovery_aggr))
                min_lossy_wait = 0.7
                if not self._allow_lossy_gap_skip:
                    min_lossy_wait = (
                        2.5
                        if (self._adaptive_lossy_gap_skip or lossy_session_mode)
                        else 0.9
                    )
                lossy_wait = max(min_lossy_wait, min(3.0, adaptive_wait * lossy_scale))
                nudge_gate = max(
                    1,
                    min_nudges - (2 if self._adaptive_lossy_gap_skip else 1),
                )
                idle_gate = lossy_wait * (
                    0.20 if self._adaptive_lossy_gap_skip else 0.30
                )
                age_gate = lossy_wait * (
                    0.25 if self._adaptive_lossy_gap_skip else 0.35
                )
                skip_cooldown_floor = (
                    2.1
                    if (self._adaptive_lossy_gap_skip or lossy_session_mode)
                    else 0.8
                )
                dynamic_skip_cooldown = skip_cooldown_floor + (
                    0.7 * min(4, recent_skip_count)
                )
                max_adaptive_skip_depth = 2
                if link_class == "clean":
                    lossy_wait *= 1.25
                    dynamic_skip_cooldown += 1.1
                    max_adaptive_skip_depth = 1
                elif link_class == "stressed":
                    lossy_wait *= 1.10
                    dynamic_skip_cooldown += 0.7
                    max_adaptive_skip_depth = 2
                elif link_class == "lossy":
                    max_adaptive_skip_depth = 2
                else:
                    lossy_wait *= 0.92
                    max_adaptive_skip_depth = 3
                if (
                    gap_pending
                    and now_ts >= skip_backoff_until
                    and stall_time > lossy_wait
                    and (not small_gap or stall_time > max(lossy_wait + 0.6, 2.2))
                    and (not tiny_gap or stall_time > max(lossy_wait + 1.2, 2.8))
                    and recovery_payload_idle > idle_gate
                    and gap_age > age_gate
                    and gap_nudges >= nudge_gate
                    and now_ts - last_skip_time
                    > max(
                        dynamic_skip_cooldown,
                        (lossy_wait * 0.55)
                        + (0.25 * skip_failures)
                        - (0.40 * recovery_aggr),
                    )
                ):
                    severe_skip_emergency = bool(
                        stall_time > 10.0
                        or backlog_secs > 5.0
                        or (gap_size >= 8 and stall_time > 6.0)
                    )
                    severe_link = bool(
                        stall_time > 5.0
                        or backlog_secs > 3.0
                        or (long_stalls_recent >= 3 and stall_time > 3.2)
                        or (recovery_aggr > 0.80 and stall_time > 3.0)
                    )
                    if (
                        severe_link
                        and not severe_skip_emergency
                        and (
                            now_ts < severe_skip_quiet_until
                            or severe_skip_recent_count >= 2
                        )
                    ):
                        severe_link = False
                    if severe_link and self._allow_lossy_gap_skip:
                        # Only forced-lossy mode can skip an unbounded gap range.
                        skip_depth = None
                    elif severe_link:
                        # For single-gap severe events, use a shallower skip to
                        # avoid over-jumping and repeated recovery churn.
                        if gap_size <= 1 and backlog_secs < 2.2 and stall_time < 6.0:
                            skip_depth = 2
                        else:
                            # Adaptive mode should remain bounded to avoid jumping too
                            # far into a potentially unstable reference chain.
                            skip_depth = 2 if not severe_skip_emergency else 3
                    elif tiny_gap:
                        skip_depth = 1
                    elif small_gap:
                        skip_depth = 2
                    elif self._allow_lossy_gap_skip:
                        skip_depth = None
                    elif self._adaptive_lossy_gap_skip or lossy_session_mode:
                        skip_depth = 2
                    else:
                        skip_depth = 2
                    if skip_depth is not None:
                        skip_depth = max(1, min(skip_depth, max_adaptive_skip_depth))
                    if kcp.skip_gap(max_gaps=skip_depth):
                        gap_severity = self._classify_gap_skip_severity(
                            severe_link=severe_link,
                            gap_size=gap_size,
                            backlog_secs=backlog_secs,
                            stall_s=stall_time,
                            recovery_aggr=recovery_aggr,
                        )
                        gap_mode = (
                            "forced-lossy"
                            if self._allow_lossy_gap_skip
                            else (
                                "adaptive-lossy"
                                if (self._adaptive_lossy_gap_skip or lossy_session_mode)
                                else "adaptive"
                            )
                        )
                        if not _skip_pframes_until_iframe:
                            _idr_wait_events += 1
                            _idr_wait_started_at = now_ts  # only set on the first skip
                        _idr_wait_reason = "kcp-gap"
                        _skip_pframes_until_iframe = True
                        _active_gap_skip_diag = self._notify_gap_skip(
                            {
                                "severity": gap_severity,
                                "started_mono": time.monotonic(),
                                "wall_time": now_ts,
                                "stall_s": round(stall_time, 3),
                                "gap_size": int(gap_size),
                                "backlog_frames": int(backlog),
                                "backlog_s": round(backlog_secs, 3),
                                "skip_depth": (
                                    -1 if skip_depth is None else int(skip_depth)
                                ),
                                "nudges": int(gap_nudges),
                                "skip_failures": int(skip_failures),
                                "recovery_aggr": round(recovery_aggr, 3),
                                "link_class": link_class,
                                "mode": gap_mode,
                            }
                        )
                        _LOGGER.info(
                            "KCP gap skip #%d (%s, %s): stall=%.2fs gap=%d backlog=%d (%.2fs) skip_depth=%s",
                            int(_active_gap_skip_diag.get("event_id", 0)),
                            gap_severity,
                            gap_mode,
                            stall_time,
                            gap_size,
                            backlog,
                            backlog_secs,
                            "all" if skip_depth is None else skip_depth,
                        )
                        if gap_severity == "severe":
                            recent_severe_skip_marks.append(now_ts)
                            severe_skip_quiet_until = max(
                                severe_skip_quiet_until, now_ts + 3.2
                            )
                        recent_skip_budget_marks.append(now_ts)
                        recent_skip_marks.append(now_ts)
                        last_skip_time = now_ts
                        kcp.flush_acks()
                        drained = _drain_kcp_queue()
                        if drained:
                            last_kcp_payload_time = time.time()
                        last_skip_video_time = last_video_time
                        skip_expect_recovery_by = now_ts + 0.8
                        # Ask the camera to emit a fresh IDR immediately.
                        # The normal gate (last_video_time > 3s) may not fire
                        # during IDR-wait because P-frames keep last_video_time fresh.
                        _idr_request = build_vvp_packet(
                            cmd=VVP_CMD_START_LIVE,
                            seq=vvp_seq,
                            host_key=host_key,
                            param=8,
                            licence_id=licence_id,
                            quality=self._vvp_quality,
                        )
                        kcp.send_iva_data(_idr_request)
                        vvp_seq += 1
                        start_live_retry_at = now_ts + 2.5
                return

            if (
                gap_pending
                and now_ts >= skip_backoff_until
                and stall_time > adaptive_wait
                and recovery_payload_idle > (adaptive_wait * 0.50)
                and gap_age > adaptive_wait
                and gap_nudges >= min_nudges
                and now_ts - last_skip_time
                > max(
                    3.0 + (skip_failures * 1.0) + (0.8 * min(4, recent_skip_count)),
                    adaptive_wait,
                )
            ):
                if kcp.skip_gap(max_gaps=1):
                    gap_severity = self._classify_gap_skip_severity(
                        severe_link=False,
                        gap_size=gap_size,
                        backlog_secs=backlog_secs,
                        stall_s=stall_time,
                        recovery_aggr=recovery_aggr,
                    )
                    if not _skip_pframes_until_iframe:
                        _idr_wait_events += 1
                        _idr_wait_started_at = now_ts  # only set on the first skip
                    _idr_wait_reason = "kcp-gap"
                    _skip_pframes_until_iframe = True
                    _active_gap_skip_diag = self._notify_gap_skip(
                        {
                            "severity": gap_severity,
                            "started_mono": time.monotonic(),
                            "wall_time": now_ts,
                            "stall_s": round(stall_time, 3),
                            "gap_size": int(gap_size),
                            "backlog_frames": int(backlog),
                            "backlog_s": round(backlog_secs, 3),
                            "skip_depth": 1,
                            "nudges": int(gap_nudges),
                            "skip_failures": int(skip_failures),
                            "recovery_aggr": round(recovery_aggr, 3),
                            "mode": "adaptive-single",
                        }
                    )
                    last_skip_time = now_ts
                    _LOGGER.warning(
                        "Adaptive KCP skip #%d (%s): stall=%.2fs gap=%d backlog=%d (%.2fs) nudges=%d wait=%.2fs fails=%d next_sn=%d",
                        int(_active_gap_skip_diag.get("event_id", 0)),
                        gap_severity,
                        stall_time,
                        gap_size,
                        backlog,
                        backlog_secs,
                        gap_nudges,
                        adaptive_wait,
                        skip_failures,
                        next_sn,
                    )
                    recent_skip_marks.append(now_ts)
                    kcp.flush_acks()
                    drained = _drain_kcp_queue()
                    if drained:
                        last_kcp_payload_time = time.time()
                    last_skip_video_time = last_video_time
                    skip_expect_recovery_by = now_ts + 0.8
                    _idr_request = build_vvp_packet(
                        cmd=VVP_CMD_START_LIVE,
                        seq=vvp_seq,
                        host_key=host_key,
                        param=8,
                        licence_id=licence_id,
                        quality=self._vvp_quality,
                    )
                    kcp.send_iva_data(_idr_request)
                    vvp_seq += 1
                    start_live_retry_at = now_ts + 2.5

            stall_reset_threshold = 1.7 if live_fps >= 10.0 else 1.4
            stall_reset_idle_threshold = 0.6 if live_fps >= 10.0 else 0.45
            stall_reset_gap_ok = not gap_pending or (
                gap_size <= 4
                and backlog_secs < 1.0
                and stall_time > (stall_reset_threshold + 0.2)
            )
            if (
                not _skip_pframes_until_iframe
                and now_ts >= skip_backoff_until
                and stall_time > stall_reset_threshold
                and recovery_payload_idle > stall_reset_idle_threshold
                and now_ts - last_skip_time > 1.6
                and (now_ts - stream_start_time) > 8.0
                and stall_reset_gap_ok
            ):
                stall_reset_gap_size = int(gap_size if gap_pending else 0)
                stall_reset_backlog = int(backlog if gap_pending else 0)
                stall_reset_backlog_s = round(backlog_secs if gap_pending else 0.0, 3)
                gap_severity = self._classify_gap_skip_severity(
                    severe_link=True,
                    gap_size=stall_reset_gap_size,
                    backlog_secs=stall_reset_backlog_s,
                    stall_s=stall_time,
                    recovery_aggr=recovery_aggr,
                )
                _idr_wait_events += 1
                _idr_wait_started_at = now_ts
                _idr_wait_reason = "video-stall"
                _skip_pframes_until_iframe = True
                _active_gap_skip_diag = self._notify_gap_skip(
                    {
                        "severity": gap_severity,
                        "started_mono": time.monotonic(),
                        "wall_time": now_ts,
                        "stall_s": round(stall_time, 3),
                        "gap_size": stall_reset_gap_size,
                        "backlog_frames": stall_reset_backlog,
                        "backlog_s": stall_reset_backlog_s,
                        "skip_depth": 0,
                        "nudges": 0,
                        "skip_failures": int(skip_failures),
                        "recovery_aggr": round(recovery_aggr, 3),
                        "payload_idle_s": round(recovery_payload_idle, 3),
                        "live_fps": round(live_fps, 2),
                        "mode": "stall-reset",
                    }
                )
                _LOGGER.warning(
                    "Video stall reset #%d (%s): stall=%.2fs idle=%.2fs live_fps=%.2f gap=%d backlog=%.2fs",
                    int(_active_gap_skip_diag.get("event_id", 0)),
                    gap_severity,
                    stall_time,
                    recovery_payload_idle,
                    live_fps,
                    stall_reset_gap_size,
                    stall_reset_backlog_s,
                )
                last_skip_time = now_ts
                last_skip_video_time = last_video_time
                skip_expect_recovery_by = now_ts + 0.8
                _idr_request = build_vvp_packet(
                    cmd=VVP_CMD_START_LIVE,
                    seq=vvp_seq,
                    host_key=host_key,
                    param=8,
                    licence_id=licence_id,
                    quality=self._vvp_quality,
                )
                kcp.send_iva_data(_idr_request)
                vvp_seq += 1
                start_live_retry_at = now_ts + 2.5
                return

            # Hard stall ceiling: if video has been completely frozen for too
            # long, restart unconditionally rather than waiting for the individual
            # condition thresholds to eventually all align.
            if stall_time > 10.0 and (now_ts - stream_start_time) > 12.0:
                _LOGGER.warning(
                    "Hard stall ceiling hit (stall=%.2fs), restarting session",
                    stall_time,
                )
                self.request_stop()

            if gap_pending and skip_failures >= 3 and stall_time > 8.0:
                _LOGGER.warning(
                    "Persistent KCP gap (stall=%.2fs, backlog=%d, failures=%d), restarting session",
                    stall_time,
                    backlog,
                    skip_failures,
                )
                self.request_stop()

            if (
                gap_pending
                and severe_skip_recent_count >= 3
                and stall_time > 4.0
                and (now_ts - stream_start_time) > 10.0
                and (now_ts - last_skip_time) < 9.0
            ):
                _LOGGER.warning(
                    "Severe KCP chain detected (%d severe skips/20s, stall=%.2fs), restarting session",
                    severe_skip_recent_count,
                    stall_time,
                )
                self.request_stop()

            if gap_pending and long_stalls_recent >= 3 and stall_time > 5.0:
                _LOGGER.warning(
                    "Repeated long stalls (%d in 20s, stall=%.2fs), restarting session",
                    long_stalls_recent,
                    stall_time,
                )
                self.request_stop()

            if (
                long_stalls_recent >= 4
                and stall_time > 3.8
                and (now_ts - stream_start_time) > 20.0
            ):
                _LOGGER.warning(
                    "Persistent degradation (%d long stalls, current %.2fs), rolling session",
                    long_stalls_recent,
                    stall_time,
                )
                self.request_stop()

            if (
                gap_pending
                and stall_time > 7.0
                and recovery_payload_idle > 2.5
                and gap_age > 4.0
            ):
                _LOGGER.warning(
                    "Frozen gap persists (stall=%.2fs, idle=%.2fs, gap_age=%.2fs), restarting session",
                    stall_time,
                    recovery_payload_idle,
                    gap_age,
                )
                self.request_stop()

        _packet_buf: deque = deque()

        def _video_drought(now_ts: float) -> bool:
            if last_video_time is not None:
                return (now_ts - last_video_time) > no_video_restart_sec
            if login_ok_at is not None:
                return (now_ts - login_ok_at) > no_video_restart_sec
            return False

        while self._running and time.time() < ice_deadline:
            now = time.time()

            if login_ok and _video_drought(now):
                _LOGGER.debug(
                    "No video received %.0fs after VVP login; restarting session",
                    no_video_restart_sec,
                )
                no_video_timeout = True
                break

            # Heartbeats
            if login_ok and now >= heartbeat_at:
                hb = build_vvp_packet(
                    cmd=VVP_CMD_HEARTBEAT,
                    seq=vvp_seq,
                    host_key=host_key,
                    param=8,
                    licence_id=licence_id,
                )
                flow["tx_heartbeat"] += 1
                kcp.send_iva_data(hb)
                vvp_seq += 1
                heartbeat_at = now + 10

            if (
                login_ok
                and (last_video_time is None or now - last_video_time > 3)
                and now >= start_live_retry_at
            ):
                start_live = build_vvp_packet(
                    cmd=VVP_CMD_START_LIVE,
                    seq=vvp_seq,
                    host_key=host_key,
                    param=8,
                    licence_id=licence_id,
                    quality=self._vvp_quality,
                )
                flow["tx_start_live"] += 1
                kcp.send_iva_data(start_live)
                vvp_seq += 1
                start_live_retry_at = now + 2.5

            if login_ok and now >= iva_heartbeat_at:
                flow["tx_iva_handshake"] += 1
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
                        turn.sock.settimeout(0.08)
                except socket.timeout:
                    if not self._running:
                        break
                    if login_ok and now >= heartbeat_at:
                        hb = build_vvp_packet(
                            cmd=VVP_CMD_HEARTBEAT,
                            seq=vvp_seq,
                            host_key=host_key,
                            param=8,
                            licence_id=licence_id,
                        )
                        flow["tx_heartbeat"] += 1
                        kcp.send_iva_data(hb)
                        vvp_seq += 1
                        heartbeat_at = now + 10
                    if (
                        login_ok
                        and (last_video_time is None or now - last_video_time > 3)
                        and now >= start_live_retry_at
                    ):
                        start_live = build_vvp_packet(
                            cmd=VVP_CMD_START_LIVE,
                            seq=vvp_seq,
                            host_key=host_key,
                            param=8,
                            licence_id=licence_id,
                            quality=self._vvp_quality,
                        )
                        flow["tx_start_live"] += 1
                        kcp.send_iva_data(start_live)
                        vvp_seq += 1
                        start_live_retry_at = now + 2.5
                    if login_ok and now >= iva_heartbeat_at:
                        flow["tx_iva_handshake"] += 1
                        kcp.send_handshake()
                        iva_heartbeat_at = now + 3
                    if login_ok and now >= turn_refresh_at:
                        try:
                            turn.refresh(lifetime=600)
                        except Exception:
                            pass
                        turn_refresh_at = now + 60
                    if now >= ice_resend_at:
                        flow["tx_ice_check"] += 1
                        _send_ice_checks()
                        kcp.retransmit_unacked()
                        ice_resend_at = now + 2
                    if now >= login_resend_at:
                        kcp.retransmit_unacked()
                        login_resend_at = now + 5
                    if login_ok and _video_drought(now):
                        _LOGGER.debug(
                            "No video received %.0fs after VVP login; restarting session",
                            no_video_restart_sec,
                        )
                        no_video_timeout = True
                        break
                    _attempt_kcp_gap_recovery(now)
                    continue

            raw, addr = _packet_buf.popleft()
            flow["udp_packets"] += 1
            if len(raw) < 4:
                continue

            data = raw
            source_addr = addr
            via_turn = False

            # Unwrap TURN framing
            if (raw[0] & 0xC0) == 0x40:
                flow["turn_channel_packets"] += 1
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
                        flow["data_indications"] += 1
                        inner_data = msg["attrs"].get(ATTR_DATA, b"")
                        pip, pport = None, None
                        if ATTR_XOR_PEER_ADDRESS in msg["attrs"]:
                            pip, pport = _decode_xor_address(
                                msg["attrs"][ATTR_XOR_PEER_ADDRESS]
                            )
                        inner_msg = _parse_stun(inner_data)
                        if inner_msg and inner_msg["type"] == BINDING_REQUEST:
                            resp = _build_ice_response(inner_msg, ice_pwd, pip, pport)
                            turn.send_to_peer(pip, pport, resp)
                            ice_count += 1
                            request_addrs.add((pip, pport))
                            if (
                                pip
                                and pport
                                and (pip, pport) != (target_ip, target_port)
                            ):
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
                        if (
                            not remote
                            and not send_addr_holder[0]
                            and addr[0] != coturn_ip
                        ):
                            send_addr_holder[0] = addr
                            direct_addr = addr
                            kcp.retransmit_unacked()
                        continue
                    else:
                        continue

            # KCP processing
            kcp_seg = parse_kcp_segment(data)
            if kcp_seg:
                flow["kcp_segments"] += 1
                result = kcp.process_input(data)
                kcp.flush_acks()
                if kcp_seg["cmd"] == 81:
                    flow["kcp_push"] += 1
                    kcp_push_count += 1
                    now_push = time.time()
                    if last_push_time > 0.0:
                        delta = now_push - last_push_time
                        if 0.002 <= delta <= 0.200:
                            inst_rate = min(1000.0, 1.0 / delta)
                            push_rate_ema = (push_rate_ema * 0.92) + (inst_rate * 0.08)
                    last_push_time = now_push
                    last_kcp_data_time = now_push
                    if not confirmed_peer[0] and source_addr:
                        cp_ip, cp_port = source_addr
                        is_direct = (
                            not via_turn and not remote and _is_private_ip(cp_ip)
                        )
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
                        flow["kcp_data_msgs"] += 1
                        last_kcp_payload_time = time.time()
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
                                login_ok_at = time.time()
                                stream_start_time = time.time()
                                if self.on_login:
                                    self.on_login()
                                hb = build_vvp_packet(
                                    cmd=VVP_CMD_HEARTBEAT,
                                    seq=vvp_seq,
                                    host_key=host_key,
                                    param=8,
                                    licence_id=licence_id,
                                )
                                flow["tx_heartbeat"] += 1
                                kcp.send_iva_data(hb)
                                vvp_seq += 1
                                # Push an explicit START_LIVE as soon as login
                                # is confirmed; some sessions otherwise remain
                                # audio-only or idle until a later retry.
                                start_live = build_vvp_packet(
                                    cmd=VVP_CMD_START_LIVE,
                                    seq=vvp_seq,
                                    host_key=host_key,
                                    param=8,
                                    licence_id=licence_id,
                                    quality=self._vvp_quality,
                                )
                                flow["tx_start_live"] += 1
                                kcp.send_iva_data(start_live)
                                vvp_seq += 1
                            ice_deadline = max(ice_deadline, time.time() + 30)
                            _handle_stream_payload(payload)
                        drained = _drain_kcp_queue()
                        if drained:
                            last_kcp_payload_time = time.time()

                # Stall recovery + deadline management
                if login_ok:
                    now2 = time.time()
                    kcp_alive = last_kcp_data_time and now2 - last_kcp_data_time < 12
                    if not kcp_alive and last_kcp_data_time:
                        break
                    if last_video_time and now2 - last_video_time < 10:
                        ice_deadline = max(ice_deadline, time.time() + 15)
                    elif kcp_alive:
                        ice_deadline = max(ice_deadline, time.time() + 10)
                    _attempt_kcp_gap_recovery(now2)
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
            _log_flow_summary("login-failed")
            return (stream_video_count, stream_total_bytes)

        if no_video_timeout:
            _log_flow_summary("no-video-timeout")
            return (stream_video_count, stream_total_bytes)

        if last_video_time and time.time() - last_video_time > 10:
            _log_flow_summary("video-stale")
            return (stream_video_count, stream_total_bytes)

        # Continuation receiver
        v2, b2 = self._receive_stream(
            turn,
            kcp,
            ice_pwd,
            host_key,
            licence_id,
            vvp_seq,
            stream_frame_count,
            stream_video_count,
            stream_total_bytes,
            stream_start_time,
        )
        _log_flow_summary("handoff-to-continuation")
        return (v2, b2)

    # ------------------------------------------------------------------
    # Continuation receiver
    # ------------------------------------------------------------------

    def _receive_stream(
        self,
        turn,
        kcp,
        ice_pwd,
        host_key,
        licence_id,
        vvp_seq_start,
        frame_count_start,
        video_count_start,
        bytes_start,
        start_time,
    ) -> tuple[int, int]:
        """Continue receiving video after the connection loop."""
        frame_count = frame_count_start
        video_frame_count = video_count_start
        total_bytes = bytes_start
        last_video_time: float | None = None
        last_kcp_data_time = time.time()
        last_kcp_payload_time = time.time()
        last_video_payload_time = time.time()
        last_nudge_time = 0.0
        last_skip_time = 0.0
        last_gap_sn = -1
        last_gap_since = 0.0
        gap_nudges = 0
        push_rate_ema = 140.0
        last_push_time = 0.0
        audio_catchup_until = 0.0
        audio_catchup_stride = 4
        audio_catchup_seen = 0
        skip_expect_recovery_by = 0.0
        skip_failures = 0
        skip_backoff_until = 0.0
        last_skip_video_time = 0.0
        long_stall_marks: deque[float] = deque()
        recent_skip_marks: deque[float] = deque()
        recent_severe_skip_marks: deque[float] = deque()
        recent_skip_budget_marks: deque[float] = deque()
        severe_skip_quiet_until = 0.0
        lossy_session_mode = False
        lossy_reentry_block_until = 0.0
        recovery_aggr = 0.0
        link_class = "clean"
        vvp_seq = vvp_seq_start
        # After a KCP gap skip, drop P-frames until next I-frame
        _skip_pframes_until_iframe = True
        _idr_wait_drops = 0
        _idr_wait_started_at = time.time()
        _idr_wait_reason = "session-start"
        _idr_wait_events = 1
        _forced_resume_count = 0
        _trusted_idr_seen = False
        _first_trusted_idr_at = 0.0
        _video_forwarded_before_trusted_idr = 0
        _video_forwarded_after_trusted_idr = 0
        # Frame type statistics
        _iframes_received = 0
        _pframes_received = 0
        _iframes_rejected = 0
        _active_gap_skip_diag: dict[str, Any] | None = None
        last_heartbeat = time.time()
        last_iva_heartbeat = time.time()
        start_live_retry_at = time.time() + 2.5
        last_turn_refresh = time.time()
        recv_flow = {
            "udp_packets": 0,
            "turn_channel_packets": 0,
            "data_indications": 0,
            "kcp_segments": 0,
            "kcp_push": 0,
            "kcp_data_msgs": 0,
            "payload_calls": 0,
            "chunks": 0,
            "parse_ok": 0,
            "parse_fail": 0,
            "video_chunks": 0,
            "audio_chunks": 0,
        }

        def _process_kcp_message(msg_data):
            nonlocal frame_count, video_frame_count, total_bytes
            nonlocal last_video_time, last_kcp_payload_time, last_video_payload_time
            nonlocal audio_catchup_until, audio_catchup_seen
            nonlocal _skip_pframes_until_iframe, _idr_wait_drops, _idr_wait_started_at
            nonlocal _idr_wait_reason, _idr_wait_events, _forced_resume_count
            nonlocal _trusted_idr_seen, _first_trusted_idr_at
            nonlocal _video_forwarded_before_trusted_idr, _video_forwarded_after_trusted_idr
            nonlocal _iframes_received, _pframes_received, _iframes_rejected
            nonlocal _active_gap_skip_diag
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
            last_kcp_payload_time = time.time()
            recv_flow["payload_calls"] += 1
            for chunk in split_stream_frames(payload):
                if len(chunk) < 4:
                    continue
                if chunk[0] != 0 or chunk[1] != 0 or chunk[2] != 1:
                    continue
                recv_flow["chunks"] += 1
                frame_type = chunk[3]
                frame_count += 1
                parsed = self._parse_stream_chunk(chunk)
                if not parsed:
                    recv_flow["parse_fail"] += 1
                    continue
                recv_flow["parse_ok"] += 1
                ftype, _, media_data = parsed
                if ftype in (STREAM_TYPE_IFRAME, STREAM_TYPE_PFRAME):
                    recv_flow["video_chunks"] += 1
                    video_frame_count += 1
                    total_bytes += len(media_data)
                    self._video_count += 1
                    self._total_bytes += len(media_data)
                    now_frame = time.time()
                    last_video_payload_time = now_frame
                    # Track frame types for diagnostics
                    if ftype == STREAM_TYPE_IFRAME:
                        _iframes_received += 1
                    elif ftype == STREAM_TYPE_PFRAME:
                        _pframes_received += 1
                    # After KCP gap skip: drop P-frames until next I-frame
                    if _skip_pframes_until_iframe:
                        force_resume = (
                            ftype == STREAM_TYPE_IFRAME
                            and _idr_wait_started_at > 0
                            and (time.time() - _idr_wait_started_at) > 12.0
                        )
                        # For decoder-reset recovery, accept bare IDR VCL (no
                        # param sets) because the coordinator will prepend its
                        # cached VPS/SPS/PPS before forwarding to ffmpeg.
                        decoder_reset_recovery = _idr_wait_reason in (
                            "kcp-gap",
                            "video-stall",
                        )
                        is_valid_idr = _is_idr_video_frame(
                            ftype,
                            media_data,
                            require_param_sets=not decoder_reset_recovery,
                        )
                        if is_valid_idr or force_resume:
                            if force_resume:
                                _forced_resume_count += 1
                                _LOGGER.warning(
                                    "IDR wait timeout after %s (%.2fs) - forcing resume on I-frame",
                                    _idr_wait_reason,
                                    time.time() - _idr_wait_started_at,
                                )
                            elif not _trusted_idr_seen:
                                _trusted_idr_seen = True
                                _first_trusted_idr_at = time.time()
                                _LOGGER.info(
                                    "Trusted IDR acquired (continuation, reason=%s, wait=%.2fs, dropped=%d, size=%d)",
                                    _idr_wait_reason,
                                    time.time() - _idr_wait_started_at,
                                    _idr_wait_drops,
                                    len(media_data),
                                )
                            elif _idr_wait_reason in ("kcp-gap", "video-stall"):
                                gap_event_id = int(
                                    (_active_gap_skip_diag or {}).get("event_id", 0)
                                )
                                gap_severity = str(
                                    (_active_gap_skip_diag or {}).get(
                                        "severity", "unknown"
                                    )
                                )
                                reset_mode = str(
                                    (_active_gap_skip_diag or {}).get(
                                        "mode", _idr_wait_reason
                                    )
                                )
                                _LOGGER.info(
                                    "IDR frame recovered after %s #%d (continuation, %s, wait=%.2fs, dropped=%d frames, size=%d)",
                                    reset_mode,
                                    gap_event_id,
                                    gap_severity,
                                    time.time() - _idr_wait_started_at,
                                    _idr_wait_drops,
                                    len(media_data),
                                )
                            _skip_pframes_until_iframe = False
                            _LOGGER.debug(
                                "IDR after decoder reset — resumed (%d frames dropped)",
                                _idr_wait_drops,
                            )
                            _idr_wait_drops = 0
                            _idr_wait_started_at = 0.0
                            _active_gap_skip_diag = None
                        else:
                            # I-frame that failed IDR validation (likely incomplete NAL structure)
                            if ftype == STREAM_TYPE_IFRAME:
                                _iframes_rejected += 1
                                gap_event_id = int(
                                    (_active_gap_skip_diag or {}).get("event_id", 0)
                                )
                                gap_severity = str(
                                    (_active_gap_skip_diag or {}).get(
                                        "severity", "unknown"
                                    )
                                )
                                if _idr_wait_reason in ("kcp-gap", "video-stall"):
                                    reset_mode = str(
                                        (_active_gap_skip_diag or {}).get(
                                            "mode", _idr_wait_reason
                                        )
                                    )
                                    _LOGGER.warning(
                                        "Rejected I-frame failing IDR validation after %s #%d (continuation, %s, size=%d bytes, potential corruption)",
                                        reset_mode,
                                        gap_event_id,
                                        gap_severity,
                                        len(media_data),
                                    )
                                else:
                                    _LOGGER.warning(
                                        "Rejected I-frame failing IDR validation (continuation, size=%d bytes, potential corruption)",
                                        len(media_data),
                                    )
                            _idr_wait_drops += 1
                            continue
                    if _trusted_idr_seen:
                        _video_forwarded_after_trusted_idr += 1
                    else:
                        _video_forwarded_before_trusted_idr += 1
                    last_video_time = now_frame
                    if self.on_video:
                        self.on_video(media_data)
                    continue
                if ftype == STREAM_TYPE_AUDIO:
                    recv_flow["audio_chunks"] += 1
                    if self.on_audio:
                        self.on_audio(media_data)
            return True

        # Drain queued KCP messages
        while True:
            queued = kcp.poll_data()
            if not queued:
                break
            _process_kcp_message(queued)

        def _kcp_gap_state() -> tuple[bool, int, int]:
            next_sn = int(getattr(kcp, "next_recv_sn", -1))
            recv_buf = getattr(kcp, "recv_buf", {})
            if next_sn < 0 or not recv_buf:
                return (False, 0, 0)
            if next_sn in recv_buf:
                return (False, 0, 0)
            higher = [sn for sn in recv_buf if sn > next_sn]
            if not higher:
                return (False, 0, 0)
            min_above = min(higher)
            max_above = max(higher)
            gap_size = max(1, min_above - next_sn)
            backlog = (max_above - next_sn) + 1
            return (True, gap_size, backlog)

        def _drain_recv_queue() -> int:
            drained = 0
            while True:
                queued = kcp.poll_data()
                if not queued:
                    break
                _process_kcp_message(queued)
                drained += 1
            return drained

        def _attempt_kcp_gap_recovery(now_ts: float) -> None:
            nonlocal last_nudge_time, last_skip_time, last_kcp_payload_time
            nonlocal last_video_payload_time
            nonlocal last_gap_sn, last_gap_since, gap_nudges
            nonlocal audio_catchup_until, audio_catchup_seen
            nonlocal push_rate_ema
            nonlocal skip_expect_recovery_by, skip_failures, skip_backoff_until
            nonlocal last_skip_video_time
            nonlocal long_stall_marks
            nonlocal recent_skip_marks
            nonlocal recent_severe_skip_marks
            nonlocal recent_skip_budget_marks
            nonlocal severe_skip_quiet_until
            nonlocal lossy_session_mode
            nonlocal lossy_reentry_block_until
            nonlocal recovery_aggr
            nonlocal link_class
            nonlocal _skip_pframes_until_iframe, _idr_wait_started_at
            nonlocal _idr_wait_reason, _idr_wait_events
            nonlocal _active_gap_skip_diag
            nonlocal vvp_seq, start_live_retry_at
            if last_kcp_data_time is None:
                last_gap_sn = -1
                last_gap_since = 0.0
                gap_nudges = 0
                skip_expect_recovery_by = 0.0
                skip_backoff_until = 0.0
                skip_failures = 0
                recovery_aggr = 0.0
                return
            if last_video_time is None:
                last_gap_sn = -1
                last_gap_since = 0.0
                gap_nudges = 0
                skip_expect_recovery_by = 0.0
                skip_backoff_until = 0.0
                skip_failures = 0
                recovery_aggr = 0.0
                return

            if skip_expect_recovery_by > 0.0 and now_ts >= skip_expect_recovery_by:
                if last_video_time <= last_skip_video_time:
                    skip_failures = min(skip_failures + 1, 6)
                    skip_backoff_until = now_ts + min(4.0, 0.5 + (skip_failures * 0.6))
                else:
                    skip_failures = max(0, skip_failures - 2)
                skip_expect_recovery_by = 0.0

            stall = now_ts - last_video_time
            if stall <= 0.15:
                recovery_aggr = max(0.0, recovery_aggr * 0.92)
                # De-escalate lossy session mode once the stream proves healthy again.
                if lossy_session_mode and recovery_aggr < 0.18 and skip_failures == 0:
                    while long_stall_marks and (now_ts - long_stall_marks[0]) > 20.0:
                        long_stall_marks.popleft()
                    if not long_stall_marks:
                        lossy_session_mode = False
                        lossy_reentry_block_until = max(
                            lossy_reentry_block_until, now_ts + 7.0
                        )
                        _LOGGER.info(
                            "Auto-de-escalating lossy session mode (stream healthy, recovery_aggr=%.3f)",
                            recovery_aggr,
                        )
                return

            if stall > 3.0:
                if not long_stall_marks or (now_ts - long_stall_marks[-1]) > 2.0:
                    long_stall_marks.append(now_ts)
            while long_stall_marks and (now_ts - long_stall_marks[0]) > 20.0:
                long_stall_marks.popleft()
            long_stalls_recent = len(long_stall_marks)
            while recent_skip_marks and (now_ts - recent_skip_marks[0]) > 12.0:
                recent_skip_marks.popleft()
            recent_skip_count = len(recent_skip_marks)
            while (
                recent_skip_budget_marks
                and (now_ts - recent_skip_budget_marks[0]) > 10.0
            ):
                recent_skip_budget_marks.popleft()
            skip_budget_10s = len(recent_skip_budget_marks)
            while (
                recent_severe_skip_marks
                and (now_ts - recent_severe_skip_marks[0]) > 20.0
            ):
                recent_severe_skip_marks.popleft()
            severe_skip_recent_count = len(recent_severe_skip_marks)

            gap_pending, gap_size, backlog = _kcp_gap_state()
            next_sn = int(getattr(kcp, "next_recv_sn", -1))
            if gap_pending:
                if next_sn != last_gap_sn:
                    last_gap_sn = next_sn
                    last_gap_since = now_ts
                    gap_nudges = 0
            else:
                last_gap_sn = -1
                last_gap_since = 0.0
                gap_nudges = 0

            backlog_secs = backlog / max(1.0, push_rate_ema)

            elapsed_s = max(1.0, now_ts - start_time)
            live_fps = video_frame_count / elapsed_s if video_frame_count > 0 else 0.0
            fps_pressure = max(0.0, min(1.0, (10.5 - live_fps) / 10.5))
            stall_pressure = max(0.0, min(1.0, (stall - 0.8) / 3.0))
            long_stall_pressure = max(0.0, min(1.0, long_stalls_recent / 3.0))
            backlog_pressure = max(0.0, min(1.0, backlog_secs / 2.0))
            failure_pressure = max(0.0, min(1.0, skip_failures / 3.0))
            gap_pressure = max(0.0, min(1.0, gap_size / 6.0))
            pressure = (
                (0.30 * stall_pressure)
                + (0.25 * long_stall_pressure)
                + (0.20 * fps_pressure)
                + (0.15 * backlog_pressure)
                + (0.10 * failure_pressure)
            )
            if self._adaptive_lossy_gap_skip:
                pressure = min(1.0, pressure + 0.15)
            recovery_aggr = (recovery_aggr * 0.75) + (pressure * 0.25)

            link_instability = (
                (0.40 * backlog_pressure)
                + (0.25 * long_stall_pressure)
                + (0.20 * gap_pressure)
                + (0.15 * min(1.0, recent_skip_count / 4.0))
            )
            if recovery_aggr >= 0.78 or link_instability >= 0.80:
                link_class = "critical"
            elif recovery_aggr >= 0.56 or link_instability >= 0.62:
                link_class = "lossy"
            elif recovery_aggr >= 0.34 or link_instability >= 0.42:
                link_class = "stressed"
            else:
                link_class = "clean"

            skip_budget_cap_10s = 3
            if link_class == "clean":
                skip_budget_cap_10s = 2
            elif link_class == "stressed":
                skip_budget_cap_10s = 3
            elif link_class == "lossy":
                skip_budget_cap_10s = 4
            elif link_class == "critical":
                skip_budget_cap_10s = 5

            if (
                skip_budget_10s >= skip_budget_cap_10s
                and stall > 2.0
                and (now_ts - start_time) > 8.0
            ):
                _LOGGER.warning(
                    "Skip budget exceeded (%d/%d in 10s, class=%s, stall=%.2fs) - restarting session",
                    skip_budget_10s,
                    skip_budget_cap_10s,
                    link_class,
                    stall,
                )
                self.request_stop()
                return

            nudge_interval = max(
                0.03,
                min(
                    0.25,
                    (0.06 + (backlog_secs * 0.04)) * (1.0 - (0.40 * recovery_aggr)),
                ),
            )

            if gap_pending and now_ts - last_nudge_time > nudge_interval:
                if kcp.send_gap_nudge():
                    last_nudge_time = now_ts
                    gap_nudges += 1

            payload_idle = now_ts - last_kcp_payload_time
            video_payload_idle = now_ts - last_video_payload_time
            recovery_payload_idle = max(payload_idle, video_payload_idle)
            gap_age = (now_ts - last_gap_since) if last_gap_since > 0.0 else 0.0
            adaptive_wait_base = max(
                2.5, min(6.0, 4.5 - (0.65 * min(backlog_secs, 2.5)))
            )
            adaptive_wait = max(
                1.4, adaptive_wait_base * (1.0 - (0.45 * recovery_aggr))
            )
            min_nudges_base = (
                3 if backlog_secs >= 2.0 else 3 if backlog_secs >= 1.0 else 4
            )
            min_nudges = max(1, int(round(min_nudges_base - (2.0 * recovery_aggr))))
            small_gap = bool(
                gap_pending and gap_size <= 4 and backlog <= 128 and backlog_secs < 0.9
            )
            tiny_gap = bool(
                gap_pending and gap_size <= 2 and backlog <= 64 and backlog_secs < 0.45
            )
            if small_gap:
                adaptive_wait = max(adaptive_wait, 2.2)
                min_nudges = max(min_nudges, 5)
            if tiny_gap:
                adaptive_wait = max(adaptive_wait, 2.8)
                min_nudges = max(min_nudges, 6)

            if (
                not self._allow_lossy_gap_skip
                and not lossy_session_mode
                and now_ts >= lossy_reentry_block_until
                and gap_pending
                and (
                    (stall > 2.5 and backlog_secs > 0.8)
                    or (
                        self._adaptive_lossy_gap_skip
                        and (now_ts - start_time) > 8.0
                        and stall > 2.0
                        and (recovery_aggr > 0.40 or live_fps < 7.0)
                    )
                    or (long_stalls_recent >= 1 and stall > 1.6)
                )
            ):
                lossy_session_mode = True
                _LOGGER.warning(
                    "Auto-escalating to lossy recovery for this session (stall=%.2fs, long_stalls=%d)",
                    stall,
                    long_stalls_recent,
                )

            effective_lossy = (
                self._allow_lossy_gap_skip
                or lossy_session_mode
                or (
                    self._adaptive_lossy_gap_skip
                    and gap_pending
                    and recovery_aggr >= 0.55
                )
                or skip_failures >= 1
                or (stall > 8.0 and gap_pending and backlog_secs > 1.6)
                or (gap_pending and long_stalls_recent >= 2 and stall > 2.6)
            )
            if effective_lossy:
                lossy_scale = 0.50 if self._allow_lossy_gap_skip else 0.58
                lossy_scale = max(0.35, lossy_scale - (0.18 * recovery_aggr))
                min_lossy_wait = 0.7
                if not self._allow_lossy_gap_skip:
                    min_lossy_wait = (
                        1.8
                        if (self._adaptive_lossy_gap_skip or lossy_session_mode)
                        else 0.9
                    )
                lossy_wait = max(min_lossy_wait, min(3.0, adaptive_wait * lossy_scale))
                nudge_gate = max(
                    1,
                    min_nudges - (2 if self._adaptive_lossy_gap_skip else 1),
                )
                idle_gate = lossy_wait * (
                    0.20 if self._adaptive_lossy_gap_skip else 0.30
                )
                age_gate = lossy_wait * (
                    0.25 if self._adaptive_lossy_gap_skip else 0.35
                )
                skip_cooldown_floor = (
                    2.1
                    if (self._adaptive_lossy_gap_skip or lossy_session_mode)
                    else 0.8
                )
                dynamic_skip_cooldown = skip_cooldown_floor + (
                    0.7 * min(4, recent_skip_count)
                )
                max_adaptive_skip_depth = 2
                if link_class == "clean":
                    lossy_wait *= 1.25
                    dynamic_skip_cooldown += 1.1
                    max_adaptive_skip_depth = 1
                elif link_class == "stressed":
                    lossy_wait *= 1.10
                    dynamic_skip_cooldown += 0.7
                    max_adaptive_skip_depth = 2
                elif link_class == "lossy":
                    max_adaptive_skip_depth = 2
                else:
                    lossy_wait *= 0.92
                    max_adaptive_skip_depth = 3
                if (
                    gap_pending
                    and now_ts >= skip_backoff_until
                    and stall > lossy_wait
                    and (not small_gap or stall > max(lossy_wait + 0.6, 2.2))
                    and (not tiny_gap or stall > max(lossy_wait + 1.2, 2.8))
                    and recovery_payload_idle > idle_gate
                    and gap_age > age_gate
                    and gap_nudges >= nudge_gate
                    and now_ts - last_skip_time
                    > max(
                        dynamic_skip_cooldown,
                        (lossy_wait * 0.55)
                        + (0.25 * skip_failures)
                        - (0.40 * recovery_aggr),
                    )
                ):
                    severe_skip_emergency = bool(
                        stall > 10.0
                        or backlog_secs > 5.0
                        or (gap_size >= 8 and stall > 6.0)
                    )
                    severe_link = bool(
                        stall > 5.0
                        or backlog_secs > 3.0
                        or (long_stalls_recent >= 3 and stall > 3.2)
                        or recovery_aggr > 0.80
                    )
                    if (
                        severe_link
                        and not severe_skip_emergency
                        and (
                            now_ts < severe_skip_quiet_until
                            or severe_skip_recent_count >= 2
                        )
                    ):
                        severe_link = False
                    if severe_link and self._allow_lossy_gap_skip:
                        # Only forced-lossy mode can skip an unbounded gap range.
                        skip_depth = None
                    elif severe_link:
                        # For single-gap severe events, use a shallower skip to
                        # avoid over-jumping and repeated recovery churn.
                        if gap_size <= 1 and backlog_secs < 2.2 and stall < 6.0:
                            skip_depth = 2
                        else:
                            # Adaptive mode should remain bounded to avoid jumping too
                            # far into a potentially unstable reference chain.
                            skip_depth = 2 if not severe_skip_emergency else 3
                    elif tiny_gap:
                        skip_depth = 1
                    elif small_gap:
                        skip_depth = 2
                    elif self._allow_lossy_gap_skip:
                        skip_depth = None
                    elif self._adaptive_lossy_gap_skip or lossy_session_mode:
                        skip_depth = 2
                    else:
                        skip_depth = 2
                    if skip_depth is not None:
                        skip_depth = max(1, min(skip_depth, max_adaptive_skip_depth))
                    if kcp.skip_gap(max_gaps=skip_depth):
                        gap_severity = self._classify_gap_skip_severity(
                            severe_link=severe_link,
                            gap_size=gap_size,
                            backlog_secs=backlog_secs,
                            stall_s=stall,
                            recovery_aggr=recovery_aggr,
                        )
                        gap_mode = (
                            "forced-lossy"
                            if self._allow_lossy_gap_skip
                            else (
                                "adaptive-lossy"
                                if (self._adaptive_lossy_gap_skip or lossy_session_mode)
                                else "adaptive"
                            )
                        )
                        if not _skip_pframes_until_iframe:
                            _idr_wait_events += 1
                            _idr_wait_started_at = now_ts  # only set on the first skip
                        _idr_wait_reason = "kcp-gap"
                        _skip_pframes_until_iframe = True
                        _active_gap_skip_diag = self._notify_gap_skip(
                            {
                                "severity": gap_severity,
                                "started_mono": time.monotonic(),
                                "wall_time": now_ts,
                                "stall_s": round(stall, 3),
                                "gap_size": int(gap_size),
                                "backlog_frames": int(backlog),
                                "backlog_s": round(backlog_secs, 3),
                                "skip_depth": (
                                    -1 if skip_depth is None else int(skip_depth)
                                ),
                                "nudges": int(gap_nudges),
                                "skip_failures": int(skip_failures),
                                "recovery_aggr": round(recovery_aggr, 3),
                                "link_class": link_class,
                                "mode": gap_mode,
                            }
                        )
                        _LOGGER.info(
                            "KCP gap skip #%d (continuation, %s, %s): stall=%.2fs gap=%d backlog=%d (%.2fs) skip_depth=%s",
                            int(_active_gap_skip_diag.get("event_id", 0)),
                            gap_severity,
                            gap_mode,
                            stall,
                            gap_size,
                            backlog,
                            backlog_secs,
                            "all" if skip_depth is None else skip_depth,
                        )
                        if gap_severity == "severe":
                            recent_severe_skip_marks.append(now_ts)
                            severe_skip_quiet_until = max(
                                severe_skip_quiet_until, now_ts + 3.2
                            )
                        recent_skip_budget_marks.append(now_ts)
                        recent_skip_marks.append(now_ts)
                        last_skip_time = now_ts
                        kcp.flush_acks()
                        drained = _drain_recv_queue()
                        if drained:
                            last_kcp_payload_time = time.time()
                        last_skip_video_time = last_video_time
                        skip_expect_recovery_by = now_ts + 0.8
                        _idr_request = build_vvp_packet(
                            cmd=VVP_CMD_START_LIVE,
                            seq=vvp_seq,
                            host_key=host_key,
                            param=8,
                            licence_id=licence_id,
                            quality=self._vvp_quality,
                        )
                        kcp.send_iva_data(_idr_request)
                        vvp_seq += 1
                        start_live_retry_at = now_ts + 2.5
                return

            if (
                gap_pending
                and now_ts >= skip_backoff_until
                and stall > adaptive_wait
                and recovery_payload_idle > (adaptive_wait * 0.50)
                and gap_age > adaptive_wait
                and gap_nudges >= min_nudges
                and now_ts - last_skip_time
                > max(
                    3.0 + (skip_failures * 1.0) + (0.8 * min(4, recent_skip_count)),
                    adaptive_wait,
                )
            ):
                if kcp.skip_gap(max_gaps=1):
                    gap_severity = self._classify_gap_skip_severity(
                        severe_link=False,
                        gap_size=gap_size,
                        backlog_secs=backlog_secs,
                        stall_s=stall,
                        recovery_aggr=recovery_aggr,
                    )
                    if not _skip_pframes_until_iframe:
                        _idr_wait_events += 1
                        _idr_wait_started_at = now_ts  # only set on the first skip
                    _idr_wait_reason = "kcp-gap"
                    _skip_pframes_until_iframe = True
                    _active_gap_skip_diag = self._notify_gap_skip(
                        {
                            "severity": gap_severity,
                            "started_mono": time.monotonic(),
                            "wall_time": now_ts,
                            "stall_s": round(stall, 3),
                            "gap_size": int(gap_size),
                            "backlog_frames": int(backlog),
                            "backlog_s": round(backlog_secs, 3),
                            "skip_depth": 1,
                            "nudges": int(gap_nudges),
                            "skip_failures": int(skip_failures),
                            "recovery_aggr": round(recovery_aggr, 3),
                            "mode": "adaptive-single",
                        }
                    )
                    last_skip_time = now_ts
                    _LOGGER.warning(
                        "Adaptive KCP skip #%d (continuation, %s): stall=%.2fs gap=%d backlog=%d (%.2fs) nudges=%d wait=%.2fs fails=%d next_sn=%d",
                        int(_active_gap_skip_diag.get("event_id", 0)),
                        gap_severity,
                        stall,
                        gap_size,
                        backlog,
                        backlog_secs,
                        gap_nudges,
                        adaptive_wait,
                        skip_failures,
                        next_sn,
                    )
                    recent_skip_marks.append(now_ts)
                    kcp.flush_acks()
                    drained = _drain_recv_queue()
                    if drained:
                        last_kcp_payload_time = time.time()
                    last_skip_video_time = last_video_time
                    skip_expect_recovery_by = now_ts + 0.8
                    _idr_request = build_vvp_packet(
                        cmd=VVP_CMD_START_LIVE,
                        seq=vvp_seq,
                        host_key=host_key,
                        param=8,
                        licence_id=licence_id,
                        quality=self._vvp_quality,
                    )
                    kcp.send_iva_data(_idr_request)
                    vvp_seq += 1
                    start_live_retry_at = now_ts + 2.5

            stall_reset_threshold = 1.7 if live_fps >= 10.0 else 1.4
            stall_reset_idle_threshold = 0.6 if live_fps >= 10.0 else 0.45
            stall_reset_gap_ok = not gap_pending or (
                gap_size <= 4
                and backlog_secs < 1.0
                and stall > (stall_reset_threshold + 0.2)
            )
            if (
                not _skip_pframes_until_iframe
                and now_ts >= skip_backoff_until
                and stall > stall_reset_threshold
                and recovery_payload_idle > stall_reset_idle_threshold
                and now_ts - last_skip_time > 1.6
                and (now_ts - start_time) > 8.0
                and stall_reset_gap_ok
            ):
                stall_reset_gap_size = int(gap_size if gap_pending else 0)
                stall_reset_backlog = int(backlog if gap_pending else 0)
                stall_reset_backlog_s = round(backlog_secs if gap_pending else 0.0, 3)
                gap_severity = self._classify_gap_skip_severity(
                    severe_link=True,
                    gap_size=stall_reset_gap_size,
                    backlog_secs=stall_reset_backlog_s,
                    stall_s=stall,
                    recovery_aggr=recovery_aggr,
                )
                _idr_wait_events += 1
                _idr_wait_started_at = now_ts
                _idr_wait_reason = "video-stall"
                _skip_pframes_until_iframe = True
                _active_gap_skip_diag = self._notify_gap_skip(
                    {
                        "severity": gap_severity,
                        "started_mono": time.monotonic(),
                        "wall_time": now_ts,
                        "stall_s": round(stall, 3),
                        "gap_size": stall_reset_gap_size,
                        "backlog_frames": stall_reset_backlog,
                        "backlog_s": stall_reset_backlog_s,
                        "skip_depth": 0,
                        "nudges": 0,
                        "skip_failures": int(skip_failures),
                        "recovery_aggr": round(recovery_aggr, 3),
                        "payload_idle_s": round(recovery_payload_idle, 3),
                        "live_fps": round(live_fps, 2),
                        "mode": "stall-reset",
                    }
                )
                _LOGGER.warning(
                    "Video stall reset #%d (continuation, %s): stall=%.2fs idle=%.2fs live_fps=%.2f gap=%d backlog=%.2fs",
                    int(_active_gap_skip_diag.get("event_id", 0)),
                    gap_severity,
                    stall,
                    recovery_payload_idle,
                    live_fps,
                    stall_reset_gap_size,
                    stall_reset_backlog_s,
                )
                last_skip_time = now_ts
                last_skip_video_time = last_video_time
                skip_expect_recovery_by = now_ts + 0.8
                _idr_request = build_vvp_packet(
                    cmd=VVP_CMD_START_LIVE,
                    seq=vvp_seq,
                    host_key=host_key,
                    param=8,
                    licence_id=licence_id,
                    quality=self._vvp_quality,
                )
                kcp.send_iva_data(_idr_request)
                vvp_seq += 1
                start_live_retry_at = now_ts + 2.5
                return

            # Hard stall ceiling: if video has been completely frozen for too
            # long, restart unconditionally rather than waiting for the individual
            # condition thresholds to eventually all align.
            if stall > 10.0 and (now_ts - start_time) > 12.0:
                _LOGGER.warning(
                    "Hard stall ceiling hit (stall=%.2fs), restarting session",
                    stall,
                )
                self.request_stop()

            if gap_pending and skip_failures >= 3 and stall > 8.0:
                _LOGGER.warning(
                    "Persistent KCP gap (stall=%.2fs, backlog=%d, failures=%d), restarting session",
                    stall,
                    backlog,
                    skip_failures,
                )
                self.request_stop()

            if (
                gap_pending
                and severe_skip_recent_count >= 3
                and stall > 4.0
                and (now_ts - start_time) > 10.0
                and (now_ts - last_skip_time) < 9.0
            ):
                _LOGGER.warning(
                    "Severe KCP chain detected (%d severe skips/20s, stall=%.2fs), restarting session",
                    severe_skip_recent_count,
                    stall,
                )
                self.request_stop()

            if gap_pending and long_stalls_recent >= 3 and stall > 5.0:
                _LOGGER.warning(
                    "Repeated long stalls (%d in 20s, stall=%.2fs), restarting session",
                    long_stalls_recent,
                    stall,
                )
                self.request_stop()

            if long_stalls_recent >= 4 and stall > 3.8 and (now_ts - start_time) > 20.0:
                _LOGGER.warning(
                    "Persistent degradation (%d long stalls, current %.2fs), rolling session",
                    long_stalls_recent,
                    stall,
                )
                self.request_stop()

            if (
                gap_pending
                and stall > 7.0
                and recovery_payload_idle > 2.5
                and gap_age > 4.0
            ):
                _LOGGER.warning(
                    "Frozen gap persists (stall=%.2fs, idle=%.2fs, gap_age=%.2fs), restarting session",
                    stall,
                    recovery_payload_idle,
                    gap_age,
                )
                self.request_stop()

        # Send initial heartbeat
        if host_key:
            hb = build_vvp_packet(
                cmd=VVP_CMD_HEARTBEAT,
                seq=vvp_seq,
                host_key=host_key,
                param=8,
                licence_id=licence_id,
            )
            kcp.send_iva_data(hb)
            vvp_seq += 1
            # Issue START_LIVE immediately after heartbeat in continuation,
            # then keep periodic retries if no video arrives.
            start_live = build_vvp_packet(
                cmd=VVP_CMD_START_LIVE,
                seq=vvp_seq,
                host_key=host_key,
                param=8,
                licence_id=licence_id,
                quality=self._vvp_quality,
            )
            kcp.send_iva_data(start_live)
            vvp_seq += 1

        _recv_buf: deque = deque()
        timeout_count = 0
        no_video_restart_sec = 15.0

        while self._running:
            if not _recv_buf:
                kcp.flush_acks()
                turn.sock.settimeout(0.08)
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
                        turn.sock.settimeout(0.08)
                except socket.timeout:
                    now = time.time()
                    if not self._running:
                        break
                    if host_key and now - last_heartbeat >= 10:
                        hb = build_vvp_packet(
                            cmd=VVP_CMD_HEARTBEAT,
                            seq=vvp_seq,
                            host_key=host_key,
                            param=8,
                            licence_id=licence_id,
                        )
                        kcp.send_iva_data(hb)
                        vvp_seq += 1
                        last_heartbeat = now
                    if (
                        last_video_time is None or now - last_video_time > 3
                    ) and now >= start_live_retry_at:
                        start_live = build_vvp_packet(
                            cmd=VVP_CMD_START_LIVE,
                            seq=vvp_seq,
                            host_key=host_key,
                            param=8,
                            licence_id=licence_id,
                            quality=self._vvp_quality,
                        )
                        kcp.send_iva_data(start_live)
                        vvp_seq += 1
                        start_live_retry_at = now + 2.5
                    if now - last_iva_heartbeat >= 3:
                        kcp.send_handshake()
                        last_iva_heartbeat = now
                    if now - last_turn_refresh > 60:
                        try:
                            turn.refresh(lifetime=600)
                        except Exception:
                            pass
                        last_turn_refresh = now
                    if (
                        now - start_time > 10
                        and video_frame_count == 0
                        and frame_count == 0
                    ):
                        timeout_count += 1
                        if timeout_count >= 60:
                            break
                    if (
                        last_video_time is None
                        and now - start_time > no_video_restart_sec
                    ) or (
                        last_video_time is not None
                        and now - last_video_time > no_video_restart_sec
                    ):
                        _LOGGER.debug(
                            "No video received %.0fs after login in continuation; restarting session",
                            no_video_restart_sec,
                        )
                        break
                    if last_kcp_data_time and now - last_kcp_data_time > 12:
                        _LOGGER.debug("No KCP data for 12s, ending session")
                        break
                    _attempt_kcp_gap_recovery(now)
                    continue

            raw, addr = _recv_buf.popleft()
            timeout_count = 0
            recv_flow["udp_packets"] += 1
            if len(raw) < 4:
                continue

            # Unwrap TURN framing
            data = raw
            if (raw[0] & 0xC0) == 0x40:
                recv_flow["turn_channel_packets"] += 1
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
                        recv_flow["data_indications"] += 1
                        inner = msg["attrs"].get(ATTR_DATA, b"")
                        pip, pport = None, None
                        if ATTR_XOR_PEER_ADDRESS in msg["attrs"]:
                            pip, pport = _decode_xor_address(
                                msg["attrs"][ATTR_XOR_PEER_ADDRESS]
                            )
                        inner_msg = _parse_stun(inner)
                        if (
                            inner_msg
                            and inner_msg["type"] == BINDING_REQUEST
                            and ice_pwd
                        ):
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
                recv_flow["kcp_segments"] += 1
                if seg["cmd"] == 81:
                    recv_flow["kcp_push"] += 1
                    now_push = time.time()
                    if last_push_time > 0.0:
                        delta = now_push - last_push_time
                        if 0.002 <= delta <= 0.200:
                            inst_rate = min(1000.0, 1.0 / delta)
                            push_rate_ema = (push_rate_ema * 0.92) + (inst_rate * 0.08)
                    last_push_time = now_push
                    last_kcp_data_time = now_push
                result = kcp.process_input(data)
                kcp.flush_acks()
                if result:
                    rtype, rdata = result
                    if rtype == "data" and rdata:
                        recv_flow["kcp_data_msgs"] += 1
                        _process_kcp_message(rdata)
                        _drain_recv_queue()
                # Stall recovery
                now = time.time()
                _attempt_kcp_gap_recovery(now)
                if last_kcp_data_time and now - last_kcp_data_time > 12:
                    break
            elif data[0] == 0xFF and data[1] == 0x01:
                kcp.process_input(data)

            # Heartbeats
            now = time.time()
            if (
                last_video_time is None and now - start_time > no_video_restart_sec
            ) or (
                last_video_time is not None
                and now - last_video_time > no_video_restart_sec
            ):
                _LOGGER.debug(
                    "No video received %.0fs after login in continuation; restarting session",
                    no_video_restart_sec,
                )
                break
            if host_key and now - last_heartbeat >= 10:
                hb = build_vvp_packet(
                    cmd=VVP_CMD_HEARTBEAT,
                    seq=vvp_seq,
                    host_key=host_key,
                    param=8,
                    licence_id=licence_id,
                )
                kcp.send_iva_data(hb)
                vvp_seq += 1
                last_heartbeat = now
            if (
                last_video_time is None or now - last_video_time > 3
            ) and now >= start_live_retry_at:
                start_live = build_vvp_packet(
                    cmd=VVP_CMD_START_LIVE,
                    seq=vvp_seq,
                    host_key=host_key,
                    param=8,
                    licence_id=licence_id,
                    quality=self._vvp_quality,
                )
                kcp.send_iva_data(start_live)
                vvp_seq += 1
                start_live_retry_at = now + 2.5
            if now - last_iva_heartbeat >= 3:
                kcp.send_handshake()
                last_iva_heartbeat = now
        _LOGGER.debug(
            "Continuation summary: udp=%d turn_ch=%d data_ind=%d kcp_seg=%d kcp_push=%d "
            "kcp_data=%d payloads=%d chunks=%d parse_ok=%d parse_fail=%d "
            "video_chunks=%d audio_chunks=%d video_frames=%d bytes=%d "
            "kcp_recv_buf=%d kcp_frag_parts=%d next_sn=%d",
            recv_flow["udp_packets"],
            recv_flow["turn_channel_packets"],
            recv_flow["data_indications"],
            recv_flow["kcp_segments"],
            recv_flow["kcp_push"],
            recv_flow["kcp_data_msgs"],
            recv_flow["payload_calls"],
            recv_flow["chunks"],
            recv_flow["parse_ok"],
            recv_flow["parse_fail"],
            recv_flow["video_chunks"],
            recv_flow["audio_chunks"],
            video_frame_count,
            total_bytes,
            len(getattr(kcp, "recv_buf", {})),
            len(getattr(kcp, "recv_frag_buf", [])),
            int(getattr(kcp, "next_recv_sn", -1)),
        )
        _LOGGER.debug(
            "Continuation IDR gate: trusted=%s first_idr_after=%.2fs wait_events=%d forced_resume=%d "
            "fwd_before_idr=%d fwd_after_idr=%d",
            _trusted_idr_seen,
            (_first_trusted_idr_at - start_time) if _first_trusted_idr_at > 0 else -1.0,
            _idr_wait_events,
            _forced_resume_count,
            _video_forwarded_before_trusted_idr,
            _video_forwarded_after_trusted_idr,
        )
        return (video_frame_count, total_bytes)
